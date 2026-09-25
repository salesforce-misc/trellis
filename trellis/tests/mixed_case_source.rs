//! Issue #561: a transform over a mixed-case table in a mixed-case schema
//! (`"Shop"."OrderItems"`) installs, backfills and applies CDC.
//!
//! The grammar keeps an identifier's case, so `FROM Shop.OrderItems` names
//! `"Shop"."OrderItems"`. Every catalog lookup of that table used to hand
//! `to_regclass` the unquoted `Shop.OrderItems`, which Postgres folds to
//! the nonexistent `shop.orderitems`: install failed with `NoPrimaryKey`.
//!
//! CDC is real `pgoutput` fed to a real intake and drained by hand
//! (`support/pgoutput_intake.rs`), so nothing here polls for convergence.

use std::collections::HashMap;

use trellis::defs::ast::ValueType;
use trellis::defs::{create_relationship, install_definition};

#[path = "support/pgoutput_intake.rs"]
mod pgoutput_intake;

use pgoutput_intake::Pipeline;

const SOURCE_DDL: &str = "create schema \"Shop\"; \
     create table \"Shop\".\"OrderItems\" (id bigint primary key, order_id bigint, qty integer); \
     alter table \"Shop\".\"OrderItems\" replica identity full; \
     insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (1, 10, 2), (2, 10, 3), (3, 20, 5)";

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn rows(pairs: &[(&str, &str)]) -> HashMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect()
}

async fn install(db: &testkit::TestDatabase, text: &str) {
    install_definition(
        &db.pool,
        text,
        &numeric_columns(&["id", "order_id", "qty"]),
        "public",
    )
    .await
    .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    trellis::intake::publication::settle_registrations(&db.pool).await;
}

/// A 1-1 transform from `"Shop"."OrderItems"` into a mixed-case target in
/// the same mixed-case schema, `"Shop"."ItemCopy"`.
#[tokio::test]
async fn a_one_to_one_transform_over_a_mixed_case_table_backfills_and_applies_cdc() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(SOURCE_DDL)
        .await
        .expect("seed the source");
    install(
        &db,
        "TRANSFORM Shop.ItemCopy FROM Shop.OrderItems SELECT qty AS qty",
    )
    .await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.OrderItems"]).await;
    const COPY: &str = "select id::text, qty::text from \"Shop\".\"ItemCopy\"";
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[("1", "2"), ("2", "3"), ("3", "5")]),
        "the backfill copied every pre-existing row"
    );

    pipeline
        .raw
        .batch_execute(
            "insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (4, 20, 7); \
             update \"Shop\".\"OrderItems\" set qty = 30 where id = 2; \
             delete from \"Shop\".\"OrderItems\" where id = 1",
        )
        .await
        .expect("write the source");
    pipeline.settle().await;
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[("2", "30"), ("3", "5"), ("4", "7")]),
        "CDC insert, update and delete all reached the target"
    );

    pipeline.finish().await;
}

/// An aggregate over `"Shop"."OrderItems"`, grouped by a column of it.
#[tokio::test]
async fn an_aggregate_over_a_mixed_case_table_backfills_and_applies_cdc() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(SOURCE_DDL)
        .await
        .expect("seed the source");
    install(
        &db,
        "TRANSFORM qty_by_order FROM Shop.OrderItems GROUP BY order_id SELECT SUM(qty) AS total",
    )
    .await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.OrderItems"]).await;
    const TOTALS: &str = "select order_id::text, total::text from qty_by_order";
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "5"), ("20", "5")]),
        "the backfill folded every pre-existing row"
    );

    pipeline
        .raw
        .batch_execute(
            "insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (4, 20, 7); \
             update \"Shop\".\"OrderItems\" set qty = 30 where id = 2; \
             delete from \"Shop\".\"OrderItems\" where id = 1; \
             insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (5, 30, 1)",
        )
        .await
        .expect("write the source");
    pipeline.settle().await;
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "30"), ("20", "12"), ("30", "1")]),
        "CDC insert, update and delete all reached the aggregate"
    );

    pipeline.finish().await;
}

/// A 1-1 transform over a mixed-case table reading a to-one relationship
/// into another mixed-case table: the forward path (a new from-side row
/// resolves its parent through the projection) and the reverse path (a
/// to-side update refreshes the projection and re-evaluates its readers)
/// both look the to-side up by its unquoted identity, quoted for the lookup.
#[tokio::test]
async fn a_relationship_between_mixed_case_tables_backfills_and_applies_cdc() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.\"Products\" (id bigint primary key, price numeric); \
         alter table public.\"Products\" replica identity full; \
         create table public.\"OrderItems\" (id bigint primary key, product_id bigint); \
         create index on public.\"OrderItems\" (product_id); \
         alter table public.\"OrderItems\" replica identity full; \
         insert into public.\"Products\" (id, price) values (1, 10), (2, 20); \
         insert into public.\"OrderItems\" (id, product_id) values (1, 1), (2, 2)",
    )
    .await
    .expect("seed the tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM OrderItems.product_id TO Products.id",
    )
    .await
    .expect("declare the relationship");
    install(
        &db,
        "TRANSFORM enriched FROM OrderItems SELECT product.price AS price",
    )
    .await;

    let mut pipeline =
        Pipeline::attach(cluster, db, raw, &["public.OrderItems", "public.Products"]).await;
    const ENRICHED: &str = "select id::text, price::text from enriched";
    // Drain whatever the install staged before reading the backfill.
    pipeline.settle().await;
    assert_eq!(
        pipeline.rows(ENRICHED).await,
        rows(&[("1", "10"), ("2", "20")]),
        "the backfill joined every pre-existing row to its product"
    );

    pipeline
        .raw
        .batch_execute("update public.\"Products\" set price = 15 where id = 1")
        .await
        .expect("update a product");
    pipeline.settle().await;
    pipeline
        .raw
        .batch_execute("insert into public.\"OrderItems\" (id, product_id) values (3, 1)")
        .await
        .expect("insert an order item");
    pipeline.settle().await;
    assert_eq!(
        pipeline.rows(ENRICHED).await,
        rows(&[("1", "15"), ("2", "20"), ("3", "15")]),
        "the to-side update and the from-side insert both reached the target"
    );

    pipeline.finish().await;
}
