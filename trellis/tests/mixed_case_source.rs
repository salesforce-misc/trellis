//! Issue #561: a transform over a mixed-case table in a mixed-case schema
//! (`"Shop"."OrderItems"`) installs, backfills and applies CDC.
//!
//! The grammar keeps an identifier's case, so `FROM Shop.OrderItems` names
//! `"Shop"."OrderItems"`. Every catalog lookup of that table used to hand
//! `to_regclass` the unquoted `Shop.OrderItems`, which Postgres folds to
//! the nonexistent `shop.orderitems`: install failed with `NoPrimaryKey`.
//!
//! Capture is the real triggers, and the ring is drained by hand
//! (`support/trigger_pipeline.rs`), so nothing here polls for convergence.

use std::collections::HashMap;

use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;

#[path = "support/trigger_pipeline.rs"]
mod trigger_pipeline;

use trigger_pipeline::Pipeline;

const SOURCE_DDL: &str = "create schema \"Shop\"; \
     create table \"Shop\".\"OrderItems\" (id bigint primary key, order_id bigint, qty integer); \
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
    install_over(db, text, &["id", "order_id", "qty"]).await;
}

async fn install_over(db: &testkit::TestDatabase, text: &str, source_columns: &[&str]) {
    install_definition(&db.pool, text, &numeric_columns(source_columns), "public")
        .await
        .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    trellis::intake::harness::settle_builds(&db.pool).await;
}

/// A 1-1 transform from `"Shop"."OrderItems"` into a mixed-case target in
/// the same mixed-case schema, `"Shop"."ItemCopy"`.
#[tokio::test]
async fn a_one_to_one_transform_over_a_mixed_case_table_backfills_and_applies_cdc() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
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
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[("2", "30"), ("3", "5"), ("4", "7")]),
        "CDC insert, update and delete all reached the target"
    );
}

/// An aggregate over `"Shop"."OrderItems"`, grouped by a column of it.
#[tokio::test]
async fn an_aggregate_over_a_mixed_case_table_backfills_and_applies_cdc() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(SOURCE_DDL)
        .await
        .expect("seed the source");
    install(
        &db,
        "TRANSFORM qty_by_order FROM Shop.OrderItems GROUP BY order_id WITHOUT SOURCE INDEX SELECT SUM(qty) AS total",
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
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "30"), ("20", "12"), ("30", "1")]),
        "CDC insert, update and delete all reached the aggregate"
    );
}

/// #623 D4: a composed field and an expression argument over mixed-case
/// columns of a mixed-case table follow CDC, with the group and argument
/// columns carrying those names quoted.
#[tokio::test]
async fn recomputed_fields_over_mixed_case_columns_follow_cdc() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create schema \"Shop\"; \
         create table \"Shop\".\"OrderItems\" (id bigint primary key, \"OrderId\" bigint, \"Qty\" integer); \
         insert into \"Shop\".\"OrderItems\" (id, \"OrderId\", \"Qty\") values (1, 10, 2), (2, 10, 3), (3, 20, 5)",
    )
    .await
    .expect("seed the source");
    // The columns' real types: `SUM(Qty + 1)` over a `numeric` would be an
    // expression with no declared scale, which the group basis doesn't take.
    let int = |w| ValueType::Integer(w);
    let columns: HashMap<String, ValueType> = [
        ("id", int(trellis::IntWidth::Int8)),
        ("OrderId", int(trellis::IntWidth::Int8)),
        ("Qty", int(trellis::IntWidth::Int4)),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect();
    install_definition(
        &db.pool,
        "TRANSFORM qty_span FROM Shop.OrderItems GROUP BY OrderId \
        WITHOUT SOURCE INDEX SELECT COUNT(Qty) AS lo, SUM(Qty + 1) AS hi1, SUM(Qty) + COUNT(Qty) AS ends",
        &columns,
        "public",
    )
    .await
    .expect("install qty_span");
    trellis::intake::harness::settle_builds(&db.pool).await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.OrderItems"]).await;
    const SPANS: &str = "select \"OrderId\"::text, concat_ws(',', lo, hi1, ends) from qty_span";
    assert_eq!(
        pipeline.rows(SPANS).await,
        rows(&[("10", "2,7,7"), ("20", "1,6,6")]),
        "the backfill folded every pre-existing row"
    );

    pipeline
        .raw
        .batch_execute(
            "insert into \"Shop\".\"OrderItems\" (id, \"OrderId\", \"Qty\") values (4, 20, 7); \
             update \"Shop\".\"OrderItems\" set \"Qty\" = 30 where id = 2; \
             delete from \"Shop\".\"OrderItems\" where id = 1; \
             delete from \"Shop\".\"OrderItems\" where id = 3",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(SPANS).await,
        rows(&[("10", "1,31,31"), ("20", "1,8,8")]),
        "CDC retracted the old rows and recomputed each group"
    );
}
