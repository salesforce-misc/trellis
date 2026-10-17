//! Issue #1106: define checks a `JOIN` definition against both tables' live
//! columns, then refuses it as `JoinNotInstallable` until its target can be
//! built. The pure rules are covered by `defs::validate`'s `join_*` tests;
//! these pin that define reads each side's real types, modifiers,
//! collations and enums, and that nothing is left behind.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{CatalogError, ValidationError, ValueType, install_definition};

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

/// `install_definition`'s refusal for `text`, with the `FROM` table's
/// columns passed the way the app passes them (only the single-table
/// validator reads them, and a JOIN never reaches it).
async fn define_err(pool: &trellis::pool::Pool, text: &str) -> CatalogError {
    let source_columns: HashMap<String, ValueType> = HashMap::new();
    install_definition(pool, text, &source_columns, "public")
        .await
        .expect_err("a JOIN definition is never installed yet")
}

async fn assert_nothing_registered(pool: &trellis::pool::Pool, target: &str) {
    let client = pool.get().await.expect("get connection");
    let exists: bool = client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(!exists, "a refused definition must not leave its target");
    let definitions: i64 = client
        .query_one("select count(*) from transform_definitions", &[])
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(definitions, 0, "a refused definition must not be stored");
}

#[tokio::test]
async fn define_validates_a_join_against_both_tables_then_refuses_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create schema other;
         create type tier as enum ('gold', 'silver');
         create table orders (id integer primary key, region text, total numeric(10,2),
                              big_id bigint, tier tier, loose numeric);
         create table items (id integer primary key, order_id integer, sku text,
                             qty integer, price numeric, tier tier);
         create table other.lines (id integer primary key, order_id integer, qty integer);",
    )
    .await;

    // A valid JOIN passes every check, from either side, then is refused.
    for text in [
        "TRANSFORM by_region FROM orders LEFT JOIN items ON orders.id = items.order_id \
         GROUP BY orders.region, items.sku \
         SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n, \
                SUM(items.qty) AS qty, AVG(orders.total) AS avg_total",
        // An enum join key is text-stable.
        "TRANSFORM by_region FROM orders JOIN items ON orders.tier = items.tier \
         GROUP BY orders.region SELECT COUNT(*) AS n",
        // An explicitly qualified joined table.
        "TRANSFORM by_region FROM orders JOIN other.lines ON orders.id = lines.order_id \
         GROUP BY orders.region SELECT SUM(lines.qty) AS qty",
    ] {
        let err = define_err(&db.pool, text).await;
        assert!(
            matches!(
                err,
                CatalogError::Validate(ValidationError::JoinNotInstallable)
            ),
            "{text}: {err:?}"
        );
    }

    // Each check reads the live column, on either side.
    let on = |on: &str, select: &str| {
        format!(
            "TRANSFORM by_region FROM orders JOIN items ON {on} \
             GROUP BY orders.region SELECT {select}"
        )
    };
    let err = define_err(
        &db.pool,
        &on("orders.big_id = items.order_id", "COUNT(*) AS n"),
    )
    .await;
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinKeyTypeMismatch { lhs_type, rhs_type, .. })
                if lhs_type == "bigint" && rhs_type == "integer"
        ),
        "{err:?}"
    );
    let err = define_err(
        &db.pool,
        &on("orders.id = items.order_id", "SUM(items.price) AS s"),
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::UnconstrainedNumericAggregate { .. })
        ),
        "{err:?}"
    );
    let err = define_err(
        &db.pool,
        &on("orders.id = items.order_id", "MIN(items.qty) AS m"),
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::DeferredAggregate { issue: 1117, .. })
        ),
        "{err:?}"
    );
    let err = define_err(&db.pool, &on("orders.id = items.nope", "COUNT(*) AS n")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinUnresolvedColumn { .. })
        ),
        "{err:?}"
    );
    // A joined table that doesn't exist.
    let err = define_err(
        &db.pool,
        "TRANSFORM by_region FROM orders JOIN missing ON orders.id = missing.order_id \
         GROUP BY orders.region SELECT COUNT(*) AS n",
    )
    .await;
    assert!(
        matches!(err, CatalogError::SourceTableNotFound(ref table) if table == "missing"),
        "{err:?}"
    );

    assert_nothing_registered(&db.pool, "by_region").await;
}

#[tokio::test]
async fn define_refuses_a_join_key_with_a_nondeterministic_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create collation case_insensitive \
             (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
         create table customers (id integer primary key,
                                 code text collate case_insensitive, region text);
         create table visits (id integer primary key,
                              code text collate case_insensitive);",
    )
    .await;
    let err = define_err(
        &db.pool,
        "TRANSFORM visits_by_region FROM customers JOIN visits \
         ON customers.code = visits.code GROUP BY customers.region SELECT COUNT(*) AS n",
    )
    .await;
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinNondeterministicCollation { collation, .. })
                if collation == "case_insensitive"
        ),
        "{err:?}"
    );
    assert_nothing_registered(&db.pool, "visits_by_region").await;
}
