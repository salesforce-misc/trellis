//! Unindexed relationship join columns (#973): `Trellis::status` reports,
//! for the definition it describes, each relationship it reads whose
//! `from_col`, or a to-many relationship's `to_col`, has no usable index.
//! The report is a warning: it never changes the definition's status.
//!
//! `self_check` carries the same list; its test is in `self_check.rs`.
//! Nothing here waits for anything (#297): a definition's status and its
//! join columns' indexes are both catalog reads, so no capture, worker or
//! drain runs.

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, Trellis, TrellisOptions, UnindexedJoin};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// `order_view` over `orders`, reading `customer` (to-one, `orders.customer_id`
/// to `customers.id`) and `items` (to-many, `orders.id` to
/// `line_items.order_id`). Neither join column of the two that can lack an
/// index has one: `orders.customer_id` and `line_items.order_id`.
async fn fixture(db: &TestDatabase) -> (Trellis, Client) {
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table customers (id int primary key, name text); \
         create table orders (id int primary key, customer_id int); \
         create table line_items (id int primary key, order_id int, qty int)",
    )
    .await
    .expect("create tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    for statement in [
        "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
        "RELATIONSHIP items FROM orders.id TO line_items.order_id",
        "TRANSFORM order_view FROM orders \
         SELECT customer.name AS customer_name, SUM(items.qty) AS total_qty",
    ] {
        trellis.apply(statement).await.expect(statement);
    }
    (trellis, raw)
}

fn join(relationship: &str, table: &str, column: &str) -> UnindexedJoin {
    UnindexedJoin {
        relationship: relationship.to_string(),
        table: table.to_string(),
        column: column.to_string(),
    }
}

async fn definition_status(trellis: &Trellis) -> trellis::DefinitionStatus {
    trellis
        .status("order_view")
        .await
        .expect("status")
        .expect("the definition exists")
}

async fn unindexed(trellis: &Trellis) -> Vec<UnindexedJoin> {
    trellis
        .status("order_view")
        .await
        .expect("status")
        .expect("the definition exists")
        .unindexed_joins
}

/// An unindexed `from_col` and an unindexed to-many `to_col` are each
/// reported, naming the relationship, table, column and fix; creating the
/// index clears its entry on the next call.
#[tokio::test]
async fn status_names_an_unindexed_join_column_until_it_is_indexed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = fixture(&db).await;

    let reported = unindexed(&trellis).await;
    assert_eq!(
        reported,
        vec![
            join("customer", "trellis.orders", "customer_id"),
            join("items", "trellis.line_items", "order_id"),
        ]
    );
    assert_eq!(
        reported[0].fix(),
        "create an index on trellis.orders (customer_id)"
    );

    raw.batch_execute("create index on orders (customer_id)")
        .await
        .expect("index the from_col");
    assert_eq!(
        unindexed(&trellis).await,
        vec![join("items", "trellis.line_items", "order_id")],
        "the from_col warning is gone, the to-many to_col's stays"
    );

    raw.batch_execute("create index on line_items (order_id)")
        .await
        .expect("index the to-many to_col");
    assert_eq!(unindexed(&trellis).await, Vec::new());

    trellis.shutdown().await.expect("shutdown");
}

/// A to-one relationship's `to_col` is the projection's primary key, so it is
/// never reported, even if the unique index that made it to-one is gone by
/// the time `status` reads.
#[tokio::test]
async fn a_to_one_relationships_to_col_is_never_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = fixture(&db).await;
    raw.batch_execute(
        "create index on orders (customer_id); create index on line_items (order_id); \
         alter table customers drop constraint customers_pkey",
    )
    .await
    .expect("index the reads, then drop the to-one's key");

    assert_eq!(unindexed(&trellis).await, Vec::new());

    trellis.shutdown().await.expect("shutdown");
}

/// An index that is not ready, or is being dropped, doesn't count: the same
/// definition of "indexed" the planner setting reads (#972). The collation
/// condition is pinned at define time
/// (`defs_relationship_catalog::an_index_that_is_not_ready_or_under_another_collation_still_warns`).
#[tokio::test]
async fn an_index_the_planner_would_not_use_does_not_silence_the_warning() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = fixture(&db).await;
    raw.batch_execute(
        "create index orders_customer_id_idx on orders (customer_id); \
         create index line_items_order_id_idx on line_items (order_id); \
         update pg_index set indisready = false \
             where indexrelid = 'orders_customer_id_idx'::regclass; \
         update pg_index set indislive = false \
             where indexrelid = 'line_items_order_id_idx'::regclass",
    )
    .await
    .expect("create indexes that are not usable");

    assert_eq!(
        unindexed(&trellis).await,
        vec![
            join("customer", "trellis.orders", "customer_id"),
            join("items", "trellis.line_items", "order_id"),
        ]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// A warning only: the definition's status is whatever it was.
#[tokio::test]
async fn the_warning_does_not_change_the_definitions_status() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = fixture(&db).await;
    let warned = definition_status(&trellis).await;
    assert!(!warned.unindexed_joins.is_empty());

    raw.batch_execute(
        "create index on orders (customer_id); create index on line_items (order_id)",
    )
    .await
    .expect("index both");
    let clean = definition_status(&trellis).await;
    assert!(clean.unindexed_joins.is_empty());
    assert_eq!(warned.status, clean.status);

    trellis.shutdown().await.expect("shutdown");
}

/// A join column that no longer exists has no index to create: `status`
/// leaves it out rather than advise indexing a column the table doesn't
/// have. What the drop does to the definition is `status`'s other fields'
/// to report.
#[tokio::test]
async fn a_dropped_join_column_is_not_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = fixture(&db).await;
    raw.batch_execute(
        "alter table orders drop column customer_id; \
         alter table line_items drop column order_id",
    )
    .await
    .expect("drop both join columns");

    assert_eq!(unindexed(&trellis).await, Vec::new());

    trellis.shutdown().await.expect("shutdown");
}
