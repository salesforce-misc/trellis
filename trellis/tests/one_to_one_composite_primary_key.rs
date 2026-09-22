//! Issue #121: end-to-end coverage for a 1-1 transform whose source has a
//! genuine multi-column (composite) primary key, driven through the full
//! public [`trellis::Client`] runtime — real publication/slot setup, real
//! logical-replication CDC intake, real ring maintenance, real application
//! workers — against a real, ephemeral Postgres instance
//! (`testkit::TestCluster`), mirroring `client_e2e.rs`'s own full-pipeline
//! convention rather than staging changes into the ring by hand.
//!
//! Before this issue, a composite source primary key was rejected outright
//! at definition time (`DdlError::CompositePrimaryKeyUnsupported`, raised by
//! the now-removed `ddl::require_single_column_pk`). This file proves the
//! replacement end to end: insert, update, and delete against a
//! two-column-keyed source all drain correctly into a target table whose own
//! primary key mirrors the source's in full, addressed throughout by this
//! crate's shared, arity-generic key-contract text
//! (`ddl::pk_key_sql_expr`/`ddl::join_pk_key`/`ddl::split_pk_key`) rather than
//! a single scalar column.
//!
//! `defs::oracle::recompute` (the cross-check oracle `client_e2e.rs` compares
//! against) is intentionally not used here: that oracle's own `pk_column: &str`
//! parameter is single-column-only, and widening it is out of this issue's
//! scope (the issue is about the engine's own composite-key support, not the
//! oracle's) — so this file asserts against the target table's contents
//! directly instead.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{create_definition, create_target_table, source_primary_key};
use trellis::{Client as TrellisClient, ClientOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`) and pins
/// `search_path`, matching `client_e2e.rs`'s helper of the same name.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

/// Polls `predicate` until it returns `true`, or panics with `message` once
/// `timeout` elapses — matching `client_e2e.rs`'s helper of the same name.
async fn poll_until<F>(timeout: Duration, interval: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("poll_until timed out after {timeout:?}: {message}");
        }
        tokio::time::sleep(interval).await;
    }
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// The transform this file exercises: a 1-1 passthrough-plus-arithmetic
/// field over `order_lines`, a source keyed by *two* columns
/// (`order_id`, `line_no`) — genuinely distinguishing rows that share either
/// column alone (order 1 has two lines; line_no 1 appears under both
/// orders), so a target-key bug that dropped or misread either column would
/// show up as a wrong row count or a cross-contaminated value, not just a
/// wrong single value.
fn line_totals_def() -> TransformDef {
    TransformDef {
        target: "line_totals".to_string(),
        source: "order_lines".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("qty".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

/// Creates `order_lines` (a composite `(order_id, line_no)` primary key)
/// plus `line_totals`'s definition and target table — mirroring
/// `client_e2e.rs`'s `setup_source_and_target` convention: `create_definition`
/// is the ring-path entry point and never runs target-table DDL itself, so
/// the caller (here) builds the physical target table separately.
async fn setup_source_and_target(pool: &trellis::Pool, raw: &Client) {
    raw.batch_execute(
        "create table order_lines (
             order_id integer,
             line_no integer,
             price numeric,
             qty numeric,
             primary key (order_id, line_no)
         )",
    )
    .await
    .expect("create source table with a composite primary key");

    let source_columns = numeric_columns(&["order_id", "line_no", "price", "qty"]);
    create_definition(
        pool,
        "TRANSFORM line_totals FROM order_lines SELECT price + qty AS total",
        &source_columns,
    )
    .await
    .expect("issue #121: a composite-PK source is accepted, not rejected");

    let pk = source_primary_key(pool, "order_lines")
        .await
        .expect("introspect the composite source primary key");
    assert_eq!(
        pk.len(),
        2,
        "order_lines' primary key is genuinely composite"
    );

    create_target_table(
        pool,
        &line_totals_def(),
        "public",
        &pk,
        &source_columns,
        "order_lines",
    )
    .await
    .expect("create the composite-PK target table");
}

/// `line_totals`'s current contents, keyed by `"{order_id}\u{1f}{line_no}"`
/// (this crate's shared composite key-contract text, `ddl::join_pk_key`) to
/// its `total` (as text) — order-independent, matching `client_e2e.rs`'s
/// `target_snapshot` convention.
async fn target_snapshot(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query(
            "select order_id::text || chr(31) || line_no::text, total::text from line_totals",
            &[],
        )
        .await
        .expect("read target table")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
async fn a_composite_primary_key_transform_converges_inserts_updates_and_deletes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    setup_source_and_target(&db.pool, &raw).await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 2,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.order_lines")],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    // Two lines under order 1, one under order 2 — `line_no = 1` repeats
    // across two different orders, and `order_id = 1` repeats across two
    // different lines, so neither column alone could serve as the key.
    raw.batch_execute(
        "insert into order_lines (order_id, line_no, price, qty) values \
         (1, 1, 10.00, 2), (1, 2, 5.00, 3), (2, 1, 7.00, 4)",
    )
    .await
    .expect("insert source rows");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged after the inserts",
        async || target_snapshot(&raw).await.len() == 3,
    )
    .await;

    let snapshot = target_snapshot(&raw).await;
    assert_eq!(snapshot.get("1\u{1f}1"), Some(&Some("12.00".to_string())));
    assert_eq!(snapshot.get("1\u{1f}2"), Some(&Some("8.00".to_string())));
    assert_eq!(snapshot.get("2\u{1f}1"), Some(&Some("11.00".to_string())));

    // Update one line's quantity — must recompute only that composite key's
    // row, leaving its same-order_id and same-line_no siblings untouched
    // (the cross-contamination a key bug that dropped one column would
    // produce).
    raw.execute(
        "update order_lines set qty = 10 where order_id = 1 and line_no = 1",
        &[],
    )
    .await
    .expect("update source row");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged after the update",
        async || target_snapshot(&raw).await.get("1\u{1f}1") == Some(&Some("20.00".to_string())),
    )
    .await;
    let snapshot = target_snapshot(&raw).await;
    assert_eq!(snapshot.len(), 3, "the update must not add or remove a row");
    assert_eq!(snapshot.get("1\u{1f}1"), Some(&Some("20.00".to_string())));
    assert_eq!(
        snapshot.get("1\u{1f}2"),
        Some(&Some("8.00".to_string())),
        "order 1's other line must be untouched by the update"
    );
    assert_eq!(
        snapshot.get("2\u{1f}1"),
        Some(&Some("11.00".to_string())),
        "the other order's line_no=1 row must be untouched by the update"
    );

    // Delete one composite-keyed row — must remove exactly that row, again
    // leaving its same-order_id and same-line_no siblings alone.
    raw.execute(
        "delete from order_lines where order_id = 1 and line_no = 1",
        &[],
    )
    .await
    .expect("delete source row");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged after the delete",
        async || target_snapshot(&raw).await.len() == 2,
    )
    .await;
    let remaining = target_snapshot(&raw).await;
    assert!(
        !remaining.contains_key("1\u{1f}1"),
        "the deleted row must be gone: {remaining:?}"
    );
    assert_eq!(remaining.get("1\u{1f}2"), Some(&Some("8.00".to_string())));
    assert_eq!(remaining.get("2\u{1f}1"), Some(&Some("11.00".to_string())));

    client.shutdown().await.expect("clean shutdown");
}
