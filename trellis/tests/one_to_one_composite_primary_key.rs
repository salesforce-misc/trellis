//! Issue #121: coverage for a 1-1 transform whose source has a genuine
//! multi-column (composite) primary key: insert, update, and delete against
//! a two-column-keyed source all drain correctly into a target table whose
//! own primary key mirrors the source's in full, addressed throughout by this
//! crate's shared, arity-generic key-contract text
//! (`ddl::pk_key_sql_expr`/`ddl::join_pk_key`/`ddl::split_pk_key`) rather than
//! a single scalar column.
//!
//! Before this issue, a composite source primary key was rejected outright
//! at definition time (`DdlError::CompositePrimaryKeyUnsupported`, raised by
//! the now-removed `ddl::require_single_column_pk`).
//!
//! No live `Client` (issue #301). This used to drive the full runtime and
//! poll the target under a 20s budget per step. Now each source change is
//! mirrored by the CDC row intake would stage for it (the composite key
//! joined with U+001F in declared order, text-valued JSON images), staged by
//! hand and drained through the engine's own apply path, so every target
//! value is still one the engine computed and wrote. Intake's own half, that
//! a composite key really is staged in that form, is covered where it can be
//! checked directly: `intake_core.rs`'s
//! `a_composite_key_declared_out_of_physical_column_order_stages_in_declared_order`
//! (real logical replication) and `intake::tests`' `extract_key_*` unit tests.
//!
//! `defs::oracle::recompute` (the cross-check oracle `client_e2e.rs` compares
//! against) is intentionally not used here: that oracle's own `pk_column: &str`
//! parameter is single-column-only, and widening it is out of this issue's
//! scope (the issue is about the engine's own composite-key support, not the
//! oracle's) — so this file asserts against the target table's contents
//! directly instead.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{create_definition, create_target_table, source_primary_key};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};

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

/// Stages one CDC change for `order_lines` into the active ring segment,
/// shaped the way intake stages it: `key` is the composite key joined with
/// U+001F in declared order, and the images are text-valued JSON.
async fn stage_cdc(
    client: &Client,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let active: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    client
        .execute(
            &format!(
                "insert into seg_{active} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[
                &format!("{DEFAULT_SCHEMA}.order_lines"),
                &key,
                &op,
                &testkit::wal_insert_lsn(client).await,
                &old_image,
                &new_image,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?}: {e}"));
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending: the hand-driven stand-in for a running `Client`'s drain workers.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // No live `Intake` stages anything here, so there is no real staged
    // watermark to hold apply back. A saturated one never does.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "composite_pk_test",
            1,
            "trellis_composite_pk_test",
            &watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
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
    let mut raw = connect_raw(db.dsn()).await;

    setup_source_and_target(&db.pool, &raw).await;

    // Two lines under each order — `line_no = 1` repeats across two
    // different orders, and each `order_id` repeats across two different
    // lines, so neither column alone could serve as the key.
    //
    // `(2, 3)` is there so the key set is not closed under swapping the two
    // columns (`(3, 2)` does not exist). Without it, a decode that read the
    // columns in the wrong order would go unnoticed: apply's issue #344 basis
    // check looks the swapped key up in the source, finds that *other* row,
    // and re-evaluates against it, so every row still lands under the right
    // key with the right value. With it, the swapped key finds no source row
    // at all and `(2, 3)` is never written.
    raw.batch_execute(
        "insert into order_lines (order_id, line_no, price, qty) values \
         (1, 1, 10.00, 2), (1, 2, 5.00, 3), (2, 1, 7.00, 4), (2, 3, 6.00, 1)",
    )
    .await
    .expect("insert source rows");
    for (key, image) in [
        (
            "1\u{1f}1",
            r#"{"order_id":"1","line_no":"1","price":"10.00","qty":"2"}"#,
        ),
        (
            "1\u{1f}2",
            r#"{"order_id":"1","line_no":"2","price":"5.00","qty":"3"}"#,
        ),
        (
            "2\u{1f}1",
            r#"{"order_id":"2","line_no":"1","price":"7.00","qty":"4"}"#,
        ),
        (
            "2\u{1f}3",
            r#"{"order_id":"2","line_no":"3","price":"6.00","qty":"1"}"#,
        ),
    ] {
        stage_cdc(&raw, key, "insert", None, Some(image)).await;
    }
    drain_to_quiescence(&db.pool, &mut raw).await;

    let snapshot = target_snapshot(&raw).await;
    assert_eq!(snapshot.len(), 4, "one target row per composite key");
    assert_eq!(snapshot.get("1\u{1f}1"), Some(&Some("12.00".to_string())));
    assert_eq!(snapshot.get("1\u{1f}2"), Some(&Some("8.00".to_string())));
    assert_eq!(snapshot.get("2\u{1f}1"), Some(&Some("11.00".to_string())));
    assert_eq!(snapshot.get("2\u{1f}3"), Some(&Some("7.00".to_string())));

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
    stage_cdc(
        &raw,
        "1\u{1f}1",
        "update",
        None,
        Some(r#"{"order_id":"1","line_no":"1","price":"10.00","qty":"10"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    let snapshot = target_snapshot(&raw).await;
    assert_eq!(snapshot.len(), 4, "the update must not add or remove a row");
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
    // leaving its same-order_id and same-line_no siblings alone. Under the
    // default replica identity a delete's old image carries only the key.
    raw.execute(
        "delete from order_lines where order_id = 1 and line_no = 1",
        &[],
    )
    .await
    .expect("delete source row");
    stage_cdc(
        &raw,
        "1\u{1f}1",
        "delete",
        Some(r#"{"order_id":"1","line_no":"1"}"#),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    let remaining = target_snapshot(&raw).await;
    assert_eq!(remaining.len(), 3, "the delete must remove exactly one row");
    assert!(
        !remaining.contains_key("1\u{1f}1"),
        "the deleted row must be gone: {remaining:?}"
    );
    assert_eq!(remaining.get("1\u{1f}2"), Some(&Some("8.00".to_string())));
    assert_eq!(remaining.get("2\u{1f}1"), Some(&Some("11.00".to_string())));
    assert_eq!(remaining.get("2\u{1f}3"), Some(&Some("7.00".to_string())));
}
