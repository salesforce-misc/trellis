//! Integration tests for aggregate (`GROUP BY`) delta maintenance (issue
//! #11's aggregate extension of stage 05's apply ∪ mark-drained), run against
//! a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).
//!
//! Mirrors `apply.rs`'s conventions throughout: source/target tables and
//! definitions built by hand, changes staged directly into the ring, drains
//! run via `apply::drain_once`. See `engine::staging::apply_aggregate`'s
//! module doc comment for the delta model these tests hold the
//! implementation to.

use std::collections::HashMap;

use engine::config::DEFAULT_SCHEMA;
use engine::defs::ast::ValueType;
use engine::defs::{create_aggregate_target_table, create_definition, parse};
use engine::staging::apply::{self, ApplyError};
use engine::staging::{claim, fold};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

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

async fn seal_active_segment(client: &mut Client) -> i64 {
    use engine::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

async fn drain(pool: &engine::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    apply::drain_once(pool, seg_seq, claimed_by, 1, "trellis_apply_aggregate_test")
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something")
}

const ORDER_SUMMARY_SOURCE: &str = "TRANSFORM order_summary FROM order_items GROUP BY order_id \
     SELECT order_id AS order_id, SUM(amount) AS total, AVG(amount) AS avg_amount, \
     MAX(amount) AS max_amount, MIN(amount) AS min_amount";

/// Fetches `order_summary`'s current rows, keyed by `order_id` text, as
/// `(total, avg_amount, max_amount, min_amount)` text tuples.
async fn read_target(
    client: &Client,
) -> HashMap<
    String,
    (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ),
> {
    client
        .query(
            "select order_id::text, total::text, avg_amount::text, max_amount::text, \
             min_amount::text from order_summary",
            &[],
        )
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|row| {
            let order_id: String = row.get(0);
            (order_id, (row.get(1), row.get(2), row.get(3), row.get(4)))
        })
        .collect()
}

/// Runs the oracle's own `SELECT ... GROUP BY` (via
/// `render_aggregate_select_sql`) against the live `order_items` table,
/// keyed by `order_id` text, in the same shape [`read_target`] returns —
/// so a direct comparison is exact-value, not encoding-dependent.
async fn read_oracle(
    client: &Client,
    def: &engine::defs::ast::TransformDef,
) -> HashMap<
    String,
    (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ),
> {
    let sql = engine::defs::render_aggregate_select_sql(def);
    let sql = format!(
        "select order_id::text, total::text, avg_amount::text, max_amount::text, \
         min_amount::text from ({sql}) o"
    );
    client
        .query(&sql, &[])
        .await
        .expect("run oracle sql")
        .into_iter()
        .map(|row| {
            let order_id: String = row.get(0);
            (order_id, (row.get(1), row.get(2), row.get(3), row.get(4)))
        })
        .collect()
}

async fn setup(db: &testkit::TestDatabase) -> engine::defs::ast::TransformDef {
    let def = parse(ORDER_SUMMARY_SOURCE).expect("parse aggregate definition");
    let source_columns = numeric_columns(&["id", "order_id", "amount"]);
    create_definition(&db.pool, ORDER_SUMMARY_SOURCE, &source_columns)
        .await
        .expect("create aggregate definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table");
    def
}

#[tokio::test]
async fn drain_matches_the_oracle_for_aggregate_insert_update_delete_and_grain_migration() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric)",
        )
        .await
        .expect("create source table");

    let def = setup(&db).await;

    // Step 1 (seed): establish groups 10 and 20's initial live state and
    // target rows via an ordinary batch of inserts.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values \
             (2, 10, 3.00), (3, 20, 4.00), (4, 20, 6.00), (5, 20, 8.00)",
            &[],
        )
        .await
        .expect("seed live order_items rows");

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"3.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "3",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"4.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "4",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"6.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "5",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"8.00"}"#),
    )
    .await;

    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1, "worker").await;
    assert_eq!(
        outcome1.keys_written, 2,
        "groups 10 and 20 are newly created"
    );

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(target, oracle, "seed batch must already match the oracle");

    // Step 2: insert (group 10), update-in-place (group 10), grain
    // migration (order_id 20 -> 30), and delete (group 20).
    client
        .batch_execute(
            "insert into order_items (id, order_id, amount) values (1, 10, 5.00); \
             update order_items set amount = 7.00 where id = 2; \
             update order_items set order_id = 30 where id = 3; \
             delete from order_items where id = 4",
        )
        .await
        .expect("apply live end-state for step 2");

    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "2",
        "update",
        Some(r#"{"order_id":"10","amount":"3.00"}"#),
        Some(r#"{"order_id":"10","amount":"7.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "3",
        "update",
        Some(r#"{"order_id":"20","amount":"4.00"}"#),
        Some(r#"{"order_id":"30","amount":"4.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "4",
        "delete",
        Some(r#"{"order_id":"20","amount":"6.00"}"#),
        None,
    )
    .await;

    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(
        outcome2.keys_written, 3,
        "groups 10, 20, and 30 are all touched"
    );

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "insert/update/grain-migration/delete batch must match the oracle"
    );

    // Group 10: members 1 (5.00) and 2 (7.00).
    let g10 = &target["10"];
    assert_eq!(g10.0.as_deref(), Some("12.00"), "group 10 total");
    assert_eq!(g10.2.as_deref(), Some("7.00"), "group 10 max");
    assert_eq!(g10.3.as_deref(), Some("5.00"), "group 10 min");

    // Group 20: only member 5 (8.00) remains.
    let g20 = &target["20"];
    assert_eq!(g20.0.as_deref(), Some("8.00"), "group 20 total");

    // Group 30: gained member 3 (4.00) via migration.
    let g30 = &target["30"];
    assert_eq!(g30.0.as_deref(), Some("4.00"), "group 30 total");

    // Step 3: delete group 10's current max (id 2, amount 7.00) — the
    // remaining member (id 1, amount 5.00) must become the new max/min via
    // the recompute-probe path, not a stale cached value.
    client
        .execute("delete from order_items where id = 2", &[])
        .await
        .expect("apply live end-state for step 3");
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "2",
        "delete",
        Some(r#"{"order_id":"10","amount":"7.00"}"#),
        None,
    )
    .await;

    let seg3 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg3, "worker").await;

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "deleting the current max must match the oracle's recomputed max/min"
    );
    let g10 = &target["10"];
    assert_eq!(
        g10.0.as_deref(),
        Some("5.00"),
        "group 10 total after deleting the max"
    );
    assert_eq!(
        g10.2.as_deref(),
        Some("5.00"),
        "group 10 max must recompute, not stay stale at 7.00"
    );
    assert_eq!(
        g10.3.as_deref(),
        Some("5.00"),
        "group 10 min must recompute too"
    );
}

#[tokio::test]
async fn independent_aggregate_batches_deltas_commute_under_out_of_order_drain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric)",
        )
        .await
        .expect("create source table");

    let def = setup(&db).await;

    // Two independent batches touching disjoint groups (10 and 20) — safe
    // to drain in either order and land on the same final state.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (1, 10, 5.00), (2, 20, 9.00)",
            &[],
        )
        .await
        .expect("seed live rows");

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"9.00"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;

    // Drain the second-sealed batch first.
    drain(&db.pool, seg2, "worker").await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "out-of-order drain of independent batches must still match the oracle"
    );
    assert_eq!(target["10"].0.as_deref(), Some("5.00"));
    assert_eq!(target["20"].0.as_deref(), Some("9.00"));
}

#[tokio::test]
async fn an_aggregate_claim_lost_mid_drain_rolls_back_and_applies_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
        )
        .await
        .expect("seed source table");

    setup(&db).await;

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    assert!(!folded.is_empty());

    client
        .execute(
            "delete from seg_claims where seg_seq = $1 and claimed_by = 'worker'",
            &[&seg_seq],
        )
        .await
        .expect("simulate a reclaim");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");
    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let err = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_aggregate_test",
    )
    .await
    .expect_err("the claim is gone; completion must fail");
    assert!(matches!(err, ApplyError::ClaimLost), "got {err:?}");
    txn.rollback().await.expect("rollback phase 3");

    let count: i64 = client
        .query_one("select count(*) from order_summary", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(
        count, 0,
        "the rolled-back aggregate apply must not have written anything"
    );

    // Now show a clean drain of the same folded change commits normally.
    let plan = apply::compute(&db.pool, &folded).await.expect("compute");
    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("re-claim");
    let outcome = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_aggregate_test",
    )
    .await
    .expect("a fresh claim's apply must commit");
    txn.commit().await.expect("commit phase 3");
    assert_eq!(outcome.keys_written, 1);

    let count: i64 = client
        .query_one("select count(*) from order_summary", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(count, 1, "the committed apply must have written group 10");
}

#[tokio::test]
async fn a_definition_change_on_an_aggregate_only_source_trips_the_version_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
        )
        .await
        .expect("seed source table");

    setup(&db).await;

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let source_oid: u32 = client
        .query_one("select 'order_items'::regclass::oid", &[])
        .await
        .expect("read source OID")
        .get(0);
    client
        .execute(
            "update seg_0 set source_relation_oid = $1::oid where src_table = 'order_items'",
            &[&source_oid],
        )
        .await
        .expect("bind staged aggregate change to its source OID");
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    client
        .execute(
            "update source_table_versions set version = version + 1 \
             where source_table = 'order_items'",
            &[],
        )
        .await
        .expect("bump order_items' version, simulating a concurrent definition change");

    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let err = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_aggregate_test",
    )
    .await
    .expect_err("order_items' version moved since compute; the fence must trip");
    match &err {
        ApplyError::VersionFenceMiss { src_table } => {
            assert_eq!(src_table.as_str(), format!("{DEFAULT_SCHEMA}.order_items"))
        }
        other => panic!("expected VersionFenceMiss, got {other:?}"),
    }
    txn.rollback().await.expect("rollback phase 3");

    let count: i64 = client
        .query_one("select count(*) from order_summary", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(count, 0, "a fence miss must not have written anything");
}

#[tokio::test]
async fn a_definition_change_on_an_unrelated_source_does_not_trip_the_aggregate_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             create table widgets (id integer primary key, cost numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
        )
        .await
        .expect("seed source tables");

    setup(&db).await;
    let widget_columns = numeric_columns(&["id", "cost"]);
    create_definition(
        &db.pool,
        "TRANSFORM widget_costs FROM widgets SELECT cost + cost AS doubled",
        &widget_columns,
    )
    .await
    .expect("create widget_costs definition");

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    client
        .execute(
            "update source_table_versions set version = version + 1 where source_table = 'widgets'",
            &[],
        )
        .await
        .expect("bump widgets' version");

    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let outcome = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_aggregate_test",
    )
    .await
    .expect("an unrelated source's version change must not trip this batch's fence");
    txn.commit().await.expect("commit phase 3");
    assert_eq!(outcome.keys_written, 1);
}

/// A bound aggregate target must continue to receive writes after its physical
/// relation is renamed and moved. The logical target name is intentionally no
/// longer resolvable when Phase 3 executes.
#[tokio::test]
async fn aggregate_writes_follow_the_bound_target_after_rename_and_schema_move() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema moved_target;
             create table order_items (id integer primary key, order_id integer, amount numeric)",
        )
        .await
        .expect("create source");

    let def = setup(&db).await;
    client
        .batch_execute(
            "alter table order_summary rename to moved_order_summary;
             alter table moved_order_summary set schema moved_target;
             insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
        )
        .await
        .expect("move target and seed source");
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 1);

    let total: String = client
        .query_one(
            "select total::text from moved_target.moved_order_summary where order_id = 10",
            &[],
        )
        .await
        .expect("read moved aggregate target")
        .get(0);
    assert_eq!(total, "5.00");
    assert_eq!(read_oracle(&client, &def).await.len(), 1);
}

/// Two batches, drained concurrently (two separate transactions), that
/// **both** touch groups 10 and 20 — with their folded changes arriving in
/// opposite group order (batch B: group 20 then group 10; batch C: group 10
/// then group 20) — must both complete rather than deadlock.
/// `apply_aggregate_target`'s ascending group-key-order sequential upserts
/// give every writer the same lock order regardless of the order its own
/// folded changes happened to arrive in, so Postgres's deadlock detector
/// never needs to intervene. Earlier this test only had the two concurrent
/// batches touch disjoint groups (one wrote group 10, the other group 20),
/// so it passed vacuously — neither batch could ever contend with the other
/// on the same row, regardless of lock order. This version gives both
/// batches real, contending writes against both groups.
#[tokio::test]
async fn two_overlapping_group_writers_serialize_via_ascending_lock_order_not_deadlock() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 1.00), (2, 20, 1.00)",
        )
        .await
        .expect("seed source table");

    setup(&db).await;

    // Batch A: touches groups 10 and 20.
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"1.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"1.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker-a").await;

    // Batch B and C: each touches BOTH groups 10 and 20, with their rows
    // inserted in opposite order (B: 20 then 10; C: 10 then 20), so a
    // per-group write mechanism that preserved arrival order (rather than
    // sorting ascending) would have the two batches acquire group locks in
    // reversed order relative to each other — the shape that would actually
    // risk a Postgres deadlock if the ascending-sort mechanism were broken.
    client
        .batch_execute(
            "insert into order_items (id, order_id, amount) values (3, 20, 3.00); \
             update order_items set amount = 2.00 where id = 1",
        )
        .await
        .unwrap();
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "3",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"3.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "update",
        Some(r#"{"order_id":"10","amount":"1.00"}"#),
        Some(r#"{"order_id":"10","amount":"2.00"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;

    client
        .batch_execute(
            "insert into order_items (id, order_id, amount) values (4, 10, 4.00); \
             update order_items set amount = 2.00 where id = 2",
        )
        .await
        .unwrap();
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "4",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"4.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "2",
        "update",
        Some(r#"{"order_id":"20","amount":"1.00"}"#),
        Some(r#"{"order_id":"20","amount":"2.00"}"#),
    )
    .await;
    let seg3 = seal_active_segment(&mut client).await;

    let pool_b = db.pool.clone();
    let handle_b = tokio::spawn(async move { drain(&pool_b, seg2, "worker-b").await });
    let pool_c = db.pool.clone();
    let handle_c = tokio::spawn(async move { drain(&pool_c, seg3, "worker-c").await });

    let outcome_b = handle_b.await.expect("worker-b task must not panic");
    let outcome_c = handle_c.await.expect("worker-c task must not panic");
    assert_eq!(outcome_b.keys_written, 2);
    assert_eq!(outcome_c.keys_written, 2);

    let total10: String = client
        .query_one(
            "select total::text from order_summary where order_id = 10",
            &[],
        )
        .await
        .expect("read group 10")
        .get(0);
    let total20: String = client
        .query_one(
            "select total::text from order_summary where order_id = 20",
            &[],
        )
        .await
        .expect("read group 20")
        .get(0);
    // Group 10: seed member 1 (2.00 after B's update) + member 4 (4.00,
    // inserted by C) = 6.00. Group 20: seed member 2 (2.00 after C's update)
    // + member 3 (3.00, inserted by B) = 5.00.
    assert_eq!(total10, "6.00");
    assert_eq!(total20, "5.00");
}

/// Issue #11 review, finding #1 (HIGH): a group forced onto the full-recompute
/// path by an image-less change (a bare recompute trigger — see the module
/// doc comment's "A known gap: image-less changes" section) must still probe
/// its `SUM` fields, not skip them for lack of a `field_accum` entry. Before
/// this fix, `AggFieldKind::Sum`'s Pass-1 check skipped whenever
/// `field_accum` had no entry for the field — checked *before* consulting
/// `force_full_recompute` — so an image-less change into an otherwise-quiet
/// group left its `SUM` column stale instead of re-probing it.
#[tokio::test]
async fn image_less_recompute_trigger_still_probes_a_stale_sum_field() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
        )
        .await
        .expect("seed source table");

    let def = setup(&db).await;

    // Seed group 10's target row via an ordinary insert batch: total = 5.00.
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let target = read_target(&client).await;
    assert_eq!(target["10"].0.as_deref(), Some("5.00"), "seed total");

    // A second row lands in group 10 entirely out-of-band (no CDC image for
    // it at all — simulating a producer that only ever stages a bare
    // recompute trigger for this key, never an insert/update image).
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (2, 10, 3.00)",
            &[],
        )
        .await
        .expect("out-of-band insert into group 10");

    // The image-less recompute trigger: both images NULL, keyed by the
    // pre-existing row (id 1) so `accumulate_changes`'s live re-read lands on
    // group 10.
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "recompute",
        None,
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "an image-less recompute trigger must match a fresh oracle recompute"
    );
    assert_eq!(
        target["10"].0.as_deref(),
        Some("8.00"),
        "the SUM field must be probed (5.00 + 3.00), not left stale at 5.00, \
         even though this batch's field_accum has no delta for it"
    );
}

/// Issue #11 review, finding #2 (MEDIUM): when a group keeps rows after a
/// delete, but every remaining row's aggregated value is NULL, the SUM
/// column must itself go `NULL` — matching Postgres's own "sum of zero
/// non-null values is NULL, never 0" rule — not `0`. Before this fix, SUM
/// tracked no running non-null count (unlike AVG), so its delta arithmetic
/// (`coalesce(old, 0) + delta`) always produced a real number, even once the
/// group's last non-null contributor was gone.
#[tokio::test]
async fn sum_goes_null_not_zero_when_a_groups_remaining_rows_are_all_null() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 5.00), (2, 10, NULL)",
        )
        .await
        .expect("seed source table");

    let def = setup(&db).await;

    // r1 (amount = 5.00) and r2 (amount = NULL) both land in group 10 via an
    // ordinary insert batch: total = 5.00 (SUM skips the NULL row).
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":null}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let target = read_target(&client).await;
    assert_eq!(target["10"].0.as_deref(), Some("5.00"), "seed total");

    // Delete r1: the group keeps r2 (still NULL), so the group itself is not
    // extinct (`probe_group_exists` is true, no target-row delete happens),
    // but its only non-null contributor is now gone.
    client
        .execute("delete from order_items where id = 1", &[])
        .await
        .expect("apply live end-state");
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "delete",
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "must match the oracle's own NULL-sum semantics"
    );
    assert_eq!(
        target["10"].0, None,
        "SUM must go NULL once the group's last non-null contributor is gone, \
         not the arithmetic-but-wrong coalesce(5.00, 0) + (0 - 5.00) = 0"
    );
}

// ---------------------------------------------------------------------
// COUNT(*) (issue #75)
// ---------------------------------------------------------------------

const ORDER_COUNTS_SOURCE: &str = "TRANSFORM order_counts FROM order_items GROUP BY order_id \
     SELECT order_id AS order_id, COUNT(*) AS item_count, AVG(amount) AS avg_amount";

/// Fetches `order_counts`'s current rows, keyed by `order_id` text, as
/// `(item_count, avg_amount)` text tuples.
async fn read_count_target(client: &Client) -> HashMap<String, (Option<String>, Option<String>)> {
    client
        .query(
            "select order_id::text, item_count::text, avg_amount::text from order_counts",
            &[],
        )
        .await
        .expect("read order_counts")
        .into_iter()
        .map(|row| {
            let order_id: String = row.get(0);
            (order_id, (row.get(1), row.get(2)))
        })
        .collect()
}

/// Runs the oracle's own `SELECT ... GROUP BY` against the live `order_items`
/// table, in the same shape [`read_count_target`] returns.
async fn read_count_oracle(
    client: &Client,
    def: &engine::defs::ast::TransformDef,
) -> HashMap<String, (Option<String>, Option<String>)> {
    let sql = engine::defs::render_aggregate_select_sql(def);
    let sql = format!("select order_id::text, item_count::text, avg_amount::text from ({sql}) o");
    client
        .query(&sql, &[])
        .await
        .expect("run oracle sql")
        .into_iter()
        .map(|row| {
            let order_id: String = row.get(0);
            (order_id, (row.get(1), row.get(2)))
        })
        .collect()
}

/// Asserts `(item_count, avg_amount)` matches expected values, comparing
/// `avg_amount` as a parsed number rather than an exact string — Postgres's
/// numeric division picks its own display scale (e.g. `5.0000000000000000`),
/// which this grammar's `AVG` inherits rather than reformats.
fn assert_count_and_avg(actual: &(Option<String>, Option<String>), count: &str, avg: f64) {
    assert_eq!(actual.0.as_deref(), Some(count), "item_count");
    let actual_avg: f64 = actual
        .1
        .as_deref()
        .expect("avg_amount must not be NULL")
        .parse()
        .expect("avg_amount must parse as a number");
    assert!(
        (actual_avg - avg).abs() < 1e-9,
        "avg_amount: expected {avg}, got {actual_avg}"
    );
}

async fn setup_counts(db: &testkit::TestDatabase) -> engine::defs::ast::TransformDef {
    let def = parse(ORDER_COUNTS_SOURCE).expect("parse COUNT(*) aggregate definition");
    let source_columns = numeric_columns(&["id", "order_id", "amount"]);
    create_definition(&db.pool, ORDER_COUNTS_SOURCE, &source_columns)
        .await
        .expect("create COUNT(*) aggregate definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create COUNT(*) aggregate target table");
    def
}

/// `COUNT(*)` composed with `AVG` on the same target (the issue's explicit
/// composability requirement): row-count and insert/update/delete/grain-
/// migration deltas must both stay correct, and match Postgres's own
/// `GROUP BY` oracle, even though `COUNT` tracks a bare running count with no
/// hidden partials while `AVG` tracks its own independent sum/count pair.
#[tokio::test]
async fn count_star_composes_with_avg_across_insert_update_delete_and_grain_migration() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric)",
        )
        .await
        .expect("create source table");

    let def = setup_counts(&db).await;

    // Seed: group 10 gets one row, group 20 gets two.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values \
             (1, 10, 5.00), (2, 20, 4.00), (3, 20, 6.00)",
            &[],
        )
        .await
        .expect("seed live order_items rows");

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"4.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "3",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"6.00"}"#),
    )
    .await;

    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let target = read_count_target(&client).await;
    let oracle = read_count_oracle(&client, &def).await;
    assert_eq!(target, oracle, "seed batch must already match the oracle");
    assert_count_and_avg(&target["10"], "1", 5.00);
    assert_count_and_avg(&target["20"], "2", 5.00);

    // Insert into group 10, update-in-place within group 20, and
    // grain-migrate group 20's other row into group 30.
    client
        .batch_execute(
            "insert into order_items (id, order_id, amount) values (4, 10, 7.00); \
             update order_items set amount = 8.00 where id = 2; \
             update order_items set order_id = 30 where id = 3",
        )
        .await
        .expect("apply live end-state for step 2");

    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "4",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"7.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "2",
        "update",
        Some(r#"{"order_id":"20","amount":"4.00"}"#),
        Some(r#"{"order_id":"20","amount":"8.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "3",
        "update",
        Some(r#"{"order_id":"20","amount":"6.00"}"#),
        Some(r#"{"order_id":"30","amount":"6.00"}"#),
    )
    .await;

    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_count_target(&client).await;
    let oracle = read_count_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "insert/update/grain-migration batch must match the oracle"
    );
    // Group 10: members 1 (5.00) and 4 (7.00) -> count 2, avg 6.00.
    assert_count_and_avg(&target["10"], "2", 6.00);
    // Group 20: only member 2 (now 8.00) remains -> count 1, avg 8.00.
    assert_count_and_avg(&target["20"], "1", 8.00);
    // Group 30: gained member 3 (6.00) via migration -> count 1, avg 6.00.
    assert_count_and_avg(&target["30"], "1", 6.00);

    // Delete group 10's last two rows: the group goes extinct and its target
    // row must be deleted outright, not left behind with a stale count.
    client
        .batch_execute("delete from order_items where order_id = 10")
        .await
        .expect("apply live end-state for step 3");
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "1",
        "delete",
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
        None,
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "4",
        "delete",
        Some(r#"{"order_id":"10","amount":"7.00"}"#),
        None,
    )
    .await;

    let seg2 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg2, "worker").await;

    let target = read_count_target(&client).await;
    let oracle = read_count_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "extinguishing group 10 must match the oracle (no row at all)"
    );
    assert!(
        !target.contains_key("10"),
        "an extinct group's target row must be deleted, not left with a stale count"
    );
}

/// An image-less recompute trigger (a bare key with no CDC image at all)
/// forces `COUNT`'s full-recompute path (`probe_count_star`), the same
/// full-recompute gap `SUM`/`AVG` already have coverage for — see
/// `image_less_recompute_trigger_still_probes_a_stale_sum_field`.
#[tokio::test]
async fn count_star_image_less_recompute_trigger_probes_a_stale_count() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
        )
        .await
        .expect("seed source table");

    let def = setup_counts(&db).await;

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let target = read_count_target(&client).await;
    assert_eq!(target["10"].0.as_deref(), Some("1"), "seed count");

    // A second row lands in group 10 entirely out-of-band.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (2, 10, 3.00)",
            &[],
        )
        .await
        .expect("out-of-band insert into group 10");

    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "recompute",
        None,
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_count_target(&client).await;
    let oracle = read_count_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "an image-less recompute trigger must match a fresh oracle recompute"
    );
    assert_eq!(
        target["10"].0.as_deref(),
        Some("2"),
        "COUNT must be probed (1 + the out-of-band row), not left stale at 1"
    );
}
