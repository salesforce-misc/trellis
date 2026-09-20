//! Integration tests for aggregate (`GROUP BY`) delta maintenance (issue
//! #11's aggregate extension of stage 05's apply ∪ mark-drained), run against
//! a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).
//!
//! Mirrors `apply.rs`'s conventions throughout: source/target tables and
//! definitions built by hand, changes staged directly into the ring, drains
//! run via `apply::drain_once`. See `trellis::staging::apply_aggregate`'s
//! module doc comment for the delta model these tests hold the
//! implementation to.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{
    create_aggregate_target_table, create_definition, create_definition_without_backfill,
    create_target_table, parse, require_single_column_pk, source_primary_key,
};
use trellis::staging::apply::{self, ApplyError};
use trellis::staging::{StagedWatermark, claim, converge, fold};

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
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every
/// bare `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `{DEFAULT_SCHEMA}, public` and never
/// qualifies its own DDL. Used by [`insert_cdc_row`] so a hand-staged ring
/// row's `src_table` matches what a real CDC producer would actually stage
/// (issue #76: always fully-qualified) and, as of issue #74, what
/// `schema_nodes`/`schema_edges` now key on. Already-qualified input
/// (containing a `.`) passes through unchanged. Mirrors `apply.rs`'s own
/// `qualify_fixture_table` helper (test files can't share private helpers).
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
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
    let src_table = qualify_fixture_table(src_table);
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

async fn drain(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    // Issue #132: a throwaway, always-caught-up watermark — no live
    // `Intake` runs in this test file, and it isn't exercising guard (a).
    apply::drain_once(
        pool,
        seg_seq,
        claimed_by,
        1,
        "trellis_apply_aggregate_test",
        &StagedWatermark::saturated(),
    )
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
    def: &trellis::defs::ast::TransformDef,
) -> HashMap<
    String,
    (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ),
> {
    let sql = trellis::defs::render_aggregate_select_sql(def);
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

async fn setup(db: &testkit::TestDatabase) -> trellis::defs::ast::TransformDef {
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
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

/// Issue #128: a source row whose grouping column is `NULL` is legal SQL
/// (`GROUP BY` folds every `NULL` in a column into one group, like any other
/// value) and must flow through the live delta path exactly like any other
/// group — written to the target, never quarantined, never left behind as a
/// stale row once its group empties out. Before the fix,
/// `create_aggregate_target_table` keyed the grouping column with a bare
/// `PRIMARY KEY`, so the live delta's upsert into a NULL group raised a real
/// `null value in column ... violates not-null constraint` error;
/// `quarantine::classify` isolated the offending row, and — because replaying
/// it reproduced the identical violation — it could never be recovered, and
/// `converge::converged_through` would treat the parked `poison_held` row as
/// permanently unconverged. This test drives the exact same live-delta path
/// (`insert_cdc_row` + `drain`, not the unit-level `apply_forced_groups_bulk`
/// harness `apply_aggregate.rs`'s own dedicated NULL-key unit test uses) and
/// asserts neither `poison_held` nor `key_deaths` ever gets a row.
#[tokio::test]
async fn a_null_grouping_key_flows_through_the_live_delta_path_without_quarantine() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             alter table order_items replica identity full",
        )
        .await
        .expect("create source table");
    // `converged_through`'s condition 1 fails closed on a missing
    // `replication_progress` row (see that function's own doc comment) —
    // unrelated to this test's actual NULL-key scenario, but needed so the
    // final convergence check below reflects condition 4 (the poison band)
    // rather than this unrelated always-false floor.
    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ('slot1', $1)",
            &[&PgLsn::from(1000u64)],
        )
        .await
        .expect("seed replication_progress");

    // `setup` installs the definition and creates the target table; its
    // return value (needed by `read_oracle`'s NULL-unsafe `order_id::text`
    // key) isn't usable here since this test's whole point is a NULL
    // `order_id` — see the direct `order_id is null` queries below instead.
    setup(&db).await;

    // Step 1: two brand-new rows land in the NULL group via an ordinary
    // insert batch — the same shape a real un-attributed order (no
    // `order_id` yet) would take.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (1, null, 5.00), (2, null, 7.00)",
            &[],
        )
        .await
        .expect("seed live NULL-keyed order_items rows");
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":null,"amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":null,"amount":"7.00"}"#),
    )
    .await;

    let seg0 = seal_active_segment(&mut client).await;
    let outcome0 = drain(&db.pool, seg0, "worker").await;
    assert_eq!(outcome0.keys_written, 1, "one NULL group is created");

    let null_group_row = client
        .query_one(
            "select total::text, min_amount::text from order_summary where order_id is null",
            &[],
        )
        .await
        .expect("the NULL group must have a target row");
    let null_group: (String, String) = (null_group_row.get(0), null_group_row.get(1));
    assert_eq!(
        null_group,
        ("12.00".to_string(), "5.00".to_string()),
        "the NULL group's SUM/MIN must reflect both contributing rows"
    );

    let poisoned: i64 = client
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("count poison_held rows")
        .get(0);
    assert_eq!(
        poisoned, 0,
        "a NULL grouping key must never be isolated into quarantine"
    );
    let deaths: i64 = client
        .query_one("select count(*) from key_deaths", &[])
        .await
        .expect("count key_deaths rows")
        .get(0);
    assert_eq!(
        deaths, 0,
        "a NULL grouping key must never charge a quarantine death"
    );

    // Step 2: one NULL-group row migrates *out* (order_id assigned, id 1 ->
    // group 40) and the other is deleted, so the NULL group must go fully
    // extinct — its target row removed, not left behind stale.
    client
        .batch_execute(
            "update order_items set order_id = 40 where id = 1; \
             delete from order_items where id = 2",
        )
        .await
        .expect("apply live end-state for step 2");
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "update",
        Some(r#"{"order_id":null,"amount":"5.00"}"#),
        Some(r#"{"order_id":"40","amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "2",
        "delete",
        Some(r#"{"order_id":null,"amount":"7.00"}"#),
        None,
    )
    .await;

    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let null_rows: i64 = client
        .query_one(
            "select count(*) from order_summary where order_id is null",
            &[],
        )
        .await
        .expect("count NULL-keyed target rows")
        .get(0);
    assert_eq!(
        null_rows, 0,
        "the NULL group's target row must be removed once it empties out"
    );
    let g40_total: String = client
        .query_one(
            "select total::text from order_summary where order_id = 40",
            &[],
        )
        .await
        .expect("group 40 must have received the migrated row")
        .get(0);
    assert_eq!(g40_total, "5.00", "group 40 total after the migration");

    let poisoned: i64 = client
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("count poison_held rows")
        .get(0);
    assert_eq!(
        poisoned, 0,
        "still no quarantine after the migration/delete"
    );

    // Issue #128's liveness half: `converge::converged_through`'s condition 4
    // deliberately blocks on any live `poison_held` row (by design, for a
    // *genuine* poison case — see that function's own doc comment). With no
    // NULL-key row ever quarantined in the first place, a token at every
    // staged change's LSN (this harness's `insert_cdc_row` always stages at
    // `PgLsn::from(1)`) must converge — the same scenario that used to hang
    // the generative suite's `quiesce` past its 30s timeout.
    let converged = converge::converged_through(&client, PgLsn::from(1u64))
        .await
        .expect("converged_through");
    assert!(
        converged,
        "a NULL grouping key must never stall convergence"
    );
}

#[tokio::test]
async fn independent_aggregate_batches_deltas_commute_under_out_of_order_drain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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
        &StagedWatermark::saturated(),
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
        &StagedWatermark::saturated(),
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    client
        .execute(
            // Issue #72: `source_table` is persisted fully-qualified now.
            &format!(
                "update source_table_versions set version = version + 1 \
                 where source_table = '{DEFAULT_SCHEMA}.order_items'"
            ),
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
        &StagedWatermark::saturated(),
    )
    .await
    .expect_err("order_items' version moved since compute; the fence must trip");
    match &err {
        ApplyError::VersionFenceMiss { src_table } => assert_eq!(src_table, "order_items"),
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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
            // Issue #72: `source_table` is persisted fully-qualified now.
            &format!(
                "update source_table_versions set version = version + 1 \
                 where source_table = '{DEFAULT_SCHEMA}.widgets'"
            ),
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
        &StagedWatermark::saturated(),
    )
    .await
    .expect("an unrelated source's version change must not trip this batch's fence");
    txn.commit().await.expect("commit phase 3");
    assert_eq!(outcome.keys_written, 1);
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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
/// doc comment's "Image-less changes, and issue #180's fix for one producer
/// of them" section) must still probe
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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

/// Issue #77 / ADR-0007's same-named-decoy regression for the *aggregate*
/// CDC-apply path — a distinct code path from the plain 1-1 case
/// `apply.rs`'s `explicitly_qualified_source_reads_the_right_table_on_a_live_refetch`
/// covers. `compute`'s own qualified-source threading (that 1-1 test, and
/// this file's `image_less_recompute_trigger_still_probes_a_stale_sum_field`
/// above, both already exercise it) only gets an aggregate definition as far
/// as forcing a group onto the full-recompute path; the *live re-read* for
/// that forced group runs through `apply_aggregate`'s own probes
/// (`probe_sum_and_count`/`probe_recompute_fields_bulk` et al., see
/// `AggregateTargetPlan::source`'s own doc comment) via
/// `ddl::qualified_source_table` — separate code from `compute`'s
/// `read_live_rows_batch`, so it needs its own coverage. Mirrors
/// `image_less_recompute_trigger_still_probes_a_stale_sum_field`'s
/// out-of-band-insert-then-image-less-recompute shape exactly, except
/// `order_items` now has a same-named decoy sitting in `public` (this pool's
/// own pinned `search_path`) while the definition explicitly names
/// `custom.order_items` as its real source — the two hold different amounts,
/// so a wrong-table probe is unmistakable in the resulting SUM.
#[tokio::test]
async fn explicitly_qualified_aggregate_source_probes_the_right_table_not_a_same_named_decoy() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.order_items (id integer primary key, order_id integer, amount numeric); \
             insert into public.order_items (id, order_id, amount) values (1, 10, 999.00), (2, 10, 888.00); \
             create schema custom; \
             create table custom.order_items (id integer primary key, order_id integer, amount numeric); \
             alter table custom.order_items replica identity full; \
             insert into custom.order_items (id, order_id, amount) values (1, 10, 5.00);",
        )
        .await
        .expect("seed the public.order_items decoy and the real custom.order_items");

    const SOURCE: &str = "TRANSFORM order_summary FROM custom.order_items GROUP BY order_id \
         SELECT order_id AS order_id, SUM(amount) AS total";
    let def = parse(SOURCE).expect("parse the explicitly-qualified aggregate definition");
    let source_columns = numeric_columns(&["id", "order_id", "amount"]);
    create_definition(&db.pool, SOURCE, &source_columns)
        .await
        .expect("create aggregate definition against the explicitly-qualified source");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table");

    // Seed group 10's target row via an ordinary insert batch: total = 5.00.
    insert_cdc_row(
        &client,
        "seg_0",
        "custom.order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let total: Option<String> = client
        .query_one(
            "select total::text from order_summary where order_id = 10",
            &[],
        )
        .await
        .expect("read seeded group")
        .get(0);
    assert_eq!(total.as_deref(), Some("5.00"), "seed total");

    // A second row lands in custom.order_items's group 10 entirely
    // out-of-band (no CDC image staged for it), forcing the image-less
    // recompute trigger below onto the full-recompute (probe) path.
    client
        .execute(
            "insert into custom.order_items (id, order_id, amount) values (2, 10, 3.00)",
            &[],
        )
        .await
        .expect("out-of-band insert into custom.order_items's group 10");

    insert_cdc_row(
        &client,
        "seg_1",
        "custom.order_items",
        "1",
        "recompute",
        None,
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let total: Option<String> = client
        .query_one(
            "select total::text from order_summary where order_id = 10",
            &[],
        )
        .await
        .expect("read probed group")
        .get(0);
    assert_eq!(
        total.as_deref(),
        Some("8.00"),
        "the probe must read custom.order_items (5.00 + 3.00 = 8.00), not the \
         same-named public.order_items decoy sitting on this pool's own \
         pinned search_path"
    );
}

/// The target-side counterpart to
/// [`explicitly_qualified_aggregate_source_probes_the_right_table_not_a_same_named_decoy`]
/// above, and `apply.rs`'s own
/// `explicitly_qualified_target_receives_a_live_cdc_write` — the aggregate
/// half of the same bug (reviewer follow-up to issue #74, epic #78's own
/// whole-branch review): an aggregate definition installed with an explicit
/// non-default *target* schema (issue #76's `TRANSFORM custom.<target> FROM
/// ...` grammar) backfills fine, but a live CDC write used to fail outright
/// — `upsert_group`/`delete_group_row` (`staging::apply_aggregate`'s own
/// per-group Phase 3 DML-emission functions) bound their `target: &str`
/// parameter straight into `quote_ident` instead of
/// [`AggregateTargetPlan::target`]'s qualified identity, so their SQL tried
/// to write bare, unqualified `order_summary`, not on this connection's
/// pinned `search_path`, even though `custom.order_summary` (the real,
/// already-backfilled target) exists.
///
/// Drains a single-group insert (`upsert_group`'s lone-delta-group path)
/// then that same group's extinction (`delete_group_row`'s path) — between
/// them, and via [`apply_aggregate_target`]'s own shared pre-lock, every
/// per-group DML-emission site this reviewer follow-up fixes runs at least
/// once.
#[tokio::test]
async fn explicitly_qualified_aggregate_target_receives_a_live_cdc_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table order_items (id integer primary key, order_id integer, amount numeric); \
             alter table order_items replica identity full",
        )
        .await
        .expect("seed source table and the custom schema");

    const SOURCE: &str = "TRANSFORM custom.order_summary FROM order_items GROUP BY order_id \
         SELECT order_id AS order_id, SUM(amount) AS total";
    let def = parse(SOURCE).expect("parse the explicitly-qualified aggregate target");
    let source_columns = numeric_columns(&["id", "order_id", "amount"]);
    create_aggregate_target_table(&db.pool, &def, "custom", &source_columns)
        .await
        .expect("materialize custom.order_summary ahead of create_definition");
    create_definition(&db.pool, SOURCE, &source_columns)
        .await
        .expect("create aggregate definition against the explicitly-qualified target");

    // Seed group 10's physical row and its matching CDC insert — mirroring
    // real replication, where the physical write and its CDC event both
    // reflect the same post-image.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
            &[],
        )
        .await
        .expect("seed physical order_items row");
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

    let total: Option<String> = client
        .query_one(
            "select total::text from custom.order_summary where order_id = 10",
            &[],
        )
        .await
        .expect(
            "custom.order_summary must exist and hold group 10 — a bare, unqualified \
             upsert_group write would have raised relation \"order_summary\" does not \
             exist instead",
        )
        .get(0);
    assert_eq!(total.as_deref(), Some("5.00"), "seed total for group 10");

    // Delete the only row in group 10, both physically and via CDC — the
    // group goes extinct, routing through `delete_group_row`.
    client
        .execute("delete from order_items where id = 1", &[])
        .await
        .expect("physically delete the only row in group 10");
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

    let remaining: i64 = client
        .query_one(
            "select count(*) from custom.order_summary where order_id = 10",
            &[],
        )
        .await
        .expect("count custom.order_summary")
        .get(0);
    assert_eq!(
        remaining, 0,
        "the extinct group's row must be gone from custom.order_summary"
    );

    let bare_decoy_exists: bool = client
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_name = 'order_summary' and table_schema <> 'custom')",
            &[],
        )
        .await
        .expect("check for a same-named decoy outside the custom schema")
        .get(0);
    assert!(
        !bare_decoy_exists,
        "the live writes must land in custom.order_summary, never create/touch a \
         same-named table in some other schema on the connection's search_path"
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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

/// A brand-new `Aggregate` group whose *only* source row carries a NULL
/// value for its sole `SUM`/`AVG` argument must still get a target row
/// written — never a silent, permanent missing row.
///
/// Root cause: `classify_fields` correctly excludes the `GROUP BY`-echo
/// field from `plan.fields` (it contributes no column of its own), so a
/// definition with only `SUM`/`AVG` fields (no `MIN`/`MAX`/`COUNT`) has no
/// `AggFieldKind::RecomputeOnly` field to anchor a write for a group whose
/// only activity is a NULL contribution. `add_contributions`/
/// `sub_contributions` used to insert a `field_accum` entry *only* when a
/// row's contribution was non-NULL, so a NULL-only group's insert produced
/// zero `field_accum` entries; `group_has_activity` then saw neither a
/// `RecomputeOnly` field nor any `field_accum` entry, reported "no
/// activity," and `apply_aggregate_target` never called `upsert_group` for
/// the group at all — even though `probe_group_exists` correctly found the
/// group's live row moments earlier.
///
/// This also exercises the symmetric direction: once that lone, NULL-only
/// row is later deleted, the group must become fully extinct again (its
/// target row removed), not left behind as a stale row.
#[tokio::test]
async fn a_brand_new_null_only_group_still_gets_a_target_row_and_is_removed_when_emptied() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table null_seed_items (id integer primary key, grp integer, amount numeric); \
             alter table null_seed_items replica identity full",
        )
        .await
        .expect("create source table");

    // Deliberately only SUM/AVG fields (no MIN/MAX/COUNT) — this definition
    // has no `AggFieldKind::RecomputeOnly` field, which is exactly the shape
    // that let a NULL-only group's write get suppressed entirely.
    let source_sql = "TRANSFORM null_seed_totals FROM null_seed_items GROUP BY grp \
         SELECT grp AS grp, SUM(amount) AS total, AVG(amount) AS avg_amount";
    let def = parse(source_sql).expect("parse null-seed aggregate definition");
    let source_columns = numeric_columns(&["id", "grp", "amount"]);
    create_definition(&db.pool, source_sql, &source_columns)
        .await
        .expect("create null-seed definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create null-seed target table");

    // The group's *only* row ever has a NULL argument.
    client
        .execute(
            "insert into null_seed_items (id, grp, amount) values (1, 20, NULL)",
            &[],
        )
        .await
        .expect("apply live end-state");
    insert_cdc_row(
        &client,
        "seg_0",
        "null_seed_items",
        "1",
        "insert",
        None,
        Some(r#"{"grp":"20","amount":null}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let target: HashMap<String, (Option<String>, Option<String>)> = client
        .query(
            "select grp::text, total::text, avg_amount::text from null_seed_totals",
            &[],
        )
        .await
        .expect("read null_seed_totals")
        .into_iter()
        .map(|row| {
            let grp: String = row.get(0);
            (grp, (row.get(1), row.get(2)))
        })
        .collect();
    assert_eq!(
        target.get("20"),
        Some(&(None, None)),
        "a brand-new group whose only row is NULL-argument must still get a \
         target row (NULL SUM/AVG, not a missing row)"
    );

    let oracle_sql = trellis::defs::render_aggregate_select_sql(&def);
    let oracle_sql =
        format!("select grp::text, total::text, avg_amount::text from ({oracle_sql}) o");
    let oracle: HashMap<String, (Option<String>, Option<String>)> = client
        .query(&oracle_sql, &[])
        .await
        .expect("run oracle sql")
        .into_iter()
        .map(|row| {
            let grp: String = row.get(0);
            (grp, (row.get(1), row.get(2)))
        })
        .collect();
    assert_eq!(target, oracle, "must match the oracle");

    // Now delete the group's only row: it must become fully extinct again,
    // not left behind as a stale NULL-valued row.
    client
        .execute("delete from null_seed_items where id = 1", &[])
        .await
        .expect("apply live end-state");
    insert_cdc_row(
        &client,
        "seg_1",
        "null_seed_items",
        "1",
        "delete",
        Some(r#"{"grp":"20","amount":null}"#),
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let remaining: i64 = client
        .query_one("select count(*) from null_seed_totals", &[])
        .await
        .expect("count null_seed_totals")
        .get(0);
    assert_eq!(
        remaining, 0,
        "the group must disappear entirely once its last (NULL-only) row is gone"
    );
}

/// Regression pin for a genuine incremental-`AVG`-maintenance correctness
/// bug (found reviewing the generative test suite's `KeySpace::Aggregate`
/// coverage): three plain `INSERT`s into the *same* group, each landing in
/// its own drain (so each contributes to the group's `AVG` one row at a
/// time, exactly like a live CDC stream rather than one bulk seed batch),
/// used to leave `avg_amount` a *different, wrong* `numeric` value than
/// Postgres's own `avg()` — not a display/formatting difference, a
/// genuinely different rational number (`46.33333333333333333333`, 22
/// digits after the point, vs. the correct `46.3333333333333333`, 16).
///
/// Root cause: `apply_aggregate::row_contribution` computed each row's
/// contribution to an `AVG` field by evaluating that field's own `AVG(...)`
/// expression over a one-row slice — which, per `eval::reduce_numeric_aggregate`,
/// really does perform a `numeric` division (`row's value / 1`). Postgres's
/// `numeric` division scale depends on the *operands'*
/// scale/weight (`Numeric::div`'s `select_div_scale`), not merely "same
/// value, unchanged" — so this inflated a single integer contribution like
/// `67` into `67.0000000000000000`, and `0` into
/// `0.00000000000000000000`. That inflated-scale text then flowed straight
/// into the hidden `__avg_amount_sum` running-sum partial via SQL `sum()`
/// (whose result scale floats up to at least its inputs'), so the scale
/// inflation compounded with every row this group ever accumulated — and
/// the *final* `sum / count` division `upsert_group` performs for the
/// visible `avg_amount` column inherited that already-inflated dividend
/// scale, landing on a different (over-precise, wrongly-rounded) result
/// than Postgres's own `avg()`, which only ever divides once, at the very
/// end, over the group's true final sum/count.
///
/// Fixed by evaluating `AVG` fields as the equivalent `SUM` for the sole
/// purpose of computing a one-row contribution (see
/// `apply_aggregate::contribution_def`'s doc comment): `SUM`'s reduction is
/// pure `Numeric::add`, whose result scale for a single addend is exactly
/// that addend's own scale — no division, no inflation.
#[tokio::test]
async fn avg_maintenance_matches_the_oracle_across_three_single_row_drains_into_one_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
        )
        .await
        .expect("create source table");

    let def = setup(&db).await;

    // Three plain inserts into the same group (order_id 0), values 67, 0,
    // 72 (sum 139, count 3) — each sealed and drained on its own, one row
    // per batch, so `avg_amount` is incrementally maintained three times in
    // a row rather than seeded in one bulk batch.
    for (seg, id, amount) in [("seg_0", 1, "67"), ("seg_1", 2, "0"), ("seg_2", 3, "72")] {
        client
            .execute(
                &format!(
                    "insert into order_items (id, order_id, amount) values ({id}, 0, {amount})"
                ),
                &[],
            )
            .await
            .expect("seed live order_items row");
        insert_cdc_row(
            &client,
            seg,
            "order_items",
            &id.to_string(),
            "insert",
            None,
            Some(&format!(r#"{{"order_id":"0","amount":"{amount}"}}"#)),
        )
        .await;
        let seg_seq = seal_active_segment(&mut client).await;
        drain(&db.pool, seg_seq, "worker").await;
    }

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "avg_amount (and every other aggregate column) must match the oracle bit-for-bit \
         after three single-row drains into one group"
    );
    assert_eq!(
        target["0"].1.as_deref(),
        Some("46.3333333333333333"),
        "avg_amount must equal Postgres's own avg(67, 0, 72), not a scale-inflated value \
         accumulated from dividing each row's contribution by 1 along the way"
    );
}

// ---------------------------------------------------------------------
// Shared count columns across SUM/AVG fields (issue #48)
// ---------------------------------------------------------------------

/// Two SUM fields over two DIFFERENT source columns. Unlike
/// `ORDER_SUMMARY_SOURCE` (whose SUM and AVG already share `amount` and so
/// exercise the shared-count-column path on the *happy* side), this fixture
/// exercises the *safety boundary*: `total_words` and `total_bytes` must each
/// keep their own hidden running-count partial, because `word_count` and
/// `byte_size` can go NULL independently of each other on the same row.
const POSTS_TOTALS_SOURCE: &str = "TRANSFORM posts_totals FROM posts_calc GROUP BY author \
     SELECT author AS author, SUM(word_count) AS total_words, SUM(byte_size) AS total_bytes";

async fn setup_posts_totals(db: &testkit::TestDatabase) -> trellis::defs::ast::TransformDef {
    let def = parse(POSTS_TOTALS_SOURCE).expect("parse posts_totals aggregate definition");
    let source_columns = numeric_columns(&["id", "author", "word_count", "byte_size"]);
    create_definition(&db.pool, POSTS_TOTALS_SOURCE, &source_columns)
        .await
        .expect("create posts_totals definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create posts_totals target table");
    def
}

/// Fetches `posts_totals`'s current rows, keyed by `author` text, as
/// `(total_words, total_bytes)` text tuples.
async fn read_posts_totals_target(
    client: &Client,
) -> HashMap<String, (Option<String>, Option<String>)> {
    client
        .query(
            "select author::text, total_words::text, total_bytes::text from posts_totals",
            &[],
        )
        .await
        .expect("read posts_totals")
        .into_iter()
        .map(|row| {
            let author: String = row.get(0);
            (author, (row.get(1), row.get(2)))
        })
        .collect()
}

/// Runs the oracle's own `SELECT ... GROUP BY` against the live `posts_calc`
/// table, in the same shape [`read_posts_totals_target`] returns.
async fn read_posts_totals_oracle(
    client: &Client,
    def: &trellis::defs::ast::TransformDef,
) -> HashMap<String, (Option<String>, Option<String>)> {
    let sql = trellis::defs::render_aggregate_select_sql(def);
    let sql = format!("select author::text, total_words::text, total_bytes::text from ({sql}) o");
    client
        .query(&sql, &[])
        .await
        .expect("run oracle sql")
        .into_iter()
        .map(|row| {
            let author: String = row.get(0);
            (author, (row.get(1), row.get(2)))
        })
        .collect()
}

/// Issue #48: `total_words` (`SUM(word_count)`) and `total_bytes`
/// (`SUM(byte_size)`) must maintain *independent* hidden running counts, even
/// though a naive fix for #48 would merge every SUM/AVG field in a target
/// down to one shared `__group_count`. This seeds two rows per group whose
/// NULL-ness is deliberately staggered across the two columns, then deletes
/// the row that is each field's *only* non-null contributor — driving one
/// field to `NULL` while the other must stay a real number. A shared/merged
/// count column would corrupt at least one of the two outcomes.
#[tokio::test]
async fn aggregate_columns_over_different_arguments_maintain_independent_counts_through_insert_and_delete()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table posts_calc (
                 id integer primary key, author integer, word_count numeric, byte_size numeric
             ); \
             alter table posts_calc replica identity full",
        )
        .await
        .expect("create source table");

    let def = setup_posts_totals(&db).await;

    // Group 1 gets two rows: r1 contributes a non-null word_count but a NULL
    // byte_size; r2 is the mirror image (NULL word_count, non-null
    // byte_size). Each field's total is driven entirely by the other row.
    client
        .execute(
            "insert into posts_calc (id, author, word_count, byte_size) values \
             (1, 1, 100, NULL), (2, 1, NULL, 20)",
            &[],
        )
        .await
        .expect("seed live posts_calc rows");
    insert_cdc_row(
        &client,
        "seg_0",
        "posts_calc",
        "1",
        "insert",
        None,
        Some(r#"{"author":"1","word_count":"100","byte_size":null}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "posts_calc",
        "2",
        "insert",
        None,
        Some(r#"{"author":"1","word_count":null,"byte_size":"20"}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    let target = read_posts_totals_target(&client).await;
    assert_eq!(
        target["1"],
        (Some("100".to_string()), Some("20".to_string())),
        "seed: total_words from r1 only, total_bytes from r2 only"
    );

    // Delete r1 (word_count = 100, byte_size = NULL): it was total_words's
    // *only* non-null contributor, so total_words must go NULL, but it made
    // no contribution to total_bytes's count at all, so total_bytes (still
    // backed by r2) must be completely unaffected.
    client
        .execute("delete from posts_calc where id = 1", &[])
        .await
        .expect("apply live end-state");
    insert_cdc_row(
        &client,
        "seg_1",
        "posts_calc",
        "1",
        "delete",
        Some(r#"{"author":"1","word_count":"100","byte_size":null}"#),
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_posts_totals_target(&client).await;
    let oracle = read_posts_totals_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "must match the oracle even once the two fields' non-null contributors diverge"
    );
    assert_eq!(
        target["1"],
        (None, Some("20".to_string())),
        "total_words must go NULL (its only non-null contributor, r1, is gone) while \
         total_bytes must stay 20 (r2, its own non-null contributor, is untouched) — a \
         shared/merged count column would corrupt one of these two independent outcomes"
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
    def: &trellis::defs::ast::TransformDef,
) -> HashMap<String, (Option<String>, Option<String>)> {
    let sql = trellis::defs::render_aggregate_select_sql(def);
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

async fn setup_counts(db: &testkit::TestDatabase) -> trellis::defs::ast::TransformDef {
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full; \
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

/// A from-scratch backfill (every source row staged image-less, so every
/// group takes the full-recompute path — issue #59) over many groups must
/// (a) land exactly the oracle's values for all of them, and (b) not scan
/// the source table once per group. The pre-#59 per-group probe loop did an
/// existence probe plus one probe per field for each group — `O(groups)`
/// source scans — so with `GROUP_COUNT` groups it would scan `order_items`
/// thousands of times; the bulk path is a fixed handful regardless. We read
/// that scan count straight off `pg_stat_user_tables` as the regression
/// guard.
#[tokio::test]
async fn backfilling_many_groups_matches_the_oracle_without_per_group_source_scans() {
    const GROUP_COUNT: i64 = 750;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
        )
        .await
        .expect("create source table");

    let def = setup(&db).await;

    // Two rows per group (distinct amounts) so SUM/AVG/MAX/MIN are all
    // non-trivial and the oracle comparison is a real correctness check, not
    // a one-row identity.
    let mut values = Vec::new();
    for g in 1..=GROUP_COUNT {
        let id_a = g * 2 - 1;
        let id_b = g * 2;
        values.push(format!("({id_a}, {g}, {g}.00)"));
        values.push(format!("({id_b}, {g}, {}.00)", g * 3));
    }
    client
        .batch_execute(&format!(
            "insert into order_items (id, order_id, amount) values {}",
            values.join(", ")
        ))
        .await
        .expect("seed live order_items rows");

    // Stage every source row as an image-less change — exactly what a
    // from-scratch backfill enqueues — so each group is forced to full
    // recompute.
    for g in 1..=GROUP_COUNT {
        for id in [g * 2 - 1, g * 2] {
            insert_cdc_row(
                &client,
                "seg_0",
                "order_items",
                &id.to_string(),
                "recompute",
                None,
                None,
            )
            .await;
        }
    }

    let seg0 = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg0, "worker").await;
    assert_eq!(
        outcome.keys_written, GROUP_COUNT as usize,
        "every one of the {GROUP_COUNT} groups is newly created"
    );

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "the bulk backfill of {GROUP_COUNT} groups must match the oracle exactly"
    );

    // The regression guard: source scans must stay a small constant, not
    // scale with GROUP_COUNT. The bulk path scans `order_items` a handful of
    // times per batch (the batched live-row refetch, the survivor probe, and
    // the INSERT ... SELECT recompute); the pre-#59 loop scanned it
    // `O(GROUP_COUNT)` times. A generous ceiling well below GROUP_COUNT
    // fails loudly on regression while tolerating planner/refetch variation.
    let scans: i64 = client
        .query_one(
            "select coalesce(seq_scan, 0) + coalesce(idx_scan, 0) \
             from pg_stat_user_tables where relname = 'order_items'",
            &[],
        )
        .await
        .expect("read order_items scan count")
        .get(0);
    assert!(
        scans < 50,
        "backfilling {GROUP_COUNT} groups scanned order_items {scans} times; \
         the bulk recompute must not scan once per group (issue #59)"
    );
}

const REGION_SALES_SOURCE: &str = "TRANSFORM region_sales FROM sales \
     GROUP BY region, order_id \
     SELECT region AS region, order_id AS order_id, SUM(amount) AS total, COUNT(*) AS cnt";

fn region_sales_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("region".to_string(), ValueType::Text),
        ("order_id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ])
}

/// `region_sales` keyed by `(region, order_id)` text, as `(total, cnt)`.
async fn read_region_target(
    client: &Client,
) -> HashMap<(String, String), (Option<String>, Option<String>)> {
    client
        .query(
            "select region::text, order_id::text, total::text, cnt::text from region_sales",
            &[],
        )
        .await
        .expect("read region_sales")
        .into_iter()
        .map(|row| {
            let region: String = row.get(0);
            let order_id: String = row.get(1);
            ((region, order_id), (row.get(2), row.get(3)))
        })
        .collect()
}

async fn read_region_oracle(
    client: &Client,
    def: &trellis::defs::ast::TransformDef,
) -> HashMap<(String, String), (Option<String>, Option<String>)> {
    let sql = trellis::defs::render_aggregate_select_sql(def);
    let sql = format!("select region::text, order_id::text, total::text, cnt::text from ({sql}) o");
    client
        .query(&sql, &[])
        .await
        .expect("run region oracle sql")
        .into_iter()
        .map(|row| {
            let region: String = row.get(0);
            let order_id: String = row.get(1);
            ((region, order_id), (row.get(2), row.get(3)))
        })
        .collect()
}

/// A from-scratch backfill over a **multi-column** `GROUP BY` (issue #59):
/// every source row is staged image-less, so every `(region, order_id)`
/// group is forced onto the bulk-recompute path — exercising
/// `keyset_unnest`/`keyset_match`/`transpose_group_values` at arity 2 (and,
/// via `region`, a non-numeric key column's `::text[]::text[]` cast). Must
/// match the oracle exactly.
#[tokio::test]
async fn backfilling_a_multi_column_group_by_matches_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table sales \
             (id integer primary key, region text, order_id integer, amount numeric); alter table sales replica identity full",
        )
        .await
        .expect("create source table");

    let def = parse(REGION_SALES_SOURCE).expect("parse multi-column aggregate definition");
    let source_columns = region_sales_columns();
    create_definition(&db.pool, REGION_SALES_SOURCE, &source_columns)
        .await
        .expect("create multi-column aggregate definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create multi-column aggregate target table");

    // Several distinct (region, order_id) groups, some sharing a region, some
    // sharing an order_id, most with more than one row.
    client
        .batch_execute(
            "insert into sales (id, region, order_id, amount) values \
             (1, 'west', 10, 2.00), (2, 'west', 10, 3.00), \
             (3, 'west', 20, 5.00), \
             (4, 'east', 10, 7.00), (5, 'east', 10, 1.00), \
             (6, 'east', 30, 9.00)",
        )
        .await
        .expect("seed live sales rows");

    for id in 1..=6 {
        insert_cdc_row(
            &client,
            "seg_0",
            "sales",
            &id.to_string(),
            "recompute",
            None,
            None,
        )
        .await;
    }

    let seg0 = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg0, "worker").await;
    assert_eq!(
        outcome.keys_written, 4,
        "four distinct (region, order_id) groups are newly created"
    );

    let target = read_region_target(&client).await;
    let oracle = read_region_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "multi-column backfill must match the oracle exactly"
    );
}

/// One `apply_aggregate_target` batch that mixes a forced (image-less)
/// group and an ordinary delta group must route each down its own path (the
/// forced group through the bulk recompute, the delta group through the
/// unchanged per-group `upsert_group`) and land both correctly against the
/// oracle.
#[tokio::test]
async fn a_mixed_forced_and_delta_batch_matches_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
        )
        .await
        .expect("create source table");

    let def = setup(&db).await;

    // Seed group 10 with a normal delta batch so it has a live target row.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (1, 10, 4.00)",
            &[],
        )
        .await
        .expect("seed group 10");
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"10","amount":"4.00"}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg0, "worker").await;

    // One batch: an image-less recompute for group 10 (forced -> bulk path)
    // alongside an ordinary insert for a brand-new group 20 (delta path).
    client
        .batch_execute(
            "update order_items set amount = 6.00 where id = 1; \
             insert into order_items (id, order_id, amount) values (2, 20, 8.00)",
        )
        .await
        .expect("apply live end-state for mixed batch");
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
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"8.00"}"#),
    )
    .await;

    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "a batch mixing a forced group and a delta group must match the oracle"
    );
}

/// Issue #63 Milestone 2 follow-up: `drain_many`'s segment-coalescing path
/// (see `apply.rs`'s `drain_many_coalesces_two_sealed_segments_into_one_apply_pass`)
/// only had 1-1 TRANSFORM coverage. M2's actual motivation is the cost of an
/// aggregate's forced-group recompute — this test exercises the coalesced
/// path against a real `KeySpace::Aggregate` target, with a group touched by
/// *both* segments, in three ways a naive per-segment (rather than
/// merged-then-applied) implementation could get wrong:
///
/// - Group 10 is touched by the *same key* (id 2) in both segments, via a
///   chained update (3.00 -> 6.00 -> 9.00). `merge_pair` must stitch the
///   earliest old-image and the latest new-image into one net delta (+6),
///   not apply +3 twice or use the wrong old-image and land on +3.
/// - Group 20 is touched by *different keys* in each segment (id 4 in
///   segment 1, id 5 in segment 2) — no key-level merge at all, so this
///   checks that `accumulate_changes` sums both segments' contributions into
///   one group write rather than only seeing whichever segment's fold
///   happened to run.
/// - Group 30 is reachable *only* via an image-less recompute trigger staged
///   in segment 1 (id 6), with segment 2 contributing nothing to that group.
///   If the forced full-recompute path silently dropped out under
///   coalescing, group 30 would never appear at all.
#[tokio::test]
async fn drain_many_coalesces_two_sealed_segments_into_one_aggregate_apply_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); alter table order_items replica identity full",
        )
        .await
        .expect("create source table");

    let def = setup(&db).await;

    // Pre-seed: group 10's baseline (members 1 and 2), established by an
    // ordinary, non-coalesced drain before the coalesced batch under test.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (1, 10, 5.00), (2, 10, 3.00)",
            &[],
        )
        .await
        .expect("seed group 10's baseline live rows");
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
        Some(r#"{"order_id":"10","amount":"3.00"}"#),
    )
    .await;
    let seg0 = seal_active_segment(&mut client).await;
    let outcome0 = drain(&db.pool, seg0, "worker").await;
    assert_eq!(outcome0.keys_written, 1, "group 10 is newly created");

    let target = read_target(&client).await;
    assert_eq!(target["10"].0.as_deref(), Some("8.00"), "group 10 baseline");

    // A row that has already landed live, entirely out-of-band (never
    // reflected by any CDC image) — group 30 doesn't exist in the target
    // yet, and only a forced full recompute can bring it in.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (6, 30, 13.00)",
            &[],
        )
        .await
        .expect("out-of-band insert into group 30");

    // Segment 1: id 2's interim update (group 10, chained across segments),
    // a brand-new row in group 20 (id 4), and an image-less recompute
    // trigger for group 30 (id 6).
    client
        .execute("update order_items set amount = 6.00 where id = 2", &[])
        .await
        .expect("apply id 2's interim live update");
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "2",
        "update",
        Some(r#"{"order_id":"10","amount":"3.00"}"#),
        Some(r#"{"order_id":"10","amount":"6.00"}"#),
    )
    .await;
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (4, 20, 7.00)",
            &[],
        )
        .await
        .expect("insert id 4 into group 20");
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "4",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"7.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "6",
        "recompute",
        None,
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    // Segment 2: id 2's final update (same key as segment 1, closing the
    // chain at 9.00) and a second, distinct row in group 20 (id 5) — group
    // 20 is now touched by both segments, but never by the same key.
    client
        .execute("update order_items set amount = 9.00 where id = 2", &[])
        .await
        .expect("apply id 2's final live update");
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "2",
        "update",
        Some(r#"{"order_id":"10","amount":"6.00"}"#),
        Some(r#"{"order_id":"10","amount":"9.00"}"#),
    )
    .await;
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (5, 20, 11.00)",
            &[],
        )
        .await
        .expect("insert id 5 into group 20");
    insert_cdc_row(
        &client,
        "seg_2",
        "order_items",
        "5",
        "insert",
        None,
        Some(r#"{"order_id":"20","amount":"11.00"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;

    let outcome = apply::drain_many(
        &db.pool,
        &[seg1, seg2],
        "worker",
        1,
        "trellis_apply_aggregate_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_many")
    .expect("drain_many must claim and drain something");

    assert_eq!(
        outcome.keys_written, 3,
        "groups 10, 20, and 30 must each be written exactly once"
    );
    assert_eq!(outcome.keys_deleted, 0);
    assert_eq!(outcome.segments_drained.len(), 2);
    for &(seg_seq, fully_drained) in &outcome.segments_drained {
        assert!(
            fully_drained,
            "segment {seg_seq} must be fully drained by the single coalesced apply pass"
        );
    }

    let target = read_target(&client).await;
    let oracle = read_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "the coalesced aggregate apply pass must match the oracle exactly"
    );

    assert_eq!(
        target["10"].0.as_deref(),
        Some("14.00"),
        "group 10: id 2's chained update (3.00 -> 6.00 -> 9.00) must net a single \
         +6.00 delta on top of the 8.00 baseline (5.00 + 3.00), not double-apply \
         +3.00 twice or mis-merge onto the wrong old image"
    );
    assert_eq!(
        target["20"].0.as_deref(),
        Some("18.00"),
        "group 20: id 4 (segment 1) and id 5 (segment 2) must both contribute to \
         one merged group write (7.00 + 11.00), even though no single key was \
         touched by both segments"
    );
    assert_eq!(
        target["30"].0.as_deref(),
        Some("13.00"),
        "group 30 is only reachable via id 6's image-less recompute trigger in \
         segment 1 — if the forced full-recompute path were dropped under \
         coalescing, this group would never appear at all"
    );
}

// ---------------------------------------------------------------------
// Cross-field-alias reference on a `GROUP BY` field (a calculated field
// referencing another calculated field by name)
// ---------------------------------------------------------------------

const ORDER_ALIAS_SOURCE: &str = "TRANSFORM order_alias_totals FROM order_items GROUP BY order_id \
     SELECT order_id AS order_id, SUM(amount) AS total, total + total AS double_total";

async fn setup_order_alias_totals(db: &testkit::TestDatabase) -> trellis::defs::ast::TransformDef {
    let def = parse(ORDER_ALIAS_SOURCE).expect("parse order_alias_totals aggregate definition");
    let source_columns = numeric_columns(&["id", "order_id", "amount"]);
    create_definition(&db.pool, ORDER_ALIAS_SOURCE, &source_columns)
        .await
        .expect("create order_alias_totals definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create order_alias_totals target table");
    def
}

/// Fetches `order_alias_totals`'s current rows, keyed by `order_id` text, as
/// `(total, double_total)` text tuples.
async fn read_order_alias_totals_target(
    client: &Client,
) -> HashMap<String, (Option<String>, Option<String>)> {
    client
        .query(
            "select order_id::text, total::text, double_total::text from order_alias_totals",
            &[],
        )
        .await
        .expect("read order_alias_totals")
        .into_iter()
        .map(|row| {
            let order_id: String = row.get(0);
            (order_id, (row.get(1), row.get(2)))
        })
        .collect()
}

/// Runs the oracle's own `SELECT ... GROUP BY` (via
/// `render_aggregate_select_sql`) against the live `order_items` table, in
/// the same shape [`read_order_alias_totals_target`] returns — exercising
/// the fix's fourth call site (the oracle used to hit the identical
/// "column does not exist" error this test's real target would, which is
/// why it was never caught as a working point of comparison for this
/// shape).
async fn read_order_alias_totals_oracle(
    client: &Client,
    def: &trellis::defs::ast::TransformDef,
) -> HashMap<String, (Option<String>, Option<String>)> {
    let sql = trellis::defs::render_aggregate_select_sql(def);
    let sql = format!("select order_id::text, total::text, double_total::text from ({sql}) o");
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

/// Regression test: a `GROUP BY` field that references another calculated
/// field by name (`double_total = total + total`, where `total =
/// SUM(amount)`) is valid per the validator (`validate_aggregate_field_expr`
/// only rejects a *bare source column* outside an aggregate call) and the
/// per-row evaluator (`eval::evaluate_aggregate` resolves it recursively via
/// `fields_by_name`), but three SQL-rendering call sites
/// (`apply_aggregate::classify_fields`, the `staging::apply` aggregate
/// dispatch's `field_exprs`, and `oracle::render_aggregate_select_sql`) used
/// to render `double_total`'s raw, un-substituted `Expr::Column("total")` as
/// a bare SQL identifier — which Postgres rejects, since `total` names
/// neither a source column nor a same-SELECT-list-visible name. This
/// exercises the live incremental-apply path specifically (an INSERT
/// followed by an UPDATE, both landing through the ordinary CDC/drain path,
/// not the direct backfill), confirming `classify_fields`/
/// `accumulate_changes`/the `RecomputeOnly` probe path (`probe_field_value`)
/// all correctly resolve `double_total` against `total`'s current value.
#[tokio::test]
async fn drain_computes_an_aggregate_cross_field_alias_through_insert_and_update() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             alter table order_items replica identity full",
        )
        .await
        .expect("create source table");

    let def = setup_order_alias_totals(&db).await;

    // Seed group 10 via an ordinary insert batch: total = 5.00, double_total
    // = 10.00.
    client
        .execute(
            "insert into order_items (id, order_id, amount) values (1, 10, 5.00)",
            &[],
        )
        .await
        .expect("seed live order_items row");
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

    let target = read_order_alias_totals_target(&client).await;
    let oracle = read_order_alias_totals_oracle(&client, &def).await;
    assert_eq!(target, oracle, "seed batch must already match the oracle");
    assert_eq!(
        target["10"],
        (Some("5.00".to_string()), Some("10.00".to_string())),
        "double_total must be 2 * total after the seed insert"
    );

    // Update the row's amount: total = 9.00, double_total must follow to
    // 18.00, re-derived by the RecomputeOnly probe path (double_total is a
    // composed expression, never incremented directly).
    client
        .execute("update order_items set amount = 9.00 where id = 1", &[])
        .await
        .expect("apply live update");
    insert_cdc_row(
        &client,
        "seg_1",
        "order_items",
        "1",
        "update",
        Some(r#"{"order_id":"10","amount":"5.00"}"#),
        Some(r#"{"order_id":"10","amount":"9.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg1, "worker").await;

    let target = read_order_alias_totals_target(&client).await;
    let oracle = read_order_alias_totals_oracle(&client, &def).await;
    assert_eq!(
        target, oracle,
        "must match the oracle after the update that changes total"
    );
    assert_eq!(
        target["10"],
        (Some("9.00".to_string()), Some("18.00".to_string())),
        "double_total must track 2 * total after the update"
    );
}

/// Issue #103: chaining an ordinary 1-1 definition onto a
/// single-`GROUP BY`-column aggregate target must not misread
/// `derive_group_key`'s internal `HashMap`-key encoding as the target's
/// real primary-key value.
///
/// `order_summary`'s real Postgres identity (its `UNIQUE NULLS NOT
/// DISTINCT` grouping-column constraint, the same one
/// `ddl::source_primary_key` falls back to when nothing is `indisprimary`)
/// is genuinely a *single*, unencoded column (`order_id`) — so
/// `ddl::source_primary_key` does not reject `order_summary_alert`
/// chaining onto it (at the time of #103 a *composite* grouping key still
/// was rejected; issue #126 lifted even that, and issue #171 then gave the
/// composite case this same encoding fix — see
/// `defs_aggregate_chained_composite_group_key.rs`), and
/// `defs::validate`/`create_definition` impose no primary-key-shape check
/// of their own. Before the fix, `derive_group_key` always
/// length-prefix-encoded the group key regardless of arity, and that
/// encoded string flowed, unchanged, into the `Recompute` this chained
/// definition automatically gets staged
/// (`apply_and_mark_drained_many`'s downstream-propagation step) — whose
/// live refetch (`apply::read_live_rows_batch`) then tried to bind the
/// encoded string as a literal primary-key value, crashing exactly as the
/// issue reports.
///
/// `order_id` is `uuid` here, not the issue's own `numeric` example:
/// `defs::catalog::is_text_stable_join_key_type` (issue #107, landed after
/// #103 was filed, unrelated to it) now makes `ddl::source_primary_key`
/// reject a `numeric`-typed 1-1 source primary key outright — and
/// `create_aggregate_target_table` only ever renders a `GROUP BY` column
/// as one of `numeric`/`text`/`boolean`/`uuid` (`ValueType`'s own
/// variants), never a narrower concrete integer type — so a `numeric`
/// group-by column can no longer even reach `create_target_table` to
/// reproduce the issue's exact wording. `uuid` is the other
/// `create_aggregate_target_table`-reachable type still on that allowlist,
/// so it's what still exercises `derive_group_key`'s live, reachable bug
/// today: a `uuid` column's length-prefixed encoding of itself is never
/// itself a valid `uuid` literal, so the chained definition's live refetch
/// still crashes the same way, on `invalid input syntax for type uuid`
/// instead of `numeric` — same root cause, same fix, still a real (not
/// merely historical) regression risk this test guards.
///
/// After the fix, a single-column `GROUP BY`'s key is the plain, unencoded
/// value, matching `order_summary`'s real PK shape exactly, so this drain
/// succeeds and correctly propagates the row.
#[tokio::test]
async fn chaining_onto_a_single_group_by_column_aggregate_target_does_not_misread_the_group_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (\
                 id integer primary key, order_id uuid, amount numeric\
             ); \
             alter table order_items replica identity full",
        )
        .await
        .expect("create source table");

    const AGG_SOURCE: &str = "TRANSFORM order_summary FROM order_items GROUP BY order_id \
         SELECT order_id AS order_id, SUM(amount) AS total";
    let agg_def = parse(AGG_SOURCE).expect("parse aggregate definition");
    let order_items_columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("order_id".to_string(), ValueType::Uuid),
        ("amount".to_string(), ValueType::Numeric),
    ]);
    // `order_items` is still empty at definition-creation time (same
    // "definition created before its source has any rows" convention
    // every other test in this file uses), so this definition's own
    // initial ring backfill enumerates nothing.
    create_definition(&db.pool, AGG_SOURCE, &order_items_columns)
        .await
        .expect("create aggregate definition");
    create_aggregate_target_table(&db.pool, &agg_def, "public", &order_items_columns)
        .await
        .expect("create order_summary target table");

    // Chain a second, ordinary 1-1 definition onto `order_summary` as its
    // source — issue #103's exact scenario.
    const CHAINED_SOURCE: &str =
        "TRANSFORM order_summary_alert FROM order_summary SELECT total AS total";
    let chained_def = parse(CHAINED_SOURCE).expect("parse chained definition");
    let summary_columns = HashMap::from([
        ("order_id".to_string(), ValueType::Uuid),
        ("total".to_string(), ValueType::Numeric),
    ]);
    // `create_definition_without_backfill`, not `create_definition`:
    // `order_summary`'s target table is keyed by a `UNIQUE NULLS NOT
    // DISTINCT` constraint rather than a real Postgres `PRIMARY KEY` (see
    // `create_aggregate_target_table`'s doc comment) — a deliberate,
    // separate design choice so a NULL grouping value stays representable.
    // `intake::publication::enumerate_and_append`'s own primary-key lookup
    // (unlike `ddl::source_primary_key`'s unique-index fallback this
    // test's `#103` fix cares about) requires a real `indisprimary` row and
    // has no such fallback, so a definition-creation backfill enumeration
    // of `order_summary` would fail with `MissingKeyValue` — an unrelated,
    // pre-existing gap. Harmless to skip here regardless: `order_summary`
    // is still empty at this point, so there's nothing to backfill.
    create_definition_without_backfill(&db.pool, CHAINED_SOURCE, &summary_columns)
        .await
        .expect("create chained definition reading order_summary");
    let summary_pk = require_single_column_pk(
        source_primary_key(&db.pool, "order_summary")
            .await
            .expect("introspect order_summary's real (single-column) primary key"),
        "order_summary",
    )
    .expect("order_summary's real PK is single-column");
    assert_eq!(
        summary_pk.name, "order_id",
        "order_summary's real PK must be its single GROUP BY column"
    );
    assert_eq!(summary_pk.data_type, "uuid");
    create_target_table(
        &db.pool,
        &chained_def,
        "public",
        &summary_pk,
        &summary_columns,
        "order_summary",
    )
    .await
    .expect("create order_summary_alert target table");

    // Step 1: a fresh group lands via an ordinary live insert into
    // `order_items` — this drain writes `order_summary`'s group row and,
    // since `order_summary_alert` reads it, automatically stages a
    // downstream `Recompute` for that group's key at `hop_gen + 1`, into
    // the *next* active segment (not this one).
    const ORDER_ID: &str = "11111111-1111-1111-1111-111111111111";
    client
        .execute(
            &format!(
                "insert into order_items (id, order_id, amount) values (1, '{ORDER_ID}', 5.00)"
            ),
            &[],
        )
        .await
        .expect("seed live order_items row");
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(&format!(r#"{{"order_id":"{ORDER_ID}","amount":"5.00"}}"#)),
    )
    .await;

    let seg0 = seal_active_segment(&mut client).await;
    let outcome0 = drain(&db.pool, seg0, "worker").await;
    assert_eq!(
        outcome0.keys_written, 1,
        "order_summary's group is newly created"
    );

    let total: String = client
        .query_one(
            &format!("select total::text from order_summary where order_id = '{ORDER_ID}'"),
            &[],
        )
        .await
        .expect("order_summary group row")
        .get(0);
    assert_eq!(total, "5.00");

    // Step 2: drain the automatically-staged downstream `Recompute` for
    // `order_summary_alert`. Before the fix, this call would panic —
    // `read_live_rows_batch`'s live refetch tried to bind the
    // length-prefixed-encoded group key (not a valid `uuid` literal) as a
    // literal `uuid` primary-key value and the underlying query failed
    // outright.
    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1, "worker").await;
    assert_eq!(
        outcome1.keys_written, 1,
        "the chained order_summary_alert definition's recompute must succeed"
    );

    let alert_total: String = client
        .query_one(
            &format!("select total::text from order_summary_alert where order_id = '{ORDER_ID}'"),
            &[],
        )
        .await
        .expect("order_summary_alert must have the chained row")
        .get(0);
    assert_eq!(
        alert_total, "5.00",
        "the chained definition must read order_summary's real (unencoded) group key value"
    );
}
