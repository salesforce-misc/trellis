//! Integration tests for column-level quarantine
//! (`docs/decisions/0003-quarantine-storage-and-api.md`'s 2026-09-12
//! amendment, `docs/decisions/0008-public-api-design.md` decision 5): the per-`(transform,
//! column)` fuse layered alongside the pre-existing row-level/transform-wide
//! one (`trellis/tests/quarantine.rs`, unmodified by this feature — see
//! `an_existing_row_level_fuse_scenario_is_unaffected` below for a targeted
//! regression check of that claim in this file too).
//!
//! Follows `trellis/tests/quarantine.rs`'s own conventions: a real, ephemeral
//! Postgres instance per test (`testkit::TestCluster`), and "reach past the
//! mechanism, insert directly" for whichever half of a scenario the
//! mechanism under test doesn't itself produce (staging a malformed CDC
//! image directly into the ring, rather than routing through the capture
//! triggers).

use std::collections::HashMap;
use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    TransformStatus, chunk_queue, create_aggregate_target_table, create_definition,
    create_relationship, create_target_table, install_definition, relationship_projection,
    source_primary_key,
};
use trellis::dev::interleave::{PausePoint, PauseScope, with_scope};
use trellis::staging::StagedWatermark;
use trellis::staging::apply::{self, ApplyError};
use trellis::staging::quarantine::{self, DEFAULT_COLUMN_DEATH_THRESHOLD};
use trellis::{BlockingTrellis, Config, Trellis, TrellisOptions};

// ---------------------------------------------------------------------
// Shared scaffolding (mirrors `tests/quarantine.rs`)
// ---------------------------------------------------------------------

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("set search_path to trellis, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'")
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// The ring table `insert_cdc_row` must target *right now* — `seg_0` is only
/// correct for a test's first, never-yet-sealed batch; every batch after
/// that has rotated the active ring slot forward (`seal_active_segment`
/// advances `segment_pointer`), and inserting into a already-sealed table
/// would silently stage into the wrong (already-closed) batch. Every test
/// below that stages more than one batch reads this fresh before each one.
async fn active_segment_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    match ring_slot {
        0 => "seg_0",
        1 => "seg_1",
        2 => "seg_2",
        3 => "seg_3",
        other => panic!("unexpected ring slot {other}"),
    }
    .to_string()
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every bare
/// `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `trellis, public` and never qualifies
/// its own DDL. Matches `tests/quarantine.rs`'s (and `tests/apply.rs`'s) own
/// `qualify_fixture_table` (issue #74, ADR-0007): a CDC-staged `src_table`
/// must be fully qualified to match what a real CDC producer stages (issue
/// #76) and what the dependency graph now keys on (issue #74), or
/// `catalog::transforms_for_source` silently finds nothing and this whole
/// file's column-fuse machinery never even gets exercised. Already-qualified
/// input (containing a `.`) passes through unchanged.
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
    let lsn = testkit::wal_insert_lsn(client).await;
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

fn order_totals_def() -> TransformDef {
    TransformDef {
        target: "order_totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

/// Creates `orders` (unpopulated) plus the `order_totals` 1-1 definition and
/// target table — the single-column fixture most tests below trip the
/// column fuse against.
async fn seed_order_totals(db: &TestDatabase, client: &Client) -> TransformDef {
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");
    def
}

/// Adds a second, chained 1-1 transform (`order_summaries`, reading straight
/// from `order_totals`'s own `total` column) — the fixture the cascade tests
/// use. Mirrors `tests/quarantine.rs`'s `order_summary` hop-bound fixture.
async fn seed_order_summaries(db: &TestDatabase, orders_def: &TransformDef) {
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summaries FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summaries definition");
    let pk = source_primary_key(&db.pool, &orders_def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &summary_def.def,
        "public",
        &pk,
        &order_totals_columns,
        &summary_def.def.source,
    )
    .await
    .expect("create order_summaries table");
}

/// Stages one malformed (`price` = `"not-a-number"`) CDC insert per id in
/// `ids`, all into the *same* segment, seals it, and drains it once —
/// expecting an evaluator failure to surface (the drive-by-real-failures
/// pattern `tests/quarantine.rs`'s own eviction test uses, just for the
/// column fuse instead of the row-level one).
///
/// Deliberately one segment per call, not one per `id`: none of these
/// batches ever actually reaches `drained` (a lone bad key's own
/// `key_deaths` never crosses the *row-level* fuse's threshold on its own,
/// so `drain_once` always gives up and propagates rather than evicting and
/// retrying to success) — the ring only has `RING_SIZE` (4) physical slots,
/// and nothing here ever retires a stuck segment, so batching every id this
/// call needs into one segment keeps every test's total segment count under
/// that ceiling instead of exhausting the ring.
async fn stage_bad_orders(client: &mut Client, pool: &trellis::Pool, ids: &[i64]) {
    let table = active_segment_table(client).await;
    for id in ids {
        insert_cdc_row(
            client,
            &table,
            "orders",
            &id.to_string(),
            "insert",
            None,
            Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
        )
        .await;
    }
    let seg_seq = seal_active_segment(client).await;
    let result = apply::drain_once(
        pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "a malformed numeric field must still surface as an evaluator failure, got {result:?}"
    );
}

/// The persisted status of the transform whose bare target name is `target`.
async fn status_named(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0)
}

async fn column_status_row(
    client: &Client,
    transform: &str,
    column: &str,
) -> Option<(bool, Option<String>)> {
    client
        .query_opt(
            "select local_fuse, last_error from column_status \
             where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_status")
        .map(|row| (row.get(0), row.get(1)))
}

async fn column_deaths_count(client: &Client, transform: &str, column: &str) -> Option<i32> {
    client
        .query_opt(
            "select deaths from column_deaths where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_deaths")
        .map(|row| row.get(0))
}

async fn cascade_edge_exists(
    client: &Client,
    downstream_transform: &str,
    downstream_column: &str,
    upstream_transform: &str,
    upstream_column: &str,
) -> bool {
    client
        .query_opt(
            "select 1 from column_pause_cascades \
             where downstream_transform = $1 and downstream_column = $2 \
               and upstream_transform = $3 and upstream_column = $4",
            &[
                &downstream_transform,
                &downstream_column,
                &upstream_transform,
                &upstream_column,
            ],
        )
        .await
        .expect("read column_pause_cascades")
        .is_some()
}

// ---------------------------------------------------------------------
// (a) The column fuse trips after the threshold is crossed, and not before.
// ---------------------------------------------------------------------

#[tokio::test]
async fn column_fuse_trips_only_once_the_threshold_is_crossed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    assert_eq!(
        DEFAULT_COLUMN_DEATH_THRESHOLD, 5,
        "test assumes the default"
    );

    // `DEFAULT_COLUMN_DEATH_THRESHOLD - 1` distinct bad rows, one batch:
    // charged, but not paused.
    let below_threshold: Vec<i64> = (1..DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &below_threshold).await;
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(DEFAULT_COLUMN_DEATH_THRESHOLD - 1),
        "every distinct bad row below threshold must be charged exactly once"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "must not be paused before the threshold is reached"
    );

    // One more distinct bad row, its own batch: trips the fuse.
    stage_bad_orders(
        &mut client,
        &db.pool,
        &[DEFAULT_COLUMN_DEATH_THRESHOLD as i64],
    )
    .await;

    let status = column_status_row(&client, "order_totals", "total")
        .await
        .expect("the column must be paused now that the threshold is crossed");
    assert!(status.0, "a threshold trip is a local fuse, not a cascade");
    assert!(status.1.is_some(), "the tripping error must be recorded");
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "the counter must reset once the fuse trips"
    );
}

/// A column fuse trip bumps its source's version fence as the first lock of
/// the transaction that writes the pause (issue #903), so it can wait out the
/// lock timeout behind a writer holding the fence. The charge that reached
/// the threshold has committed by then and the trip has not, so the count is
/// left at the threshold with the column live. The next failure of a row
/// already charged then trips the fuse; without that, only another row's
/// first failure would. Here the drain's sessions run with a 100 ms
/// `lock_timeout` (a setting shorter than Trellis's 30 s cap is kept) while a
/// raw transaction holds `orders`' fence `for share`, as a page does.
#[tokio::test]
async fn a_column_fuse_trip_that_waits_out_the_lock_timeout_trips_on_the_next_failure() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;
    let below_threshold: Vec<i64> = (1..DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &below_threshold).await;

    let impatient = trellis::Pool::new(
        &Config::from_dsn(format!("{} options='-c lock_timeout=100'", db.dsn()))
            .expect("valid dsn"),
    )
    .expect("pool");
    let mut page = connect_raw(db.dsn()).await;
    let holder = page.transaction().await.expect("begin the fence holder");
    holder
        .execute(
            "select v.version from source_table_versions v \
             join transform_definitions d on d.source_table = v.source_table \
             where split_part(d.target_table, '.', 2) = 'order_totals' for share of v",
            &[],
        )
        .await
        .expect("hold orders' fence");

    let threshold_key = DEFAULT_COLUMN_DEATH_THRESHOLD as i64;
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        &threshold_key.to_string(),
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(
        &impatient,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        !matches!(result, Err(ApplyError::Eval(_)) | Ok(_)),
        "the trip's bump must wait out the lock timeout, got {result:?}"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the trip that timed out paused nothing"
    );
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(DEFAULT_COLUMN_DEATH_THRESHOLD),
        "the threshold row's charge committed before the trip"
    );
    holder.rollback().await.expect("end the fence holder");

    // The same row fails again. It's charged already, so only the count
    // left at the threshold can trip the fuse.
    let result = apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "the row still fails, got {result:?}"
    );
    let status = column_status_row(&client, "order_totals", "total")
        .await
        .expect("the row's next failure trips the fuse the timeout lost");
    assert!(status.0, "a threshold trip is a local fuse");
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "the counter resets once the fuse trips"
    );
}

// ---------------------------------------------------------------------
// (b) A paused column's value freezes across subsequent CDC deltas.
// ---------------------------------------------------------------------

#[tokio::test]
async fn paused_column_freezes_instead_of_going_null_or_being_overwritten() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    // A healthy row, computed successfully before anything pauses. The
    // source row exists too: Phase 3 checks each write against it (#344).
    client
        .execute(
            "insert into orders (id, price, tax) values (100, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("insert healthy source row");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "100",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the healthy row");
    let frozen_total: String = client
        .query_one("select total::text from order_totals where id = 100", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(frozen_total, "11.50");

    // Trip the column fuse via `DEFAULT_COLUMN_DEATH_THRESHOLD` distinct bad
    // rows, none of which is id 100.
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the fuse must have tripped"
    );

    // A brand-new, perfectly healthy delta to the *already-written* row: its
    // `total` must stay exactly as it was, not be recomputed, not go null.
    client
        .execute(
            "update orders set price = 999.00, tax = 999.00 where id = 100",
            &[],
        )
        .await
        .expect("update healthy source row");
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        "100",
        "update",
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
        Some(r#"{"price":"999.00","tax":"999.00"}"#),
    )
    .await;
    let seg_seq2 = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq2,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the update, minus the paused column");

    let total_after: Option<String> = client
        .query_one("select total::text from order_totals where id = 100", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(
        total_after,
        Some("11.50".to_string()),
        "a paused column's value must freeze at its last successfully computed value, not go \
         null and not be overwritten by new (even valid) source data"
    );
}

// ---------------------------------------------------------------------
// (c) Cascading a paused column's pause to a dependent (chained) transform.
// ---------------------------------------------------------------------

#[tokio::test]
async fn a_dependent_transforms_column_cascades_to_paused_when_its_upstream_column_pauses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have paused"
    );
    let downstream_status = column_status_row(&client, "order_summaries", "grand_total")
        .await
        .expect("the dependent column must have cascaded to paused");
    assert!(
        !downstream_status.0,
        "a purely cascaded pause is not this column's own local fuse"
    );
    assert!(
        cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the cascade edge must be recorded so resume can later un-cascade it correctly"
    );
}

/// Adds a third 1-1 transform (`order_reports`, reading `order_summaries`'s
/// `grand_total`), so a cascade from `order_totals.total` reaches a child
/// and a grandchild, each fenced on its own source.
async fn seed_order_reports(db: &TestDatabase, orders_def: &TransformDef) {
    let order_summaries_columns = numeric_columns(&["id", "grand_total"]);
    let reports_def = create_definition(
        &db.pool,
        "TRANSFORM order_reports FROM order_summaries SELECT grand_total + 1 AS report_total",
        &order_summaries_columns,
    )
    .await
    .expect("create order_reports definition");
    let pk = source_primary_key(&db.pool, &orders_def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &reports_def.def,
        "public",
        &pk,
        &order_summaries_columns,
        &reports_def.def.source,
    )
    .await
    .expect("create order_reports table");
}

/// Holds the version fence of the source `target`'s definition reads `for
/// share`, as a page in flight does, until the returned transaction ends.
async fn hold_fence_of<'a>(raw: &'a mut Client, target: &str) -> tokio_postgres::Transaction<'a> {
    raw.execute(
        "insert into source_table_versions (source_table, version) \
         select d.source_table, 1 from transform_definitions d \
         where split_part(d.target_table, '.', 2) = $1 \
         on conflict (source_table) do nothing",
        &[&target],
    )
    .await
    .expect("seed the fence");
    let holder = raw.transaction().await.expect("begin the fence holder");
    let held = holder
        .execute(
            "select v.version from source_table_versions v \
             join transform_definitions d on d.source_table = v.source_table \
             where split_part(d.target_table, '.', 2) = $1 for share of v",
            &[&target],
        )
        .await
        .expect("hold the fence");
    assert_eq!(held, 1, "{target}'s fence is held");
    holder
}

/// A pool whose sessions give up on a lock after 100 ms (a setting shorter
/// than Trellis's 30 s cap is kept), so a pause's fence bump behind a held
/// fence fails fast.
fn impatient_pool(db: &TestDatabase) -> trellis::Pool {
    trellis::Pool::new(
        &Config::from_dsn(format!("{} options='-c lock_timeout=100'", db.dsn()))
            .expect("valid dsn"),
    )
    .expect("pool")
}

async fn cascade_pending(client: &Client, transform: &str, column: &str) -> Option<bool> {
    client
        .query_opt(
            "select cascade_pending from column_status \
             where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_status")
        .map(|row| row.get(0))
}

/// Issue #912: each pair of a column pause's cascade bumps its own source's
/// fence in its own transaction, so a writer holding a grandchild's fence
/// fails the walk after the child has paused. The child was paused by the
/// first attempt, and a retried `PAUSE` still walks on through it to the
/// grandchild.
#[tokio::test]
async fn a_retried_pause_completes_a_cascade_that_failed_at_a_grandchild() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;
    seed_order_reports(&db, &orders_def).await;

    let mut page = connect_raw(db.dsn()).await;
    let holder = hold_fence_of(&mut page, "order_reports").await;
    let result = quarantine::pause_column(&impatient_pool(&db), "order_totals", "total").await;
    assert!(
        result.is_err(),
        "the grandchild's fence bump must wait out the lock timeout, got {result:?}"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        Some((true, None)),
        "the pause's own row commits before its cascade"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the child paused before the walk failed"
    );
    assert_eq!(
        column_status_row(&client, "order_reports", "report_total").await,
        None,
        "the walk failed at the grandchild"
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(true),
        "the pause still owes its cascade"
    );
    holder.rollback().await.expect("end the fence holder");

    quarantine::pause_column(&db.pool, "order_totals", "total")
        .await
        .expect("retry the pause");
    let grandchild = column_status_row(&client, "order_reports", "report_total")
        .await
        .expect("the retry walks through the child it paused already to the grandchild");
    assert!(!grandchild.0, "the grandchild's pause is a cascade's");
    assert!(
        cascade_edge_exists(
            &client,
            "order_reports",
            "report_total",
            "order_summaries",
            "grand_total"
        )
        .await,
        "the grandchild's edge is recorded so the resume releases it"
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(false),
        "the finished walk clears the mark"
    );

    // The resume releases the whole chain the two attempts paused.
    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("resume the head");
    assert_eq!(resumed.len(), 3, "resumed {resumed:?}");
}

/// Issue #912: a column fuse trip commits its pause and then cascades. A
/// cascade that fails at the grandchild has no caller to retry it, and the
/// paused column stops failing, so nothing trips the fuse again. The trip
/// leaves its pause marked as owing the cascade, and the capture pass's
/// [`quarantine::complete_pause_cascades`] finishes the walk.
#[tokio::test]
async fn a_fuse_trip_whose_cascade_failed_at_a_grandchild_is_completed_by_the_capture_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;
    seed_order_reports(&db, &orders_def).await;
    let below_threshold: Vec<i64> = (1..DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &below_threshold).await;

    let mut page = connect_raw(db.dsn()).await;
    let holder = hold_fence_of(&mut page, "order_reports").await;
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        &DEFAULT_COLUMN_DEATH_THRESHOLD.to_string(),
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(
        &impatient_pool(&db),
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(result.is_err(), "the drain fails, got {result:?}");
    let root = column_status_row(&client, "order_totals", "total")
        .await
        .expect("the fuse tripped");
    assert!(root.0, "a threshold trip is a local fuse");
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the child paused before the walk failed"
    );
    assert_eq!(
        column_status_row(&client, "order_reports", "report_total").await,
        None,
        "the walk failed at the grandchild"
    );

    // While the writer still holds the grandchild's fence, the capture
    // pass's completion gives up after its own short lock timeout, not the
    // session's 30 s: the maintenance loop that runs it is the only sealer.
    let started = std::time::Instant::now();
    assert_eq!(
        quarantine::complete_pause_cascades(&db.pool)
            .await
            .expect("a walk's failure is logged, not returned"),
        0,
        "no walk finishes while the fence is held"
    );
    let took = started.elapsed();
    assert!(
        took < quarantine::CASCADE_COMPLETION_LOCK_TIMEOUT + Duration::from_secs(5),
        "the completion waited {took:?} behind the held fence"
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(true),
        "the pause still owes its cascade"
    );
    holder.rollback().await.expect("end the fence holder");

    // The staging worker's capture pass, as the maintenance loop runs it.
    trellis::client::reconcile_pass(
        &mut client,
        &db.pool,
        DEFAULT_SCHEMA,
        "wake",
        Duration::from_secs(5),
    )
    .await
    .expect("reconcile pass");
    assert!(
        column_status_row(&client, "order_reports", "report_total")
            .await
            .is_some(),
        "the capture pass's completion reaches the grandchild"
    );
    assert!(
        cascade_edge_exists(
            &client,
            "order_reports",
            "report_total",
            "order_summaries",
            "grand_total"
        )
        .await
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(false)
    );
    assert_eq!(
        quarantine::complete_pause_cascades(&db.pool)
            .await
            .expect("nothing left to finish"),
        0,
        "a finished walk isn't walked again"
    );
}

/// Holds a session advisory lock on a connection of its own, for a
/// [`PauseScope`] arming to block on until [`release_gate`].
async fn take_gate(db: &TestDatabase, key: i64) -> Client {
    let gate = connect_raw(db.dsn()).await;
    gate.execute("select pg_advisory_lock($1)", &[&key])
        .await
        .expect("take the pause lock");
    gate
}

async fn release_gate(gate: &Client, key: i64) {
    gate.execute("select pg_advisory_unlock($1)", &[&key])
        .await
        .expect("release the pause lock");
}

/// Issue #912: a cascade pair checks, after its fence bump, that its
/// upstream column is still paused. A walk frozen between the bump and that
/// check while the upstream column's `RESUME` commits (the two bump
/// different fences: the reader's definition reads the paused column's
/// target) writes nothing, rather than pausing a reader the resume never
/// saw, with an edge from a live column that no later resume would delete.
#[tokio::test]
async fn a_cascade_pair_that_loses_the_race_to_its_upstreams_resume_pauses_nothing() {
    const PAUSE_LOCK: i64 = 912;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(
        PausePoint::AfterCascadeFenceBump,
        "order_summaries",
        PAUSE_LOCK,
    );
    let pool = db.pool.clone();
    let mut pause = tokio::spawn(with_scope(scope, async move {
        quarantine::pause_column(&pool, "order_totals", "total").await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut pause => panic!("the pause finished without reaching the pair: {finished:?}"),
    }

    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("the resume doesn't wait on the frozen pair");
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())],
        "the pair hadn't reached the reader when the resume ran"
    );

    release_gate(&gate, PAUSE_LOCK).await;
    pause
        .await
        .expect("pause task")
        .expect("the walk stops at a resumed upstream");
    assert_eq!(
        column_status_row(&client, "order_summaries", "grand_total").await,
        None,
        "the reader of a resumed column isn't paused"
    );
    assert!(
        !cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "no edge from a live column"
    );
}

/// Issue #912: a walk clears its pause's `cascade_pending` mark only if the
/// row is as the walk found it. Here a second `PAUSE` of the same column
/// commits while the first walk is frozen at the grandchild, and its own
/// walk fails there. The first walk then finishes, and leaves the second
/// pause's mark for the capture pass's completion.
#[tokio::test]
async fn a_walk_leaves_the_mark_of_a_pause_that_committed_after_it_started() {
    const PAUSE_LOCK: i64 = 9120;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;
    seed_order_reports(&db, &orders_def).await;

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(
        PausePoint::AfterCascadeFenceBump,
        "order_reports",
        PAUSE_LOCK,
    );
    let pool = db.pool.clone();
    let mut first = tokio::spawn(with_scope(scope, async move {
        quarantine::pause_column(&pool, "order_totals", "total").await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut first => panic!("the pause finished without reaching the grandchild: {finished:?}"),
    }

    let second = quarantine::pause_column(&impatient_pool(&db), "order_totals", "total").await;
    assert!(
        second.is_err(),
        "the second walk waits out its lock timeout behind the first at the grandchild, got {second:?}"
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(true)
    );

    release_gate(&gate, PAUSE_LOCK).await;
    first
        .await
        .expect("pause task")
        .expect("the first walk finishes");
    assert!(
        column_status_row(&client, "order_reports", "report_total")
            .await
            .is_some(),
        "the first walk reached the grandchild"
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(true),
        "the first walk leaves the mark the second pause wrote"
    );

    assert_eq!(
        quarantine::complete_pause_cascades(&db.pool)
            .await
            .expect("finish the owed cascade"),
        1
    );
    assert_eq!(
        cascade_pending(&client, "order_totals", "total").await,
        Some(false),
        "a walk started after the last pause clears it"
    );
}

// ---------------------------------------------------------------------
// (d) Resume clears the pause, recomputes, and respects an independent
//     reason a dependent has to stay paused.
// ---------------------------------------------------------------------

#[tokio::test]
async fn resume_recomputes_and_does_not_un_pause_a_dependent_with_its_own_reason() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    // Real, valid rows in the actual source table — the malformed CDC
    // images below are purely staged/synthetic (as in every other test in
    // this file), so resume's recompute (which reads the live table) finds
    // clean data once the fuse trips and is resumed.
    client
        .batch_execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.00), (2, 20.00, 2.00), (3, 30.00, 3.00), \
             (4, 40.00, 4.00), (5, 50.00, 5.00)",
        )
        .await
        .expect("seed valid orders rows");

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some()
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some()
    );

    // Give the dependent its *own*, independent reason to stay paused —
    // reached past the mechanism directly, the same convention
    // `tests/quarantine.rs` uses for seeding counters/markers by hand.
    client
        .execute(
            "update column_status set local_fuse = true \
             where transform_table = 'order_summaries' and column_name = 'grand_total'",
            &[],
        )
        .await
        .expect("mark the dependent as also independently paused");

    // The batch that tripped the fuse rolled back entirely (it never
    // resolved), so `order_totals` has no rows for ids 1-5 yet — retry them
    // now that `total` is paused/excluded: this batch succeeds (nothing left
    // to error on) and creates the bare rows resume's recompute will then
    // fill in, exactly like a real drain loop retrying a previously-failing
    // batch once the column that broke it is out of the way.
    let table = active_segment_table(&client).await;
    for id in 1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64 {
        insert_cdc_row(
            &client,
            &table,
            "orders",
            &id.to_string(),
            "insert",
            None,
            Some(&format!(
                r#"{{"price":"{}.00","tax":"{}.00"}}"#,
                id * 10,
                id
            )),
        )
        .await;
    }
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain now that the broken column is excluded");

    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())],
        "the dependent must NOT be auto-resumed — it has its own independent reason"
    );

    // #625 F8b: the resume registers a field build of the column, so the
    // transform reports `backfilling` (it keeps applying) until the build's
    // chunks, driven here as the drain workers would, take it back to
    // `live`. Nothing is parked for a catch-up.
    let status_of = async |client: &tokio_postgres::Client| -> String {
        client
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = 'order_totals'",
                &[],
            )
            .await
            .expect("read order_totals' status")
            .get(0)
    };
    assert_eq!(status_of(&client).await, "backfilling");
    let parked: i64 = client
        .query_one(
            "select count(*) from pending_backfill where table_name = $1",
            &[&format!("{DEFAULT_SCHEMA}.orders")],
        )
        .await
        .expect("count pending_backfill")
        .get(0);
    assert_eq!(parked, 0, "a column resume parks no catch-up");
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(status_of(&client).await, "live");

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the resumed column itself must no longer be paused"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the dependent must remain paused: its own local_fuse reason still holds"
    );
    assert!(
        !cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the specific cascade edge from the now-resumed upstream must be gone"
    );

    for id in 1..=5i32 {
        let total: String = client
            .query_one("select total::text from order_totals where id = $1", &[&id])
            .await
            .unwrap_or_else(|e| panic!("read order_totals for id {id}: {e}"))
            .get(0);
        let expected = match id {
            1 => "11.00",
            2 => "22.00",
            3 => "33.00",
            4 => "44.00",
            5 => "55.00",
            _ => unreachable!(),
        };
        assert_eq!(
            total, expected,
            "resume must have recomputed against the real (valid) source row for id {id}"
        );
    }
}

/// Reviewer follow-up to issue #74 (epic #78's own whole-branch review): the
/// quarantine-recompute counterpart to `apply.rs`'s
/// `explicitly_qualified_target_receives_a_live_cdc_write` and
/// `apply_aggregate.rs`'s
/// `explicitly_qualified_aggregate_target_receives_a_live_cdc_write` — a
/// definition installed with an explicit non-default *target* schema (issue
/// #76's grammar) used to fail `resume_column`'s recompute outright:
/// `recompute_column`'s own `UPDATE` (`staging::quarantine`) bound
/// `def.def.target` — always bare — straight into `quote_ident`, instead of
/// `Definition::target_table`'s qualified identity, so the write tried
/// bare, unqualified `order_totals`, not on this connection's pinned
/// `search_path`, even though `custom.order_totals` (the real target) does
/// exist and already carries the bare (non-`total`) columns a prior,
/// successful drain wrote for it.
#[tokio::test]
async fn resume_column_recomputes_an_explicitly_qualified_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table orders (id integer primary key, price numeric, tax numeric)",
        )
        .await
        .expect("seed source table and the custom schema");

    const DEF_TEXT: &str = "TRANSFORM custom.order_totals FROM orders SELECT price + tax AS total";
    let def = trellis::defs::parse(DEF_TEXT).expect("parse the explicitly-qualified target");
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "custom", &pk, &source_columns, &def.source)
        .await
        .expect("materialize custom.order_totals ahead of create_definition");
    create_definition(&db.pool, DEF_TEXT, &source_columns)
        .await
        .expect("create definition against the explicitly-qualified target");

    // Trip the column fuse exactly like this file's other tests — the
    // malformed images below are purely staged/synthetic and never touch
    // the physical `orders` table.
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the fuse must have tripped"
    );

    // A real, valid row in the physical source table, plus a bare (`total`
    // excluded) target row for it — standing in for what a drain retried
    // after the column paused would have already written, the state
    // `resume_column`'s recompute is meant to fill in.
    client
        .execute(
            "insert into orders (id, price, tax) values (200, 10.00, 5.00)",
            &[],
        )
        .await
        .expect("seed a valid source row");
    client
        .execute("insert into custom.order_totals (id) values (200)", &[])
        .await
        .expect("pre-populate a bare target row for the row resume must fill in");

    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect(
            "resume_column must recompute against custom.order_totals — a bare, \
             unqualified UPDATE would have raised relation \"order_totals\" does \
             not exist instead",
        );
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())]
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the column must no longer be paused after a successful resume"
    );
    trellis::staging::build::settle_builds(&db.pool).await;

    let total: String = client
        .query_one(
            "select total::text from custom.order_totals where id = 200",
            &[],
        )
        .await
        .expect("read custom.order_totals")
        .get(0);
    assert_eq!(
        total, "15.00",
        "resume must have recomputed custom.order_totals against the real source row"
    );

    let bare_decoy_exists: bool = client
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_name = 'order_totals' and table_schema <> 'custom')",
            &[],
        )
        .await
        .expect("check for a same-named decoy outside the custom schema")
        .get(0);
    assert!(
        !bare_decoy_exists,
        "the recompute must land in custom.order_totals, never create/touch a \
         same-named table in some other schema on the connection's search_path"
    );
}

// ---------------------------------------------------------------------
// (e) The three read methods and the resume method, on both `Trellis` and
//     `BlockingTrellis`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn trellis_exposes_the_read_and_resume_methods() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let list = trellis.quarantined().await.expect("quarantined");
    assert!(
        list.iter().any(|entry| entry.target
            == trellis::app::QuarantineTarget::Column(
                "order_totals".to_string(),
                "total".to_string()
            )),
        "the paused column must appear in the flat list: {list:?}"
    );

    let status = trellis
        .quarantine_status("order_totals.total")
        .await
        .expect("quarantine_status");
    assert_eq!(
        status.state,
        trellis::app::QuarantineState::Paused,
        "the column address must report paused"
    );
    assert!(status.paused_at.is_some());

    let whole_transform_status = trellis
        .quarantine_status("order_totals")
        .await
        .expect("quarantine_status for the bare transform");
    assert_eq!(
        whole_transform_status.state,
        trellis::app::QuarantineState::Live,
        "the transform's own lifecycle status is untouched by a column pause"
    );

    let sample = trellis
        .sample_quarantined("order_totals.total", None, 10)
        .await
        .expect("sample_quarantined");
    assert_eq!(
        sample.len(),
        DEFAULT_COLUMN_DEATH_THRESHOLD as usize,
        "every distinct poisoned row that contributed to the trip must be sampleable: {sample:?}"
    );

    let resumed = trellis
        .apply("RESUME TRANSFORM order_totals.total")
        .await
        .expect("resume a paused column");
    assert!(
        matches!(&resumed, trellis::Applied::Resumed { columns }
            if columns == &vec![("order_totals".to_string(), "total".to_string())]),
        "a column resume reports the pairs it resumed, got {resumed:?}"
    );

    let status_after = trellis
        .quarantine_status("order_totals.total")
        .await
        .expect("quarantine_status after resume");
    assert_eq!(status_after.state, trellis::app::QuarantineState::Live);

    // Issue #227 replaced `resume_column`'s "a bare address is caller error"
    // rejection with a grammar that makes the two spellings *different
    // statements*: `RESUME TRANSFORM <t>.<c>` resumes one column (above),
    // while `RESUME TRANSFORM <t>` is the whole-transform rebuild. So the bare
    // form is no longer an addressing error at all — it is the other
    // operation, judged against that operation's own precondition, which this
    // still-live transform doesn't meet (only a frozen definition can be
    // resumed).
    let bare = trellis.apply("RESUME TRANSFORM order_totals").await;
    assert!(
        matches!(
            bare,
            Err(trellis::TrellisError::Apply(
                ApplyError::TransformNotPaused { .. }
            ))
        ),
        "a bare address is the whole-transform resume, refused here because the transform \
         itself is live — not an ambiguous-address error, got {bare:?}"
    );
}

#[test]
fn blocking_trellis_exposes_the_read_and_resume_methods() {
    let cluster = TestCluster::start();

    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_isolated_database());
    setup_runtime.block_on(async {
        let mut client = connect_raw(db.dsn()).await;
        seed_order_totals(&db, &client).await;
        let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
        stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    });
    drop(setup_runtime);

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");

    let list = trellis.quarantined().expect("quarantined (sync)");
    assert!(
        list.iter().any(|entry| entry.target
            == trellis::app::QuarantineTarget::Column(
                "order_totals".to_string(),
                "total".to_string()
            )),
        "the paused column must appear in the flat list: {list:?}"
    );

    let status = trellis
        .quarantine_status("order_totals.total")
        .expect("quarantine_status (sync)");
    assert_eq!(status.state, trellis::app::QuarantineState::Paused);

    let sample = trellis
        .sample_quarantined("order_totals.total", None, 10)
        .expect("sample_quarantined (sync)");
    assert_eq!(sample.len(), DEFAULT_COLUMN_DEATH_THRESHOLD as usize);

    let resumed = trellis
        .apply("RESUME TRANSFORM order_totals.total")
        .expect("resume a paused column (sync)");
    assert!(
        matches!(&resumed, trellis::Applied::Resumed { columns }
            if columns == &vec![("order_totals".to_string(), "total".to_string())]),
        "a column resume reports the pairs it resumed, got {resumed:?}"
    );

    let status_after = trellis
        .quarantine_status("order_totals.total")
        .expect("quarantine_status after resume (sync)");
    assert_eq!(status_after.state, trellis::app::QuarantineState::Live);

    trellis.shutdown().expect("shutdown (sync)");
}

// ---------------------------------------------------------------------
// (f) The existing row-level/transform-wide fuse still works unchanged.
// ---------------------------------------------------------------------

/// A targeted regression check living alongside the new feature's own
/// tests, on top of `trellis/tests/quarantine.rs`'s full existing suite
/// (unmodified, still green): a single stubborn key retried past the
/// row-level threshold must still evict via `key_deaths`/`poison` exactly as
/// before, and — the specific new-feature interaction this test is really
/// about — must NOT also trip the column fuse, since it is one row failing
/// repeatedly, not a breadth of distinct rows (see `column_failures`'
/// migration comment / `charge_column_failure`'s doc comment on why the
/// column fuse counts distinct rows, not attempts).
#[tokio::test]
async fn an_existing_row_level_fuse_scenario_is_unaffected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .batch_execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
        )
        .await
        .expect("seed orders rows");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let outcome = loop {
        match apply::drain_once(
            &db.pool,
            seg_seq,
            "worker",
            1,
            "trellis_column_quarantine_test",
            &StagedWatermark::saturated(),
        )
        .await
        {
            Ok(Some(outcome)) => break outcome,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => continue,
        }
    };
    assert_eq!(outcome.keys_written, 1, "only the survivor, key 2, writes");

    let orders = qualify_fixture_table("orders");
    let poisoned: bool = client
        .query_one(
            "select exists(select 1 from poison where src_table = $1 and key = '1')",
            &[&orders],
        )
        .await
        .expect("read poison")
        .get(0);
    assert!(poisoned, "the row-level fuse must still evict as before");

    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(1),
        "one repeatedly-retried key must charge the column counter exactly once, not once per \
         attempt — it never crosses the column fuse's own threshold alone"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "a single stubborn row must never trip the column fuse on its own"
    );
}

// ---------------------------------------------------------------------
// (g) Regression: a re-executed durable backfill chunk must leave a paused
//     column untouched (must-fix 1).
// ---------------------------------------------------------------------

/// A durable backfill chunk (`defs::chunk_queue`, ADR-0007's amendment) is
/// claimable and crash-recoverable: a chunk whose worker wrote it and died
/// before finishing it is reclaimed and re-executed by another worker (see
/// `trellis/tests/defs_backfill_chunk_queue.rs`'s own reclaim-and-redo
/// tests), and `defs::backfill::write_one_to_one_range`/
/// `execute_one_to_one_chunk` had zero awareness of `column_status`: a
/// re-executed chunk's `ON CONFLICT DO UPDATE` blindly overwrote *every*
/// field, including one live CDC had since paused, silently undoing the
/// freeze. This drives the chunk-queue execution path directly (in lieu of
/// orchestrating a real crash) to prove the fix: a
/// paused column's value must survive a re-executed chunk write untouched,
/// while a sibling, non-paused column in the very same row must still pick
/// up the re-executed chunk's freshly computed value.
///
/// The source is made to read as another definition's target
/// (`markers::feed_from_a_test_definition`; no staging worker runs here):
/// #625 F8a gives a plain 1-1 over a captured table to the Re-derive build,
/// whose chunk commits with its done mark and so never re-executes, and the
/// chunked build this test exercises survives for a seam-fed source.
#[tokio::test]
async fn a_reexecuted_backfill_chunk_leaves_a_paused_column_untouched() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric, b numeric); \
             insert into s (id, a, b) values (1, 10, 100), (2, 20, 200)",
        )
        .await
        .expect("seed source");
    trellis::intake::markers::feed_from_a_test_definition(&client, &format!("{DEFAULT_SCHEMA}.s"))
        .await
        .expect("make s another definition's target");

    let cols = numeric_columns(&["a", "b"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x, b + b AS y",
        &cols,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(
        def.status,
        TransformStatus::WaitingToBackfill,
        "registration only records the definition (ADR-0016)"
    );
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        status_named(&client, def.def.target.as_str()).await,
        "backfilling",
        "a plain 1-1 definition sits at backfilling until its chunk is claimed and finished"
    );

    let claimed = chunk_queue::claim_chunks(&client, "worker-1", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");

    // The first worker writes the chunk and dies before finishing it.
    chunk_queue::run_claimed_chunk(
        &db.pool,
        &claimed[0],
        "worker-1",
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
    .await
    .expect("run_claimed_chunk (initial build)");

    let initial = client
        .query_one("select x::text, y::text from t where id = 1", &[])
        .await
        .expect("read t after initial build");
    let (x_initial, y_initial): (String, String) = (initial.get(0), initial.get(1));
    assert_eq!(x_initial, "20");
    assert_eq!(y_initial, "200");

    // Simulate live CDC having since paused column `x` — reached past the
    // mechanism, inserted directly, the same convention the rest of this
    // file uses for whichever half of a scenario isn't the mechanism under
    // test (here, *how* the pause happened is irrelevant; only its effect
    // on a re-executed chunk is).
    client
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('t', 'x', 'synthetic pause for backfill freeze test', true)",
            &[],
        )
        .await
        .expect("seed column_status directly");

    // Mutate the source row so a naive re-execution would compute different
    // values for *both* columns — `x` must stay frozen; `y` must not.
    client
        .execute("update s set a = 999, b = 500 where id = 1", &[])
        .await
        .expect("mutate source row 1");

    // The dead worker's claim is reclaimed (a zero TTL makes it stale), and
    // a different worker claims the chunk and re-executes it.
    chunk_queue::reclaim_stale_chunks(&mut client, Duration::ZERO)
        .await
        .expect("reclaim the dead worker's claim");
    let reclaimed = chunk_queue::claim_chunks(&client, "worker-2", 10)
        .await
        .expect("claim_chunks after the reclaim");
    assert_eq!(reclaimed.len(), 1, "the reclaimed chunk is claimable again");
    chunk_queue::run_claimed_chunk(
        &db.pool,
        &reclaimed[0],
        "worker-2",
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
    .await
    .expect("run_claimed_chunk (re-executed after the reclaim)");

    let after = client
        .query_one("select x::text, y::text from t where id = 1", &[])
        .await
        .expect("read t after re-executed chunk");
    let (x_after, y_after): (String, String) = (after.get(0), after.get(1));
    assert_eq!(
        x_after, x_initial,
        "a paused column's value must be untouched by a re-executed backfill chunk"
    );
    assert_eq!(
        y_after, "1000",
        "a non-paused column in the very same row must still be updated by the re-executed chunk"
    );
}

// ---------------------------------------------------------------------
// (h) Regression: cascade must never reach a KeySpace::Aggregate transform
//     (must-fix 2).
// ---------------------------------------------------------------------

/// `column_dependents` used to scan every `transform_definitions` row with
/// no `KeySpace` filter, so a downstream aggregate transform whose field
/// happens to read a just-paused upstream 1-1 column would get a wrongly
/// cascaded `column_status` row — one the aggregate apply path
/// has no notion of and would never clear, and one
/// `resume_column`'s cascade walk would later mishandle via the 1-1-only
/// `recompute_column`. Proves the fix: pausing the upstream column must
/// never create a `column_status` row for the aggregate, and the aggregate's
/// own write path must keep functioning normally afterward.
#[tokio::test]
async fn pausing_an_upstream_column_never_cascades_into_a_downstream_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    // A downstream aggregate transform reading `order_totals.total` — the
    // scope-cut this fix enforces: column-level pause/cascade/resume never
    // touches an aggregate transform.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let stats_def = create_definition(
        &db.pool,
        "TRANSFORM order_stats FROM order_totals GROUP BY id SELECT id AS id, \
         SUM(total) AS total_sum",
        &order_totals_columns,
    )
    .await
    .expect("create order_stats definition");
    create_aggregate_target_table(&db.pool, &stats_def.def, "public", &order_totals_columns)
        .await
        .expect("create order_stats table");

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have paused"
    );
    assert_eq!(
        column_status_row(&client, "order_stats", "total_sum").await,
        None,
        "an aggregate transform must never get a column_status row via cascade — aggregates are \
         out of scope for column-level pause/cascade/resume"
    );

    // A subsequent, healthy batch flowing all the way through to the
    // aggregate: the write path must still complete normally (no error, no
    // wedge) — proving the (correctly withheld) cascade never left the
    // aggregate's own status or write path in a broken state.
    client
        .batch_execute("insert into orders (id, price, tax) values (999, 5.00, 1.00)")
        .await
        .expect("seed a healthy order row");
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        "999",
        "insert",
        None,
        Some(r#"{"price":"5.00","tax":"1.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("a healthy batch must still drain normally through order_totals");

    // The write to `order_totals` above stages a downstream `Recompute`
    // marker for `order_stats` into the (still-open) active ring segment —
    // seal and drain once more to actually run the aggregate's own write
    // path against it.
    let seg_seq2 = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq2,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect(
        "the aggregate's own batch must still drain normally, unaffected by the upstream \
             (correctly non-cascaded) pause",
    );

    let stats_row = client
        .query_opt(
            "select total_sum::text from order_stats where id = 999",
            &[],
        )
        .await
        .expect("read order_stats");
    assert!(
        stats_row.is_some(),
        "the aggregate transform must still write a row normally, unaffected by the upstream \
         (correctly non-cascaded) pause"
    );
}

// ---------------------------------------------------------------------
// (i) Regression: resuming a column must not silently no-op because a
//     sibling column on the same transform is also paused (must-fix 3).
// ---------------------------------------------------------------------

/// `resume_column`'s `recompute_column` used to call the un-excluding
/// `eval::evaluate_with_relationships`, so a still-paused sibling column
/// (`busted` below, whose formula throws for every row given the malformed —
/// but genuinely persisted — source data) made the *whole* per-row
/// evaluation fail, and the resumed column (`doubled`) silently never got
/// recomputed for any row, even though its own formula is perfectly healthy.
/// Proves the fix: excluding the still-paused sibling from evaluation lets
/// the resumed column recompute correctly for every row regardless.
#[tokio::test]
async fn resume_recomputes_correctly_even_when_a_sibling_column_still_throws() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // `tax` is an `integer` column holding `integer`'s largest value, so
    // `busted`'s formula overflows (`22003`, as Postgres's `tax + tax` would)
    // on every row, for every recompute attempt, indefinitely. The
    // definition is one define accepts, so the resume's re-validation
    // (#708) passes; only the data makes the sibling throw.
    client
        .batch_execute(
            "create table calc_src (id integer primary key, price numeric, tax integer); \
             insert into calc_src (id, price, tax) values \
             (1, 10, 2147483647), (2, 20, 2147483647), (3, 30, 2147483647)",
        )
        .await
        .expect("seed source table");

    let source_columns: HashMap<String, ValueType> = [
        ("id", ValueType::Integer(trellis::integer::IntWidth::Int4)),
        ("price", ValueType::Numeric),
        ("tax", ValueType::Integer(trellis::integer::IntWidth::Int4)),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect();
    create_definition(
        &db.pool,
        "TRANSFORM calc FROM calc_src SELECT price + price AS doubled, tax + tax AS busted",
        &source_columns,
    )
    .await
    .expect("create calc definition");
    let calc_def = TransformDef {
        target: "calc".to_string(),
        source: "calc_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "doubled".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("price".to_string())),
                    rhs: Box::new(Expr::Column("price".to_string())),
                },
            },
            FieldDef {
                name: "busted".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("tax".to_string())),
                    rhs: Box::new(Expr::Column("tax".to_string())),
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let pk = source_primary_key(&db.pool, "calc_src")
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &calc_def,
        "public",
        &pk,
        &source_columns,
        &calc_def.source,
    )
    .await
    .expect("create calc table");

    // Seed the target with sentinel values distinct from any real
    // recomputed result, so a successful recompute is unambiguous.
    client
        .batch_execute(
            "insert into calc (id, doubled, busted) values \
             (1, -999, -999), (2, -999, -999), (3, -999, -999)",
        )
        .await
        .expect("seed sentinel target rows");

    // Both columns independently paused — reached past the mechanism,
    // inserted directly (same convention as this file's other tests).
    client
        .batch_execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('calc', 'doubled', 'synthetic pause A', true), \
                    ('calc', 'busted', 'synthetic pause B (always throws)', true)",
        )
        .await
        .expect("seed column_status for both columns");

    let resumed = quarantine::resume_column(&db.pool, "calc", "doubled")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("calc".to_string(), "doubled".to_string())],
        "only the resumed column itself, nothing cascaded"
    );

    assert_eq!(
        column_status_row(&client, "calc", "doubled").await,
        None,
        "the resumed column must no longer be paused"
    );
    assert!(
        column_status_row(&client, "calc", "busted").await.is_some(),
        "the still-broken sibling must remain paused — resuming `doubled` must not touch it"
    );
    trellis::staging::build::settle_builds(&db.pool).await;

    for (id, expected_doubled) in [(1, "20"), (2, "40"), (3, "60")] {
        let row = client
            .query_one(
                "select doubled::text, busted::text from calc where id = $1",
                &[&id],
            )
            .await
            .unwrap_or_else(|e| panic!("read calc for id {id}: {e}"));
        let doubled: String = row.get(0);
        let busted: String = row.get(1);
        assert_eq!(
            doubled, expected_doubled,
            "the resumed column must be correctly recomputed for id {id}, even though the \
             still-paused sibling's formula throws on every row"
        );
        assert_eq!(
            busted, "-999",
            "the still-paused sibling's own value must be left untouched by this resume"
        );
    }
}

/// Issue #377: `recompute_column`'s write-back matches each chunk of keys
/// against the target's own primary-key columns (an indexed keyset join)
/// rather than against the computed key-contract text. Resuming over a
/// composite key, across more than one write-back chunk, with key parts that
/// hold the key contract's own separator and escape characters, must still
/// land every recomputed value on exactly its own row. A resume is a field
/// build (#625 F8b): it rewrites the column of the rows the target has, and
/// writes no row the target lacks (Apply and a rebuild own a row's
/// existence).
#[tokio::test]
async fn resume_recomputes_every_row_of_a_composite_key_target_across_chunks() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table comp_src (region text, id integer, price numeric, \
                                    primary key (region, id)); \
             insert into comp_src (region, id, price) \
             select 'r' || (g % 3), g, g from generate_series(1, 2500) g; \
             insert into comp_src (region, id, price) values \
             ('a' || chr(31) || 'b', 1, 7), ('c' || chr(30) || 'd', 1, 8)",
        )
        .await
        .expect("seed source table");

    let source_columns = numeric_columns(&["id", "price"]);
    create_definition(
        &db.pool,
        "TRANSFORM comp FROM comp_src SELECT price + price AS doubled",
        &source_columns,
    )
    .await
    .expect("create comp definition");
    let comp_def = TransformDef {
        target: "comp".to_string(),
        source: "comp_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "doubled".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("price".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let pk = source_primary_key(&db.pool, "comp_src")
        .await
        .expect("introspect source primary key");
    assert_eq!(pk.len(), 2, "the fixture must exercise a composite key");
    create_target_table(
        &db.pool,
        &comp_def,
        "public",
        &pk,
        &source_columns,
        &comp_def.source,
    )
    .await
    .expect("create comp table");

    // Sentinel values for every source row but one, which the target
    // deliberately lacks.
    client
        .batch_execute(
            "insert into comp (region, id, doubled) \
             select region, id, -999 from comp_src where not (region = 'r1' and id = 7); \
             insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('comp', 'doubled', 'synthetic pause', true)",
        )
        .await
        .expect("seed sentinel target rows and the pause");

    quarantine::resume_column(&db.pool, "comp", "doubled")
        .await
        .expect("resume_column");
    trellis::staging::build::settle_builds(&db.pool).await;

    let row = client
        .query_one(
            "select count(*), \
                    count(*) filter (where t.doubled is distinct from s.price * 2), \
                    count(*) filter (where s.id is null) \
             from comp t left join comp_src s using (region, id)",
            &[],
        )
        .await
        .expect("compare target to source");
    let (total, wrong, orphans): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    assert_eq!(
        total, 2501,
        "the field build writes no row the target lacks"
    );
    assert_eq!(
        wrong, 0,
        "every existing row must hold its own recomputed value"
    );
    assert_eq!(orphans, 0);
}

// ---------------------------------------------------------------------
// (j) Regression: ambiguous field-name attribution must fall back to no
//     column-level attribution (should-fix 4).
// ---------------------------------------------------------------------

/// Two sibling `KeySpace::OneToOne` transforms on the same source table,
/// both declaring a field named `total` — only `sib_b`'s formula is
/// actually broken (it reads `qty`, staged as unparseable below; `sib_a`
/// only reads `price`/`tax`, both fine). `attribute_column_failure` used to
/// resolve the ambiguity by picking whichever candidate
/// `transforms_for_source` happened to return first (lowest id), a
/// deterministic misattribution: it could freeze `sib_a`'s perfectly
/// healthy `total` while `sib_b`'s actually-broken one never accumulates a
/// `column_status` entry at all. Proves the fix: neither sibling gets a
/// column-level attribution, while the pre-existing row-level/transform-wide
/// fuse still evicts the stubborn key exactly as it did before this
/// feature.
#[tokio::test]
async fn ambiguous_field_name_attribution_falls_back_to_no_column_level_attribution() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table shared_src (id integer primary key, price numeric, tax numeric, \
             qty numeric)",
        )
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax", "qty"]);

    create_definition(
        &db.pool,
        "TRANSFORM sib_a FROM shared_src SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create sib_a definition");
    create_definition(
        &db.pool,
        "TRANSFORM sib_b FROM shared_src SELECT qty + qty AS total",
        &source_columns,
    )
    .await
    .expect("create sib_b definition");

    let pk = source_primary_key(&db.pool, "shared_src")
        .await
        .expect("introspect source primary key");
    let def_a = TransformDef {
        target: "sib_a".to_string(),
        source: "shared_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    create_target_table(
        &db.pool,
        &def_a,
        "public",
        &pk,
        &source_columns,
        &def_a.source,
    )
    .await
    .expect("create sib_a table");
    let def_b = TransformDef {
        target: "sib_b".to_string(),
        source: "shared_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("qty".to_string())),
                rhs: Box::new(Expr::Column("qty".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    create_target_table(
        &db.pool,
        &def_b,
        "public",
        &pk,
        &source_columns,
        &def_b.source,
    )
    .await
    .expect("create sib_b table");

    // `DEFAULT_COLUMN_DEATH_THRESHOLD` distinct bad rows, all in one batch —
    // the same "breadth of distinct rows, not one row retried" shape
    // `stage_bad_orders`/`column_fuse_trips_only_once_the_threshold_is_crossed`
    // use elsewhere in this file, chosen deliberately here too: it's enough
    // volume to actually cross a column fuse's threshold, so this test would
    // catch the old deterministic-misattribution bug (which would have
    // tripped `sib_a`'s fuse — the healthy sibling, since it's the
    // lower-`id` candidate `transforms_for_source` returns first) rather
    // than vacuously passing because nothing ever reached threshold.
    let table = active_segment_table(&client).await;
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    for id in &bad_ids {
        insert_cdc_row(
            &client,
            &table,
            "shared_src",
            &id.to_string(),
            "insert",
            None,
            Some(r#"{"price":"10.00","tax":"1.50","qty":"not-a-number"}"#),
        )
        .await;
    }
    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "the malformed qty must still surface as an evaluator failure, got {result:?}"
    );

    assert_eq!(
        column_status_row(&client, "sib_a", "total").await,
        None,
        "the healthy sibling must never be attributed to, even though its field name matches \
         the actually-broken sibling's"
    );
    assert_eq!(
        column_status_row(&client, "sib_b", "total").await,
        None,
        "the actually-broken sibling must also get no column-level attribution — ambiguous \
         attribution must fall back to no attribution at all, not a guess"
    );

    // The pre-existing row-level fuse's own bookkeeping must be completely
    // unaffected by the ambiguity fallback: every distinct bad row is still
    // charged toward its own key-level counter exactly as it would be
    // without this feature (`tests/quarantine.rs`'s own fuse, untouched).
    let shared_src = qualify_fixture_table("shared_src");
    for id in &bad_ids {
        let deaths: Option<i32> = client
            .query_opt(
                "select deaths from key_deaths where src_table = $1 and key = $2",
                &[&shared_src, &id.to_string()],
            )
            .await
            .expect("read key_deaths")
            .map(|row| row.get(0));
        assert_eq!(
            deaths,
            Some(1),
            "row-level key_deaths bookkeeping for id {id} must proceed normally, unaffected by \
             the ambiguity fallback"
        );
    }
}

// ---------------------------------------------------------------------
// (k) Regression: `resume_column` must refuse a column whose definition
//     isn't live yet (the mid-backfill cascade bug a final holistic review
//     agent found).
// ---------------------------------------------------------------------

/// A pause reaching `(transform, column)` while `transform` is still
/// `Backfilling` — most concretely via this branch's cascade pause
/// (`defs::catalog::column_dependents`, unlike the `status = 'live'`
/// filtered lookups ordinary CDC apply uses, does *not* require the
/// downstream dependent to be live before cascading a pause onto it) —
/// must not be resumable. `recompute_column` takes exactly one snapshot of
/// the *source* table and writes back only via `update ... where pk = $2`
/// (no `insert`/upsert fallback); `column_status` for the paused column
/// stays present for the entire duration of that recompute and is only
/// deleted afterward. If the target definition is still mid-backfill, its
/// own `backfill_chunks` queue can be actively inserting brand-new rows
/// into the target the whole time `paused_columns_for` (backfill) and
/// `compute` (live CDC apply) are excluding this column from — a row
/// inserted after `recompute_column`'s snapshot was taken is never in its
/// `rows_by_pk` map and is never revisited once `column_status` is cleared,
/// permanently stranding that row's column even though `resume_column`
/// reports success. Proves the fix instead: `resume_column` returns
/// [`ApplyError::DefinitionNotLive`] and leaves everything untouched.
#[tokio::test]
async fn resume_column_refuses_a_column_on_a_not_yet_live_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table stuck_src (id bigint primary key, a numeric); \
             insert into stuck_src (id, a) values (1, 10), (2, 20), (3, 30)",
        )
        .await
        .expect("seed source table");

    let cols = numeric_columns(&["a"]);
    // Nothing in this test starts the definition's build, so it stays
    // `waiting_to_backfill`: it doesn't apply yet. (A plain 1-1 under its
    // Re-derive build does apply, and a column resume joins its build
    // instead, #625 F8b.)
    let def = install_definition(
        &db.pool,
        "TRANSFORM stuck FROM stuck_src SELECT a + a AS doubled",
        &cols,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(def.status, TransformStatus::WaitingToBackfill);

    // Simulate a pause landing on `stuck.doubled` while it's mid-backfill —
    // reached past the mechanism, inserted directly (this file's own
    // convention throughout; a real cascade would populate the same rows
    // via `cascade_pause`). Seed `column_deaths` too, so this test can also
    // prove the gate is all-or-nothing rather than partially cleaning up.
    client
        .batch_execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('stuck', 'doubled', 'paused because upstream column X is paused', false); \
             insert into column_deaths (transform_table, column_name, deaths) \
             values ('stuck', 'doubled', 3)",
        )
        .await
        .expect("seed column_status/column_deaths for the cascaded pause");

    let result = quarantine::resume_column(&db.pool, "stuck", "doubled").await;
    assert!(
        matches!(
            &result,
            Err(ApplyError::DefinitionNotLive { transform }) if transform == "stuck"
        ),
        "resuming a column on a not-yet-live definition must be refused, got {result:?}"
    );

    assert_eq!(
        column_status_row(&client, "stuck", "doubled").await,
        Some((
            false,
            Some("paused because upstream column X is paused".to_string())
        )),
        "a refused resume must leave column_status completely untouched"
    );
    assert_eq!(
        column_deaths_count(&client, "stuck", "doubled").await,
        Some(3),
        "a refused resume must be all-or-nothing: column_deaths must not be cleared either"
    );
}

/// The end-to-end version of the same bug, through the *real*
/// `trip_column_fuse` -> `cascade_pause` path rather than a hand-seeded
/// `column_status` row: `order_totals.total` (live) pauses and cascades onto
/// `order_summaries.grand_total`, whose definition is genuinely stuck
/// `Backfilling` behind its own never-drained `backfill_chunks` queue
/// (`install_definition`, same determinism as the test above). Proves the
/// cascade-queue's per-pair check (the second gate inside `resume_column`'s
/// `while` loop, distinct from the initial-pair gate the test above
/// exercises): resuming the upstream column must still succeed and commit,
/// while the downstream pair it cascaded onto is left exactly as it was —
/// still paused, not resumed, and the call does not error out just because
/// one pair deep in the queue isn't live yet.
#[tokio::test]
async fn resume_column_leaves_a_cascaded_not_yet_live_dependent_paused_without_erroring() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    // Real, valid source rows, drained so `order_totals` has actual target
    // rows for `install_definition` (below) to enumerate chunk work over —
    // an empty source table would enqueue zero chunks and `order_summaries`
    // would complete its (trivial) backfill synchronously instead of
    // sticking in `Backfilling` the way this test needs.
    client
        .batch_execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.00), (2, 20.00, 2.00), (3, 30.00, 3.00)",
        )
        .await
        .expect("seed valid orders rows");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "3",
        "insert",
        None,
        Some(r#"{"price":"30.00","tax":"3.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the seed rows into order_totals");

    // `order_summaries`, reading straight from `order_totals` (mirrors
    // `seed_order_summaries`), but installed via `install_definition` so it
    // enumerates real chunk work and is left `Backfilling` — nothing here
    // ever drains that queue, the same determinism
    // `resume_column_refuses_a_column_on_a_not_yet_live_definition` (above)
    // and `trellis/tests/defs_backfill_chunk_queue.rs` rely on.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    // The old chunked build, which isn't applying, is what this test pins:
    // a Re-derive build applies, and a resume resumes it (#625 F6).
    trellis::intake::markers::hold_out_of_the_rederive_build(&client, "public.order_totals")
        .await
        .expect("hold order_totals' readers out of the Re-derive build");
    let summary_def = install_definition(
        &db.pool,
        "TRANSFORM order_summaries FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(summary_def.status, TransformStatus::WaitingToBackfill);
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        status_named(&client, summary_def.def.target.as_str()).await,
        "backfilling",
        "nothing drains the chunk queue in this test, so order_summaries must still be \
         backfilling"
    );

    // Trip `order_totals.total`'s own fuse (distinct ids from the 3 seeded
    // above, so as not to disturb them) — the real cascade path,
    // `defs::catalog::column_dependents`, must reach `order_summaries` here
    // exactly as it does in `a_dependent_transforms_column_cascades_to_paused_when_its_upstream_column_pauses`,
    // regardless of `order_summaries` still being `Backfilling`.
    let bad_ids: Vec<i64> = (101..=100 + DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have tripped its own fuse"
    );
    let downstream_status = column_status_row(&client, "order_summaries", "grand_total")
        .await
        .expect(
            "the cascade must reach order_summaries even though its definition is still \
             backfilling",
        );
    assert!(
        !downstream_status.0,
        "a purely cascaded pause is not order_summaries's own local fuse"
    );

    let result = quarantine::resume_column(&db.pool, "order_totals", "total").await;
    assert_eq!(
        result.expect("resuming the live upstream column must succeed"),
        vec![("order_totals".to_string(), "total".to_string())],
        "the cascaded dependent must NOT be resumed alongside it: its definition isn't live yet"
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the resumed upstream column itself must no longer be paused"
    );
    // Issue #916: the walk deleted its edge into the reader, and the reader's
    // build has already run chunks with the column held out, so it can't be
    // released. It stays paused as a pause of its own, with a reason that
    // is true and the way out, not as a row with no edge that says it waits
    // on an upstream that has resumed.
    let (local_fuse, last_error) = column_status_row(&client, "order_summaries", "grand_total")
        .await
        .expect(
            "the cascaded-onto column must remain paused: its definition is mid-build, so \
             resume_column must not release it",
        );
    assert!(
        local_fuse,
        "with its edge gone, the column is held by a pause of its own"
    );
    let last_error = last_error.expect("the held column says why");
    assert!(
        last_error.contains("backfilling")
            && last_error.contains("RESUME it once the definition is live"),
        "the reason names the definition's state and the way out, got {last_error:?}"
    );
    assert!(
        !cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the upstream resumed, so no edge into the reader remains"
    );
}

// ---------------------------------------------------------------------
// (l) Issue #130, epic #127: `recompute_column`'s to-one relationship read
//     moved onto the settled parent projection (#129), same as the ordinary
//     forward-apply path (`staging::apply::compute`) — quarantine replay
//     (this call site) must see the exact same semantics as normal drain,
//     not a live re-read that happens to disagree with it.
// ---------------------------------------------------------------------

/// `resume_column`'s recompute (`recompute_column`, the second of
/// `build_relationship_context`'s two call sites) must resolve a to-one
/// relationship path the same way the live CDC-apply path now does (#130):
/// from the settled parent projection, not a live read of the parent. Proved
/// by renaming the live `categories` row *after* the projection has already
/// settled on its original name, with no reverse recompute or projection
/// advance in between (#131 doesn't exist yet) — a live read would pick up
/// the rename; the projection cannot, because #130 alone never advances a
/// projection row's data columns on a parent-side mutation.
#[tokio::test]
async fn resume_column_resolves_a_to_one_relationship_from_the_projection_not_live_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer); \
             insert into articles (id, category_id) values (1, 10)",
        )
        .await
        .expect("create + seed tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // Front door (issue #40): `create_definition`'s own widen call (#129)
    // catches category 10's `name` = 'Tech' into the projection right here,
    // while it's still the row's only-ever value.
    let source_columns = numeric_columns(&["id", "category_id"]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
    )
    .await
    .expect("create to-one enrichment definition");

    let article_cat_def = TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::RelationshipPath {
                rel: "category".to_string(),
                column: "name".to_string(),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &article_cat_def,
        "public",
        &pk,
        &source_columns,
        &article_cat_def.source,
    )
    .await
    .expect("create target table");

    // Sentinel, distinct from either the live or the projected name, so a
    // successful recompute is unambiguous.
    client
        .execute(
            "insert into article_cat (id, category_name) values (1, 'SENTINEL')",
            &[],
        )
        .await
        .expect("seed sentinel target row");

    // Reached past the mechanism, same convention as this file's other
    // tests: seed `column_status` directly rather than driving a real
    // failure to trip the fuse.
    client
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('article_cat', 'category_name', 'synthetic pause', true)",
            &[],
        )
        .await
        .expect("seed column_status");

    // Rename the live category — no CDC staged, no reverse recompute, no
    // projection advance. Only #131 (not built) would keep the projection in
    // step with this.
    client
        .execute("update categories set name = 'Renamed' where id = 10", &[])
        .await
        .expect("mutate the live category without touching the projection");

    // Confirm the projection genuinely still disagrees with live truth
    // before asserting anything about the recompute's output — otherwise a
    // passing assertion below wouldn't distinguish "read the projection"
    // from "coincidentally read the right value some other way".
    let projection = relationship_projection(&db.pool, relationship.id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection");
    let projected_name: Option<String> = client
        .query_one(
            &format!(
                "select name from {} where id = 10",
                projection.projection_table
            ),
            &[],
        )
        .await
        .expect("read projection row")
        .get(0);
    assert_eq!(
        projected_name.as_deref(),
        Some("Tech"),
        "sanity check: the projection must still hold the pre-rename name"
    );

    let resumed = quarantine::resume_column(&db.pool, "article_cat", "category_name")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("article_cat".to_string(), "category_name".to_string())]
    );
    // #625 F8b: a relationship-enriched 1-1's field build re-derives each
    // key's whole row, as a page does.
    trellis::staging::build::settle_builds(&db.pool).await;

    let recomputed: Option<String> = client
        .query_one("select category_name from article_cat where id = 1", &[])
        .await
        .expect("read article_cat")
        .get(0);
    assert_eq!(
        recomputed.as_deref(),
        Some("Tech"),
        "the column's field build (the quarantine-replay call site) must resolve the to-one \
         relationship from the settled projection, exactly like normal drain's forward \
         path (#130) — not the live category row, which was renamed to 'Renamed' after \
         the projection had already settled"
    );
}

// ---------------------------------------------------------------------
// Issue #281: column attribution must resolve a *bare* `src_table` before
// asking the catalog which transforms read that source.
// ---------------------------------------------------------------------

/// Stages one malformed CDC insert per id under the **bare** `orders`
/// spelling — `insert_cdc_row`'s deliberate opposite (it qualifies every
/// `src_table` through `qualify_fixture_table`, precisely so this whole
/// file's machinery is exercised at all). This is what a durable pre-#267
/// ring row looks like, and what several of this crate's other integration
/// fixtures still stage by hand.
async fn stage_bad_orders_bare(client: &mut Client, pool: &trellis::Pool, ids: &[i64]) {
    let table = active_segment_table(client).await;
    for id in ids {
        client
            .execute(
                &format!(
                    "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ('orders', $1, 'insert', $2, null, $3::text::jsonb, 0)"
                ),
                &[
                    &id.to_string(),
                    &testkit::wal_insert_lsn(&*client).await,
                    &Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
                ],
            )
            .await
            .expect("stage bare-src_table cdc row");
    }
    let seg_seq = seal_active_segment(client).await;
    let result = apply::drain_once(
        pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "a malformed numeric field must still surface as an evaluator failure for a bare \
         src_table (`apply::compute` resolves it via `qualified_schema_node_key`), got \
         {result:?}"
    );
}

/// A calculated-field failure on a row staged with a **bare** `src_table`
/// must still be attributed to its `(transform, column)` pair.
///
/// `attribute_column_failure` used to pass its raw `src_table` straight to
/// `catalog::transforms_for_source`, which requires an already-qualified name
/// (issue #74, ADR-0007) and answers a bare one with an *empty* candidate set
/// rather than an error. The lookup therefore matched no definition, the
/// function fell out through its "no candidate" arm, and the failure went
/// completely unattributed — indistinguishable, from the outside, from the
/// deliberate ambiguous-match fallback, and with no log line either.
///
/// Note `apply::compute` itself handles the same bare row fine (hence the
/// `ApplyError::Eval` this fixture asserts on): it routes every
/// `transforms_for_source` call through its own `qualified_schema_node_key`.
/// The gap was quarantine's two call sites alone. Pre-fix both assertions
/// below see `None`; post-fix the column is charged and, at the threshold,
/// paused.
#[tokio::test]
async fn a_bare_src_table_failure_is_still_attributed_to_its_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    assert_eq!(
        DEFAULT_COLUMN_DEATH_THRESHOLD, 5,
        "test assumes the default"
    );

    let ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders_bare(&mut client, &db.pool, &ids).await;

    let status = column_status_row(&client, "order_totals", "total")
        .await
        .expect(
            "a threshold's worth of evaluator failures on a bare `src_table` must pause the \
             column, not go unattributed (issue #281)",
        );
    assert!(status.0, "a threshold trip is a local fuse, not a cascade");
    assert!(status.1.is_some(), "the tripping error must be recorded");
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "the counter must reset once the fuse trips"
    );
}

// ---------------------------------------------------------------------
// (g) The operator-driven half of a column pause — issue #227, ADR-0014's
//     "one state, two triggers" at column granularity, reached through the
//     unified `PAUSE TRANSFORM <target>.<column>` grammar rather than by a
//     fuse tripping.
// ---------------------------------------------------------------------

/// `PAUSE TRANSFORM <target>.<column>` must reach exactly the state the fuse
/// reaches — a `column_status` row plus a cascade onto every dependent reader
/// — so that a deliberately paused column freezes its value and is recovered
/// by the very same `RESUME`. Anything else would be the second freezing
/// mechanism ADR-0014 rules out.
#[tokio::test]
async fn pausing_a_column_by_statement_reaches_the_fuse_s_own_state_and_cascades() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let applied = trellis
        .apply("PAUSE TRANSFORM order_totals.total")
        .await
        .expect("pause a healthy column deliberately");
    assert!(
        matches!(applied, trellis::Applied::Paused),
        "got {applied:?}"
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        Some((true, None)),
        "an operator pause records its own reason to stay paused (local_fuse), with no \
         error of its own — nothing failed"
    );
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "no fuse tripped, so nothing may have been charged against one"
    );
    assert!(
        cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "a dependent reader must pause too rather than silently consume a frozen value"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the cascaded dependent must actually be paused, not only edge-recorded"
    );

    // ADR-0014's idempotency clause: pause runs outside any host migration
    // transaction, so a replayed migration has to be safe to re-run.
    trellis
        .apply("PAUSE TRANSFORM order_totals.total")
        .await
        .expect("pausing an already-paused column is a success");
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        Some((true, None)),
    );

    // The same grammar's resume un-cascades the dependent along with it: the
    // dependent's only reason to be paused was this cascade.
    let resumed = trellis
        .apply("RESUME TRANSFORM order_totals.total")
        .await
        .expect("resume the deliberately-paused column");
    let trellis::Applied::Resumed { columns } = resumed else {
        panic!("a RESUME statement must report a resume, got {resumed:?}");
    };
    assert_eq!(
        columns,
        vec![
            ("order_totals".to_string(), "total".to_string()),
            ("order_summaries".to_string(), "grand_total".to_string()),
        ],
        "the addressed column first, then the dependent un-cascaded with it"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None
    );
    assert_eq!(
        column_status_row(&client, "order_summaries", "grand_total").await,
        None
    );

    // #625 F8b: each resumed column is a field build. `order_totals` was
    // `live`, so it reads `backfilling`; `order_summaries` reads a seam-fed
    // source, so `create_definition` left it `catching_up`, and it stays so.
    // Its catch-up's discharge, with its field chunks still to run, hands it
    // to the build (`backfilling`) rather than flipping it `live`, and the
    // build's last chunk does.
    assert_eq!(status_named(&client, "order_totals").await, "backfilling");
    assert_eq!(
        status_named(&client, "order_summaries").await,
        "catching_up"
    );
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("discharge order_summaries' catch-up");
    assert_eq!(
        status_named(&client, "order_summaries").await,
        "backfilling"
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(status_named(&client, "order_totals").await, "live");
    assert_eq!(status_named(&client, "order_summaries").await, "live");
}

/// Issue #228's decision-5 clause about "addressing something that doesn't
/// exist": a column address is validated against the definition's own declared
/// fields before anything is written, because `column_status` has no foreign
/// key that would catch it — an unchecked pause would park a row naming
/// nothing and only surface on a later resume.
#[tokio::test]
async fn pausing_a_column_that_does_not_exist_is_refused_before_anything_is_written() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let err = trellis
        .apply("PAUSE TRANSFORM order_totals.nonesuch")
        .await
        .expect_err("a column the definition never declared must be refused");
    assert_eq!(err.code(), trellis::ErrorCode::NotFound);
    let message = err.to_string();
    assert!(
        message.contains("nonesuch") && message.contains("total"),
        "the refusal must name both the bad column and the ones that exist: {message}"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "nonesuch").await,
        None,
        "a refused pause must leave no column_status row behind"
    );

    // And the whole-transform half of the same check: a dotted address whose
    // *transform* half is unknown. (This is also the shape a caller gets if
    // they wrote `<schema>.<transform>`, which this grammar reads as
    // `<transform>.<column>` — see `Trellis::apply`'s "Addressing".)
    let err = trellis
        .apply("PAUSE TRANSFORM no_such_transform.total")
        .await
        .expect_err("an unknown transform must be refused");
    assert_eq!(err.code(), trellis::ErrorCode::NotFound);
    assert!(err.to_string().contains("no_such_transform"), "got {err}");
}

/// Issue #305, restated for #625 F8b: a row changed while a resumed
/// column's field build runs needs no catch-up. The resume unpauses the
/// column in the transaction that registers its build, so the column
/// applies from there: Apply writes it for every later change, and the
/// build's chunks rewrite it under the keys' entry lock. The resume parks
/// no marker, and a change applied after the build's chunk is right.
#[tokio::test]
async fn a_row_changed_after_a_column_resume_needs_no_catch_up() {
    use trellis::staging::{has_pending, retire_drained_segments};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    client
        .execute("insert into orders (id, price, tax) values (1, 10, 5)", &[])
        .await
        .expect("seed a source row");
    // Stand-in for the target row live CDC apply already built for it: a
    // field build rewrites the column of rows that exist.
    client
        .execute("insert into public.order_totals (id) values (1)", &[])
        .await
        .expect("seed the target row");
    quarantine::pause_column(&db.pool, "order_totals", "total")
        .await
        .expect("pause the column");

    quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("resume the column");
    let parked: i64 = client
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count pending_backfill")
        .get(0);
    assert_eq!(parked, 0, "a column resume parks no catch-up marker");
    trellis::staging::build::settle_builds(&db.pool).await;
    let total: String = client
        .query_one(
            "select total::text from public.order_totals where id = 1",
            &[],
        )
        .await
        .expect("read the rebuilt column")
        .get(0);
    assert_eq!(total, "15", "the field build populated the row");
    assert_eq!(status_named(&client, "order_totals").await, "live");

    // A change after the build: Apply writes the resumed column.
    client
        .execute("update orders set price = 20 where id = 1", &[])
        .await
        .expect("update the source row");
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        "1",
        "update",
        Some(r#"{"id":"1","price":"10","tax":"5"}"#),
        Some(r#"{"id":"1","price":"20","tax":"5"}"#),
    )
    .await;
    for _ in 0..16 {
        let seg_seq = seal_active_segment(&mut client).await;
        while apply::drain_once(
            &db.pool,
            seg_seq,
            "worker",
            1,
            "trellis_column_quarantine_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(&mut client)
            .await
            .expect("retire drained segments");
        if !has_pending(&client).await.expect("has_pending") {
            break;
        }
    }

    let total: String = client
        .query_one(
            "select total::text from public.order_totals where id = 1",
            &[],
        )
        .await
        .expect("read the column after the change")
        .get(0);
    assert_eq!(
        total, "25",
        "Apply writes the resumed column for a change after its resume"
    );
}

// ---------------------------------------------------------------------
// Issue #748: a field that reads a paused field by alias is paused with it.
// ---------------------------------------------------------------------

/// Creates source `items (id, price, tax, bonus)` and the 1-1 transform
/// `sib` over it from `select` (its `SELECT` list), with its target table.
async fn seed_items_transform(db: &TestDatabase, client: &Client, select: &str) {
    client
        .batch_execute(
            "create table items (id integer primary key, price numeric, tax numeric, \
             bonus numeric)",
        )
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax", "bonus"]);
    let definition = create_definition(
        &db.pool,
        &format!("TRANSFORM sib FROM items SELECT {select}"),
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &definition.def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &definition.def,
        "public",
        &pk,
        &source_columns,
        &definition.def.source,
    )
    .await
    .expect("create target table");
}

/// Writes `items` row `id` as `(price, tax, bonus)` and stages its change
/// (`before` the row's prior values, `None` for an insert), then drains
/// everything staged.
async fn change_item(
    client: &mut Client,
    pool: &trellis::Pool,
    id: i64,
    before: Option<(i64, i64, i64)>,
    after: (i64, i64, i64),
) {
    let (price, tax, bonus) = after;
    client
        .execute(
            "insert into items (id, price, tax, bonus) values ($1, $2::bigint, $3::bigint, \
             $4::bigint) \
             on conflict (id) do update set price = excluded.price, tax = excluded.tax, \
             bonus = excluded.bonus",
            &[&(id as i32), &price, &tax, &bonus],
        )
        .await
        .expect("write the source row");
    let image = |(price, tax, bonus): (i64, i64, i64)| {
        format!(r#"{{"id":"{id}","price":"{price}","tax":"{tax}","bonus":"{bonus}"}}"#)
    };
    let table = active_segment_table(client).await;
    let (op, old_image) = match before {
        Some(before) => ("update", Some(image(before))),
        None => ("insert", None),
    };
    insert_cdc_row(
        client,
        &table,
        "items",
        &id.to_string(),
        op,
        old_image.as_deref(),
        Some(&image(after)),
    )
    .await;
    drain_staged(client, pool).await;
}

/// Seals and drains every staged change, as the last test above does.
async fn drain_staged(client: &mut Client, pool: &trellis::Pool) {
    use trellis::staging::{has_pending, retire_drained_segments};
    for _ in 0..16 {
        let seg_seq = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg_seq,
            "worker",
            1,
            "trellis_column_quarantine_test",
            &StagedWatermark::saturated(),
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
    panic!("the staged changes never drained");
}

/// `sib`'s row `id`, as `column = value` text for each of `columns`.
async fn sib_row(client: &Client, id: i32, columns: &[&str]) -> Vec<Option<String>> {
    let select: Vec<String> = columns.iter().map(|c| format!("{c}::text")).collect();
    let row = client
        .query_one(
            &format!("select {} from public.sib where id = $1", select.join(", ")),
            &[&id],
        )
        .await
        .expect("read the target row");
    (0..columns.len()).map(|i| row.get(i)).collect()
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

/// The repro's shape (`derived = CHAR_LENGTH(c3)` over field `c3`), in
/// numbers: while `total` is paused, a change to the key keeps `doubled`
/// (which reads `total` by alias) at its old value instead of writing it
/// NULL, `status` lists `doubled` as paused, and the resume releases and
/// rebuilds both.
#[tokio::test]
async fn a_field_reading_a_paused_field_by_alias_freezes_and_its_resume_rebuilds_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(
        &db,
        &client,
        "price + tax AS total, total + total AS doubled",
    )
    .await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["15", "30"])
    );

    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert_eq!(
        column_status_row(&client, "sib", "doubled").await,
        Some((
            false,
            Some("paused because upstream column 'sib.total' is paused".to_string())
        )),
        "a sibling reading the paused field is paused too, for that reason only"
    );
    assert!(cascade_edge_exists(&client, "sib", "doubled", "sib", "total").await);
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");
    let status = trellis
        .quarantine_status("sib.doubled")
        .await
        .expect("read the sibling's status");
    assert_eq!(status.state, trellis::QuarantineState::Paused);

    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["15", "30"]),
        "both freeze: the sibling keeps its value rather than reading the paused field as \
         absent and going NULL"
    );

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib".to_string(), "doubled".to_string()),
        ]
    );
    assert_eq!(column_status_row(&client, "sib", "doubled").await, None);
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["25", "50"]),
        "the resume's field build rebuilds the sibling with the resumed field"
    );
    assert_eq!(status_named(&client, "sib").await, "live");
}

/// The same, through a chain: `b` reads `a` and `c` reads `b`, so pausing
/// `a` pauses and freezes both, and resuming it rebuilds all three.
#[tokio::test]
async fn a_transitive_alias_chain_freezes_and_rebuilds_with_its_paused_head() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS a, a + a AS b, b + 1 AS c").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["a", "b", "c"]).await,
        some(&["15", "30", "31"])
    );

    quarantine::pause_column(&db.pool, "sib", "a")
        .await
        .expect("pause a");
    assert!(column_status_row(&client, "sib", "b").await.is_some());
    assert!(column_status_row(&client, "sib", "c").await.is_some());
    assert!(cascade_edge_exists(&client, "sib", "c", "sib", "b").await);

    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["a", "b", "c"]).await,
        some(&["15", "30", "31"]),
        "every field down the chain freezes"
    );

    let resumed = quarantine::resume_column(&db.pool, "sib", "a")
        .await
        .expect("resume a");
    assert_eq!(resumed.len(), 3, "a, b and c all resume: {resumed:?}");
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["a", "b", "c"]).await,
        some(&["25", "50", "51"])
    );
}

/// A sibling reading two paused fields stays paused, and frozen, until both
/// are resumed: resuming one rebuilds only that one.
#[tokio::test]
async fn a_sibling_reading_two_paused_fields_stays_paused_until_both_resume() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + 0 AS p, tax + 0 AS t, p + t AS s").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["p", "t", "s"]).await,
        some(&["10", "5", "15"])
    );

    quarantine::pause_column(&db.pool, "sib", "p")
        .await
        .expect("pause p");
    quarantine::pause_column(&db.pool, "sib", "t")
        .await
        .expect("pause t");
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 7, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["p", "t", "s"]).await,
        some(&["10", "5", "15"])
    );

    // An operator pause of s too, then its resume while p and t are still
    // paused: that drops only the operator's reason, and s stays paused for
    // the fields it reads.
    quarantine::pause_column(&db.pool, "sib", "s")
        .await
        .expect("pause s");
    let resumed = quarantine::resume_column(&db.pool, "sib", "s")
        .await
        .expect("resume s");
    assert!(resumed.is_empty(), "s reads paused fields: {resumed:?}");
    assert_eq!(
        column_status_row(&client, "sib", "s")
            .await
            .map(|(local_fuse, _)| local_fuse),
        Some(false),
        "s stays paused, now for the cascade's reason only"
    );

    let resumed = quarantine::resume_column(&db.pool, "sib", "p")
        .await
        .expect("resume p");
    assert_eq!(resumed, vec![("sib".to_string(), "p".to_string())]);
    assert!(
        column_status_row(&client, "sib", "s").await.is_some(),
        "s still reads the paused t, so it stays paused"
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["p", "t", "s"]).await,
        some(&["20", "5", "15"]),
        "p is rebuilt; s stays frozen while t is paused"
    );
    change_item(&mut client, &db.pool, 1, Some((20, 7, 0)), (30, 7, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["p", "t", "s"]).await,
        some(&["30", "5", "15"]),
        "Apply writes p and still holds s"
    );

    let resumed = quarantine::resume_column(&db.pool, "sib", "t")
        .await
        .expect("resume t");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "t".to_string()),
            ("sib".to_string(), "s".to_string()),
        ]
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["p", "t", "s"]).await,
        some(&["30", "7", "37"])
    );
}

/// `ALTER TRANSFORM` changing a field another reads by alias: `doubled`'s
/// value moves with `total`'s, so the edit's field build rebuilds it too,
/// rather than leaving every key the edit's build alone reaches at the value
/// from the old formula.
#[tokio::test]
async fn an_alter_of_a_field_a_sibling_reads_by_alias_rebuilds_the_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(
        &db,
        &client,
        "price + tax AS total, total + total AS doubled",
    )
    .await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 3)).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["15", "30"])
    );

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");
    trellis
        .apply("ALTER TRANSFORM sib ALTER total AS price + tax + 1")
        .await
        .expect("alter the field doubled reads");
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["16", "32"]),
        "doubled reads the altered field, so the edit rebuilds it"
    );
    assert_eq!(status_named(&client, "sib").await, "live");
}

/// Apply's own read of the paused fields closes over alias readers, not
/// only `cascade_pause`'s rows: a field paused by a `column_status` row
/// that no cascade has walked yet (the moment between a pause's own row and
/// its cascade, or an `ALTER TRANSFORM` field awaiting its capture) still
/// holds its readers out.
#[tokio::test]
async fn apply_holds_out_an_alias_reader_of_a_paused_field_without_a_row_of_its_own() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(
        &db,
        &client,
        "price + tax AS total, total + total AS doubled",
    )
    .await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;

    client
        .execute(
            "insert into column_status (transform_table, column_name, paused_at, local_fuse) \
             values ('sib', 'total', now(), true)",
            &[],
        )
        .await
        .expect("pause total with no cascade");
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["15", "30"])
    );
}

/// An edit that makes a field read a paused field by alias pauses it with
/// that field, as a pause after the edit would: it is listed as paused, by
/// a row and an edge, and the paused field's resume releases it and builds
/// it. Here `doubled` is added while `total` is paused.
#[tokio::test]
async fn an_alter_adding_a_reader_of_a_paused_field_pauses_it_until_that_fields_resume() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");
    trellis
        .apply("ALTER TRANSFORM sib ADD total + total AS doubled")
        .await
        .expect("add a reader of the paused field");
    assert_eq!(
        column_status_row(&client, "sib", "doubled").await,
        Some((
            false,
            Some("paused because upstream column 'sib.total' is paused".to_string())
        )),
        "the new reader is paused with the field it reads"
    );
    assert!(cascade_edge_exists(&client, "sib", "doubled", "sib", "total").await);
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        vec![Some("15".to_string()), None],
        "the edit's build leaves the paused reader alone"
    );
    assert!(column_status_row(&client, "sib", "doubled").await.is_some());

    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib".to_string(), "doubled".to_string()),
        ]
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "doubled"]).await,
        some(&["25", "50"])
    );
    assert_eq!(status_named(&client, "sib").await, "live");
}

/// A definition reading a paused field's sibling reader is paused through
/// it, and resumed with it: `sib_sum.d1` reads `sib.doubled`, which reads
/// the paused `sib.total` by alias.
#[tokio::test]
async fn a_definition_reading_a_frozen_sibling_is_paused_and_resumed_through_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_items_transform(
        &db,
        &client,
        "price + tax AS total, total + total AS doubled",
    )
    .await;
    let sib_columns = numeric_columns(&["id", "total", "doubled"]);
    let downstream = create_definition(
        &db.pool,
        "TRANSFORM sib_sum FROM sib SELECT doubled + 1 AS d1",
        &sib_columns,
    )
    .await
    .expect("create the downstream definition");
    let pk = source_primary_key(&db.pool, &downstream.def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &downstream.def,
        "public",
        &pk,
        &sib_columns,
        &downstream.def.source,
    )
    .await
    .expect("create the downstream target table");

    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "d1", "sib", "doubled").await);
    assert!(column_status_row(&client, "sib_sum", "d1").await.is_some());

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib".to_string(), "doubled".to_string()),
            ("sib_sum".to_string(), "d1".to_string()),
        ]
    );
    assert_eq!(column_status_row(&client, "sib_sum", "d1").await, None);
}

// ---------------------------------------------------------------------
// Issue #914: a field defined or added while a column it reads is paused is
// paused at birth, as the cascade pauses a reader that existed before.
// ---------------------------------------------------------------------

/// A `Trellis` on `db`, for the statement-driven tests below.
async fn trellis_on(db: &TestDatabase) -> Trellis {
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
}

/// `sib_sum`'s row `id`, as text for each of `columns`.
async fn sib_sum_row(client: &Client, id: i32, columns: &[&str]) -> Vec<Option<String>> {
    let select: Vec<String> = columns.iter().map(|c| format!("{c}::text")).collect();
    let row = client
        .query_one(
            &format!(
                "select {} from public.sib_sum where id = $1",
                select.join(", ")
            ),
            &[&id],
        )
        .await
        .expect("read the downstream target row");
    (0..columns.len()).map(|i| row.get(i)).collect()
}

/// Defines `sib_sum` reading `sib.total` while `total` is paused: `t1`
/// reads it directly and `t2` reads `t1` by alias. Both are paused at
/// birth, with the edges a pause after the define would record, so neither
/// applies `total`'s frozen value; `c1`, reading a live column, builds.
/// `total`'s resume releases both and builds them from the resumed value.
#[tokio::test]
async fn a_definition_reading_a_paused_column_is_paused_at_birth_until_that_columns_resume() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    assert_eq!(
        sib_row(&client, 1, &["total", "cost"]).await,
        some(&["15", "20"]),
        "total is frozen"
    );

    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1, t1 + 1 AS t2, cost + 1 AS c1")
        .await
        .expect("define a reader of the paused column");
    assert_eq!(
        column_status_row(&client, "sib_sum", "t1").await,
        Some((
            false,
            Some("paused because upstream column 'sib.total' is paused".to_string())
        )),
        "a field reading the paused column is paused at birth, for that reason only"
    );
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    assert_eq!(
        column_status_row(&client, "sib_sum", "t2").await,
        Some((
            false,
            Some("paused because upstream column 'sib_sum.t1' is paused".to_string())
        )),
        "a sibling reading the paused-at-birth field is paused with it"
    );
    assert!(cascade_edge_exists(&client, "sib_sum", "t2", "sib_sum", "t1").await);
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);
    let status = trellis
        .quarantine_status("sib_sum.t1")
        .await
        .expect("read the reader's status");
    assert_eq!(status.state, trellis::QuarantineState::Paused);

    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(status_named(&client, "sib_sum").await, "live");
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "t2", "c1"]).await,
        vec![None, None, Some("21".to_string())],
        "the define's build applies no frozen value; the live field builds"
    );

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib_sum".to_string(), "t1".to_string()),
            ("sib_sum".to_string(), "t2".to_string()),
        ]
    );
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "t2").await, None);
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(sib_row(&client, 1, &["total"]).await, some(&["25"]));
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "t2", "c1"]).await,
        some(&["26", "27", "21"]),
        "the resume builds the readers from the resumed value"
    );
}

/// Issue #916: the column's resume that comes before the new definition's
/// registration settles. `sib_sum` is `waiting_to_backfill`, so it can't
/// take a field build, but it has built nothing either: the resume releases
/// its born-paused fields by deleting their rows, and its own build, which
/// reads the paused set when it runs, writes them from the resumed value.
/// Before the fix the resume deleted the edge and skipped the pair,
/// leaving `t1` paused with no edge and a reason that no longer held.
#[tokio::test]
async fn a_resume_before_the_readers_registration_settles_releases_its_born_paused_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;

    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1, t1 + 1 AS t2, cost + 1 AS c1")
        .await
        .expect("define a reader of the paused column");
    assert_eq!(
        status_named(&client, "sib_sum").await,
        "waiting_to_backfill"
    );
    assert!(column_status_row(&client, "sib_sum", "t1").await.is_some());

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total before sib_sum's registration settles");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib_sum".to_string(), "t1".to_string()),
            ("sib_sum".to_string(), "t2".to_string()),
        ],
        "a definition that hasn't started building has nothing to rebuild, so its fields are released"
    );
    for column in ["t1", "t2"] {
        assert_eq!(
            column_status_row(&client, "sib_sum", column).await,
            None,
            "{column} must not be left paused with no edge and a reason that no longer holds"
        );
    }
    assert!(!cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);

    trellis::intake::markers::settle_registrations(&db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(status_named(&client, "sib_sum").await, "live");
    assert_eq!(sib_row(&client, 1, &["total"]).await, some(&["25"]));
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "t2", "c1"]).await,
        some(&["26", "27", "21"]),
        "the definition's build writes the released fields from the resumed value"
    );
}

/// Issue #916, the other pair a column's resume can't release: a reader
/// whose definition no longer validates (#708). The walk deleted its edge,
/// so the reader is held as a pause of its own, with a reason that says
/// why, and its sibling keeps the edge from it. An operator `RESUME` of the
/// held field is refused while define would refuse the definition, and
/// once the schema is fixed it releases and rebuilds the field and its
/// sibling.
#[tokio::test]
async fn a_resume_holds_a_reader_whose_definition_no_longer_validates_until_its_own_resume() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1, t1 + 1 AS t2, cost + 1 AS c1")
        .await
        .expect("define a reader of total");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(status_named(&client, "sib_sum").await, "live");
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "t2", "c1"]).await,
        some(&["16", "17", "11"])
    );

    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "t2", "sib_sum", "t1").await);
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    // A column of `sib_sum`'s source named like its calculated field `t1`
    // would shadow it, so define would refuse `sib_sum` now.
    client
        .batch_execute("alter table public.sib add column t1 numeric")
        .await
        .expect("add a column named like the reader's field");

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![("sib".to_string(), "total".to_string())],
        "the reader whose definition no longer validates is not resumed"
    );
    let (local_fuse, last_error) = column_status_row(&client, "sib_sum", "t1")
        .await
        .expect("t1 stays paused");
    assert!(
        local_fuse,
        "with its edge gone, t1 is held by a pause of its own"
    );
    let last_error = last_error.expect("the held column says why");
    assert!(
        last_error.contains("no longer validates")
            && last_error.contains("shares its name with a source column")
            && last_error.contains("RESUME it once the definition validates again"),
        "the reason names the refusal and the way out, got {last_error:?}"
    );
    assert!(!cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    assert_eq!(
        column_status_row(&client, "sib_sum", "t2").await,
        Some((
            false,
            Some("paused because upstream column 'sib_sum.t1' is paused".to_string())
        )),
        "the held field's reader keeps its edge from it, and its reason"
    );
    assert!(cascade_edge_exists(&client, "sib_sum", "t2", "sib_sum", "t1").await);

    match trellis.apply("RESUME TRANSFORM sib_sum.t1").await {
        Err(trellis::TrellisError::Apply(ApplyError::ResumeRefused { .. })) => {}
        other => panic!("expected the field resume to be refused, got {other:?}"),
    }
    client
        .batch_execute("alter table public.sib drop column t1")
        .await
        .expect("drop the ambiguous column");
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(sib_row(&client, 1, &["total"]).await, some(&["25"]));
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "t2", "c1"]).await,
        some(&["16", "17", "21"]),
        "the held fields stay frozen; the live field applies"
    );

    trellis
        .apply("RESUME TRANSFORM sib_sum.t1")
        .await
        .expect("resume the held field once its definition validates");
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "t2").await, None);
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "t2", "c1"]).await,
        some(&["26", "27", "21"]),
        "the held field's resume rebuilds it and its sibling from the resumed value"
    );
}

/// The same for an edit: `ALTER TRANSFORM ... ADD` of a field reading
/// another definition's paused column pauses the field at birth, and the
/// column's resume releases it.
#[tokio::test]
async fn an_alter_adding_a_reader_of_another_definitions_paused_column_pauses_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT cost + 1 AS c1")
        .await
        .expect("define the downstream definition");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");

    trellis
        .apply("ALTER TRANSFORM sib_sum ADD total + 1 AS t1")
        .await
        .expect("add a reader of the paused column");
    assert_eq!(
        column_status_row(&client, "sib_sum", "t1").await,
        Some((
            false,
            Some("paused because upstream column 'sib.total' is paused".to_string())
        )),
        "the added field is paused at birth"
    );
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "c1"]).await,
        vec![None, Some("11".to_string())],
        "the edit's build leaves the paused field alone"
    );

    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib_sum".to_string(), "t1".to_string()),
        ]
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(
        sib_sum_row(&client, 1, &["t1", "c1"]).await,
        some(&["26", "21"])
    );
}

/// An aggregate defined while a column it reads is paused is not paused:
/// column pauses stop at aggregates whenever the aggregate was defined
/// (known correctness gap 19), so define records nothing for it.
#[tokio::test]
async fn an_aggregate_defined_over_a_paused_column_is_not_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");

    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_stats FROM sib GROUP BY id SELECT id AS id, SUM(total) AS total_sum")
        .await
        .expect("define an aggregate over the paused column");
    assert_eq!(
        column_status_row(&client, "sib_stats", "total_sum").await,
        None
    );
    let edges: i64 = client
        .query_one(
            "select count(*) from column_pause_cascades where downstream_transform = 'sib_stats'",
            &[],
        )
        .await
        .expect("count edges")
        .get(0);
    assert_eq!(edges, 0);
}

/// A pause of a column waits for a define reading it that read no pause
/// yet: otherwise the define could miss the pause while the pause's
/// cascade, reading the dependency graph before the define commits, missed
/// the define, and the reader would apply the frozen value. Here the define
/// is frozen after its read, so a pause that can't wait (100 ms) fails
/// with nothing written; once the define commits, a pause reaches it.
#[tokio::test]
async fn a_pause_waits_for_a_define_reading_its_column_that_read_no_pause() {
    const PAUSE_LOCK: i64 = 9140;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterUpstreamPausesRead, "sib_sum", PAUSE_LOCK);
    let pool = db.pool.clone();
    let mut define = tokio::spawn(with_scope(scope, async move {
        create_definition(
            &pool,
            "TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1",
            &numeric_columns(&["id", "total"]),
        )
        .await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut define => panic!("the define finished without reaching its read: {finished:?}"),
    }

    let early = quarantine::pause_column(&impatient_pool(&db), "sib", "total").await;
    assert!(
        early.is_err(),
        "the pause waits for the define that read no pause, got {early:?}"
    );
    assert_eq!(column_status_row(&client, "sib", "total").await, None);

    release_gate(&gate, PAUSE_LOCK).await;
    define
        .await
        .expect("define task")
        .expect("the define commits");
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);

    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(column_status_row(&client, "sib_sum", "t1").await.is_some());
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
}

/// The same one hop further down: a pause's cascade into a column of
/// `sib_sum` waits for a define reading `sib_sum` that read no pause of it.
/// Without the column-pause lock in the cascade pair, the pair would pause
/// `sib_sum.t1` while the define, having read it live, was still to
/// commit, and the walk, reading the graph before that commit, would miss
/// the new reader. The pause's own transaction commits first, and its walk
/// is frozen at the pair's fence bump while the define of `sib_down` reads
/// its upstream's pauses and freezes holding the lock. The pair can't wait
/// (100 ms), so the pause returns the error with the walk still owed; once
/// the define commits, finishing the walk reaches it.
#[tokio::test]
async fn a_cascade_pair_waits_for_a_define_reading_its_column_that_read_no_pause() {
    const PAIR_GATE: i64 = 9141;
    const DEFINE_GATE: i64 = 9142;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    trellis_on(&db)
        .await
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1")
        .await
        .expect("define the middle definition");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;

    // The pause commits its own row, then its walk freezes at the pair.
    let pair_gate = take_gate(&db, PAIR_GATE).await;
    let scope = PauseScope::new();
    let pair_reached = scope.arm(PausePoint::AfterCascadeFenceBump, "sib_sum", PAIR_GATE);
    let impatient = impatient_pool(&db);
    let mut pause = tokio::spawn(with_scope(scope, async move {
        quarantine::pause_column(&impatient, "sib", "total").await
    }));
    tokio::select! {
        reached = pair_reached => { reached.expect("pause scope dropped"); }
        finished = &mut pause => panic!("the pause finished without reaching its pair: {finished:?}"),
    }
    assert!(column_status_row(&client, "sib", "total").await.is_some());

    // The define reads `sib_sum`'s pauses (none) and freezes, holding the lock.
    let define_gate = take_gate(&db, DEFINE_GATE).await;
    let scope = PauseScope::new();
    let define_reached = scope.arm(PausePoint::AfterUpstreamPausesRead, "sib_down", DEFINE_GATE);
    let pool = db.pool.clone();
    let mut define = tokio::spawn(with_scope(scope, async move {
        create_definition(
            &pool,
            "TRANSFORM sib_down FROM sib_sum SELECT t1 + 1 AS d1",
            &numeric_columns(&["id", "t1"]),
        )
        .await
    }));
    tokio::select! {
        reached = define_reached => { reached.expect("pause scope dropped"); }
        finished = &mut define => panic!("the define finished without reaching its read: {finished:?}"),
    }

    // The pair goes on, and can't get the lock the define holds.
    release_gate(&pair_gate, PAIR_GATE).await;
    let early = pause.await.expect("pause task");
    assert!(
        matches!(early, Err(ApplyError::ColumnPauseLockTimeout(_))),
        "the cascade pair waits for the define that read no pause, got {early:?}"
    );
    assert_eq!(cascade_pending(&client, "sib", "total").await, Some(true));
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);

    release_gate(&define_gate, DEFINE_GATE).await;
    define
        .await
        .expect("define task")
        .expect("the define commits");
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);

    assert_eq!(
        quarantine::complete_pause_cascades(&db.pool)
            .await
            .expect("finish the walk"),
        1
    );
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "t1").await);
    assert_eq!(cascade_pending(&client, "sib", "total").await, Some(false));
}

// ---------------------------------------------------------------------
// Issue #915: an `ALTER TRANSFORM` that pauses a field that already existed
// at birth owes the cascade to that field's readers, as any other pause.
// ---------------------------------------------------------------------

/// `sib (total, cost)` over `items`, row 1 drained, and the chain
/// `sib_sum.c1 = cost + 1` -> `sib_down.d1 = c1 + 1`, both live and built;
/// then `sib.total` paused. Returns the client and a `Trellis` on `db`.
async fn seed_reader_chain_with_total_paused(db: &TestDatabase) -> (Client, Trellis) {
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT cost + 1 AS c1")
        .await
        .expect("define sib_sum");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    trellis
        .apply("TRANSFORM sib_down FROM sib_sum SELECT c1 + 1 AS d1")
        .await
        .expect("define sib_down");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(status_named(&client, "sib_down").await, "live");
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    (client, trellis)
}

/// The staging worker's capture pass, as the maintenance loop runs it: it
/// finishes every column pause's cascade still owed.
async fn run_capture_pass(client: &mut Client, db: &TestDatabase) {
    trellis::client::reconcile_pass(
        client,
        &db.pool,
        DEFAULT_SCHEMA,
        "wake",
        Duration::from_secs(5),
    )
    .await
    .expect("reconcile pass");
}

/// The issue's reproduction: editing `sib_sum.c1` to read the paused
/// `sib.total` pauses `c1` at birth, and the edit walks `c1`'s cascade once
/// it commits, so `sib_down.d1`, which already read `c1`, is paused by the
/// time the edit returns, with no capture pass run, rather than applying
/// `c1`'s frozen value until one runs.
#[tokio::test]
async fn an_alter_pausing_an_existing_field_at_birth_cascades_to_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (client, trellis) = seed_reader_chain_with_total_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 100")
        .await
        .expect("edit c1 to read the paused column");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "total").await);
    assert_eq!(
        column_status_row(&client, "sib_down", "d1").await,
        Some((
            false,
            Some("paused because upstream column 'sib_sum.c1' is paused".to_string())
        )),
        "the existing reader of the edited field is paused"
    );
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);
    assert_eq!(
        cascade_pending(&client, "sib_sum", "c1").await,
        Some(false),
        "the edit's walk cleared the mark"
    );
}

/// The same through issue #748's sibling path: editing `sib.cost` to read
/// the paused `sib.total` by alias pauses `cost` with it, and the edit's
/// walk pauses `sib_sum.c1`, which already read `cost`, and its reader.
#[tokio::test]
async fn an_alter_pausing_an_existing_field_with_a_paused_sibling_cascades_to_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (client, trellis) = seed_reader_chain_with_total_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib ALTER cost AS total + 0")
        .await
        .expect("edit cost to read the paused sibling");
    assert!(cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
    assert_eq!(cascade_pending(&client, "sib", "cost").await, Some(false));
}

/// `RESUME` of the paused upstream column releases the whole chain the
/// edit's cascade paused, and builds each link from the resumed value.
#[tokio::test]
async fn resuming_the_column_an_alter_paused_a_field_for_releases_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 100")
        .await
        .expect("edit c1 to read the paused column");
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
    trellis::staging::build::settle_builds(&db.pool).await;

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib_sum".to_string(), "c1".to_string()),
            ("sib_down".to_string(), "d1".to_string()),
        ]
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["115"]));
    let d1: Option<String> = client
        .query_one("select d1::text from public.sib_down where id = 1", &[])
        .await
        .expect("read sib_down")
        .get(0);
    assert_eq!(d1.as_deref(), Some("116"));
}

/// An edit that pauses `sib_sum.c1` at birth waits for a define reading
/// `sib_sum` that read no pause of it yet. Otherwise the define could read
/// `c1` live and commit after the edit's cascade read the dependency graph,
/// and its reader would apply `c1`'s frozen value. Here the define of
/// `sib_down` is frozen after its read, so an edit that can't wait (100 ms)
/// fails with nothing written; once the define commits, the edit's walk
/// reaches it.
#[tokio::test]
async fn an_alter_pausing_a_field_waits_for_a_define_reading_it_that_read_no_pause() {
    const PAUSE_LOCK: i64 = 9150;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT cost + 1 AS c1")
        .await
        .expect("define sib_sum");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterUpstreamPausesRead, "sib_down", PAUSE_LOCK);
    let pool = db.pool.clone();
    let mut define = tokio::spawn(with_scope(scope, async move {
        create_definition(
            &pool,
            "TRANSFORM sib_down FROM sib_sum SELECT c1 + 1 AS d1",
            &numeric_columns(&["id", "c1"]),
        )
        .await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut define => panic!("the define finished without reaching its read: {finished:?}"),
    }

    let impatient = Trellis::connect(
        Config::from_dsn(format!("{} options='-c lock_timeout=100'", db.dsn())).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let early = impatient
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 100")
        .await;
    assert!(
        early.is_err(),
        "the edit waits for the define that read no pause, got {early:?}"
    );
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);

    release_gate(&gate, PAUSE_LOCK).await;
    define
        .await
        .expect("define task")
        .expect("the define commits");
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 100")
        .await
        .expect("edit c1 to read the paused column");
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
}

/// An edit whose field both waits for its capture widen and reads a paused
/// sibling by alias: `ALTER sib ALTER cost AS total + bonus` reads `bonus`,
/// which `sib` didn't read, so `cost` is written `awaiting_capture`, and
/// then reads the paused `total`, so the sibling path gives it an edge. The
/// edge is a reason to cascade that the capture wait alone isn't, so the
/// row is marked even though it was written awaiting its capture, and the
/// edit's walk pauses `cost`'s readers. Releasing the capture wait leaves
/// `cost` paused on its edge, and `total`'s resume releases the chain.
#[tokio::test]
async fn an_alter_pausing_a_field_awaiting_its_capture_on_a_paused_sibling_cascades_to_its_readers()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib ALTER cost AS total + bonus")
        .await
        .expect("edit cost to read a new source column and the paused sibling");
    let awaiting: bool = client
        .query_one(
            "select awaiting_capture from column_status \
             where transform_table = 'sib' and column_name = 'cost'",
            &[],
        )
        .await
        .expect("read cost's row")
        .get(0);
    assert!(awaiting, "cost waits for its capture widen");
    assert!(cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);
    assert_eq!(cascade_pending(&client, "sib", "cost").await, Some(false));

    trellis::staging::build::settle_builds(&db.pool).await;
    let still_awaiting: Option<bool> = client
        .query_opt(
            "select awaiting_capture from column_status \
             where transform_table = 'sib' and column_name = 'cost'",
            &[],
        )
        .await
        .expect("read cost's row")
        .map(|row| row.get(0));
    assert_eq!(
        still_awaiting,
        Some(false),
        "the capture release leaves cost paused on its edge"
    );

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib".to_string(), "cost".to_string()),
            ("sib_sum".to_string(), "c1".to_string()),
            ("sib_down".to_string(), "d1".to_string()),
        ]
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["16"]));
    let d1: Option<String> = client
        .query_one("select d1::text from public.sib_down where id = 1", &[])
        .await
        .expect("read sib_down")
        .get(0);
    assert_eq!(d1.as_deref(), Some("17"));
}

/// The ALTER of #918's routes: `cost` reads `bonus`, which `sib` didn't
/// read, so it waits for its capture widen, and `total`, by alias.
const ALTER_COST_AWAITING_CAPTURE: &str = "ALTER TRANSFORM sib ALTER cost AS total + bonus";

/// Nothing is left of the pause `sib.cost` passed on: the field has no row
/// and no edge leaves it, and its readers are released, rebuilt from the
/// resumed value and live.
async fn assert_cost_readers_released(client: &mut Client, db: &TestDatabase) {
    assert_eq!(column_status_row(client, "sib", "cost").await, None);
    assert_eq!(column_status_row(client, "sib_sum", "c1").await, None);
    assert_eq!(column_status_row(client, "sib_down", "d1").await, None);
    let edges: i64 = client
        .query_one("select count(*) from column_pause_cascades", &[])
        .await
        .expect("count edges")
        .get(0);
    assert_eq!(edges, 0, "no edge outlives the rows it joined");
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(client, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(client, &db.pool).await;
    assert_eq!(sib_sum_row(client, 1, &["c1"]).await, some(&["16"]));
    let d1: Option<String> = client
        .query_one("select d1::text from public.sib_down where id = 1", &[])
        .await
        .expect("read sib_down")
        .get(0);
    assert_eq!(d1.as_deref(), Some("17"));
}

/// Issue #918, route 2. `cost` waits for its capture and has passed `total`'s
/// pause on to `sib_sum.c1` and `sib_down.d1`. `total`'s resume deletes the
/// edge into `cost` and leaves the row, which is awaiting its capture. The
/// capture release then deletes that row; it has to release `cost`'s
/// readers as the resume would have, or they stay paused on a column that
/// isn't.
#[tokio::test]
async fn the_capture_release_of_a_field_that_passed_a_pause_on_releases_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    trellis
        .apply(ALTER_COST_AWAITING_CAPTURE)
        .await
        .expect("edit cost");
    quarantine::complete_pause_cascades(&db.pool)
        .await
        .expect("finish the walks");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);

    let resumed = quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert_eq!(resumed, vec![("sib".to_string(), "total".to_string())]);
    assert!(
        column_status_row(&client, "sib", "cost").await.is_some(),
        "the resume leaves the field that awaits its capture"
    );

    trellis::staging::build::settle_builds(&db.pool).await;
    assert_cost_readers_released(&mut client, &db).await;
}

/// Issue #918, route 3: `total` is resumed before the edit's cascade walk
/// runs, so the walk starts from a row that only awaits its capture.
#[tokio::test]
async fn a_resume_before_the_cascade_walk_leaves_no_reader_paused_by_the_capture_release() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    trellis
        .apply(ALTER_COST_AWAITING_CAPTURE)
        .await
        .expect("edit cost");
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    quarantine::complete_pause_cascades(&db.pool)
        .await
        .expect("finish the walks");

    trellis::staging::build::settle_builds(&db.pool).await;
    assert_cost_readers_released(&mut client, &db).await;
}

/// Issue #918, route 1: the edit comes first, and `total`'s pause then
/// cascades through the field that awaits its capture.
#[tokio::test]
async fn a_pause_cascading_through_a_field_awaiting_its_capture_is_released_with_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    trellis
        .apply(ALTER_COST_AWAITING_CAPTURE)
        .await
        .expect("edit cost");
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");

    trellis::staging::build::settle_builds(&db.pool).await;
    assert_cost_readers_released(&mut client, &db).await;
}

/// Issue #918 through the other release: the definition is frozen and
/// resumed (`RESUME TRANSFORM`) while `cost` still awaits its capture, so the
/// resume discards the field build that would have released it, and the
/// rebuild's start releases it instead.
#[tokio::test]
async fn a_rebuilds_release_of_a_field_that_passed_a_pause_on_releases_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    trellis
        .apply(ALTER_COST_AWAITING_CAPTURE)
        .await
        .expect("edit cost");
    quarantine::complete_pause_cascades(&db.pool)
        .await
        .expect("finish the walks");
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    assert!(column_status_row(&client, "sib", "cost").await.is_some());
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);

    trellis
        .apply("PAUSE TRANSFORM sib")
        .await
        .expect("freeze sib");
    trellis
        .apply("RESUME TRANSFORM sib")
        .await
        .expect("resume sib");
    assert_eq!(status_named(&client, "sib").await, "waiting_to_backfill");
    run_capture_pass(&mut client, &db).await;
    assert_cost_readers_released(&mut client, &db).await;
}

/// Issue #918's release, one sibling deeper: the edit also adds `z`, which
/// reads `cost` by alias and `bonus`, so it awaits its capture too, and
/// `total`'s pause reaches it through `cost`. The release's first delete
/// can't take `z` (the edge from `cost` is still there); the un-cascade from
/// `cost` deletes that edge, and the delete runs again for `z`. Without the
/// second run, `z` would stay paused with no reason left, out of Apply.
#[tokio::test]
async fn the_capture_release_releases_a_sibling_awaiting_its_capture_behind_the_released_field() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    trellis
        .apply("ALTER TRANSFORM sib ALTER cost AS total + bonus, ADD cost + bonus AS z")
        .await
        .expect("edit cost, add z");
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib", "z", "sib", "cost").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");

    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(column_status_row(&client, "sib", "z").await, None);
    assert_cost_readers_released(&mut client, &db).await;
    assert_eq!(sib_row(&client, 1, &["z"]).await, some(&["15"]));
}

/// The edit has committed when it walks its cascade, so a walk that fails
/// doesn't fail the edit: it keeps its mark, and the capture pass finishes
/// it. Here a page in flight holds `sib_down`'s fence, so the walk's pause
/// of `sib_down.d1` gives up after 100 ms; the edit still returns `Ok`, `d1`
/// is still live and `c1` still owes its cascade. Once the page commits,
/// the capture pass pauses `d1`.
#[tokio::test]
async fn an_alter_whose_cascade_walk_fails_leaves_it_to_the_capture_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, _trellis) = seed_reader_chain_with_total_paused(&db).await;
    let impatient = Trellis::connect(
        Config::from_dsn(format!("{} options='-c lock_timeout=100'", db.dsn())).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");

    let mut page = connect_raw(db.dsn()).await;
    let holder = hold_fence_of(&mut page, "sib_down").await;
    impatient
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 100")
        .await
        .expect("the edit commits though its walk fails");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "total").await);
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);
    assert_eq!(
        cascade_pending(&client, "sib_sum", "c1").await,
        Some(true),
        "the failed walk keeps its mark"
    );

    holder.commit().await.expect("the page commits");
    run_capture_pass(&mut client, &db).await;
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
    assert_eq!(cascade_pending(&client, "sib_sum", "c1").await, Some(false));
}

// ---------------------------------------------------------------------
// Issue #950: an `ALTER TRANSFORM` that stops a paused reader reading the
// column it was paused through ends that pause.
// ---------------------------------------------------------------------

/// `seed_reader_chain_with_total_paused`, then `RESUME sib.total` (built and
/// drained) and `PAUSE sib.cost`, which cascades to `sib_sum.c1` (edge
/// `sib.cost -> sib_sum.c1`) and on to `sib_down.d1`.
async fn seed_reader_chain_with_cost_paused(db: &TestDatabase) -> (Client, Trellis) {
    let (mut client, trellis) = seed_reader_chain_with_total_paused(db).await;
    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    quarantine::pause_column(&db.pool, "sib", "cost")
        .await
        .expect("pause cost");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);
    (client, trellis)
}

/// Settles the builds the edits started and drains what they staged.
async fn settle_and_drain(client: &mut Client, db: &TestDatabase) {
    for _ in 0..2 {
        trellis::staging::build::settle_builds(&db.pool).await;
        drain_staged(client, &db.pool).await;
    }
}

async fn cascade_edge_count(client: &Client) -> i64 {
    client
        .query_one("select count(*) from column_pause_cascades", &[])
        .await
        .expect("count edges")
        .get(0)
}

/// When `sib_sum.c1` and `sib_down.d1` were paused, in that order.
async fn paused_at_of_c1_and_d1(client: &Client) -> Vec<std::time::SystemTime> {
    client
        .query(
            "select paused_at from column_status \
             where (transform_table, column_name) in (('sib_sum', 'c1'), ('sib_down', 'd1')) \
             order by transform_table desc",
            &[],
        )
        .await
        .expect("read paused_at")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn d1_text(client: &Client) -> Option<String> {
    client
        .query_one("select d1::text from public.sib_down where id = 1", &[])
        .await
        .expect("read sib_down")
        .get(0)
}

/// The state between the issue's steps 3 and 4: `c1` was edited to read
/// `total + 2`, so it no longer reads the paused `sib.cost`. The edit
/// deletes the edge from `cost`; `c1` has no reason left, so it is released
/// and rebuilt, and so is `d1`, which was paused behind it. Before the fix
/// `c1` stayed paused on a column it doesn't read.
#[tokio::test]
async fn an_alter_that_stops_a_paused_reader_reading_its_upstream_releases_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_cost_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 2")
        .await
        .expect("edit c1 to stop reading cost");
    assert!(!cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);
    assert_eq!(cascade_edge_count(&client).await, 0);
    assert!(
        column_status_row(&client, "sib", "cost").await.is_some(),
        "the upstream column stays paused"
    );

    settle_and_drain(&mut client, &db).await;
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["17"]));
    assert_eq!(d1_text(&client).await.as_deref(), Some("18"));
}

/// The issue's reproduction, steps 1 to 5: after the edit and a `DROP` of
/// the column it stopped reading, the reader and its own reader are live and
/// rebuilt, not paused on a column that no longer exists.
#[tokio::test]
async fn dropping_a_column_a_reader_was_edited_to_stop_reading_strands_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_cost_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 2")
        .await
        .expect("edit c1 to stop reading cost");
    trellis
        .apply("ALTER TRANSFORM sib DROP cost")
        .await
        .expect("drop cost");
    settle_and_drain(&mut client, &db).await;

    assert_eq!(column_status_row(&client, "sib", "cost").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);
    assert_eq!(cascade_edge_count(&client).await, 0);
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["17"]));
    assert_eq!(d1_text(&client).await.as_deref(), Some("18"));
}

/// A reader that stops reading one paused column and starts reading another
/// stays paused, through the new one: its edge from `cost` goes, the one
/// from `total` stays, and `total`'s resume releases it and its reader.
#[tokio::test]
async fn an_alter_to_another_paused_column_moves_the_readers_pause_to_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_cost_paused(&db).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    let paused_since = paused_at_of_c1_and_d1(&client).await;

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 2")
        .await
        .expect("edit c1 to read total");
    assert!(!cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "total").await);
    assert_eq!(
        column_status_row(&client, "sib_sum", "c1").await,
        Some((
            false,
            Some("paused because upstream column 'sib.total' is paused".to_string())
        )),
        "c1's reason names the column it is still paused through"
    );
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
    // The edit's own pauses are written before it releases what lost an
    // edge, so c1 never left its pause, nor d1 with it: their rows are the
    // ones the pause of `cost` wrote, not ones written again after a
    // release and a resume of d1 over c1's frozen value.
    assert_eq!(paused_at_of_c1_and_d1(&client).await, paused_since);

    quarantine::resume_column(&db.pool, "sib", "total")
        .await
        .expect("resume total");
    settle_and_drain(&mut client, &db).await;
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["17"]));
    assert_eq!(d1_text(&client).await.as_deref(), Some("18"));
}

/// A reader paused through two upstream columns that one's resume leaves
/// paused through the other names the other as its reason, not the resumed
/// one.
#[tokio::test]
async fn a_reader_still_paused_through_another_edge_names_that_upstream() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (client, trellis) = seed_reader_chain_with_cost_paused(&db).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS cost + total")
        .await
        .expect("edit c1 to read both");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "total").await);

    quarantine::resume_column(&db.pool, "sib", "cost")
        .await
        .expect("resume cost");
    assert_eq!(
        column_status_row(&client, "sib_sum", "c1").await,
        Some((
            false,
            Some("paused because upstream column 'sib.total' is paused".to_string())
        ))
    );
}

/// A reader paused by an operator as well keeps that pause when its edit
/// drops the cascade edge: the edge goes, `c1` and `d1` stay paused, and
/// `c1`'s own resume releases them.
#[tokio::test]
async fn an_alter_keeps_a_readers_own_pause_when_it_drops_the_edge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_reader_chain_with_cost_paused(&db).await;
    quarantine::pause_column(&db.pool, "sib_sum", "c1")
        .await
        .expect("pause c1 itself");

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 2")
        .await
        .expect("edit c1 to stop reading cost");
    assert!(!cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert_eq!(
        column_status_row(&client, "sib_sum", "c1")
            .await
            .map(|(local_fuse, _)| local_fuse),
        Some(true)
    );
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
    assert!(cascade_edge_exists(&client, "sib_down", "d1", "sib_sum", "c1").await);

    quarantine::resume_column(&db.pool, "sib_sum", "c1")
        .await
        .expect("resume c1");
    settle_and_drain(&mut client, &db).await;
    assert_eq!(column_status_row(&client, "sib_down", "d1").await, None);
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["17"]));
    assert_eq!(d1_text(&client).await.as_deref(), Some("18"));
}

/// An edit that keeps reading the paused column leaves the reader paused,
/// with its edge.
#[tokio::test]
async fn an_alter_that_still_reads_the_paused_column_keeps_the_readers_pause() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (client, trellis) = seed_reader_chain_with_cost_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS cost + 2")
        .await
        .expect("edit c1");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(column_status_row(&client, "sib_sum", "c1").await.is_some());
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
}

/// An edit that still reads a column awaiting its capture keeps the edge
/// from it, which the edit's own pause pass doesn't rewrite (a column that
/// awaits its capture pauses no reader there): the reader stays paused.
#[tokio::test]
async fn an_alter_that_still_reads_a_column_awaiting_its_capture_keeps_the_edge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (client, trellis) = seed_reader_chain_with_total_paused(&db).await;
    trellis
        .apply(ALTER_COST_AWAITING_CAPTURE)
        .await
        .expect("edit cost");
    quarantine::complete_pause_cascades(&db.pool)
        .await
        .expect("finish the walks");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);

    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS cost + 5")
        .await
        .expect("edit c1 to keep reading cost");
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert!(column_status_row(&client, "sib_sum", "c1").await.is_some());
    assert!(column_status_row(&client, "sib_down", "d1").await.is_some());
}

/// `sib (total = price + tax, cost = total + 0)` over `items`, row 1 drained,
/// `sib_sum.c1 = cost + 1` live, and `sib.total` paused: the pause reaches
/// `cost` through an alias edge and `c1` through `cost`.
async fn seed_alias_chain_with_total_paused(db: &TestDatabase) -> (Client, Trellis) {
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(db, &client, "price + tax AS total, total + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT cost + 1 AS c1")
        .await
        .expect("define sib_sum");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    (client, trellis)
}

/// The same through a sibling edge: `cost` is edited to stop reading the
/// paused `total` by alias, so it is released in the edit's own field build,
/// and `sib_sum.c1`, paused behind it, is resumed.
#[tokio::test]
async fn an_alter_that_stops_a_field_reading_a_paused_sibling_releases_it_and_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_alias_chain_with_total_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib ALTER cost AS price + 0")
        .await
        .expect("edit cost to stop reading total");
    assert!(!cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);
    assert_eq!(column_status_row(&client, "sib", "cost").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);
    assert_eq!(cascade_edge_count(&client).await, 0);
    assert!(
        column_status_row(&client, "sib", "total").await.is_some(),
        "total stays paused"
    );

    settle_and_drain(&mut client, &db).await;
    assert_eq!(sib_row(&client, 1, &["cost"]).await, some(&["10"]));
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["11"]));
}

/// An edit that stops a paused field reading its upstream and starts reading
/// a source column the capture may not image yet holds it for that capture
/// instead of releasing it, as a fresh edit does: its edge is gone, it
/// awaits its capture, and its reader stays paused behind it until the
/// build's plan job releases both.
#[tokio::test]
async fn an_alter_that_stops_a_paused_field_reading_its_upstream_holds_it_for_its_capture() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_alias_chain_with_total_paused(&db).await;

    trellis
        .apply("ALTER TRANSFORM sib ALTER cost AS bonus + 0")
        .await
        .expect("edit cost to read a new source column");
    assert!(!cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);
    let awaiting: Option<(bool, Option<String>)> = client
        .query_opt(
            "select awaiting_capture, last_error from column_status \
             where transform_table = 'sib' and column_name = 'cost'",
            &[],
        )
        .await
        .expect("read cost's row")
        .map(|row| (row.get(0), row.get(1)));
    assert_eq!(
        awaiting,
        Some((true, None)),
        "cost waits for its capture widen, and no longer names total as its reason"
    );
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);

    settle_and_drain(&mut client, &db).await;
    assert_eq!(column_status_row(&client, "sib", "cost").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "c1").await, None);
    assert_eq!(cascade_edge_count(&client).await, 0);
    assert_eq!(sib_row(&client, 1, &["cost"]).await, some(&["0"]));
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["1"]));
}

// ---------------------------------------------------------------------
// Issue #917: a define reading a paused target's rows is ordered against a
// resume of one of its columns that releases a sibling.
// ---------------------------------------------------------------------

/// The one backend waiting on `pid`, once there is one. A short wait for a
/// lock queue to form, not for anything to converge.
async fn blocked_behind(client: &Client, pid: i32) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let waiters: Vec<i32> = client
            .query(
                "select pid from pg_stat_activity where $1 = any(pg_blocking_pids(pid))",
                &[&pid],
            )
            .await
            .expect("read pg_stat_activity")
            .iter()
            .map(|row| row.get(0))
            .collect();
        if let [waiter] = waiters[..] {
            return waiter;
        }
        assert!(
            waiters.is_empty(),
            "one backend waits on {pid}: {waiters:?}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "nothing queued behind {pid}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The issue's reproduction, with no stress loop. `sib.cost` reads
/// `sib.total` by alias, so pausing `total` pauses `cost` too, and `cost`
/// sorts first. `RESUME sib.total` deletes `total`'s row, then releases
/// `cost` and deletes its row. A define of a reader of `sib` reads `sib`'s
/// paused rows in name order, `cost` then `total`. Frozen between its two
/// deletes, the resume holds `total`'s row; the define then takes `cost`'s
/// and waits for `total`'s, and the resume's delete of `cost` closes the
/// cycle (`deadlock detected`). The resume's column-pause lock makes the
/// define wait for the whole resume instead, so both commit, and the define
/// finds nothing paused. With the one column-pause lock (#922) the define
/// waits for the resume at the lock, before it reads any row.
#[tokio::test]
async fn a_define_reading_a_target_waits_for_a_resume_releasing_a_sibling_that_sorts_first() {
    const PAUSE_LOCK: i64 = 9170;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, total + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterResumedColumnDeleted, "sib", PAUSE_LOCK);
    let pool = db.pool.clone();
    let mut resume = tokio::spawn(with_scope(scope, async move {
        quarantine::resume_column(&pool, "sib", "total").await
    }));
    let resume_pid = tokio::select! {
        reached = reached => reached.expect("pause scope dropped").backend_pid,
        finished = &mut resume => panic!("the resume finished without reaching its delete: {finished:?}"),
    };

    let pool = db.pool.clone();
    let define = tokio::spawn(async move {
        create_definition(
            &pool,
            "TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1",
            &numeric_columns(&["id", "total", "cost"]),
        )
        .await
    });
    blocked_behind(&client, resume_pid).await;

    release_gate(&gate, PAUSE_LOCK).await;
    let resumed = resume
        .await
        .expect("resume task")
        .expect("the resume commits");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib".to_string(), "cost".to_string()),
        ]
    );
    define
        .await
        .expect("define task")
        .expect("the define commits");
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);
    assert_eq!(column_status_row(&client, "sib", "cost").await, None);
}

// ---------------------------------------------------------------------
// Issue #922: one column-pause lock per instance. Lock order, DROP vs RESUME
// (#921), the timeout, and a define holding the lock for its catalog
// transaction only.
// ---------------------------------------------------------------------

/// The default-schema instance's column-pause lock key as `pg_locks` shows it
/// (`classid` and `objid` are unsigned `oid`s).
fn column_pause_lock_oids() -> (u32, u32) {
    let (class, object) = trellis::locks::column_pause_lock_key(trellis::config::DEFAULT_SCHEMA);
    (class as u32, object as u32)
}

/// Whether backend `pid` holds the column-pause lock, in either mode.
async fn holds_column_pause_lock(client: &Client, pid: i32) -> bool {
    let (class, object) = column_pause_lock_oids();
    client
        .query_one(
            "select exists (select 1 from pg_locks \
             where pid = $1 and locktype = 'advisory' and granted \
               and classid = $2 and objid = $3 and objsubid = 2)",
            &[&pid, &class, &object],
        )
        .await
        .expect("read pg_locks")
        .get(0)
}

/// Whether anything holds the column-pause lock.
async fn column_pause_lock_is_held(client: &Client) -> bool {
    let (class, object) = column_pause_lock_oids();
    client
        .query_one(
            "select exists (select 1 from pg_locks \
             where locktype = 'advisory' and granted \
               and classid = $1 and objid = $2 and objsubid = 2)",
            &[&class, &object],
        )
        .await
        .expect("read pg_locks")
        .get(0)
}

/// Whether `target`'s source fence is write-locked by some transaction.
async fn fence_is_write_locked(db: &TestDatabase, target: &str) -> bool {
    let mut probe = connect_raw(db.dsn()).await;
    let txn = probe.transaction().await.expect("begin");
    let err = txn
        .execute(
            "select v.version from source_table_versions v \
             join transform_definitions d on d.source_table = v.source_table \
             where split_part(d.target_table, '.', 2) = $1 for update of v nowait",
            &[&target],
        )
        .await;
    match err {
        Ok(_) => false,
        Err(err) => {
            assert!(trellis::locks::is_lock_not_available(&err), "{err}");
            true
        }
    }
}

/// Holds `target`'s definition row `for update`, as a transaction that took
/// its row lock before the column-pause lock would, until the returned
/// transaction ends. Returns it with its backend pid.
async fn hold_definition_row<'a>(
    raw: &'a mut Client,
    target: &str,
) -> (tokio_postgres::Transaction<'a>, i32) {
    let holder = raw.transaction().await.expect("begin the row holder");
    let held = holder
        .execute(
            "select id from transform_definitions \
             where split_part(target_table, '.', 2) = $1 for update",
            &[&target],
        )
        .await
        .expect("hold the definition row");
    assert_eq!(held, 1, "{target}'s definition row is held");
    let pid = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("read the backend pid")
        .get(0);
    (holder, pid)
}

/// A pause waits out its source's fence before it takes the column-pause
/// lock, so a pause stuck behind busy writers (up to the lock timeout)
/// never holds the lock against everything else. Rule 3 of #922: the fence
/// comes first.
#[tokio::test]
async fn a_pause_waits_out_its_fence_without_holding_the_column_pause_lock() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;

    let mut raw = connect_raw(db.dsn()).await;
    let writer = hold_fence_of(&mut raw, "sib").await;
    let writer_pid: i32 = writer
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("read the backend pid")
        .get(0);
    let pool = db.pool.clone();
    let pause = tokio::spawn(async move { quarantine::pause_column(&pool, "sib", "total").await });
    let waiting = blocked_behind(&client, writer_pid).await;
    assert!(
        !holds_column_pause_lock(&client, waiting).await,
        "the pause waits for the writer's fence first"
    );
    assert!(!column_pause_lock_is_held(&client).await);

    writer.commit().await.expect("the writer commits");
    pause.await.expect("pause task").expect("the pause commits");
    assert!(column_status_row(&client, "sib", "total").await.is_some());
}

/// A define waits out its source's fence before it takes the column-pause
/// lock (shared), for the same reason.
#[tokio::test]
async fn a_define_waits_out_its_fence_without_holding_the_column_pause_lock() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;

    let mut raw = connect_raw(db.dsn()).await;
    let writer = hold_fence_of(&mut raw, "sib").await;
    let writer_pid: i32 = writer
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("read the backend pid")
        .get(0);
    let pool = db.pool.clone();
    let define = tokio::spawn(async move {
        create_definition(
            &pool,
            "TRANSFORM sib_two FROM items SELECT price + 1 AS p1",
            &numeric_columns(&["id", "price", "tax", "bonus"]),
        )
        .await
    });
    let waiting = blocked_behind(&client, writer_pid).await;
    assert!(
        !holds_column_pause_lock(&client, waiting).await,
        "the define waits for the writer's fence first"
    );

    writer.commit().await.expect("the writer commits");
    define
        .await
        .expect("define task")
        .expect("the define commits");
}

/// A resume takes the column-pause lock after its fence and before its
/// definition row: stopped at the row, it holds the fence and the lock.
#[tokio::test]
async fn a_resume_takes_the_fence_then_the_column_pause_lock_then_the_definition_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");

    let mut raw = connect_raw(db.dsn()).await;
    let (row, row_pid) = hold_definition_row(&mut raw, "sib").await;
    let pool = db.pool.clone();
    let resume =
        tokio::spawn(async move { quarantine::resume_column(&pool, "sib", "total").await });
    let waiting = blocked_behind(&client, row_pid).await;
    assert!(holds_column_pause_lock(&client, waiting).await);
    assert!(fence_is_write_locked(&db, "sib").await);

    row.commit().await.expect("release the row");
    resume
        .await
        .expect("resume task")
        .expect("the resume commits");
}

/// `ALTER TRANSFORM` that builds a field: fence, then the column-pause lock,
/// then the definition row.
#[tokio::test]
async fn an_alter_takes_the_fence_then_the_column_pause_lock_then_the_definition_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    trellis::intake::markers::settle_registrations(&db.pool).await;
    let trellis = trellis_on(&db).await;

    let mut raw = connect_raw(db.dsn()).await;
    let (row, row_pid) = hold_definition_row(&mut raw, "sib").await;
    let mut alter = tokio::spawn(async move {
        trellis
            .apply("ALTER TRANSFORM sib ADD price + price AS twice")
            .await
    });
    let waiting = tokio::select! {
        waiting = blocked_behind(&client, row_pid) => waiting,
        finished = &mut alter => panic!("the alter finished without waiting: {finished:?}"),
    };
    assert!(holds_column_pause_lock(&client, waiting).await);
    assert!(fence_is_write_locked(&db, "sib").await);

    row.commit().await.expect("release the row");
    alter.await.expect("alter task").expect("the alter commits");
}

/// `DROP TRANSFORM` has no fence: the column-pause lock is its first lock,
/// before the definition row (#921).
#[tokio::test]
async fn a_drop_takes_the_column_pause_lock_before_the_definition_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("PAUSE TRANSFORM sib")
        .await
        .expect("pause the transform");

    let mut raw = connect_raw(db.dsn()).await;
    let (row, row_pid) = hold_definition_row(&mut raw, "sib").await;
    let drop = tokio::spawn(async move { trellis.apply("DROP TRANSFORM sib").await });
    let waiting = blocked_behind(&client, row_pid).await;
    assert!(holds_column_pause_lock(&client, waiting).await);

    row.commit().await.expect("release the row");
    drop.await.expect("drop task").expect("the drop commits");
}

/// Issue #921: a `DROP TRANSFORM` of a paused reader races a `RESUME` of the
/// upstream column it reads, and both delete the same cascade edges, in
/// different orders. Frozen after the resume has deleted its column's row,
/// the resume holds the column-pause lock; the drop must wait for the whole
/// resume instead of interleaving its edge deletes with it, and both
/// commit, in whichever order they take the lock.
#[tokio::test]
async fn a_drop_serializes_with_a_resume_deleting_the_same_edges() {
    const PAUSE_LOCK: i64 = 9210;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, total + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1, cost + 1 AS t2")
        .await
        .expect("define the reader");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "t2", "sib", "cost").await);
    trellis
        .apply("PAUSE TRANSFORM sib_sum")
        .await
        .expect("pause the reader");

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterResumedColumnDeleted, "sib", PAUSE_LOCK);
    let pool = db.pool.clone();
    let mut resume = tokio::spawn(with_scope(scope, async move {
        quarantine::resume_column(&pool, "sib", "total").await
    }));
    let resume_pid = tokio::select! {
        reached = reached => reached.expect("pause scope dropped").backend_pid,
        finished = &mut resume => panic!("the resume finished without reaching its delete: {finished:?}"),
    };

    let drop = tokio::spawn(async move { trellis.apply("DROP TRANSFORM sib_sum").await });
    let waiting = blocked_behind(&client, resume_pid).await;
    assert!(
        !holds_column_pause_lock(&client, waiting).await,
        "the drop waits for the lock the resume holds"
    );
    assert!(column_pause_lock_is_held(&client).await);

    release_gate(&gate, PAUSE_LOCK).await;
    resume
        .await
        .expect("resume task")
        .expect("the resume commits");
    drop.await.expect("drop task").expect("the drop commits");
    let edges: i64 = client
        .query_one(
            "select count(*) from column_pause_cascades \
             where downstream_transform in ('sib', 'sib_sum') \
                or upstream_transform in ('sib', 'sib_sum')",
            &[],
        )
        .await
        .expect("count edges")
        .get(0);
    assert_eq!(edges, 0);
    assert_eq!(column_status_row(&client, "sib", "total").await, None);
    assert_eq!(column_status_row(&client, "sib", "cost").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);
}

async fn status(client: &Client) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = 'sib_sum'",
            &[],
        )
        .await
        .expect("read the status")
        .get(0)
}

/// A define holds the (shared) column-pause lock for its catalog
/// transaction, and not past it: registration reads no source rows, and the
/// build and backfill it starts run later in transactions of their own.
/// With the definition registered (`waiting_to_backfill`, its build not yet
/// begun), nothing holds the lock, and a pause (exclusive) isn't kept
/// waiting behind it.
#[tokio::test]
async fn a_define_does_not_hold_the_column_pause_lock_across_its_build() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1")
        .await
        .expect("define the reader");

    assert_eq!(status(&client).await, "waiting_to_backfill");
    assert!(!column_pause_lock_is_held(&client).await);
    quarantine::pause_column(&impatient_pool(&db), "sib", "total")
        .await
        .expect("a pause doesn't wait behind a registered define");
    quarantine::resume_column(&impatient_pool(&db), "sib", "total")
        .await
        .expect("nor a resume");

    // Its build and backfill run after, in transactions of their own, and
    // take the lock only to release an edit's capture wait. Run to the end,
    // none leaves it held, and a second define takes it at once.
    trellis::intake::markers::settle_registrations(&db.pool).await;
    assert_eq!(status(&client).await, "live");
    assert!(!column_pause_lock_is_held(&client).await);
    create_definition(
        &impatient_pool(&db),
        "TRANSFORM sib_two FROM items SELECT price + 1 AS p1",
        &numeric_columns(&["id", "price", "tax", "bonus"]),
    )
    .await
    .expect("a second define takes the lock at once");
}

/// Issue #922 rule 5: a held column-pause lock makes a pause fail with the
/// named, retryable error (code `Timeout`) after the transaction's
/// `lock_timeout`, writing nothing; the same call succeeds once it is free.
#[tokio::test]
async fn a_pause_behind_a_held_column_pause_lock_times_out_retryably() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;

    let mut holder = db.pool.get().await.expect("connect");
    let hold = holder.transaction().await.expect("begin");
    trellis::locks::lock_column_pauses(
        &*hold,
        trellis::config::DEFAULT_SCHEMA,
        trellis::locks::ColumnPauseLock::Exclusive,
        trellis::locks::ColumnPauseOp::Resume,
    )
    .await
    .expect("take the lock");

    let err = quarantine::pause_column(&impatient_pool(&db), "sib", "total")
        .await
        .expect_err("the pause waits for the lock");
    assert!(
        matches!(&err, ApplyError::ColumnPauseLockTimeout(timeout)
            if timeout.op == trellis::locks::ColumnPauseOp::Pause),
        "{err:?}"
    );
    assert_eq!(err.code(), trellis::ErrorCode::Timeout);
    assert_eq!(column_status_row(&client, "sib", "total").await, None);

    hold.commit().await.expect("release the lock");
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("the retry pauses");
    assert!(column_status_row(&client, "sib", "total").await.is_some());
}

/// Issue #922: a resume of a column that stays paused with a sibling it reads
/// clears the column's own reason (`local_fuse`) under the column-pause lock.
/// A resume of that sibling decides whether to release the column from that
/// same `local_fuse`, so the two serialize: here the column's resume holds
/// the lock, frozen before its clear, and the sibling's resume waits for it,
/// then reads the clear and releases the column with itself. Without the
/// lock, the sibling's resume would read `local_fuse` still set and keep the
/// column paused, and the clear would then leave it paused with no reason at
/// all, which no resume of the sibling would ever release.
#[tokio::test]
async fn a_resume_held_by_a_paused_sibling_serializes_with_that_siblings_resume() {
    const PAUSE_LOCK: i64 = 9220;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total, total + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    quarantine::pause_column(&db.pool, "sib", "cost")
        .await
        .expect("pause cost too");
    assert!(cascade_edge_exists(&client, "sib", "cost", "sib", "total").await);
    assert_eq!(
        column_status_row(&client, "sib", "cost").await.map(|r| r.0),
        Some(true)
    );

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::BeforeSiblingHeldResume, "sib", PAUSE_LOCK);
    let pool = db.pool.clone();
    let mut held = tokio::spawn(with_scope(scope, async move {
        quarantine::resume_column(&pool, "sib", "cost").await
    }));
    let held_pid = tokio::select! {
        reached = reached => reached.expect("pause scope dropped").backend_pid,
        finished = &mut held => panic!("the resume of cost finished without reaching its clear: {finished:?}"),
    };

    let pool = db.pool.clone();
    let mut sibling =
        tokio::spawn(async move { quarantine::resume_column(&pool, "sib", "total").await });
    let waiting = tokio::select! {
        waiting = blocked_behind(&client, held_pid) => waiting,
        finished = &mut sibling => panic!("the resume of total didn't wait for cost's: {finished:?}"),
    };
    assert!(!holds_column_pause_lock(&client, waiting).await);

    release_gate(&gate, PAUSE_LOCK).await;
    assert_eq!(
        held.await.expect("resume task").expect("cost's resume"),
        Vec::<(String, String)>::new(),
        "cost stays paused with total"
    );
    assert_eq!(
        sibling.await.expect("resume task").expect("total's resume"),
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib".to_string(), "cost".to_string()),
        ],
        "total's resume releases cost with it"
    );
    assert_eq!(column_status_row(&client, "sib", "cost").await, None);
}

/// Issue #922: an `ALTER TRANSFORM` that only drops a field deletes that
/// field's pause row and the cascade edges into it, which a resume of the
/// column it reads deletes too, so it takes the column-pause lock, as `DROP
/// TRANSFORM` does (#921): it waits for a resume frozen holding it. The
/// resume reads the reader's definition again under the lock, so the
/// dropped field, which its cascade queued before the edit committed, is
/// skipped rather than resumed, and the reader's other field is resumed.
#[tokio::test]
async fn a_drop_only_alter_waits_for_a_resume_holding_the_column_pause_lock() {
    const PAUSE_LOCK: i64 = 9221;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(&db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT total + 1 AS t1, total + 2 AS t2")
        .await
        .expect("define the reader");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "t1", "sib", "total").await);
    assert!(cascade_edge_exists(&client, "sib_sum", "t2", "sib", "total").await);

    let gate = take_gate(&db, PAUSE_LOCK).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterResumedColumnDeleted, "sib", PAUSE_LOCK);
    let pool = db.pool.clone();
    let mut resume = tokio::spawn(with_scope(scope, async move {
        quarantine::resume_column(&pool, "sib", "total").await
    }));
    let resume_pid = tokio::select! {
        reached = reached => reached.expect("pause scope dropped").backend_pid,
        finished = &mut resume => panic!("the resume finished without reaching its delete: {finished:?}"),
    };

    let mut alter =
        tokio::spawn(async move { trellis.apply("ALTER TRANSFORM sib_sum DROP t1").await });
    let waiting = tokio::select! {
        waiting = blocked_behind(&client, resume_pid) => waiting,
        finished = &mut alter => panic!("the drop-only alter didn't wait for the resume: {finished:?}"),
    };
    assert!(
        !holds_column_pause_lock(&client, waiting).await,
        "the alter waits for the lock the resume holds"
    );

    release_gate(&gate, PAUSE_LOCK).await;
    let resumed = resume
        .await
        .expect("resume task")
        .expect("the resume commits");
    alter.await.expect("alter task").expect("the alter commits");
    assert!(resumed.contains(&("sib".to_string(), "total".to_string())));
    assert!(
        resumed.contains(&("sib_sum".to_string(), "t2".to_string())),
        "{resumed:?}"
    );
    assert!(
        !resumed.contains(&("sib_sum".to_string(), "t1".to_string())),
        "the dropped field isn't resumed: {resumed:?}"
    );
    let edges: i64 = client
        .query_one(
            "select count(*) from column_pause_cascades \
             where downstream_transform = 'sib_sum' or upstream_transform = 'sib'",
            &[],
        )
        .await
        .expect("count edges")
        .get(0);
    assert_eq!(edges, 0);
    assert_eq!(column_status_row(&client, "sib_sum", "t1").await, None);
    assert_eq!(column_status_row(&client, "sib_sum", "t2").await, None);
}

/// Issue #922 rule 5, for the capture pass: a Re-derive build's start takes
/// the column-pause lock. Behind a held lock it times out with nothing
/// written, and the pass goes on rather than failing: the definition stays
/// `waiting_to_backfill`, held back from the old build path's registration
/// marker as a start held by its capture gate is, and the next pass starts
/// it.
#[tokio::test]
async fn a_build_start_behind_a_held_column_pause_lock_waits_for_the_next_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_items_transform(&db, &client, "price + tax AS total").await;
    let defined = install_definition(
        &db.pool,
        "TRANSFORM sib_two FROM items SELECT price + 1 AS p1",
        &numeric_columns(&["id", "price", "tax", "bonus"]),
        "public",
    )
    .await
    .expect("define");
    async fn definition_status(client: &Client, id: i64) -> String {
        client
            .query_one(
                "select status from transform_definitions where id = $1",
                &[&id],
            )
            .await
            .expect("read the status")
            .get(0)
    }
    assert_eq!(
        definition_status(&client, defined.id).await,
        "waiting_to_backfill"
    );

    let mut holder = db.pool.get().await.expect("connect");
    let hold = holder.transaction().await.expect("begin");
    trellis::locks::lock_column_pauses(
        &*hold,
        trellis::config::DEFAULT_SCHEMA,
        trellis::locks::ColumnPauseLock::Exclusive,
        trellis::locks::ColumnPauseOp::Pause,
    )
    .await
    .expect("take the lock");

    let impatient = impatient_pool(&db);
    let mut pass = impatient.get().await.expect("connect");
    let taken = trellis::staging::build::start_ready_builds(&mut pass, &impatient, &[defined.id])
        .await
        .expect("the pass goes on past a start that waited out the lock");
    assert_eq!(taken, vec![defined.id], "held back from the old build path");
    assert_eq!(
        definition_status(&client, defined.id).await,
        "waiting_to_backfill"
    );

    hold.commit().await.expect("release the lock");
    let taken = trellis::staging::build::start_ready_builds(&mut pass, &impatient, &[defined.id])
        .await
        .expect("the next pass");
    assert_eq!(taken, vec![defined.id]);
    assert_eq!(definition_status(&client, defined.id).await, "backfilling");
}

// ---------------------------------------------------------------------
// Issue #955: a cascade pair re-checks, under the column-pause lock, that
// its reader still reads the paused column.
// ---------------------------------------------------------------------

/// `sib (total, cost)` over `items`, row 1 drained, and `sib_sum` reading
/// `cost` (`c1`) and `total` (`c2`), built and live. Returns the client and
/// a `Trellis` on `db`.
async fn seed_two_column_reader(db: &TestDatabase) -> (Client, Trellis) {
    let mut client = connect_raw(db.dsn()).await;
    seed_items_transform(db, &client, "price + tax AS total, price + 0 AS cost").await;
    change_item(&mut client, &db.pool, 1, None, (10, 5, 0)).await;
    let trellis = trellis_on(db).await;
    trellis
        .apply("TRANSFORM sib_sum FROM sib SELECT cost + 1 AS c1, total + 1 AS c2")
        .await
        .expect("define sib_sum");
    trellis::intake::markers::settle_registrations(&db.pool).await;
    drain_staged(&mut client, &db.pool).await;
    (client, trellis)
}

/// Starts `PAUSE sib.cost` and freezes its walk after it read `cost`'s
/// dependents (`sib_sum.c1`), holding no lock and no transaction. Returns
/// the walk's task, the gate to release it with and the gate's key.
async fn pause_cost_frozen_after_dependents_read(
    db: &TestDatabase,
    gate_key: i64,
) -> (tokio::task::JoinHandle<Result<(), ApplyError>>, Client, i64) {
    let gate = take_gate(db, gate_key).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterCascadeDependentsRead, "sib", gate_key);
    let pool = db.pool.clone();
    let mut pause = tokio::spawn(with_scope(scope, async move {
        quarantine::pause_column(&pool, "sib", "cost").await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut pause => panic!("the pause finished without reaching its walk: {finished:?}"),
    }
    (pause, gate, gate_key)
}

/// The race's first outcome: `c1` is edited to read `total` instead of
/// `cost` after the walk listed it as `cost`'s reader and before its pair.
/// The pair writes nothing: `c1` isn't paused on a column it doesn't read,
/// and has no edge for a later `DROP cost` to strand it on (#950).
#[tokio::test]
async fn a_cascade_pair_skips_a_reader_edited_to_stop_reading_the_column_after_the_walk_listed_it()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_two_column_reader(&db).await;

    let (pause, gate, key) = pause_cost_frozen_after_dependents_read(&db, 955).await;
    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c1 AS total + 2")
        .await
        .expect("the edit doesn't wait on the frozen walk");
    release_gate(&gate, key).await;
    pause.await.expect("pause task").expect("the walk finishes");

    assert!(column_status_row(&client, "sib", "cost").await.is_some());
    assert_eq!(
        column_status_row(&client, "sib_sum", "c1").await,
        None,
        "c1 no longer reads cost"
    );
    assert_eq!(cascade_edge_count(&client).await, 0);
    assert_eq!(cascade_pending(&client, "sib", "cost").await, Some(false));

    settle_and_drain(&mut client, &db).await;
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["17"]));
}

/// The race's second outcome: `c1` is dropped after the walk listed it and
/// before its pair. The pair writes no `column_status` row and no edge for
/// a field that no longer exists, so a field of the same name added later
/// isn't born paused (#309).
#[tokio::test]
async fn a_cascade_pair_skips_a_reader_dropped_after_the_walk_listed_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_two_column_reader(&db).await;

    let (pause, gate, key) = pause_cost_frozen_after_dependents_read(&db, 9550).await;
    trellis
        .apply("ALTER TRANSFORM sib_sum DROP c1")
        .await
        .expect("the drop doesn't wait on the frozen walk");
    release_gate(&gate, key).await;
    pause.await.expect("pause task").expect("the walk finishes");

    assert_eq!(
        column_status_row(&client, "sib_sum", "c1").await,
        None,
        "c1 no longer exists"
    );
    assert_eq!(cascade_edge_count(&client).await, 0);
    assert_eq!(cascade_pending(&client, "sib", "cost").await, Some(false));

    trellis
        .apply("ALTER TRANSFORM sib_sum ADD total + 3 AS c1")
        .await
        .expect("add a field of the dropped name");
    assert_eq!(
        column_status_row(&client, "sib_sum", "c1").await,
        None,
        "the new c1 reads a live column, so it isn't born paused"
    );
    settle_and_drain(&mut client, &db).await;
    assert_eq!(sib_sum_row(&client, 1, &["c1"]).await, some(&["18"]));
}

/// The pair still pauses a reader the race left alone: an edit of another
/// field of its definition, committed while the walk is frozen, doesn't make
/// the pair skip `c1`, which still reads `cost`.
#[tokio::test]
async fn a_cascade_pair_still_pauses_a_reader_when_an_edit_of_a_sibling_field_raced_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (client, trellis) = seed_two_column_reader(&db).await;

    let (pause, gate, key) = pause_cost_frozen_after_dependents_read(&db, 9551).await;
    trellis
        .apply("ALTER TRANSFORM sib_sum ALTER c2 AS total + 2")
        .await
        .expect("the edit doesn't wait on the frozen walk");
    release_gate(&gate, key).await;
    pause.await.expect("pause task").expect("the walk finishes");

    assert!(column_status_row(&client, "sib_sum", "c1").await.is_some());
    assert!(cascade_edge_exists(&client, "sib_sum", "c1", "sib", "cost").await);
    assert_eq!(column_status_row(&client, "sib_sum", "c2").await, None);
}

// ---------------------------------------------------------------------
// Issue #965: a resume's walk validates a reader's definition under the
// column-pause lock, not before it.
// ---------------------------------------------------------------------

/// A reader whose definition does not validate when the resume's walk reads
/// it (a source column named like its field `c1` shadows the field), and an
/// `ALTER TRANSFORM` that drops that field and commits before the pair's
/// transaction. The pair validates the definition it reads under the lock,
/// which is the edited one and validates, so it resumes `c2`. Validating the
/// definition read before the lock refused the reader on the old answer and
/// held `c2` as a pause of its own, with a reason that no longer held.
#[tokio::test]
async fn a_resume_validates_a_readers_definition_under_the_lock_an_edit_committed_before() {
    const GATE: i64 = 965;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, trellis) = seed_two_column_reader(&db).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "c2", "sib", "total").await);
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;
    // A column of `sib_sum`'s source named like its calculated field `c1`
    // shadows it, so define would refuse `sib_sum` now.
    client
        .batch_execute("alter table public.sib add column c1 numeric")
        .await
        .expect("add a column named like the reader's field");

    let gate = take_gate(&db, GATE).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterResumePairDefinitionRead, "sib_sum", GATE);
    let pool = db.pool.clone();
    let mut resume = tokio::spawn(with_scope(scope, async move {
        quarantine::resume_column(&pool, "sib", "total").await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut resume => panic!("the resume finished without reaching sib_sum's pair: {finished:?}"),
    }
    trellis
        .apply("ALTER TRANSFORM sib_sum DROP c1")
        .await
        .expect("the edit doesn't wait on the frozen walk, and validates");
    release_gate(&gate, GATE).await;

    let resumed = resume.await.expect("resume task").expect("the resume");
    assert_eq!(
        resumed,
        vec![
            ("sib".to_string(), "total".to_string()),
            ("sib_sum".to_string(), "c2".to_string()),
        ],
        "the edited definition validates, so c2 is resumed rather than held"
    );
    assert_eq!(
        column_status_row(&client, "sib_sum", "c2").await,
        None,
        "c2 is not left paused with a reason that no longer holds"
    );
    settle_and_drain(&mut client, &db).await;
    assert_eq!(sib_sum_row(&client, 1, &["c2"]).await, some(&["26"]));
}

/// The other direction, from host DDL rather than an edit: a reader whose
/// definition validates when the resume's walk reads it, and a source column
/// named like its field `c1` added before the pair's transaction. The pair
/// validates under the lock, sees the shadowing column and holds `c2` as a
/// pause of its own with the refusal as its reason. Validating before the
/// lock passed on the old schema and resumed a definition define would
/// refuse.
#[tokio::test]
async fn a_resume_holds_a_reader_whose_source_changed_after_the_walk_read_its_definition() {
    const GATE: i64 = 9651;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, _trellis) = seed_two_column_reader(&db).await;
    quarantine::pause_column(&db.pool, "sib", "total")
        .await
        .expect("pause total");
    assert!(cascade_edge_exists(&client, "sib_sum", "c2", "sib", "total").await);
    change_item(&mut client, &db.pool, 1, Some((10, 5, 0)), (20, 5, 0)).await;

    let gate = take_gate(&db, GATE).await;
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterResumePairDefinitionRead, "sib_sum", GATE);
    let pool = db.pool.clone();
    let mut resume = tokio::spawn(with_scope(scope, async move {
        quarantine::resume_column(&pool, "sib", "total").await
    }));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        finished = &mut resume => panic!("the resume finished without reaching sib_sum's pair: {finished:?}"),
    }
    client
        .batch_execute("alter table public.sib add column c1 numeric")
        .await
        .expect("add a column named like the reader's field");
    release_gate(&gate, GATE).await;

    let resumed = resume.await.expect("resume task").expect("the resume");
    assert_eq!(
        resumed,
        vec![("sib".to_string(), "total".to_string())],
        "sib_sum no longer validates under the lock, so c2 is not resumed"
    );
    let (local_fuse, last_error) = column_status_row(&client, "sib_sum", "c2")
        .await
        .expect("c2 stays paused");
    assert!(
        local_fuse,
        "with its edge gone, c2 is held by a pause of its own"
    );
    let last_error = last_error.expect("the held column says why");
    assert!(
        last_error.contains("no longer validates")
            && last_error.contains("shares its name with a source column"),
        "the reason names the refusal seen under the lock, got {last_error:?}"
    );
}
