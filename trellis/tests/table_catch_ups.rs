//! Issue #522: a marker that re-reads a table because its applying readers
//! may have missed changes to it is a go-live catch-up for every one of them
//! (issue #476, ADR-0002's "What `live` promises"): an explicit
//! `Trellis::request_backfill` and a table whose capture triggers are
//! installed (issue #533)
//! (`intake::markers::park_table_catch_ups`).
//!
//! Someone asks for a re-backfill because a target may have drifted from its
//! source, a delete that never reached it being the case a re-read alone
//! can't repair: the discharge enumerates the keys the source still has, so a
//! target row whose source row is gone is only removed by the orphan sweep,
//! and the sweep covers `catching_up` readers. So each applying reader moves
//! to `catching_up` when the marker is parked, and the discharge re-reads the
//! table, sweeps the reader's target and flips it back `live`.
//!
//! Each test brings its definitions `live`, then changes the source with no
//! CDC staged for it (the lost change the marker is there to repair), parks
//! the marker, and drives the discharge and the drain directly.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::markers;
use trellis::staging::{CdcOp, StagedChange, StagedWatermark, apply, seal};
use trellis::staging::{has_pending, retire_drained_segments};
use trellis::{Config, Trellis, TrellisOptions};

const TEST_NAME: &str = "table_catch_ups_test";
const WAKE: &str = "table_catch_ups_wake";

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

async fn connect_trellis(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        while apply::drain_once(pool, outcome.sealed_seg_seq, TEST_NAME, 1, WAKE)
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

/// Builds every registered definition and discharges its go-live catch-up.
async fn bring_live(pool: &trellis::Pool, client: &mut Client) {
    markers::settle_registrations(pool).await;
    drain_to_quiescence(pool, client).await;
    let not_live: i64 = client
        .query_one(
            "select count(*) from transform_definitions where status <> 'live'",
            &[],
        )
        .await
        .expect("count definitions not live")
        .get(0);
    assert_eq!(
        not_live, 0,
        "every definition is live before the test starts"
    );
}

/// One capture reconcile pass over `tables` (every other table's capture is
/// uninstalled), which parks a join marker for each table it installs.
async fn capture(client: &mut Client, tables: &[&str]) {
    let desired: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    let outcome = trellis::capture::reconcile::reconcile(
        client,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("capture pass");
    assert!(
        outcome.failed.is_empty() && outcome.waiting.is_empty(),
        "{outcome:?}"
    );
}

/// Drops `table`'s capture, as an operator dropping its triggers would.
async fn uninstall(client: &mut Client, table: &str) {
    trellis::capture::install::uninstall(client, DEFAULT_SCHEMA, table, None)
        .await
        .expect("uninstall capture")
        .done()
        .expect("nothing holds the table");
}

/// One discharge pass over every settled marker, then a drain of what it
/// staged. Asserts the pass discharged every marker, so a deferral shows up
/// here rather than as a wrong target further on.
async fn discharge_markers(pool: &trellis::Pool, client: &mut Client) {
    markers::run_pending_backfills(client, WAKE, &StagedWatermark::saturated(), Duration::ZERO)
        .await
        .expect("run_pending_backfills");
    let markers: i64 = client
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count markers")
        .get(0);
    assert_eq!(markers, 0, "the pass discharged every marker");
    drain_to_quiescence(pool, client).await;
}

async fn status_of(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("read the definition's status")
        .get(0)
}

/// Commits `sql` and stages `change` (built from the write's LSN) in one
/// transaction, as capture would stage it.
async fn commit_and_stage(client: &mut Client, sql: &str, change: impl Fn(PgLsn) -> StagedChange) {
    let txn = client.transaction().await.expect("begin source write");
    txn.batch_execute(sql).await.expect("source write");
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the write's lsn")
        .get(0);
    trellis::staging::append(&txn, &[change(lsn)])
        .await
        .expect("stage the change's CDC row");
    txn.commit().await.expect("commit source write");
}

fn sales_insert(lsn: PgLsn, id: i32, sku: &str, amount: i32) -> StagedChange {
    StagedChange::Cdc {
        src_table: "public.sales".to_string(),
        key: id.to_string(),
        op: CdcOp::Insert,
        lsn: Some(lsn),
        old_image: None,
        new_image: Some(format!(
            r#"{{"id":"{id}","sku":"{sku}","amount":"{amount}"}}"#
        )),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
    }
}

fn sales_types() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("sku".to_string(), ValueType::Text),
        ("amount".to_string(), ValueType::Numeric),
    ])
}

async fn seed_sales(client: &Client) {
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), \
                                             (4, 'a', 1000)",
        )
        .await
        .expect("create and seed sales");
}

/// A 1-1 target row whose source row was deleted with no CDC reaching the
/// target is gone once the re-backfill discharges. Before #522 the
/// definition stayed `live`, the discharge swept nothing, and row 4 stayed.
/// This is the catch-up flavour, for a source the Re-derive build leaves to
/// the old build (`held_out`); [`a_requested_backfill_rebuilds_a_stale_one_to_one_row`]
/// is the Re-derive flavour.
#[tokio::test]
async fn a_requested_backfill_sweeps_a_stale_one_to_one_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    markers::hold_out_of_the_rederive_build(&client, "public.sales")
        .await
        .expect("hold sales out of the Re-derive build");
    install_definition(
        &db.pool,
        "TRANSFORM sales_copy FROM sales SELECT amount AS amt",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the 1-1");
    bring_live(&db.pool, &mut client).await;

    client
        .batch_execute("delete from public.sales where id = 4")
        .await
        .expect("delete a source row, its CDC lost");
    let trellis = connect_trellis(db.dsn()).await;
    trellis
        .request_backfill("sales")
        .await
        .expect("request a re-backfill");
    assert_eq!(
        status_of(&client, "public.sales_copy").await,
        "catching_up",
        "a definition whose re-backfill is pending isn't in its steady state"
    );

    // A change committed while the re-backfill is pending still applies.
    commit_and_stage(
        &mut client,
        "insert into public.sales values (5, 'b', 50)",
        |lsn| sales_insert(lsn, 5, "b", 50),
    )
    .await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sales_copy").await, "live");
    assert_eq!(
        copy_rows(&client).await,
        vec![
            (1, "5".to_string()),
            (2, "7".to_string()),
            (3, "2".to_string()),
            (5, "50".to_string()),
        ],
        "the row no source row backs is swept, and the rest match the source"
    );
}

async fn copy_rows(client: &Client) -> Vec<(i32, String)> {
    client
        .query("select id, amt::text from sales_copy order by id", &[])
        .await
        .expect("read sales_copy")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn sku_totals_rows(client: &Client) -> Vec<(String, String)> {
    client
        .query("select sku, total::text from sku_totals order by sku", &[])
        .await
        .expect("read sku_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// The jobs and segment a definition's rebuild left: its plan and sweep
/// jobs (not yet done), and its `build_seg`.
async fn rebuild_rows(client: &Client, target: &str) -> (i64, i64, Option<i64>, Option<String>) {
    let row = client
        .query_one(
            "select (select count(*) from backfill_chunks bc \
                     where bc.definition_id = d.id and bc.kind = 'plan' and not bc.done), \
                    (select count(*) from backfill_chunks bc \
                     where bc.definition_id = d.id and bc.kind = 'sweep' and not bc.done), \
                    d.build_seg, d.build \
             from transform_definitions d where d.target_table = $1",
            &[&target],
        )
        .await
        .expect("read the definition's rebuild jobs");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

/// Runs every claimable plan job and `rederive` chunk, as a drain worker
/// would, but no sweep and no merge, so a rebuild is left part way.
async fn run_plan_and_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const BY: &str = "table_catch_ups_test_build_worker";
    let options = trellis::staging::build::WorkerOptions {
        chunk_rows: trellis::staging::build::DEFAULT_CHUNK_ROWS,
        drain_batch_cap: usize::MAX,
        heartbeat_interval: Duration::from_secs(5),
        reclaim_ttl: Duration::from_secs(60),
    };
    loop {
        let claimed = {
            let client = pool.get().await.expect("acquire connection");
            chunk_queue::claim_chunks_of(
                &**client,
                BY,
                1,
                &[chunk_queue::KIND_PLAN, chunk_queue::KIND_REDERIVE],
            )
            .await
            .expect("claim build work")
        };
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            trellis::staging::build::run_claimed(pool, chunk, BY, &options).await;
        }
    }
}

async fn active_seq(client: &Client) -> i64 {
    client
        .query_one("select active_seq from segment_pointer", &[])
        .await
        .expect("read the active segment")
        .get(0)
}

/// The Re-derive flavour (#625 F7): the call's own transaction moves the
/// 1-1 definition `live -> backfilling` and enqueues a plan job and a sweep
/// job, one row each, so it returns with the rebuild already visible. No
/// marker catch-up is parked for it. The definition keeps applying while the
/// rebuild runs, and ends `live` with the stale row swept.
#[tokio::test]
async fn a_requested_backfill_rebuilds_a_stale_one_to_one_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sales_copy FROM sales SELECT amount AS amt",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the 1-1");
    bring_live(&db.pool, &mut client).await;

    client
        .batch_execute("delete from public.sales where id = 4")
        .await
        .expect("delete a source row, its CDC lost");
    let trellis = connect_trellis(db.dsn()).await;
    trellis
        .request_backfill("sales")
        .await
        .expect("request a re-backfill");
    assert_eq!(
        status_of(&client, "public.sales_copy").await,
        "backfilling",
        "the call's own transaction started the rebuild"
    );
    let (plans, sweeps, build_seg, build) = rebuild_rows(&client, "public.sales_copy").await;
    assert_eq!((plans, sweeps), (1, 1), "one plan and one sweep, a rebuild");
    assert_eq!(build.as_deref(), Some("rederive"));
    assert_eq!(
        build_seg,
        Some(active_seq(&client).await),
        "pages of the segments the rebuild's start closed re-derive their keys"
    );

    // A change committed while the rebuild is pending still applies.
    commit_and_stage(
        &mut client,
        "insert into public.sales values (5, 'b', 50)",
        |lsn| sales_insert(lsn, 5, "b", 50),
    )
    .await;
    markers::settle_builds(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sales_copy").await, "live");
    assert_eq!(
        copy_rows(&client).await,
        vec![
            (1, "5".to_string()),
            (2, "7".to_string()),
            (3, "2".to_string()),
            (5, "50".to_string()),
        ],
        "the row no source row backs is swept, and the rest match the source"
    );
}

/// Issue #1202: a source outside `public` and Trellis's own schemas is named
/// by its qualified name or, when exactly one captured table carries it, its
/// bare one; a bare name two captured sources share is refused naming both.
#[tokio::test]
async fn request_backfill_names_a_source_outside_the_search_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create schema shop; create schema blog; \
             create table shop.orders (id integer primary key, amount integer); \
             create table blog.orders (id integer primary key, amount integer); \
             create table shop.refunds (id integer primary key, amount integer)",
        )
        .await
        .expect("create the schemas and tables");
    let types = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ]);
    for sql in [
        "TRANSFORM shop_orders_copy FROM shop.orders SELECT amount AS amt",
        "TRANSFORM blog_orders_copy FROM blog.orders SELECT amount AS amt",
        "TRANSFORM refunds_copy FROM shop.refunds SELECT amount AS amt",
    ] {
        install_definition(&db.pool, sql, &types, "public")
            .await
            .expect("install the 1-1");
    }
    let trellis = connect_trellis(db.dsn()).await;

    trellis
        .request_backfill("shop.orders")
        .await
        .expect("a qualified name resolves");
    trellis
        .request_backfill("refunds")
        .await
        .expect("a bare name only one captured source carries resolves");

    let err = trellis.request_backfill("orders").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        matches!(err, trellis::TrellisError::AmbiguousSourceTable { .. })
            && msg.contains("shop.orders")
            && msg.contains("blog.orders"),
        "an ambiguous bare name is refused naming both: {msg}"
    );
    assert!(matches!(
        trellis.request_backfill("public.orders").await,
        Err(trellis::TrellisError::SourceTableNotFound(_))
    ));
}

/// The aggregate case: group `b` lost its only row and group `a` one of its
/// rows, neither delete reaching the target. The re-read re-derives `a`
/// from what it still has, and only the sweep can drop `b`. This is the
/// catch-up flavour ([`a_requested_backfill_sweeps_a_stale_one_to_one_row`]'s
/// held-out source).
#[tokio::test]
#[ignore = "pins the per-row ledger, which the default path no longer takes: run with TRELLIS_DEV_GROUP_BASIS=0 and --ignored; #1105 deletes it"]
async fn a_requested_backfill_sweeps_a_stale_aggregate_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    markers::hold_out_of_the_rederive_build(&client, "public.sales")
        .await
        .expect("hold sales out of the Re-derive build");
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku WITHOUT SOURCE INDEX SELECT sum(amount) AS total",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the aggregate");
    bring_live(&db.pool, &mut client).await;

    client
        .batch_execute("delete from public.sales where id in (3, 4)")
        .await
        .expect("delete source rows, their CDC lost");
    let trellis = connect_trellis(db.dsn()).await;
    trellis
        .request_backfill("sales")
        .await
        .expect("request a re-backfill");
    assert_eq!(status_of(&client, "public.sku_totals").await, "catching_up");

    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(
        sku_totals_rows(&client).await,
        vec![("a".to_string(), "12".to_string())]
    );
}

/// The Re-derive flavour of the aggregate case (#625 F7): `request_backfill`
/// on a `live` aggregate returns with the status already `backfilling`, so a
/// caller that polls for `live` and then takes its token waits out the
/// rebuild. A group-basis aggregate's rebuild is its sweep job alone, which
/// recomputes `a` and drops `b`.
#[tokio::test]
async fn a_requested_backfill_rebuilds_a_stale_aggregate_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku WITHOUT SOURCE INDEX SELECT sum(amount) AS total",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the aggregate");
    bring_live(&db.pool, &mut client).await;

    client
        .batch_execute("delete from public.sales where id in (3, 4)")
        .await
        .expect("delete source rows, their CDC lost");
    let trellis = connect_trellis(db.dsn()).await;
    trellis
        .request_backfill("sales")
        .await
        .expect("request a re-backfill");
    assert_eq!(status_of(&client, "public.sku_totals").await, "backfilling");
    let (plans, sweeps, build_seg, _) = rebuild_rows(&client, "public.sku_totals").await;
    assert_eq!((plans, sweeps), (0, 1), "one sweep, a group-basis rebuild");
    assert_eq!(build_seg, Some(active_seq(&client).await));
    assert_eq!(
        sku_totals_rows(&client).await,
        vec![
            ("a".to_string(), "1012".to_string()),
            ("b".to_string(), "2".to_string())
        ],
        "the call itself repairs nothing; the background rebuild does"
    );

    markers::settle_builds(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(
        sku_totals_rows(&client).await,
        vec![("a".to_string(), "12".to_string())]
    );
}

/// A request while a rebuild runs rebuilds again (#625 F7): the first
/// rebuild may have read rows before whatever the second request repairs,
/// so the call enqueues a rebuild of its own (for a group-basis aggregate,
/// a sweep job alone), and the definition goes `live` when both rebuilds
/// are done.
#[tokio::test]
async fn a_requested_backfill_during_a_rebuild_enqueues_another() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku WITHOUT SOURCE INDEX SELECT sum(amount) AS total",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the aggregate");
    bring_live(&db.pool, &mut client).await;
    let trellis = connect_trellis(db.dsn()).await;

    trellis.request_backfill("sales").await.expect("request");
    run_plan_and_chunks(&db.pool).await;
    assert_eq!(
        rebuild_rows(&client, "public.sku_totals").await.0,
        0,
        "the first rebuild's plan has run"
    );
    client
        .batch_execute("delete from public.sales where id in (3, 4)")
        .await
        .expect("delete source rows, their CDC lost");
    trellis
        .request_backfill("sales")
        .await
        .expect("request again");

    assert_eq!(status_of(&client, "public.sku_totals").await, "backfilling");
    let (plans, sweeps, ..) = rebuild_rows(&client, "public.sku_totals").await;
    assert_eq!(
        (plans, sweeps),
        (0, 2),
        "the second request enqueued a sweep of its own"
    );

    markers::settle_builds(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(
        sku_totals_rows(&client).await,
        vec![("a".to_string(), "12".to_string())]
    );
}

/// Only the readers the Re-derive build takes are rebuilt: a reader the
/// build leaves to the old build keeps the marker's catch-up (#625 F7).
#[tokio::test]
async fn a_requested_backfill_rebuilds_only_the_readers_the_build_takes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    markers::hold_target_out_of_the_rederive_build(&client, "public.sales", "public.sales_held")
        .await
        .expect("hold sales_held out of the Re-derive build");
    for definition in [
        "TRANSFORM sales_held FROM sales SELECT amount AS amt",
        "TRANSFORM sales_copy FROM sales SELECT amount AS amt",
    ] {
        install_definition(&db.pool, definition, &sales_types(), "public")
            .await
            .expect(definition);
    }
    bring_live(&db.pool, &mut client).await;
    let trellis = connect_trellis(db.dsn()).await;

    trellis
        .request_backfill("sales")
        .await
        .expect("request a re-backfill");

    assert_eq!(
        status_of(&client, "public.sales_copy").await,
        "backfilling",
        "the plain 1-1 the Re-derive build takes is rebuilt"
    );
    assert_eq!(
        status_of(&client, "public.sales_held").await,
        "catching_up",
        "the held-out reader keeps the marker's catch-up"
    );
}

/// A table whose capture an operator dropped while a definition applied from
/// it is captured again at the next reconcile. Its readers missed everything
/// written meanwhile, so the install's join marker is their catch-up. This is
/// the catch-up flavour, for a source the Re-derive build leaves to the old
/// build; the Re-derive flavour is [`a_table_whose_capture_is_reinstalled_rebuilds_its_readers`].
#[tokio::test]
async fn a_table_whose_capture_is_reinstalled_catches_up_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    markers::hold_out_of_the_rederive_build(&client, "public.sales")
        .await
        .expect("hold sales out of the Re-derive build");
    install_definition(
        &db.pool,
        "TRANSFORM sales_copy FROM sales SELECT amount AS amt",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the 1-1");
    bring_live(&db.pool, &mut client).await;
    capture(&mut client, &["public.sales"]).await;
    discharge_markers(&db.pool, &mut client).await;

    uninstall(&mut client, "public.sales").await;
    client
        .batch_execute("delete from public.sales where id = 4")
        .await
        .expect("delete a row while sales isn't captured");
    capture(&mut client, &["public.sales"]).await;
    assert_eq!(status_of(&client, "public.sales_copy").await, "catching_up");

    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sales_copy").await, "live");
    let ids: Vec<i32> = client
        .query("select id from sales_copy order by id", &[])
        .await
        .expect("read sales_copy")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

/// The Re-derive flavour (#625 F7): the install's own transaction rebuilds
/// the readers the Re-derive build takes, so they read `backfilling` from the
/// install's commit, with a plan and a sweep job, and end `live` with the
/// row deleted while the table wasn't captured gone.
#[tokio::test]
async fn a_table_whose_capture_is_reinstalled_rebuilds_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sales_copy FROM sales SELECT amount AS amt",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the 1-1");
    bring_live(&db.pool, &mut client).await;
    capture(&mut client, &["public.sales"]).await;
    markers::settle_builds(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.sales_copy").await, "live");

    uninstall(&mut client, "public.sales").await;
    client
        .batch_execute("delete from public.sales where id = 4")
        .await
        .expect("delete a row while sales isn't captured");
    capture(&mut client, &["public.sales"]).await;
    assert_eq!(status_of(&client, "public.sales_copy").await, "backfilling");
    let (plans, sweeps, build_seg, _) = rebuild_rows(&client, "public.sales_copy").await;
    assert_eq!((plans, sweeps), (1, 1), "one plan and one sweep, a rebuild");
    assert_eq!(build_seg, Some(active_seq(&client).await));

    markers::settle_builds(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sales_copy").await, "live");
    let ids: Vec<i32> = copy_rows(&client).await.into_iter().map(|r| r.0).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}
