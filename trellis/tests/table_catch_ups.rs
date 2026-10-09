//! Issue #522: a marker that re-reads a table because its applying readers
//! may have missed changes to it is a go-live catch-up for every one of them
//! (issue #476, ADR-0016's "What `live` promises"): an explicit
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
//! table, sweeps the reader's target and flips it back `live`. A to-one
//! relationship consumer reads the to-side's settled projection rather than
//! the table, so the discharge refreshes that projection too.
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
use trellis::defs::{ValueType, create_relationship, install_definition};
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
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        while apply::drain_once(pool, outcome.sealed_seg_seq, TEST_NAME, 1, WAKE, &watermark)
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
        group_key: None,
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

/// The aggregate case: group `b` lost its only row and group `a` one of its
/// rows, neither delete reaching the target. The re-read re-derives `a`
/// from what it still has, and only the sweep can drop `b`. This is the
/// catch-up flavour ([`a_requested_backfill_sweeps_a_stale_one_to_one_row`]'s
/// held-out source).
#[tokio::test]
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
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
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
/// rebuild. The plan re-derives `a` and the sweep drops `b`.
#[tokio::test]
async fn a_requested_backfill_rebuilds_a_stale_aggregate_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
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
    assert_eq!((plans, sweeps), (1, 1), "one plan and one sweep, a rebuild");
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
/// plan's chunks may have read rows before whatever the second request
/// repairs, so the call enqueues a plan and a sweep of its own, and the
/// definition goes `live` when both rebuilds are done.
#[tokio::test]
async fn a_requested_backfill_during_a_rebuild_enqueues_another() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &sales_types(),
        "public",
    )
    .await
    .expect("install the aggregate");
    bring_live(&db.pool, &mut client).await;
    let trellis = connect_trellis(db.dsn()).await;

    trellis.request_backfill("sales").await.expect("request");
    trellis
        .request_backfill("sales")
        .await
        .expect("request again");

    assert_eq!(status_of(&client, "public.sku_totals").await, "backfilling");
    let (plans, sweeps, ..) = rebuild_rows(&client, "public.sku_totals").await;
    assert_eq!((plans, sweeps), (2, 2), "each request enqueued its own");

    client
        .batch_execute("delete from public.sales where id in (3, 4)")
        .await
        .expect("delete source rows, their CDC lost");
    markers::settle_builds(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(
        sku_totals_rows(&client).await,
        vec![("a".to_string(), "12".to_string())]
    );
}

/// Only the readers the Re-derive build takes are rebuilt: a relationship
/// consumer of the same table keeps the catch-up (#625 F7).
#[tokio::test]
async fn a_requested_backfill_rebuilds_only_the_readers_the_build_takes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;
    install_definition(
        &db.pool,
        "TRANSFORM order_copy FROM orders SELECT id AS oid",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("customer_id".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install a plain 1-1 over orders");
    bring_live(&db.pool, &mut client).await;

    trellis
        .request_backfill("orders")
        .await
        .expect("request a re-backfill");

    assert_eq!(
        status_of(&client, "public.order_copy").await,
        "backfilling",
        "the plain 1-1 is rebuilt"
    );
    assert_eq!(
        status_of(&client, "public.order_names").await,
        "catching_up",
        "the relationship consumer keeps the marker's catch-up"
    );
}

/// Seeds `customers` (the to-side) and `orders`, declares
/// the to-one `customer` relationship, registers `order_names` (which reads
/// `customer.name` through it) and brings it `live`. Returns the define-only
/// `Trellis` it registered through.
async fn live_relationship_consumer(
    dsn: &str,
    pool: &trellis::Pool,
    client: &mut Client,
) -> Trellis {
    client
        .batch_execute(
            "create table public.customers (id integer primary key, name text); \
             create table public.orders (id integer primary key, customer_id integer); \
             insert into public.customers values (1, 'ann'), (2, 'bob'); \
             insert into public.orders values (10, 1), (11, 2), (12, 1)",
        )
        .await
        .expect("create and seed customers and orders");
    create_relationship(
        pool,
        "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
    )
    .await
    .expect("declare the relationship");
    let trellis = connect_trellis(dsn).await;
    trellis
        .apply("TRANSFORM order_names FROM orders SELECT customer.name AS customer_name")
        .await
        .expect("register the consumer");
    bring_live(pool, client).await;
    trellis
}

async fn order_names(client: &Client) -> Vec<(i32, Option<String>)> {
    client
        .query("select id, customer_name from order_names order by id", &[])
        .await
        .expect("read order_names")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// `order_names` after customer 1 was renamed `ann2`.
fn renamed() -> Vec<(i32, Option<String>)> {
    vec![
        (10, Some("ann2".to_string())),
        (11, Some("bob".to_string())),
        (12, Some("ann2".to_string())),
    ]
}

/// A re-backfill of a relationship's to-side catches up the consumer that
/// reads through it, not only the definitions whose source it is. The
/// consumer reads the to-side's settled projection, which the lost rename
/// never reached, so the discharge refreshes the projection before the
/// re-read's `Recompute`s re-derive the consumer from it.
#[tokio::test]
async fn a_requested_backfill_of_a_to_side_catches_up_its_relationship_consumer() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    assert_eq!(
        status_of(&client, "public.order_names").await,
        "catching_up",
        "the consumer reads the re-backfilled table through its relationship"
    );

    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(order_names(&client).await, renamed());
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

fn customer_update(lsn: PgLsn, id: i32, old: &str, new: &str) -> StagedChange {
    let image = |name: &str| format!(r#"{{"id":"{id}","name":"{name}"}}"#);
    StagedChange::Cdc {
        src_table: "public.customers".to_string(),
        key: id.to_string(),
        op: CdcOp::Update,
        lsn: Some(lsn),
        old_image: Some(image(old)),
        new_image: Some(image(new)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// The `customer` relationship's settled projection row for `id`: its
/// `name`, or `None` when the projection has no row for the key.
async fn projected_name(client: &Client, id: i32) -> Option<Option<String>> {
    let projection: String = client
        .query_one(
            "select rp.projection_table from relationship_projections rp \
             join relationship_definitions rd on rd.id = rp.relationship_id \
             where rd.name = 'customer'",
            &[],
        )
        .await
        .expect("find the customer projection")
        .get(0);
    client
        .query_opt(
            &format!("select name from \"{projection}\" where id = $1"),
            &[&id],
        )
        .await
        .expect("read the customer projection")
        .map(|r| r.get(0))
}

/// Issue #531: a rename staged but not yet drained when a later rename is
/// lost (the table isn't captured) is older than what the rejoin's refresh
/// writes into the projection, and must not put its image back. Under
/// trigger capture the install's join marker is gated on the table's
/// pending ring rows (#622 C3), so the refresh waits for the older rename to
/// drain rather than relying on the relationship's stamp.
#[tokio::test]
async fn pending_older_to_side_cdc_does_not_undo_a_rejoins_projection_refresh() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann1' where id = 1",
        |lsn| customer_update(lsn, 1, "ann", "ann1"),
    )
    .await;
    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer while customers isn't captured");
    capture(&mut client, &["public.customers", "public.orders"]).await;
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    let gated: Vec<String> = client
        .query(
            "select table_name from pending_backfill where capture_gate_lsn is not null",
            &[],
        )
        .await
        .expect("read gated markers")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        gated,
        ["public.customers"],
        "the join marker waits for the older rename still in the ring"
    );
    drain_to_quiescence(&db.pool, &mut client).await;
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Issue #531 through a requested re-backfill, which parks no capture gate:
/// the refresh runs while the older rename is still in the ring, and the
/// rename drains after it. The refresh stamped the relationship, so a record
/// at or below the stamp writes the projection from the live row rather
/// than putting its image back.
#[tokio::test]
async fn pending_older_to_side_cdc_does_not_undo_a_requested_projection_refresh() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann1' where id = 1",
        |lsn| customer_update(lsn, 1, "ann", "ann1"),
    )
    .await;
    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Issue #531's delete shape: the pending rename would re-insert the
/// projection row the refresh removed for a customer whose delete was lost.
#[tokio::test]
async fn pending_older_to_side_cdc_does_not_resurrect_a_refreshed_out_projection_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann1' where id = 1",
        |lsn| customer_update(lsn, 1, "ann", "ann1"),
    )
    .await;
    client
        .batch_execute("delete from public.customers where id = 1")
        .await
        .expect("delete a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(projected_name(&client, 1).await, None);
    assert_eq!(
        order_names(&client).await,
        vec![(10, None), (11, Some("bob".to_string())), (12, None)]
    );
}

/// A customer's insert or delete CDC, as capture stages it.
fn customer_cdc(lsn: PgLsn, op: CdcOp, id: i32, name: &str) -> StagedChange {
    let image = Some(format!(r#"{{"id":"{id}","name":"{name}"}}"#));
    let (old_image, new_image) = match op {
        CdcOp::Delete => (image, None),
        _ => (None, image),
    };
    StagedChange::Cdc {
        src_table: "public.customers".to_string(),
        key: id.to_string(),
        op,
        lsn: Some(lsn),
        old_image,
        new_image,
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// Issue #726: a customer inserted before a projection refresh and deleted
/// after it, with both changes still in the ring, folds to a record with no
/// image at all, so no reverse record ever names the customer. A refresh
/// that wrote the customer's projection row from the live table left that
/// row behind, and every order pointing at the customer kept its name. The
/// refresh leaves a key a pending change names to that change.
#[tokio::test]
async fn a_refresh_leaves_a_to_side_key_with_pending_cdc_to_that_cdc() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "insert into public.customers values (3, 'cat')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 3, "cat"),
    )
    .await;
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 3",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 3, "cat"),
    )
    .await;
    commit_and_stage(
        &mut client,
        "insert into public.orders values (13, 3)",
        |lsn| StagedChange::Cdc {
            src_table: "public.orders".to_string(),
            key: "13".to_string(),
            op: CdcOp::Insert,
            lsn: Some(lsn),
            old_image: None,
            new_image: Some(r#"{"id":"13","customer_id":"3"}"#.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: Some(vec!["3".to_string()]),
        },
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(projected_name(&client, 3).await, None);
    assert_eq!(
        order_names(&client).await,
        vec![
            (10, Some("ann".to_string())),
            (11, Some("bob".to_string())),
            (12, Some("ann".to_string())),
            (13, None),
        ]
    );
}

/// Issue #726 through the other catch-up insert: registering a second
/// consumer of the relationship catches the projection up from the live
/// to-side (`ensure_relationship_projection_in_txn`), and must leave a
/// customer whose insert is still in the ring to that insert, or the
/// customer's delete, folded with it, leaves the row behind.
#[tokio::test]
async fn a_new_consumers_projection_catch_up_leaves_a_key_with_pending_cdc_to_that_cdc() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "insert into public.customers values (3, 'cat')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 3, "cat"),
    )
    .await;
    trellis
        .apply("TRANSFORM order_names2 FROM orders SELECT customer.name AS customer_name")
        .await
        .expect("register a second consumer");
    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 3",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 3, "cat"),
    )
    .await;
    bring_live(&db.pool, &mut client).await;

    assert_eq!(projected_name(&client, 3).await, None);
}

/// Issue #726 when the key's latest pending change is an update that keeps
/// it, so names it in both images: that change still writes the key, and the
/// refresh leaves it alone.
#[tokio::test]
async fn a_refresh_leaves_a_key_whose_latest_pending_cdc_keeps_it_to_that_cdc() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "insert into public.customers values (3, 'cat')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 3, "cat"),
    )
    .await;
    commit_and_stage(
        &mut client,
        "update public.customers set name = 'cat2' where id = 3",
        |lsn| customer_update(lsn, 3, "cat", "cat2"),
    )
    .await;
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 3",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 3, "cat2"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(projected_name(&client, 3).await, None);
}

/// Issue #726's skip must not swallow a key whose pending changes end with it
/// gone while the to-side has it back through a write no pending change
/// carries (a lost change, as a requested re-backfill repairs). The pending
/// insert and delete fold to a record with no image, so only the refresh can
/// write the customer's projection row.
#[tokio::test]
async fn a_refresh_writes_a_key_back_whose_pending_cdc_ends_with_it_gone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "insert into public.customers values (3, 'cat')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 3, "cat"),
    )
    .await;
    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 3",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 3, "cat"),
    )
    .await;
    client
        .batch_execute("insert into public.customers values (3, 'cat2')")
        .await
        .expect("re-insert a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 3).await,
        Some(Some("cat2".to_string()))
    );
}

/// Issue #531 for a deferred reverse (issue #134): a rename a guard deferred
/// before the refresh keeps its original LSN, so its retry is at or below the
/// stamp as well and must not put its image back either.
#[tokio::test]
async fn a_deferred_older_reverse_does_not_undo_a_projection_refresh() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;
    let relationship_id: i64 = client
        .query_one(
            "select id from relationship_definitions where name = 'customer'",
            &[],
        )
        .await
        .expect("read the relationship id")
        .get(0);

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann1' where id = 1",
        |lsn| StagedChange::RelationshipReverseDeferred {
            src_table: format!("\u{1f}trellis-rel-reverse-deferred:{relationship_id}"),
            key: "1".to_string(),
            old_image: Some(r#"{"id":"1","name":"ann"}"#.to_string()),
            new_image: Some(r#"{"id":"1","name":"ann1"}"#.to_string()),
            lsn: Some(lsn),
            src_changed: None,
            origin_lsn: None,
            relationship_id,
            retry_count: 1,
            group_key: None,
        },
    )
    .await;
    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Issue #531's cost bound: a to-side change after the refresh's stamp
/// applies its own image without reading the live row. The live row here
/// has moved on by a later change whose CDC was lost after the refresh, which
/// no refresh has read, so the projection taking the image rather than the
/// live name shows the check never ran for a record above the stamp.
#[tokio::test]
async fn a_to_side_change_after_the_refresh_applies_its_image_unchecked() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann3' where id = 1",
        |lsn| customer_update(lsn, 1, "ann2", "ann3"),
    )
    .await;
    client
        .batch_execute("update public.customers set name = 'ann4' where id = 1")
        .await
        .expect("rename the customer again, the CDC lost");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann3".to_string()))
    );
}

fn customers_truncate(lsn: PgLsn) -> StagedChange {
    StagedChange::Truncate {
        src_table: "public.customers".to_string(),
        lsn,
        origin_lsn: None,
        src_changed: None,
    }
}

/// Issue #531's truncate shape: a to-side `TRUNCATE` streamed but not yet
/// drained when a re-insert is lost is older than what the refresh writes
/// into the projection. Its clear (issue #168) must not empty the refreshed
/// projection: the refresh already read the table after the truncate. A
/// truncate after the refresh still clears it.
#[tokio::test]
async fn a_pending_to_side_truncate_does_not_empty_a_refreshed_projection() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(&mut client, "truncate public.customers", customers_truncate).await;
    client
        .batch_execute("insert into public.customers values (1, 'ann2')")
        .await
        .expect("re-insert a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(
        order_names(&client).await,
        vec![
            (10, Some("ann2".to_string())),
            (11, None),
            (12, Some("ann2".to_string()))
        ]
    );

    commit_and_stage(&mut client, "truncate public.customers", customers_truncate).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(projected_name(&client, 1).await, None);
    assert_eq!(
        order_names(&client).await,
        vec![(10, None), (11, None), (12, None)]
    );
}

/// Issue #531 on issue #135's fairness escalation: a deferred rename at the
/// fairness threshold whose guard still fails escalates instead of deferring
/// again, and the escalation writes the projection too. It must take the
/// same live-row check below the refresh stamp as an ordinary apply.
#[tokio::test]
async fn an_escalated_older_reverse_does_not_undo_a_projection_refresh() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;
    let relationship_id: i64 = client
        .query_one(
            "select id from relationship_definitions where name = 'customer'",
            &[],
        )
        .await
        .expect("read the relationship id")
        .get(0);

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann1' where id = 1",
        |lsn| StagedChange::RelationshipReverseDeferred {
            src_table: format!("\u{1f}trellis-rel-reverse-deferred:{relationship_id}"),
            key: "1".to_string(),
            old_image: Some(r#"{"id":"1","name":"ann"}"#.to_string()),
            new_image: Some(r#"{"id":"1","name":"ann1"}"#.to_string()),
            lsn: Some(lsn),
            src_changed: None,
            origin_lsn: None,
            relationship_id,
            retry_count: apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD - 1,
            group_key: None,
        },
    )
    .await;
    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer, the CDC lost");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");

    // A watermark that never advanced fails guard (a), so the deferred
    // record, at the threshold, escalates.
    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    let unadvanced = StagedWatermark::new();
    let mut escalations = 0;
    while let Some(applied) = apply::drain_once(
        &db.pool,
        outcome.sealed_seg_seq,
        TEST_NAME,
        1,
        WAKE,
        &unadvanced,
    )
    .await
    .expect("drain_once")
    {
        escalations += applied.fairness_escalations;
    }
    assert_eq!(escalations, 1, "the deferred rename escalated");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Drains sealed segment `seg_seq` alone.
async fn drain_sealed(pool: &trellis::Pool, seg_seq: i64) {
    while apply::drain_once(
        pool,
        seg_seq,
        TEST_NAME,
        1,
        WAKE,
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .is_some()
    {}
}

/// `order_names` once customer 1 is gone.
fn without_ann() -> Vec<(i32, Option<String>)> {
    vec![(10, None), (11, Some("bob".to_string())), (12, None)]
}

/// Issue #754: a customer's delete at or below the refresh stamp drains
/// while the customer's re-insert is committed but still in the ring, so the
/// delete is superseded and written from the live row. Written from the live
/// row, the re-insert's projection row outlived the customer: the re-insert
/// and a later delete fold to a record with no image, which names no key, so
/// nothing removed it. The live write leaves a key whose latest later
/// pending change names it in its new image to that change.
#[tokio::test]
async fn a_superseded_delete_leaves_a_key_with_a_pending_reinsert_to_that_reinsert() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 1",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 1, "ann"),
    )
    .await;
    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    commit_and_stage(
        &mut client,
        "insert into public.customers values (1, 'ann2')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 1, "ann2"),
    )
    .await;
    drain_sealed(&db.pool, outcome.sealed_seg_seq).await;
    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 1",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 1, "ann2"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(projected_name(&client, 1).await, None);
    assert_eq!(order_names(&client).await, without_ann());
}

/// Issue #754's other side: with no later pending change, a superseded
/// delete still writes the key back from the live row, which a re-insert
/// whose CDC was lost put there.
#[tokio::test]
async fn a_superseded_delete_writes_back_a_key_a_lost_reinsert_restored() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 1",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 1, "ann"),
    )
    .await;
    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    client
        .batch_execute("insert into public.customers values (1, 'ann2')")
        .await
        .expect("re-insert customer 1, the CDC lost");
    drain_sealed(&db.pool, outcome.sealed_seg_seq).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
}

/// Parks customer `id`'s change as a poisoned key's batch would: the change
/// is in `poison_held`, not the ring, and the key is marked poisoned for
/// `order_names`, the definition reading customers through the relationship
/// (#799: whole-key poison is per transform). It is the relationship's only
/// reader, so its reverse work leaves the key out. The held row keeps the
/// change's pre-image and no join values: the relationship's `to_col` is the
/// customers' key, whose only value is the ring key (#803).
async fn park_customer_change(client: &Client, id: i32, old_image: Option<&str>) {
    let key = id.to_string();
    client
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, 'public.customers', $1, 'test' from transform_definitions \
             where target_table like '%.order_names'",
            &[&key],
        )
        .await
        .expect("mark the customer poisoned");
    client
        .execute(
            "insert into poison_held \
                 (transform_id, src_table, key, seg_seq, lsn, old_image, hop_gen) \
             select id, 'public.customers', $1, 1, pg_current_wal_insert_lsn(), \
                    $2::text::jsonb, 0 \
             from transform_definitions where target_table like '%.order_names'",
            &[&key, &old_image],
        )
        .await
        .expect("park the customer's change");
}

/// Issue #754: releasing a parked to-side key stages an image-less
/// `Recompute`, which re-derives the key's from-side rows but never moved
/// the relationship's projection, so they re-derived from the name the
/// projection held before the parked rename. The release writes the key's
/// projection row from the live to-side row.
#[tokio::test]
async fn releasing_a_parked_to_side_rename_advances_the_projection() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename customer 1");
    park_customer_change(&client, 1, Some(r#"{"id":"1","name":"ann"}"#)).await;
    trellis::staging::release_key(&db.pool, "order_names", "public.customers", "1")
        .await
        .expect("release the parked customer");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Issue #754 for a parked delete: the release removes the key's projection
/// row, and re-derives the from-side rows that pointed at it. The live row
/// is gone, so only the parked change's pre-image names those rows.
#[tokio::test]
async fn releasing_a_parked_to_side_delete_removes_the_projection_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute("delete from public.customers where id = 1")
        .await
        .expect("delete customer 1");
    park_customer_change(&client, 1, Some(r#"{"id":"1","name":"ann"}"#)).await;
    trellis::staging::release_key(&db.pool, "order_names", "public.customers", "1")
        .await
        .expect("release the parked customer");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(projected_name(&client, 1).await, None);
    assert_eq!(order_names(&client).await, without_ann());
}

/// Issue #754 review: a parked change's ring row stays in its segment, and
/// counts as pending until the segment drains, which another bucket can hold
/// up after the parked key's own bucket finished. The release discards the
/// parked change, so it must not leave the key to that ring row: nothing
/// would ever write it.
#[tokio::test]
async fn releasing_a_parked_rename_whose_segment_is_still_draining_advances_the_projection() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann2' where id = 1",
        |lsn| customer_update(lsn, 1, "ann", "ann2"),
    )
    .await;
    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    // The rename's bucket parked it; another bucket is still draining.
    client
        .execute(
            "update segments set state = 'draining' where seg_seq = $1",
            &[&outcome.sealed_seg_seq],
        )
        .await
        .expect("mark the segment draining");
    park_customer_change(&client, 1, Some(r#"{"id":"1","name":"ann"}"#)).await;
    trellis::staging::release_key(&db.pool, "order_names", "public.customers", "1")
        .await
        .expect("release the parked customer");
    // The other bucket finishes; the parked rename is never applied.
    client
        .execute(
            "update segments set state = 'drained' where seg_seq = $1",
            &[&outcome.sealed_seg_seq],
        )
        .await
        .expect("mark the segment drained");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// The name the `customer` projection holds for `code`: `None` for no row.
async fn projected_by_code(
    client: &Client,
    projection: &str,
    code: &str,
) -> Option<Option<String>> {
    client
        .query_opt(
            &format!("select name from \"{projection}\" where code = $1"),
            &[&code],
        )
        .await
        .expect("read the customer projection")
        .map(|r| r.get(0))
}

/// Customer 1 (`ann`) moving from join value `old` to `new`, as its capture
/// stages it.
fn customer_recode(lsn: PgLsn, old: &str, new: &str) -> StagedChange {
    let image = |code: &str| format!(r#"{{"id":"1","code":"{code}","name":"ann"}}"#);
    StagedChange::Cdc {
        src_table: "public.customers".to_string(),
        key: "1".to_string(),
        op: CdcOp::Update,
        lsn: Some(lsn),
        old_image: Some(image(old)),
        new_image: Some(image(new)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// #803 review: batches drain out of order, so a held key's parked changes
/// need not follow one another. Here the customer's to-one join value moved
/// `a` -> `b` -> `c` -> `d`: the `b` -> `c` batch drained first, unheld, and
/// wrote the projection row `c` from the live row; the `a` -> `b` batch drained
/// next and parked the key (the hand-parked earliest row); and the `c` -> `d`
/// batch parked it too. Only that last park's pre-image names `c`, and the
/// one held row keeps the earliest park's pre-image (`a`), so the park folds
/// each pre-image's join values into `join_values`. Without it the release
/// rewrites `a`, `b` and `d` from live but never `c`, which keeps the
/// customer's name with no customer behind it.
#[tokio::test]
async fn releasing_a_key_parked_around_an_out_of_order_drain_removes_its_stale_projection_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.customers (id integer primary key, code text unique, name text); \
             create table public.orders (id integer primary key, customer_code text); \
             insert into public.customers values (1, 'b', 'ann'); \
             insert into public.orders values (10, 'c'), (11, 'd')",
        )
        .await
        .expect("create and seed customers and orders");
    create_relationship(
        &db.pool,
        "RELATIONSHIP customer FROM orders.customer_code TO customers.code",
    )
    .await
    .expect("declare the relationship");
    let trellis = connect_trellis(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_names FROM orders SELECT customer.name AS customer_name")
        .await
        .expect("register the consumer");
    bring_live(&db.pool, &mut client).await;
    let projection: String = client
        .query_one(
            "select rp.projection_table from relationship_projections rp \
             join relationship_definitions rd on rd.id = rp.relationship_id \
             where rd.name = 'customer'",
            &[],
        )
        .await
        .expect("find the customer projection")
        .get(0);

    // The `b` -> `c` batch, drained before the key was held.
    commit_and_stage(
        &mut client,
        "update public.customers set code = 'c' where id = 1",
        |lsn| customer_recode(lsn, "b", "c"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        projected_by_code(&client, &projection, "c").await,
        Some(Some("ann".to_string()))
    );

    // The `a` -> `b` batch, drained after it, held the key and parked its
    // change: the earliest park, whose pre-image is `a`.
    client
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, 'public.customers', '1', 'test' from transform_definitions \
             where target_table like '%.order_names'",
            &[],
        )
        .await
        .expect("mark the customer poisoned");
    client
        .execute(
            "insert into poison_held \
                 (transform_id, src_table, key, seg_seq, lsn, old_image, join_values) \
             select id, 'public.customers', '1', 0, pg_current_wal_insert_lsn(), \
                    '{\"id\": 1, \"code\": \"a\", \"name\": \"ann\"}', \
                    array['{\"code\": \"b\"}'::jsonb] \
             from transform_definitions where target_table like '%.order_names'",
            &[],
        )
        .await
        .expect("park the a -> b change");

    // The `c` -> `d` batch parks the key through the real page path.
    commit_and_stage(
        &mut client,
        "update public.customers set code = 'd' where id = 1",
        |lsn| customer_recode(lsn, "c", "d"),
    )
    .await;
    let parked = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, parked.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    drain_sealed(&db.pool, parked.sealed_seg_seq).await;
    let held: Vec<String> = client
        .query(
            "select j::text from poison_held, unnest(join_values) j order by 1",
            &[],
        )
        .await
        .expect("read the held row's join values")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert!(
        held.contains(&r#"{"code": "d"}"#.to_string()),
        "the page parked the c -> d change into the held row: {held:?}"
    );
    assert_eq!(
        projected_by_code(&client, &projection, "c").await,
        Some(Some("ann".to_string())),
        "the parked c -> d change left the projection alone"
    );

    trellis::staging::release_key(&db.pool, "order_names", "public.customers", "1")
        .await
        .expect("release the parked customer");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_by_code(&client, &projection, "c").await,
        None,
        "no customer has code c"
    );
    assert_eq!(
        projected_by_code(&client, &projection, "d").await,
        Some(Some("ann".to_string()))
    );
    // #944: order 10 read the customer under `c`, which neither the prior
    // image (`a`) nor the live row (`d`) names, so only the held row's
    // `join_values` tell the release to re-derive it.
    assert_eq!(
        order_names(&client).await,
        vec![(10, None), (11, Some("ann".to_string()))],
        "every order reads the customer by its live code"
    );
}

/// `customer_orders`' counts, by customer.
async fn counts(client: &Client) -> Vec<(i32, Option<i64>)> {
    client
        .query("select id, n from customer_orders order by id", &[])
        .await
        .expect("read customer_orders")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Order 10 moving from join value `old` to `new`, as its capture stages it.
fn order_recode(lsn: PgLsn, old: &str, new: &str) -> StagedChange {
    let image = |code: &str| format!(r#"{{"id":"10","customer_code":"{code}"}}"#);
    StagedChange::Cdc {
        src_table: "public.orders".to_string(),
        key: "10".to_string(),
        op: CdcOp::Update,
        lsn: Some(lsn),
        old_image: Some(image(old)),
        new_image: Some(image(new)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// #944, the to-many twin of the test above: the release re-derives every
/// from-side row a held to-side key's parked changes could have been read
/// under. Order 10's `customer_code` moved `a` -> `b` -> `c` -> `d`: the
/// `b` -> `c` batch drained first and counted it for customer 1 (`c`), then
/// the `a` -> `b` and `c` -> `d` batches parked it. The release's prior
/// image names `a` and its live row `d`; only the held row's `join_values`
/// name `c`, so without them customer 1 kept counting an order it no longer
/// has.
#[tokio::test]
async fn releasing_a_to_many_key_parked_around_an_out_of_order_drain_recounts_every_parent() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.customers (id integer primary key, code text); \
             create table public.orders (id integer primary key, customer_code text); \
             insert into public.customers values (1, 'c'), (2, 'd'); \
             insert into public.orders values (10, 'b')",
        )
        .await
        .expect("create and seed customers and orders");
    create_relationship(
        &db.pool,
        "RELATIONSHIP orders FROM customers.code TO orders.customer_code",
    )
    .await
    .expect("declare the relationship");
    let trellis = connect_trellis(db.dsn()).await;
    trellis
        .apply("TRANSFORM customer_orders FROM customers SELECT count(orders.id) AS n")
        .await
        .expect("register the consumer");
    bring_live(&db.pool, &mut client).await;
    // The `b` -> `c` batch, drained before the key was held.
    commit_and_stage(
        &mut client,
        "update public.orders set customer_code = 'c' where id = 10",
        |lsn| order_recode(lsn, "b", "c"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(counts(&client).await, vec![(1, Some(1)), (2, Some(0))]);

    // The `a` -> `b` batch, drained after it, held the key and parked its
    // change.
    client
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, 'public.orders', '10', 'test' from transform_definitions \
             where target_table like '%.customer_orders'",
            &[],
        )
        .await
        .expect("mark the order poisoned");
    client
        .execute(
            "insert into poison_held \
                 (transform_id, src_table, key, seg_seq, lsn, old_image, join_values) \
             select id, 'public.orders', '10', 0, pg_current_wal_insert_lsn(), \
                    '{\"id\": 10, \"customer_code\": \"a\"}', \
                    array['{\"customer_code\": \"b\"}'::jsonb] \
             from transform_definitions where target_table like '%.customer_orders'",
            &[],
        )
        .await
        .expect("park the a -> b change");

    // The `c` -> `d` batch parks the key through the real page path.
    commit_and_stage(
        &mut client,
        "update public.orders set customer_code = 'd' where id = 10",
        |lsn| order_recode(lsn, "c", "d"),
    )
    .await;
    let parked = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, parked.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    drain_sealed(&db.pool, parked.sealed_seg_seq).await;
    assert_eq!(
        counts(&client).await,
        vec![(1, Some(1)), (2, Some(0))],
        "the parked c -> d change moved no count"
    );

    trellis::staging::release_key(&db.pool, "customer_orders", "public.orders", "10")
        .await
        .expect("release the parked order");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        counts(&client).await,
        vec![(1, Some(0)), (2, Some(1))],
        "every customer counts the orders that name its live code"
    );
}

/// Issue #754 review: a change whose bucket already applied it, while
/// another bucket holds its segment up, is not pending (issue #762: its
/// bucket's `drained_mask` bit says so), so a superseded older delete writes
/// the key from the live row rather than leaving it to a change that will
/// never write it again.
#[tokio::test]
async fn a_superseded_delete_writes_a_key_whose_pending_reinsert_already_applied() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 1",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 1, "ann"),
    )
    .await;
    let delete = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, delete.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    commit_and_stage(
        &mut client,
        "insert into public.customers values (1, 'ann2')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 1, "ann2"),
    )
    .await;
    let reinsert = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, reinsert.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    drain_sealed(&db.pool, reinsert.sealed_seg_seq).await;
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string())),
        "the re-insert applied"
    );
    // Another bucket of the re-insert's segment is still draining.
    hold_open_beside(&client, reinsert.sealed_seg_seq, 1).await;
    drain_sealed(&db.pool, delete.sealed_seg_seq).await;
    finish_holding(&client, reinsert.sealed_seg_seq).await;
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Puts drained segment `seg_seq` back to what a segment looks like while
/// another bucket is still draining (issue #762): two buckets, and only the
/// one customer `id`'s ring rows route to has drained.
async fn hold_open_beside(client: &Client, seg_seq: i64, id: i32) {
    let slot: i16 = client
        .query_one(
            "select ring_slot from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("read the segment's ring slot")
        .get(0);
    let held = client
        .execute(
            &format!(
                "update segments set state = 'draining', bucket_count = 2, \
                     drained_mask = 1::bigint << (select (r.route % 2)::int from seg_{slot} r \
                         where r.src_table = 'public.customers' and r.key = $2 limit 1) \
                 where seg_seq = $1"
            ),
            &[&seg_seq, &id.to_string()],
        )
        .await
        .expect("hold the segment open in its other bucket");
    assert_eq!(held, 1);
}

/// Retires every drained segment, whatever its successor's state, so a test
/// has the whole ring but the active slot to seal into. Setup leaves its last
/// drained segment in place (`retire_drained_segments` waits for the
/// successor), and its slot holds no rows a test reads.
async fn free_drained_slots(client: &Client) {
    let slots: Vec<i16> = client
        .query(
            "delete from segments where state = 'drained' returning ring_slot",
            &[],
        )
        .await
        .expect("retire the drained segments")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for slot in slots {
        client
            .batch_execute(&format!("truncate seg_{slot}"))
            .await
            .expect("truncate the retired slot");
    }
}

/// The other bucket of a segment [`hold_open_beside`] held open drains.
async fn finish_holding(client: &Client, seg_seq: i64) {
    client
        .execute(
            "update segments set state = 'drained', drained_mask = 3 where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("drain the segment's other bucket");
}

/// Issue #762: an applied change still counted as pending while another
/// bucket held its segment open, and the projection row's `lsn` decided
/// whether it had applied. That test is not monotonic. A re-insert applies
/// and its segment stays open; an older superseded rename writes the key
/// from the live row, stamping its own, lower `lsn`; an older superseded
/// delete then took the re-insert for unapplied and deleted the key, which
/// nothing wrote again. A change is pending only while its bucket has not
/// drained it.
#[tokio::test]
async fn superseded_records_older_than_an_applied_reinsert_keep_its_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;
    // Three sealed segments and the active one fill the ring.
    free_drained_slots(&client).await;

    commit_and_stage(
        &mut client,
        "update public.customers set name = 'ann1' where id = 1",
        |lsn| customer_update(lsn, 1, "ann", "ann1"),
    )
    .await;
    let rename = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, rename.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    commit_and_stage(
        &mut client,
        "delete from public.customers where id = 1",
        |lsn| customer_cdc(lsn, CdcOp::Delete, 1, "ann1"),
    )
    .await;
    let delete = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, delete.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    trellis
        .request_backfill("customers")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    commit_and_stage(
        &mut client,
        "insert into public.customers values (1, 'ann2')",
        |lsn| customer_cdc(lsn, CdcOp::Insert, 1, "ann2"),
    )
    .await;
    let reinsert = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, reinsert.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");

    drain_sealed(&db.pool, reinsert.sealed_seg_seq).await;
    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string())),
        "the re-insert applied"
    );
    hold_open_beside(&client, reinsert.sealed_seg_seq, 1).await;
    drain_sealed(&db.pool, rename.sealed_seg_seq).await;
    drain_sealed(&db.pool, delete.sealed_seg_seq).await;
    finish_holding(&client, reinsert.sealed_seg_seq).await;
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projected_name(&client, 1).await,
        Some(Some("ann2".to_string()))
    );
    assert_eq!(order_names(&client).await, renamed());
}

/// Seeds `accounts` (the to-side, joined on its unique non-key `code`) and
/// `bills`, declares the to-one `account` relationship, registers
/// `bill_names` (which reads `account.name` through it) and brings it
/// `live`. Two accounts' changes can then name one projection key.
async fn live_code_relationship_consumer(
    dsn: &str,
    pool: &trellis::Pool,
    client: &mut Client,
) -> Trellis {
    client
        .batch_execute(
            "create table public.accounts (id integer primary key, code text unique, name text); \
             create table public.bills (id integer primary key, account_code text); \
             insert into public.accounts values (1, 'A', 'ann'), (2, 'K', 'kim'); \
             insert into public.bills values (10, 'K'), (11, 'A')",
        )
        .await
        .expect("create and seed accounts and bills");
    create_relationship(
        pool,
        "RELATIONSHIP account FROM bills.account_code TO accounts.code",
    )
    .await
    .expect("declare the relationship");
    let trellis = connect_trellis(dsn).await;
    trellis
        .apply("TRANSFORM bill_names FROM bills SELECT account.name AS account_name")
        .await
        .expect("register the consumer");
    bring_live(pool, client).await;
    trellis
}

/// An `accounts` image, as capture stages it.
fn account_image(id: i32, code: &str, name: &str) -> String {
    format!(r#"{{"id":"{id}","code":"{code}","name":"{name}"}}"#)
}

/// An `accounts` change for account `id`, as capture stages it.
fn account_cdc(
    lsn: PgLsn,
    op: CdcOp,
    id: i32,
    old: Option<(&str, &str)>,
    new: Option<(&str, &str)>,
) -> StagedChange {
    StagedChange::Cdc {
        src_table: "public.accounts".to_string(),
        key: id.to_string(),
        op,
        lsn: Some(lsn),
        old_image: old.map(|(code, name)| account_image(id, code, name)),
        new_image: new.map(|(code, name)| account_image(id, code, name)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// The `account` relationship's projection table.
async fn account_projection(client: &Client) -> String {
    client
        .query_one(
            "select rp.projection_table from relationship_projections rp \
             join relationship_definitions rd on rd.id = rp.relationship_id \
             where rd.name = 'account'",
            &[],
        )
        .await
        .expect("find the account projection")
        .get(0)
}

/// The `account` projection row for `code`: its `name`, or `None` when the
/// projection has no row for the key.
async fn projected_account(client: &Client, code: &str) -> Option<Option<String>> {
    let projection = account_projection(client).await;
    client
        .query_opt(
            &format!("select name from \"{projection}\" where code = $1"),
            &[&code],
        )
        .await
        .expect("read the account projection")
        .map(|r| r.get(0))
}

async fn bill_names(client: &Client) -> Vec<(i32, Option<String>)> {
    client
        .query("select id, account_name from bill_names order by id", &[])
        .await
        .expect("read bill_names")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Seals the active segment and returns its `seg_seq`.
async fn seal_active(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Issue #762 review: the Phase 3 transaction applying a page counts its own
/// page's ring rows as applied (`ClaimScope`). Two accounts' changes name key
/// `K`: account 3's insert, applied first in the transaction, and an older,
/// superseded rename of account 1 into `K`, applied after it. Drain state
/// for the page is written only when the transaction completes, so without
/// the claim the rename's live write took the insert for pending and
/// deleted the row the insert had just written, and nothing wrote `K`
/// again.
///
/// The two segments drain coalesced, in one transaction, older segment
/// first, so the insert sits in the older segment and the rename, staged at
/// its earlier `lsn`, in the newer one.
#[tokio::test]
async fn a_page_mate_that_wrote_a_key_keeps_it_from_a_superseded_live_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_code_relationship_consumer(db.dsn(), &db.pool, &mut client).await;
    free_drained_slots(&client).await;

    client
        .batch_execute("update public.accounts set code = 'Z' where id = 2")
        .await
        .expect("free code K, the CDC lost");
    let rename_lsn: PgLsn = {
        let txn = client.transaction().await.expect("begin the rename");
        txn.batch_execute("update public.accounts set code = 'K' where id = 1")
            .await
            .expect("rename account 1 into K");
        let lsn = txn
            .query_one("select pg_current_wal_insert_lsn()", &[])
            .await
            .expect("read the rename's lsn")
            .get(0);
        txn.commit().await.expect("commit the rename");
        lsn
    };
    client
        .batch_execute("update public.accounts set code = 'B' where id = 1")
        .await
        .expect("rename account 1 off K, the CDC lost");
    trellis
        .request_backfill("accounts")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    assert_eq!(projected_account(&client, "K").await, None);
    // The re-read's own recomputes drain first, so no other record shares a
    // key with the two below.
    let reread = seal_active(&mut client).await;
    drain_sealed(&db.pool, reread).await;

    commit_and_stage(
        &mut client,
        "insert into public.accounts values (3, 'K', 'cat')",
        |lsn| account_cdc(lsn, CdcOp::Insert, 3, None, Some(("K", "cat"))),
    )
    .await;
    let insert = seal_active(&mut client).await;
    {
        let txn = client.transaction().await.expect("begin staging");
        trellis::staging::append(
            &txn,
            &[account_cdc(
                rename_lsn,
                CdcOp::Update,
                1,
                Some(("A", "ann")),
                Some(("K", "ann")),
            )],
        )
        .await
        .expect("stage the rename");
        txn.commit().await.expect("commit staging");
    }
    let rename = seal_active(&mut client).await;

    while apply::drain_many(
        &db.pool,
        &[insert, rename],
        TEST_NAME,
        1,
        WAKE,
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_many")
    .is_some()
    {}
    assert_eq!(
        projected_account(&client, "K").await,
        Some(Some("cat".to_string())),
        "the rename's live write kept the insert's row"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        projected_account(&client, "K").await,
        Some(Some("cat".to_string()))
    );
    assert_eq!(
        bill_names(&client).await,
        vec![(10, Some("cat".to_string())), (11, None)]
    );
}

/// Issue #762 review: the live write's held delete names the row as its
/// snapshot read it, so a to-side write that commits while the delete waits
/// on the row is left alone. A from-side apply's `__trellis_gen` bump also
/// rewrites the row and holds it until it commits, but applies no to-side
/// change, so the delete still goes ahead after it. Account 1's superseded
/// rename into `K` drains while account 3's later update of `K` is pending,
/// so the live write deletes `K` and leaves it to that update. Testing the
/// row version (`xmin`) kept the stale row instead, which a pending change
/// that folds to no image would then never remove (#754).
#[tokio::test]
async fn a_held_delete_goes_ahead_after_a_concurrent_gen_bump() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_code_relationship_consumer(db.dsn(), &db.pool, &mut client).await;
    free_drained_slots(&client).await;

    client
        .batch_execute("update public.accounts set code = 'Z' where id = 2")
        .await
        .expect("free code K, the CDC lost");
    commit_and_stage(
        &mut client,
        "update public.accounts set code = 'K' where id = 1",
        |lsn| {
            account_cdc(
                lsn,
                CdcOp::Update,
                1,
                Some(("A", "ann")),
                Some(("K", "ann")),
            )
        },
    )
    .await;
    let rename = seal_active(&mut client).await;
    client
        .batch_execute(
            "update public.accounts set code = 'B' where id = 1; \
             insert into public.accounts values (3, 'K', 'cat')",
        )
        .await
        .expect("move account 1 off K and insert account 3 at K, the CDC lost");
    trellis
        .request_backfill("accounts")
        .await
        .expect("request a re-backfill of the to-side");
    markers::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    assert_eq!(
        projected_account(&client, "K").await,
        Some(Some("cat".to_string())),
        "the refresh wrote K from the live row"
    );
    commit_and_stage(
        &mut client,
        "update public.accounts set name = 'cat2' where id = 3",
        |lsn| {
            account_cdc(
                lsn,
                CdcOp::Update,
                3,
                Some(("K", "cat")),
                Some(("K", "cat2")),
            )
        },
    )
    .await;

    // A from-side apply bumps K's generation and has not committed yet.
    let projection = account_projection(&client).await;
    let mut bumper = connect_raw(db.dsn()).await;
    let bump = bumper.transaction().await.expect("begin the bump");
    bump.execute(
        &format!("update \"{projection}\" set __trellis_gen = __trellis_gen + 1 where code = 'K'"),
        &[],
    )
    .await
    .expect("bump K's generation");

    let pool = db.pool.clone();
    let drain = tokio::spawn(async move { drain_sealed(&pool, rename).await });
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let waiting: bool = client
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where datname = current_database() and wait_event_type = 'Lock')",
                &[],
            )
            .await
            .expect("look for a lock wait")
            .get(0);
        if waiting {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rename's drain never waited on K's row"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bump.commit().await.expect("commit the bump");
    drain.await.expect("the rename's drain");

    assert_eq!(
        projected_account(&client, "K").await,
        None,
        "the live write left K to account 3's pending update"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        projected_account(&client, "K").await,
        Some(Some("cat2".to_string()))
    );
    assert_eq!(
        bill_names(&client).await,
        vec![(10, Some("cat2".to_string())), (11, None)]
    );
}
