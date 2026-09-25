//! Issue #522: a marker that re-reads a table because its applying readers
//! may have missed changes to it is a go-live catch-up for every one of them
//! (issue #476, ADR-0016's "What `live` promises"): an explicit
//! `Trellis::request_backfill`, a fresh install's slot, a table rejoining
//! the publication, and a lost slot's to-sides (issue #533)
//! (`intake::publication::park_table_catch_ups`).
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
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, create_relationship, install_definition};
use trellis::intake::{publication, slot_loss};
use trellis::staging::session::ProducerSession;
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
    publication::settle_registrations(pool).await;
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

/// One discharge pass over every settled marker, then a drain of what it
/// staged. Asserts the pass discharged every marker, so a deferral shows up
/// here rather than as a wrong target further on.
async fn discharge_markers(pool: &trellis::Pool, client: &mut Client) {
    publication::run_pending_backfills(client, WAKE, &StagedWatermark::saturated(), Duration::ZERO)
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
/// transaction, as intake would stage it.
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
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), \
                                             (4, 'a', 1000); \
             create publication trellis_pub for table public.sales",
        )
        .await
        .expect("create, seed and publish sales");
}

/// A 1-1 target row whose source row was deleted with no CDC reaching the
/// target is gone once the re-backfill discharges. Before #522 the
/// definition stayed `live`, the discharge swept nothing, and row 4 stayed.
#[tokio::test]
async fn a_requested_backfill_sweeps_a_stale_one_to_one_row() {
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
    let rows: Vec<(i32, String)> = client
        .query("select id, amt::text from sales_copy order by id", &[])
        .await
        .expect("read sales_copy")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
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
/// from what it still has, and only the sweep can drop `b`.
#[tokio::test]
async fn a_requested_backfill_sweeps_a_stale_aggregate_group() {
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
    assert_eq!(status_of(&client, "public.sku_totals").await, "catching_up");

    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    let totals: Vec<(String, String)> = client
        .query("select sku, total::text from sku_totals order by sku", &[])
        .await
        .expect("read sku_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(totals, vec![("a".to_string(), "12".to_string())]);
}

/// Seeds `customers` (the to-side) and `orders`, publishes both, declares
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
             alter table public.customers replica identity full; \
             alter table public.orders replica identity full; \
             insert into public.customers values (1, 'ann'), (2, 'bob'); \
             insert into public.orders values (10, 1), (11, 2), (12, 1); \
             create publication trellis_pub for table public.customers, public.orders",
        )
        .await
        .expect("create, seed and publish customers and orders");
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

/// #393's case for a to-side: the catalog outlived its slot, and a change
/// before the new slot's consistent point is neither streamed nor in the
/// to-side's projection. Setup's marker catches the consumer up as a
/// requested re-backfill does.
#[tokio::test]
async fn a_fresh_slot_catches_up_a_to_sides_relationship_consumer() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute("update public.customers set name = 'ann2' where id = 1")
        .await
        .expect("rename a customer before the slot exists");
    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    publication::create_slot_and_park_markers(
        &mut session,
        "table_catch_ups_slot",
        &["public.customers".to_string(), "public.orders".to_string()],
    )
    .await
    .expect("create the slot");
    assert_eq!(
        status_of(&client, "public.order_names").await,
        "catching_up"
    );

    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(status_of(&client, "public.order_names").await, "live");
    assert_eq!(order_names(&client).await, renamed());
    client
        .execute(
            "select pg_drop_replication_slot('table_catch_ups_slot')",
            &[],
        )
        .await
        .expect("drop the slot");
}

fn orders_update(lsn: PgLsn, id: i32, old_customer: i32, new_customer: i32) -> StagedChange {
    let image = |customer: i32| format!(r#"{{"id":"{id}","customer_id":"{customer}"}}"#);
    StagedChange::Cdc {
        src_table: "public.orders".to_string(),
        key: id.to_string(),
        op: CdcOp::Update,
        lsn: Some(lsn),
        old_image: Some(image(old_customer)),
        new_image: Some(image(new_customer)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// Issue #533: a to-side change lost with the slot never reached the
/// to-side's projection. A 1-1 consumer of a to-one lookup is built by the
/// ring, whose `Recompute`s re-derive each row through the projection
/// (`defs::backfill::collect_agg_leaves`), so without a
/// refresh the resumed consumer comes back `live` with the old name, and a
/// later from-side change reads it again. The recovery re-reads each
/// published to-side as a catch-up that refreshes its projections, so both
/// read the renamed customer.
#[tokio::test]
async fn a_slot_loss_refreshes_a_to_sides_projection_for_its_resumed_consumer() {
    const LOST: &str = "table_catch_ups_lost_slot";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute(&format!(
            "insert into replication_progress (slot_name, confirmed_lsn) values ('{LOST}', '0/10'); \
             update public.customers set name = 'ann2' where id = 1"
        ))
        .await
        .expect("lose the slot, and rename a customer it never delivered");
    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    let recovery = slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "trellis_pub")
        .await
        .expect("recover from the lost slot")
        .expect("a slot that doesn't exist is lost");
    assert_eq!(recovery.paused, vec!["order_names"]);
    discharge_markers(&db.pool, &mut client).await;

    trellis
        .apply("RESUME TRANSFORM order_names")
        .await
        .expect("resume the consumer");
    bring_live(&db.pool, &mut client).await;
    assert_eq!(order_names(&client).await, renamed());

    // Order 11 moves to customer 1: its row is re-derived through the
    // projection, not by a read of `customers`.
    commit_and_stage(
        &mut client,
        "update public.orders set customer_id = 1 where id = 11",
        |lsn| orders_update(lsn, 11, 2, 1),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        order_names(&client).await,
        vec![
            (10, Some("ann2".to_string())),
            (11, Some("ann2".to_string())),
            (12, Some("ann2".to_string())),
        ],
        "the from-side change reads the rename the lost slot never delivered"
    );

    drop(session);
    client
        .execute("select pg_drop_replication_slot($1)", &[&LOST])
        .await
        .expect("drop the recreated slot");
}

/// Issue #533: the to-side refresh commits with the new slot's start. A
/// recovery that dies after creating the slot but before that commit (here,
/// a park that fails) leaves the old position, behind the new slot's, so the
/// next startup takes the slot for lost again, pauses nothing new, and
/// parks the refresh then.
#[tokio::test]
async fn a_slot_loss_recovery_cut_short_parks_its_refresh_on_the_redo() {
    const LOST: &str = "table_catch_ups_lost_slot_redo";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute(&format!(
            "insert into replication_progress (slot_name, confirmed_lsn) values ('{LOST}', '0/10'); \
             update public.customers set name = 'ann2' where id = 1; \
             create function fail_park() returns trigger language plpgsql as \
               $$ begin raise exception 'the recovery dies before its commit'; end $$; \
             create trigger fail_park before insert on pending_backfill \
               for each row execute function fail_park()"
        ))
        .await
        .expect("lose the slot, rename a customer, and make the park fail");
    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    let err = slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "trellis_pub")
        .await
        .expect_err("the park fails the recovery's last transaction");
    assert!(
        format!("{err:?}").contains("the recovery dies before its commit"),
        "{err:?}"
    );
    session.release().await.expect("release the producer lock");
    let confirmed: PgLsn = client
        .query_one(
            "select confirmed_lsn from replication_progress where slot_name = $1",
            &[&LOST],
        )
        .await
        .expect("read the confirmed position")
        .get(0);
    assert_eq!(
        confirmed,
        PgLsn::from(0x10),
        "the new start never committed"
    );
    client
        .batch_execute("drop trigger fail_park on pending_backfill")
        .await
        .expect("let the park through");

    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("reconnect the producer session");
    let redo = slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "trellis_pub")
        .await
        .expect("redo the recovery")
        .expect("the uncommitted slot looks recreated under the same name");
    assert!(redo.paused.is_empty(), "the first pass already paused it");
    assert_eq!(redo.already_frozen, vec!["order_names"]);
    discharge_markers(&db.pool, &mut client).await;

    trellis
        .apply("RESUME TRANSFORM order_names")
        .await
        .expect("resume the consumer");
    bring_live(&db.pool, &mut client).await;
    assert_eq!(order_names(&client).await, renamed());

    drop(session);
    client
        .execute("select pg_drop_replication_slot($1)", &[&LOST])
        .await
        .expect("drop the recreated slot");
}

/// Issue #533: the recovery's refresh of a to-side failed and is backing
/// off (issue #407) when the consumer is resumed, so the consumer's marker
/// discharges first and its ring build re-derives every row through the
/// stale projection (and flips it `live`, the limitation ADR-0016's "A
/// re-read table's readers" names). The refresh's retry re-reads the to-side
/// with the consumer applying, and that re-read re-derives it from the
/// refreshed projection.
#[tokio::test]
async fn a_slot_loss_refresh_that_backs_off_still_reaches_a_consumer_resumed_meanwhile() {
    const LOST: &str = "table_catch_ups_lost_slot_backoff";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let trellis = live_relationship_consumer(db.dsn(), &db.pool, &mut client).await;

    client
        .batch_execute(&format!(
            "insert into replication_progress (slot_name, confirmed_lsn) values ('{LOST}', '0/10'); \
             update public.customers set name = 'ann2' where id = 1"
        ))
        .await
        .expect("lose the slot, and rename a customer it never delivered");
    let mut session = ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
        .await
        .expect("connect producer session");
    slot_loss::pause_if_slot_lost(&mut session, &db.pool, LOST, "trellis_pub")
        .await
        .expect("recover from the lost slot")
        .expect("a slot that doesn't exist is lost");
    let backing_off = client
        .execute(
            "update pending_backfill set attempts = 1, \
               next_attempt_at = now() + interval '1 hour' \
             where table_name = 'public.customers' and refresh_projections",
            &[],
        )
        .await
        .expect("back the refresh off");
    assert_eq!(backing_off, 1, "the recovery parked a refresh on customers");

    trellis
        .apply("RESUME TRANSFORM order_names")
        .await
        .expect("resume the consumer");
    publication::run_pending_backfills(
        &mut client,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the consumer's marker");
    drain_to_quiescence(&db.pool, &mut client).await;
    let pending: Vec<String> = client
        .query("select table_name from pending_backfill", &[])
        .await
        .expect("read the markers")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        pending,
        vec!["public.customers"],
        "the consumer built while the refresh was still backing off"
    );

    client
        .execute("update pending_backfill set next_attempt_at = now()", &[])
        .await
        .expect("make the refresh due");
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(order_names(&client).await, renamed());
    assert_eq!(status_of(&client, "public.order_names").await, "live");

    drop(session);
    client
        .execute("select pg_drop_replication_slot($1)", &[&LOST])
        .await
        .expect("drop the recreated slot");
}

/// A table an operator dropped from the publication while a definition
/// applied from it rejoins at the next reconcile. Its readers missed
/// everything written meanwhile, so the join marker is their catch-up.
#[tokio::test]
async fn a_table_rejoining_the_publication_catches_up_its_readers() {
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
        .batch_execute(
            "alter publication trellis_pub drop table public.sales; \
             delete from public.sales where id = 4",
        )
        .await
        .expect("unpublish sales, then delete a row");
    publication::reconcile_publication(&mut client, "trellis_pub", &["public.sales".to_string()])
        .await
        .expect("reconcile the publication");
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

/// Issue #531: a rename streamed but not yet drained when a later rename is
/// lost (the table is out of the publication) is older than what the
/// rejoin's refresh writes into the projection. It drains after the refresh
/// and must not put its image back: the refresh stamped the relationship, so
/// a record at or below the stamp writes the projection from the live row.
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
        .batch_execute(
            "alter publication trellis_pub drop table public.customers; \
             update public.customers set name = 'ann2' where id = 1",
        )
        .await
        .expect("unpublish customers, then rename a customer");
    publication::reconcile_publication(
        &mut client,
        "trellis_pub",
        &["public.customers".to_string(), "public.orders".to_string()],
    )
    .await
    .expect("reconcile the publication");
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
        lsn: Some(lsn),
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
    publication::run_pending_backfills(
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
