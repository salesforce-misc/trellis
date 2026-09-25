//! Issue #430: a direct build (aggregate, or relationship-enriched 1-1) must
//! not lose a source change that drains while the build is still running.
//!
//! The backfill discharge dispatches these shapes' build as one background job
//! (ADR-0016, #419), which a drain worker runs while the definition sits
//! `backfilling`, and the apply path skips any definition still being built
//! (`catalog::dependents_of`). A change to an already-published source that
//! commits after the build's read and drains before the build finishes is
//! therefore skipped by the drain and missing from what the build wrote. Only
//! a catch-up marker parked when the build finishes
//! (`complete_direct_backfill`) brings it back, and the definition reports
//! `catching_up` rather than `live` until that catch-up has run (#476).
//!
//! The tests hold the build between its read and its target write with an
//! event trigger (see [`install_build_hold`]) that blocks on an advisory lock
//! the test holds. While the build is parked there, the test commits a change,
//! stages its CDC row and drains it, then lets the build finish.
//!
//! Issue #442 is the reverse case: a change the build read whose delta drains
//! only after the build finishes, on top of the build's own count of it. For an
//! aggregate the recompute horizon the build stamps on each group row sends
//! that delta to re-derive its group. Those tests check the target before
//! the go-live catch-up runs: the horizon alone must keep it right, however
//! the late delta arrives (still in the ring, staged late by a lagging
//! intake, or released from quarantine), and whether it is on the source or
//! on a relationship's to-side table.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{
    TransformStatus, ValueType, chunk_queue, create_relationship, install_definition,
};
use trellis::intake::publication;
use trellis::staging::{CdcOp, StagedChange, StagedWatermark, apply, seal};
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "direct_build_catchup_marker_test";
const WAKE: &str = "direct_build_catchup_marker_wake";
/// The advisory lock the event trigger blocks the build on.
const HOLD_LOCK: i64 = 430;

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

/// Seals and drains until nothing is pending anywhere in the ring. A bounded
/// deterministic loop, not a wait-for-convergence poll.
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

/// Discharges every settled catch-up marker, then drains what it staged.
async fn discharge_markers(pool: &trellis::Pool, client: &mut Client) {
    publication::run_pending_backfills(client, WAKE, &StagedWatermark::saturated(), Duration::ZERO)
        .await
        .expect("run_pending_backfills");
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

async fn pending_markers(client: &Client) -> Vec<String> {
    client
        .query("select table_name from pending_backfill order by 1", &[])
        .await
        .expect("read pending_backfill")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

/// Installs an event trigger that parks any `ALTER TABLE` on advisory lock
/// [`HOLD_LOCK`] until the test releases it. Both builds read each table once,
/// into a temp staging table (`CREATE TEMP TABLE ... AS SELECT`), then
/// `ALTER` that staging table to add its key before any target write. The
/// read has committed by then, and the `ALTER` is held at
/// `ddl_command_start`, before it has an xid, so the held build doesn't hold
/// back the seal gate: the ring keeps sealing and draining around it, as it
/// does between a real build's autocommit statements.
async fn install_build_hold(client: &Client) {
    client
        .batch_execute(&format!(
            "create function hold_direct_build() returns event_trigger \
             language plpgsql as $$ \
             begin \
               perform pg_advisory_lock({HOLD_LOCK}); \
               perform pg_advisory_unlock({HOLD_LOCK}); \
             end $$; \
             create event trigger hold_direct_build on ddl_command_start \
               when tag in ('ALTER TABLE') execute function hold_direct_build()"
        ))
        .await
        .expect("install the build-hold event trigger");
}

/// Waits until some backend is blocked on [`HOLD_LOCK`]: the build has read
/// its source and is parked before writing the target. Polls a lock wait the
/// test itself controls, not the system's convergence.
async fn wait_for_build_held(client: &Client) {
    for _ in 0..500 {
        let held: bool = client
            .query_one(
                "select exists (select 1 from pg_locks \
                 where locktype = 'advisory' and objid = $1 and not granted)",
                &[&(HOLD_LOCK as u32)],
            )
            .await
            .expect("read pg_locks")
            .get(0);
        if held {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the direct build never reached the hold point");
}

/// Commits `insert_sql` and stages the CDC insert intake would stage for it
/// (`key`, `new_image` on `src_table`), in one transaction.
async fn commit_and_stage_insert(
    client: &mut Client,
    insert_sql: &str,
    src_table: &str,
    key: &str,
    new_image: &str,
) {
    let txn = client.transaction().await.expect("begin source write");
    txn.batch_execute(insert_sql).await.expect("source write");
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the write's lsn")
        .get(0);
    trellis::staging::append(&txn, &[cdc_insert(src_table, key, lsn, new_image)])
        .await
        .expect("stage the change's CDC row");
    txn.commit().await.expect("commit source write");
}

/// The CDC insert intake would stage for a write at `lsn`.
fn cdc_insert(src_table: &str, key: &str, lsn: PgLsn, new_image: &str) -> StagedChange {
    StagedChange::Cdc {
        src_table: src_table.to_string(),
        key: key.to_string(),
        op: CdcOp::Insert,
        lsn: Some(lsn),
        old_image: None,
        new_image: Some(new_image.to_string()),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

/// The #416 reviewer's repro: an aggregate build on an already-published
/// source, held between its read and its target write while a change to the
/// source drains. Before the fix the target ended one change short and no
/// catch-up marker was parked.
///
/// Issue #476's repro is the same schedule read at the moment the definition
/// reports `live`: that used to be the build's completion, with the target
/// still at `12`. Now the build leaves it `catching_up`, and only the
/// catch-up's discharge, with `1012` in the target once its enumeration
/// drains, takes it `live`.
#[tokio::test]
async fn aggregate_build_recovers_a_change_drained_during_the_build() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
        )
        .await
        .expect("create + seed sales");
    install_build_hold(&client).await;
    client
        .query_one("select pg_advisory_lock($1)", &[&HOLD_LOCK])
        .await
        .expect("take the hold lock");

    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("sku", ValueType::Text),
            ("amount", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    let pool = db.pool.clone();
    let build = tokio::spawn(async move { publication::settle_builds(&pool).await });
    wait_for_build_held(&client).await;

    commit_and_stage_insert(
        &mut client,
        "insert into public.sales values (4, 'a', 1000)",
        "public.sales",
        "4",
        r#"{"id":"4","sku":"a","amount":"1000"}"#,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .query_one("select pg_advisory_unlock($1)", &[&HOLD_LOCK])
        .await
        .expect("release the build");
    build.await.expect("build task");
    client
        .batch_execute("drop event trigger hold_direct_build")
        .await
        .expect("drop the build hold");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        total_of_sku_a(&client).await,
        "12",
        "the build's own read misses the change that drained while it ran"
    );
    assert_eq!(
        status_of(&client, "public.sku_totals").await,
        "catching_up",
        "so the finished build doesn't report live"
    );
    assert_eq!(
        pending_markers(&client).await,
        vec!["public.sales".to_string()],
        "the build parks a catch-up marker on the source, as the chunked path does"
    );

    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    assert_eq!(
        total_of_sku_a(&client).await,
        "1012",
        "at live, the change drained while the build was running is folded in"
    );
}

/// `public.sales`, seeded with `sku = 'a'` totalling `12`.
async fn seed_sales(client: &Client) {
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
        )
        .await
        .expect("create + seed sales");
}

/// Registers `sku_totals` over `public.sales` and runs its direct build to
/// completion, as the discharge and a drain thread would (ADR-0016, #419),
/// leaving it `catching_up` with its go-live catch-up parked (#476).
async fn build_sku_totals_to_go_live(pool: &trellis::Pool, client: &Client) {
    let definition = install_definition(
        pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("sku", ValueType::Text),
            ("amount", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    assert_eq!(
        definition.status,
        TransformStatus::WaitingToBackfill,
        "registration only records the definition; the build is a background job"
    );
    publication::settle_builds(pool).await;
    assert_eq!(status_of(client, "public.sku_totals").await, "catching_up");
}

async fn total_of_sku_a(client: &Client) -> String {
    client
        .query_one("select total::text from sku_totals where sku = 'a'", &[])
        .await
        .expect("read sku_totals")
        .get(0)
}

/// Drains everything staged, then discharges the go-live catch-up, checking
/// `sku = 'a'` counts the change the build read once at both points. The
/// first check is the one that matters: it runs before the catch-up could
/// re-derive anything, so only the recompute horizon the build stamped on
/// the group can have kept the drained delta from counting the change again.
async fn assert_the_read_change_is_counted_once(pool: &trellis::Pool, client: &mut Client) {
    drain_to_quiescence(pool, client).await;
    assert_eq!(
        total_of_sku_a(client).await,
        "1012",
        "the change the build read is counted once, not again by its drained delta"
    );
    discharge_markers(pool, client).await;
    assert_eq!(status_of(client, "public.sku_totals").await, "live");
    assert_eq!(total_of_sku_a(client).await, "1012");
}

/// Issue #442: a change committed *before* the build reads the source, whose
/// CDC is still undrained when the definition goes live. The build counts the
/// change, and folding its delta in as well would leave the group at `2012`.
/// The recompute horizon the build stamps on each group row it writes (#419)
/// is above the change's LSN, so the delta re-derives the group instead.
#[tokio::test]
async fn aggregate_build_does_not_double_count_a_pre_fence_change_drained_after_go_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    commit_and_stage_insert(
        &mut client,
        "insert into public.sales values (4, 'a', 1000)",
        "public.sales",
        "4",
        r#"{"id":"4","sku":"a","amount":"1000"}"#,
    )
    .await;

    build_sku_totals_to_go_live(&db.pool, &client).await;
    assert_the_read_change_is_counted_once(&db.pool, &mut client).await;
}

/// Issue #442, the intake half: with intake lagging, a change the build read
/// may not have been staged at all by the time the definition goes live. Its
/// CDC reaches the ring only after the flip, carrying its original commit
/// position, and meets the same recompute horizon.
#[tokio::test]
async fn aggregate_build_does_not_double_count_a_read_change_intake_stages_after_go_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    let txn = client.transaction().await.expect("begin source write");
    txn.batch_execute("insert into public.sales values (4, 'a', 1000)")
        .await
        .expect("source write");
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the write's lsn")
        .get(0);
    txn.commit().await.expect("commit source write");

    build_sku_totals_to_go_live(&db.pool, &client).await;

    let txn = client.transaction().await.expect("begin late staging");
    trellis::staging::append(
        &txn,
        &[cdc_insert(
            "public.sales",
            "4",
            lsn,
            r#"{"id":"4","sku":"a","amount":"1000"}"#,
        )],
    )
    .await
    .expect("stage the change's CDC row after go-live");
    txn.commit().await.expect("commit late staging");
    assert_the_read_change_is_counted_once(&db.pool, &mut client).await;
}

/// Issue #442, the quarantine half: a change from before the build, parked in
/// `poison_held`, has left the ring, but releasing its key after go-live
/// replays it into the now-`live` definition. Release keeps each replayed
/// row's original LSN (doc 06), so the replay meets the build's recompute
/// horizon like any other late delta.
#[tokio::test]
async fn aggregate_build_does_not_double_count_a_parked_pre_fence_change_released_after_go_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_sales(&client).await;
    client
        .batch_execute(
            "insert into public.sales values (4, 'a', 1000); \
             insert into poison_held (src_table, key, seg_seq, op, lsn, new_image) \
             values ('public.sales', '4', 1, 'insert', pg_current_wal_insert_lsn(), \
                     '{\"id\":\"4\",\"sku\":\"a\",\"amount\":\"1000\"}')",
        )
        .await
        .expect("commit the change and park its CDC");

    build_sku_totals_to_go_live(&db.pool, &client).await;

    let replayed = trellis::staging::release_key(&db.pool, "public.sales", "4")
        .await
        .expect("release the parked key");
    assert_eq!(replayed, 1);
    assert_the_read_change_is_counted_once(&db.pool, &mut client).await;
}

/// Issue #442, the to-side half: an aggregate grouped by a relationship key
/// reads the relationship's to-side table too, and a to-side change the
/// build read can drain after go-live just the same. Moving customer 3 from
/// `apac` to `us` before the build, with its CDC still undrained, must leave
/// `us` at `7`, not add order 4's `4` a second time, before the catch-up
/// runs.
#[tokio::test]
async fn aggregate_build_does_not_double_count_a_read_to_side_change_drained_after_go_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.customers (id bigint primary key, region text); \
             alter table public.customers replica identity full; \
             create table public.orders (id bigint primary key, customer_id bigint, a numeric); \
             alter table public.orders replica identity full; \
             insert into public.customers values (1, 'eu'), (2, 'us'), (3, 'apac'); \
             insert into public.orders values (1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 3, 4)",
        )
        .await
        .expect("create + seed customers and orders");
    create_relationship(
        &db.pool,
        "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
    )
    .await
    .expect("create the customer relationship");

    let txn = client.transaction().await.expect("begin to-side write");
    txn.batch_execute("update public.customers set region = 'us' where id = 3")
        .await
        .expect("to-side write");
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the write's lsn")
        .get(0);
    trellis::staging::append(
        &txn,
        &[StagedChange::Cdc {
            src_table: "public.customers".to_string(),
            key: "3".to_string(),
            op: CdcOp::Update,
            lsn: Some(lsn),
            old_image: Some(r#"{"id":"3","region":"apac"}"#.to_string()),
            new_image: Some(r#"{"id":"3","region":"us"}"#.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }],
    )
    .await
    .expect("stage the to-side change's CDC row");
    txn.commit().await.expect("commit to-side write");

    install_definition(
        &db.pool,
        "TRANSFORM region_totals FROM orders GROUP BY customer.region SELECT sum(a) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("customer_id", ValueType::Numeric),
            ("a", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    publication::settle_builds(&db.pool).await;
    assert_eq!(
        status_of(&client, "public.region_totals").await,
        "catching_up"
    );
    let expected = vec!["eu=3".to_string(), "us=7".to_string()];
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        region_totals(&client).await,
        expected,
        "the to-side change the build read is counted once, not again by its drained delta"
    );
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.region_totals").await, "live");
    assert_eq!(region_totals(&client).await, expected);
}

async fn region_totals(client: &Client) -> Vec<String> {
    client
        .query(
            "select region || '=' || total::text from region_totals order by region",
            &[],
        )
        .await
        .expect("read region_totals")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// The relationship-enriched 1-1 shape, with the change on a relationship's
/// to-side table rather than the source. The change reaches the definition as
/// a from-side recompute that drains while it is still `backfilling`, and the
/// source itself never changed, so only a catch-up on `comments` recovers it.
#[tokio::test]
async fn relationship_build_recovers_a_to_side_change_drained_during_the_build() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.authors (id integer primary key, name text); \
             create table public.comments (id integer primary key, author_id integer); \
             alter table public.comments replica identity full; \
             insert into public.authors values (1, 'a'), (2, 'b'); \
             insert into public.comments values (200, 1), (201, 1), (202, 2)",
        )
        .await
        .expect("create + seed authors and comments");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create the comments relationship");
    install_build_hold(&client).await;
    client
        .query_one("select pg_advisory_lock($1)", &[&HOLD_LOCK])
        .await
        .expect("take the hold lock");

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT COUNT(comments.id) AS comment_count",
        &columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]),
        "public",
    )
    .await
    .expect("install the relationship-enriched 1-1");
    let pool = db.pool.clone();
    let build = tokio::spawn(async move { publication::settle_builds(&pool).await });
    wait_for_build_held(&client).await;

    commit_and_stage_insert(
        &mut client,
        "insert into public.comments values (203, 1)",
        "public.comments",
        "203",
        r#"{"id":"203","author_id":"1"}"#,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .query_one("select pg_advisory_unlock($1)", &[&HOLD_LOCK])
        .await
        .expect("release the build");
    build.await.expect("build task");
    assert_eq!(
        status_of(&client, "public.author_totals").await,
        "catching_up"
    );
    client
        .batch_execute("drop event trigger hold_direct_build")
        .await
        .expect("drop the build hold");

    let markers = pending_markers(&client).await;
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(
        status_of(&client, "public.author_totals").await,
        "live",
        "live once the catch-ups on both tables the build read have run"
    );

    let count: String = client
        .query_one(
            "select comment_count::text from author_totals where id = 1",
            &[],
        )
        .await
        .expect("read author_totals")
        .get(0);
    assert_eq!(
        count, "3",
        "the to-side change drained while the build was running is folded in"
    );
    assert_eq!(
        markers,
        vec!["public.authors".to_string(), "public.comments".to_string()],
        "the build parks a catch-up marker on every table it read"
    );
}

/// Issue #476: a definition that reads several tables goes `live` only with
/// the discharge of the last of its catch-ups. Discharging the source's
/// catch-up while the to-side's is still pending (held back here by a
/// failure backoff) leaves it `catching_up`: the to-side changes that drained
/// while it was building are still missing.
#[tokio::test]
async fn a_multi_table_build_goes_live_only_with_its_last_catch_up() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.authors (id integer primary key, name text); \
             create table public.comments (id integer primary key, author_id integer); \
             alter table public.comments replica identity full; \
             insert into public.authors values (1, 'a'); \
             insert into public.comments values (200, 1)",
        )
        .await
        .expect("create + seed authors and comments");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create the comments relationship");
    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT COUNT(comments.id) AS comment_count",
        &columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]),
        "public",
    )
    .await
    .expect("install the relationship-enriched 1-1");
    publication::settle_builds(&db.pool).await;
    assert_eq!(
        status_of(&client, "public.author_totals").await,
        "catching_up"
    );
    assert_eq!(
        pending_markers(&client).await,
        vec!["public.authors".to_string(), "public.comments".to_string()]
    );

    client
        .execute(
            "update pending_backfill set next_attempt_at = now() + interval '1 hour' \
             where table_name = 'public.comments'",
            &[],
        )
        .await
        .expect("hold the to-side's catch-up back");
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(
        pending_markers(&client).await,
        vec!["public.comments".to_string()],
        "only the source's catch-up was due"
    );
    assert_eq!(
        status_of(&client, "public.author_totals").await,
        "catching_up",
        "the to-side's catch-up is still pending"
    );

    client
        .execute(
            "update pending_backfill set next_attempt_at = null \
             where table_name = 'public.comments'",
            &[],
        )
        .await
        .expect("let the to-side's catch-up run");
    discharge_markers(&db.pool, &mut client).await;
    assert!(pending_markers(&client).await.is_empty());
    assert_eq!(
        status_of(&client, "public.author_totals").await,
        "live",
        "the last catch-up's discharge takes it live"
    );
}

/// Going live and parking the catch-ups commit together: when a park fails,
/// the definition is not left `live` with some of its tables uncovered, which
/// would lose a build-window change exactly as before the fix. The failure is
/// injected with a trigger rejecting the marker on the second table parked,
/// so finishing the direct-build job fails as a whole.
#[tokio::test]
async fn a_failed_catchup_park_does_not_leave_the_definition_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.authors (id integer primary key, name text); \
             create table public.comments (id integer primary key, author_id integer); \
             alter table public.comments replica identity full; \
             insert into public.authors values (1, 'a'); \
             insert into public.comments values (200, 1)",
        )
        .await
        .expect("create + seed authors and comments");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create the comments relationship");
    client
        .batch_execute(
            "create function reject_comments_marker() returns trigger \
             language plpgsql as $$ \
             begin raise exception 'injected: cannot park a marker on %', new.table_name; end $$; \
             create trigger reject_comments_marker before insert on pending_backfill \
               for each row when (new.table_name = 'public.comments') \
               execute function reject_comments_marker()",
        )
        .await
        .expect("install the park-failure trigger");

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT COUNT(comments.id) AS comment_count",
        &columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]),
        "public",
    )
    .await
    .expect("install the relationship-enriched 1-1");
    publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the direct-build job");
    const WORKER: &str = "direct_build_catchup_marker_worker";
    let job = {
        let conn = db.pool.get().await.expect("acquire connection");
        chunk_queue::claim_chunks(&**conn, WORKER, 10)
            .await
            .expect("claim the job")
    };
    assert_eq!(job.len(), 1);
    chunk_queue::run_claimed_chunk(
        &db.pool,
        &job[0],
        WORKER,
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
    .await
    .expect("run the build");
    let outcome = chunk_queue::finish_chunk(&db.pool, &job[0], WORKER).await;
    assert!(outcome.is_err(), "the injected park failure surfaces");

    let status = status_of(&client, "public.author_totals").await;
    assert_ne!(
        status, "live",
        "a definition whose catch-ups didn't all park must not be live"
    );
    assert!(
        pending_markers(&client).await.is_empty(),
        "no catch-up is left half-parked"
    );
}

/// A direct-build job a worker still holds when its definition is paused and
/// resumed again keeps running: the pause only withholds new claims. Its
/// worker reaches its writes, with its old read, after the resume's rebuild
/// has already gone live. The claim's fence (#434) stops it there: it writes
/// nothing, so the live target keeps the rebuild's read and nothing needs a
/// catch-up. A write landing then would have bypassed the target-mutation
/// seam, which a reader attached after the rebuild depends on.
#[tokio::test]
async fn a_superseded_job_reaching_its_writes_after_the_rebuild_went_live_writes_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2); \
             create table public.hold_armed (armed boolean)",
        )
        .await
        .expect("create + seed sales");
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("sku", ValueType::Text),
            ("amount", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    publication::settle_registrations(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");

    // Holds a build between its read and its write only while armed, so the
    // superseded job can be parked there and the rebuild run past it.
    client
        .batch_execute(&format!(
            "create function hold_direct_build() returns event_trigger \
             language plpgsql as $$ \
             begin \
               if exists (select 1 from public.hold_armed) then \
                 perform pg_advisory_lock({HOLD_LOCK}); \
                 perform pg_advisory_unlock({HOLD_LOCK}); \
               end if; \
             end $$; \
             create event trigger hold_direct_build on ddl_command_start \
               when tag in ('ALTER TABLE') execute function hold_direct_build(); \
             insert into public.hold_armed values (true)"
        ))
        .await
        .expect("install the build hold");
    client
        .query_one("select pg_advisory_lock($1)", &[&HOLD_LOCK])
        .await
        .expect("take the hold lock");

    let operator = trellis::Trellis::connect(
        trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis");
    let pause_and_resume = || async {
        operator
            .apply("PAUSE TRANSFORM sku_totals")
            .await
            .expect("pause");
        operator
            .apply("RESUME TRANSFORM sku_totals")
            .await
            .expect("resume");
    };

    // The first resume's rebuild job reads the source and is held.
    pause_and_resume().await;
    publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the first rebuild");
    const OLD_WORKER: &str = "superseded_worker";
    let superseded = {
        let conn = db.pool.get().await.expect("acquire connection");
        chunk_queue::claim_chunks(&**conn, OLD_WORKER, 10)
            .await
            .expect("claim the first rebuild")
    };
    assert_eq!(superseded.len(), 1);
    let pool = db.pool.clone();
    let job = superseded[0].clone();
    let old_build = tokio::spawn(async move {
        chunk_queue::run_claimed_chunk(
            &pool,
            &job,
            OLD_WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
    });
    wait_for_build_held(&client).await;

    // A change the held job didn't read. Its CDC drains while the definition
    // isn't live, so it reaches the target only through a rebuild's read.
    client
        .batch_execute("update public.sales set amount = 1007 where id = 2")
        .await
        .expect("change the source");

    // Paused and resumed again, the held job is superseded, and the second
    // rebuild runs past the hold and goes live.
    pause_and_resume().await;
    client
        .batch_execute("delete from public.hold_armed")
        .await
        .expect("disarm the hold");
    publication::settle_registrations(&db.pool).await;
    discharge_markers(&db.pool, &mut client).await;
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    let read_a = async |client: &Client| -> String {
        client
            .query_one("select total::text from sku_totals where sku = 'a'", &[])
            .await
            .expect("read sku_totals")
            .get(0)
    };
    assert_eq!(read_a(&client).await, "1012", "the rebuild read the change");

    // The superseded job now reaches its writes, is fenced out of them, and
    // gives up its claim, which discards it.
    client
        .query_one("select pg_advisory_unlock($1)", &[&HOLD_LOCK])
        .await
        .expect("release the superseded job");
    old_build
        .await
        .expect("superseded build task")
        .expect("a superseded job is not a failure");
    chunk_queue::finish_chunk(&db.pool, &superseded[0], OLD_WORKER)
        .await
        .expect("give up the superseded job");
    client
        .batch_execute("drop event trigger hold_direct_build")
        .await
        .expect("drop the build hold");
    assert_eq!(
        read_a(&client).await,
        "1012",
        "the superseded job didn't overwrite the rebuild's read"
    );
    assert!(
        pending_markers(&client).await.is_empty(),
        "there is nothing to catch up on"
    );
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
}
