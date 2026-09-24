//! Issue #430: a direct build (aggregate, or relationship-enriched 1-1) must
//! not lose a source change that drains while the build is still running.
//!
//! The backfill discharge dispatches these shapes' build as one background job
//! (ADR-0016, #419), which a drain worker runs while the definition sits
//! `backfilling`, and the apply path skips any definition that isn't `live`
//! (`catalog::dependents_of`). A change to an already-published source that
//! commits after the build's read and drains before go-live is therefore
//! skipped by the drain and missing from what the build wrote. Only a catch-up
//! marker parked at go-live (`complete_direct_backfill`) brings it back.
//!
//! The tests hold the build between its read and its target write with an
//! event trigger (see [`install_build_hold`]) that blocks on an advisory lock
//! the test holds. While the build is parked there, the test commits a change,
//! stages its CDC row and drains it, then lets the build finish.
//!
//! Issue #442 is the reverse case: a change committed before the build's
//! coverage fence whose delta drains only after go-live, and is folded in on
//! top of the build's own read of it. Those tests check the build keeps its
//! coverage record only when no change from before its fence can still
//! arrive that way.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, create_relationship, install_definition};
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
    let change = StagedChange::Cdc {
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
    };
    trellis::staging::append(&txn, &[change])
        .await
        .expect("stage the change's CDC row");
    txn.commit().await.expect("commit source write");
}

/// The #416 reviewer's repro: an aggregate build on an already-published
/// source, held between its read and its target write while a change to the
/// source drains. Before the fix the target ended one change short and no
/// catch-up marker was parked.
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
    let build = tokio::spawn(async move { publication::settle_registrations(&pool).await });
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
    assert_eq!(status_of(&client, "public.sku_totals").await, "live");
    client
        .batch_execute("drop event trigger hold_direct_build")
        .await
        .expect("drop the build hold");

    let markers = pending_markers(&client).await;
    discharge_markers(&db.pool, &mut client).await;

    let total: String = client
        .query_one("select total::text from sku_totals where sku = 'a'", &[])
        .await
        .expect("read sku_totals")
        .get(0);
    assert_eq!(
        total, "1012",
        "the change drained while the build was running is folded in"
    );
    assert_eq!(
        markers,
        vec!["public.sales".to_string()],
        "going live parks a catch-up marker on the source, as the chunked path does"
    );
}

/// Issue #442: a change committed *before* the build's coverage fence, whose
/// CDC is still undrained when the definition goes live. The build reads the
/// change and the drain folds its delta in again after the flip. The fence
/// sees every row, so a coverage record would let the go-live catch-up skip
/// the re-derivation that corrects the double fold, and the group would stay
/// at `2012`.
#[tokio::test]
async fn aggregate_build_does_not_double_count_a_pre_fence_change_drained_after_go_live() {
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
    commit_and_stage_insert(
        &mut client,
        "insert into public.sales values (4, 'a', 1000)",
        "public.sales",
        "4",
        r#"{"id":"4","sku":"a","amount":"1000"}"#,
    )
    .await;

    let definition = install_definition(
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
    assert_eq!(definition.status, TransformStatus::Live);

    let covered: bool = client
        .query_one(
            "select exists (select 1 from backfill_coverage where table_name = 'public.sales')",
            &[],
        )
        .await
        .expect("read backfill_coverage")
        .get(0);
    assert!(
        !covered,
        "the build's source still had undrained CDC from before its fence, \
         so its coverage must not let the catch-up skip re-deriving"
    );

    drain_to_quiescence(&db.pool, &mut client).await;
    discharge_markers(&db.pool, &mut client).await;

    let total: String = client
        .query_one("select total::text from sku_totals where sku = 'a'", &[])
        .await
        .expect("read sku_totals")
        .get(0);
    assert_eq!(
        total, "1012",
        "the pre-fence change is counted once, not by both the build and its drained delta"
    );
}

/// The quiet-source counterpart: nothing is in flight for the source when the
/// build goes live, so the coverage record stands and the catch-up skips
/// re-reading the source (issue #79's optimization is kept).
#[tokio::test]
async fn aggregate_build_on_a_quiet_source_keeps_its_coverage() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
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

    let covered: bool = client
        .query_one(
            "select exists (select 1 from backfill_coverage where table_name = 'public.sales')",
            &[],
        )
        .await
        .expect("read backfill_coverage")
        .get(0);
    assert!(
        covered,
        "nothing was in flight, so the coverage record stands"
    );
}

/// Issue #442, the quarantine half: a pre-fence change parked in
/// `poison_held` has drained out of the ring, but releasing its key replays
/// it into the now-`live` definition on top of the build's own read, so it
/// counts as still in flight and the coverage is cleared.
#[tokio::test]
async fn aggregate_build_with_a_parked_pre_fence_change_clears_its_coverage() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             insert into public.sales values (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), \
                                             (4, 'a', 1000)",
        )
        .await
        .expect("create + seed sales");
    client
        .batch_execute(
            "insert into poison_held (src_table, key, seg_seq, op, lsn, new_image) \
             values ('public.sales', '4', 1, 'insert', pg_current_wal_insert_lsn(), \
                     '{\"id\":\"4\",\"sku\":\"a\",\"amount\":\"1000\"}')",
        )
        .await
        .expect("park the pre-fence change");

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

    let covered: bool = client
        .query_one(
            "select exists (select 1 from backfill_coverage where table_name = 'public.sales')",
            &[],
        )
        .await
        .expect("read backfill_coverage")
        .get(0);
    assert!(
        !covered,
        "a parked pre-fence change replays on release, so the coverage must not stand"
    );
}

/// Issue #442, the intake half: with a slot in place, a change the build read
/// may not have been staged yet at all, so the ring alone can't show it has
/// drained. Coverage stands only once intake's durable progress has passed the
/// fence.
#[tokio::test]
async fn aggregate_build_keeps_coverage_only_once_intake_has_passed_its_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.sales (id integer primary key, sku text, amount integer); \
             create table public.refunds (id integer primary key, sku text, amount integer); \
             alter table public.sales replica identity full; \
             alter table public.refunds replica identity full; \
             insert into public.sales values (1, 'a', 5); \
             insert into public.refunds values (1, 'a', 3)",
        )
        .await
        .expect("create + seed sources");
    // Its own statement: a slot can't be created in a transaction that wrote.
    client
        .execute(
            "select pg_create_logical_replication_slot('coverage_442', 'pgoutput')",
            &[],
        )
        .await
        .expect("create a slot");
    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) \
             values ('coverage_442', '0/0')",
            &[],
        )
        .await
        .expect("record the slot's lagging progress");
    let cols = columns(&[
        ("id", ValueType::Numeric),
        ("sku", ValueType::Text),
        ("amount", ValueType::Numeric),
    ]);
    let covered = |table: &'static str| {
        let client = &client;
        async move {
            client
                .query_one(
                    "select exists (select 1 from backfill_coverage where table_name = $1)",
                    &[&table],
                )
                .await
                .expect("read backfill_coverage")
                .get::<_, bool>(0)
        }
    };

    install_definition(
        &db.pool,
        "TRANSFORM sku_sales FROM sales GROUP BY sku SELECT sum(amount) AS total",
        &cols,
        "public",
    )
    .await
    .expect("install behind a lagging intake");
    assert!(
        !covered("public.sales").await,
        "intake hasn't staged through the fence, so a change the build read may still stream"
    );

    client
        .execute(
            "update replication_progress set confirmed_lsn = 'FFFFFFFF/FFFFFFFF'",
            &[],
        )
        .await
        .expect("move intake past any fence");
    install_definition(
        &db.pool,
        "TRANSFORM sku_refunds FROM refunds GROUP BY sku SELECT sum(amount) AS total",
        &cols,
        "public",
    )
    .await
    .expect("install behind a caught-up intake");
    assert!(
        covered("public.refunds").await,
        "intake is past the fence and nothing is pending, so the coverage stands"
    );

    client
        .execute("select pg_drop_replication_slot('coverage_442')", &[])
        .await
        .expect("drop the slot");
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
    let build = tokio::spawn(async move { publication::settle_registrations(&pool).await });
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
    assert_eq!(status_of(&client, "public.author_totals").await, "live");
    client
        .batch_execute("drop event trigger hold_direct_build")
        .await
        .expect("drop the build hold");

    let markers = pending_markers(&client).await;
    discharge_markers(&db.pool, &mut client).await;

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
        "going live parks a catch-up marker on every table the build read"
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
    chunk_queue::run_claimed_chunk(&db.pool, &job[0], WORKER, Duration::from_secs(5))
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
/// worker can write the target from its old read after the resume's rebuild
/// has already gone live and recorded its coverage, and the source may not
/// change after that at all. Discarding the stale job parks a catch-up on the
/// source (#331) that must re-derive the target anyway, not skip the source
/// as covered by the rebuild: the rebuild's coverage vouches for its own
/// write, not for one that landed on top of it.
#[tokio::test]
async fn a_superseded_job_writing_after_the_rebuild_went_live_is_repaired() {
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
        chunk_queue::run_claimed_chunk(&pool, &job, OLD_WORKER, Duration::from_secs(5)).await
    });
    wait_for_build_held(&client).await;

    // A change the held job didn't read. Its CDC drains while the definition
    // isn't live, so it reaches the target only through a rebuild's read.
    client
        .batch_execute("update public.sales set amount = 1007 where id = 2")
        .await
        .expect("change the source");

    // Paused and resumed again, the held job is superseded, and the second
    // rebuild runs past the hold, goes live and records its coverage.
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

    // The superseded job now writes its old read on top, and gives up its
    // claim, which discards it and parks the source's catch-up.
    client
        .query_one("select pg_advisory_unlock($1)", &[&HOLD_LOCK])
        .await
        .expect("release the superseded job");
    old_build
        .await
        .expect("superseded build task")
        .expect("the superseded job's build runs to the end");
    chunk_queue::finish_chunk(&db.pool, &superseded[0], OLD_WORKER)
        .await
        .expect("give up the superseded job");
    client
        .batch_execute("drop event trigger hold_direct_build")
        .await
        .expect("drop the build hold");
    assert_eq!(
        pending_markers(&client).await,
        vec!["public.sales".to_string()],
        "discarding the superseded job parks the source's catch-up"
    );
    client
        .batch_execute("select txid_current()")
        .await
        .expect("consume an xid");
    discharge_markers(&db.pool, &mut client).await;

    assert_eq!(
        read_a(&client).await,
        "1012",
        "the catch-up re-derives what the superseded job overwrote"
    );
}
