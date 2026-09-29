//! Integration tests for claim liveness (issue #15, stage 04's third
//! piece), run against a real, ephemeral Postgres instance via the shared
//! harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/04-claiming-and-the-fold.md, "Keeping a
//! claim alive" and "Two ways a claim comes back" for the design these
//! tests hold the implementation to. Staleness is made by backdating a
//! claim's `claimed_at` rather than by waiting out a short TTL against a
//! live heartbeat, and the daemon's timing is only ever waited on as a lower
//! bound (issue #513), so nothing here depends on how promptly a loaded box
//! schedules a thread.
//!
//! [`FenceMissBackoff`]'s pure sequence is covered in-module
//! (`trellis/src/staging/liveness.rs`'s `#[cfg(test)]`), not here.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_aggregate_target_table, create_definition, parse};
use trellis::staging::apply::{self, DrainHooks};
use trellis::staging::{
    HeartbeatDaemon, HeartbeatDaemonConfig, SegmentState, StagedWatermark, claim, liveness, seal,
};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `claims.rs`/`sealing.rs`/`fold.rs`'s convention.
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

async fn insert_recompute(client: &Client, table: &str, key: &str) {
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) \
                 values ('orders', $1, 'recompute', 0)"
            ),
            &[&key],
        )
        .await
        .expect("insert recompute row");
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn segment_state(client: &Client, seg_seq: i64) -> SegmentState {
    let state: String = client
        .query_one("select state from segments where seg_seq = $1", &[&seg_seq])
        .await
        .expect("read segment state")
        .get(0);
    SegmentState::from_sql(&state).unwrap_or_else(|| panic!("unrecognized state {state:?}"))
}

async fn claimed_buckets(client: &Client, seg_seq: i64, claimed_by: &str) -> Vec<i16> {
    client
        .query(
            "select bucket from seg_claims where seg_seq = $1 and claimed_by = $2 order by bucket",
            &[&seg_seq, &claimed_by],
        )
        .await
        .expect("read seg_claims")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// A single-bucket sealed batch, ready to be claimed — the fixture every
/// test in this file starts from.
async fn seal_one_bucket_batch(client: &mut Client, key: &str) -> i64 {
    insert_recompute(client, "seg_0", key).await;
    seal_active_segment(client).await
}

/// Backdates every claim on `seg_seq` by an hour: far past any TTL a test
/// sweeps with, without waiting for real time to pass.
async fn backdate_claims(client: &Client, seg_seq: i64) {
    client
        .execute(
            "update seg_claims set claimed_at = now() - interval '1 hour' where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("backdate claims");
}

/// How long a test waits on something the daemon does before calling it a
/// hang. Only ever a bound on a hang: no test asserts the daemon got there
/// quickly.
const HANG: Duration = Duration::from_secs(30);

/// Polls `probe` every 10ms until it holds, panicking with `what` after
/// [`HANG`].
async fn wait_until<F, Fut>(what: &str, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + HANG;
    while !probe().await {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn reclaim_frees_a_stale_claim_and_a_fresh_claim_picks_it_up() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    let ttl = Duration::from_millis(300);

    let won = claim::claim(&client, seg_seq, "dead-worker", 1)
        .await
        .expect("initial claim");
    assert!(!won.is_empty(), "the initial claim must win the one bucket");

    // No heartbeat at all: after the TTL, the claim is stale.
    tokio::time::sleep(ttl + Duration::from_millis(100)).await;

    let reclaimed = liveness::reclaim_stale(&client, ttl)
        .await
        .expect("reclaim_stale");
    assert_eq!(reclaimed, 1, "the one stale claim must be reclaimed");
    assert!(
        claimed_buckets(&client, seg_seq, "dead-worker")
            .await
            .is_empty(),
        "the dead worker's claim row must be gone"
    );

    // A fresh claim by a different worker now wins those buckets — the
    // batch stayed `draining` throughout, so it just re-picks the freed
    // buckets normally.
    assert_eq!(
        segment_state(&client, seg_seq).await,
        SegmentState::Draining
    );
    let re_won = claim::claim(&client, seg_seq, "fresh-worker", 1)
        .await
        .expect("re-claim");
    assert_eq!(
        re_won, won,
        "the fresh worker must win exactly the buckets the dead worker lost"
    );

    // TODO(#11): assert the reclaimed worker's apply rolls back once apply
    // exists (blocked on aggregate transform-defs) — today's test only
    // covers the claim-side reclaim, not the apply-side rollback.
}

#[tokio::test]
async fn a_daemon_heartbeat_survives_a_bulk_drain_that_outlives_the_ttl() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let daemon_seg = seal_one_bucket_batch(&mut client, "daemon-kept").await;
    let plain_seg = seal_one_bucket_batch(&mut client, "never-heartbeat").await;

    // A production-sized TTL: both claims are made stale by backdating them
    // past it, never by real time elapsing against it.
    let ttl = Duration::from_secs(60);
    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_millis(50),
            idle_timeout: Duration::from_secs(60),
        },
    );

    let daemon_won = claim::claim(&client, daemon_seg, "bulk-worker", 1)
        .await
        .expect("claim daemon-tracked batch");
    assert!(!daemon_won.is_empty());
    daemon.register(daemon_seg, "bulk-worker").await;

    let plain_won = claim::claim(&client, plain_seg, "unwatched-worker", 1)
        .await
        .expect("claim plain batch");
    assert!(!plain_won.is_empty());

    // Simulate a bulk-shape drain that has outlived the TTL: no in-line
    // `claimed_at` refresh at all, and both claims an hour old. The daemon is
    // the only thing that can bring `daemon_seg`'s claim back within the TTL.
    backdate_claims(&client, daemon_seg).await;
    backdate_claims(&client, plain_seg).await;
    wait_until("the daemon to refresh its backdated claim", || async {
        client
            .query_one(
                "select bool_and(claimed_at >= now() - (interval '1 second' * $3)) \
                 from seg_claims where seg_seq = $1 and claimed_by = $2",
                &[&daemon_seg, &"bulk-worker", &ttl.as_secs_f64()],
            )
            .await
            .expect("read the daemon-tracked claim")
            .get::<_, Option<bool>>(0)
            .unwrap_or(false)
    })
    .await;

    let reclaimed = liveness::reclaim_stale(&client, ttl)
        .await
        .expect("reclaim_stale");
    assert_eq!(
        reclaimed, 1,
        "exactly the plain (non-daemon-tracked) claim must be reclaimed"
    );

    assert_eq!(
        claimed_buckets(&client, daemon_seg, "bulk-worker").await,
        daemon_won,
        "the daemon-tracked claim must survive the sweep unchanged"
    );
    assert!(
        claimed_buckets(&client, plain_seg, "unwatched-worker")
            .await
            .is_empty(),
        "the claim nobody heartbeat must be gone"
    );

    daemon.deregister(daemon_seg, "bulk-worker").await;
}

#[tokio::test]
async fn release_is_scoped_to_claimed_by_and_leaves_other_workers_claims_alone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    client
        .execute(
            "update segments set bucket_count = 8 where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("widen bucket_count so both workers can hold a bucket");

    let won_a = claim::claim(&client, seg_seq, "worker-a", 2)
        .await
        .expect("worker a claim");
    let won_b = claim::claim(&client, seg_seq, "worker-b", 2)
        .await
        .expect("worker b claim");
    assert!(!won_a.is_empty());
    assert!(!won_b.is_empty());

    let released = liveness::release(&client, seg_seq, "worker-a")
        .await
        .expect("release worker-a");
    assert_eq!(
        released,
        won_a.len() as u64,
        "release must delete exactly the buckets worker-a held"
    );

    assert!(
        claimed_buckets(&client, seg_seq, "worker-a")
            .await
            .is_empty(),
        "worker-a's claim must be gone"
    );
    assert_eq!(
        claimed_buckets(&client, seg_seq, "worker-b").await,
        won_b,
        "worker-b's claim must be untouched by worker-a's release"
    );

    // Releasing again (already released) matches zero rows — a no-op, not
    // an error, and it must not touch worker-b either.
    let released_again = liveness::release(&client, seg_seq, "worker-a")
        .await
        .expect("release worker-a again");
    assert_eq!(released_again, 0);
    assert_eq!(claimed_buckets(&client, seg_seq, "worker-b").await, won_b);
}

/// Issue #660: the worker loop's release after a failed drain covers every
/// segment of the batch in one statement and names the `(seg_seq, bucket)`
/// claims it freed, so the drain-failure log can report them. Scoped to the
/// claimant exactly like [`liveness::release`].
#[tokio::test]
async fn release_segments_frees_and_reports_only_the_claimants_buckets() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let first = seal_one_bucket_batch(&mut client, "k1").await;
    let second = seal_one_bucket_batch(&mut client, "k2").await;
    client
        .execute(
            "update segments set bucket_count = 8 where seg_seq = any($1)",
            &[&vec![first, second]],
        )
        .await
        .expect("widen bucket_count so both workers can hold a bucket");

    let mut expected = Vec::new();
    for seg_seq in [first, second] {
        let mut won = claim::claim(&client, seg_seq, "worker-a", 2)
            .await
            .expect("worker a claim");
        assert!(!won.is_empty());
        won.sort_unstable();
        expected.extend(won.into_iter().map(|bucket| (seg_seq, bucket)));
    }
    let won_b = claim::claim(&client, first, "worker-b", 2)
        .await
        .expect("worker b claim");
    assert!(!won_b.is_empty());

    let released = liveness::release_segments(&client, &[first, second], "worker-a")
        .await
        .expect("release worker-a");
    assert_eq!(released, expected, "exactly worker-a's claims, in order");
    assert!(claimed_buckets(&client, first, "worker-a").await.is_empty());
    assert!(
        claimed_buckets(&client, second, "worker-a")
            .await
            .is_empty()
    );
    assert_eq!(
        claimed_buckets(&client, first, "worker-b").await.len(),
        won_b.len(),
        "worker-b's claim must be untouched"
    );

    let released_again = liveness::release_segments(&client, &[first, second], "worker-a")
        .await
        .expect("release worker-a again");
    assert!(released_again.is_empty());
}

#[tokio::test]
async fn daemon_opens_no_connection_for_a_claim_that_never_lasts_a_full_interval() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_secs(3600), // long enough this test never ticks
            idle_timeout: Duration::from_secs(60),
        },
    );

    daemon.register(1, "fast-worker").await;
    daemon.deregister(1, "fast-worker").await;
    // No tick has fired (the interval is an hour), so the registry's
    // register-then-deregister must never have been observed.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        daemon.connections_opened(),
        0,
        "a claim that never survives one full interval must never cost a connection"
    );
    assert!(!daemon.is_connected());
}

#[tokio::test]
async fn daemon_closes_its_connection_after_idle_timeout_with_no_registered_claims() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    claim::claim(&client, seg_seq, "worker", 1)
        .await
        .expect("claim");

    let idle_timeout = Duration::from_millis(200);
    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_millis(50),
            idle_timeout,
        },
    );

    daemon.register(seg_seq, "worker").await;
    wait_until(
        "the daemon to open a connection for a claim that outlived one interval",
        || async { daemon.is_connected() },
    )
    .await;
    assert_eq!(daemon.connections_opened(), 1);

    // The idle clock starts at the first tick that sees the registry empty,
    // which is no earlier than this, so the close can't be observed before
    // `idle_timeout` has passed from here: a lower bound that holds however
    // late the daemon's ticks run.
    let deregistered_at = Instant::now();
    daemon.deregister(seg_seq, "worker").await;
    wait_until(
        "the daemon to close its connection once idle for longer than idle_timeout",
        || async { !daemon.is_connected() },
    )
    .await;
    assert!(
        deregistered_at.elapsed() >= idle_timeout,
        "the daemon closed its connection after {:?}, before its {idle_timeout:?} idle timeout",
        deregistered_at.elapsed()
    );
}

/// Issue #654, first half. A page's claim check doubles as a heartbeat, and it
/// used to stamp `claimed_at = now()`: the page transaction's *start* time. A
/// page whose apply ran for 18 s therefore wrote a claim that was already 18 s
/// old at commit, overwriting whatever fresher value the daemon had set while
/// the page ran (the daemon skips the row only while the page holds its lock).
/// The sweep then saw a live claim as old as the page, and one page longer than
/// the TTL let it reclaim a claim still in use.
///
/// Here the page's apply is held behind a lock on the target table. The claim
/// its commit leaves behind must be stamped no earlier than the moment the
/// lock was released, i.e. at the time of the heartbeat, not at the time the
/// transaction began. No timing tolerance: the comparison is between two
/// server clock readings with a strict order between them.
#[tokio::test]
async fn a_long_page_leaves_its_claim_stamped_at_commit_time_not_transaction_start() {
    const GROUP_TOTALS: &str = "TRANSFORM grp_totals FROM items GROUP BY grp \
         SELECT grp AS grp, SUM(amount) AS total, COUNT(*) AS n";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table items (id integer primary key, grp integer, amount numeric); \
             alter table items replica identity full",
        )
        .await
        .expect("create items");
    let columns: HashMap<String, ValueType> = ["id", "grp", "amount"]
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect();
    let def = parse(GROUP_TOTALS).expect("parse");
    create_definition(&db.pool, GROUP_TOTALS, &columns)
        .await
        .expect("create aggregate definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target");

    // 300 keys at cap 40: page 1 is not the last page, so it runs the claim
    // check (and heartbeat) rather than the completion.
    let slot = trellis::staging::active_ring_slot(&client)
        .await
        .expect("active ring slot");
    let src_table = format!("{DEFAULT_SCHEMA}.items");
    for id in 1..=300i32 {
        client
            .execute(
                "insert into items (id, grp, amount) values ($1, $1 % 7, $1)",
                &[&id],
            )
            .await
            .expect("insert item");
        let image = format!(r#"{{"grp":"{}","amount":"{id}"}}"#, id % 7);
        client
            .execute(
                &format!(
                    "insert into seg_{slot} (src_table, key, op, lsn, new_image, hop_gen) \
                     values ($1, $2, 'insert', pg_current_wal_insert_lsn(), $3::text::jsonb, 0)"
                ),
                &[&src_table, &id.to_string(), &image],
            )
            .await
            .expect("stage insert");
    }
    let seg = seal_active_segment(&mut client).await;

    // Writes to the target wait behind this lock; reads (the fold, compute)
    // do not.
    let mut blocker = connect_raw(db.dsn()).await;
    let blocker_txn = blocker.transaction().await.expect("begin blocker");
    blocker_txn
        .batch_execute("lock table public.grp_totals in exclusive mode")
        .await
        .expect("lock the target");

    let pool = db.pool.clone();
    let drain = tokio::spawn(async move {
        let mut hooks = DrainHooks {
            stop_after_pages: Some(1),
            ..DrainHooks::default()
        };
        apply::drain_many_with_hooks(
            &pool,
            &[seg],
            "worker",
            1,
            "trellis_liveness_test",
            &StagedWatermark::saturated(),
            40,
            &mut hooks,
        )
        .await
    });

    // Waiting on a precondition, not on convergence: the page's apply
    // transaction has begun and is parked on the lock.
    wait_until("the page's apply to block on the target lock", || async {
        client
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where datname = current_database() and wait_event_type = 'Lock')",
                &[],
            )
            .await
            .expect("read pg_stat_activity")
            .get::<_, bool>(0)
    })
    .await;
    // Let the blocked transaction age visibly before releasing it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let released_at: SystemTime = blocker_txn
        .query_one("select clock_timestamp()", &[])
        .await
        .expect("read the release time")
        .get(0);
    blocker_txn.commit().await.expect("release the target lock");

    let outcome = drain
        .await
        .expect("drain task")
        .expect("drain")
        .expect("the drain claims the segment");
    assert_eq!(outcome.pages, 1, "the hook stops after the first page");

    let stamps: Vec<SystemTime> = client
        .query(
            "select claimed_at from seg_claims where seg_seq = $1 and claimed_by = 'worker'",
            &[&seg],
        )
        .await
        .expect("read the claim")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(!stamps.is_empty(), "the claim is still held after page 1");
    for stamp in stamps {
        let lag = released_at
            .duration_since(stamp)
            .map(|d| format!("{d:?} before"))
            .unwrap_or_else(|_| "after".into());
        assert!(
            stamp >= released_at,
            "page 1's heartbeat stamped the claim {lag} the target lock was released, \
             so the sweep sees a live claim as old as the page's transaction"
        );
    }
}

/// Issue #654, second half. `drainers.last_seen` used to be bumped only by the
/// worker loop, between drains, so a worker inside a paged drain longer than
/// the drainer window stopped counting toward the share denominator. The
/// daemon that keeps a worker's claims alive now keeps its drainer row alive
/// too: a claimant the daemon holds claims for is refreshed, one it doesn't
/// is left to age out.
#[tokio::test]
async fn a_daemon_keeps_its_claimants_drainer_row_fresh_through_a_long_drain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    assert!(
        !claim::claim(&client, seg_seq, "busy-worker", 1)
            .await
            .expect("claim")
            .is_empty()
    );
    // Both workers last came around their loop an hour ago: the busy one
    // because it has been inside one drain ever since.
    for id in ["busy-worker", "idle-worker"] {
        claim::register_drainer(&client, id)
            .await
            .expect("register drainer");
    }
    client
        .execute(
            "update drainers set last_seen = now() - interval '1 hour'",
            &[],
        )
        .await
        .expect("backdate drainers");

    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_millis(50),
            idle_timeout: Duration::from_secs(60),
        },
    );
    daemon.register(seg_seq, "busy-worker").await;

    let window = claim::DEFAULT_DRAINER_WINDOW.as_secs_f64();
    let live = |id: &'static str| {
        let client = &client;
        async move {
            client
                .query_one(
                    "select last_seen > now() - (interval '1 second' * $2) \
                     from drainers where drainer_id = $1",
                    &[&id, &window],
                )
                .await
                .expect("read drainer")
                .get::<_, bool>(0)
        }
    };
    wait_until(
        "the daemon to refresh the busy worker's drainer row",
        || live("busy-worker"),
    )
    .await;
    assert!(
        !live("idle-worker").await,
        "a worker with no claim registered must not be kept alive"
    );

    daemon.deregister(seg_seq, "busy-worker").await;
}
