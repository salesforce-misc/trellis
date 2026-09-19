//! Integration tests for claim liveness (issue #15, stage 04's third
//! piece), run against a real, ephemeral Postgres instance via the shared
//! harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/04-claiming-and-the-fold.md, "Keeping a
//! claim alive" and "Two ways a claim comes back" for the design these
//! tests hold the implementation to. Short real-time windows throughout
//! (ttl ~300ms, daemon interval ~50ms) so this runs fast against the real
//! cluster.
//!
//! [`FenceMissBackoff`]'s pure sequence is covered in-module
//! (`trellis/src/staging/liveness.rs`'s `#[cfg(test)]`), not here.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{
    HeartbeatDaemon, HeartbeatDaemonConfig, SegmentState, claim, liveness, seal,
};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `claims.rs`/`sealing.rs`/`fold.rs`'s convention.
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
    seal::seal_phase2(client, outcome.sealed_seg_seq)
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

    let ttl = Duration::from_millis(300);
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

    // Simulate a bulk-shape drain: no in-line `claimed_at` refresh at all,
    // sleep well past the TTL. The daemon (registered, ticking every 50ms)
    // is the only thing keeping `daemon_seg`'s claim alive.
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;

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

    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_millis(50),
            idle_timeout: Duration::from_millis(200),
        },
    );

    daemon.register(seg_seq, "worker").await;
    // Let a few ticks pass so the daemon actually opens its connection.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        daemon.is_connected(),
        "the daemon must have opened a connection for a claim that outlived one interval"
    );
    assert_eq!(daemon.connections_opened(), 1);

    daemon.deregister(seg_seq, "worker").await;
    // Wait past the idle timeout (measured in ticks past the registry going
    // empty), plus slack for a couple of ticks to observe it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !daemon.is_connected(),
        "the daemon must close its connection once idle for longer than idle_timeout"
    );
}
