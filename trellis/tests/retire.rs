//! Integration tests for stage 06's retirement pass (issue #13/#58), run
//! against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/06-cleanup-and-reclaim.md, "Retiring a
//! batch", for the four eligibility conditions these tests hold the
//! implementation to.

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{StagingError, retire, seal};

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

async fn seal_and_fence(sealer: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(sealer).await.expect("seal phase 1");
    seal::seal_phase2(sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn mark_drained(client: &Client, seg_seq: i64) {
    client
        .execute(
            "update segments set state = 'drained' where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("mark drained");
}

async fn live_seg_seqs(client: &Client) -> Vec<i64> {
    client
        .query("select seg_seq from segments order by seg_seq", &[])
        .await
        .expect("read segments")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// The straight-line case: two segments sealed, both drained and truncated,
/// the older one's successor also drained — all four conditions hold, so it
/// retires.
#[tokio::test]
async fn a_fully_drained_batch_with_a_drained_successor_retires() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    seal_and_fence(&mut sealer).await; // segment 2, slot 1

    mark_drained(&sealer, 1).await;
    mark_drained(&sealer, 2).await;
    sealer
        .batch_execute("truncate seg_0; truncate seg_1")
        .await
        .expect("truncate seg_0 and seg_1");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments");
    assert_eq!(retired, vec![1]);
    assert_eq!(live_seg_seqs(&sealer).await, vec![2, 3]);
}

/// The `TRUNCATE`'s actual effect: a drained slot still holding its applied
/// batch's rows (every other test truncates the slot manually before
/// retiring, which never exercises `retire_one`'s own `truncate` statement)
/// must come back empty, and the registry row must be gone.
#[tokio::test]
async fn retirement_truncates_a_slot_still_holding_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    sealer
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) values ('orders', 'k', 'recompute', 0)",
            &[],
        )
        .await
        .expect("insert a row into segment 1's slot");
    seal_and_fence(&mut sealer).await; // segment 2, slot 1

    mark_drained(&sealer, 1).await;
    mark_drained(&sealer, 2).await;
    sealer
        .batch_execute("truncate seg_1")
        .await
        .expect("truncate seg_1 (segment 2 has no rows to prove anything with)");

    let row_count_before: i64 = sealer
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0 before retirement")
        .get(0);
    assert_eq!(row_count_before, 1, "segment 1's slot should hold its row");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments");
    assert_eq!(retired, vec![1]);
    assert_eq!(live_seg_seqs(&sealer).await, vec![2, 3]);

    let row_count_after: i64 = sealer
        .query_one("select count(*) from seg_0", &[])
        .await
        .expect("count seg_0 after retirement")
        .get(0);
    assert_eq!(
        row_count_after, 0,
        "retirement's own truncate must clear the slot, not just the registry row"
    );
}

/// Condition 2, in its actual blocking direction: a still-open transaction
/// keeps `pg_snapshot_xmin(pg_current_snapshot())` at or below the
/// candidate's `seal_step2`, so retirement must wait for it to settle —
/// exactly the same "hasn't finished yet" proof the seal gate itself checks,
/// just re-run at retirement time for the far slot boundary.
#[tokio::test]
async fn an_open_transaction_below_the_fence_blocks_condition_two() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0 — fenced first

    // Only now does the holder acquire its xid: after segment 1's own fence
    // is captured (so it doesn't trip the *seal gate* on segment 2, which
    // checks against segment 1's fence, not segment 2's seal_step2), but
    // before segment 2 seals (so its xid lands below segment 2's seal_step1
    // — segment 1's seal_step2 — holding xmin down against it).
    let mut holder = connect_raw(db.dsn()).await;
    let holder_txn = holder.transaction().await.expect("begin holder");
    holder_txn
        .query_one("select pg_current_xact_id()", &[])
        .await
        .expect("assign the holder an xid");

    seal_and_fence(&mut sealer).await; // segment 2, slot 1

    mark_drained(&sealer, 1).await;
    mark_drained(&sealer, 2).await;
    sealer
        .batch_execute("truncate seg_0; truncate seg_1")
        .await
        .expect("truncate seg_0 and seg_1");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments while the holder is open");
    assert!(
        retired.is_empty(),
        "segment 1 must not retire while a transaction below its fence is still open: {retired:?}"
    );

    holder_txn.commit().await.expect("release the holder");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments after the holder settles");
    assert_eq!(retired, vec![1]);
}

/// Condition 3: an **absent** successor (already retired) qualifies too —
/// retirement must not require a present-and-drained successor, which would
/// permanently strand a batch whenever its successor retired first.
#[tokio::test]
async fn an_absent_successor_qualifies_condition_three() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    seal_and_fence(&mut sealer).await; // segment 2, slot 1

    mark_drained(&sealer, 1).await;
    sealer
        .batch_execute("truncate seg_0")
        .await
        .expect("truncate seg_0");

    // Simulate segment 2 already having been retired by an earlier pass:
    // its registry row is gone (its own eligibility isn't this test's
    // concern), leaving segment 1's successor absent.
    sealer
        .execute("delete from segments where seg_seq = 2", &[])
        .await
        .expect("delete segment 2's row");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments");
    assert_eq!(retired, vec![1]);
}

/// Condition 3, the other direction: a successor that exists but isn't
/// drained blocks retirement.
#[tokio::test]
async fn a_sealed_but_not_yet_drained_successor_blocks_retirement() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    seal_and_fence(&mut sealer).await; // segment 2, slot 1 — sealed, not drained

    mark_drained(&sealer, 1).await;
    sealer
        .batch_execute("truncate seg_0")
        .await
        .expect("truncate seg_0");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments");
    assert!(
        retired.is_empty(),
        "segment 1 must not retire while segment 2 is only sealed: {retired:?}"
    );
    assert_eq!(live_seg_seqs(&sealer).await, vec![1, 2, 3]);
}

/// Condition 1/2: no fence at all (segment sealed via phase 1 only, no
/// `seal_step2`/no successor sealed) blocks retirement even if the row were
/// somehow marked drained early.
#[tokio::test]
async fn an_unclosed_fence_boundary_blocks_retirement() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    // No second seal: segment 1 has no successor, so seal_step2 is null.
    mark_drained(&sealer, 1).await;
    sealer
        .batch_execute("truncate seg_0")
        .await
        .expect("truncate seg_0");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments");
    assert!(
        retired.is_empty(),
        "a segment with no successor sealed yet (seal_step2 is null) must not retire: {retired:?}"
    );
}

/// Condition 4: one batch stuck below the boundary makes every later
/// candidate ineligible, not just its own slot — the instance-wide-stop
/// semantic.
#[tokio::test]
async fn a_stuck_older_batch_blocks_every_later_candidate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0 — left merely sealed
    seal_and_fence(&mut sealer).await; // segment 2, slot 1
    seal_and_fence(&mut sealer).await; // segment 3, slot 2

    // Segment 2 and 3 both look individually eligible (drained, drained
    // successor) — but segment 1, older than both, is stuck `sealed`.
    mark_drained(&sealer, 2).await;
    mark_drained(&sealer, 3).await;
    sealer
        .batch_execute("truncate seg_1; truncate seg_2")
        .await
        .expect("truncate seg_1 and seg_2");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments");
    assert!(
        retired.is_empty(),
        "segment 1 being stuck sealed must block segments 2 and 3 too: {retired:?}"
    );
    assert_eq!(live_seg_seqs(&sealer).await, vec![1, 2, 3, 4]);
}

/// `NOWAIT`: a held lock on the slot's table means skip-and-retry, never an
/// error and never a block.
#[tokio::test]
async fn a_locked_slot_is_skipped_not_errored() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    seal_and_fence(&mut sealer).await; // segment 2, slot 1

    mark_drained(&sealer, 1).await;
    mark_drained(&sealer, 2).await;
    sealer
        .batch_execute("truncate seg_0; truncate seg_1")
        .await
        .expect("truncate seg_0 and seg_1");

    // Another connection holds a conflicting lock on seg_0 (e.g. a
    // concurrent reader) across the retirement attempt.
    let mut locker = connect_raw(db.dsn()).await;
    let locker_txn = locker.transaction().await.expect("begin locker");
    locker_txn
        .batch_execute("lock table seg_0 in access share mode")
        .await
        .expect("lock seg_0");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments must not error on lock contention");
    assert!(
        retired.is_empty(),
        "segment 1 must be skipped while its slot is locked: {retired:?}"
    );
    assert_eq!(live_seg_seqs(&sealer).await, vec![1, 2, 3]);

    locker_txn.commit().await.expect("release the lock");

    let retired = retire::retire_drained_segments(&mut sealer)
        .await
        .expect("retire_drained_segments after the lock releases");
    assert_eq!(retired, vec![1]);
}

/// Two reclaimers racing the same candidate: exactly one truncates, the
/// other's `DELETE` matches zero rows and it rolls back without truncating.
#[tokio::test]
async fn two_concurrent_retirement_passes_never_double_retire() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    seal_and_fence(&mut sealer).await; // segment 2, slot 1

    mark_drained(&sealer, 1).await;
    mark_drained(&sealer, 2).await;
    sealer
        .batch_execute("truncate seg_0; truncate seg_1")
        .await
        .expect("truncate seg_0 and seg_1");

    let mut racer_a = connect_raw(db.dsn()).await;
    let mut racer_b = connect_raw(db.dsn()).await;

    let (result_a, result_b) = tokio::join!(
        retire::retire_drained_segments(&mut racer_a),
        retire::retire_drained_segments(&mut racer_b),
    );
    let retired_a = result_a.expect("racer a");
    let retired_b = result_b.expect("racer b");

    let total_retired = retired_a.len() + retired_b.len();
    assert_eq!(
        total_retired, 1,
        "exactly one racer should retire segment 1: a={retired_a:?} b={retired_b:?}"
    );
    assert_eq!(live_seg_seqs(&sealer).await, vec![2, 3]);
}

/// The liveness unblock: after three isolated seals wedge the ring
/// (issue #58's exact repro), a fully drained oldest segment lets the
/// stuck fourth seal succeed via `seal_if_active_nonempty`'s one retry.
#[tokio::test]
async fn ring_full_self_heals_once_the_oldest_segment_is_retirable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    seal_and_fence(&mut sealer).await; // segment 1, slot 0
    seal_and_fence(&mut sealer).await; // segment 2, slot 1
    seal_and_fence(&mut sealer).await; // segment 3, slot 2 — segment 4 now active, slot 3

    match seal::seal_phase1(&mut sealer).await {
        Err(StagingError::RingFull { ring_slot: 0 }) => {}
        other => panic!("expected RingFull{{ring_slot: 0}}, got {other:?}"),
    }

    mark_drained(&sealer, 1).await;
    mark_drained(&sealer, 2).await;
    sealer
        .batch_execute("truncate seg_0; truncate seg_1; insert into seg_3 (src_table, key, op, hop_gen) values ('orders', 'k', 'recompute', 0)")
        .await
        .expect("truncate retired slots and give segment 4 a row to seal");

    let outcome = seal::seal_if_active_nonempty(&mut sealer, "wake")
        .await
        .expect("seal_if_active_nonempty should retire segment 1 and retry")
        .expect("segment 4 is non-empty and must seal");
    assert_eq!(outcome.sealed_seg_seq, 4);
    assert_eq!(outcome.next_ring_slot, 0);
}
