//! Integration tests for sealing and the fence (issue #9, stage 03), run
//! against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/03-sealing-and-the-fence.md for the design
//! these tests hold the implementation to. Every test here is meant to fail
//! on a naive implementation: a uniform (not predecessor-scoped) exclusion
//! filter, a fence captured inside the flip transaction, a missing
//! `RingFull`/seal-gate/`Raced` guard, or crash recovery without an age
//! gate.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, IsolationLevel, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{StagedChange, StagingError, TRUNCATE_SENTINEL_KEY, seal};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), for tests that
/// need a plain `tokio_postgres::Client`/`Transaction` — the type
/// `trellis::staging::seal`'s functions take, which the pooled
/// `deadpool_postgres::Client` doesn't satisfy (it has its own, unrelated
/// `GenericClient` trait).
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

async fn insert_truncate(client: &Client, table: &str) {
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) \
                 values ('orders', $1, 'truncate', 0)"
            ),
            &[&TRUNCATE_SENTINEL_KEY],
        )
        .await
        .expect("insert truncate row");
}

async fn active_pointer(client: &Client) -> (i64, i16) {
    let row = client
        .query_one("select active_seq, ring_slot from segment_pointer", &[])
        .await
        .expect("read pointer");
    (row.get(0), row.get(1))
}

/// The physical identity (`tableoid:ctid`) of the one row matching
/// `(src_table, key)`, wherever in the ring it lives. Used to check
/// "claimed exactly once" without the fold (stage 04) built yet.
async fn row_identity(client: &Client, key: &str) -> String {
    let row = client
        .query_one(
            "select tableoid::text || ':' || ctid::text from (
                 select tableoid, ctid, key from seg_0
                 union all select tableoid, ctid, key from seg_1
                 union all select tableoid, ctid, key from seg_2
                 union all select tableoid, ctid, key from seg_3
             ) rows where key = $1",
            &[&key],
        )
        .await
        .expect("locate row by key");
    row.get(0)
}

/// Asserts the row `key` appears in exactly one of `batches`' both-slots
/// fence reads — the partition-of-all-rows check the design doc's
/// "disjoint and complete" argument promises, asserted directly since the
/// fold (stage 04) isn't built yet.
async fn assert_claimed_exactly_once(client: &Client, batches: &[i64], key: &str) {
    let identity = row_identity(client, key).await;
    let mut hits = Vec::new();
    for &seg_seq in batches {
        let rows = seal::fenced_rows(client, seg_seq)
            .await
            .unwrap_or_else(|e| panic!("fenced_rows({seg_seq}) failed: {e}"));
        if rows.contains(&identity) {
            hits.push(seg_seq);
        }
    }
    assert_eq!(
        hits.len(),
        1,
        "row {key:?} should be claimed by exactly one of {batches:?}, but was claimed by {hits:?}"
    );
}

#[tokio::test]
async fn the_straddler_is_claimed_exactly_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // The straddler: begins before the seal, targets the segment being
    // sealed's own slot (seg_0, since it resolved the pointer as segment 1
    // before any flip), and stays open across the seal.
    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer.transaction().await.expect("begin writer");
    writer_txn
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) values ('orders', 'straddler', 'recompute', 0)",
            &[],
        )
        .await
        .expect("insert straddler");

    // Seal segment 1 while the straddler's transaction is still open: phase
    // 1's flip doesn't wait for it (that's the whole point of a lock-free
    // append), and phase 2's capture of S_1 happens while the straddler is
    // still in-progress, so it must be invisible in S_1.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    assert_eq!(outcome.sealed_seg_seq, 1);
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    // Now the straddler commits — landing, for good, in slot 0 (segment
    // 1's slot), after segment 1's fence was already captured.
    writer_txn.commit().await.expect("commit straddler");

    // Segment 2 (the new active segment) needs to seal too before the
    // straddler can be claimed by anyone — its predecessor half is what
    // picks the straddler up.
    let outcome2 = seal::seal_phase1(&mut sealer)
        .await
        .expect("seal phase 1 (segment 2)");
    assert_eq!(outcome2.sealed_seg_seq, 2);
    seal::seal_phase2(&sealer, outcome2.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2 (segment 2)");

    assert_claimed_exactly_once(&sealer, &[1, 2], "straddler").await;
}

#[tokio::test]
async fn the_phase_gap_writer_is_claimed_exactly_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Phase 1 for segment 1: flips the pointer to segment 2 (slot 1) and
    // commits, but nothing has captured S_1 yet.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    assert_eq!(outcome.sealed_seg_seq, 1);

    // A writer resolves the (already-flipped) pointer as segment 2 and
    // commits into the *new* slot before S_1 is captured — the window the
    // scoping bug is about. Its xid therefore ends up visible in S_1 (it's
    // already committed by the time S_1 is taken) despite living in a slot
    // S_1 never scans.
    let phase_gap_writer = connect_raw(db.dsn()).await;
    insert_recompute(&phase_gap_writer, "seg_1", "phase-gap").await;

    // Only now does phase 2 for segment 1 run.
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    // Seal segment 2 so its own fence exists to check against.
    let outcome2 = seal::seal_phase1(&mut sealer)
        .await
        .expect("seal phase 1 (segment 2)");
    seal::seal_phase2(&sealer, outcome2.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2 (segment 2)");

    // A naive implementation that applied "NOT visible in S_1" uniformly
    // (including to segment 2's *own* slot, not just its predecessor half)
    // would wrongly exclude this row from batch 2 — it's visible in S_1 —
    // while batch 1 never scans slot 1 at all. Neither batch would claim
    // it. This implementation's own-slot term has no such filter.
    assert_claimed_exactly_once(&sealer, &[1, 2], "phase-gap").await;
}

/// Regression test for a real bug the generative suite's client-restart/
/// scale-out lifecycle properties found: `seal::seal_if_active_nonempty` —
/// the entry point `client::maintenance_loop` actually calls, not the bare
/// `seal_phase1`/`seal_phase2` pair the tests above drive directly — used to
/// refuse to seal an *empty* active segment unconditionally. That is right
/// for the ordinary idle case, but wrong the moment the segment it's about
/// to become the successor of is stranding a still-unfenced phase-gap
/// straggler (same race as [`the_phase_gap_writer_is_claimed_exactly_once`]
/// above): nothing else will ever force that successor's own fence to be
/// captured if the ring goes quiet right after, so the straggler — sitting
/// in a slot whose owning segment already reports `state = 'drained'` — is
/// silently stranded forever, never folded into any target
/// (`converge::converged_through`'s condition 3 doesn't treat a `'drained'`
/// segment's slot as pending either, so this was also a false "converged").
/// See `generative/tests/client_lifecycle.rs` and
/// `generative/src/generate/mod.rs`'s `program_with_client_restart`/
/// `program_with_scale_out` doc comments for the full investigation history:
/// both a client restart and a scale-out were found to reproduce this, not
/// because either is special, but because both perturb timing enough to
/// make this always-latent race common.
#[tokio::test]
async fn an_empty_active_segment_still_seals_to_catch_a_stranded_straggler() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Segment 1's own fence, S_1, is captured while nothing has yet
    // committed into its slot beyond genesis — an ordinary, uneventful seal.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    assert_eq!(outcome.sealed_seg_seq, 1);
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    // A straddler resolves the pointer as segment 1 (reading it before the
    // flip above) and only commits into slot 0 now, after S_1 was already
    // captured — invisible in S_1 forever, by construction.
    let straddler = connect_raw(db.dsn()).await;
    insert_recompute(&straddler, "seg_0", "stranded").await;

    // Stand in for #14/#15's real apply-and-mark: everything segment 1
    // *could* see has already drained, so it reports `'drained'` — exactly
    // like the real bug, where a tiny batch drains almost instantly. The
    // straddler landed in its slot regardless, and segment 1's own fence
    // (already published) can never be made to include it.
    sealer
        .execute(
            "update segments set state = 'drained' where seg_seq = $1",
            &[&outcome.sealed_seg_seq],
        )
        .await
        .expect("mark segment 1 drained");

    // The active segment (2) is genuinely empty — nothing else has happened
    // since the flip. The old, unconditional "only seal if non-empty" guard
    // would return `Ok(None)` here and never look back, stranding the
    // straddler for good.
    let sealed = seal::seal_if_active_nonempty(&mut sealer, "wake")
        .await
        .expect("seal_if_active_nonempty");
    assert!(
        sealed.is_some(),
        "an empty active segment must still seal when its predecessor is stranding an unfenced \
         straggler — otherwise nothing ever captures the fence that would catch it"
    );
    let outcome2 = sealed.expect("checked above");
    assert_eq!(outcome2.sealed_seg_seq, 2);

    // `seal_if_active_nonempty` runs phase 2 itself (unlike bare
    // `seal_phase1`), so segment 2's fence already exists — the straggler is
    // claimable right away via the both-slots read.
    assert_claimed_exactly_once(&sealer, &[1, 2], "stranded").await;
}

/// The companion negative control: an empty active segment whose
/// predecessor has nothing unfenced left in its slot (the ordinary idle-tail
/// case — every real system reaches this state at the end of any burst of
/// traffic) must *not* seal. Without this, [`an_empty_active_segment_still_seals_to_catch_a_stranded_straggler`]'s
/// fix could regress into resealing empty segments forever on a genuinely
/// idle ring — the exact busy-loop
/// docs/staging-and-claiming/03-sealing-and-the-fence.md's "Who seals, and
/// when" section calls out as the reason seal-on-demand only fires for a
/// non-empty active segment in the first place.
#[tokio::test]
async fn an_empty_active_segment_with_a_fully_fenced_predecessor_does_not_seal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // An entirely ordinary row, present before the seal — genuinely visible
    // in segment 1's own fence, not a straggler at all.
    insert_recompute(&sealer, "seg_0", "ordinary").await;
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    sealer
        .execute(
            "update segments set state = 'drained' where seg_seq = $1",
            &[&outcome.sealed_seg_seq],
        )
        .await
        .expect("mark segment 1 drained");

    // The active segment (2) is empty, and segment 1 has nothing unfenced
    // left to catch — sealing now would be pure, unbounded busy-work.
    let sealed = seal::seal_if_active_nonempty(&mut sealer, "wake")
        .await
        .expect("seal_if_active_nonempty");
    assert!(
        sealed.is_none(),
        "an empty active segment with a fully-fenced predecessor must not seal — nothing to \
         catch, and sealing anyway would busy-loop an idle ring forever"
    );
}

#[tokio::test]
async fn the_high_xid_writer_is_claimed_exactly_once_via_the_xmax_fix() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Phase 1 for segment 1 flips the pointer to segment 2 (slot 1) and
    // commits, assigning its own xid — call it P1 — before anything else in
    // this test has touched the database.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    assert_eq!(outcome.sealed_seg_seq, 1);

    // Only *now* does the writer acquire an xid, by resolving the
    // already-stale pointer value (slot 0, segment 1's slot) and writing
    // into it. Its xid is therefore guaranteed to be > P1 — the writer is
    // the highest xid in the system, and it stays open. This is the precise
    // condition the xmax trap needs: if phase 1's own commit already sat
    // above the writer (as it would if the writer wrote *before* phase 1),
    // that commit would incidentally advance `latestCompletedXid` past the
    // writer for free, masking the bug this test exists to catch.
    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer.transaction().await.expect("begin writer");
    writer_txn
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) values ('orders', 'xmax-case', 'recompute', 0)",
            &[],
        )
        .await
        .expect("insert xmax-case row");

    // Phase 2 captures S_1 while the writer — with the highest xid in the
    // system — is still open. Without the autocommit `pg_current_xact_id()`
    // fix, `xmax(S_1)` sits at (at most) the writer's own xid: the trap.
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    // The seal gate must now block segment 2's seal: the writer targeting
    // segment 1's slot hasn't settled. Without the xmax fix, xmax(S_1)
    // would already be at-or-below the writer's own xid, so the gate would
    // (wrongly) already be open here.
    match seal::seal_phase1(&mut sealer).await {
        Err(StagingError::SealGateBlocked) => {}
        other => panic!("expected the seal gate to block on the still-open writer, got {other:?}"),
    }

    writer_txn.commit().await.expect("commit writer");

    // The gate opens once the writer settles.
    let outcome2 = seal::seal_phase1(&mut sealer)
        .await
        .expect("seal phase 1 (segment 2) after the writer settles");
    seal::seal_phase2(&sealer, outcome2.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2 (segment 2)");

    assert_claimed_exactly_once(&sealer, &[1, 2], "xmax-case").await;
}

/// A `Recompute` for `key`, the plainest [`trellis::staging::StagedChange`]:
/// the tests below append through the real `trellis::staging::append`, so
/// they exercise the writer's own pointer read rather than naming a slot.
fn recompute(key: &str) -> StagedChange {
    StagedChange::Recompute {
        src_table: "orders".to_string(),
        key: key.to_string(),
        hop_gen: 0,
        group_key: None,
        src_changed: None,
        prior_image: None,
        origin_lsn: None,
    }
}

async fn seal_both_phases(sealer: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(sealer).await.expect("seal phase 1");
    seal::seal_phase2(sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Issue #595: a writer at a snapshot isolation level takes its snapshot
/// with a first `SELECT`, two seals flip the pointer past it, and only then
/// does it append. Read from `segment_pointer`, the active slot comes out of
/// its snapshot — segment 1's slot, two flips stale — and the row lands in
/// `seg_0` with an xid above both `xmax(S_1)` and `xmax(S_2)`: batch 1 can't
/// see it, batch 2's predecessor half reads `seg_1` not `seg_0`, and no
/// later batch reads `seg_0` at all. The writer must read the slot from the
/// snapshot-independent mirror instead, so it lands in the live slot.
async fn a_snapshot_isolation_writer_is_claimed_exactly_once(level: IsolationLevel) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer
        .build_transaction()
        .isolation_level(level)
        .start()
        .await
        .expect("begin writer");
    // The first statement fixes the transaction's snapshot: slot 0 active.
    let seen: i16 = writer_txn
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("take the writer's snapshot")
        .get(0);
    assert_eq!(seen, 0);

    assert_eq!(seal_both_phases(&mut sealer).await, 1);
    assert_eq!(seal_both_phases(&mut sealer).await, 2);

    trellis::staging::append(&writer_txn, &[recompute("snapshot-writer")])
        .await
        .expect("append from the snapshot-isolation writer");
    writer_txn.commit().await.expect("commit writer");

    assert_eq!(seal_both_phases(&mut sealer).await, 3);

    assert_claimed_exactly_once(&sealer, &[1, 2, 3], "snapshot-writer").await;
}

#[tokio::test]
async fn a_repeatable_read_writer_that_took_its_snapshot_before_a_seal_is_claimed_exactly_once() {
    a_snapshot_isolation_writer_is_claimed_exactly_once(IsolationLevel::RepeatableRead).await;
}

#[tokio::test]
async fn a_serializable_writer_that_took_its_snapshot_before_a_seal_is_claimed_exactly_once() {
    a_snapshot_isolation_writer_is_claimed_exactly_once(IsolationLevel::Serializable).await;
}

/// Which ring table (`seg_0`..`seg_3`) holds the one row keyed `key`.
async fn ring_table_of(client: &Client, key: &str) -> String {
    client
        .query_one(
            "select tableoid::regclass::text from (
                 select tableoid, key from seg_0
                 union all select tableoid, key from seg_1
                 union all select tableoid, key from seg_2
                 union all select tableoid, key from seg_3
             ) rows where key = $1",
            &[&key],
        )
        .await
        .expect("locate row's ring table")
        .get(0)
}

/// Appends `key` through the real `trellis::staging::append` in its own
/// committed transaction.
async fn append_committed(client: &mut Client, key: &str) {
    let txn = client.transaction().await.expect("begin append");
    trellis::staging::append(&txn, &[recompute(key)])
        .await
        .expect("append");
    txn.commit().await.expect("commit append");
}

/// Issue #595's crash case: the sealer dies after phase 1's flip commits and
/// before phase 2 moves the mirror, so writers keep resolving the sealed
/// slot. Recovery's reconstruction runs through `seal_phase2`, which sets
/// the mirror before its xid bump, so a writer still open in the sealed slot
/// has an xid below `xmax(S_1)`: in `S_1`'s in-progress list, holding the
/// gate, and claimed by batch 2. Writers after recovery land in the live
/// slot.
#[tokio::test]
async fn a_crash_between_the_flip_and_the_mirror_set_is_recovered_and_the_writer_is_claimed_exactly_once()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Phase 1 only: the pointer names slot 1, the mirror still slot 0.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    assert_eq!(outcome.sealed_seg_seq, 1);
    assert_eq!(active_pointer(&sealer).await, (2, 1));

    // A writer appends through the real pointer read and stays open across
    // the recovery: it lands in the sealed slot.
    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer.transaction().await.expect("begin writer");
    trellis::staging::append(&writer_txn, &[recompute("crash-window")])
        .await
        .expect("append into the crash window");
    let landed: String = writer_txn
        .query_one(
            "select tableoid::regclass::text from seg_0 where key = 'crash-window'",
            &[],
        )
        .await
        .expect("the crash-window row is in seg_0")
        .get(0);
    assert_eq!(landed, "seg_0");

    // Same age-gate wait as `age_gated_recovery_unwedges_a_crashed_seal_...`.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let tight = seal::SealConfig {
        age_gate: Duration::from_millis(5),
    };
    let recovered = seal::recover_stuck_seals(&sealer, &tight, "wake")
        .await
        .expect("recover_stuck_seals");
    assert_eq!(recovered, vec![outcome.sealed_seg_seq]);

    // Recovery moved the mirror: new writers target the live slot.
    append_committed(&mut sealer, "after-recovery").await;
    assert_eq!(ring_table_of(&sealer, "after-recovery").await, "seg_1");

    // The crash-window writer is in S_1's in-progress list, so it holds the
    // gate until it settles.
    match seal::seal_phase1(&mut sealer).await {
        Err(StagingError::SealGateBlocked) => {}
        other => {
            panic!("expected the seal gate to block on the crash-window writer, got {other:?}")
        }
    }
    writer_txn.commit().await.expect("commit writer");

    assert_eq!(seal_both_phases(&mut sealer).await, 2);
    assert_claimed_exactly_once(&sealer, &[1, 2], "crash-window").await;
    assert_claimed_exactly_once(&sealer, &[1, 2], "after-recovery").await;
}

/// The guard on phase 2's mirror set: a phase 2 that stalls past the
/// recovery age gate runs after recovery published `S_1` and after segment 2
/// sealed and moved the mirror to slot 2. Setting the mirror back to slot 1
/// would send writers into a sealed slot with xids above `xmax(S_2)`, which
/// is issue #595's loss again. The stalled call must leave the mirror alone.
#[tokio::test]
async fn a_phase_two_that_stalls_past_recovery_does_not_move_the_mirror_backwards() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let tight = seal::SealConfig {
        age_gate: Duration::from_millis(5),
    };
    let recovered = seal::recover_stuck_seals(&sealer, &tight, "wake")
        .await
        .expect("recover_stuck_seals");
    assert_eq!(recovered, vec![outcome.sealed_seg_seq]);
    assert_eq!(seal_both_phases(&mut sealer).await, 2);

    // Segment 1's original phase 2 finally runs.
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("stalled phase 2");

    append_committed(&mut sealer, "after-stalled-phase-2").await;
    assert_eq!(
        ring_table_of(&sealer, "after-stalled-phase-2").await,
        "seg_2"
    );
}

/// The other half of the mirror argument: "a writer that read the old slot
/// did so before the mirror set, hence has an xid below the bump" needs the
/// writer's xid to exist when it reads. `active_ring_slot` assigns it in the
/// same statement. A writer that resolves slot 0, then sees segment 1 seal
/// around it before its insert, must be in `S_1`'s in-progress list and
/// hold the gate; without the xid it would be absent from `S_1` altogether,
/// segment 2 could seal past it, and its row would land in `seg_0` behind
/// both fences.
#[tokio::test]
async fn a_writer_that_resolved_its_slot_before_a_seal_holds_the_seal_gate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer.transaction().await.expect("begin writer");
    let slot = trellis::staging::active_ring_slot(&writer_txn)
        .await
        .expect("resolve the active slot");
    assert_eq!(slot, 0);

    assert_eq!(seal_both_phases(&mut sealer).await, 1);

    match seal::seal_phase1(&mut sealer).await {
        Err(StagingError::SealGateBlocked) => {}
        other => panic!(
            "expected the seal gate to block on a writer that resolved slot 0 before the seal, \
             got {other:?}"
        ),
    }

    writer_txn
        .execute(
            &format!(
                "insert into seg_{slot} (src_table, key, op, hop_gen) \
                 values ('orders', 'resolved-early', 'recompute', 0)"
            ),
            &[],
        )
        .await
        .expect("insert into the resolved slot");
    writer_txn.commit().await.expect("commit writer");

    assert_eq!(seal_both_phases(&mut sealer).await, 2);
    assert_claimed_exactly_once(&sealer, &[1, 2], "resolved-early").await;
}

#[tokio::test]
async fn seal_gate_blocks_until_the_straddler_settles() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    let mut writer = connect_raw(db.dsn()).await;
    let writer_txn = writer.transaction().await.expect("begin writer");
    writer_txn
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) values ('orders', 'gated', 'recompute', 0)",
            &[],
        )
        .await
        .expect("insert gated row");

    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    for _ in 0..3 {
        match seal::seal_phase1(&mut sealer).await {
            Err(StagingError::SealGateBlocked) => {}
            other => panic!("expected SealGateBlocked while the writer is open, got {other:?}"),
        }
    }

    writer_txn.commit().await.expect("commit writer");

    seal::seal_phase1(&mut sealer)
        .await
        .expect("the gate must open once the writer settles");
}

#[tokio::test]
async fn seal_gate_blocks_when_the_predecessor_fence_is_not_yet_published() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Seal segment 1 via phase 1 only: state = 'sealed', seal_step1 set,
    // fence_snapshot still NULL. Deliberately skip phase 2.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    assert_eq!(outcome.sealed_seg_seq, 1);

    // Sealing the new active segment (2) needs to check the gate against
    // segment 1's fence — which doesn't exist yet. A predecessor *row* is
    // there, just with no fence to prove settlement against, so this must
    // block rather than silently proceed (proceeding here would let an
    // in-flight writer straddling segment 1's slot land in neither batch).
    match seal::seal_phase1(&mut sealer).await {
        Err(StagingError::SealGateBlocked) => {}
        other => panic!(
            "expected the seal gate to block on segment 1's unpublished fence, got {other:?}"
        ),
    }

    // Publishing S_1 self-heals the wedge: the same seal now goes through.
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    let outcome2 = seal::seal_phase1(&mut sealer)
        .await
        .expect("the gate must open once the predecessor's fence is published");
    assert_eq!(outcome2.sealed_seg_seq, 2);
}

#[tokio::test]
async fn a_raced_seal_backs_off_instead_of_double_sealing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut client_a = connect_raw(db.dsn()).await;
    let mut client_b = connect_raw(db.dsn()).await;

    // Two idle workers both plan a seal of the same active segment
    // concurrently. Exactly one must win.
    let (result_a, result_b) = tokio::join!(
        seal::seal_phase1(&mut client_a),
        seal::seal_phase1(&mut client_b),
    );

    let outcomes = [result_a, result_b];
    let wins = outcomes.iter().filter(|r| r.is_ok()).count();
    let races = outcomes
        .iter()
        .filter(|r| matches!(r, Err(StagingError::Raced)))
        .count();
    assert_eq!(
        wins, 1,
        "exactly one racer should win the seal: {outcomes:?}"
    );
    assert_eq!(
        races, 1,
        "exactly one racer should back off with Raced: {outcomes:?}"
    );
}

#[tokio::test]
async fn ring_full_runs_retirement_then_retries_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Walk the ring around: seal segments 1, 2, 3 so slots 0, 1, 2 all hold
    // a live registry row, leaving segment 4 active in slot 3.
    for _ in 0..3 {
        let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
        seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
    }
    let (active_seq, ring_slot) = active_pointer(&sealer).await;
    assert_eq!((active_seq, ring_slot), (4, 3));

    // Sealing segment 4 would need slot 0 — still occupied by segment 1's
    // registry row — so it must fail with RingFull, not lap it.
    match seal::seal_phase1(&mut sealer).await {
        Err(StagingError::RingFull { ring_slot: 0 }) => {}
        other => panic!("expected RingFull{{ring_slot: 0}}, got {other:?}"),
    }

    // Simulate stage 05's apply ∪ mark-drained having fully drained segments
    // 1 and 2 (issue #11 isn't wired into this test's own drain path, so
    // this stands in for it): both flip to 'drained' and their slots empty
    // out. Segment 2 must be drained too — retirement's condition 3 needs
    // segment 1's *successor* to be drained, not merely segment 1 itself.
    sealer
        .execute(
            "update segments set state = 'drained' where seg_seq in (1, 2)",
            &[],
        )
        .await
        .expect("mark segments 1 and 2 drained");
    sealer
        .batch_execute("truncate seg_0; truncate seg_1")
        .await
        .expect("truncate seg_0 and seg_1");

    let outcome = seal::seal_if_active_nonempty(&mut sealer, "wake")
        .await
        .expect("seal_if_active_nonempty after marking segment 1 retirable");
    // The active segment (4) is empty, so seal_if_active_nonempty alone
    // wouldn't seal it; insert a row first so there's something to seal.
    assert!(
        outcome.is_none(),
        "segment 4 is empty; seal-on-demand must not seal an empty active segment"
    );

    insert_recompute(&sealer, "seg_3", "ring-full-probe").await;
    let outcome = seal::seal_if_active_nonempty(&mut sealer, "wake")
        .await
        .expect("seal_if_active_nonempty should retry once after retirement frees slot 0")
        .expect("segment 4 is non-empty and must seal");
    assert_eq!(outcome.sealed_seg_seq, 4);
    assert_eq!(
        outcome.next_ring_slot, 0,
        "slot 0 must be free again after retirement"
    );

    // Segment 1 (fully retired) no longer has a registry row; segment 2
    // stays behind (its own successor, segment 3, isn't drained yet).
    let remaining: Vec<i64> = sealer
        .query("select seg_seq from segments order by seg_seq", &[])
        .await
        .expect("read segments")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(remaining, vec![2, 3, 4, 5]);
}

#[tokio::test]
async fn a_sealed_but_unfenced_segment_fails_loud_rather_than_guessing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Phase 1 only: leaves state = 'sealed', seal_step1 set, fence_snapshot
    // NULL — the crash window.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");

    match seal::fenced_rows(&sealer, outcome.sealed_seg_seq).await {
        Err(StagingError::UnfencedSealedSegment { seg_seq }) => {
            assert_eq!(seg_seq, outcome.sealed_seg_seq);
        }
        other => panic!("expected UnfencedSealedSegment, got {other:?}"),
    }
}

#[tokio::test]
async fn age_gated_recovery_unwedges_a_crashed_seal_without_stomping_a_fresh_one() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Segment 1: phase 1 only, simulating a crash between phase 1 and
    // phase 2. Left behind: state = 'sealed', seal_step1 set,
    // fence_snapshot NULL.
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");

    // A generous age gate must not touch a seal that crashed only moments
    // ago — this is the "never stomps a healthy in-flight seal's
    // sub-millisecond phase gap" guarantee.
    let generous = seal::SealConfig {
        age_gate: Duration::from_secs(600),
    };
    let recovered = seal::recover_stuck_seals(&sealer, &generous, "wake")
        .await
        .expect("recover_stuck_seals (generous gate)");
    assert!(
        recovered.is_empty(),
        "a generous age gate must not recover a seal that only just wedged"
    );
    match seal::fenced_rows(&sealer, outcome.sealed_seg_seq).await {
        Err(StagingError::UnfencedSealedSegment { .. }) => {}
        other => panic!("expected the segment to still be unfenced, got {other:?}"),
    }

    // Once the wedge has actually aged past a tight gate, recovery
    // reconstructs the fence.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let tight = seal::SealConfig {
        age_gate: Duration::from_millis(5),
    };
    let recovered = seal::recover_stuck_seals(&sealer, &tight, "wake")
        .await
        .expect("recover_stuck_seals (tight gate)");
    assert_eq!(recovered, vec![outcome.sealed_seg_seq]);

    // The segment is now claimable: fenced_rows no longer fails loud.
    seal::fenced_rows(&sealer, outcome.sealed_seg_seq)
        .await
        .expect("fenced_rows should succeed once the fence is reconstructed");

    // Recovery is idempotent / scoped to the still-incomplete state: a
    // second pass matches zero rows rather than double-writing the fence.
    let recovered_again = seal::recover_stuck_seals(&sealer, &tight, "wake")
        .await
        .expect("recover_stuck_seals (second pass)");
    assert!(
        recovered_again.is_empty(),
        "a segment that already has a fence must not be reported as recovered again"
    );
}

/// The truncate barrier's seal-time half (issue #60): a batch containing a
/// truncate sentinel must seal single-bucket regardless of its row count —
/// the whole-keyspace clear and any same-batch post-truncate writes need to
/// run as one atomic Phase-3 transaction, never split across workers — and
/// `segments.has_truncate` must be set so `apply::next_claimable_segment`
/// can find it. A batch with no truncate at all, well past the split
/// threshold, is sealed alongside it in the same test as the negative case.
#[tokio::test]
async fn a_truncate_bearing_batch_seals_single_bucket_with_has_truncate_set() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut sealer = connect_raw(db.dsn()).await;

    // Enough rows to clear MIN_ROWS_TO_SPLIT on its own — proving
    // `has_truncate` overrides the row-count-driven split, not merely
    // coincides with a small batch.
    for i in 0..2000 {
        insert_recompute(&sealer, "seg_0", &format!("row-{i}")).await;
    }
    insert_truncate(&sealer, "seg_0").await;

    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    let row = sealer
        .query_one(
            "select bucket_count, has_truncate from segments where seg_seq = $1",
            &[&outcome.sealed_seg_seq],
        )
        .await
        .expect("read segment row");
    let bucket_count: i16 = row.get(0);
    let has_truncate: bool = row.get(1);
    assert_eq!(
        bucket_count, 1,
        "a truncate-bearing batch must seal single-bucket even past the split threshold"
    );
    assert!(has_truncate, "has_truncate must be set at seal time");

    // A second, ordinary batch (well past the split threshold, no truncate)
    // seals normally — has_truncate must not spill over from the first.
    for i in 0..2000 {
        insert_recompute(&sealer, "seg_1", &format!("row2-{i}")).await;
    }
    let outcome2 = seal::seal_phase1(&mut sealer)
        .await
        .expect("seal phase 1 (segment 2)");
    seal::seal_phase2(&sealer, outcome2.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2 (segment 2)");

    let row2 = sealer
        .query_one(
            "select bucket_count, has_truncate from segments where seg_seq = $1",
            &[&outcome2.sealed_seg_seq],
        )
        .await
        .expect("read segment row");
    let bucket_count2: i16 = row2.get(0);
    let has_truncate2: bool = row2.get(1);
    assert!(
        bucket_count2 > 1,
        "a truncate-free batch past the split threshold must still split"
    );
    assert!(
        !has_truncate2,
        "has_truncate must not spill over from an earlier segment"
    );
}

/// Issue #271: a seal actually completing is the one transition that makes
/// a segment claimable, so `seal_phase2` now `pg_notify`s `wake_channel` the
/// instant it publishes the fence. Mirrors
/// `intake_core.rs`'s `notify_is_only_delivered_after_staged_rows_are_visible`
/// — the same "notify must not precede the fact it announces" property,
/// proven the same way: a dedicated `LISTEN` connection must never observe
/// the notification before a completely separate connection can already see
/// the fence it announces. `seal_phase2`'s own doc comment explains why this
/// holds here even though, unlike `intake::advance_watermark_and_notify`,
/// there is no explicit wrapping `Transaction` to point to: the `update` and
/// the `pg_notify` are one statement, so Postgres's own single-statement
/// implicit transaction is what makes the two atomic.
#[tokio::test]
async fn seal_notify_is_only_delivered_after_the_fence_is_visible() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // A dedicated LISTEN connection, driven on its own task so its
    // `Connection` future keeps polling for asynchronous notifications
    // rather than being spawned-and-ignored like `connect_raw`.
    let (listener, mut connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect listener");

    // Drive `connection` *before* issuing anything on `listener`: nothing is
    // flushed or read back until something polls `connection`, so issuing
    // `batch_execute` first would hang forever.
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
                    let _ = notify_tx.send(());
                }
                Some(Ok(_)) => continue,
                _ => break,
            }
        }
    });

    listener
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'; listen wake"
        ))
        .await
        .expect("listen");

    let mut sealer = connect_raw(db.dsn()).await;
    insert_recompute(&sealer, "seg_0", "k1").await;
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");

    tokio::time::timeout(Duration::from_secs(5), notify_rx.recv())
        .await
        .expect("must receive the notification")
        .expect("channel must not have closed");

    // By the time the listener sees the notification, the fence is already
    // published and visible on a completely separate connection — no
    // listener wakes to a segment it can't yet actually fold or claim.
    let observer = connect_raw(db.dsn()).await;
    let fence_is_published: bool = observer
        .query_one(
            "select fence_snapshot is not null from segments where seg_seq = $1",
            &[&outcome.sealed_seg_seq],
        )
        .await
        .expect("read segment fence")
        .get(0);
    assert!(
        fence_is_published,
        "the fence must already be published by the time the notify is observed"
    );
}

/// The other half of `seal_phase2`'s new notify discipline: a raced/duplicate
/// call against an already-fenced segment (`seal_phase2`'s own doc comment:
/// "a raced call ... matches zero rows — a benign no-op, not an error") must
/// not send a second notify. Without the `with published as (...) select
/// pg_notify(...) from published` conditioning, an unconditional `pg_notify`
/// after the (no-op) `update` would fire every time, including from
/// `recover_stuck_seals` re-observing a segment a normal seal already
/// finished — spurious wakes this issue exists to eliminate, not add.
#[tokio::test]
async fn seal_notify_does_not_fire_on_a_raced_no_op_seal_phase2() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (listener, mut connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect listener");
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
                    let _ = notify_tx.send(());
                }
                Some(Ok(_)) => continue,
                _ => break,
            }
        }
    });
    listener
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'; listen wake"
        ))
        .await
        .expect("listen");

    let mut sealer = connect_raw(db.dsn()).await;
    insert_recompute(&sealer, "seg_0", "k1").await;
    let outcome = seal::seal_phase1(&mut sealer).await.expect("seal phase 1");
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2 (first, real, publish)");

    // Drain the one notify the real publish above sent.
    tokio::time::timeout(Duration::from_secs(5), notify_rx.recv())
        .await
        .expect("must receive the first notification")
        .expect("channel must not have closed");

    // A second call against the same already-fenced segment is the benign
    // no-op described above: its `with` clause matches zero rows, so
    // `pg_notify` never evaluates.
    seal::seal_phase2(&sealer, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2 (second, no-op)");

    assert!(
        tokio::time::timeout(Duration::from_millis(500), notify_rx.recv())
            .await
            .is_err(),
        "a no-op seal_phase2 call must not send a second notify"
    );
}
