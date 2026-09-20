//! Integration tests for convergence and the read-your-writes predicate
//! (issue #12, stage 07), run against a real, ephemeral Postgres instance
//! via the shared harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/07-convergence-and-await.md for the design
//! these tests hold the implementation to. The focus throughout is
//! SOUNDNESS: every test here is meant to fail on a predicate that narrows
//! condition 3 (drained-only, band-only, buckets-not-drained), that consults
//! the summary band, or that treats a missing progress row or a NULL
//! `origin_lsn` as "not pending".
//!
//! Deliberately out of scope: the doc's "a round did no work" classification
//! (refused-seal backpressure, peer-holds-claim) is a worker-side
//! drain-to-convergence helper that depends on claiming/sweeping
//! (#14/#15), which aren't built yet — not tested here.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{StagingError, converge, seal};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `sealing.rs`/`fold.rs`'s convention.
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

async fn seed_progress(client: &Client, slot: &str, confirmed_lsn: u64) {
    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
            &[&slot, &PgLsn::from(confirmed_lsn)],
        )
        .await
        .expect("seed replication_progress");
}

async fn advance_progress(client: &Client, slot: &str, confirmed_lsn: u64) {
    client
        .execute(
            "update replication_progress set confirmed_lsn = $2 where slot_name = $1",
            &[&slot, &PgLsn::from(confirmed_lsn)],
        )
        .await
        .expect("advance replication_progress");
}

/// Inserts a bare recompute row with an explicit `origin_lsn` (recompute's
/// own type, `StagedChange::Recompute`, never carries one — this reaches
/// past the type to set it directly, the way `fold.rs`'s `RawRow` does for
/// its own column it needs to control).
async fn insert_with_origin(client: &Client, table: &str, key: &str, origin_lsn: Option<u64>) {
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen, origin_lsn) \
                 values ('orders', $1, 'recompute', 0, $2)"
            ),
            &[&key, &origin_lsn.map(PgLsn::from)],
        )
        .await
        .expect("insert recompute row with origin_lsn");
}

async fn set_segment_state(client: &Client, seg_seq: i64, state: &str) {
    client
        .execute(
            "update segments set state = $2 where seg_seq = $1",
            &[&seg_seq, &state],
        )
        .await
        .expect("set segment state");
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

#[tokio::test]
async fn never_false_converged_until_progress_active_and_sealed_all_clear() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let token = PgLsn::from(50);

    // Nothing staged at all yet, and no progress row: condition 1 alone
    // must block.
    seed_progress(&client, "slot1", 10).await;
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "confirmed_lsn (10) has not reached the token (50) yet"
    );

    // Advance progress past the token, but stage a row into the active
    // segment first — condition 2 (the active tail) must still block even
    // though condition 1 now clears.
    insert_with_origin(&client, "seg_0", "k1", Some(20)).await;
    advance_progress(&client, "slot1", 100).await;
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "the active segment still holds a row with origin_lsn (20) <= token (50)"
    );

    // Seal the active segment: the row now lives in a sealed, non-active
    // slot. Condition 3 must pick up exactly where condition 2 left off —
    // still not converged.
    let sealed_seg_seq = seal_active_segment(&mut client).await;
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "the sealed slot still holds the pending row; condition 3 must catch it"
    );

    // Only once the segment is marked drained (standing in for #14/#15's
    // apply-and-mark, not built yet) does the row stop gating.
    set_segment_state(&client, sealed_seg_seq, "drained").await;
    assert!(
        converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "progress, the (now-empty) active tail, and the drained slot should all clear"
    );
}

#[tokio::test]
async fn missing_progress_row_reads_as_not_converged() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // No replication_progress row seeded at all, and nothing staged.
    // Conditions 2/3 vacuously hold (nothing pending anywhere), but
    // condition 1 must still fail closed on the missing row.
    let converged = converge::converged_through(&client, PgLsn::from(0))
        .await
        .expect("converged_through");
    assert!(
        !converged,
        "a missing replication_progress row must never read as converged"
    );
}

/// The band would lie, the slot does not: this simulates a row that landed
/// in an already-sealed slot after the seal (a stand-in for the phase-gap
/// writer docs/staging-and-claiming/03-sealing-and-the-fence.md describes;
/// stage 03 owns actually producing that race, so this test reaches past it
/// and inserts directly). A per-batch summary band computed once at seal
/// time could not see this row; condition 3 asks the slot itself, so it
/// must still catch it.
#[tokio::test]
async fn the_band_would_lie_the_slot_does_not() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_progress(&client, "slot1", 1000).await;

    // A row with a comfortably high origin, present before the seal — if a
    // band existed, this is the only row it would have summarized.
    insert_with_origin(&client, "seg_0", "high-origin", Some(900)).await;
    seal_active_segment(&mut client).await;

    let token = PgLsn::from(50);
    assert!(
        converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "the only row in the sealed slot has origin_lsn (900) above the token (50)"
    );

    // Now a straggler lands directly in the already-sealed slot's table,
    // with an origin far below the token — the shape a band finalized at
    // seal time would have missed entirely, since it postdates the seal.
    insert_with_origin(&client, "seg_0", "straggler", Some(10)).await;
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "condition 3 asks the slot directly, so the straggler must still gate"
    );
}

/// Regression test for a real bug the generative suite's client-restart/
/// scale-out lifecycle properties found: a `'drained'` segment used to clear
/// *every* row in its slot unconditionally, including a phase-gap straggler
/// (docs/staging-and-claiming/03-sealing-and-the-fence.md, "The scoping bug
/// worth knowing about") that landed there *after* the segment's own fence
/// was captured — such a row can never become visible in that immutable
/// fence, so it is never folded into anything by *this* segment, and only
/// the immediate successor's own fenced read (once that successor itself
/// seals) can ever pick it up. If the segment's apply-and-mark nonetheless
/// reaches `'drained'` (real for a tiny batch: everything it *could* see
/// drains almost instantly) before that successor seal happens,
/// `converged_through` used to report `true` — a straggler with no target
/// write yet, silently misreported as fully converged. See
/// `generative/tests/client_lifecycle.rs` and
/// `generative/src/generate/mod.rs`'s `program_with_client_restart`/
/// `program_with_scale_out` doc comments for the full investigation
/// history, and `trellis/tests/sealing.rs`'s
/// `an_empty_active_segment_still_seals_to_catch_a_stranded_straggler` for
/// the other half of the fix (making sure that successor seal actually
/// happens).
#[tokio::test]
async fn a_drained_slots_unfenced_straggler_still_gates_convergence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_progress(&client, "slot1", 1000).await;

    // Nothing in the active segment yet; seal it as-is, capturing S_1 over
    // an empty slot.
    let sealed_seg_seq = seal_active_segment(&mut client).await;

    // A straggler lands directly in the now-sealed slot, after S_1 was
    // captured — genuinely invisible in S_1 forever, the same shape
    // `sealing.rs`'s phase-gap/straddler tests produce via real timing.
    insert_with_origin(&client, "seg_0", "stranded", Some(10)).await;

    let token = PgLsn::from(50);
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "a freshly-sealed (not yet drained) slot must still gate on its own straggler"
    );

    // Stand in for #14/#15's real apply-and-mark: segment 1 has finished
    // applying everything *it* could see and reports `'drained'` — but the
    // straggler was never visible in S_1, so nothing has actually applied
    // it yet.
    set_segment_state(&client, sealed_seg_seq, "drained").await;
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "a 'drained' owner must not clear a row it never actually saw — only a row genuinely \
         visible in its own fence stops gating"
    );

    // Once the straggler is actually resolved — the successor's own fold
    // claims it and it is retired out of the ring, exactly like any other
    // fully-processed row — nothing is left to gate on. Simulated directly
    // here (retirement's own truncate-then-delete is `retire.rs`'s concern,
    // already covered there) rather than running the full claim/apply
    // pipeline this module doesn't otherwise exercise.
    client
        .execute("truncate seg_0", &[])
        .await
        .expect("truncate seg_0");
    assert!(
        converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "once the straggler is gone (resolved and retired), the slot must stop gating"
    );
}

#[tokio::test]
async fn a_part_drained_batch_reports_its_whole_slot_pending() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_progress(&client, "slot1", 1000).await;
    insert_with_origin(&client, "seg_0", "k1", Some(10)).await;
    let sealed_seg_seq = seal_active_segment(&mut client).await;

    let token = PgLsn::from(50);

    // Draining (not drained) is over-reporting's home: some of the batch
    // may already be applied, but condition 3 is deliberately not narrowed
    // to "buckets not yet drained" — the whole slot still counts as
    // pending.
    set_segment_state(&client, sealed_seg_seq, "draining").await;
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "a draining (part-applied) segment must still report its whole slot pending"
    );

    // Once the segment actually reaches drained, it stops gating.
    set_segment_state(&client, sealed_seg_seq, "drained").await;
    assert!(
        converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "a fully drained segment must no longer gate convergence"
    );
}

#[tokio::test]
async fn zero_origin_and_null_origin_rows_gate_any_token() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    seed_progress(&client, "slot1", 1_000_000).await;

    // `origin_lsn = '0/0'` — "unknown, conservatively old" per the doc.
    insert_with_origin(&client, "seg_0", "zero-origin", Some(0)).await;
    let far_future_token = PgLsn::from(1_000_000);
    assert!(
        !converge::converged_through(&client, far_future_token)
            .await
            .expect("converged_through"),
        "a 0-origin pending row must gate any token"
    );

    // A NULL origin_lsn (what `StagedChange::Recompute` actually produces —
    // it never sets the column at all) means the same "unknown" and must
    // gate identically, not silently fall out of the WHERE filter.
    client
        .execute("truncate seg_0", &[])
        .await
        .expect("truncate seg_0");
    insert_with_origin(&client, "seg_0", "null-origin", None).await;
    assert!(
        !converge::converged_through(&client, far_future_token)
            .await
            .expect("converged_through"),
        "a NULL-origin pending row must gate any token, same as 0/0"
    );
}

#[tokio::test]
async fn has_pending_reflects_ring_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    assert!(
        !converge::has_pending(&client)
            .await
            .expect("has_pending on an empty ring"),
        "an empty ring has nothing pending"
    );

    insert_with_origin(&client, "seg_0", "k1", Some(5)).await;
    assert!(
        converge::has_pending(&client)
            .await
            .expect("has_pending with an active-tail row"),
        "a row in the active tail counts as pending"
    );

    let sealed_seg_seq = seal_active_segment(&mut client).await;
    assert!(
        converge::has_pending(&client)
            .await
            .expect("has_pending with a sealed, non-drained row"),
        "a sealed (not yet drained) slot still counts as pending"
    );

    set_segment_state(&client, sealed_seg_seq, "drained").await;
    assert!(
        !converge::has_pending(&client)
            .await
            .expect("has_pending once the only slot is drained"),
        "once every slot is drained, nothing is pending"
    );
}

#[tokio::test]
async fn pending_count_counts_every_pending_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    assert_eq!(
        converge::pending_count(&client)
            .await
            .expect("pending_count on an empty ring"),
        0
    );

    insert_with_origin(&client, "seg_0", "k1", Some(5)).await;
    insert_with_origin(&client, "seg_0", "k2", Some(6)).await;
    assert_eq!(
        converge::pending_count(&client)
            .await
            .expect("pending_count with two active-tail rows"),
        2
    );

    let sealed_seg_seq = seal_active_segment(&mut client).await;
    set_segment_state(&client, sealed_seg_seq, "drained").await;
    assert_eq!(
        converge::pending_count(&client)
            .await
            .expect("pending_count once the only slot is drained"),
        0
    );
}

#[tokio::test]
async fn await_converged_returns_once_the_predicate_flips_true() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_progress(&client, "slot1", 1000).await;
    insert_with_origin(&client, "seg_0", "k1", Some(10)).await;
    let sealed_seg_seq = seal_active_segment(&mut client).await;

    let token = PgLsn::from(50);

    // Flip convergence true on a background task, shortly after the poll
    // starts, so the poll must actually wait and retry rather than only
    // succeeding when the first check happens to pass.
    let db2 = db.dsn().to_string();
    let flip = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let flipper = connect_raw(&db2).await;
        set_segment_state(&flipper, sealed_seg_seq, "drained").await;
    });

    converge::await_converged(&client, token, Duration::from_secs(5))
        .await
        .expect("await_converged should succeed once the segment drains");
    flip.await.expect("flip task");
}

#[tokio::test]
async fn await_converged_times_out_with_a_named_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_progress(&client, "slot1", 1000).await;
    insert_with_origin(&client, "seg_0", "k1", Some(10)).await;
    seal_active_segment(&mut client).await;
    // Deliberately never mark the segment drained — convergence can never
    // be reached.

    let token = PgLsn::from(50);
    let timeout = Duration::from_millis(50);
    match converge::await_converged(&client, token, timeout).await {
        Err(StagingError::ConvergenceTimeout {
            token: got_token,
            waited,
        }) => {
            assert_eq!(got_token, token);
            assert!(waited >= timeout);
        }
        other => panic!("expected ConvergenceTimeout, got {other:?}"),
    }
}
