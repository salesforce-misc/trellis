//! Improvement-plan task D4 ("a second runtime"): the *concurrent* runtime —
//! the same generated [`Program`]s, the same three-way oracle
//! (`generative::oracle`), driven through the same
//! [`generative::run::run_convergence`]/[`run_convergence_bursty`] machinery
//! as `tests/convergence.rs`, but against a real multi-worker
//! [`EngineClient`] pool (`ManualBackend::connect_with_workers`) instead of
//! the single-worker default. A divergence that shows up here and *not*
//! under `tests/convergence.rs`'s single-worker runtime is, by construction,
//! a concurrency bug rather than a general correctness bug — the generator,
//! oracle, and per-op checking are all identical between the two files; only
//! the worker count (and, here, the burst-batching knob below) differ.
//!
//! [`EngineClient`]: trellis::Client
//!
//! # What this is *not*: harness-issued concurrent/out-of-order ops
//!
//! This file still applies every op **in the exact order `Program::ops`
//! generated it**, one at a time, from a single harness "thread" of control
//! (see [`run_convergence_bursty`]'s own doc comment) — it only changes
//! *when* the harness calls `quiesce()`, via the burst-batching knob below.
//! It never reorders, races, or concurrently issues ops against each other
//! from the harness side.
//!
//! A harness that actually did that — generating and issuing genuinely
//! concurrent or out-of-order op streams, rather than a serialized stream
//! against a multi-worker engine underneath — is a real, deliberately
//! **out-of-scope** follow-up. It is a substantially bigger task than this
//! file (an interleaving generator/scheduler, not a backend constructor), and
//! its natural prerequisite already exists on this branch: D2's commutation
//! machinery (`ops_commute`/`target_key_for`/`commute_groups` in
//! `generative::generate`, exercised by `tests/order_insensitivity.rs`) is
//! exactly the "which ops are even safe to reorder relative to each other"
//! analysis such a scheduler would need before it could issue anything out of
//! order. Nobody should mistake this file's worker-count/burst widening for
//! that follow-up having been done.
//!
//! # The burst-batching knob
//!
//! `run_convergence`'s loop fully quiesces after every single op, so at most
//! one row's change is ever in flight — a naive N-worker backend driven that
//! way mostly proves "N idle-ish workers don't duplicate/corrupt a single
//! claim," not that a real batch gets split and drained by several workers at
//! once (`trellis::staging::claim`'s bucket-splitting only kicks in above a
//! sealed batch's own row-count threshold). [`run_convergence_bursty`]
//! (`generative::run`) applies several already-generated ops back-to-back,
//! with no `quiesce()` in between, before checking convergence once per
//! burst — see its doc comment for the full rationale and the coarser
//! divergence localization this implies.
//!
//! The property below draws a small burst size (1-4) alongside each program:
//! `trivial_program`'s typical shape (at most `MAX_TABLES` tables, each with
//! at most `MAX_SEED_ROWS` seeds and `MAX_MUTATES` mutates —
//! `generative::generate`'s current constants keep any single table's op
//! count under a couple dozen) has no realistic path to a batch anywhere
//! near the engine's real split threshold, even fully un-quiesced in one
//! burst. Widening the generator's own ranges to reach that threshold would
//! bloat every *other* property's case count and shrink time for a benefit
//! only this one property needs — so this property's bursting is exercised
//! for its own sake (more than one row's change genuinely in flight against
//! N workers, below the split threshold) and a **separate, dedicated
//! hand-built pin** below
//! ([`a_batch_that_exceeds_the_split_threshold_converges_across_workers`])
//! is what actually drives a batch past the split threshold, deterministically
//! rather than hoping proptest's small default ranges happen to get there.
//!
//! # Shrink-trust convention
//!
//! Per the improvement plan's own caveat: **a proptest shrink under this
//! property is advisory, not a trusted minimal repro.** Two independent
//! sources of nondeterminism sit between "this program failed" and "this
//! program is a minimal concurrency bug": which of N workers actually claims
//! which bucket of a split batch, and (per the module doc comment above) that
//! a failure is only localized to a whole *burst*, not the individual op
//! within it. Shrinking a failing case can therefore converge on a program
//! that isn't actually minimal, or that doesn't even reproduce reliably on
//! its next run.
//!
//! **If this property ever fails, do not trust the shrunk `Program`/burst
//! size as-is.** Instead:
//!
//! 1. Print the failing case's [`Program`] (`{:?}`/`{:#?}`, or the printed
//!    [`generative::oracle::ThreeWayReport`] the failure message already
//!    carries) and hand-transcribe it into a `build_program`/
//!    `build_program_multi_with_shapes`-style hand-built pin, exactly like
//!    every other hand-built pin in this crate.
//! 2. Drive that pin through the plain single-worker
//!    `ManualBackend::connect` + [`run_convergence`] (burst size 1, in
//!    effect) in `tests/convergence.rs`'s style.
//! 3. If it **still** diverges under the single-worker runtime, it is a
//!    general correctness bug (unrelated to concurrency) and belongs as a
//!    pin there instead. If it only diverges under the multi-worker/bursty
//!    runtime, it is a genuine, reportable concurrency-specific bug — pin it
//!    here, driven through `ManualBackend::connect_with_workers` with burst
//!    size 1 first (isolating "more workers" from "bursting" as the cause)
//!    and then with bursting re-enabled if burst size 1 alone doesn't
//!    reproduce it.
//!
//! [`Program`]/[`generative::oracle::ThreeWayReport`] are plain, mostly-`pub`,
//! `Debug`-printable data (design doc §1) specifically so this
//! hand-transcription is mechanical — no automatic shrink-replay tooling is
//! built or needed for this.
//!
//! # Operational shape
//!
//! Same shared-cluster-per-thread, isolated-database-per-case `Harness`
//! pattern as `tests/convergence.rs` (see that file's module doc comment for
//! why); this file's `HARNESS` is its own, independent `thread_local`, so the
//! two test binaries never share a cluster or a slot/publication.

use generative::backend::ManualBackend;
use generative::generate::{
    Mutate, build_program, schedule_restart, schedule_scale_out, trivial_program,
};
use generative::run::{RunError, run_convergence, run_convergence_bursty};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::{Config, Pool};

/// How many application-worker tasks the property's `ManualBackend` runs.
/// Fixed (not drawn) and small: this property's job is proving the
/// multi-worker runtime doesn't diverge from the single-worker one on the
/// same generated programs, not sweeping worker counts — `> 1` is all that
/// matters here, and 3 is a cheap, non-trivial degree of concurrency for the
/// tiny op streams `trivial_program` draws.
const PROPERTY_WORKERS: usize = 3;

/// See the module doc comment's "burst-batching knob" section: a small,
/// lightly-randomized burst size, drawn alongside the program.
fn burst_size() -> impl Strategy<Value = usize> {
    1usize..=4usize
}

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<generative::run::Coverage>,
}

impl Drop for Harness {
    /// Same rationale as `tests/convergence.rs`'s `Harness::drop`: print-only,
    /// never a pass/fail gate, and deliberately non-panicking since this can
    /// run during an unwind.
    fn drop(&mut self) {
        eprintln!(
            "generative: concurrent_convergence run coverage:\n{}",
            self.coverage.borrow()
        );
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start(),
        coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs one generated program, bursted at `burst_size`, against a fresh
/// isolated database in the shared cluster, driven through the concurrent
/// (`PROPERTY_WORKERS`-worker) backend. See the module doc comment's
/// shrink-trust convention before treating a failure's shrunk case as a
/// minimal repro.
fn run_one(program: &generative::model::Program, burst_size: usize) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        h.coverage.borrow_mut().record_program(program);

        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_workers(db.dsn(), PROPERTY_WORKERS)
                .await
                .expect("connect concurrent backend");
            // Issue #188: unique per-case slot/publication names, not the
            // shared `ClientOptions::default()` literals — see
            // `tests/convergence.rs`'s module doc comment for the
            // shared-cluster slot-collision this avoids.
            let unique = db.name().replace('-', "_");
            backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

            match run_convergence_bursty(&mut backend, &pool, program, burst_size).await {
                Ok(outcome) => {
                    if outcome.as_pass() {
                        Ok(())
                    } else {
                        Err(TestCaseError::fail(format!("run did not pass: {outcome}")))
                    }
                }
                Err(RunError::Diverged(d)) => Err(TestCaseError::fail(format!(
                    "concurrent convergence diverged after op {} (target {}), burst_size {burst_size} \
                     — see this file's module doc comment's shrink-trust convention before trusting \
                     this as a minimal repro:\n{}",
                    d.op_index, d.def_target, d.report
                ))),
                Err(other) => Err(TestCaseError::fail(format!(
                    "run error (burst_size {burst_size}): {other:?}"
                ))),
            }
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    fn property_convergence_holds_under_the_concurrent_backend(
        program in trivial_program(),
        burst_size in burst_size(),
    ) {
        run_one(&program, burst_size)?;
    }
}

/// The concurrent runtime's own "first end-to-end green" (mirroring
/// `tests/convergence.rs`'s `a_hand_built_program_converges_end_to_end`): a
/// small, fully-worked program driven through
/// `ManualBackend::connect_with_workers` with more than one worker and a
/// burst size greater than 1, so every seeded row's insert lands before the
/// single quiesce call, and both the update and the delete land in a second
/// burst — proving the concurrent-backend/bursty-runner combination itself
/// works end-to-end on a legible, hand-built case before trusting the
/// property above to explore it.
#[tokio::test(flavor = "multi_thread")]
async fn a_hand_built_program_converges_under_the_concurrent_backend() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[
            (Some(10), Some(1)),
            (Some(20), Some(2)),
            (Some(30), Some(3)),
        ],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(100),
                c2: Some(5),
            },
            Mutate::Delete { pk: 2 },
        ],
    );

    let mut backend = ManualBackend::connect_with_workers(db.dsn(), 3)
        .await
        .expect("connect concurrent backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // Burst size 3: all three seed inserts land in one un-quiesced burst,
    // then the update and delete land in a second.
    let outcome = run_convergence_bursty(&mut backend, &pool, &program, 3)
        .await
        .expect("hand-built program must converge end-to-end under the concurrent backend");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// The non-negotiable D4 pin: a batch that **genuinely exceeds** the engine's
/// real split threshold (`trellis::staging::claim::MIN_ROWS_TO_SPLIT`, 256 as
/// of this writing — not imported here, per the backend seam's "nothing
/// outside `generative::backend` may import `trellis::staging`" rule; see
/// `ManualBackend::max_bucket_count`'s doc comment for the one sanctioned
/// door back out) gets seen, sealed, split into more than one bucket, and
/// drained correctly by more than one worker.
///
/// 1,000 single-row inserts (no updates/deletes — the simplest possible
/// shape that still stresses splitting) is a comfortable ~4x margin above
/// that threshold, so the property still holds even if the threshold moves
/// somewhat before this pin is next touched. `4` application workers is
/// `> 1` (the whole point) while comfortably below `SEG_BUCKETS` (8, also not
/// imported here for the same reason), so more than one worker has real
/// odds of actually claiming a share.
///
/// **Why `maintenance_interval` is widened for this pin specifically:** the
/// engine's maintenance loop (which performs the seal that fixes a batch's
/// `bucket_count`) ticks on its own fixed cadence (300ms by default),
/// independent of anything this harness does. If 1,000 sequential
/// raw-DML round trips happened to straddle a maintenance tick, the rows
/// would split across two (or more) smaller sealed batches instead of
/// landing in one — each individually below the split threshold, making the
/// pin flaky rather than deterministic. Widening the interval to comfortably
/// exceed how long 1,000 sequential inserts over a local connection could
/// plausibly take (generously budgeted at 10s, well inside `quiesce`'s own
/// 30s timeout) makes "all 1,000 rows land in the same sealed batch" a
/// property of the pin's construction, not a race against a fixed timer.
///
/// The burst itself is one single un-quiesced application of every op in the
/// program (`burst_size` equal to the whole op count) followed by exactly one
/// `quiesce()` — the harness never calls `quiesce()` mid-burst, so nothing
/// forces an early, partial seal from this side either.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_that_exceeds_the_split_threshold_converges_across_workers() {
    const ROW_COUNT: i64 = 1_000;
    const WORKERS: usize = 4;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let seed_values: Vec<(Option<i64>, Option<i64>)> =
        (1..=ROW_COUNT).map(|i| (Some(i), Some(i))).collect();
    let program = build_program(&seed_values, &[]);
    assert_eq!(program.ops.len(), ROW_COUNT as usize);

    let mut backend = ManualBackend::connect_with_options(
        db.dsn(),
        WORKERS,
        Some(std::time::Duration::from_secs(10)),
    )
    .await
    .expect("connect concurrent backend with widened maintenance interval");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence_bursty(&mut backend, &pool, &program, program.ops.len())
        .await
        .expect("a batch that exceeds the split threshold must still converge");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let max_bucket_count = backend
        .max_bucket_count()
        .await
        .expect("read back the sealed batch's bucket_count");
    assert!(
        max_bucket_count > 1,
        "a {ROW_COUNT}-row batch (well above the engine's real split threshold) must have been \
         partitioned into more than one bucket at seal — got max bucket_count \
         {max_bucket_count}, which means either the batch didn't land in one sealed batch as \
         intended (see this test's doc comment on `maintenance_interval`) or the engine's own \
         splitting decision regressed"
    );
}

/// Cross-cutting holistic-review pin: the phase-gap-straggler fix
/// (`trellis::staging::seal::seal_if_active_nonempty`'s straggler-catching
/// case, `trellis::staging::converge::converged_through`'s condition 3 — see
/// `generative/tests/client_lifecycle.rs`'s module doc comment for the full
/// writeup) was found and fixed entirely under the *single*-worker,
/// burst-batching runtime (`ManualBackend::connect`/`connect_with_options`
/// with `application_threads: 1`). That fix's own regression coverage
/// (`client_lifecycle.rs`, `trellis/tests/sealing.rs`, `trellis/tests/converge.rs`)
/// never drives it through this file's genuinely-concurrent, more-than-one-
/// application-worker runtime at the same time as a restart/scale-out —
/// i.e. nothing on this branch previously confirmed the fix holds when the
/// *other* source of timing perturbation (D4's real multi-worker draining)
/// is layered on top of E3's lifecycle events rather than exercised alone.
/// This pin closes that gap directly: the same restart-then-scale-out
/// sequence as `client_lifecycle.rs`'s
/// `a_restart_and_a_scale_out_interleaved_mid_stream_still_converge`
/// (restart right before a brand-new key's insert — the one shape that pin's
/// own doc comment identifies as what can land as an unfenced phase-gap
/// straggler), but against a `PROPERTY_WORKERS`-worker `ManualBackend`
/// instead of the single-worker default.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_and_a_scale_out_interleaved_still_converge_under_the_concurrent_backend() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // ops: [insert x4, update, delete] (indices 0..=5) — identical fixture
    // shape to client_lifecycle.rs's single-worker pin of the same sequence.
    let program = build_program(
        &[
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(4), Some(4)),
        ],
        &[
            Mutate::Update {
                pk: 2,
                c1: Some(99),
                c2: Some(1),
            },
            Mutate::Delete { pk: 3 },
        ],
    );
    assert_eq!(program.ops.len(), 6, "sanity check on the fixture shape");

    let program = schedule_restart(program, 2);
    let program = schedule_scale_out(program, 5);

    let mut backend = ManualBackend::connect_with_workers(db.dsn(), PROPERTY_WORKERS)
        .await
        .expect("connect concurrent backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "restart + scale-out interleaved mid-stream must converge under the concurrent \
                 backend too: {e:?}"
            )
        });
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}
