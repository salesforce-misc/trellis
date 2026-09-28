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
//! # Two properties
//!
//! [`property_convergence_holds_under_the_concurrent_backend`] is the
//! original D4 tier: `trivial_program`'s small programs, applied **in the
//! exact order `Program::ops` generated them**, one at a time from one
//! harness task, with only the quiesce points changed (the burst-batching
//! knob below). Its programs never come near the engine's split threshold.
//!
//! [`property_hot_keys_converge_under_concurrent_drains`] is the hot-key
//! tier (issue #557, part 1). It draws `generate::hot_key_case`: a hot table
//! of 8 to 24 rows taking 1,000 to 2,400 writes, always read by a `GROUP BY`
//! definition, with rows moving between groups (`Mutate::MoveGroup`) over a
//! group window that slides up through the run, so groups nobody has used
//! before keep appearing and existing ones empty and refill inside a burst.
//! Each burst (500 to 1,000 ops) is split into 2 to 4 lanes
//! (`generate::concurrent_plan`) and every lane is issued from its own task
//! on its own connection ([`run_convergence_concurrent`]), so Postgres
//! commits the burst's ops in whatever order the tasks reach it. Only ops on
//! the same row, or on a table the program truncates, share a lane:
//! `concurrent_plan`'s section comment explains why that, and not
//! `ops_commute`, is the constraint. The engine runs 4 to 8 drain workers
//! and seals every 20 to 200ms, so a burst seals into several batches, many
//! of them over the split threshold, and one hot key's changes sit in several
//! batches at once.
//!
//! What the hot-key tier reaches is reported two ways when the property
//! finishes (the harness's `Drop`): the program-side shapes each case offers
//! (`Coverage::concurrent_shape_cases`: hot keys, hot groups, split-sized
//! bursts, new groups, groups that empty and refill), and what the engine
//! actually did with them, read back through triggers on its own staging
//! registry (`Coverage::drain`: batches sealed, split, and split batches
//! claimed by two or more workers). The second is the evidence that batches
//! really split across workers; the first only says the program gave the
//! engine the chance.
//!
//! [`property_mid_burst_rereads_converge_under_concurrent_drains`] is the
//! mid-burst tier (issue #557, part 2). It draws `generate::mid_burst_case`:
//! a hot-key case whose bursts also stage re-reads while CDC for the same
//! keys is in flight, instead of only at quiescent points. The harness takes
//! each action from its own task while the lanes run, once a drawn share of
//! the burst's ops has been applied (`model::TimedAction`):
//!
//! - `Trellis::request_backfill` on a table a live definition reads;
//! - `PAUSE TRANSFORM` then `RESUME TRANSFORM`, of a whole definition or of
//!   one column of a 1-1 definition (the column resume is the same call that
//!   recovers a column after its poison fuse trips);
//! - installing a `GROUP BY` definition over the hot table mid-burst, so its
//!   build overlaps CDC on its source (#625's build-under-load shape);
//! - a `TRUNCATE` of a relationship's to-side table, and to-side row updates,
//!   while the from-side hot table churns.
//!
//! The actions are named after the public API an operator calls, not the
//! mechanism behind it, because the mechanism is what epic #556 rewrites:
//! today each stages a catch-up re-read, and under ADR-0002 milestone F
//! (#625) each becomes a Re-derive. The coverage report adds per-shape
//! counts (`mid_burst_backfill`, `mid_burst_resume`,
//! `mid_burst_column_resume`, `mid_burst_install`, `to_side_truncate`,
//! `parent_update`) and per-action counts (`concurrent_actions`).
//!
//! Planted ordering bugs are #557's part 3.
//!
//! # A disk-backed cluster
//!
//! This binary's shared cluster normally lives in the system temp dir, a
//! tmpfs on the nightly box, where fsync and WAL writes cost nothing. #625's
//! build-under-load divergence reproduced 5 times in 6 on disk and never on
//! tmpfs, so commit timing matters. Set `GENERATIVE_CLUSTER_DIR` to a
//! directory on a real disk to put the cluster there (the run refuses a
//! tmpfs), and `TRELLIS_TESTKIT_PG_OPTIONS` for any server settings, e.g.
//!
//! ```text
//! GENERATIVE_CLUSTER_DIR=$PWD/target/generative-disk \
//! TRELLIS_TESTKIT_PG_OPTIONS='checkpoint_timeout=30s' \
//! cargo test -p generative --test concurrent_convergence \
//!     property_mid_burst -- --ignored
//! ```
//!
//! On disk both concurrent-tier properties find a `GROUP BY` divergence
//! within a few cases, where the tmpfs cluster converges. It needs neither
//! the mid-burst actions nor concurrency:
//! [`group_moves_converge_on_disk`] pins it, `#[ignore]`d until #625.
//!
//! # The burst-batching knob
//!
//! `run_convergence`'s loop fully quiesces after every single op, so at most
//! one row's change is ever in flight — a naive N-worker backend driven that
//! way mostly proves "N idle-ish workers don't duplicate/corrupt a single
//! claim," not that a real batch gets split and drained by several workers at
//! once (`staging::claim`'s bucket-splitting only kicks in above a
//! sealed batch's own row-count threshold). [`run_convergence_bursty`]
//! (`generative::run`) applies several already-generated ops back-to-back,
//! with no `quiesce()` in between, before checking convergence once per
//! burst — see its doc comment for the full rationale and the coarser
//! divergence localization this implies.
//!
//! The original property draws a small burst size (1-4) alongside each program:
//! `trivial_program`'s typical shape (at most `MAX_TABLES` tables, each with
//! at most `MAX_SEED_ROWS` seeds and `MAX_MUTATES` mutates —
//! `generative::generate`'s current constants keep any single table's op
//! count under a couple dozen) has no realistic path to a batch anywhere
//! near the engine's real split threshold, even fully un-quiesced in one
//! burst. Widening the generator's own ranges to reach that threshold would
//! bloat every *other* property's case count and shrink time for a benefit
//! only this one property needs — so this property's bursting is exercised
//! for its own sake (more than one row's change genuinely in flight against
//! N workers, below the split threshold). The hot-key tier above is the one
//! that draws its own, much larger shape to get past the threshold, and the
//! hand-built pin below
//! ([`a_batch_that_exceeds_the_split_threshold_converges_across_workers`])
//! drives one batch past it deterministically, which is also where the drain
//! audit the hot-key tier reports through is checked against a known split.
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

use generative::backend::{Backend, ConcurrentBackend, ManualBackend, SPLIT_THRESHOLD_ROWS};
use generative::generate::{
    ActionDraw, ActionKind, AggregateColumn, AggregateFn, ConcurrentCase, DefShape, Mutate,
    TableSpec, add_burst_actions, build_program, build_program_multi_with_shapes, concurrent_plan,
    defer_def_install, hot_key_case, mid_burst_case, schedule_restart, schedule_scale_out,
    trivial_program,
};
use generative::run::{
    RunError, run_convergence, run_convergence_bursty, run_convergence_concurrent,
};
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
    /// The hot-key property's own report (issue #557), kept apart from
    /// `coverage` so each property's numbers read on their own.
    hot_key_coverage: std::cell::RefCell<generative::run::Coverage>,
    /// The mid-burst property's own report (issue #557 part 2).
    mid_burst_coverage: std::cell::RefCell<generative::run::Coverage>,
}

impl Drop for Harness {
    /// Same rationale as `tests/convergence.rs`'s `Harness::drop`: print-only,
    /// never a pass/fail gate, and deliberately non-panicking since this can
    /// run during an unwind.
    fn drop(&mut self) {
        if self.coverage.borrow().cases > 0 {
            eprintln!(
                "generative: concurrent_convergence run coverage:\n{}",
                self.coverage.borrow()
            );
        }
        if self.hot_key_coverage.borrow().cases > 0 {
            eprintln!(
                "generative: concurrent_convergence hot-key tier coverage:\n{}",
                self.hot_key_coverage.borrow()
            );
        }
        if self.mid_burst_coverage.borrow().cases > 0 {
            eprintln!(
                "generative: concurrent_convergence mid-burst tier coverage:\n{}",
                self.mid_burst_coverage.borrow()
            );
        }
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: start_cluster(),
        coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
        hot_key_coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
        mid_burst_coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
    };
}

/// The environment variable that puts the harness's cluster on a real disk
/// (see the module doc comment's "A disk-backed cluster").
const CLUSTER_DIR_ENV: &str = "GENERATIVE_CLUSTER_DIR";

/// The harness's shared cluster: in the system temp dir, or under
/// [`CLUSTER_DIR_ENV`] when it is set. Prints which storage the run uses, and
/// refuses a [`CLUSTER_DIR_ENV`] on a tmpfs, since a tmpfs run passed off as a
/// disk run would say nothing about disk timing.
fn start_cluster() -> TestCluster {
    let Some(dir) = std::env::var_os(CLUSTER_DIR_ENV) else {
        eprintln!(
            "generative: concurrent_convergence cluster in the temp dir; set {CLUSTER_DIR_ENV} \
             for a disk-backed one"
        );
        return TestCluster::start();
    };
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).expect("create the cluster dir");
    // Postgres data on btrfs shouldn't be copy-on-write; `+C` only takes
    // effect on files created after it's set. Best effort: other filesystems
    // refuse it, harmlessly.
    let _ = std::process::Command::new("chattr")
        .arg("+C")
        .arg(&dir)
        .status();
    let fs_type = std::process::Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(&dir)
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_default();
    assert_ne!(
        fs_type,
        "tmpfs",
        "{CLUSTER_DIR_ENV}={} is a tmpfs; point it at a real disk",
        dir.display()
    );
    eprintln!(
        "generative: concurrent_convergence cluster under {} ({fs_type})",
        dir.display()
    );
    TestCluster::start_in(&dir)
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

/// Runs one concurrent-tier case (issue #557) against a fresh isolated
/// database: the case's own worker count and seal cadence, its plan's lanes
/// issued from separate tasks, its mid-burst actions taken while they run.
/// Records the case's shapes and the engine's drain audit into `coverage`.
fn run_concurrent_case(
    case: &ConcurrentCase,
    coverage: impl Fn(&Harness) -> &std::cell::RefCell<generative::run::Coverage>,
) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        let coverage = coverage(h);
        coverage.borrow_mut().record_program(&case.program);
        coverage
            .borrow_mut()
            .record_concurrent_plan(&case.program, &case.plan);

        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_options(
                db.dsn(),
                case.workers,
                Some(std::time::Duration::from_millis(case.seal_interval_ms)),
            )
            .await
            .expect("connect concurrent backend");
            // A unique slot per case, as issue #188 requires on a shared
            // cluster. The publication keeps its default name: it is
            // per-database, so it can't collide, and `request_backfill`
            // only looks for the default one.
            let unique = db.name().replace('-', "_");
            backend.set_slot_and_publication(
                format!("{unique}_slot"),
                trellis::ClientOptions::default().publication,
            );
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

            let actions: usize = case.plan.bursts.iter().map(|b| b.actions.len()).sum();
            let shape = format!(
                "{} ops, burst_size {}, {} bursts, up to {} lanes, {actions} actions, {} workers, \
                 seal every {}ms",
                case.program.ops.len(),
                case.burst_size,
                case.plan.bursts.len(),
                case.plan
                    .bursts
                    .iter()
                    .map(|b| b.lanes.len())
                    .max()
                    .unwrap_or(0),
                case.workers,
                case.seal_interval_ms,
            );
            match run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan).await {
                Ok(run) => {
                    coverage.borrow_mut().record_drain(&run.drain);
                    if run.outcome.as_pass() {
                        Ok(())
                    } else {
                        Err(TestCaseError::fail(format!(
                            "run did not pass ({shape}): {}",
                            run.outcome
                        )))
                    }
                }
                Err(RunError::Diverged(d)) => Err(TestCaseError::fail(format!(
                    "concurrent case diverged in the burst ending at op {} (target {}; {shape}) \
                     — see this file's module doc comment's shrink-trust convention before \
                     trusting this as a minimal repro:\n{}",
                    d.op_index, d.def_target, d.report
                ))),
                Err(other) => Err(TestCaseError::fail(format!(
                    "run error ({shape}): {other:?}"
                ))),
            }
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_convergence_holds_under_the_concurrent_backend(
        program in trivial_program(),
        burst_size in burst_size(),
    ) {
        run_one(&program, burst_size)?;
    }

    /// Issue #557: the hot-key tier. See the module doc comment's
    /// "The hot-key tier" section.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_hot_keys_converge_under_concurrent_drains(case in hot_key_case()) {
        run_concurrent_case(&case, |h| &h.hot_key_coverage)?;
    }

    /// Issue #557 part 2: the mid-burst tier. See the module doc comment.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_mid_burst_rereads_converge_under_concurrent_drains(case in mid_burst_case()) {
        run_concurrent_case(&case, |h| &h.mid_burst_coverage)?;
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
/// real split threshold (`staging::claim::MIN_ROWS_TO_SPLIT`, 256 as
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
    // Issue #557: the drain audit the hot-key tier reports through, checked
    // here against a batch whose split is deterministic.
    backend
        .start_drain_audit()
        .await
        .expect("start the drain audit");

    let outcome = run_convergence_bursty(&mut backend, &pool, &program, program.ops.len())
        .await
        .expect("a batch that exceeds the split threshold must still converge");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let audit = backend.drain_audit().await.expect("read the drain audit");
    assert!(
        audit.split >= 1
            && audit.max_rows_per_batch >= SPLIT_THRESHOLD_ROWS as u64
            && audit.max_workers_per_batch >= 1,
        "the drain audit must see the split batch, its row count and its claims: {audit:?}"
    );

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
/// (`staging::seal::seal_if_active_nonempty`'s straggler-catching
/// case, `staging::converge::converged_through`'s condition 3 — see
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

/// Issue #557 part 2: every kind of mid-burst action goes through the
/// concurrent runner and the backend's public-API calls while its burst's
/// lanes still write the source, and the burst still converges. The
/// property draws these actions; this pin is what exercises them in the
/// default suite, where the property doesn't run.
///
/// One hot table of 12 rows takes 240 writes, read by a `GROUP BY`
/// definition (`d0`) and a 1-1 definition (`d1`). A second `GROUP BY`
/// definition (`d2`) installs mid-burst. The second of two bursts also
/// takes a `request_backfill`, a pause and resume of `d0`, and a column
/// pause and resume of `d1`.
#[tokio::test(flavor = "multi_thread")]
async fn every_mid_burst_action_runs_while_its_burst_writes_the_source() {
    const ROWS: i64 = 12;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mutates = (0..240)
        .map(|i| {
            let pk = 1 + i % ROWS;
            if i % 3 == 0 {
                Mutate::MoveGroup {
                    pk,
                    grain: Some(i % 4),
                }
            } else {
                Mutate::Update {
                    pk,
                    c1: Some(i),
                    c2: Some(1),
                }
            }
        })
        .collect();
    let mut hot =
        TableSpec::numeric_only((1..=ROWS).map(|i| (Some(i), Some(1))).collect(), mutates);
    hot.grain_values = (1..=ROWS).map(|i| Some((i % 3).to_string())).collect();
    let program = build_program_multi_with_shapes(
        &[hot],
        &[
            (
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Count, AggregateFn::Sum(AggregateColumn::C1)],
                },
            ),
            (0, DefShape::OneToOne),
            (
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Count],
                },
            ),
        ],
    );
    assert_eq!(program.ops.len(), 252, "sanity check on the fixture shape");
    // Bursts of 126 ops: `d2` installs 24 ops into the second.
    let program = defer_def_install(program, 2, 150);
    let draw = |kind, start| ActionDraw {
        kind,
        burst: 0,
        target: 0,
        start,
        span: 300,
    };
    let plan = add_burst_actions(
        concurrent_plan(&program, 126, 2),
        &program,
        &[
            draw(ActionKind::RequestBackfill, 100),
            draw(ActionKind::PauseResume, 300),
            draw(ActionKind::ColumnPauseResume, 500),
        ],
    );
    assert!(plan.bursts[0].actions.is_empty());
    let mut names: Vec<&str> = plan.bursts[1]
        .actions
        .iter()
        .map(|a| a.action.name())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "install",
            "pause",
            "pause_column",
            "request_backfill",
            "resume",
            "resume_column"
        ]
    );

    let mut backend = ManualBackend::connect_with_options(
        db.dsn(),
        4,
        Some(std::time::Duration::from_millis(50)),
    )
    .await
    .expect("connect concurrent backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let run = run_convergence_concurrent(&mut backend, &pool, &program, &plan)
        .await
        .expect("every mid-burst action must be taken and the bursts must converge");
    assert!(run.outcome.as_pass(), "run did not pass: {}", run.outcome);
}

/// The fixture [`group_moves_converge_on_disk`] runs: one table of 12 rows
/// spread over groups `0..3`, then 1,488 updates that each move one row to
/// another group (`Mutate::MoveGroup`, nothing else), read by one `GROUP BY`
/// definition with `COUNT(*)` and `SUM`. Half the moves pick one of the 3
/// hottest rows. The target group is drawn from a window of 8 that slides up
/// 2 groups every 150 moves, so groups empty for good as well as refill. A
/// fixed-seed generator draws it all, so every run gets the same program.
fn group_moves() -> generative::model::Program {
    const ROWS: i64 = 12;
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = move |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };
    let mutates = (0..1_488)
        .map(|i| {
            let pk = 1 + if next(2) == 0 {
                next(3)
            } else {
                next(ROWS as u64)
            } as i64;
            let grain = (i / 150) as i64 * 2 + next(8) as i64;
            Mutate::MoveGroup {
                pk,
                grain: Some(grain),
            }
        })
        .collect();
    let mut table =
        TableSpec::numeric_only((1..=ROWS).map(|i| (Some(i), Some(1))).collect(), mutates);
    table.grain_values = (1..=ROWS).map(|i| Some((i % 3).to_string())).collect();
    build_program_multi_with_shapes(
        &[table],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![AggregateFn::Count, AggregateFn::Sum(AggregateColumn::C2)],
            },
        )],
    )
}

/// Found by #557 part 2's disk-backed cluster, and the shape #625's
/// build-under-load divergence reports: on a real disk, plain group moves
/// leave a `GROUP BY` target wrong for good. A group every row has left
/// keeps its target row, or a group's `COUNT(*)` is one to three too high.
/// No build overlaps the moves once the first burst has passed its check,
/// and nothing is re-read or paused. It needs no concurrency either: one
/// lane issues every op in program order and one drain worker applies
/// them, so this is the serial runtime in effect. On the tmpfs test cluster
/// the same run converges.
///
/// Runs [`group_moves`] `ATTEMPTS` times, each on a fresh database, in
/// bursts of 500 ops sealed every 85ms, and fails if any attempt diverged,
/// reporting how many did and whether each divergence was still there after
/// another 5 seconds and quiesce. Run it on disk:
///
/// ```text
/// GENERATIVE_CLUSTER_DIR=$PWD/target/generative-disk \
/// cargo test -p generative --test concurrent_convergence \
///     group_moves_converge_on_disk -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "diverges on a disk-backed cluster until #625 (#556 milestone F) fixes it; \
            run with GENERATIVE_CLUSTER_DIR set"]
async fn group_moves_converge_on_disk() {
    const ATTEMPTS: usize = 20;
    let cluster = start_cluster();
    let program = group_moves();
    let plan = concurrent_plan(&program, 500, 1);
    let mut diverged = Vec::new();
    for attempt in 1..=ATTEMPTS {
        let db = cluster.create_isolated_database().await;
        let mut backend = ManualBackend::connect_with_options(
            db.dsn(),
            1,
            Some(std::time::Duration::from_millis(85)),
        )
        .await
        .expect("connect backend");
        let unique = db.name().replace('-', "_");
        backend.set_slot_and_publication(
            format!("{unique}_slot"),
            trellis::ClientOptions::default().publication,
        );
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        match run_convergence_concurrent(&mut backend, &pool, &program, &plan).await {
            Ok(run) => assert!(run.outcome.as_pass(), "run did not pass: {}", run.outcome),
            Err(RunError::Diverged(d)) => {
                // Diagnostic only: whether the engine corrects the target
                // given more time, or it stays wrong (#625 saw the latter).
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                backend.quiesce().await.expect("quiesce again");
                let snapshot = backend.snapshot().await.expect("snapshot again");
                let later = generative::run::check_defs(&pool, &program, &program.defs, &snapshot)
                    .await
                    .expect("check again");
                // The report's first lines name the wrong rows; the rest
                // prints the whole program.
                let report = d.report.to_string();
                let rows: Vec<&str> = report
                    .lines()
                    .skip(1)
                    .take_while(|l| l.starts_with("  "))
                    .collect();
                eprintln!(
                    "attempt {attempt}: diverged in the burst ending at op {}: {} ({} 5s and \
                     another quiesce later)",
                    d.op_index,
                    rows.join(";"),
                    if later.is_some() {
                        "still wrong"
                    } else {
                        "corrected"
                    }
                );
                diverged.push(attempt);
            }
            Err(other) => panic!("attempt {attempt}: run error: {other:?}"),
        }
    }
    assert!(
        diverged.is_empty(),
        "{} of {ATTEMPTS} attempts diverged: {diverged:?}",
        diverged.len()
    );
}
