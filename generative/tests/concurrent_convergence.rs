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
//! # The properties
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
//! - a `TRUNCATE` of the case's second table while the hot table churns.
//!
//! The actions are named after the public API an operator calls, not the
//! mechanism behind it. The coverage report adds per-shape counts (`mid_burst_backfill`, `mid_burst_resume`,
//! `mid_burst_column_resume`, `mid_burst_install`) and per-action counts
//! (`concurrent_actions`).
//!
//! [`property_cooling_keys_converge_under_concurrent_drains`] is the
//! cooling-key tier (issue #557 part 3b). It draws
//! `generate::cooling_key_case`: a hot-key case plus a cooling table of 150
//! to 400 keys read by a 1-1 definition. Each cooling key is inserted and
//! written one to three more times close together, then never again, with
//! its last write in the middle of a burst while the hot table keeps
//! taking writes (`generate::place_cooling_keys`). The engine runs 3 to 6
//! drain workers and seals every 5 to 20ms, several times per burst, so a
//! key's writes seal into different batches that drain side by side. It is
//! the shape where a 1-1 write applied out of order survives to the end of
//! the burst, because no later change to the key heals it (#344), which
//! the hot-key tier's keys, busy until the burst ends, almost never offer.
//! The coverage report counts it as `cooling_key`.
//!
//! The steady-load tier (#720, #725) draws `generate::steady_load_case`: a
//! mid-burst case run under a steady load
//! (`model::SteadyLoad`). Once an action starts a build, each lane waits a
//! drawn 0.5 to 2ms after every op for the rest of the burst, so its writes
//! trickle in while the build runs; and the engine's pages and build chunks
//! stall now and then at their entry-lock step, for up to a few seal
//! intervals (`trellis::dev::interleave::set_stall`). Its builds plan chunks
//! of 1 to 4 rows and start at a reconcile pass on the seal cadence rather
//! than the engine's 5s default. Flat out, a burst is written before the
//! build it starts runs a chunk, and the drain keeps up with every seal, so
//! races that need a page held mid-step never get a window: a build chunk
//! reading an entry a page has locked but not yet written
//! (`chunk_without_entry_lock`), and a segment draining past an older one
//! whose page hasn't taken its entry lock yet, so that a tombstone the older
//! change needs can be collected (`early_tombstone_gc`).
//!
//! The cooling-backfill tier (#734) draws `generate::cooling_backfill_case`: a
//! cooling-key case whose cooling table a `GROUP BY` definition reads rather
//! than a 1-1 one, with a `request_backfill` of that table in every burst
//! after the first, at 25 to 65% of its ops, among the cooling keys' last
//! writes. It runs under the steady-load tier's stall, with the lanes paced
//! from each burst's start (`SteadyLoad::pace_from_start`), so the
//! backfill's rebuild runs among writes still coming in. It is a sweep tier
//! only, like the steady-load one, and it gates the plants a backfill under
//! load reaches.
//!
//! The stall is process-wide, so neither of these two tiers has a property
//! test: they run only in [`planted_bugs_are_caught`]'s sweep processes, one
//! case at a time with nothing else in the process, and `run_concurrent_case`
//! refuses a case with a steady load anywhere else.
//!
//! # Planted ordering bugs
//!
//! [`planted_bugs_are_caught`] is the tier's check on itself (issue #557
//! part 3). The engine carries a few known ordering bugs behind test-only
//! hooks (`trellis::dev::plant::Plant`, `trellis/src/plant.rs`). None is
//! compiled into a production build, and none is armed unless the process
//! starts with `TRELLIS_TEST_PLANT=<name>`. The sweep runs the same seeded
//! cases unplanted and then once per plant, each in its own process, and
//! reports each plant's catch rate and each seed's cases-to-first-catch.
//! It draws from the cooling-key tier by default. The sweep itself is
//! `generative::plant_sweep`, which the `JOIN` tier shares. Each tier gates
//! the plants whose shapes it runs, and says why it doesn't gate the others
//! (`SweepTier::not_gated`): the group-basis plants that need an absolute
//! write racing pages are the mid-burst, steady-load and cooling-backfill
//! tiers'; the `JOIN` plants are the `JOIN` tier's; and the 1-1 plants
//! `early_tombstone_gc` and `chunk_without_entry_lock` are no tier's, since
//! with every aggregate on the group basis no schedule opens their windows:
//! deterministic pins gate them (`ONE_TO_ONE_PINNED`).
//! `trellis/src/plant.rs`'s module doc says how to add a plant.
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
//! the mid-burst actions nor concurrency. It is #494's shape (a key passing
//! through a group inside one folded batch), which disk commit timing makes
//! common. The old aggregate path diverged on it; [`group_moves_converge_on_disk`]
//! is its regression pin.
//!
//! # Pinned cases
//!
//! A tier failure worth keeping is written out as data, not kept as a
//! proptest seed: a seed replays into a different program as soon as the
//! strategy changes shape. [`hot_key_case_3_11_converges`] is the first, the
//! hot-key case that fails on tmpfs with no plant armed. Its ops and plan
//! live in `tests/pins/hot_key_3_11.ops`. It was #623's exit check, and it
//! stays a regression pin.
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
//! two test binaries never share a cluster or a database.

use generative::backend::{
    Backend, ConcurrentBackend, ManualBackend, SEAL_ON_DEMAND_INTERVAL, SPLIT_THRESHOLD_ROWS,
};
use generative::baseline_quarantine;
use generative::generate::{
    ActionDraw, ActionKind, AggregateColumn, AggregateFn, ConcurrentCase, DefShape, Mutate,
    TableSpec, add_burst_actions, build_program, build_program_multi_with_shapes, concurrent_plan,
    cooling_backfill_case, cooling_key_case, defer_def_install, hot_key_case, mid_burst_case,
    partitioned_program, schedule_restart, schedule_scale_out, steady_load_case, trivial_program,
};
use generative::plant_sweep::{self, PLANT_CHILD_ENV, PLANT_TIER_ENV, SweepTier};
use generative::run::{
    RunError, check_program, run_convergence, run_convergence_bursty, run_convergence_concurrent,
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
    /// The cooling-key property's own report (issue #557 part 3b).
    cooling_coverage: std::cell::RefCell<generative::run::Coverage>,
    /// A planted-bug sweep's report (issue #557 part 3), printed by each
    /// sweep process for the cases it ran.
    plant_coverage: std::cell::RefCell<generative::run::Coverage>,
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
        if self.cooling_coverage.borrow().cases > 0 {
            eprintln!(
                "generative: concurrent_convergence cooling-key tier coverage:\n{}",
                self.cooling_coverage.borrow()
            );
        }
        if self.plant_coverage.borrow().cases > 0 {
            eprintln!(
                "generative: concurrent_convergence planted-bug sweep coverage:\n{}",
                self.plant_coverage.borrow()
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
        cooling_coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
        plant_coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
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
        return TestCluster::start().long_lived();
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
    // Fails closed: a filesystem it can't name is refused too, rather than
    // run as though it were a disk.
    let fs_type = std::process::Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(&dir)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_default();
    assert!(
        !fs_type.is_empty(),
        "{CLUSTER_DIR_ENV}={}: couldn't tell its filesystem type (`stat -f`)",
        dir.display()
    );
    assert!(
        !matches!(fs_type.as_str(), "tmpfs" | "ramfs"),
        "{CLUSTER_DIR_ENV}={} is a {fs_type}; point it at a real disk",
        dir.display()
    );
    eprintln!(
        "generative: concurrent_convergence cluster under {} ({fs_type})",
        dir.display()
    );
    TestCluster::start_in(&dir).long_lived()
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
            if program.tables.iter().any(|t| t.partitioning.is_some()) {
                backend.set_reconcile_interval(generative::generate::partition::RECONCILE_INTERVAL);
            }
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
                    "concurrent convergence diverged after op {} (targets {}), burst_size \
                     {burst_size} — see this file's module doc comment's shrink-trust convention \
                     before trusting this as a minimal repro:\n{}",
                    d.op_index,
                    d.targets().collect::<Vec<_>>().join(", "),
                    divergence_reports(&d)
                ))),
                Err(other) => Err(TestCaseError::fail(format!(
                    "run error (burst_size {burst_size}): {other:?}"
                ))),
            }
        })
    })
}

/// Why a concurrent-tier case failed.
#[derive(Debug)]
struct CaseFailure {
    /// Every target that diverged from the oracle (#671: all of them, not
    /// just the first, so the sweep can attribute a failure to a plant even
    /// when an unplanted divergence shows up in the same check). Empty when
    /// the case failed by erroring rather than by diverging.
    targets: Vec<String>,
    message: String,
}

/// Every diverging target's report, each headed by its target.
fn divergence_reports(d: &generative::run::Divergence) -> String {
    d.all()
        .map(|(target, report)| format!("target {target}:\n{report}"))
        .collect::<Vec<_>>()
        .join("\n")
}

impl From<CaseFailure> for TestCaseError {
    fn from(failure: CaseFailure) -> Self {
        TestCaseError::fail(failure.message)
    }
}

/// Runs one concurrent-tier case (issue #557) against a fresh isolated
/// database: the case's own worker count and seal cadence, its plan's lanes
/// issued from separate tasks, its mid-burst actions taken while they run.
/// Records the case's shapes and the engine's drain audit into `coverage`.
fn run_concurrent_case(
    case: &ConcurrentCase,
    coverage: impl Fn(&Harness) -> &std::cell::RefCell<generative::run::Coverage>,
) -> Result<(), CaseFailure> {
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
            // The load's stall is process-wide (`set_stall`): every other
            // test in this process would run under it too. A sweep process
            // runs one case at a time and nothing else.
            assert!(
                case.plan.steady_load.is_none() || std::env::var_os(PLANT_CHILD_ENV).is_some(),
                "a steady-load case runs only in a planted-bug sweep's own process"
            );
            if let Some(rows) = case.build_chunk_rows {
                backend.set_build_chunk_rows(rows);
            }
            if let Some(ms) = case.reconcile_interval_ms {
                backend.set_reconcile_interval(std::time::Duration::from_millis(ms));
            }
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
                        Err(CaseFailure {
                            targets: Vec::new(),
                            message: format!("run did not pass ({shape}): {}", run.outcome),
                        })
                    }
                }
                Err(RunError::Diverged(d)) => Err(CaseFailure {
                    message: format!(
                        "concurrent case diverged in the burst ending at op {} (targets {}; \
                         {shape}) — see this file's module doc comment's shrink-trust \
                         convention before trusting this as a minimal repro:\n{}",
                        d.op_index,
                        d.targets().collect::<Vec<_>>().join(", "),
                        divergence_reports(&d)
                    ),
                    targets: d.targets().map(str::to_string).collect(),
                }),
                Err(other) => Err(CaseFailure {
                    targets: Vec::new(),
                    message: format!("run error ({shape}): {other:?}"),
                }),
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

    /// ADR-0017: `partitioned_program` under the concurrent backend, so
    /// writes to different leaves of one partitioned source drain in
    /// parallel.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_a_partitioned_source_converges_under_the_concurrent_backend(
        program in partitioned_program(),
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

    /// Issue #557 part 3b: the cooling-key tier. See the module doc
    /// comment.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_cooling_keys_converge_under_concurrent_drains(case in cooling_key_case()) {
        run_concurrent_case(&case, |h| &h.cooling_coverage)?;
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
/// **Why the harness does all the sealing:** the engine's maintenance loop
/// (which performs the seal that fixes a batch's `bucket_count`) ticks on its
/// own fixed cadence (300ms by default), independent of anything this
/// harness does. If 1,000 sequential raw-DML round trips straddled a tick,
/// the rows would split across two (or more) smaller sealed batches, each
/// possibly below the split threshold. So the engine runs with
/// [`SEAL_ON_DEMAND_INTERVAL`] (no tick after its first, issue #453), and the
/// pin brackets the inserts between two forced seals of its own. The two
/// sealed segments must be consecutive: no other seal (the first tick's, say)
/// landed between them, so every insert, committed after the first and
/// before the second, is in the second's batch. "All 1,000 rows land in the
/// same sealed batch" is then asserted, not just likely.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_that_exceeds_the_split_threshold_converges_across_workers() {
    const ROW_COUNT: i64 = 1_000;
    const WORKERS: usize = 4;
    // The batch must stay well above the split threshold if it moves.
    const _: () = assert!(ROW_COUNT as usize >= 2 * SPLIT_THRESHOLD_ROWS);

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let seed_values: Vec<(Option<i64>, Option<i64>)> =
        (1..=ROW_COUNT).map(|i| (Some(i), Some(i))).collect();
    let program = build_program(&seed_values, &[]);
    assert_eq!(program.ops.len(), ROW_COUNT as usize);

    let mut backend =
        ManualBackend::connect_with_options(db.dsn(), WORKERS, Some(SEAL_ON_DEMAND_INTERVAL))
            .await
            .expect("connect concurrent backend that seals only on demand");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    // Issue #557: the drain audit the hot-key tier reports through, checked
    // here against a batch whose split is deterministic.
    backend
        .start_drain_audit()
        .await
        .expect("start the drain audit");

    backend.install(&program).await.expect("install program");
    backend
        .quiesce_forcing_seals()
        .await
        .expect("quiesce the empty install");
    let before_inserts = backend
        .force_seal_active_segment()
        .await
        .expect("seal whatever precedes the inserts");
    for op in &program.ops {
        let inserted = backend.apply(op).await.expect("insert a row");
        assert_eq!(inserted, 1, "op {op:?} must insert exactly one row");
    }
    assert!(
        backend.has_pending().await.expect("check pending"),
        "a committed insert must be in the ring at commit"
    );
    let inserts_seg = backend
        .force_seal_active_segment()
        .await
        .expect("seal every insert into one batch");
    // The definition goes live off the start-up capture pass (its build
    // starts there and the drain workers flip it), not off the maintenance
    // loop's first tick, so nothing orders that tick's one seal before the
    // quiesce above returns. In practice it runs well before, but this is
    // what shows no seal split the inserts.
    assert_eq!(
        inserts_seg,
        before_inserts + 1,
        "another seal landed while the inserts were applied (segments {before_inserts} and \
         {inserts_seg} aren't consecutive), so they may not share one batch"
    );
    backend
        .quiesce_forcing_seals()
        .await
        .expect("a batch that exceeds the split threshold must still converge");

    let snapshot = backend.snapshot().await.expect("snapshot");
    let diverged = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check");
    assert!(
        diverged.is_empty(),
        "the split batch diverged: {diverged:?}"
    );

    let audit = backend.drain_audit().await.expect("read the drain audit");
    assert!(
        audit.split >= 1
            && audit.max_rows_per_batch >= ROW_COUNT as u64
            && audit.max_workers_per_batch >= 1,
        "the drain audit must see the {ROW_COUNT}-insert batch, split, and its claims: {audit:?}"
    );

    let max_bucket_count = backend
        .max_bucket_count()
        .await
        .expect("read back the sealed batch's bucket_count");
    assert!(
        max_bucket_count > 1,
        "a {ROW_COUNT}-row batch (well above the engine's real split threshold) must have been \
         partitioned into more than one bucket at seal — got max bucket_count \
         {max_bucket_count}, so the engine's own splitting decision regressed"
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
    // `request_backfill` has to find the table among the ones this backend
    // captures (#641).
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

/// Found by #557 part 2's disk-backed cluster: on a real disk, plain group
/// moves leave a `GROUP BY` target wrong for good. A group every row has
/// left keeps its target row, or a group's `COUNT(*)` is one to three too
/// high. It needs no concurrency: one lane issues every op in program order
/// and one drain worker applies them. It needs no `SUM` (`COUNT(*)` alone
/// fails as often) and no build (it fails as often when the definition is
/// `live` before the first op), but it does need group moves: inserts and
/// deletes alone converge. On the tmpfs test cluster the same run converges.
///
/// It is #494's shape, and this is its regression pin. On the old aggregate
/// path (before #623 D5) a batch's forced re-derive
/// read the source live, so it counted a key that a later, still-undrained
/// commit moved into group `z`, and stamped the group's recompute horizon
/// above that commit. The key then left `z` above the horizon, and both
/// moves folded into one later batch as `a -> b`, so nothing named `z` and
/// its count was never taken back. On disk, the drain runs far enough behind
/// the source that most group writes were such re-derives. The group basis
/// has no horizons: a change carries its old and new images, so leaving `z`
/// takes `z`'s count back.
///
/// Runs [`group_moves`] `ATTEMPTS` times, each on a fresh database, in
/// bursts of 500 ops sealed every 85ms, and fails if any attempt diverged,
/// reporting how many did and whether each divergence was still there after
/// another 5 seconds and quiesce. It says nothing on a tmpfs, so without
/// `GENERATIVE_CLUSTER_DIR` it returns at once, so the default suite pays
/// nothing for it. Run it on disk:
///
/// ```text
/// GENERATIVE_CLUSTER_DIR=$PWD/target/generative-disk \
/// cargo test -p generative --test concurrent_convergence \
///     group_moves_converge_on_disk -- --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn group_moves_converge_on_disk() {
    const ATTEMPTS: usize = 20;
    if std::env::var_os(CLUSTER_DIR_ENV).is_none() {
        eprintln!("group_moves_converge_on_disk: skipped, {CLUSTER_DIR_ENV} is not set");
        return;
    }
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
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        match run_convergence_concurrent(&mut backend, &pool, &program, &plan).await {
            Ok(run) => assert!(run.outcome.as_pass(), "run did not pass: {}", run.outcome),
            Err(RunError::Diverged(d)) => {
                // Diagnostic only: whether the engine corrects the target
                // given more time, or it stays wrong.
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
                    if !later.is_empty() {
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

// ---------------------------------------------------------------------
// Pinned hot-key case 3:11: a regression pin, and a re-check for #624.
// ---------------------------------------------------------------------

/// How many attempts [`hot_key_case_3_11_converges`] makes. Unset, it
/// returns at once, so the default suite pays nothing for it.
const PIN_ATTEMPTS_ENV: &str = "GENERATIVE_PIN_ATTEMPTS";

/// A table whose primary key is its first column.
fn pin_table(
    name: &str,
    columns: &[(&str, trellis::dev::defs::ast::ValueType)],
    unique: &[&str],
) -> generative::model::Table {
    generative::model::Table {
        name: name.to_string(),
        pk_col: columns[0].0.to_string(),
        columns: columns
            .iter()
            .map(|(name, value_type)| generative::model::Column {
                name: name.to_string(),
                value_type: *value_type,
            })
            .collect(),
        unique_cols: unique.iter().map(|c| c.to_string()).collect(),
        partitioning: None,
    }
}

/// Reads a pinned case's ops and plan, one op per line, tab-separated,
/// each line led by the burst and the lane that issue it:
///
/// ```text
/// <burst>  <lane>  insert  <table>  <expect>  <col>=<value> ...
/// <burst>  <lane>  update  <table>  <expect>  <pk>  <col>=<value> ...
/// <burst>  <lane>  delete  <table>  <expect>  <pk>
/// ```
///
/// Bursts run in file order, and each lane issues its ops in file order.
/// `<expect>` is `ok`, `fails` or `none` (affects no rows). A value of `\N`
/// is NULL, and `\\` is a backslash.
fn pin_ops(
    text: &str,
) -> (
    Vec<generative::model::Op>,
    generative::model::ConcurrentPlan,
) {
    use generative::model::{Burst, ConcurrentPlan, Op, OpOutcome};
    let value = |v: &str| (v != "\\N").then(|| v.replace("\\\\", "\\"));
    let columns = |fields: &[&str]| -> Vec<(String, Option<String>)> {
        fields
            .iter()
            .map(|field| {
                let (column, v) = field
                    .split_once('=')
                    .unwrap_or_else(|| panic!("pinned op field {field:?}: expected <col>=<value>"));
                (column.to_string(), value(v))
            })
            .collect()
    };
    let mut ops = Vec::new();
    let mut plan = ConcurrentPlan::default();
    for line in text.lines() {
        let fields: Vec<&str> = line.split('\t').collect();
        let [burst, lane, kind, table, expect, rest @ ..] = fields.as_slice() else {
            panic!("pinned op {line:?}: too few fields");
        };
        let burst: usize = burst.parse().expect("a burst number");
        let lane: usize = lane.parse().expect("a lane number");
        let table = table.to_string();
        let expect = match *expect {
            "ok" => OpOutcome::Succeeds,
            "fails" => OpOutcome::Fails,
            "none" => OpOutcome::AffectsNoRows,
            other => panic!("pinned op {line:?}: unknown outcome {other:?}"),
        };
        ops.push(match (*kind, rest) {
            ("insert", row) if !row.is_empty() => Op::Insert {
                table,
                row: columns(row),
                expect,
            },
            ("update", [pk, changes @ ..]) => Op::Update {
                table,
                pk: pk.to_string(),
                changes: columns(changes),
                expect,
            },
            ("delete", [pk]) => Op::Delete {
                table,
                pk: pk.to_string(),
                expect,
            },
            _ => panic!("pinned op {line:?}: unknown shape"),
        });
        assert!(
            burst == plan.bursts.len() || burst + 1 == plan.bursts.len(),
            "pinned op {line:?}: bursts must come in order, without gaps"
        );
        if burst == plan.bursts.len() {
            plan.bursts.push(Burst::default());
        }
        let lanes = &mut plan.bursts[burst].lanes;
        if lanes.len() <= lane {
            lanes.resize(lane + 1, Vec::new());
        }
        lanes[lane].push(ops.len() - 1);
    }
    assert!(!ops.is_empty(), "a pinned case with no ops");
    (ops, plan)
}

/// [`pin_ops`] refuses a malformed line instead of skipping it, so a pin
/// can't quietly lose ops.
#[test]
fn pin_ops_rejects_malformed_lines() {
    let (ops, plan) = pin_ops("0\t0\tinsert\tt0\tok\tc0=1\tc1=\\N\n1\t1\tdelete\tt0\tnone\t1\n");
    assert_eq!(ops.len(), 2);
    assert_eq!(plan.bursts.len(), 2);
    assert_eq!(plan.bursts[1].lanes, vec![vec![], vec![1]]);
    let delete = |burst: usize, pk: usize| format!("{burst}\t0\tdelete\tt0\tok\t{pk}\n");
    for bad in [
        String::new(),
        delete(0, 1) + "\n" + &delete(0, 2),
        "0\t0\tinsert\tt0".to_string(),
        "0\t0\tinsert\tt0\tok".to_string(),
        "0\t0\tinsert\tt0\tok\tc0".to_string(),
        "0\t0\tinsert\tt0\tmaybe\tc0=1".to_string(),
        "0\t0\tupsert\tt0\tok\tc0=1".to_string(),
        "0\t0\tdelete\tt0\tok\t1\t2".to_string(),
        "x\t0\tdelete\tt0\tok\t1".to_string(),
        delete(0, 1) + &delete(2, 2),
        delete(0, 1) + &delete(1, 2) + &delete(0, 3),
    ] {
        assert!(
            std::panic::catch_unwind(|| pin_ops(&bad)).is_err(),
            "{bad:?} parsed"
        );
    }
}

/// Hot-key case 3:11, the one case #557 part 3a's planted-bug sweep saw
/// fail on tmpfs with no plant armed (`GENERATIVE_PLANT_ONLY=3:11`), cut down
/// by hand and written out op for op in `tests/pins/hot_key_3_11.ops`, so no
/// generator change can move it.
///
/// `t2`, `t3` and `t4` all group `t0` by `c6`, on the group basis, which
/// admits only `COUNT`/`SUM`/`AVG` (#1100): each drawn `MIN(x)` is now
/// `COUNT(x)`, each `MAX(x)` `SUM(x)`, and each `BOOL_AND(c4)`/`BOOL_OR(c4)`
/// `COUNT(c4)`. `t2` has two SUMs, `COUNT(*)`, AVG and two COUNTs, `t3`
/// AVG, `COUNT(c1)` and `SUM(c2)`, `t4` two SUMs, AVG and two COUNTs. The ops
/// are three bursts over two lanes, 1,270 in all: 4 `t1` inserts and, on
/// `t0`, 117 inserts, 101 deletes, 601 updates to `c1` and `c2`, 273 that move
/// a row between `c6` groups, and 174 to `c1` or `c4` alone.
///
/// Cut from the drawn case: its 9 `t1` updates and its 741 ops that fail or
/// touch no row. Both cuts made it fail more often, not less (see
/// [`hot_key_case_3_11_converges`]).
fn hot_key_case_3_11() -> (
    generative::model::Program,
    generative::model::ConcurrentPlan,
) {
    use trellis::IntWidth;
    use trellis::dev::defs::ast::ValueType::{Boolean, Integer, Numeric, Text, Uuid};
    let tables = vec![
        pin_table(
            "t0",
            &[
                ("c0", Integer(IntWidth::Int8)),
                ("c1", Numeric),
                ("c2", Numeric),
                ("c3", Text),
                ("c4", Boolean),
                ("c5", Uuid),
                ("c6", Numeric),
                ("c7", Text),
                ("c8", Text),
            ],
            &["c7"],
        ),
        pin_table(
            "t1",
            &[
                ("c9", Integer(IntWidth::Int8)),
                ("c10", Numeric),
                ("c11", Numeric),
                ("c12", Text),
                ("c13", Boolean),
                ("c14", Uuid),
                ("c15", Numeric),
                ("c16", Text),
                ("c17", Text),
            ],
            &["c16"],
        ),
    ];
    let defs: Vec<_> = [
        "TRANSFORM t2 FROM t0 GROUP BY c6 SELECT c6 AS c6, SUM(c2) AS sum_c2, \
         COUNT(*) AS cnt, AVG(c1) AS avg_c1, COUNT(c2) AS count_c2, SUM(c1) AS sum_c1, \
         COUNT(c4) AS count_c4",
        "TRANSFORM t3 FROM t0 GROUP BY c6 SELECT c6 AS c6, AVG(c1) AS avg_c1, \
         COUNT(c1) AS count_c1, SUM(c2) AS sum_c2",
        "TRANSFORM t4 FROM t0 GROUP BY c6 SELECT c6 AS c6, SUM(c2) AS sum_c2, \
         AVG(c1) AS avg_c1, COUNT(c2) AS count_c2, SUM(c1) AS sum_c1, COUNT(c4) AS count_c4",
    ]
    .iter()
    .map(|text| trellis::dev::defs::parse(text).expect("a pinned definition parses"))
    .collect();
    let (ops, plan) = pin_ops(include_str!("pins/hot_key_3_11.ops"));
    let program = generative::model::Program {
        tables,
        def_install_after_op: vec![0; defs.len()],
        defs,
        ops,
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };
    (program, plan)
}

/// The pinned case loads in the default suite, so a change to the model or
/// to `pin_ops` that breaks it shows up here rather than only when someone
/// runs the pin with `GENERATIVE_PIN_ATTEMPTS` set. It touches no database.
#[test]
fn hot_key_case_3_11_loads() {
    let (program, plan) = hot_key_case_3_11();
    assert_eq!(program.ops.len(), 1_270);
    assert_eq!(plan.bursts.len(), 3);
    assert!(plan.bursts.iter().all(|burst| burst.lanes.len() == 2));
    assert_eq!(program.defs.len(), 3);
}

/// [`hot_key_case_3_11`] fails some of the time on tmpfs, at a rate that
/// swings with whatever else the box is doing: 15 of 80 alone and 13 of 200
/// with four processes at once when it was written, but 0 of 70 alone and 3
/// of 80 with four processes at once in review, while other work shared the
/// box. Usually a `GROUP BY` `SUM` is wrong once a burst quiesces
/// (`t2[1:0].sum_c2: expected=68`, with `got` anywhere from 4 to 148), or
/// the run never converges
/// (`ConvergenceTimeout`). The input is identical every time; only the drain
/// workers' timing differs. It was the exit check of #556's milestone D
/// (#623) and is a regression pin since; milestone E (#624) re-runs it as
/// its own check. Either check needs the same box to show it failing on the
/// unfixed engine first: a pass means little unless the unfixed engine
/// fails the same run.
///
/// What the failure needs, from cutting the drawn case down on tmpfs with
/// four processes at once, 160 to 200 runs per row:
///
/// | the drawn case 3:11 ... | failed |
/// |---|---|
/// | as drawn | 6/160 (2 diverged, 4 never converged) |
/// | without the fields that read `t1` | 3/200 |
/// | without the fields that read `t1` and any `t1` op | 2/160 |
/// | without the `t1` updates | 17/200 |
/// | without the `t1` updates or the ops that change nothing (this pin) | 18/160 |
/// | without the `t1` updates, `t2` alone | 0/160 |
/// | without the group moves | 0/200 |
///
/// So it needs group moves, and it needs more than one `GROUP BY` definition
/// over the hot table, or at least the apply load a second one adds: the
/// table can't tell those apart. It doesn't need the fields that read `t1`
/// (it still diverges without them) or the `t1` updates (without them it
/// fails more often, not less). Both
/// remaining suspects are #623's scope. Group moves under `MIN`/`MAX`
/// recomputes are #494's shape: a key passing through a group inside one
/// folded batch, under a recompute horizon. And #649's review saw up to 22
/// `deadlock detected` per run between the three definitions' aggregate
/// pre-locks (#326's group pre-lock). #623 D5 removed both the horizons and
/// the pre-lock.
///
/// Runs the case `GENERATIVE_PIN_ATTEMPTS` times, each on a fresh database
/// with 8 drain workers and a 112ms seal, and fails if any attempt diverged
/// or didn't converge. Unset, it returns at once:
///
/// ```text
/// GENERATIVE_PIN_ATTEMPTS=20 cargo test -p generative \
///     --test concurrent_convergence hot_key_case_3_11_converges \
///     -- --nocapture
/// ```
///
/// Twenty attempts take two to three minutes. At 19% they would miss the
/// failure about one time in fifty, but at 4% about half the time, so an
/// exit check wants a hundred or more attempts, split over several
/// processes. It respects `GENERATIVE_CLUSTER_DIR`, so it runs on disk too.
#[tokio::test(flavor = "multi_thread")]
async fn hot_key_case_3_11_converges() {
    let Some(attempts) = std::env::var(PIN_ATTEMPTS_ENV).ok().map(|v| {
        v.parse::<usize>()
            .expect("GENERATIVE_PIN_ATTEMPTS: a number")
    }) else {
        eprintln!("hot_key_case_3_11_converges: skipped, {PIN_ATTEMPTS_ENV} is not set");
        return;
    };
    let cluster = start_cluster();
    let (program, plan) = hot_key_case_3_11();
    let mut failed = Vec::new();
    for attempt in 1..=attempts {
        let db = cluster.create_isolated_database().await;
        let mut backend = ManualBackend::connect_with_options(
            db.dsn(),
            8,
            Some(std::time::Duration::from_millis(112)),
        )
        .await
        .expect("connect concurrent backend");
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        let started = std::time::Instant::now();
        let result = run_convergence_concurrent(&mut backend, &pool, &program, &plan).await;
        let secs = started.elapsed().as_secs_f64();
        let failure = match result {
            Ok(run) if run.outcome.as_pass() => None,
            Ok(run) => Some(format!("did not pass: {}", run.outcome)),
            Err(RunError::Diverged(d)) => {
                // The report's first lines name the wrong rows; the rest
                // prints the whole program.
                let report = d.report.to_string();
                let rows: Vec<&str> = report
                    .lines()
                    .skip(1)
                    .take_while(|l| l.starts_with("  "))
                    .collect();
                Some(format!(
                    "diverged in the burst ending at op {}: {}",
                    d.op_index,
                    rows.join(";")
                ))
            }
            Err(other) => Some(format!("{other:?}")),
        };
        match failure {
            None => eprintln!("attempt {attempt}: converged in {secs:.1}s"),
            Some(failure) => {
                eprintln!("attempt {attempt}: FAILED after {secs:.1}s: {failure}");
                failed.push(attempt);
            }
        }
    }
    assert!(
        failed.is_empty(),
        "{} of {attempts} attempts failed: {failed:?}",
        failed.len()
    );
}

// ---------------------------------------------------------------------
// Issue #557 part 3: planted ordering bugs.
// ---------------------------------------------------------------------

/// Why the hot-key-based tiers miss `stale_one_to_one_write` (#344): the
/// plant fires in a third to a half of their cases, but a stale 1-1 value
/// only survives when the batch holding a key's *last* change commits before
/// an older batch for that key. Their hot keys keep changing until the burst
/// ends, so a later batch nearly always rewrites the key. It was caught once
/// in about 390 cases (#557 part 3a's PR). The cooling-key tier is the shape
/// that catches it.
const STALE_ONE_TO_ONE_MISS: &str = "stale_one_to_one_write";

/// The cooling-key tier's known misses.
const COOLING_KEY_MISSES: &[&str] = &[];
/// The known misses of the tiers built on the hot-key case: hot-key,
/// mid-burst, and steady-load with the group basis off (with it on,
/// [`STEADY_LOAD_MISSES`]).
const HOT_KEY_BASED_MISSES: &[&str] = &[STALE_ONE_TO_ONE_MISS];

const COOLING_KEY_NOT_GATED: &[(&str, &str)] = &[];
const HOT_KEY_NOT_GATED: &[(&str, &str)] = &[];
const MID_BURST_NOT_GATED: &[(&str, &str)] = &[];

/// The cooling-backfill tier's programs are the hot table and the cooling
/// table, each read by a `GROUP BY` definition, and install or resume
/// nothing mid-burst (#734). It gates the plants a `request_backfill` under
/// load reaches, and not the 1-1 or build plants.
const COOLING_BACKFILL_NOT_GATED: &[(&str, &str)] = &[(
    "stale_one_to_one_write",
    "it needs a 1-1 definition, and the tier's programs have none (0 of 48 \
     cases fired it); the cooling-key tier gates it",
)];
const STEADY_LOAD_NOT_GATED: &[(&str, &str)] = &[];

/// The 1-1 plants no tier gates: with every aggregate on the group basis,
/// neither has a window a random schedule opens, and each is pinned by
/// deterministic tests instead.
const ONE_TO_ONE_PINNED: &[(&str, &str)] = &[
    (
        "early_tombstone_gc",
        "it fires only when a segment drains past an older one, and with the group \
         basis no tier's drain does: it fired in 0 of 48 cases in each of the \
         steady-load and cooling-backfill tiers on 42545471 (2026-10-23); the \
         one_to_one_ledger_interleavings pins exp2_9_gc_waits_for_the_older_update_one_to_one, \
         an_out_of_order_drain_holds_gc_back_one_to_one and \
         gc_under_a_concurrent_reinsert_one_to_one gate it",
    ),
    (
        "chunk_without_entry_lock",
        "on a 1-1 target the unlocked chunk still writes the row its read saw and its \
         entry rewrite still queues behind the page, so nothing diverges (it fired in \
         21 of 48 steady-load cases and was caught in none on 42545471, 2026-10-23); \
         a_page_queues_behind_a_chunk_holding_its_key (one_to_one_build_interleavings) \
         pins the lock",
    ),
];
const NO_JOIN: &str = "the concurrent tier draws no `JOIN`; the JOIN tier \
                       (join_convergence.rs, join_planted_bugs_are_caught) gates it";

/// Measured catch rates of the group-basis plants (#1114, 4 seeds x 12
/// cases per tier, logbook `20261024T051124-a-sweep`): `gb_skip_group_lock`
/// 24 to 44 of 48 in every tier; `gb_fold_sides_per_key` 48 of 48 in the
/// steady-load, mid-burst and cooling-backfill tiers; `gb_lsn_not_visibility`
/// 12 and 10 of 48 in the steady-load and mid-burst tiers;
/// `gb_build_snapshot_before_locks` 5 of 48 in the steady-load tier, whose
/// stall holds a build between its enumeration and its locks; and
/// `gb_early_zero_gc` in none (see [`EARLY_ZERO_GC_MISS`]).
///
/// The cooling-key and hot-key tiers run no absolute write after the first
/// writes, so they gate only `gb_skip_group_lock`.
const NO_ABSOLUTE_WRITE_UNDER_LOAD: &[(&str, &str)] = &[
    (
        "gb_build_snapshot_before_locks",
        "it needs a build or rebuild whose groups a page changes between its \
         enumeration and its locks, and the tier installs and resumes nothing \
         mid-burst; the steady-load tier gates it",
    ),
    (
        "gb_lsn_not_visibility",
        "it needs a basis taken while a writer to its group is in flight, and \
         only an absolute write takes one, which this tier runs only before the \
         first writes; the steady-load and mid-burst tiers gate it",
    ),
    (
        "gb_fold_sides_per_key",
        "not measured here; the steady-load, mid-burst and cooling-backfill tiers \
         gate it (48 of 48 cases each)",
    ),
    (
        "gb_early_zero_gc",
        "it needs a zero group whose basis saw a change not yet drained, and only \
         an absolute write takes one, which this tier runs only before the first \
         writes; the steady-load tier runs it as a known miss",
    ),
];

/// Why no tier catches `gb_early_zero_gc` yet (#1114): it needs the GC pass
/// to meet a zero group whose stamp is past the drained prefix (an absolute
/// write's zero group with a change it saw still undrained) and then a page
/// of that change to land on the fresh row; the plant changed the GC's
/// choice in 0 to 4 of 48 cases per tier and the tiers caught none. The
/// deterministic pins (`group_basis_interleavings`'s `r4_…`,
/// `join_interleavings`'s `family13_…`) gate it meanwhile; #1373 tracks
/// the generator reach.
const EARLY_ZERO_GC_MISS: &str = "gb_early_zero_gc";

/// The steady-load tier's known misses with the group basis on.
const STEADY_LOAD_MISSES: &[&str] = &[STALE_ONE_TO_ONE_MISS, EARLY_ZERO_GC_MISS];

/// The mid-burst tier's group-basis plants it doesn't gate.
const MID_BURST_GROUP_BASIS: &[(&str, &str)] = &[
    (
        "gb_build_snapshot_before_locks",
        "it fired in 48 of 48 cases and was caught in none: a page has to commit \
         between a build's enumeration and its locks, and only the steady-load \
         tier's stall holds a build there; the steady-load tier gates it",
    ),
    (
        "gb_early_zero_gc",
        "it fired in 0 of 48 cases; the steady-load tier runs it as a known miss",
    ),
];

/// The cooling-backfill tier's group-basis plants it doesn't gate.
const COOLING_BACKFILL_GROUP_BASIS: &[(&str, &str)] = &[
    (
        "gb_build_snapshot_before_locks",
        "it fired in 48 of 48 cases and was caught in none: a page has to commit \
         between a rebuild's enumeration and its locks, and only the steady-load \
         tier's stall holds a build there; the steady-load tier gates it",
    ),
    (
        "gb_lsn_not_visibility",
        "it fired in 0 of 48 cases: no rebuild's basis caught a writer of its \
         groups in flight; the steady-load and mid-burst tiers gate it",
    ),
    (
        "gb_early_zero_gc",
        "it fired in 0 of 48 cases; the steady-load tier runs it as a known miss",
    ),
];

/// A tier's `not_gated`: `listed`, the 1-1 plants no tier gates and the
/// `JOIN` plants, with their reasons.
fn not_gated(listed: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    use generative::plant_sweep::{PlantPath, not_on_path};
    let mut out: Vec<(&'static str, &'static str)> = listed.to_vec();
    for (plant, why) in
        not_on_path(&[PlantPath::Join], NO_JOIN).chain(ONE_TO_ONE_PINNED.iter().copied())
    {
        if !out.iter().any(|(listed, _)| *listed == plant) {
            out.push((plant, why));
        }
    }
    out
}

/// The tier a sweep draws from, by [`PLANT_TIER_ENV`].
///
/// Every tier's baseline is held to the same bar: no failure outside
/// `generative/baseline-quarantine.txt` (`generative::baseline_quarantine`,
/// #786).
fn sweep_tier() -> (SweepTier, BoxedStrategy<ConcurrentCase>) {
    let name = std::env::var(PLANT_TIER_ENV);
    sweep_tier_named(name.as_deref().unwrap_or("cooling_key")).unwrap_or_else(|| {
        panic!(
            "{PLANT_TIER_ENV}={name:?}: expected one of {:?}",
            baseline_quarantine::CONCURRENT_TIERS
        )
    })
}

/// The tier named `name`, and its cases' strategy, if there is one.
fn sweep_tier_named(name: &str) -> Option<(SweepTier, BoxedStrategy<ConcurrentCase>)> {
    let with_no_absolute_write = |listed: &[(&'static str, &'static str)]| {
        let mut all = listed.to_vec();
        all.extend_from_slice(NO_ABSOLUTE_WRITE_UNDER_LOAD);
        not_gated(&all)
    };
    Some(match name {
        "cooling_key" => (
            SweepTier {
                name: "cooling_key",
                known_misses: COOLING_KEY_MISSES,
                not_gated: with_no_absolute_write(COOLING_KEY_NOT_GATED),
            },
            cooling_key_case().boxed(),
        ),
        "hot_key" => (
            SweepTier {
                name: "hot_key",
                known_misses: HOT_KEY_BASED_MISSES,
                not_gated: with_no_absolute_write(HOT_KEY_NOT_GATED),
            },
            hot_key_case().boxed(),
        ),
        "mid_burst" => (
            SweepTier {
                name: "mid_burst",
                known_misses: HOT_KEY_BASED_MISSES,
                not_gated: not_gated(&[MID_BURST_NOT_GATED, MID_BURST_GROUP_BASIS].concat()),
            },
            mid_burst_case().boxed(),
        ),
        "steady_load" => (
            SweepTier {
                name: "steady_load",
                known_misses: STEADY_LOAD_MISSES,
                not_gated: not_gated(STEADY_LOAD_NOT_GATED),
            },
            steady_load_case().boxed(),
        ),
        "cooling_backfill" => (
            SweepTier {
                name: "cooling_backfill",
                known_misses: &[],
                not_gated: not_gated(
                    &[COOLING_BACKFILL_NOT_GATED, COOLING_BACKFILL_GROUP_BASIS].concat(),
                ),
            },
            cooling_backfill_case().boxed(),
        ),
        _ => return None,
    })
}

#[test]
fn every_concurrent_tier_the_quarantine_list_can_name_is_a_sweep_tier() {
    for name in baseline_quarantine::CONCURRENT_TIERS {
        let (tier, _) = sweep_tier_named(name).expect("a sweep tier");
        assert_eq!(tier.name, *name);
    }
}

/// Every tier's `known_misses` and `not_gated` name real plants, each at
/// most once.
#[test]
fn every_tier_lists_real_plants() {
    use trellis::dev::plant::Plant;
    for name in baseline_quarantine::CONCURRENT_TIERS {
        let (tier, _) = sweep_tier_named(name).expect("a sweep tier");
        let listed: Vec<&str> = tier
            .known_misses
            .iter()
            .copied()
            .chain(tier.not_gated.iter().map(|(plant, _)| *plant))
            .collect();
        for plant in &listed {
            assert!(
                Plant::from_name(plant).is_some(),
                "the {name} tier lists {plant:?}, which names no plant"
            );
            assert_eq!(
                listed.iter().filter(|p| *p == plant).count(),
                1,
                "the {name} tier lists {plant} more than once"
            );
        }
        assert!(
            Plant::ALL
                .iter()
                .filter(|p| generative::plant_sweep::plant_path(**p)
                    == generative::plant_sweep::PlantPath::Join)
                .all(|p| tier.not_gated.iter().any(|(name, _)| *name == p.name())),
            "the {name} tier draws no JOIN, so it gates no JOIN plant"
        );
    }
}

/// Issue #557 part 3: shows the concurrent tier catches each planted ordering
/// bug (`trellis::dev::plant::Plant`), and how often.
///
/// Draws the same seeded cases ([`plant_sweep::PLANT_SEEDS_ENV`] seeds of
/// [`plant_sweep::PLANT_CASES_ENV`] cases each, from the cooling-key tier unless
/// [`PLANT_TIER_ENV`] names another) and runs them once with no plant, as
/// the baseline, then once per plant (`generative::plant_sweep`, which says
/// how the sweep is judged). Each run is its own process (this test binary,
/// re-run on this test alone), because a plant is armed per process by
/// `TRELLIS_TEST_PLANT`; see `trellis/src/plant.rs` for why. Every case runs
/// to the end, caught or not, so the report gives each plant's catch rate
/// and each seed's cases-to-first-catch, not just whether it was caught once.
///
/// Fails if the baseline fails a case the quarantine list doesn't name, or a
/// plant the tier gates (not in its `SweepTier::known_misses` or
/// `SweepTier::not_gated`) is never caught. `all` leaves out the plants the
/// tier doesn't gate; `none` runs the baseline alone. Run it on the tmpfs
/// cluster, which is what the tiers are calibrated for.
///
/// Returns at once without [`plant_sweep::PLANTS_ENV`], so the nightly's
/// `--include-ignored` run of this binary pays nothing for it. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all cargo test -p generative --test concurrent_convergence \
///     planted_bugs_are_caught -- --ignored --nocapture
/// ```
///
/// The properties catch a plant on their own too, one process per plant:
/// `TRELLIS_TEST_PLANT=gb_skip_group_lock cargo test -p generative --test
/// concurrent_convergence property_hot_keys -- --ignored`.
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn planted_bugs_are_caught() {
    let (tier, strategy) = sweep_tier();
    plant_sweep::sweep(
        "planted_bugs_are_caught",
        &tier,
        |seed, cases| plant_sweep::seeded_cases(&strategy, seed, cases),
        |case: &ConcurrentCase| {
            run_concurrent_case(case, |h| &h.plant_coverage).map_err(
                |CaseFailure { targets, message }| plant_sweep::CaseFailure {
                    key_spaces: targets
                        .iter()
                        .map(|target| {
                            case.program
                                .defs
                                .iter()
                                .find(|def| def.target == *target)
                                .map(|def| def.key_space.clone())
                        })
                        .collect(),
                    message,
                },
            )
        },
    );
}
