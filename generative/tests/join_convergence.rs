//! The JOIN tier (#1113, J8): `JOIN … GROUP BY` targets kept by the engine
//! from writes to both of their tables, checked against the definition's own
//! `JOIN … GROUP BY` query over the two tables
//! (`generative::oracle::render_join_select`).
//!
//! # The property
//!
//! [`property_join_targets_converge_under_concurrent_drains`] draws
//! `generate::join::join_case_strategy`: one to three `LEFT` or `INNER`
//! definitions over `ja` and `jb`, joined on `k` or on `(t, k)`, grouped by
//! `jb.gb` (#1091 J2's S1), by `ja.ga, jb.gb` (S2) or by `ja.ga`, with
//! `COUNT`/`SUM`/`AVG` fields on either side, an expression argument and
//! composed fields; and 120 to 900 inserts, updates and deletes over both
//! tables, with `NULL` keys, groups and values now and then, and about one
//! op in a hundred a `TRUNCATE` of either table (ADR-0019's per-side
//! truncate, #1110). Its key shape
//! is one of:
//!
//! - `spread`: a dozen join keys;
//! - `hot_key`: four in five writes on key 0, so pages on both sides queue
//!   on one key's `J` row and cells;
//! - `fan_out`: three keys, both tables written alike, so each key has many
//!   rows on each side and one change fans out over many cells;
//! - `wide`: 64 keys, both tables written alike, so two workers' pages of
//!   one batch often change a key from opposite sides without sharing a
//!   `P` or `M` cell: only the `J` lock orders them (with the denser
//!   shapes every page locks the same cells, which hides a missing `J`
//!   lock).
//!
//! - `racing` (half the cases): 40 keys, both tables written alike, 500 to
//!   900 ops in bursts of 600 to 900 paced from the first op, every
//!   definition live before the first write, at least 3 workers sealing
//!   every 20 to 40 ms, and every page stalled before its commit for up to
//!   2 to 3.5 s (the stall is skewed toward short, so that maximum keeps
//!   many pages open at once). Pages hold a few rows each and often share
//!   no `P` or `M` cell, so only the `J` and `T` locks order them: it is the
//!   only shape that has caught `join_no_j_lock` or `join_t_skip_lock`, now
//!   and then. A case takes about 15 s.
//!
//! The ops are issued as the concurrent tier issues them
//! (`generative::run::run_convergence_concurrent`): bursts of 40 to 600 ops,
//! each split over 2 to 4 lanes on their own connections, so writes to both
//! tables race into the same sealed batches and onto the same keys, against
//! 2 to 6 drain workers sealing every 20 to 200 ms. A burst past the
//! engine's split threshold can seal into one batch whose parts several
//! workers claim; the coverage report's `drain` line says how often that
//! happened. About half the cases also stall every page for a random time
//! of up to 5 to 40 ms between its last write and its commit
//! (`ConcurrentPlan::commit_stall`), so two pages of one key's opposite
//! sides overlap there, where only the `J` lock orders them. Every burst is checked
//! once it settles (against the oracle, then by a `Strict` `self_check` of
//! every definition, which audits the hidden `P`/`M`/`J`/`T` tables the
//! oracle cannot see, #1388), and at the end every definition must still be `live`
//! (a `JOIN` that halted would otherwise only show as a stale target).
//!
//! Like every property in this crate it is `#[ignore]`d: run it with
//! `cargo test -p generative --test join_convergence -- --ignored`
//! (`PROPTEST_CASES` sets the case count, 16 by default).
//!
//! # Builds over rows
//!
//! Outside the `racing` shape, a definition is installed up front or part way
//! through the stream, so most of those `JOIN`s build (per-side steps and the final `T` step, #1109) over tables
//! that hold rows, or that the lanes are still writing. Later bursts pause
//! and resume a definition (the resume rebuilds its target in place) and
//! re-read `ja` or `jb` (`request_backfill`), while the lanes keep writing
//! both tables. About a quarter of those cases first settle the up-front
//! definitions over the empty tables, as the tier did before it drew builds.
//! The oracle is the same per-burst comparison with the definition's own
//! query, so a build that lost or doubled a row diverges at the burst it ran
//! in, and a definition a build left paused, halted or quarantined fails the
//! end check. The coverage report's `builds` lines count, per run, the
//! installs, resumes and re-reads that fired over rows left by earlier
//! bursts (more rows than their own burst can delete, so the build certainly
//! read rows on both sides), the installs that fired in a burst whose own
//! writes were still landing, and the cases that drew a build over rows; a
//! run with none has drawn nothing worth reading. A last line reads the
//! engine's build-rows counter (`trellis_build_rows_total`) around each
//! passed case: the rows its builds wrote, the cases where they wrote any,
//! and the cases with a build over rows in the model where the engine's
//! builds wrote none (an `INNER` build with no matching key writes none, so
//! that count is a prompt to look, not a failure).
//!
//! Not drawn: partitioned sides, `DDL` between a build's steps, and
//! definitions that read another's target.
//!
//! The fast tests below (not ignored) run one hand-built case end to end,
//! and check that the oracle tells a corrupted target from a good one.

use generative::backend::{Backend, ConcurrentBackend, ManualBackend};
use generative::generate::join::{
    BuildCoverage, JoinActionDraw, JoinActionKind, JoinBuildDraws, JoinCase, JoinDefSpec,
    JoinField, JoinGroups, JoinKey, JoinOpDraw, JoinOpKind, KeyShape, build_coverage, join_case,
    join_case_strategy,
};
use generative::model::Op;
use generative::plant_sweep::{self, SweepTier};
use generative::run::{RunError, check_program, run_convergence_concurrent};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::{Config, Pool};

/// What the property's cases reached, printed when the binary exits.
#[derive(Default)]
struct JoinCoverage {
    cases: usize,
    shapes: std::collections::BTreeMap<&'static str, usize>,
    definitions: std::collections::BTreeMap<String, usize>,
    from_ops: usize,
    joined_ops: usize,
    truncates: usize,
    builds: BuildCoverage,
    /// Cases with at least one build over rows from earlier bursts.
    cases_building_over_rows: usize,
    /// Cases that settled their up-front definitions over empty tables.
    cases_settled: usize,
    /// Cases whose pages stalled before their commits.
    cases_commit_stalled: usize,
    /// Read back from the engine: cases whose run moved the build-rows
    /// counter, its total over the run, and the cases with a build over rows
    /// in the model whose run didn't move it.
    cases_engine_built: usize,
    engine_build_rows: u64,
    cases_over_rows_engine_silent: usize,
    coverage: generative::run::Coverage,
}

impl JoinCoverage {
    fn record(&mut self, case: &JoinCase) {
        self.cases += 1;
        *self.shapes.entry(case.shape.name()).or_insert(0) += 1;
        for def in &case.program.defs {
            let trellis::dev::defs::ast::KeySpace::Join(join) = &def.key_space else {
                unreachable!("the JOIN tier draws only JOIN definitions");
            };
            let groups: Vec<String> = join
                .group_by
                .iter()
                .map(|g| format!("{}.{}", g.table, g.column))
                .collect();
            let shape = format!(
                "{:?}/{}-column key/{}",
                join.kind,
                join.on.len(),
                groups.join("+")
            );
            *self.definitions.entry(shape).or_insert(0) += 1;
        }
        for op in &case.program.ops {
            let table = match op {
                Op::Insert { table, .. } | Op::Update { table, .. } | Op::Delete { table, .. } => {
                    table
                }
                Op::Truncate { table, .. } => {
                    self.truncates += 1;
                    table
                }
                other => unreachable!("the JOIN tier draws no {other:?}"),
            };
            if table == "jb" {
                self.joined_ops += 1;
            } else {
                self.from_ops += 1;
            }
        }
        let builds = build_coverage(&case.program, &case.plan);
        self.cases_building_over_rows += usize::from(builds.builds_over_rows() > 0);
        self.cases_settled += usize::from(case.plan.settle_after_install);
        self.cases_commit_stalled += usize::from(case.plan.commit_stall.is_some());
        self.builds.add(&builds);
        self.coverage.record_program(&case.program);
        self.coverage
            .record_concurrent_plan(&case.program, &case.plan);
    }

    /// Records what a passed run's engine says its builds wrote.
    fn record_engine_builds(&mut self, case: &JoinCase, rows: u64) {
        self.cases_engine_built += usize::from(rows > 0);
        self.engine_build_rows += rows;
        let over_rows = build_coverage(&case.program, &case.plan).builds_over_rows() > 0;
        self.cases_over_rows_engine_silent += usize::from(over_rows && rows == 0);
    }
}

/// The engine's `trellis_build_rows_total`, summed over its instances: the
/// group rows its builds' chunks and steps have written so far in this
/// process.
fn engine_build_rows() -> u64 {
    trellis::Metrics::new()
        .render_prometheus()
        .lines()
        .filter(|line| line.starts_with("trellis_build_rows_total"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum::<f64>() as u64
}

impl std::fmt::Display for JoinCoverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "cases: {}", self.cases)?;
        writeln!(f, "key shapes: {:?}", self.shapes)?;
        writeln!(f, "definitions: {:?}", self.definitions)?;
        writeln!(
            f,
            "ops: {} on ja, {} on jb, {} of them truncates",
            self.from_ops, self.joined_ops, self.truncates
        )?;
        writeln!(
            f,
            "commit stalls: {} of {} cases",
            self.cases_commit_stalled, self.cases
        )?;
        let b = &self.builds;
        writeln!(
            f,
            "builds: {} of {} cases build over rows from earlier bursts, {} settled their \
             up-front definitions first",
            self.cases_building_over_rows, self.cases, self.cases_settled
        )?;
        writeln!(
            f,
            "builds: installs after the first op {} ({} over rows, {} racing their burst's \
             writes)",
            b.deferred_installs, b.installs_over_rows, b.installs_racing
        )?;
        writeln!(
            f,
            "builds: pauses {}, resumes {} ({} over rows), re-reads {} of ja and {} of jb \
             ({} over rows)",
            b.pauses,
            b.resumes,
            b.resumes_over_rows,
            b.rebuilds_from,
            b.rebuilds_joined,
            b.rebuilds_over_rows
        )?;
        writeln!(
            f,
            "builds: the engine wrote {} build rows in {} passed cases; {} cases with a build \
             over rows in the model wrote none",
            self.engine_build_rows, self.cases_engine_built, self.cases_over_rows_engine_silent
        )?;
        write!(f, "{}", self.coverage)
    }
}

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<JoinCoverage>,
}

impl Drop for Harness {
    /// Print-only, and non-panicking: this can run during an unwind.
    fn drop(&mut self) {
        if self.coverage.borrow().cases > 0 {
            eprintln!(
                "generative: join_convergence coverage:\n{}",
                self.coverage.borrow()
            );
        }
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start().long_lived(),
        coverage: std::cell::RefCell::new(JoinCoverage::default()),
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

/// Runs one case against a fresh isolated database, then checks every
/// definition is still `live`.
fn run_case(case: &JoinCase) -> Result<(), String> {
    HARNESS.with(|h| {
        h.coverage.borrow_mut().record(case);
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_options(
                db.dsn(),
                case.workers,
                Some(std::time::Duration::from_millis(case.seal_interval_ms)),
            )
            .await
            .expect("connect the backend");
            backend.set_strict_audit(true);
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
            let shape = format!(
                "{} key shape, {} ops, burst_size {}, {} bursts, {} workers, seal every {}ms",
                case.shape.name(),
                case.program.ops.len(),
                case.burst_size,
                case.plan.bursts.len(),
                case.workers,
                case.seal_interval_ms
            );
            let build_rows_before = engine_build_rows();
            match run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan).await {
                Ok(run) => {
                    let build_rows = engine_build_rows().saturating_sub(build_rows_before);
                    let mut coverage = h.coverage.borrow_mut();
                    coverage.coverage.record_drain(&run.drain);
                    coverage.record_engine_builds(case, build_rows);
                    drop(coverage);
                    if !run.outcome.as_pass() {
                        return Err(format!("run did not pass ({shape}): {}", run.outcome));
                    }
                }
                Err(RunError::Diverged(d)) => {
                    return Err(format!(
                        "JOIN case diverged in the burst ending at op {} ({shape}):\n{}",
                        d.op_index,
                        d.all()
                            .map(|(target, report)| format!("target {target}:\n{report}"))
                            .collect::<Vec<_>>()
                            .join("\n")
                    ));
                }
                Err(RunError::Audit { burst, failures }) => {
                    return Err(format!(
                        "Strict self_check found a divergence the oracle did not, after burst \
                         {burst} ({shape}):\n{}",
                        failures.join("\n")
                    ));
                }
                Err(other) => return Err(format!("run error ({shape}): {other:?}")),
            }
            let statuses = backend.definition_statuses().await.expect("read statuses");
            if statuses.iter().any(|(_, status)| status != "live") {
                return Err(format!(
                    "a JOIN left `live` ({shape}): {statuses:?}\nprogram: {:#?}",
                    case.program
                ));
            }
            Ok(())
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// #1113: see the module doc comment.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_join_targets_converge_under_concurrent_drains(case in join_case_strategy()) {
        run_case(&case).map_err(TestCaseError::fail)?;
    }
}

/// The `JOIN` tier's sweep: the plants a `JOIN` page or build can reach are
/// gated (the `JOIN` plants and the group-basis writer's, which `P` and `M`
/// share); the others break paths the tier never takes.
///
/// Measured (4 seeds x 12 cases, logbooks `20261024T142312-b-sweep` and
/// `20261024T152844-b-sweep`): `join_drop_flip` caught in 34 of 48. Known
/// misses: `join_no_j_lock` and `join_t_skip_lock` fired in 41 of 48 and were
/// caught in 1 or 2, only in `racing` cases, and not reliably: the race needs
/// two pages of one key's opposite sides (or one `T` group) open at once with
/// no shared `P` or `M` cell, and whether a case lines them up depends on
/// timing, not only on the drawn case (repro `20261024T161704-b-repro`: the
/// catching cases caught in 9 and 5 of 10 runs), so a sweep misses either
/// plant often enough that gating them would fail at random. Neither more
/// groups nor a longer commit stall raised the rate (`20261024T164110-b-sweep`;
/// a stall of seconds more times the baseline out). A sweep that catches one
/// says so. `gb_early_zero_gc` fired in 0 of 48: it needs a rebuild whose basis
/// stamp is past the drained prefix while the GC pass runs, and the GC pass
/// runs every `housekeeping_interval` (5 s by default, which the harness
/// doesn't set). The deterministic pins named in `trellis/src/plant.rs` gate
/// all three.
fn join_sweep_tier() -> SweepTier {
    use generative::plant_sweep::{PlantPath, not_on_path};
    let mut not_gated: Vec<(&'static str, &'static str)> = vec![(
        "gb_lsn_not_visibility",
        "it fired in 0 of 48 cases: no build's basis caught a writer of its cells in \
         flight (the tier's writes are single statements); the concurrent tier's \
         steady-load and mid-burst tiers gate it",
    )];
    not_gated.extend(not_on_path(
        &[PlantPath::OneToOne, PlantPath::GroupBasis],
        "it breaks a 1-1 or plain group-basis aggregate path, and the JOIN tier \
             draws only JOIN definitions",
    ));
    SweepTier {
        name: generative::baseline_quarantine::JOIN_TIER,
        known_misses: &["join_no_j_lock", "join_t_skip_lock", "gb_early_zero_gc"],
        not_gated,
    }
}

/// #1114: shows the `JOIN` tier catches each planted bug a `JOIN` can reach
/// (`trellis::dev::plant::Plant`), and how often: the concurrent tier's
/// sweep (`generative::plant_sweep`, `concurrent_convergence.rs`'s
/// `planted_bugs_are_caught`) over [`join_case_strategy`]'s seeded cases.
/// Returns at once without `GENERATIVE_PLANTS`. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all cargo test -p generative --test join_convergence \
///     join_planted_bugs_are_caught -- --ignored --nocapture
/// ```
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn join_planted_bugs_are_caught() {
    let strategy = join_case_strategy().boxed();
    plant_sweep::sweep(
        "join_planted_bugs_are_caught",
        &join_sweep_tier(),
        |seed, cases| plant_sweep::seeded_cases(&strategy, seed, cases),
        |case: &JoinCase| {
            run_case(case).map_err(|message| plant_sweep::CaseFailure {
                // Every target is a `JOIN`.
                key_spaces: case
                    .program
                    .defs
                    .iter()
                    .map(|def| Some(def.key_space.clone()))
                    .collect(),
                message,
            })
        },
    );
}

/// Every `JOIN` plant and every group-basis writer plant is run by the tier
/// (gated or a known miss), except `gb_lsn_not_visibility`, which the tier
/// never reaches; nothing else is.
#[test]
fn the_join_tier_runs_every_join_plant() {
    use trellis::dev::plant::Plant;
    let tier = join_sweep_tier();
    for plant in Plant::ALL {
        let gated = !tier.not_gated.iter().any(|(name, _)| *name == plant.name())
            || plant.name() == "gb_lsn_not_visibility";
        let path = generative::plant_sweep::plant_path(*plant);
        assert_eq!(
            gated,
            matches!(
                path,
                generative::plant_sweep::PlantPath::Join
                    | generative::plant_sweep::PlantPath::GroupBasisWriter
            ),
            "{}",
            plant.name()
        );
    }
}

/// A small hand-built case: a `LEFT` S1 on `k` and an `INNER` S2 on `(t,
/// k)`, both tables written, rows moved between keys and groups, keys
/// matched and unmatched, each table truncated once mid-stream, over two
/// lanes and two workers. With `builds`, only `jb` is truncated (in the
/// second burst), and in the last burst, over rows that its deletes can't
/// remove, the S1 installs part way through while the S2 is paused and
/// resumed and both tables are re-read.
fn hand_built_case(with_builds: bool) -> JoinCase {
    let specs = [
        JoinDefSpec {
            left: true,
            key: JoinKey::Single,
            groups: JoinGroups::Joined,
            fields: vec![JoinField::SumX, JoinField::CountY, JoinField::CoalesceSumY],
        },
        JoinDefSpec {
            left: false,
            key: JoinKey::Pair,
            groups: JoinGroups::Both,
            fields: vec![JoinField::AvgX, JoinField::SumXPlusSumY],
        },
    ];
    let mut draws = Vec::new();
    for i in 0..48u8 {
        let joined = i % 3 == 0;
        let kind = match i % 5 {
            _ if i == 21 || (!with_builds && i == 34) => JoinOpKind::Truncate,
            0..=2 => JoinOpKind::Insert,
            3 => JoinOpKind::Update,
            _ => JoinOpKind::Delete,
        };
        draws.push(JoinOpDraw {
            joined,
            kind,
            pick: usize::from(i) * 7,
            key: i.wrapping_mul(37),
            t: i,
            group: (i % 7 != 0).then_some(i),
            value: (i % 11 != 0).then_some(i as i8 - 10),
            columns: i % 16,
        });
    }
    let action = |kind, target| JoinActionDraw {
        kind,
        burst: 1,
        target,
        start: 200,
        span: 400,
    };
    let builds = if with_builds {
        JoinBuildDraws {
            installs: vec![700, 0],
            actions: vec![
                action(JoinActionKind::PauseResume, 0),
                action(JoinActionKind::PauseResume, 1),
                action(JoinActionKind::RebuildFrom, 0),
                action(JoinActionKind::RebuildJoined, 0),
            ],
            settle_after_install: false,
        }
    } else {
        JoinBuildDraws {
            settle_after_install: true,
            ..JoinBuildDraws::default()
        }
    };
    join_case(&specs, KeyShape::Spread, &draws, &builds, 16, 2, 2, 50)
}

/// The empty-table path: both definitions install up front and settle over
/// the empty tables before the first write.
#[test]
fn a_hand_built_join_case_converges() {
    let case = hand_built_case(false);
    if let Err(message) = run_case(&case) {
        panic!("{message}");
    }
}

/// The build path: a definition installs over rows, then is paused and
/// resumed (rebuilt in place) and both tables are re-read while the lanes
/// write. The case must draw builds over rows, or it tests nothing.
#[test]
fn a_hand_built_join_case_builds_over_rows_and_converges() {
    let case = hand_built_case(true);
    let builds = build_coverage(&case.program, &case.plan);
    assert!(builds.installs_over_rows >= 1, "{builds:?}");
    assert!(builds.resumes_over_rows >= 1, "{builds:?}");
    assert!(
        builds.rebuilds_from >= 1 && builds.rebuilds_joined >= 1,
        "{builds:?}"
    );
    let build_rows_before = engine_build_rows();
    if let Err(message) = run_case(&case) {
        panic!("{message}");
    }
    // The counter is process-wide and this binary's tests run in parallel,
    // so this can't tell this case's builds from another test's; it does
    // catch builds that never write a row.
    assert!(engine_build_rows() > build_rows_before);
}

/// The oracle tells a corrupted `JOIN` target from a good one: the same
/// converged run, one target cell changed by hand, now diverges.
#[test]
fn the_join_oracle_catches_a_corrupted_target() {
    let case = hand_built_case(false);
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend =
                ManualBackend::connect_with_options(db.dsn(), 2, None)
                    .await
                    .expect("connect the backend");
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
            run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan)
                .await
                .expect("the hand-built case converges");
            let snapshot = backend.snapshot().await.expect("snapshot");
            assert_eq!(
                check_program(&pool, &case.program, &snapshot)
                    .await
                    .expect("check")
                    .len(),
                0,
                "converged"
            );
            backend
                .execute_raw("update public.j0 set n = n + 1 where gb is not distinct from (select min(gb) from public.j0)")
                .await
                .expect("corrupt the target");
            let snapshot = backend.snapshot().await.expect("snapshot");
            let diverged = check_program(&pool, &case.program, &snapshot)
                .await
                .expect("check");
            assert_eq!(
                diverged.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>(),
                ["j0"],
                "{diverged:?}"
            );
        })
    });
}

/// #1388: the `Strict` audit tells a damaged hidden table from a good one,
/// where the oracle (which reads the visible target) cannot: after a
/// converged run, `T`'s row counts are changed by hand. The target is as it
/// was, so the oracle still agrees, and the audit reports `T`.
#[test]
fn the_strict_audit_catches_a_corrupted_hidden_table() {
    let case = hand_built_case(false);
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_options(db.dsn(), 2, None)
                .await
                .expect("connect the backend");
            backend.set_strict_audit(true);
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
            run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan)
                .await
                .expect("the hand-built case converges and audits clean");
            assert!(
                backend.strict_audits_converged() > 0,
                "the run audited no target at any burst"
            );
            let targets: Vec<String> = case.program.defs.iter().map(|d| d.target.clone()).collect();
            let clean = backend.strict_audit(&targets).await.expect("audit");
            assert!(clean.is_empty(), "{clean:?}");
            backend
                .execute_raw(
                    "do $$ declare r record; begin \
                     for r in select schemaname, tablename from pg_tables \
                              where tablename ~ '^join_[0-9]+_t$' loop \
                       execute format('update %I.%I set __n = __n + 1', r.schemaname, r.tablename); \
                     end loop; end $$",
                )
                .await
                .expect("corrupt T");
            let snapshot = backend.snapshot().await.expect("snapshot");
            assert!(
                check_program(&pool, &case.program, &snapshot)
                    .await
                    .expect("check")
                    .is_empty(),
                "the oracle reads the visible target, which is untouched"
            );
            let failures = backend.strict_audit(&targets).await.expect("audit");
            for target in &targets {
                assert!(
                    failures.iter().any(|f| f.starts_with(&format!("{target}: "))),
                    "the audit missed {target}'s damaged T: {failures:?}"
                );
            }
        })
    });
}
