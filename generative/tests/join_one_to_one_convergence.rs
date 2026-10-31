//! The 1-1 JOIN tier (ADR-0021, #1392 step 9a): `FROM ja [LEFT | INNER] JOIN
//! jb ON … SELECT …` targets with no `GROUP BY`, one row per `ja` row, kept by
//! the engine from writes to both tables and checked against the definition's
//! own query over the two tables
//! (`generative::oracle::render_join_one_to_one_select`).
//!
//! # The property
//!
//! [`property_one_to_one_join_targets_converge_under_concurrent_drains`] draws
//! `generate::join_one_to_one::one_to_one_case_strategy`: one to three `LEFT`
//! or `INNER` definitions over `ja` and `jb`, joined on one of seven `ON`
//! shapes (`ja.id = jb.id`, `ja.k = jb.id`, `ja.k = jb.u`, two columns of a
//! primary key or a unique column with `t`, a composite unique key
//! `(c1, c2)`, a text key), with fields that read either table; and 120 to 900
//! inserts, updates and deletes over both tables, with the join key `NULL` now
//! and then or only the second column of the pair `NULL`, and about one op in a
//! hundred a `TRUNCATE` of either table. `jb`'s ON columns hold a unique key,
//! so the stream gives each live `jb` row a key of its own. The key shapes,
//! sizes, lanes, workers, commit stalls and `racing` shape are the aggregate
//! JOIN tier's (`join_convergence.rs`), with these differences:
//!
//! - all of a burst's `jb` ops are on one lane (`jb`'s key is unique, so two
//!   lanes could race two rows for a key one is about to free), and the `ja`
//!   ops are spread over the others;
//! - `fan_out` (three keys) gives every `jb` row many `ja` rows on its key: one
//!   `b` change fans out over many target rows;
//! - `ja` primary keys are drawn from 64 ids and `jb` primary keys from as many
//!   ids as there are keys, so an id comes back after a delete.
//!
//! The ops are issued as the concurrent tier issues them
//! (`generative::run::run_convergence_concurrent`). Every burst is checked once
//! it settles (against the oracle, then by a `Strict` `self_check` of every
//! definition, which audits the hidden ledgers the oracle cannot see), and at
//! the end every definition must still be `live`.
//!
//! The model of a correct target is the one the seam-fed families use
//! (`trellis/tests/join_one_to_one_over_targets.rs`): after each step the
//! pipeline settles and every target equals its query over the tables, and
//! `self_check` finds nothing. The families map to draw dimensions here:
//! `LEFT` and `INNER` are drawn per definition; `a` on its primary key (`PkPk`)
//! and on a plain column (every other shape) are the two `ON` forms of the
//! pins; `TRUNCATE` and `RESUME` (a pause, then a resume that rebuilds in
//! place) are drawn in the later bursts, as are re-reads of either table.
//!
//! Not drawn: a side that is another transform's target (the seam-fed
//! families S1 to S9 of `join_one_to_one_over_targets`; the draw dimension is
//! #1488), and a value that makes a field fail, which holds the row in
//! quarantine (`join_one_to_one_quarantine` pins it).
//!
//! Like every property in this crate it is `#[ignore]`d: run it with
//! `cargo test -p generative --test join_one_to_one_convergence -- --ignored`
//! (`PROPTEST_CASES` sets the case count, 16 by default).
//!
//! The fast tests below (not ignored) run hand-built cases end to end, and
//! check that the oracle and the `Strict` audit tell a damaged target or
//! ledger from a good one.

use generative::backend::{Backend, ConcurrentBackend, ManualBackend};
use generative::generate::join::{
    BuildCoverage, JoinActionDraw, JoinActionKind, JoinBuildDraws, JoinCase, JoinOpKind, KeyShape,
    build_coverage,
};
use generative::generate::join_one_to_one::{
    OnShape, OneToOneField, OneToOneOpDraw, OneToOneSpec, one_to_one_case,
    one_to_one_case_strategy,
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
struct OneToOneCoverage {
    cases: usize,
    shapes: std::collections::BTreeMap<&'static str, usize>,
    definitions: std::collections::BTreeMap<String, usize>,
    from_ops: usize,
    joined_ops: usize,
    truncates_from: usize,
    truncates_joined: usize,
    /// Inserts and key updates of `ja` that leave a `NULL` in one `ON` column
    /// (the key's, or only the pair's second column).
    null_keys: usize,
    builds: BuildCoverage,
    cases_building_over_rows: usize,
    cases_settled: usize,
    cases_commit_stalled: usize,
    cases_engine_built: usize,
    engine_build_rows: u64,
    cases_over_rows_engine_silent: usize,
    coverage: generative::run::Coverage,
}

impl OneToOneCoverage {
    fn record(&mut self, case: &JoinCase) {
        self.cases += 1;
        *self.shapes.entry(case.shape.name()).or_insert(0) += 1;
        for def in &case.program.defs {
            let trellis::dev::defs::ast::KeySpace::JoinOneToOne(join) = &def.key_space else {
                unreachable!("the 1-1 JOIN tier draws only 1-1 JOIN definitions");
            };
            let on: Vec<String> = join
                .on
                .iter()
                .map(|on| format!("{}.{}={}.{}", on.lhs.table, on.lhs.column, on.rhs.table, on.rhs.column))
                .collect();
            let shape = format!("{:?}/{}", join.kind, on.join("&"));
            *self.definitions.entry(shape).or_insert(0) += 1;
        }
        for op in &case.program.ops {
            let (table, row) = match op {
                Op::Insert { table, row, .. } => (table, Some(row)),
                Op::Update { table, .. } | Op::Delete { table, .. } => (table, None),
                Op::Truncate { table, .. } => {
                    if table == "jb" {
                        self.truncates_joined += 1;
                    } else {
                        self.truncates_from += 1;
                    }
                    (table, None)
                }
                other => unreachable!("the 1-1 JOIN tier draws no {other:?}"),
            };
            if table == "jb" {
                self.joined_ops += 1;
            } else {
                self.from_ops += 1;
            }
            if let Some(row) = row {
                let null = |column: &str| row.iter().any(|(c, v)| c == column && v.is_none());
                self.null_keys += usize::from(null("c1") || null("c2") || null("s"));
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

/// The engine's `trellis_build_rows_total`, summed over its instances.
fn engine_build_rows() -> u64 {
    trellis::Metrics::new()
        .render_prometheus()
        .lines()
        .filter(|line| line.starts_with("trellis_build_rows_total"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum::<f64>() as u64
}

impl std::fmt::Display for OneToOneCoverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "cases: {}", self.cases)?;
        writeln!(f, "key shapes: {:?}", self.shapes)?;
        writeln!(f, "definitions: {:?}", self.definitions)?;
        writeln!(
            f,
            "ops: {} on ja, {} on jb; truncates: {} of ja, {} of jb; inserts with a NULL \
             in an ON column: {}",
            self.from_ops,
            self.joined_ops,
            self.truncates_from,
            self.truncates_joined,
            self.null_keys
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
    coverage: std::cell::RefCell<OneToOneCoverage>,
}

impl Drop for Harness {
    /// Print-only, and non-panicking: this can run during an unwind.
    fn drop(&mut self) {
        if self.coverage.borrow().cases > 0 {
            eprintln!(
                "generative: join_one_to_one_convergence coverage:\n{}",
                self.coverage.borrow()
            );
        }
    }
}

thread_local! {
    static HARNESS: Harness = {
        // The 1-1 JOIN is behind a dev knob until #1404; this tier is its
        // generative gate.
        trellis::dev::defs::set_one_to_one_join(true);
        Harness {
            runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
            cluster: TestCluster::start().long_lived(),
            coverage: std::cell::RefCell::new(OneToOneCoverage::default()),
        }
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
                        "1-1 JOIN case diverged in the burst ending at op {} ({shape}):\n{}",
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
                    "a 1-1 JOIN left `live` ({shape}): {statuses:?}\nprogram: {:#?}",
                    case.program
                ));
            }
            Ok(())
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// See the module doc comment.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_one_to_one_join_targets_converge_under_concurrent_drains(
        case in one_to_one_case_strategy()
    ) {
        run_case(&case).map_err(TestCaseError::fail)?;
    }
}

/// The 1-1 `JOIN` tier's sweep: the plants a 1-1 `JOIN` page, build or
/// truncate can reach are gated (the `Join1*` plants).
fn one_to_one_sweep_tier() -> SweepTier {
    use generative::plant_sweep::{PlantPath, not_on_path};
    let mut not_gated: Vec<(&'static str, &'static str)> = Vec::new();
    not_gated.extend(not_on_path(
        &[PlantPath::OneToOne, PlantPath::GroupBasis],
        "it breaks a 1-1 or plain group-basis aggregate path, and the 1-1 JOIN tier \
         draws only 1-1 JOIN definitions",
    ));
    not_gated.extend(not_on_path(
        &[PlantPath::Join],
        "it breaks the aggregate JOIN path, and the 1-1 JOIN tier draws only 1-1 JOIN \
         definitions",
    ));
    not_gated.extend(not_on_path(
        &[PlantPath::GroupBasisWriter],
        "it breaks the group-basis writer's statements, which a 1-1 JOIN doesn't use",
    ));
    not_gated.extend(not_on_path(
        &[PlantPath::Chain],
        "it breaks a target's feed to a definition reading it, and no tier draws a \
         chain in one instance until #1488; `join_one_to_one_over_targets` pins it",
    ));
    SweepTier {
        name: generative::baseline_quarantine::JOIN_ONE_TO_ONE_TIER,
        known_misses: &[],
        not_gated,
    }
}

/// Shows the 1-1 `JOIN` tier catches each planted bug a 1-1 `JOIN` can reach
/// (`trellis::dev::plant::Plant`), and how often: the concurrent tier's sweep
/// (`generative::plant_sweep`) over [`one_to_one_case_strategy`]'s seeded
/// cases. Returns at once without `GENERATIVE_PLANTS`. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all cargo test -p generative --test join_one_to_one_convergence \
///     one_to_one_join_planted_bugs_are_caught -- --ignored --nocapture
/// ```
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn one_to_one_join_planted_bugs_are_caught() {
    let strategy = one_to_one_case_strategy().boxed();
    plant_sweep::sweep(
        "one_to_one_join_planted_bugs_are_caught",
        &one_to_one_sweep_tier(),
        |seed, cases| plant_sweep::seeded_cases(&strategy, seed, cases),
        |case: &JoinCase| {
            run_case(case).map_err(|message| plant_sweep::CaseFailure {
                // Every target is a 1-1 `JOIN`.
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

/// Every 1-1 `JOIN` plant is run by the tier (gated); nothing else is.
#[test]
fn the_one_to_one_join_tier_runs_every_one_to_one_join_plant() {
    use trellis::dev::plant::Plant;
    let tier = one_to_one_sweep_tier();
    for plant in Plant::ALL {
        let gated = !tier.not_gated.iter().any(|(name, _)| *name == plant.name());
        let path = generative::plant_sweep::plant_path(*plant);
        assert_eq!(
            gated,
            path == generative::plant_sweep::PlantPath::OneToOneJoin,
            "{}",
            plant.name()
        );
    }
}

/// A small hand-built case: a `LEFT` join on `ja.k = jb.u` and an `INNER` join
/// on the composite `(c1, c2)`, both tables written, rows moved between keys
/// and onto `NULL` keys, keys matched and unmatched, each table truncated once
/// mid-stream, over two lanes and two workers. With `builds`, only `jb` is
/// truncated (in the second burst), and in the last burst, over rows that its
/// deletes can't remove, the `LEFT` installs part way through while the
/// `INNER` is paused and resumed and both tables are re-read.
fn hand_built_case(with_builds: bool) -> JoinCase {
    let specs = [
        OneToOneSpec {
            left: true,
            on: OnShape::KeyUnique,
            fields: vec![
                OneToOneField::BId,
                OneToOneField::CoalesceXy,
                OneToOneField::CaseW,
            ],
        },
        OneToOneSpec {
            left: false,
            on: OnShape::Composite,
            fields: vec![OneToOneField::SumXy, OneToOneField::AS, OneToOneField::BW],
        },
    ];
    let mut draws = Vec::new();
    for i in 0..64u8 {
        let joined = i % 3 == 0;
        let kind = match i % 5 {
            _ if i == 18 || (!with_builds && i == 47) => JoinOpKind::Truncate,
            0..=2 => JoinOpKind::Insert,
            3 => JoinOpKind::Update,
            _ => JoinOpKind::Delete,
        };
        draws.push(OneToOneOpDraw {
            joined,
            kind,
            pick: usize::from(i) * 7,
            key: i.wrapping_mul(37) % 12,
            id: i,
            t: i,
            half_null: i % 9 == 4,
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
    one_to_one_case(&specs, KeyShape::Spread, &draws, &builds, 16, 2, 2, 50)
}

/// The empty-table path: both definitions install up front and settle over
/// the empty tables before the first write.
#[test]
fn a_hand_built_one_to_one_join_case_converges() {
    let case = hand_built_case(false);
    if let Err(message) = run_case(&case) {
        panic!("{message}");
    }
}

/// The build path: a definition installs over rows, then is paused and
/// resumed (rebuilt in place) and both tables are re-read while the lanes
/// write. The case must draw builds over rows, or it tests nothing.
#[test]
fn a_hand_built_one_to_one_join_case_builds_over_rows_and_converges() {
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

/// The oracle tells a corrupted 1-1 `JOIN` target from a good one: the same
/// converged run, one target cell changed by hand, now diverges.
#[test]
fn the_one_to_one_join_oracle_catches_a_corrupted_target() {
    let case = hand_built_case(false);
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_options(db.dsn(), 2, None)
                .await
                .expect("connect the backend");
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
            run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan)
                .await
                .expect("the hand-built case converges");
            async fn check(
                backend: &mut ManualBackend,
                pool: &Pool,
                case: &JoinCase,
            ) -> Vec<String> {
                let snapshot = backend.snapshot().await.expect("snapshot");
                check_program(pool, &case.program, &snapshot)
                    .await
                    .expect("check")
                    .into_iter()
                    .map(|(target, _)| target)
                    .collect()
            }
            assert!(check(&mut backend, &pool, &case).await.is_empty(), "converged");
            // A cell changed.
            backend
                .execute_raw(
                    "update public.j0 set co_xy = co_xy::int + 1 \
                     where id = (select min(id) from public.j0)",
                )
                .await
                .expect("corrupt a cell");
            assert_eq!(check(&mut backend, &pool, &case).await, ["j0"]);
            // Put back, the target agrees again.
            backend
                .execute_raw(
                    "update public.j0 set co_xy = co_xy::int - 1 \
                     where id = (select min(id) from public.j0)",
                )
                .await
                .expect("restore the cell");
            assert!(check(&mut backend, &pool, &case).await.is_empty(), "restored");
        })
    });
}

/// The `Strict` audit tells a damaged hidden ledger from a good one, where the
/// oracle (which reads the visible target) cannot: after a converged run, the
/// `R` ledger's join key column is changed by hand. The target is as it was,
/// so the oracle still agrees, and the audit reports the damage.
#[test]
fn the_strict_audit_catches_a_corrupted_one_to_one_join_ledger() {
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
            // Drop every live `R` entry's applied position: the entry no
            // longer matches `ja`.
            let tables = backend
                .execute_raw(
                    "do $$ declare r record; begin \
                     for r in select schemaname, tablename from pg_tables \
                              where tablename ~ '^join1_[0-9]+_r$' loop \
                       execute format('delete from %I.%I where not __tombstone', \
                                      r.schemaname, r.tablename); \
                     end loop; end $$",
                )
                .await;
            tables.expect("damage R");
            let snapshot = backend.snapshot().await.expect("snapshot");
            assert!(
                check_program(&pool, &case.program, &snapshot)
                    .await
                    .expect("check")
                    .is_empty(),
                "the oracle reads the visible target, which is untouched"
            );
            let failures = backend.strict_audit(&targets).await.expect("audit");
            assert!(!failures.is_empty(), "the audit missed the damaged R");
        })
    });
}
