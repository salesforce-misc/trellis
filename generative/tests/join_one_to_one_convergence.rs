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
//! `(c1, c2)`, a text key), with fields that read either table; and inserts,
//! updates and deletes over both tables, with the join key `NULL` now and then
//! or only the second column of the pair `NULL`. `jb`'s ON columns hold a
//! unique key, so the stream gives each live `jb` row a key of its own. The
//! key shapes, lanes, workers, commit stalls and `racing` shape are the
//! aggregate JOIN tier's (`join_convergence.rs`), with these differences:
//!
//! - `racing` is two cases in seven (the aggregate tier's half), and `fan_out`
//!   is drawn twice as often as the other shapes;
//! - every other shape but `wide` draws 120 to 600 ops in short bursts (30 to
//!   150 ops), each checked once it settles, with two to six pauses (each
//!   with its `RESUME`, a rebuild in place) and re-reads in the bursts after
//!   the first: the plants of a `TRUNCATE`, a rebuild's orphan pass and its
//!   `M` pass leave a target wrong only until later writes repair it, so the
//!   check has to come soon after;
//! - in three in four non-`racing` cases about one op in a hundred is a
//!   `TRUNCATE` of either table, and two to four more land in the last four
//!   ops of a burst;
//! - one join key in ten is `NULL` (the aggregate tier's one in forty): a
//!   row with no `J` row, which only its `R` entry lock orders;
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
//! the end every definition must still be `live`. While a burst runs, and once
//! it settles, the in-run invariant (`generative::invariant`) reads each
//! definition's hidden state under one snapshot and checks it against itself
//! and the tables as applied; `GENERATIVE_JOIN1_INVARIANT` turns it off or sets
//! its interval.
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
//! Not drawn: a side that is another transform's target (the chained-reader
//! tier, `join_chained_convergence.rs`, draws it over these programs; the
//! seam-fed families S1 to S9 of `join_one_to_one_over_targets` pin it), and
//! a value that makes a field fail, which holds the row in quarantine
//! (`join_one_to_one_quarantine` pins it).
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
    OnShape, OneToOneField, OneToOneOpDraw, OneToOneSpec, StarCase, one_to_one_case,
    one_to_one_case_strategy, star_case_strategy, star_tier_enabled,
};
use generative::invariant::{self, Clause, InRunInvariant};
use generative::model::Op;
use generative::plant_sweep::{self, SweepTier};
use generative::run::{
    RunError, check_program, run_convergence_concurrent, run_convergence_concurrent_sampled,
};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::pool::Pool;

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
    /// The in-run invariant's samples, over the passed cases.
    invariant: invariant::SampleStats,
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
            let on: Vec<String> = join.joins[0]
                .on
                .iter()
                .map(|on| {
                    format!(
                        "{}.{}={}.{}",
                        on.lhs.table, on.lhs.column, on.rhs.table, on.rhs.column
                    )
                })
                .collect();
            let shape = format!("{:?}/{}", join.joins[0].kind, on.join("&"));
            *self.definitions.entry(shape).or_insert(0) += 1;
        }
        for op in &case.program.ops {
            let (table, row) = match op {
                Op::Insert { table, row, .. } => (table, Some(row)),
                Op::Update { table, .. } | Op::Delete { table, .. } => (table, None),
                Op::Truncate { table, .. } => {
                    if table != "ja" {
                        self.truncates_joined += 1;
                    } else {
                        self.truncates_from += 1;
                    }
                    (table, None)
                }
                other => unreachable!("the 1-1 JOIN tier draws no {other:?}"),
            };
            if table != "ja" {
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
        writeln!(
            f,
            "in-run invariant: {} snapshots checked, {} skipped, {} R entries exempted by a \
             pending fan-out",
            self.invariant.checked, self.invariant.skipped, self.invariant.exempt
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
        max_shrink_time: generative::shrink::max_shrink_time_ms(),
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs one case against a fresh isolated database, then checks every
/// definition is still `live`.
fn run_case(case: &JoinCase) -> Result<(), String> {
    run_case_with(case, None)
}

/// [`run_case`] with each drain holding at most `drain_batch_cap` records,
/// when set.
fn run_case_with(case: &JoinCase, drain_batch_cap: Option<usize>) -> Result<(), String> {
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
            if let Some(cap) = drain_batch_cap {
                backend.set_drain_batch_cap(cap);
            }
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
            match run_convergence_concurrent_sampled(
                &mut backend,
                &pool,
                &case.program,
                &case.plan,
                InRunInvariant::from_env(),
            )
            .await
            {
                Ok(run) => {
                    let build_rows = engine_build_rows().saturating_sub(build_rows_before);
                    let mut coverage = h.coverage.borrow_mut();
                    coverage.coverage.record_drain(&run.drain);
                    coverage.invariant.add(&run.invariant);
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
                Err(RunError::InRunInvariant {
                    burst,
                    settled,
                    failure: Ok(failure),
                }) => {
                    let when = if settled {
                        "once it settled"
                    } else {
                        "mid-burst"
                    };
                    return Err(format!(
                        "the in-run hidden-state invariant broke in burst {burst}, {when} \
                         ({shape}), target {}:\n{}",
                        failure.target,
                        failure
                            .violations
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join("\n")
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

/// Runs one star tier case (`generate::join_one_to_one::star_case_strategy`)
/// as [`run_case`] runs a case, its drains capped at the case's page size,
/// and then, when it draws one, its metamorphic twin: the same program with
/// every definition's `JOIN` clauses permuted, which must converge to its
/// own oracle (the same rows: `join_star_tier.rs` checks the two oracles
/// agree). The knobs' shares of stalled lock steps and re-derived pages
/// await engine hooks that stall or re-derive a share of them; the case's
/// commit stall bounds every page's.
fn run_star_case(case: &StarCase) -> Result<(), String> {
    let cap = Some(case.knobs.drain_batch_cap);
    run_case_with(&case.case, cap)?;
    if case.knobs.metamorphic {
        let twin = JoinCase {
            program: case.permuted_program(),
            ..case.case.clone()
        };
        run_case_with(&twin, cap).map_err(|message| {
            format!(
                "the metamorphic twin (JOIN order {:?}) of a case that converged: {message}",
                case.join_order
            )
        })?;
    }
    Ok(())
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

    /// The star tier (see the module doc comment). Every case passes at once
    /// while `star_tier_enabled` is off.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_star_join_targets_converge_under_concurrent_drains(
        case in star_case_strategy()
    ) {
        if star_tier_enabled() {
            run_star_case(&case).map_err(TestCaseError::fail)?;
        }
    }
}

/// The `Join1*` plants this tier doesn't gate, each with why: the runner can't
/// draw the state it needs, or the target it leaves is repaired before a
/// burst settles. The deterministic pin named in `trellis/src/plant.rs` gates
/// each. Rates are of the 24 cases of seeds 1 to 4 (six cases each).
const ONE_TO_ONE_NOT_GATED: &[(&str, &str)] = &[
    (
        "star_j_unsorted",
        "it leaves the J statements unsorted, which only changes the order two pages queue \
         on J rows in: a deadlock, which the engine retries, never a wrong row; the unit \
         test u1_the_j_statements_are_one_insert_and_one_lock_sorted_by_position_and_key \
         pins it",
    ),
    (
        "join1_fan_out_held_row",
        "it needs a row held in quarantine (a field that fails on it), and the tier draws \
         no value that makes a field fail: a held row keeps a burst from converging; \
         `join_one_to_one_quarantine` pins it",
    ),
    (
        "join1_release_no_rederive",
        "it needs a held row released while a fan-out of its key is pending, and the tier \
         draws no held row; `join_one_to_one_quarantine` pins it",
    ),
    (
        "join1_m_pass_b_only",
        "it needs a b row deleted past capture before a rebuild in place (an uncaptured \
         write), and every write the runner issues is captured: it fired in 24 of 24 \
         cases (the M pass runs) and was caught in none; `join_one_to_one_build` pins it",
    ),
    (
        "join1_truncate_no_floor",
        "it needs a page from before a TRUNCATE drained after the truncate's page, which \
         the barrier never lets the driver do (the pin claims it out of order by hand): \
         it fired in 9 of 24 cases and was caught in none; \
         `join_one_to_one_interleavings` pins it",
    ),
];

/// Plants the tier draws the shape of but catches rarely or not at all. Each
/// is a narrow interleaving (a row moving keys between a read and its lock,
/// two writers of one row or key, a truncate racing a page or a rebuild's
/// start) or leaves a target that the rest of the build or the next writes
/// repair before the burst settles; its deterministic pin gates it. Rates are
/// caught and fired in, of 48 cases: the 24 of seeds 1 to 4 (six each), run
/// twice. A plant caught in one case in twelve is listed here too: a 24-case
/// sweep would miss it about one time in eight.
///
/// The in-run invariant (`generative::invariant`) reaches the plants that
/// leave hidden state wrong where the target can't show it, and shows no
/// measurable reach for the lost updates later writes repair. Over 4 × 12
/// sweeps (seeds 1 to 4, twelve cases each), caught of 48 with it off, then
/// with it on in two sweeps (in brackets, the cases one of its samples
/// caught first: in the second sweep each was a mid-burst sample; the first
/// sweep didn't record which):
///
/// - a join key with no `J` row: `join1_brd_no_key` 0; 1 (1), 3 (3);
///   `join1_rd_no_key` 0; 1 (1), 3 (3); `join1_chunk_no_key` 1 (1) in the
///   second sweep only.
/// - an `R` or `M` entry off its table as applied: `join1_lsn_only` 0;
///   1 (1), 1 (1); `join1_snapshot_after_read` 0; 1 (1), 1 (1).
/// - a lost update: `join1_no_j_lock` 4; 5 (1), 5 (2); `join1_vis_only` 3;
///   3 (0), 3 (1); the gated `join1_m_before_j` 8; 11 (5), 10 (3). Each
///   catch of `join1_no_j_lock` is one of the 15 `racing` cases. The
///   differences are within run-to-run noise (Fisher's exact test, p > 0.5
///   for each), and sweeps this size would show only a gain of about two and
///   a half times the rate or more. A sample catching first doesn't mean the
///   end-state check would have missed it: with the invariant off, the
///   oracle caught `join1_no_j_lock`'s case 1:8 and `join1_m_before_j`'s
///   case 1:12, which a sample catches first with it on.
/// - `join1_no_entry_lock` 0; 1 (0), 0; `join1_no_recheck` 0, 0;
///   `join1_truncate_keeps_m` 0, 0 (both measured with it on only).
///
/// None reaches the one case in five that gating takes (a 48-case sweep at
/// `join1_no_j_lock`'s rate with it on misses it about one time in 200), so
/// the list stands.
///
/// - `join1_no_j_lock`: caught 4, fired 48 (this tier's `racing` cases
///   don't cap their drains or pair their ops, which is what reaches the
///   aggregate tier's `join_no_j_lock`).
/// - `join1_truncate_no_rebuild`: caught 4, fired 14 (it needs a `TRUNCATE
///   jb` late in its burst, with no `jb` write after it on its keys).
/// - `join1_brd_no_key`: caught 1, fired 48.
/// - `join1_no_recheck`: caught 1, fired 48.
/// - `join1_vis_only`: caught 1, fired 48.
/// - `join1_chunk_no_key`: caught 0, fired 31.
/// - `join1_lsn_only`: caught 0, fired 48.
/// - `join1_no_entry_lock`: caught 0, fired 48.
/// - `join1_orphan_delete`: caught 0, fired 24.
/// - `join1_rd_no_key`: caught 0, fired 48.
/// - `join1_snapshot_after_read`: caught 0, fired 48.
/// - `join1_truncate_def_after_lock`: caught 0, fired 14 (its failure is a
///   deadlock, which its pin fails on directly: the rebuild start's lock read
///   errors, or the server log records it. The engine retries a deadlocked
///   transaction, and the tier checks only the targets).
/// - `join1_truncate_keeps_m`: caught 0, fired 14 (its pin checks the target
///   after an `a` insert drains and before the rebuild a `TRUNCATE b` starts
///   runs; the tier checks once the rebuild has rewritten the row).
/// - `join1_truncate_share_lock`: caught 0, fired 18.
const ONE_TO_ONE_KNOWN_MISSES: &[&str] = &[
    "join1_no_j_lock",
    "join1_truncate_no_rebuild",
    "join1_brd_no_key",
    "join1_no_recheck",
    "join1_vis_only",
    "join1_chunk_no_key",
    "join1_lsn_only",
    "join1_no_entry_lock",
    "join1_orphan_delete",
    "join1_rd_no_key",
    "join1_snapshot_after_read",
    "join1_truncate_def_after_lock",
    "join1_truncate_keeps_m",
    "join1_truncate_share_lock",
];

/// The 1-1 `JOIN` tier's sweep: it gates the `Join1*` plants it catches in
/// about one case in five or more (of 48: `join1_no_placeholder` 46,
/// `join1_old_key_unlocked` 44, `join1_no_orphan_pass` 24,
/// `join1_truncate_keeps_r` 10, `join1_m_before_j` 9), runs and reports
/// [`ONE_TO_ONE_KNOWN_MISSES`], and leaves out [`ONE_TO_ONE_NOT_GATED`] and
/// every plant of another path.
fn one_to_one_sweep_tier() -> SweepTier {
    sweep_tier(
        generative::baseline_quarantine::JOIN_ONE_TO_ONE_TIER,
        ONE_TO_ONE_KNOWN_MISSES,
    )
}

/// The star tier's sweep (#1781): the 1-1 `JOIN` plants over
/// [`star_case_strategy`]'s cases, each plant on any position. It leaves out
/// [`ONE_TO_ONE_NOT_GATED`] for the reasons listed there: the star tier runs
/// the same runner, which draws no held row, no uncaptured write and no
/// page claimed out of order, and `star_j_unsorted`'s failure is a deadlock
/// the engine retries. Until the reach sweep measures the star tier, its
/// known misses are [`STAR_KNOWN_MISSES`].
fn star_sweep_tier() -> SweepTier {
    sweep_tier(
        generative::baseline_quarantine::JOIN_STAR_TIER,
        STAR_KNOWN_MISSES,
    )
}

/// The star tier's known misses: the two-table tier's
/// ([`ONE_TO_ONE_KNOWN_MISSES`]), a placeholder no star sweep has measured
/// (no star case runs while `star_tier_enabled` is off), until the star
/// tier's reach sweep
/// (`star_join_planted_bugs_are_caught`, 4 seeds × 12 cases) records its
/// own fired and caught counts here.
const STAR_KNOWN_MISSES: &[&str] = ONE_TO_ONE_KNOWN_MISSES;

/// A 1-1 `JOIN` sweep tier named `name`: it gates every 1-1 `JOIN` plant but
/// `known_misses` and [`ONE_TO_ONE_NOT_GATED`], and leaves out every plant of
/// another path.
fn sweep_tier(name: &'static str, known_misses: &'static [&'static str]) -> SweepTier {
    use generative::plant_sweep::{PlantPath, not_on_path};
    let mut not_gated: Vec<(&'static str, &'static str)> = ONE_TO_ONE_NOT_GATED.to_vec();
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
        "it breaks a target's feed to a definition reading it, which only the \
         chained-reader tier (join_chained_convergence.rs) draws",
    ));
    not_gated.extend(not_on_path(
        &[PlantPath::Extremes, PlantPath::SelfCheckExtremes],
        generative::plant_sweep::NO_EXTREMES,
    ));
    SweepTier {
        name,
        known_misses,
        not_gated,
    }
}

/// Shows the 1-1 `JOIN` tier catches each planted bug a 1-1 `JOIN` can reach
/// (`trellis::dev::plant::Plant`), and how often: the concurrent tier's sweep
/// (`generative::plant_sweep`) over [`one_to_one_case_strategy`]'s seeded
/// cases. Returns at once without `GENERATIVE_PLANTS`. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all GENERATIVE_PLANT_CASES=6 cargo test -p generative \
///     --test join_one_to_one_convergence one_to_one_join_planted_bugs_are_caught \
///     -- --ignored --nocapture
/// ```
///
/// Six cases per seed (24 in all) catch every gated plant, and the sweep runs
/// its processes one after another: at the default twelve, its 20 processes
/// of about 13 s a case take some three and a half hours.
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

/// The star tier's reach sweep (#1781, #1464 section 4): the sweep above
/// over [`star_case_strategy`]'s seeded cases. Returns at once without
/// `GENERATIVE_PLANTS`, and while `star_tier_enabled` is off. Run it with:
///
/// ```text
/// TRELLIS_DEV_STAR_JOIN=1 GENERATIVE_PLANTS=all cargo test -p generative \
///     --test join_one_to_one_convergence star_join_planted_bugs_are_caught \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn star_join_planted_bugs_are_caught() {
    if !star_tier_enabled() {
        eprintln!("star_join_planted_bugs_are_caught: skipped, the star tier is off");
        return;
    }
    let strategy = star_case_strategy().boxed();
    plant_sweep::sweep(
        "star_join_planted_bugs_are_caught",
        &star_sweep_tier(),
        |seed, cases| plant_sweep::seeded_cases(&strategy, seed, cases),
        |case: &StarCase| {
            run_star_case(case).map_err(|message| plant_sweep::CaseFailure {
                key_spaces: case
                    .case
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

/// The tier runs every 1-1 `JOIN` plant but the ones it lists as not gated
/// with a reason, and nothing else; its known misses are 1-1 `JOIN` plants it
/// runs. The star tier's lists are the same.
#[test]
fn the_one_to_one_join_tier_runs_every_one_to_one_join_plant() {
    use generative::plant_sweep::{PlantPath, plant_path};
    use trellis::dev::plant::Plant;
    let star = star_sweep_tier();
    let tier = one_to_one_sweep_tier();
    assert_eq!(star.not_gated, tier.not_gated);
    assert_eq!(star.name, "join_star");
    for name in star.known_misses {
        let plant = Plant::from_name(name).unwrap_or_else(|| panic!("{name} names no plant"));
        assert_eq!(plant_path(plant), PlantPath::OneToOneJoin, "{name}");
    }
    for plant in Plant::ALL {
        let name = plant.name();
        let gated = !tier.not_gated.iter().any(|(not, _)| *not == name);
        let own_exception = ONE_TO_ONE_NOT_GATED.iter().any(|(not, _)| *not == name);
        assert_eq!(
            gated,
            plant_path(*plant) == PlantPath::OneToOneJoin && !own_exception,
            "{name}"
        );
    }
    for name in tier.known_misses {
        let plant = Plant::from_name(name).unwrap_or_else(|| panic!("{name} names no plant"));
        assert_eq!(plant_path(plant), PlantPath::OneToOneJoin, "{name}");
        assert!(
            !tier.not_gated.iter().any(|(not, _)| not == name),
            "{name} is both a known miss and not gated"
        );
    }
    for (name, _) in ONE_TO_ONE_NOT_GATED {
        let plant = Plant::from_name(name).unwrap_or_else(|| panic!("{name} names no plant"));
        assert_eq!(plant_path(plant), PlantPath::OneToOneJoin, "{name}");
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
            more: Vec::new(),
        },
        OneToOneSpec {
            left: false,
            on: OnShape::Composite,
            fields: vec![OneToOneField::SumXy, OneToOneField::AS, OneToOneField::BW],
            more: Vec::new(),
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
            position: u8::from(joined),
            kind,
            pick: usize::from(i) * 7,
            key: i.wrapping_mul(37) % 12,
            id: i,
            t: i,
            half_null: i % 9 == 4,
            value: (i % 11 != 0).then_some(i as i8 - 10),
            columns: i % 16,
            own_keys: [0; 3],
            on_key: false,
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
            assert!(
                check(&mut backend, &pool, &case).await.is_empty(),
                "converged"
            );
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
            assert!(
                check(&mut backend, &pool, &case).await.is_empty(),
                "restored"
            );
        })
    });
}

/// The `Strict` audit tells a damaged hidden ledger from a good one, where the
/// oracle (which reads the visible target) cannot: after a converged run,
/// every live `R` entry is deleted by hand. The target is as it was, so the
/// oracle still agrees, and the audit reports the damage.
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
            // Delete every live `R` entry: the ledger no longer matches `ja`.
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

/// The in-run invariant reads a hand-built case's hidden state clean while it
/// runs and once it settles, and tells each kind of damage to the settled
/// state from a good one: a target cell changed and put back, then every `J`
/// row, every live `M` entry and every live `R` entry deleted by hand, each
/// on top of the last.
#[test]
fn the_in_run_invariant_reads_a_run_clean_and_catches_damaged_hidden_state() {
    let case = hand_built_case(false);
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect_with_options(db.dsn(), 2, None)
                .await
                .expect("connect the backend");
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
            let run = run_convergence_concurrent_sampled(
                &mut backend,
                &pool,
                &case.program,
                &case.plan,
                Some(InRunInvariant {
                    interval: std::time::Duration::from_millis(5),
                }),
            )
            .await
            .expect("the hand-built case keeps the invariant");
            assert!(run.invariant.checked > 0, "{:?}", run.invariant);
            async fn clauses(pool: &Pool, target: &str) -> Vec<Clause> {
                let mut stats = invariant::SampleStats::default();
                let mut found: Vec<Clause> = invariant::sample(pool, target, &mut stats)
                    .await
                    .expect("sample")
                    .map(|f| f.violations.iter().map(|v| v.clause).collect())
                    .unwrap_or_default();
                assert_eq!(stats.checked, 1, "a settled live definition is checked");
                found.sort();
                found.dedup();
                found
            }
            // Runs `sql` with `%I.%I` naming each definition's hidden table
            // `part` (`r`, `m` or `j`), then checks `j0`.
            async fn damaged(
                backend: &mut ManualBackend,
                pool: &Pool,
                part: &str,
                sql: &str,
            ) -> Vec<Clause> {
                backend
                    .execute_raw(&format!(
                        "do $$ declare r record; begin \
                         for r in select schemaname, tablename from pg_tables \
                                  where tablename ~ '^join1_[0-9]+_{part}$' loop \
                           execute format('{sql}', r.schemaname, r.tablename); \
                         end loop; end $$"
                    ))
                    .await
                    .expect("damage a hidden table");
                clauses(pool, "j0").await
            }
            assert_eq!(clauses(&pool, "j0").await, []);
            assert_eq!(clauses(&pool, "j1").await, []);

            // A target cell.
            backend
                .execute_raw(
                    "update public.j0 set co_xy = co_xy::int + 1 \
                     where id = (select min(id) from public.j0)",
                )
                .await
                .expect("corrupt a cell");
            assert_eq!(clauses(&pool, "j0").await, [Clause::Target]);
            backend
                .execute_raw(
                    "update public.j0 set co_xy = co_xy::int - 1 \
                     where id = (select min(id) from public.j0)",
                )
                .await
                .expect("restore the cell");
            assert_eq!(clauses(&pool, "j0").await, []);

            // Every `J` row.
            assert_eq!(
                damaged(&mut backend, &pool, "j", "delete from %I.%I").await,
                [Clause::KeyRow]
            );
            // Every live `M` entry, on top: `M` no longer matches `jb`, and
            // the target rows joined to them no longer match `R` and `M`.
            assert_eq!(
                damaged(
                    &mut backend,
                    &pool,
                    "m",
                    "delete from %I.%I where not __tombstone"
                )
                .await,
                [Clause::JoinedEntry, Clause::Target, Clause::KeyRow]
            );
            // Every live `R` entry, on top: the same for `ja`, and every
            // target row is extra. No live entry is left to need a `J` row.
            assert_eq!(
                damaged(
                    &mut backend,
                    &pool,
                    "r",
                    "delete from %I.%I where not __tombstone"
                )
                .await,
                [Clause::FromEntry, Clause::JoinedEntry, Clause::Target]
            );
        })
    });
}
