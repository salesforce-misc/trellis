//! The chained-reader tier (#1488): `JOIN`s (aggregate and 1-1) one of whose
//! sides is a plain 1-1 target of another definition in the same instance,
//! so the target-mutation seam (its image feed and write token) delivers
//! that side's changes rather than capture. Checked against each `JOIN`'s
//! query written over the upstream's query
//! (`generative::oracle::chained_relation`), once both have converged.
//!
//! # The property
//!
//! [`property_chained_join_targets_converge_under_concurrent_drains`] draws
//! `generate::join_chained::chained_case_strategy`: a case of the aggregate
//! `JOIN` tier (`join_convergence.rs`) or of the 1-1 `JOIN` tier
//! (`join_one_to_one_convergence.rs`), half each, over the same tables and op
//! stream, with one side read through an upstream:
//!
//! - the upstream (`ua` of `ja`, or `ub` of `jb`) passes every column through
//!   but one, which it computes as `<column> + n` or `COALESCE(<column>, n)`:
//!   the join key (`k`, or `t` beside a 1-1 `JOIN`'s chained `b`), so the
//!   computed column moves rows between keys, or the value a field reads;
//! - every `JOIN` reads the upstream's target in place of the table. A 1-1
//!   `JOIN`'s chained `b` is joined on columns that include its primary key
//!   (`ja.id = ub.id`, `ja.k = ub.id`, `ja.k = ub.id AND ja.t = ub.t1`), as
//!   define requires (#1482); a chained `a`, and either side of an aggregate
//!   `JOIN`, on any of its tier's `ON` shapes;
//! - the op stream is the tier's own (key moves, `NULL` keys, `TRUNCATE`s of
//!   either table now and then), plus updates of the upstream source's
//!   primary keys: every fourth to tenth update of that table moves its row
//!   to an id no live row holds, which the upstream sees as a delete and an
//!   insert;
//! - the bursts after the first pause and resume the upstream (the resume
//!   rebuilds it outside the seam, and so rebuilds the `JOIN`s that read it)
//!   and redefine its computed column (`ALTER TRANSFORM … ALTER`, a field
//!   build that rewrites every row through the seam), as well as the tier's
//!   own installs, pauses, resumes and re-reads (which may pick the
//!   upstream too, and a re-read of its source rebuilds it in place through
//!   the seam).
//!
//! The ops are issued as the concurrent tier issues them
//! (`generative::run::run_convergence_concurrent`), in short bursts (30 to
//! 150 ops; 260 to 600 for `wide`), each checked once it settles: every
//! target against its oracle (the upstream against its own query), then by
//! a `Strict` `self_check` of every definition. At the end every definition
//! must still be `live`. The `racing` key shape isn't drawn: its
//! seconds-long commit stalls don't fit short bursts.
//!
//! Not drawn: a chain of more than one hop, a side that is an aggregate or
//! `JOIN` target (`join_over_targets` and `group_basis_chained` pin those),
//! an upstream column paused on its own (S7 of
//! `join_one_to_one_over_targets`), and a held row (S8).
//!
//! Like every property in this crate it is `#[ignore]`d: run it with
//! `cargo test -p generative --test join_chained_convergence -- --ignored`
//! (`PROPTEST_CASES` sets the case count, 16 by default). The fast tests
//! below (not ignored) run a hand-built case of each kind end to end, and
//! check that the oracle reads the upstream's query rather than its target.

use generative::backend::{Backend, ManualBackend};
use generative::generate::join::{
    BuildCoverage, JoinActionDraw, JoinActionKind, JoinBuildDraws, JoinCase, JoinDefSpec,
    JoinField, JoinGroups, JoinKey, JoinOpDraw, JoinOpKind, KeyShape, build_coverage,
};
use generative::generate::join_chained::{
    ChainActionDraw, ChainActionKind, ChainSpec, Computed, Form, UPSTREAM_OF_FROM,
    UPSTREAM_OF_JOINED, aggregate_chained_case, chained_case_strategy, is_rekey,
    one_to_one_chained_case,
};
use generative::generate::join_one_to_one::{OnShape, OneToOneField, OneToOneOpDraw, OneToOneSpec};
use generative::model::{BurstAction, Op};
use generative::plant_sweep::{self, SweepTier};
use generative::run::{RunError, check_program, run_convergence_concurrent};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::defs::ast::{Expr, KeySpace};
use trellis::dev::pool::Pool;

/// What the property's cases reached, printed when the binary exits.
#[derive(Default)]
struct ChainedCoverage {
    cases: usize,
    /// `(kind, side the upstream feeds)`.
    chains: std::collections::BTreeMap<String, usize>,
    /// The upstream's computed column as drawn.
    computed: std::collections::BTreeMap<String, usize>,
    shapes: std::collections::BTreeMap<&'static str, usize>,
    upstream_ops: usize,
    rekeys: usize,
    upstream_truncates: usize,
    upstream_pauses: usize,
    alters: usize,
    builds: BuildCoverage,
    cases_building_over_rows: usize,
    coverage: generative::run::Coverage,
}

/// The upstream (the program's first definition) and the source it reads.
fn upstream(case: &JoinCase) -> (&str, &str) {
    let def = &case.program.defs[0];
    assert!(
        matches!(def.key_space, KeySpace::OneToOne),
        "the chained tier's first definition is its upstream"
    );
    (def.target.as_str(), def.source.as_str())
}

impl ChainedCoverage {
    fn record(&mut self, case: &JoinCase) {
        self.cases += 1;
        let (target, source) = upstream(case);
        let kind = match &case.program.defs[1].key_space {
            KeySpace::Join(_) => "aggregate",
            KeySpace::JoinOneToOne(_) => "1-1",
            other => unreachable!("the chained tier draws no {other:?} reader"),
        };
        let side = if target == UPSTREAM_OF_JOINED {
            "b"
        } else {
            "a"
        };
        *self.chains.entry(format!("{kind}/{side}")).or_insert(0) += 1;
        let computed = case.program.defs[0]
            .fields
            .iter()
            .find(|f| !matches!(f.expr, Expr::Column(_) | Expr::SourceColumn { .. }))
            .map_or("none".to_string(), |f| {
                let form = match f.expr {
                    Expr::FunctionCall { .. } => "coalesce",
                    _ => "plus",
                };
                format!("{}:{form}", f.name)
            });
        *self.computed.entry(computed).or_insert(0) += 1;
        *self.shapes.entry(case.shape.name()).or_insert(0) += 1;
        for op in &case.program.ops {
            if op.table() != source {
                continue;
            }
            self.upstream_ops += 1;
            self.rekeys += usize::from(is_rekey(op));
            self.upstream_truncates += usize::from(matches!(op, Op::Truncate { .. }));
        }
        for timed in case.plan.bursts.iter().flat_map(|b| &b.actions) {
            match &timed.action {
                BurstAction::Pause { target: t, .. } if t == target => self.upstream_pauses += 1,
                BurstAction::Alter { .. } => self.alters += 1,
                _ => {}
            }
        }
        let builds = build_coverage(&case.program, &case.plan);
        self.cases_building_over_rows += usize::from(builds.builds_over_rows() > 0);
        self.builds.add(&builds);
        self.coverage.record_program(&case.program);
        self.coverage
            .record_concurrent_plan(&case.program, &case.plan);
    }
}

impl std::fmt::Display for ChainedCoverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "cases: {}", self.cases)?;
        writeln!(f, "chains (kind/side): {:?}", self.chains)?;
        writeln!(f, "computed columns: {:?}", self.computed)?;
        writeln!(f, "key shapes: {:?}", self.shapes)?;
        writeln!(
            f,
            "upstream source: {} ops, {} primary-key updates, {} truncates",
            self.upstream_ops, self.rekeys, self.upstream_truncates
        )?;
        writeln!(
            f,
            "upstream: {} pauses (each with its RESUME), {} ALTERs",
            self.upstream_pauses, self.alters
        )?;
        let b = &self.builds;
        writeln!(
            f,
            "builds: {} of {} cases build over rows; installs after the first op {} ({} over \
             rows), pauses {}, resumes {} ({} over rows), re-reads {} of ja and {} of jb",
            self.cases_building_over_rows,
            self.cases,
            b.deferred_installs,
            b.installs_over_rows,
            b.pauses,
            b.resumes,
            b.resumes_over_rows,
            b.rebuilds_from,
            b.rebuilds_joined
        )?;
        write!(f, "{}", self.coverage)
    }
}

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<ChainedCoverage>,
}

impl Drop for Harness {
    /// Print-only, and non-panicking: this can run during an unwind.
    fn drop(&mut self) {
        if self.coverage.borrow().cases > 0 {
            eprintln!(
                "generative: join_chained_convergence coverage:\n{}",
                self.coverage.borrow()
            );
        }
    }
}

thread_local! {
    static HARNESS: Harness = {
        // The 1-1 JOIN is behind a dev knob until #1404; its chained cases
        // run under it, as the 1-1 JOIN tier's do.
        trellis::dev::defs::set_one_to_one_join(true);
        Harness {
            runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
            cluster: TestCluster::start().long_lived(),
            coverage: std::cell::RefCell::new(ChainedCoverage::default()),
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
                "{} key shape, upstream {}, {} ops, burst_size {}, {} bursts, {} workers, seal \
                 every {}ms",
                case.shape.name(),
                upstream(case).0,
                case.program.ops.len(),
                case.burst_size,
                case.plan.bursts.len(),
                case.workers,
                case.seal_interval_ms
            );
            match run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan).await {
                Ok(run) => {
                    h.coverage.borrow_mut().coverage.record_drain(&run.drain);
                    if !run.outcome.as_pass() {
                        return Err(format!("run did not pass ({shape}): {}", run.outcome));
                    }
                }
                Err(RunError::Diverged(d)) => {
                    return Err(format!(
                        "chained case diverged in the burst ending at op {} ({shape}):\n{}",
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
                         {burst} ({shape}):\n{}\nprogram: {:#?}",
                        failures.join("\n"),
                        case.program
                    ));
                }
                Err(other) => {
                    return Err(format!(
                        "run error ({shape}): {other:?}\nprogram: {:#?}\nplan: {:#?}",
                        case.program, case.plan
                    ));
                }
            }
            let statuses = backend.definition_statuses().await.expect("read statuses");
            if statuses.iter().any(|(_, status)| status != "live") {
                return Err(format!(
                    "a definition left `live` ({shape}): {statuses:?}\nprogram: {:#?}",
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
    fn property_chained_join_targets_converge_under_concurrent_drains(
        case in chained_case_strategy()
    ) {
        run_case(&case).map_err(TestCaseError::fail)?;
    }
}

/// The seam's plants this tier draws the shape of but catches rarely or not
/// at all; their deterministic pins gate them. Rates are caught and fired
/// in, of the 48 cases of seeds 1 to 4 (twelve each).
///
/// - `seam_token_early`: caught 0, fired 30. It needs two upstream pages of
///   one key from different batches, the newer one reading its token before
///   the older one and locking the key after the older one commits. Pages of
///   one key claim their batches in order, so that inversion is narrow;
///   `join_one_to_one_over_targets`'s
///   `s2_seam_rows_of_one_key_order_by_their_writers_commits` orders the two
///   pages by hand.
const CHAINED_KNOWN_MISSES: &[&str] = &["seam_token_early"];

/// The chained-reader tier's sweep: it gates the plants of the seam's feed
/// to a definition reading a target ([`generative::plant_sweep::PlantPath::Chain`])
/// but [`CHAINED_KNOWN_MISSES`] (of 48 cases, `seam_imageless_row` caught 21,
/// fired 39, first caught by the second to fourth case of each seed), and
/// leaves out every plant of another path, which the tier whose programs
/// take that path gates.
fn chained_sweep_tier() -> SweepTier {
    use generative::plant_sweep::{PlantPath, not_on_path};
    let not_gated = not_on_path(
        &[
            PlantPath::OneToOne,
            PlantPath::GroupBasis,
            PlantPath::GroupBasisWriter,
            PlantPath::Join,
            PlantPath::OneToOneJoin,
        ],
        "it breaks a definition's own apply path, which the concurrent, JOIN and 1-1 JOIN \
         tiers gate; this tier gates the seam's feed to a JOIN reading a target",
    )
    .collect();
    SweepTier {
        name: generative::baseline_quarantine::JOIN_CHAINED_TIER,
        known_misses: CHAINED_KNOWN_MISSES,
        not_gated,
    }
}

/// Shows the chained-reader tier catches each planted bug of the seam's
/// feed (`trellis::dev::plant::Plant`), and how often: the concurrent tier's
/// sweep (`generative::plant_sweep`) over [`chained_case_strategy`]'s seeded
/// cases. Returns at once without `GENERATIVE_PLANTS`. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all cargo test -p generative \
///     --test join_chained_convergence chained_join_planted_bugs_are_caught \
///     -- --ignored --nocapture
/// ```
///
/// Its three processes (the baseline and two plants) of 48 cases at about
/// 20 s a case take some 45 minutes.
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn chained_join_planted_bugs_are_caught() {
    let strategy = chained_case_strategy().boxed();
    plant_sweep::sweep(
        "chained_join_planted_bugs_are_caught",
        &chained_sweep_tier(),
        |seed, cases| plant_sweep::seeded_cases(&strategy, seed, cases),
        |case: &JoinCase| {
            run_case(case).map_err(|message| plant_sweep::CaseFailure {
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

/// The tier runs exactly the seam's plants, and its known misses are among
/// them.
#[test]
fn the_chained_tier_runs_every_chain_plant() {
    use generative::plant_sweep::{PlantPath, plant_path};
    use trellis::dev::plant::Plant;
    let tier = chained_sweep_tier();
    for plant in Plant::ALL {
        let name = plant.name();
        let gated = !tier.not_gated.iter().any(|(not, _)| *not == name);
        assert_eq!(gated, plant_path(*plant) == PlantPath::Chain, "{name}");
    }
    for name in tier.known_misses {
        let plant = Plant::from_name(name).unwrap_or_else(|| panic!("{name} names no plant"));
        assert_eq!(plant_path(plant), PlantPath::Chain, "{name}");
    }
}

/// The tier's actions for a hand-built case: in burst 1 a pause of the
/// upstream and its resume, in burst 2 an `ALTER` of it, in burst 3 another
/// pause, and the tier's own re-read of the upstream's source in burst 1.
fn chain_actions(alter: Form) -> Vec<ChainActionDraw> {
    vec![
        ChainActionDraw {
            kind: ChainActionKind::PauseResume,
            burst: 0,
            start: 200,
            span: 400,
        },
        ChainActionDraw {
            kind: ChainActionKind::Alter(alter),
            burst: 1,
            start: 300,
            span: 0,
        },
        ChainActionDraw {
            kind: ChainActionKind::PauseResume,
            burst: 2,
            start: 100,
            span: 1000,
        },
    ]
}

fn builds(rebuild: JoinActionKind) -> JoinBuildDraws {
    JoinBuildDraws {
        // The second `JOIN` installs half way through, over rows.
        installs: vec![0, 500],
        actions: vec![JoinActionDraw {
            kind: rebuild,
            burst: 0,
            target: 0,
            start: 500,
            span: 0,
        }],
        settle_after_install: false,
    }
}

/// An aggregate `JOIN` case over `ub`, the upstream of `jb` computing its
/// join key (`k + 1`, then `COALESCE(k, 0)`): a `LEFT` join on `k` grouped by
/// `ub.gb` and an `INNER` join on `(t, k)` grouped by both sides, over 64 ops
/// in bursts of 16 on two lanes, with `jb` rekeyed, its keys moved and
/// truncated once, and the upstream paused, resumed and altered.
fn hand_built_aggregate_case() -> JoinCase {
    let specs = [
        JoinDefSpec {
            left: true,
            key: JoinKey::Single,
            groups: JoinGroups::Joined,
            fields: vec![JoinField::SumY, JoinField::CountX],
        },
        JoinDefSpec {
            left: false,
            key: JoinKey::Pair,
            groups: JoinGroups::Both,
            fields: vec![JoinField::SumXPlusSumY, JoinField::AvgY],
        },
    ];
    let chain = ChainSpec {
        joined: true,
        computed: Computed {
            key: true,
            form: Form::Plus(1),
        },
        actions: chain_actions(Form::Coalesce(0)),
    };
    let mut draws = Vec::new();
    for i in 0..64u8 {
        let joined = i % 2 == 0;
        let kind = match i % 7 {
            _ if i == 38 && joined => JoinOpKind::Truncate,
            0..=2 => JoinOpKind::Insert,
            3 => JoinOpKind::Update,
            4 if joined => JoinOpKind::Rekey,
            4 | 5 => JoinOpKind::Update,
            _ => JoinOpKind::Delete,
        };
        draws.push(JoinOpDraw {
            joined,
            kind,
            pick: usize::from(i) * 5,
            key: if i % 13 == 6 {
                255
            } else {
                i.wrapping_mul(37) % 12
            },
            t: i,
            group: (i % 9 != 4).then_some(i % 4),
            value: (i % 11 != 0).then_some(i as i8 - 10),
            columns: i % 16,
        });
    }
    aggregate_chained_case(
        &specs,
        &chain,
        KeyShape::Spread,
        &draws,
        &builds(JoinActionKind::RebuildJoined),
        16,
        2,
        2,
        50,
    )
}

/// A 1-1 `JOIN` case over `ua`, the upstream of `ja` computing its join key
/// (`k + 1`, then `COALESCE(k, 2)`): a `LEFT` join on the non-key
/// `ua.k1 = jb.u` (admitted for an `a` side, #1482) and an `INNER` join on
/// `ua.id = jb.id`, with `ja` rekeyed, its keys moved and truncated once,
/// and the upstream paused, resumed and altered.
fn hand_built_one_to_one_case() -> JoinCase {
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
            on: OnShape::PkPk,
            fields: vec![OneToOneField::SumXy, OneToOneField::AX, OneToOneField::BW],
        },
    ];
    let chain = ChainSpec {
        joined: false,
        computed: Computed {
            key: true,
            form: Form::Plus(1),
        },
        actions: chain_actions(Form::Coalesce(2)),
    };
    let mut draws = Vec::new();
    for i in 0..64u8 {
        let joined = i % 3 == 0;
        let kind = match i % 5 {
            _ if i == 41 && !joined => JoinOpKind::Truncate,
            0..=2 => JoinOpKind::Insert,
            3 if !joined && i % 2 == 1 => JoinOpKind::Rekey,
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
    one_to_one_chained_case(
        &specs,
        &chain,
        KeyShape::Spread,
        &draws,
        &builds(JoinActionKind::RebuildFrom),
        16,
        2,
        2,
        50,
    )
}

/// What a hand-built case must draw, or it tests nothing.
fn assert_draws_the_chain(case: &JoinCase) {
    let (target, source) = upstream(case);
    let ops = &case.program.ops;
    assert!(
        ops.iter().any(|op| op.table() == source && is_rekey(op)),
        "a rekey"
    );
    assert!(
        ops.iter()
            .any(|op| op.table() == source && matches!(op, Op::Truncate { .. })),
        "a truncate"
    );
    let actions: Vec<&BurstAction> = case
        .plan
        .bursts
        .iter()
        .flat_map(|b| &b.actions)
        .map(|a| &a.action)
        .collect();
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, BurstAction::Resume { target: t, .. } if t == target)),
        "{actions:?}"
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, BurstAction::Alter { .. })),
        "{actions:?}"
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, BurstAction::Install { .. })),
        "{actions:?}"
    );
}

#[test]
fn a_hand_built_chained_aggregate_join_case_converges() {
    let case = hand_built_aggregate_case();
    assert_eq!(upstream(&case).0, UPSTREAM_OF_JOINED);
    assert_draws_the_chain(&case);
    if let Err(message) = run_case(&case) {
        panic!("{message}");
    }
}

#[test]
fn a_hand_built_chained_one_to_one_join_case_converges() {
    let case = hand_built_one_to_one_case();
    assert_eq!(upstream(&case).0, UPSTREAM_OF_FROM);
    assert_draws_the_chain(&case);
    if let Err(message) = run_case(&case) {
        panic!("{message}");
    }
}

/// The `JOIN`'s oracle reads the upstream's query, not its target: after a
/// converged run, the upstream's target changed by hand (a target isn't
/// captured, so nothing propagates it) in cells a `JOIN` reads makes the
/// upstream diverge and leaves the `JOIN`s agreeing; a cell of a `JOIN`
/// target changed by hand makes that `JOIN` diverge.
#[test]
fn the_chained_oracle_reads_the_upstreams_query() {
    let case = hand_built_aggregate_case();
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
            async fn diverged(
                backend: &mut ManualBackend,
                pool: &Pool,
                case: &JoinCase,
            ) -> Vec<String> {
                // The upstream as the last `ALTER` left it.
                let mut program = case.program.clone();
                if let Some(BurstAction::Alter { def, .. }) = case
                    .plan
                    .bursts
                    .iter()
                    .flat_map(|b| &b.actions)
                    .map(|a| &a.action)
                    .rfind(|a| matches!(a, BurstAction::Alter { .. }))
                {
                    program.defs[0] = (**def).clone();
                }
                let snapshot = backend.snapshot().await.expect("snapshot");
                check_program(pool, &program, &snapshot)
                    .await
                    .expect("check")
                    .into_iter()
                    .map(|(target, _)| target)
                    .collect()
            }
            assert!(
                diverged(&mut backend, &pool, &case).await.is_empty(),
                "converged"
            );
            // Every row of the upstream's target moved onto a join key of
            // `ja`, with a value `SUM(ub.y)` reads: cells `j0` reads, which
            // an oracle reading the target would see. (The run leaves no
            // row of `ja` matching one of `ub`, so a change of `y` alone
            // would reach nothing `j0` reads.)
            backend
                .execute_raw("create table public.ub_saved as select * from public.ub")
                .await
                .expect("save the upstream");
            backend
                .execute_raw(
                    "update public.ub set k1 = (select min(k) from ja), \
                                          y = coalesce(y, 0) + 1000",
                )
                .await
                .expect("corrupt the upstream");
            let joined: i64 = pool
                .get()
                .await
                .expect("a connection")
                .query_one(
                    "select count(*) from ja join public.ub on ja.k = ub.k1",
                    &[],
                )
                .await
                .expect("count the corrupted rows j0 reads")
                .get(0);
            assert!(joined > 0, "the corruption reaches no row j0 reads");
            assert_eq!(diverged(&mut backend, &pool, &case).await, ["ub"]);
            backend
                .execute_raw(
                    "update public.ub set k1 = s.k1, y = s.y from public.ub_saved s \
                     where s.id = ub.id",
                )
                .await
                .expect("restore the upstream");
            assert!(
                diverged(&mut backend, &pool, &case).await.is_empty(),
                "restored"
            );
            backend
                .execute_raw("update public.j0 set n = n + 1 where n > 0")
                .await
                .expect("corrupt a JOIN target");
            assert_eq!(diverged(&mut backend, &pool, &case).await, ["j0"]);
        })
    });
}
