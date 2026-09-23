//! Issue #236 (layer-3 bucket 5, `docs/generative-test-suite.md` §7):
//! database-administration actions interleaved with a program's ops —
//! `CHECKPOINT`, a Postgres restart (fast, and immediate so the next start
//! runs crash recovery), and losing the replication slot (dropped, or
//! invalidated by the server). Each is an oracle identity, so every op is
//! still judged by the unchanged recompute oracle at quiescence; see
//! `generative::run::run_convergence_with_db_admin` for what each action
//! demands of the engine, and the extra detection check a slot loss gets.
//!
//! Every hand-built pin owns its cluster, because a restart severs every
//! connection on it. The property shares one per thread like the other
//! properties, restarting it under earlier cases' leftovers, which is
//! harmless: those cases are finished.

use generative::backend::ManualBackend;
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes,
    db_admin_plan_for, trivial_program,
};
use generative::model::{
    DbAdminAction, DbAdminEvent, DbAdminPlan, Op, Program, RestartMode, SlotLossKind,
};
use generative::run::{RunError, run_convergence_with_db_admin};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{
    Config as ProptestConfig, FileFailurePersistence, TestCaseError, TestRunner,
};
use testkit::TestCluster;
use trellis::{Config, Pool};

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start(),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs `program` with `plan` against a fresh database on `cluster`, with
/// slot and publication names unique to that database (issue #188).
async fn run(cluster: &TestCluster, program: &Program, plan: &DbAdminPlan) -> Result<(), String> {
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let unique = db.name().replace('-', "_");
    backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    match run_convergence_with_db_admin(&mut backend, &pool, program, plan, cluster).await {
        Ok(outcome) if outcome.as_pass() => Ok(()),
        Ok(outcome) => Err(format!("run did not pass: {outcome}")),
        Err(RunError::Diverged(d)) => Err(format!(
            "convergence diverged after op {} (target {}) with db-admin plan {plan:?}:\n{}",
            d.op_index, d.def_target, d.report
        )),
        Err(other) => Err(format!("run error with db-admin plan {plan:?}: {other:?}")),
    }
}

/// A program and a plan sized to it, drawn together so the plan shrinks
/// with the program.
fn program_with_db_admin_plan() -> impl Strategy<Value = (Program, DbAdminPlan)> {
    trivial_program().prop_flat_map(|program| {
        let plan = db_admin_plan_for(&program);
        (Just(program), plan)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// Checkpoints, Postgres restarts and slot losses anywhere in a
    /// generated program never perturb convergence, and a slot loss always
    /// pauses every transform before the operator resumes them.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_db_admin_actions_never_perturb_convergence((program, plan) in program_with_db_admin_plan()) {
        HARNESS.with(|h| {
            h.runtime
                .block_on(run(&h.cluster, &program, &plan))
                .map_err(TestCaseError::fail)
        })?;
    }
}

/// No database: every drawn plan is anchored inside its program, holds at
/// most three events, anchors a slot loss only on an insert or update
/// (issue #330, see `db_admin_plan_for`), and across enough draws every
/// action shows up, so none silently drops out of the sweep.
#[test]
fn drawn_plans_stay_in_bounds_and_cover_every_action() {
    let mut runner = TestRunner::default();
    let strategy = program_with_db_admin_plan();
    let (mut checkpoint, mut fast, mut immediate, mut dropped, mut invalidated) =
        (false, false, false, false, false);
    for _ in 0..300 {
        let (program, plan) = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        assert!(plan.events.len() <= 3, "{plan:?}");
        for event in &plan.events {
            assert!(event.op < program.ops.len(), "{event:?} past the last op");
            match &event.action {
                DbAdminAction::Checkpoint => checkpoint = true,
                DbAdminAction::RestartPostgres(RestartMode::Fast) => fast = true,
                DbAdminAction::RestartPostgres(RestartMode::Immediate) => immediate = true,
                DbAdminAction::LoseSlot(kind) => {
                    assert!(
                        matches!(
                            program.ops[event.op],
                            Op::Insert { .. } | Op::BulkInsert { .. } | Op::Update { .. }
                        ),
                        "slot loss anchored on {:?}",
                        program.ops[event.op]
                    );
                    match kind {
                        SlotLossKind::Dropped => dropped = true,
                        SlotLossKind::Invalidated => invalidated = true,
                    }
                }
            }
        }
    }
    assert!(
        checkpoint && fast && immediate && dropped && invalidated,
        "checkpoint={checkpoint} fast={fast} immediate={immediate} dropped={dropped} \
         invalidated={invalidated}"
    );
}

/// One source table with a 1-1 and an aggregate transform over it:
///
/// - ops 0-2 seed pks 1-3 (grains 0, 0, 1)
/// - op 3 updates pk 1
/// - op 4 deletes pk 2
/// - op 5 updates pk 3
fn one_to_one_and_aggregate_program() -> Program {
    let program = build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![
                (Some(10), Some(1)),
                (Some(20), Some(2)),
                (Some(30), Some(3)),
            ],
            text_values: vec![None, None, None],
            bool_values: vec![None, None, None],
            uuid_values: vec![None, None, None],
            grain_values: vec![
                Some("0".to_string()),
                Some("0".to_string()),
                Some("1".to_string()),
            ],
            rel_fk_values: vec![None, None, None],
            mutates: vec![
                Mutate::Update {
                    pk: 1,
                    c1: Some(100),
                    c2: Some(5),
                },
                Mutate::Delete { pk: 2 },
                Mutate::Update {
                    pk: 3,
                    c1: Some(7),
                    c2: None,
                },
            ],
        }],
        &[
            (0, DefShape::OneToOne),
            (
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Sum(AggregateColumn::C1), AggregateFn::Count],
                },
            ),
        ],
    );
    assert_eq!(
        program.ops.len(),
        6,
        "3 seed inserts + 2 updates + 1 delete"
    );
    assert!(matches!(program.ops[3], Op::Update { .. }));
    assert!(matches!(program.ops[4], Op::Delete { .. }));
    assert!(matches!(program.ops[5], Op::Update { .. }));
    program
}

fn plan(events: &[(usize, DbAdminAction)]) -> DbAdminPlan {
    DbAdminPlan {
        events: events
            .iter()
            .map(|(op, action)| DbAdminEvent {
                op: *op,
                action: action.clone(),
            })
            .collect(),
    }
}

/// Postgres restarts with each op's change still in flight: a fast restart
/// after a seed insert, an immediate one (crash recovery on the next start)
/// after the delete, and a checkpoint after the last update. The engine
/// reconnects on its own every time: intake through its supervisor, the
/// pool by recycling, and the harness never touches the engine client.
#[tokio::test(flavor = "multi_thread")]
async fn postgres_restarts_with_work_in_flight_converge() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = plan(&[
        (1, DbAdminAction::RestartPostgres(RestartMode::Fast)),
        (4, DbAdminAction::RestartPostgres(RestartMode::Immediate)),
        (5, DbAdminAction::Checkpoint),
    ]);
    run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Issue #310's recovery, driven through the generative harness: the slot
/// is dropped while the engine is stopped and an update lands in the gap.
/// The restart must pause both transforms (the run checks this before
/// resuming), and the operator's `RESUME` must rebuild both targets,
/// including the gap's update, to the oracle.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_slot_pauses_every_transform_and_resuming_converges() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = plan(&[(3, DbAdminAction::LoseSlot(SlotLossKind::Dropped))]);
    run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The same with the slot invalidated by the server (`wal_status = 'lost'`)
/// rather than dropped, which takes #310's other recovery path: the dead
/// slot is dropped before a fresh one is created.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalidated_slot_pauses_every_transform_and_resuming_converges() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = plan(&[(5, DbAdminAction::LoseSlot(SlotLossKind::Invalidated))]);
    run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// A slot loss whose gap deletes a source row. The resume's rebuild only
/// visits source rows that still exist, so the deleted row's 1-1 target
/// row survives and the run diverges. That is issue #330; un-ignore this
/// once it's fixed, and widen `db_admin_plan_for` to anchor a slot loss on
/// any op.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #330: RESUME keeps target rows the gap deleted from the source"]
async fn a_slot_loss_whose_gap_deletes_a_row_converges_after_resume() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = plan(&[(4, DbAdminAction::LoseSlot(SlotLossKind::Dropped))]);
    run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}
