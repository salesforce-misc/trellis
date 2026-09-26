//! Issue #236 (layer-3 bucket 5, `docs/generative-test-suite.md` §7): the
//! backup-and-restore action. The cluster is backed up with a cold,
//! file-level copy right after op *k*, the program runs to its end, the
//! backup is restored into a fresh cluster, and ops *k+1..n* are replayed
//! against the restored database. The restore must be consistent with
//! itself: its source is exactly what the original's was at *k*, its targets
//! converge at *k* and after every replayed op, and no transform ever
//! pauses, since a cold copy brings the replication slot back with
//! everything else. See `generative::run::run_convergence_with_restore`.
//!
//! The `pg_basebackup` variant (no slot after the restore, so issue #310's
//! pause, a resume and a fresh backfill) is held for #558.
//!
//! Every hand-built pin owns its cluster. The property shares one per thread
//! like the other properties; the backup stops it, which is harmless to
//! earlier cases' leftovers since those cases are finished. Each case's
//! backup and restored cluster are deleted before the case returns, so a
//! deep run holds at most two extra copies of the cluster at a time.
//!
//! A restore starts a second cluster while the run still holds its first.
//! testkit draws restored clusters from their own permit pool (issue #569),
//! so these runs can go in parallel without four of them each holding one
//! cluster and waiting forever for another.

use generative::backend::ManualBackend;
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes,
    restore_plan_for, trivial_program,
};
use generative::model::{BackupKind, Op, Program, RestorePlan};
use generative::run::{RunError, run_convergence_with_restore};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
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
async fn run(cluster: &TestCluster, program: &Program, plan: &RestorePlan) -> Result<(), String> {
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let unique = db.name().replace('-', "_");
    backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    match run_convergence_with_restore(&mut backend, &pool, db.name(), program, plan, cluster).await
    {
        Ok(outcome) if outcome.as_pass() => Ok(()),
        Ok(outcome) => Err(format!("run did not pass: {outcome}")),
        Err(RunError::Diverged(d)) => Err(format!(
            "convergence diverged after op {} (target {}) with restore plan {plan:?}:\n{}",
            d.op_index, d.def_target, d.report
        )),
        Err(other) => Err(format!("run error with restore plan {plan:?}: {other:?}")),
    }
}

/// A program and a plan sized to it, drawn together so the plan shrinks
/// with the program.
fn program_with_restore_plan() -> impl Strategy<Value = (Program, RestorePlan)> {
    trivial_program().prop_flat_map(|program| {
        let plan = restore_plan_for(&program);
        (Just(program), plan)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// A cold-copy backup taken after any op restores into a database that
    /// is consistent with itself: it converges, keeps converging as the rest
    /// of the program replays against it, and never pauses a transform.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_a_cold_copy_restore_converges_without_a_pause((program, plan) in program_with_restore_plan()) {
        HARNESS.with(|h| {
            h.runtime
                .block_on(run(&h.cluster, &program, &plan))
                .map_err(TestCaseError::fail)
        })?;
    }
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
    program
}

fn cold_copy_at(backup_op: usize) -> RestorePlan {
    RestorePlan {
        backup_op,
        kind: BackupKind::ColdCopy,
    }
}

/// Runs [`one_to_one_and_aggregate_program`] backed up at `backup_op` on a
/// cluster of its own.
fn pin(backup_op: usize) {
    let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let cluster = TestCluster::start();
    runtime
        .block_on(run(
            &cluster,
            &one_to_one_and_aggregate_program(),
            &cold_copy_at(backup_op),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Backed up right after the first update, usually with it still in flight
/// (in the WAL ahead of the slot, or staged but not yet applied), then the
/// delete and the last update replay against the restore. The restored
/// engine finishes the update from wherever the backup caught it, streams
/// the replayed ops from the restored slot, and pauses nothing.
#[test]
fn a_restore_from_mid_program_replays_the_rest_and_converges() {
    pin(3);
}

/// Backed up with the very first insert in flight, before the engine has
/// applied anything, then the whole rest of the program replays.
#[test]
fn a_restore_from_the_first_op_replays_the_whole_program() {
    pin(0);
}

/// Backed up after the last op: nothing replays, so the restored database
/// has to converge on the backed-up in-flight work alone.
#[test]
fn a_restore_from_the_last_op_converges_with_nothing_to_replay() {
    pin(5);
}
