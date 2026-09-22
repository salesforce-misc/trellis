//! Improvement-plan task E6: bulk operations (`TRUNCATE`, bulk insert) in the
//! generator's action space.
//!
//! **The load-bearing gotcha this file specifically guards against**:
//! Postgres's `TRUNCATE` command tag always reports `0` rows affected,
//! regardless of how many rows actually existed — `run_convergence`'s
//! classifier (`Ok(0) => AffectsNoRows`, `Ok(_) => Succeeds`) would
//! misclassify every non-empty truncate as a no-op if
//! `ManualBackend::apply`'s `Op::Truncate` arm ever trusted that raw command
//! tag instead of synthesizing a real count itself (a `SELECT count(*)` in
//! the same transaction, before the `TRUNCATE`). `truncating_a_nonempty_table_classifies_as_succeeds_not_affects_no_rows`
//! below is the test that would have caught getting this wrong.
//!
//! `Op::Truncate` is also drawn by the *default* strategy now (`trivial_program`'s
//! shared `mutate()` strategy, weighted rare — see `generate::strategy::mutate`'s
//! doc comment), so `tests/convergence.rs`'s existing
//! `property_convergence_holds_for_trivial_programs` property already exercises it
//! probabilistically end-to-end; this file adds the pins that pin down the
//! specific scenarios the task calls out directly, plus a dedicated property
//! for the bulk-insert row-count dimension.

use generative::backend::{Backend, ManualBackend};
use generative::generate::{Mutate, build_bulk_insert_program, build_program, bulk_insert_program};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use generative::run::{RunError, run_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::dev::defs::ast::ValueType;
use trellis::{Config, Pool};

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<generative::run::Coverage>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        eprintln!(
            "generative: bulk-operations run coverage:\n{}",
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

fn run_one(program: &Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        h.coverage.borrow_mut().record_program(program);
        h.runtime.block_on(async {
            let db = h.cluster.create_isolated_database().await;
            let mut backend = ManualBackend::connect(db.dsn())
                .await
                .expect("connect manual backend");
            // Issue #188: unique per-case slot/publication names, not the
            // shared `ClientOptions::default()` literals — see
            // `tests/convergence.rs`'s module doc comment for the
            // shared-cluster slot-collision this avoids.
            let unique = db.name().replace('-', "_");
            backend.set_slot_and_publication(format!("{unique}_slot"), format!("{unique}_pub"));
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

            match run_convergence(&mut backend, &pool, program).await {
                Ok(outcome) if outcome.as_pass() => Ok(()),
                Ok(outcome) => Err(TestCaseError::fail(format!("run did not pass: {outcome}"))),
                Err(RunError::Diverged(d)) => Err(TestCaseError::fail(format!(
                    "convergence diverged after op {} (target {}):\n{}",
                    d.op_index, d.def_target, d.report
                ))),
                Err(other) => Err(TestCaseError::fail(format!("run error: {other:?}"))),
            }
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// The bulk-insert row-count dimension, shrunk toward the smallest
    /// row count that still reproduces a failure (`generate::bulk_insert_program`'s
    /// `1..=MAX_BULK_INSERT_ROWS` via `prop_flat_map`, the same idiom
    /// `MAX_SEED_ROWS`/`MAX_MUTATES` already use).
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_convergence_holds_for_bulk_insert_programs(program in bulk_insert_program()) {
        run_one(&program)?;
    }
}

/// The specific gotcha this whole task is about: `TRUNCATE`'s Postgres
/// command tag always reports `0`, so `ManualBackend::apply`'s `Op::Truncate`
/// arm must synthesize the real affected-row count itself (a `SELECT
/// count(*)` before the `TRUNCATE`, in the same transaction) rather than
/// trusting the raw execute() return value — otherwise `run_convergence`'s
/// `Ok(0) => AffectsNoRows` classifier would misclassify every non-empty
/// truncate as a no-op, defeating "operation errors are checked, not
/// swallowed" for this op. This test drives `ManualBackend::apply` directly
/// (bypassing `run_convergence`) and re-applies the exact same classifier
/// inline, so it fails loudly if that synthesis is ever removed or broken.
#[tokio::test(flavor = "multi_thread")]
async fn truncating_a_nonempty_table_classifies_as_succeeds_not_affects_no_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut pool = NamePool::new();
    let table = Table::new(&mut pool, &[ValueType::Numeric]);
    let program = Program {
        tables: vec![table.clone()],
        relationships: Vec::new(),
        defs: Vec::new(),
        def_install_after_op: Vec::new(),
        ops: Vec::new(),
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    backend.install(&program).await.expect("install program");

    for pk in ["1", "2", "3"] {
        backend
            .apply(&Op::Insert {
                table: table.name.clone(),
                row: vec![
                    (table.pk_col.clone(), Some(pk.to_string())),
                    ("c1".to_string(), Some("1".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            })
            .await
            .expect("seed row");
    }

    let affected = backend
        .apply(&Op::Truncate {
            table: table.name.clone(),
            expect: OpOutcome::Succeeds,
        })
        .await
        .expect("truncate a non-empty table");

    // The exact classifier `run_convergence` applies to every op's `apply()`
    // return value.
    let actual = match affected {
        0 => OpOutcome::AffectsNoRows,
        _ => OpOutcome::Succeeds,
    };
    assert_eq!(
        actual,
        OpOutcome::Succeeds,
        "truncating a 3-row table must classify as Succeeds (affected = {affected}) — if this \
         is AffectsNoRows, ManualBackend::apply's Op::Truncate arm is trusting Postgres's \
         TRUNCATE command tag (always 0) instead of synthesizing a real pre-truncate count"
    );

    // And the mirror direction, so this file also pins the *other* side of
    // the classifier: truncating an already-empty table is a genuine no-op.
    let affected_again = backend
        .apply(&Op::Truncate {
            table: table.name.clone(),
            expect: OpOutcome::AffectsNoRows,
        })
        .await
        .expect("truncate an already-empty table");
    assert_eq!(
        affected_again, 0,
        "truncating an already-empty table must report 0 affected rows"
    );
}

/// Hand-built pin, end-to-end through `run_convergence`: seed three rows,
/// `TRUNCATE` the table (clearing every row and, via the engine's
/// TRUNCATE-propagation path — `docs/staging-and-claiming/truncate-propagation-spec.md`
/// — the maintained target too), then insert one more row afterward. The
/// whole sequence must converge, and the truncate op itself must be
/// classified `Succeeds` (checked via `Mutate::Truncate`'s own
/// `render_mutate` liveness bookkeeping, exercised end-to-end here rather
/// than unit-tested in isolation).
#[tokio::test(flavor = "multi_thread")]
async fn truncate_on_a_nonempty_tracked_table_converges_and_a_post_truncate_insert_still_works() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(1), Some(1)), (Some(2), Some(2)), (Some(3), Some(3))],
        &[Mutate::Truncate],
    );
    // ops: [insert, insert, insert, truncate].
    assert_eq!(program.ops.len(), 4, "sanity check on the fixture shape");
    assert_eq!(
        program.ops[3].expect(),
        &OpOutcome::Succeeds,
        "truncating a 3-row table must be predicted Succeeds by the generator's own liveness \
         simulation: {:#?}",
        program.ops[3]
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .unwrap_or_else(|e| panic!("truncate on a non-empty tracked table must converge: {e:?}"));
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    // A fresh insert at a pk truncate just cleared out must succeed and
    // converge too — proves the table (and its target) are genuinely empty
    // afterward, not just that the truncate op itself didn't error.
    backend
        .apply(&Op::Insert {
            table: program.tables[0].name.clone(),
            row: vec![
                (program.tables[0].pk_col.clone(), Some("1".to_string())),
                (
                    program.tables[0].columns[1].name.clone(),
                    Some("9".to_string()),
                ),
                (
                    program.tables[0].columns[2].name.clone(),
                    Some("9".to_string()),
                ),
                (program.tables[0].columns[3].name.clone(), None),
                (program.tables[0].columns[4].name.clone(), None),
                (program.tables[0].columns[5].name.clone(), None),
                (
                    program.tables[0].columns[6].name.clone(),
                    Some("0".to_string()),
                ),
            ],
            expect: OpOutcome::Succeeds,
        })
        .await
        .expect("post-truncate insert at a cleared pk must succeed");
    backend
        .quiesce()
        .await
        .expect("quiesce after post-truncate insert");

    let snapshot = backend.snapshot().await.expect("snapshot");
    let checked = generative::run::check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        checked.is_none(),
        "post-truncate insert must still converge onto the target: {checked:?}"
    );
}

/// Hand-built pin for the bulk-insert side: a single `Op::BulkInsert` well
/// past `MAX_SEED_ROWS`' scale, converging under both a `OneToOne` and an
/// `Aggregate` definition over the same source table at once
/// (`build_bulk_insert_program`'s own shape).
#[tokio::test(flavor = "multi_thread")]
async fn a_large_bulk_insert_converges_under_both_key_spaces() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_bulk_insert_program(250);
    assert_eq!(program.defs.len(), 2, "sanity check on the fixture shape");

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .unwrap_or_else(|e| panic!("a large bulk insert must converge: {e:?}"));
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}
