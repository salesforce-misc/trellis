//! Improvement-plan task E2 (rescoped): definition lifecycle — installing a
//! new [`TransformDef`] mid-stream, after some ops have already run against
//! its source table, rather than every definition installing up front
//! alongside every table (`generative::run::run_convergence`'s only behavior
//! before this task).
//!
//! **Explicitly out of scope, per the rescoping** (see
//! `local_docs/generative-suite-improvement-plan.md`'s workstream E and this
//! branch's own task description): "redefine" (altering an existing
//! definition) and "remove" (dropping one) are not attempted here.
//! `defs::catalog` has no drop/alter-definition API today — only
//! `create_definition`/`create_definition_without_backfill`/`install_definition`
//! — so that half of definition lifecycle is blocked on missing engine
//! functionality, a deferred follow-up, not something this file works around.
//!
//! Reuses the shared-cluster/isolated-database-per-case `Harness` pattern
//! from `tests/convergence.rs` (see that file's module doc comment for why
//! the cluster is a `thread_local`).

use generative::backend::ManualBackend;
use generative::generate::{
    Mutate, build_program, defer_def_install, program_with_mid_stream_def_install, schedule_restart,
};
use generative::run::{RunError, run_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::{Config, Pool};

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<generative::run::Coverage>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        eprintln!(
            "generative: mid-stream-definition-install run coverage:\n{}",
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

fn run_one(program: &generative::model::Program) -> Result<(), TestCaseError> {
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

    /// Define-then-load vs. load-then-define equivalence (this task's whole
    /// point): a definition installed after some of its own source table's
    /// rows already exist must still converge exactly like every other
    /// definition, i.e. must correctly backfill those preexisting rows and
    /// then keep up with whatever ops follow.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_convergence_holds_with_a_mid_stream_definition_install(
        program in program_with_mid_stream_def_install(true)
    ) {
        run_one(&program)?;
    }
}

/// Hand-built pin: one source table, two seed rows inserted, *then* the
/// definition installs (backfilling those two preexisting rows directly —
/// `generative/tests/backfill.rs`'s scenario, now driven end-to-end through
/// `run_convergence` itself rather than two hand-rolled `ManualBackend::install`
/// calls), then a third seed row and an update run afterward and must also
/// converge. This is the concrete "some rows inserted before the definition
/// is installed, then it backfills, then more ops run and converge" scenario
/// the task calls for.
#[tokio::test(flavor = "multi_thread")]
async fn a_definition_installed_after_preexisting_rows_backfills_and_keeps_converging() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Four seed rows, one update and one delete after — `build_program`'s
    // usual seed-before-mutate shape, so `program.ops` is
    // [insert, insert, insert, insert, update, delete].
    let program = build_program(
        &[
            (Some(10), Some(1)),
            (Some(20), Some(2)),
            (Some(30), Some(3)),
            (Some(40), Some(4)),
        ],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(100),
                c2: Some(5),
            },
            Mutate::Delete { pk: 4 },
        ],
    );
    assert_eq!(program.ops.len(), 6, "sanity check on the fixture shape");

    // Defer the sole definition's install to after the first two seed rows
    // (op index 2) — real, preexisting source rows by the time it installs,
    // with two more seed inserts and both mutates still to come afterward.
    let program = defer_def_install(program, 0, 2);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .unwrap_or_else(|e| panic!("mid-stream definition install must converge: {e:?}"));
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Cross-cutting holistic-review pin: does a mid-stream definition install
/// interact with the phase-gap-straggler fix (the engine's own
/// `staging::seal::seal_if_active_nonempty`'s straggler-catching case,
/// `staging::converge::converged_through`'s condition 3 — see
/// `generative/tests/client_lifecycle.rs`'s module doc comment for the full
/// writeup) on a table whose definition was *just* installed? Neither of
/// this task's own properties/pins above ever combine with a restart, and
/// `client_lifecycle.rs`'s restart pin only ever exercises definitions that
/// were already installed up front — so nothing on this branch previously
/// drove "install a definition, then immediately have a restart's extra
/// timing perturbation land right on that definition's own source table's
/// first post-install insert" at once.
///
/// Mirrors `client_lifecycle.rs`'s
/// `a_restart_and_a_scale_out_interleaved_mid_stream_still_converge` doc
/// comment's identified repro shape (a restart scheduled immediately before
/// a brand-new key's insert is the one pattern that can land as an unfenced
/// phase-gap straggler) but anchors the restart to the exact same op index
/// as the deferred definition's own install, so the straggler-risking insert
/// is also the definition's first-ever backfill-adjacent write.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_right_at_a_mid_stream_definition_install_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // ops: [insert x4, update, delete] (indices 0..=5).
    let program = build_program(
        &[
            (Some(10), Some(1)),
            (Some(20), Some(2)),
            (Some(30), Some(3)),
            (Some(40), Some(4)),
        ],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(100),
                c2: Some(5),
            },
            Mutate::Delete { pk: 4 },
        ],
    );
    assert_eq!(program.ops.len(), 6, "sanity check on the fixture shape");

    // Defer the sole definition's install to op index 2 (two preexisting
    // seed rows to backfill from), and anchor the restart to that exact same
    // op index: the install (and its backfill) happens, then the restart,
    // then op 2 — a brand-new key's insert into the just-installed
    // definition's own source table — runs immediately after, followed by
    // this op's own quiesce poll.
    let program = defer_def_install(program, 0, 2);
    let program = schedule_restart(program, 2);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .unwrap_or_else(|e| {
            panic!("a restart landing right at a mid-stream definition install must still converge: {e:?}")
        });
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}
