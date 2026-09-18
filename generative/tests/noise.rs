//! Improvement-plan Workstream E, tasks E1 (untracked-object noise) and E5
//! (cluster-administration noise — `CHECKPOINT` only, see below) — growing
//! the generative suite's action space past DML without changing what a
//! converged run is supposed to look like: both tasks are "bucket 1" (the
//! plan's own term), meaning `generative::oracle` needs zero changes. This
//! file's whole job is to prove that claim empirically, not just assert it:
//! [`generative::run::run_convergence_with_noise`] reuses
//! [`generative::run::check_program`] completely unchanged, and this file
//! interleaves increasingly adversarial noise/administration around a real
//! generated program's ops and asserts convergence still holds throughout.
//!
//! # E5 scope cut: `CHECKPOINT` only, not a full Postgres restart
//!
//! A full `TestCluster` restart was investigated and deliberately **not**
//! built. `trellis::intake::Intake::run` (the staging worker's logical-
//! replication consumer) returns `Err` the instant its replication
//! connection drops — confirmed by reading `trellis/src/intake/mod.rs`
//! directly, not assumed — and `trellis::client::run` spawns it as
//! `let _ = intake.run().await;`, silently discarding that error with no
//! reconnect logic at all. A live `ManualBackend`'s own `raw` connection
//! (`ManualBackend::connect`) has the same shape: one `tokio_postgres::connect`
//! call, no supervisor. A real Postgres restart severs every one of these
//! connections at once, so recovering would need genuinely new machinery —
//! a way to tear down and rebuild `ManualBackend`'s connection *and* a fresh
//! `trellis::Client` against the same already-installed schema (never
//! re-running `install`, which would try to recreate tables that already
//! exist) — which is real, separate, engine-adjacent work, not a "bucket 1,
//! converged-state-unchanged" widening. This mirrors exactly the reasoning
//! that already scoped slot-invalidation out of this same task: "the client
//! needs to surface a loud, specific recovery story" is a different shape of
//! test than "convergence still holds unattended." Both restart and slot
//! invalidation are follow-ups, not built here.
//!
//! `CHECKPOINT` has none of that problem — it's a plain SQL statement over
//! the same connection everything else already uses, changes no connection
//! state, and (per Postgres's own docs) only flushes dirty buffers and
//! writes a WAL checkpoint record, so it has no reason to disturb a live
//! replication slot or the staging pipeline's own watermark polling. This
//! file's property is exactly the empirical check that this is really true
//! (design doc §6: don't assert what you haven't run), not just a restated
//! assumption.
//!
//! Reuses the shared-cluster/isolated-database-per-case `Harness` pattern
//! from `tests/convergence.rs` (see that file's module doc comment for why
//! the cluster is a `thread_local`).

use generative::backend::ManualBackend;
use generative::generate::{
    Mutate, adversarial_noise_table, build_program, checkpoint_plan_for, noise_plan_for,
    trivial_program,
};
use generative::model::{NoiseAction, NoiseEvent, NoiseEventKind, NoisePlan, Program};
use generative::run::{RunError, run_convergence_with_noise};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;

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
        .unwrap_or(16);
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs one generated program with `noise` interleaved, against a fresh
/// isolated database in the shared cluster — the noise-aware counterpart of
/// `tests/convergence.rs`'s `run_one`.
fn run_one(program: &Program, noise: &NoisePlan) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
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
            let pool = trellis::Pool::new(
                &trellis::Config::from_dsn(db.dsn().to_string()).expect("config"),
            )
            .expect("pool");

            match run_convergence_with_noise(&mut backend, &pool, program, noise).await {
                Ok(outcome) => {
                    if outcome.as_pass() {
                        Ok(())
                    } else {
                        Err(TestCaseError::fail(format!("run did not pass: {outcome}")))
                    }
                }
                Err(RunError::Diverged(d)) => Err(TestCaseError::fail(format!(
                    "convergence diverged after op {} (target {}) with noise plan {noise:?}:\n{}",
                    d.op_index, d.def_target, d.report
                ))),
                Err(other) => Err(TestCaseError::fail(format!(
                    "run error with noise plan {noise:?}: {other:?}"
                ))),
            }
        })
    })
}

/// Pairs a drawn [`Program`] with an E1 noise plan sized to its own op count,
/// via `prop_flat_map` (the same "draw the dependency, then draw off of it"
/// idiom `generative::generate`'s own `table_spec`/`def_shape_and_derived`
/// strategies use), so proptest's integrated shrinking can shrink the noise
/// plan alongside the program rather than drawing noise off an independent,
/// unshrinkable second `TestRunner`.
fn program_with_noise_plan() -> impl Strategy<Value = (Program, NoisePlan)> {
    trivial_program().prop_flat_map(|program| {
        let op_count = program.ops.len();
        (Just(program), noise_plan_for(op_count))
    })
}

/// The E5 counterpart of [`program_with_noise_plan`]: pairs a drawn
/// [`Program`] with a `CHECKPOINT`-only administration plan sized to its own
/// op count.
fn program_with_checkpoint_plan() -> impl Strategy<Value = (Program, NoisePlan)> {
    trivial_program().prop_flat_map(|program| {
        let op_count = program.ops.len();
        (Just(program), checkpoint_plan_for(op_count))
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// E1's property: arbitrary DML/DDL noise against an untracked table,
    /// interleaved throughout a real generated program's op stream, must
    /// never perturb the tracked convergence check.
    #[test]
    fn property_untracked_table_noise_never_perturbs_convergence((program, noise) in program_with_noise_plan()) {
        run_one(&program, &noise)?;
    }

    /// E5's property: interleaved `CHECKPOINT`s must never perturb the
    /// tracked convergence check either.
    #[test]
    fn property_interleaved_checkpoints_never_perturb_convergence((program, noise) in program_with_checkpoint_plan()) {
        run_one(&program, &noise)?;
    }
}

/// The named "first end-to-end green" for E1: a fully-worked, hand-built
/// convergent program with untracked-table noise interleaved throughout,
/// including the deliberately adversarial [`adversarial_noise_table`] (pk
/// column named `"c0"`, extra column named `"total"` — both names a real
/// tracked table/def commonly uses) and both DML (`Insert`/`Update`/
/// `Delete`) and DDL (`AddColumn`/`DropColumn`) noise actions, at
/// positions before, between, and after every real op.
#[tokio::test(flavor = "multi_thread")]
async fn noise_on_an_adversarially_shaped_untracked_table_never_perturbs_convergence() {
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
    assert_eq!(program.ops.len(), 5, "3 seed inserts + 1 update + 1 delete");

    let noise = NoisePlan {
        table: Some(adversarial_noise_table()),
        events: vec![
            // Before any real op: seed two noise rows, one of them NULL.
            NoiseEvent {
                before_op: 0,
                kind: NoiseEventKind::Table(NoiseAction::Insert {
                    pk: 1,
                    value: Some("hello".to_string()),
                }),
            },
            NoiseEvent {
                before_op: 0,
                kind: NoiseEventKind::Table(NoiseAction::Insert { pk: 2, value: None }),
            },
            // Mid-stream: DDL noise (add then drop a column), plus DML.
            NoiseEvent {
                before_op: 2,
                kind: NoiseEventKind::Table(NoiseAction::AddColumn {
                    name: "extra1".to_string(),
                    value_type: trellis::defs::ast::ValueType::Numeric,
                }),
            },
            NoiseEvent {
                before_op: 3,
                kind: NoiseEventKind::Table(NoiseAction::Update {
                    pk: 1,
                    value: Some("updated".to_string()),
                }),
            },
            NoiseEvent {
                before_op: 3,
                kind: NoiseEventKind::Table(NoiseAction::DropColumn {
                    name: "extra1".to_string(),
                }),
            },
            // A delete that hits nothing (noise never seeded pk 99) — must
            // be tolerated silently, just like a real op's missing-pk case.
            NoiseEvent {
                before_op: 4,
                kind: NoiseEventKind::Table(NoiseAction::Delete { pk: 99 }),
            },
            // After the very last real op.
            NoiseEvent {
                before_op: 5,
                kind: NoiseEventKind::Table(NoiseAction::Delete { pk: 1 }),
            },
        ],
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool =
        trellis::Pool::new(&trellis::Config::from_dsn(db.dsn().to_string()).expect("config"))
            .expect("pool");

    let outcome = run_convergence_with_noise(&mut backend, &pool, &program, &noise)
        .await
        .expect("adversarial untracked-table noise must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// E5's hand-built pin: a `CHECKPOINT` fired before, between, and after every
/// real op in a small hand-built program — a direct, legible demonstration
/// that `quiesce()`/convergence tolerate a real Postgres checkpoint at any
/// point in the run, not just the property happening to draw one sometimes.
#[tokio::test(flavor = "multi_thread")]
async fn checkpoints_interleaved_with_every_op_never_perturb_convergence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(1), Some(2)), (Some(3), Some(4))],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(10),
                c2: Some(20),
            },
            Mutate::Delete { pk: 2 },
        ],
    );
    assert_eq!(program.ops.len(), 4, "2 seed inserts + 1 update + 1 delete");

    let noise = NoisePlan {
        table: None,
        events: (0..=program.ops.len())
            .map(|before_op| NoiseEvent {
                before_op,
                kind: NoiseEventKind::Admin("CHECKPOINT".to_string()),
            })
            .collect(),
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool =
        trellis::Pool::new(&trellis::Config::from_dsn(db.dsn().to_string()).expect("config"))
            .expect("pool");

    let outcome = run_convergence_with_noise(&mut backend, &pool, &program, &noise)
        .await
        .expect("a CHECKPOINT before/between/after every op must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}
