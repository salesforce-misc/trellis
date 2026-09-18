//! Improvement-plan task D2: order-insensitivity over commuting ops.
//!
//! `generative::generate`'s commutation analysis (`ops_commute`/
//! `commute_groups`/`reordered_by_commute_groups`, see that module's doc
//! comment for the design — in particular why it is keyed off each
//! definition's own *target key*, not the raw source pk) says two ops that
//! land on different rows of a table (and, for a future `KeySpace::Aggregate`
//! definition, different groups) may be freely reordered relative to each
//! other without changing the program's eventual converged state. This file
//! is the DB-backed half of that claim: run the *same* [`Program`] under two
//! different, independently-valid orderings against two independent fresh
//! databases, and assert the two runs converge to bit-for-bit identical
//! [`generative::backend::Snapshot`]s.
//!
//! Reuses the shared-cluster/isolated-database-per-case `Harness` pattern
//! from `tests/convergence.rs` (see that file's module doc comment for why
//! the cluster is a `thread_local`).

use generative::backend::{Backend, ManualBackend, Snapshot};
use generative::generate::{
    Mutate, TableSpec, build_program, build_program_multi, reordered_by_commute_groups,
    trivial_program,
};
use generative::model::{Op, OpOutcome, Program};
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

/// Installs `program` against a fresh isolated database, applies every op in
/// `ops` (which must be some valid reordering of `program.ops` — same
/// multiset, only positions may differ) checking each one's actual outcome
/// against what the generator recorded on it, quiesces once at the end (not
/// per-op: this property only cares about the *final* converged state, not
/// intermediate ones), and returns the resulting [`Snapshot`].
///
/// Checking `expect()` here (rather than only comparing final snapshots)
/// gives an early, precise failure if a reordering ever broke an op's
/// predicted outcome — which `reordered_by_commute_groups`'s doc comment
/// argues can't happen (an op's outcome only depends on earlier ops sharing
/// its own commute group, and those are never reordered relative to it) —
/// instead of that bug only surfacing as an opaque snapshot mismatch.
async fn run_ops_to_snapshot(cluster: &TestCluster, program: &Program, ops: &[Op]) -> Snapshot {
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");

    backend.install(program).await.expect("install program");

    for op in ops {
        let actual = match backend.apply(op).await {
            Err(_) => OpOutcome::Fails,
            Ok(0) => OpOutcome::AffectsNoRows,
            Ok(_) => OpOutcome::Succeeds,
        };
        assert!(
            op.expect().matches(&actual),
            "op {op:?} expected {:?} but produced {actual:?}",
            op.expect()
        );
    }

    backend.quiesce().await.expect("quiesce");
    backend.snapshot().await.expect("snapshot")
}

/// Runs one generated program under its original ordering and under
/// [`reordered_by_commute_groups`]'s reordering, against two independent
/// fresh databases in the shared cluster, and asserts the converged
/// snapshots are identical.
fn run_one(program: &Program) -> Result<(), TestCaseError> {
    let reordered = reordered_by_commute_groups(program);
    HARNESS.with(|h| {
        h.runtime.block_on(async {
            let original_snapshot = run_ops_to_snapshot(&h.cluster, program, &program.ops).await;
            let reordered_snapshot =
                run_ops_to_snapshot(&h.cluster, &reordered, &reordered.ops).await;
            if original_snapshot != reordered_snapshot {
                return Err(TestCaseError::fail(format!(
                    "two commuting orderings of the same program converged to different \
                     snapshots:\noriginal ops: {:#?}\nreordered ops: {:#?}\noriginal snapshot: \
                     {original_snapshot:#?}\nreordered snapshot: {reordered_snapshot:#?}",
                    program.ops, reordered.ops,
                )));
            }
            Ok(())
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// D2's property: reordering ops across distinct target keys must not
    /// change the converged state.
    #[test]
    fn property_commuting_reorderings_converge_identically(program in trivial_program()) {
        run_one(&program)?;
    }
}

/// D2 item 3 (`local_docs/generative-suite-improvement-plan.md`): a coverage
/// floor confirming the generator actually produces programs where
/// `reordered_by_commute_groups` does something non-trivial often enough for
/// the property above to be non-vacuous. If every generated program had only
/// one op, or every op shared one target key, the property would hold
/// trivially (both "orderings" would literally be the same sequence) without
/// ever exercising the reordering logic at all.
#[test]
fn trivial_program_sometimes_has_ops_with_genuinely_distinct_target_keys() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_genuine_reorder = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        reordered_by_commute_groups(&program).ops != program.ops
    });
    assert!(
        saw_genuine_reorder,
        "expected at least one sample across 500 where reordering by commute group actually \
         changes the op order (i.e. the generator drew 2+ ops with genuinely distinct target \
         keys) — otherwise `property_commuting_reorderings_converge_identically` never exercises anything"
    );
}

/// D2 item 4: a hand-built pin. Two independent ops on two different pks of
/// the same table — a `Delete` of pk 1 and an `Update` of pk 2 — applied in
/// reverse relative order, must converge identically. Built directly (not
/// through `reordered_by_commute_groups`) so this is a legible, from-first-
/// principles demonstration of the property the generic machinery above
/// tests broadly.
#[tokio::test(flavor = "multi_thread")]
async fn two_independent_ops_on_different_pks_converge_identically_in_either_order() {
    let cluster = TestCluster::start();

    let forward = build_program(
        &[(Some(1), Some(2)), (Some(3), Some(4))],
        &[
            Mutate::Delete { pk: 1 },
            Mutate::Update {
                pk: 2,
                c1: Some(30),
                c2: Some(40),
            },
        ],
    );
    // The two mutates (indices 2 and 3, after the two seed inserts) target
    // different pks on the same table and no shared def-level key (OneToOne:
    // target key == pk) — `ops_commute` says these two are free to swap.
    let mut reversed = forward.clone();
    reversed.ops.swap(2, 3);

    let forward_snapshot = run_ops_to_snapshot(&cluster, &forward, &forward.ops).await;
    let reversed_snapshot = run_ops_to_snapshot(&cluster, &reversed, &reversed.ops).await;
    assert_eq!(
        forward_snapshot, reversed_snapshot,
        "a Delete on pk 1 and an Update on pk 2 must converge identically whichever comes first"
    );
}

/// The mirror of the pin above, but across two different *tables* rather
/// than two pks of the same table — the other half of `ops_commute`'s
/// contract ("ops on different tables always commute").
#[tokio::test(flavor = "multi_thread")]
async fn two_ops_on_different_tables_converge_identically_in_either_order() {
    let cluster = TestCluster::start();

    let forward = build_program_multi(
        &[
            TableSpec::numeric_only(
                vec![(Some(1), Some(2))],
                vec![Mutate::Update {
                    pk: 1,
                    c1: Some(100),
                    c2: Some(200),
                }],
            ),
            TableSpec::numeric_only(vec![(Some(9), Some(9))], vec![Mutate::Delete { pk: 1 }]),
        ],
        &[0, 1],
    );
    // ops: [seed A pk1, update A pk1, seed B pk1, delete B pk1]. Swap the two
    // whole tables' op blocks relative to each other; each table's own
    // internal (seed-then-mutate) order is preserved.
    let mut reversed = forward.clone();
    reversed.ops = vec![
        forward.ops[2].clone(),
        forward.ops[3].clone(),
        forward.ops[0].clone(),
        forward.ops[1].clone(),
    ];

    let forward_snapshot = run_ops_to_snapshot(&cluster, &forward, &forward.ops).await;
    let reversed_snapshot = run_ops_to_snapshot(&cluster, &reversed, &reversed.ops).await;
    assert_eq!(
        forward_snapshot, reversed_snapshot,
        "two independent tables' op streams must converge identically in either relative order"
    );
}
