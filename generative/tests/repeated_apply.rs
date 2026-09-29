//! Improvement-plan task D1, scoped down from its original framing.
//!
//! The plan as originally written asked for "apply every op twice" as a
//! stand-in for at-least-once CDC redelivery. That framing doesn't survive
//! contact with how this backend seam actually works:
//!
//! - Trellis's derived-write path is structurally exactly-once by
//!   construction — claim, fold, apply, and mark-drained all happen in one
//!   transaction (`docs/data-flow.md`,
//!   `docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md`). There
//!   is no code path where an already-staged change gets applied twice.
//! - [`Backend::apply`] only issues raw source DML (design doc §1's backend
//!   seam). Calling it twice with the same [`Op`] does not redeliver
//!   anything — it produces a second, genuinely distinct source transaction
//!   with its own new WAL record, its own new staged row, its own new
//!   applied delta. That's real, valid traffic (an application legitimately
//!   re-running the same statement), not a simulation of the engine
//!   double-processing one delta.
//!
//! So "apply every op twice via `Backend::apply`" cannot actually exercise
//! at-least-once *redelivery* of a single change — that would require
//! injecting the same already-staged row into the ring twice, below
//! `Backend::apply`'s deliberately narrow API (staging-ring-level fault
//! injection, out of scope here; flagged in the workstream report as a real
//! gap for a future fault-injection effort).
//!
//! What's left, and what this file actually tests: **repeated identical
//! *transactions* still converge to the correct — cumulative, not
//! double-counted — state.** Concretely: call `backend.apply(op)` twice in a
//! row for the same [`Op`], and check the final materialized state still
//! matches the SQL oracle. Two of the three op kinds degenerate into cases
//! already covered elsewhere:
//!
//! - **Insert twice** is exactly `generative/tests/convergence.rs`'s
//!   `a_duplicate_pk_insert_error_still_converges` (a second `INSERT` at a
//!   live pk is a real primary-key-violation `apply()` error) — not repeated
//!   here.
//! - **Delete twice** is a close cousin of
//!   `no_op_mutations_on_missing_rows_still_converge` (the second `DELETE`
//!   hits zero rows) — included below anyway (cheap, and "the same delete
//!   run twice" is a slightly different scenario than "a delete that always
//!   targeted a dead pk") but not the interesting case.
//! - **Update twice** is the only genuinely non-trivial case (as the
//!   research pass that rescoped this task found): each of the two
//!   `apply()` calls is a real, valid delta (the row is live both times), so
//!   the target must reflect the net result of *both* having actually run —
//!   not double-count anything the way a naive incremental-`SUM`-style
//!   target might if the engine's `Aggregate` fold were implemented wrong,
//!   and not drop either one, leaving the target stuck reflecting a stale
//!   intermediate value. The two applies below deliberately carry
//!   *different* `c1`/`c2` values (not the same `Op` re-applied verbatim):
//!   with identical values, "both deltas landed" and "the engine silently
//!   dropped one of the two" are indistinguishable — the source row and its
//!   full recompute end up identical either way. Distinct values make the
//!   assertion below a real trap: the target must equal `f(row after both
//!   updates)`, not `f(row after only the first)`, so a dropped second delta
//!   (or a stale-image read that folds two deltas using the first's
//!   captured row rather than the current one) shows up as a divergence.
//!   Every definition this branch's generator draws is `KeySpace::OneToOne`
//!   (a full recompute from the current source row, not an incremental
//!   delta fold), so this doesn't yet reach the `Aggregate`-specific
//!   double-counting risk — but it's still real, valuable coverage of "the
//!   pipeline handles two back-to-back real transactions against the same
//!   row without dropping or double-applying either one," which is the
//!   actual, honest scope of this task.

use generative::backend::{Backend, ManualBackend};
use generative::generate::{Mutate, build_program};
use generative::model::OpOutcome;
use generative::run::check_program;
use testkit::TestCluster;
use trellis::{Config, Pool};

/// The genuinely non-trivial D1 case: two `Update`s against the same row,
/// back to back (no quiesce in between, so both deltas can land close
/// together in the staging ring before either is folded). Both calls are
/// real, live-row updates with *distinct* `c1`/`c2` values — see the module
/// doc comment for why the values must differ for this to be a real trap
/// rather than a vacuous pin — so both succeed, and the converged target
/// must reflect the *second* update's values, not the first's (a dropped
/// second delta) and not some other combination (a stale-image fold).
#[tokio::test(flavor = "multi_thread")]
async fn two_distinct_updates_to_the_same_row_back_to_back_still_converge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(1), Some(2))],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(10),
                c2: Some(20),
            },
            Mutate::Update {
                pk: 1,
                c1: Some(99),
                c2: Some(1),
            },
        ],
    );
    // ops: [seed insert, first update, second update]
    let seed = &program.ops[0];
    let first_update = &program.ops[1];
    let second_update = &program.ops[2];
    assert_eq!(first_update.expect(), &OpOutcome::Succeeds);
    assert_eq!(second_update.expect(), &OpOutcome::Succeeds);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(&program).await.expect("install program");

    let affected = backend.apply(seed).await.expect("seed insert must succeed");
    assert!(affected > 0);

    // Two back-to-back applies of two different Updates, no quiesce between
    // them: both are real, distinct transactions against a still-live row.
    let first = backend
        .apply(first_update)
        .await
        .expect("first update application must succeed");
    assert_eq!(
        first, 1,
        "the first update must affect exactly the one seeded row"
    );
    let second = backend
        .apply(second_update)
        .await
        .expect("second update application must still succeed (the row is still live)");
    assert_eq!(
        second, 1,
        "the second update must still affect exactly the one live row — it is a real, distinct \
         transaction, not a no-op"
    );

    backend.quiesce().await.expect("quiesce");
    let snapshot = backend.snapshot().await.expect("snapshot");
    let divergence = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        divergence.is_empty(),
        "two back-to-back Updates to the same row must not leave the target diverged from the \
         real (net-of-both, not dropped-or-double-counted) source state: {divergence:?}"
    );
}

/// The lesser D1 case: the same `Delete` applied twice in a row. The first
/// call is a genuine delete (the row is live); the second finds nothing (the
/// row is already gone) and is a source no-op (`0` rows affected), not an
/// error — the "operation errors are checked, not swallowed" no-op path
/// (design doc §4), just reached via a literal repeat of the same statement
/// rather than a mutate that always targeted a dead pk. Included primarily
/// so a reader doesn't have to wonder whether `Delete` was skipped from this
/// file's coverage; see the module doc comment for why `Update` (above), not
/// this, is the case that actually matters.
#[tokio::test(flavor = "multi_thread")]
async fn the_same_delete_applied_twice_in_a_row_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(&[(Some(1), Some(2))], &[Mutate::Delete { pk: 1 }]);
    let seed = &program.ops[0];
    let delete = &program.ops[1];
    assert_eq!(delete.expect(), &OpOutcome::Succeeds);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(&program).await.expect("install program");
    backend.apply(seed).await.expect("seed insert must succeed");

    let first = backend
        .apply(delete)
        .await
        .expect("the first delete must succeed");
    assert_eq!(first, 1, "the first delete must affect the one live row");

    // Relaxed on purpose (per the module doc comment): the generator tagged
    // this `Op` `OpOutcome::Succeeds` for its *single*, originally-drawn
    // application — the second, repeated call is legitimately a source
    // no-op (`AffectsNoRows`), not a re-check of that same expectation.
    let second = backend
        .apply(delete)
        .await
        .expect("re-applying a delete against an already-deleted row is a no-op, not an error");
    assert_eq!(
        second, 0,
        "the second, identical delete must affect zero rows — the row is already gone"
    );

    backend.quiesce().await.expect("quiesce");
    let snapshot = backend.snapshot().await.expect("snapshot");
    let divergence = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        divergence.is_empty(),
        "repeating an already-applied Delete must not leave the target diverged: {divergence:?}"
    );
}
