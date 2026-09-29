//! Workstream E, tasks E1 (untracked-object noise) and E5 (cluster-
//! administration noise, `CHECKPOINT` only — see the module doc comment on
//! why plain restart is out of scope) both need to interleave raw SQL
//! against [`crate::backend::ManualBackend`]'s own connection with a
//! program's real op stream — something [`super::run_convergence`]'s generic
//! `Backend` trait deliberately never exposes (design doc §1: only `backend`
//! may touch trellis/connection internals, and the trait's four methods are
//! exactly "what a convergence run needs", nothing broader).
//!
//! This module is a narrow, explicitly-scoped exception to `run`'s own
//! module doc comment ("names no cluster/harness types and no generator"):
//! it is concrete over [`ManualBackend`] rather than generic over
//! [`super::Backend`], because raw-SQL noise execution has no meaning for a
//! hypothetical future backend that doesn't expose a raw connection at all.
//! [`super::run_convergence`] itself is completely untouched by this file.
//!
//! Both tasks are "bucket 1" (`local_docs/generative-suite-improvement-plan.md`
//! workstream E): neither is meant to perturb a tracked program's
//! oracle-verified convergence at all, so [`run_convergence_with_noise`]
//! below reuses [`super::check_program`] unchanged and asserts the exact
//! same thing [`super::run_convergence`] does after every op — the only
//! difference is that a [`crate::model::NoisePlan`]'s events are free to fire
//! at the positions it names, interleaved with the real ops.

use trellis::Pool;

use crate::backend::{Backend, ManualBackend};
use crate::model::{NoisePlan, OpOutcome, Program};

use super::{Divergence, Outcome, RunError, check_program};

/// Fires every event in `plan.events` whose `before_op` equals `position`, in
/// the order they appear in `plan.events`. A no-op when nothing matches
/// (the common case for most positions in a sparsely-populated plan).
async fn fire_events(backend: &ManualBackend, plan: &NoisePlan, position: usize) {
    for event in plan.events.iter().filter(|e| e.before_op == position) {
        backend.fire_noise_event(plan.table.as_ref(), event).await;
    }
}

/// [`super::run_convergence`], widened to interleave `noise`'s events into
/// the real op stream (workstream E, tasks E1 + E5). `noise.table` (if
/// `Some`) is created once, right after `program`'s own install, via
/// [`ManualBackend::install_noise_table`] — never registered as tracked, so
/// it cannot affect anything [`check_program`] looks at (see that function's
/// and [`ManualBackend::install_noise_table`]'s doc comments). Every other
/// line mirrors `run_convergence`'s own loop exactly; the two are kept as
/// separate functions; not one shared implementation parameterized by an
/// `Option<&NoisePlan>`, so the noise-free property's hot path never has to
/// thread a parameter it doesn't need, and so a future backend-agnostic
/// change to `run_convergence` can't accidentally acquire a `ManualBackend`
/// dependency through this file.
pub async fn run_convergence_with_noise(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    noise: &NoisePlan,
) -> Result<Outcome, RunError> {
    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;
    if let Some(table) = &noise.table {
        backend
            .install_noise_table(table)
            .await
            .map_err(|e| RunError::Install(format!("{e:?}")))?;
    }

    fire_events(backend, noise, 0).await;

    for (op_index, op) in program.ops.iter().enumerate() {
        let actual = match backend.apply(op).await {
            Err(_) => OpOutcome::Fails,
            Ok(0) => OpOutcome::AffectsNoRows,
            Ok(_) => OpOutcome::Succeeds,
        };
        let expected = op.expect();
        if !expected.matches(&actual) {
            return Err(RunError::UnexpectedOpOutcome {
                op_index,
                expected: expected.clone(),
                actual,
            });
        }

        backend
            .quiesce()
            .await
            .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;

        let snapshot = backend
            .snapshot()
            .await
            .map_err(|e| RunError::Snapshot(format!("{e:?}")))?;

        let checked = check_program(pool, program, &snapshot).await;
        if let Some(divergence) =
            Divergence::from_checked(op_index, checked.map_err(RunError::Oracle)?)
        {
            return Err(RunError::Diverged(divergence));
        }

        // Fires *after* op_index's own full cycle (position `op_index + 1`),
        // so a divergence caused by real op `op_index` is never masked by
        // noise racing in before the check above ran.
        fire_events(backend, noise, op_index + 1).await;
    }

    Ok(Outcome::Ran)
}
