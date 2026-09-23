//! Issue #236 (layer-3 bucket 5, `docs/generative-test-suite.md` §7):
//! database-administration actions interleaved with a program's ops.
//!
//! Every action is an oracle identity: none of them changes the correct
//! converged state, so the per-op comparison is [`super::run_convergence`]'s
//! unchanged. What they add is a demand on the engine at the moment they
//! fire (see [`DbAdminAction`] for each), and one extra check for a lost
//! slot: before the harness plays operator and resumes anything, every
//! installed transform must already be `paused`. A transform that came up
//! any other way carried on over a gap its stream never delivered, which
//! issue #310 says must not happen. [`check_slot_loss_detected`] is that
//! check.
//!
//! Like [`super::run_convergence_with_noise`], this is concrete over
//! [`ManualBackend`]: stopping the engine, losing its slot and reconnecting
//! its raw session are not part of the generic [`crate::backend::Backend`]
//! seam.
//!
//! Scope: the whole program installs up front. A program's
//! `def_install_after_op`, `restart_after_ops` and `scale_out_after_ops` are
//! not honoured here, the same as in the noise runner.

use trellis::Pool;

use crate::backend::{Backend, ClusterControl, ManualBackend, await_pool_usable};
use crate::model::{DbAdminAction, DbAdminPlan, Program, SlotLossKind};

use super::{Outcome, RunError, apply_and_check_outcome, quiesce_snapshot_and_check};

/// The status issue #310's recovery gives every transform the lost slot fed.
const PAUSED: &str = "paused";

/// Issue #236's detection check for a lost slot, run after the engine came
/// back up and before anything is resumed. `statuses` is every installed
/// definition's `(target, status)`. Every definition in a generated program
/// reads a published source table, so every one of them must be `paused`.
pub fn check_slot_loss_detected(
    op_index: usize,
    statuses: Vec<(String, String)>,
) -> Result<(), RunError> {
    let not_paused: Vec<(String, String)> = statuses
        .into_iter()
        .filter(|(_, status)| status != PAUSED)
        .collect();
    if not_paused.is_empty() {
        Ok(())
    } else {
        Err(RunError::SlotLossUndetected {
            op_index,
            not_paused,
        })
    }
}

/// [`super::run_convergence`] with `plan`'s database-administration actions
/// interleaved (issue #236). `cluster` restarts the server `backend` and
/// `pool` point at. `pool` is the oracle's handle; it is waited on after a
/// restart the same way the backend's own pool is.
pub async fn run_convergence_with_db_admin<C: ClusterControl>(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    plan: &DbAdminPlan,
    cluster: &C,
) -> Result<Outcome, RunError> {
    for event in &plan.events {
        assert!(
            event.op < program.ops.len(),
            "db-admin event {event:?} is anchored past the last op ({} ops) — a generator bug",
            program.ops.len()
        );
    }

    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;

    for (op_index, op) in program.ops.iter().enumerate() {
        let events: Vec<&DbAdminAction> = plan
            .events
            .iter()
            .filter(|e| e.op == op_index)
            .map(|e| &e.action)
            .collect();
        let slot_loss = events.iter().find_map(|action| match action {
            DbAdminAction::LoseSlot(kind) => Some(*kind),
            _ => None,
        });

        if let Some(kind) = slot_loss {
            lose_slot(backend, op_index, kind).await?;
        }

        apply_and_check_outcome(backend, op_index, op, false).await?;

        if let Some(kind) = slot_loss {
            recover_from_slot_loss(backend, op_index, kind).await?;
        }

        for action in events {
            match action {
                DbAdminAction::LoseSlot(_) => {}
                DbAdminAction::Checkpoint => {
                    backend
                        .execute_raw("CHECKPOINT")
                        .await
                        .map_err(|e| admin_error(op_index, action, e))?;
                }
                DbAdminAction::RestartPostgres(mode) => {
                    cluster.restart_postgres(*mode);
                    backend
                        .reconnect_after_server_restart()
                        .await
                        .map_err(|e| admin_error(op_index, action, e))?;
                    await_pool_usable(pool)
                        .await
                        .map_err(|e| admin_error(op_index, action, e))?;
                }
            }
        }

        quiesce_snapshot_and_check(backend, pool, program, op_index, &program.defs, false).await?;
    }

    Ok(Outcome::Ran)
}

/// The first half of a [`DbAdminAction::LoseSlot`]: the engine stops and its
/// slot goes, before the anchor op is applied.
async fn lose_slot(
    backend: &mut ManualBackend,
    op_index: usize,
    kind: SlotLossKind,
) -> Result<(), RunError> {
    let action = DbAdminAction::LoseSlot(kind);
    backend
        .stop_engine()
        .await
        .map_err(|e| admin_error(op_index, &action, e))?;
    backend
        .lose_slot(kind)
        .await
        .map_err(|e| admin_error(op_index, &action, e))
}

/// The second half of a [`DbAdminAction::LoseSlot`], after the anchor op
/// landed in the gap: the engine starts again, the detection check runs,
/// then every transform is resumed.
async fn recover_from_slot_loss(
    backend: &mut ManualBackend,
    op_index: usize,
    kind: SlotLossKind,
) -> Result<(), RunError> {
    let action = DbAdminAction::LoseSlot(kind);
    backend
        .start_engine()
        .await
        .map_err(|e| admin_error(op_index, &action, e))?;
    let statuses = backend
        .definition_statuses()
        .await
        .map_err(|e| admin_error(op_index, &action, e))?;
    check_slot_loss_detected(op_index, statuses)?;
    backend
        .resume_all()
        .await
        .map_err(|e| admin_error(op_index, &action, e))
}

fn admin_error(op_index: usize, action: &DbAdminAction, error: impl std::fmt::Debug) -> RunError {
    RunError::DbAdmin {
        op_index,
        action: action.clone(),
        error: format!("{error:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statuses(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(t, s)| (t.to_string(), s.to_string()))
            .collect()
    }

    #[test]
    fn every_transform_paused_counts_as_detected() {
        check_slot_loss_detected(3, statuses(&[("d0", "paused"), ("d1", "paused")]))
            .expect("all paused is the #310 contract");
    }

    /// The control: a transform that came up anything but `paused` after a
    /// slot loss is reported, naming it and the status it had.
    #[test]
    fn a_transform_left_running_over_the_gap_is_reported() {
        match check_slot_loss_detected(
            3,
            statuses(&[("d0", "paused"), ("d1", "live"), ("d2", "<missing>")]),
        ) {
            Err(RunError::SlotLossUndetected {
                op_index,
                not_paused,
            }) => {
                assert_eq!(op_index, 3);
                assert_eq!(not_paused, statuses(&[("d1", "live"), ("d2", "<missing>")]));
            }
            other => panic!("expected SlotLossUndetected, got {other:?}"),
        }
    }
}
