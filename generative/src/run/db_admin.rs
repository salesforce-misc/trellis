//! Issue #236 (layer-3 bucket 5, `docs/generative-test-suite.md` §7):
//! database-administration actions interleaved with a program's ops.
//!
//! Every action is an oracle identity: none of them changes the correct
//! converged state, so the per-op comparison is [`super::run_convergence`]'s
//! unchanged. What they add is a demand on the engine at the moment they
//! fire (see [`DbAdminAction`] for each).
//!
//! Like [`super::run_convergence_with_noise`], this is concrete over
//! [`ManualBackend`]: reconnecting its raw session after a restart is not
//! part of the generic [`crate::backend::Backend`] seam.
//!
//! Scope: the whole program installs up front. A program's
//! `def_install_after_op`, `restart_after_ops` and `scale_out_after_ops` are
//! not honoured here, the same as in the noise runner.

use trellis::Pool;

use crate::backend::{Backend, ClusterControl, ManualBackend, await_pool_usable};
use crate::model::{DbAdminAction, DbAdminPlan, Program};

use super::{Outcome, RunError, apply_and_check_outcome, quiesce_snapshot_and_check};

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

        apply_and_check_outcome(backend, op_index, op, false).await?;

        for action in events {
            match action {
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

fn admin_error(op_index: usize, action: &DbAdminAction, error: impl std::fmt::Debug) -> RunError {
    RunError::DbAdmin {
        op_index,
        action: action.clone(),
        error: format!("{error:?}"),
    }
}
