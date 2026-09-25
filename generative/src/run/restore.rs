//! Issue #236's backup-and-restore action (`docs/generative-test-suite.md`
//! §7, layer-3 bucket 5): back the cluster up at op *k*, run the program to
//! its end, restore the backup into a fresh cluster, replay ops *k+1..n*
//! against the restored database, and converge.
//!
//! What this checks is the assumption the 2026-09-24 decision on issue #236
//! rests on: Trellis keeps its catalog, staging ring, `replication_progress`
//! row and targets in the database it reads from, so a restore rolls all of
//! them back to the same point and what comes back is consistent with
//! itself. Trellis has no restore-specific handling, and needs none.
//!
//! The oracle is "cloned" at *k* for free: it recomputes from the source
//! tables, and those are in the backup. So the restored database is held to
//! two things before anything is replayed on it. Its source tables must be
//! exactly what the original's were after op *k* (the restore lost or kept
//! nothing it shouldn't), and its targets must converge to the oracle over
//! that source. Then every replayed op is checked like any other.
//!
//! A cold copy brings the replication slot back exactly where the restored
//! progress row expects it, so intake must carry on from the restored point
//! with no pause. A transform paused anywhere on the restored side is a
//! failure ([`RunError::PausedAfterRestore`]), not a recovery: nothing here
//! resumes one.
//!
//! The backup is taken right after op *k* is applied, before the run waits
//! for the engine to catch up, so the backed-up ring, progress row and slot
//! usually have that op's change in flight.
//!
//! Concrete over [`ManualBackend`] for the same reason as
//! [`super::run_convergence_with_db_admin`]. The whole program installs up
//! front; a program's `def_install_after_op`, `restart_after_ops` and
//! `scale_out_after_ops` are not honoured here.

use trellis::{Config, Pool};

use crate::backend::{Backend, BackupRestore, ManualBackend, Snapshot, await_pool_usable};
use crate::model::{BackupKind, Program, RestorePlan};

use super::{
    Divergence, Outcome, RunError, apply_and_check_outcome, check_defs, quiesce_snapshot_and_check,
};

/// The status issue #310's recovery gives a transform whose slot was lost,
/// which a cold-copy restore must never produce.
const PAUSED: &str = "paused";

/// Issue #236's check that a cold-copy restore carried on without a pause.
/// `statuses` is every installed definition's `(target, status)` on the
/// restored database, read after the engine caught up with `op_index`.
///
/// Panics on an empty `statuses`, like
/// [`super::check_slot_loss_detected`]: a program with no transform has
/// nothing that could have paused.
pub fn check_no_pause_after_restore(
    op_index: usize,
    statuses: Vec<(String, String)>,
) -> Result<(), RunError> {
    assert!(
        !statuses.is_empty(),
        "no-pause check at op {op_index} over no transforms proves nothing"
    );
    let paused: Vec<(String, String)> = statuses
        .into_iter()
        .filter(|(_, status)| status == PAUSED)
        .collect();
    if paused.is_empty() {
        Ok(())
    } else {
        Err(RunError::PausedAfterRestore { op_index, paused })
    }
}

/// The source tables of `program` in `snapshot`, which is all a restore is
/// compared on directly: the targets are the oracle's to judge.
fn source_tables(program: &Program, snapshot: &Snapshot) -> Snapshot {
    program
        .tables
        .iter()
        .filter_map(|t| {
            snapshot
                .get(&t.name)
                .map(|rows| (t.name.clone(), rows.clone()))
        })
        .collect()
}

/// [`super::run_convergence`] with a backup at `plan.backup_op` and a restore
/// after the last op (issue #236); see the module doc comment.
///
/// `backend` and `pool` are the original database's backend and oracle
/// handle, and `database` its name, the same on the restored cluster.
/// `cluster` is the server both point at: it is stopped for the backup and
/// then carries on. The restored cluster lives only for this call.
///
/// The original's engine is stopped before the restore, so the caller can
/// drop its database.
pub async fn run_convergence_with_restore<C: BackupRestore>(
    backend: &mut ManualBackend,
    pool: &Pool,
    database: &str,
    program: &Program,
    plan: &RestorePlan,
    cluster: &C,
) -> Result<Outcome, RunError> {
    let k = plan.backup_op;
    assert!(
        k < program.ops.len(),
        "restore plan {plan:?} backs up past the last op ({} ops) — a generator bug",
        program.ops.len()
    );
    match plan.kind {
        BackupKind::ColdCopy => {}
    }

    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;

    // The original run, backing up at `k`.
    let mut backup = None;
    let mut sources_at_backup = None;
    for (op_index, op) in program.ops.iter().enumerate() {
        apply_and_check_outcome(backend, op_index, op, false).await?;
        if op_index == k {
            backup = Some(cluster.cold_backup());
            backend
                .reconnect_after_server_restart()
                .await
                .map_err(|e| restore_error(op_index, "reconnect after the backup", e))?;
            await_pool_usable(pool)
                .await
                .map_err(|e| restore_error(op_index, "reconnect after the backup", e))?;
        }
        quiesce_snapshot_and_check(backend, pool, program, op_index, &program.defs, false).await?;
        if op_index == k {
            let snapshot = backend
                .snapshot()
                .await
                .map_err(|e| RunError::Snapshot(format!("{e:?}")))?;
            sources_at_backup = Some(source_tables(program, &snapshot));
        }
    }
    let backup = backup.expect("the loop reaches the backup op");
    let sources_at_backup = sources_at_backup.expect("the loop reaches the backup op");
    let last = program.ops.len() - 1;
    backend
        .stop_engine()
        .await
        .map_err(|e| restore_error(last, "stop the original engine", e))?;

    // The restore. The backup's copy goes as soon as it's been restored
    // from: it and the restored cluster are each a full copy of the server.
    let restored_cluster = C::restore(&backup);
    drop(backup);
    let dsn = restored_cluster.database_dsn(database);
    let mut restored = backend
        .connect_to_restore(dsn.clone())
        .await
        .map_err(|e| restore_error(k, "start the engine on the restore", e))?;
    let restored_pool = Config::from_dsn(dsn)
        .and_then(|config| Pool::new(&config))
        .map_err(|e| restore_error(k, "open the oracle's pool on the restore", e))?;

    // TODO(#558): a `BackupKind::BaseBackup` restore has no slot. Here is
    // where it would instead check issue #310's pause
    // (`super::check_slot_loss_detected`), resume every transform and let the
    // fresh backfill converge, rather than requiring no pause below.

    // Op `k` again, now on the restore: nothing paused, the source is what
    // the original's was, and the targets converge over it. The pause check
    // goes first, as after every replayed op below: a paused transform stops
    // following its source, so the oracle would report it too, but only as
    // a divergence that doesn't name the cause.
    quiesce_and_check_not_paused(&mut restored, k).await?;
    let snapshot = restored
        .snapshot()
        .await
        .map_err(|e| RunError::Snapshot(format!("{e:?}")))?;
    let restored_sources = source_tables(program, &snapshot);
    if restored_sources != sources_at_backup {
        return Err(RunError::RestoreDiffers {
            op_index: k,
            at_backup: sources_at_backup,
            restored: restored_sources,
        });
    }
    if let Some((def_target, report)) =
        check_defs(&restored_pool, program, &program.defs, &snapshot)
            .await
            .map_err(RunError::Oracle)?
    {
        return Err(RunError::Diverged(Divergence {
            op_index: k,
            def_target,
            report,
        }));
    }

    // The replay of `k+1..n`.
    for (op_index, op) in program.ops.iter().enumerate().skip(k + 1) {
        apply_and_check_outcome(&mut restored, op_index, op, false).await?;
        quiesce_and_check_not_paused(&mut restored, op_index).await?;
        quiesce_snapshot_and_check(
            &mut restored,
            &restored_pool,
            program,
            op_index,
            &program.defs,
            false,
        )
        .await?;
    }

    restored
        .stop_engine()
        .await
        .map_err(|e| restore_error(last, "stop the restored engine", e))?;
    Ok(Outcome::Ran)
}

/// Waits for the restored engine to catch up with `op_index`, then runs
/// [`check_no_pause_after_restore`]. A `paused` transform counts as caught up
/// (quiescing treats it as terminal), so this can't hang on one.
async fn quiesce_and_check_not_paused(
    restored: &mut ManualBackend,
    op_index: usize,
) -> Result<(), RunError> {
    restored
        .quiesce()
        .await
        .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;
    let statuses = restored
        .definition_statuses()
        .await
        .map_err(|e| restore_error(op_index, "read definition statuses", e))?;
    check_no_pause_after_restore(op_index, statuses)
}

fn restore_error(op_index: usize, step: &'static str, error: impl std::fmt::Debug) -> RunError {
    RunError::Restore {
        op_index,
        step,
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
    fn no_transform_paused_passes() {
        check_no_pause_after_restore(2, statuses(&[("d0", "live"), ("d1", "live")]))
            .expect("nothing paused");
    }

    #[test]
    #[should_panic(expected = "over no transforms proves nothing")]
    fn an_empty_status_list_is_not_a_vacuous_pass() {
        let _ = check_no_pause_after_restore(2, Vec::new());
    }

    /// The control: a paused transform is reported by name.
    #[test]
    fn a_paused_transform_is_reported() {
        match check_no_pause_after_restore(4, statuses(&[("d0", "live"), ("d1", "paused")])) {
            Err(RunError::PausedAfterRestore { op_index, paused }) => {
                assert_eq!(op_index, 4);
                assert_eq!(paused, statuses(&[("d1", "paused")]));
            }
            other => panic!("expected PausedAfterRestore, got {other:?}"),
        }
    }
}
