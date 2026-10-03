//! The concurrent tier's runner (issue #557): a [`ConcurrentPlan`]'s lanes
//! issued from separate tasks, each on its own connection, against a
//! multi-worker engine.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;
use tokio::task::JoinSet;
use trellis::Pool;
use trellis::dev::defs::ast::TransformDef;

use super::{Outcome, RunError, check_op_outcome, quiesce_snapshot_and_check};
use crate::backend::{ConcurrentBackend, DrainAudit, OpApplier};
use crate::model::{BurstAction, ConcurrentPlan, Op, Program};

/// What [`run_convergence_concurrent`] reports alongside [`Outcome::Ran`]:
/// how the engine actually sealed and claimed the run's batches.
#[derive(Debug)]
pub struct ConcurrentRun {
    pub outcome: Outcome,
    pub drain: DrainAudit,
}

/// Installs `program`, then runs `plan`'s bursts one after another. Within a
/// burst, each lane is one spawned task applying its ops in order on its own
/// [`OpApplier`], concurrently with the other lanes, and checking each op's
/// outcome against [`Op::expect`] as [`super::run_convergence`] does. Once
/// every lane has finished, the engine is quiesced and every definition
/// installed so far is checked against the oracle, once per burst.
///
/// While the lanes run, this task takes the burst's operator actions
/// ([`crate::model::Burst::actions`], issue #557 part 2) in order, each once
/// its [`crate::model::TimedAction::after`] count of the burst's ops has been
/// applied, through [`ConcurrentBackend::act`]. A definition deferred by
/// [`Program::def_install_after_op`] is installed by the plan's
/// [`BurstAction::Install`] rather than up front.
///
/// Like [`super::run_convergence_bursty`], a divergence is only localized to
/// its burst: [`super::Divergence::op_index`] is the burst's highest op
/// index. Unlike it, the order the burst's ops committed in isn't known
/// either, since lanes race. See `tests/concurrent_convergence.rs`'s
/// shrink-trust convention before treating a shrunk case as minimal.
///
/// The drain audit ([`ConcurrentBackend::start_drain_audit`]) is on for the
/// whole run, and its counts come back with the outcome.
///
/// With a [`ConcurrentPlan::steady_load`], the engine's pages and build
/// chunks stall at their entry-lock step for the whole run, and once an
/// action starts a build ([`BurstAction::starts_build`]) each lane waits
/// its pace after every op for the rest of the burst.
///
/// # Panics
///
/// If `plan` doesn't cover `program.ops` exactly once, a lane isn't in
/// program order, or the plan's install actions aren't exactly the
/// program's deferred definitions: each is a generator bug
/// (`crate::generate::concurrent_plan` builds every plan).
pub async fn run_convergence_concurrent<B: ConcurrentBackend>(
    backend: &mut B,
    pool: &Pool,
    program: &Program,
    plan: &ConcurrentPlan,
) -> Result<ConcurrentRun, RunError> {
    let mut covered: Vec<usize> = plan.bursts.iter().flat_map(|b| b.ops()).collect();
    covered.sort_unstable();
    assert!(
        covered.iter().copied().eq(0..program.ops.len()),
        "run_convergence_concurrent: the plan must issue every op exactly once — a generator bug"
    );
    assert!(
        plan.bursts
            .iter()
            .flat_map(|b| &b.lanes)
            .all(|lane| lane.is_sorted()),
        "run_convergence_concurrent: every lane must be in program order — a generator bug"
    );
    let mut installs: Vec<usize> = plan
        .bursts
        .iter()
        .flat_map(|b| &b.actions)
        .filter_map(|a| match a.action {
            BurstAction::Install { def } => Some(def),
            _ => None,
        })
        .collect();
    installs.sort_unstable();
    let deferred: Vec<usize> = (0..program.defs.len())
        .filter(|&def| program.def_install_after_op[def] != 0)
        .collect();
    assert_eq!(
        installs, deferred,
        "run_convergence_concurrent: the plan must install each deferred definition once — a \
         generator bug"
    );
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();

    let mut installed: Vec<TransformDef> = program
        .defs
        .iter()
        .zip(&program.def_install_after_op)
        .filter(|(_, at)| **at == 0)
        .map(|(def, _)| def.clone())
        .collect();
    backend
        .install(&Program {
            tables: program.tables.clone(),
            relationships: program.relationships.clone(),
            defs: installed.clone(),
            def_install_after_op: vec![0; installed.len()],
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        })
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;
    backend
        .start_drain_audit()
        .await
        .map_err(|e| RunError::Install(format!("drain audit: {e:?}")))?;

    // The plan's steady load's stall, if any, holds for the whole run, and
    // is cleared when the run ends, however it ends.
    let _stall = plan.steady_load.map(|load| StallGuard::set(load.stall));
    let pace = plan.steady_load.map(|load| load.pace);

    let lane_count = plan.bursts.iter().map(|b| b.lanes.len()).max().unwrap_or(0);
    let mut appliers = Vec::with_capacity(lane_count);
    for _ in 0..lane_count {
        appliers.push(Some(backend.applier().await.map_err(|e| {
            RunError::Install(format!("open a lane's connection: {e:?}"))
        })?));
    }

    for (index, burst) in plan.bursts.iter().enumerate() {
        let Some(last_op) = burst.last_op() else {
            continue;
        };
        backend
            .begin_burst(index)
            .await
            .map_err(|e| RunError::Quiesce(format!("drain audit: {e:?}")))?;

        // How many of the burst's ops the lanes have applied so far, which
        // is what an action waits on. A lane that stops at a failed op
        // counts the rest of its ops as applied, so no action waits forever.
        let progress = Arc::new(watch::channel(0usize).0);
        // Set once one of the burst's actions starts a build: from then on,
        // under a steady load, the lanes keep to its pace.
        let building = Arc::new(AtomicBool::new(false));
        let mut tasks = JoinSet::new();
        for (lane_index, lane) in burst.lanes.iter().enumerate() {
            let building = Arc::clone(&building);
            let mut applier = appliers[lane_index]
                .take()
                .expect("each lane's applier is returned before the next burst");
            let ops: Vec<(usize, Op)> = lane.iter().map(|&i| (i, program.ops[i].clone())).collect();
            let progress = Arc::clone(&progress);
            tasks.spawn(async move {
                let mut result = Ok(());
                for (done, (op_index, op)) in ops.iter().enumerate() {
                    let applied = applier.apply(op).await;
                    if let Err(e) = check_op_outcome(*op_index, op, &applied) {
                        result = Err(e);
                        progress.send_modify(|n| *n += ops.len() - done);
                        break;
                    }
                    progress.send_modify(|n| *n += 1);
                    if let Some(pace) = pace.filter(|_| building.load(Ordering::Relaxed)) {
                        tokio::time::sleep(pace).await;
                    }
                }
                (lane_index, applier, result)
            });
        }

        let mut action_error: Option<RunError> = None;
        let mut applied = progress.subscribe();
        for timed in &burst.actions {
            applied
                .wait_for(|&n| n >= timed.after)
                .await
                .expect("this task holds the sender");
            if timed.action.starts_build() {
                building.store(true, Ordering::Relaxed);
            }
            if let Err(e) = backend.act(program, &timed.action).await {
                action_error = Some(RunError::BurstAction {
                    burst: index,
                    action: timed.action.clone(),
                    error: format!("{e:?}"),
                });
                break;
            }
            if let BurstAction::Install { def } = timed.action {
                installed.push(program.defs[def].clone());
            }
        }

        // Every lane runs to completion (or its own first failure) before
        // any error is reported, so no task outlives the burst.
        let mut first_error: Option<RunError> = None;
        while let Some(joined) = tasks.join_next().await {
            let (lane_index, applier, result) = joined.expect("a lane task panicked");
            appliers[lane_index] = Some(applier);
            if let Err(e) = result {
                let earlier = match (&first_error, &e) {
                    (
                        Some(RunError::UnexpectedOpOutcome { op_index: a, .. }),
                        RunError::UnexpectedOpOutcome { op_index: b, .. },
                    ) => b < a,
                    (None, _) => true,
                    _ => false,
                };
                if earlier {
                    first_error = Some(e);
                }
            }
        }
        if let Some(e) = first_error.or(action_error) {
            return Err(e);
        }

        quiesce_snapshot_and_check(backend, pool, program, last_op, &installed, timing_enabled)
            .await?;
    }

    let drain = backend
        .drain_audit()
        .await
        .map_err(|e| RunError::Snapshot(format!("drain audit: {e:?}")))?;
    Ok(ConcurrentRun {
        outcome: Outcome::Ran,
        drain,
    })
}

/// Sets a stall (`trellis::dev::interleave::set_stall`) for as long as it
/// lives.
struct StallGuard;

impl StallGuard {
    fn set(stall: std::time::Duration) -> Self {
        trellis::dev::interleave::set_stall(Some(stall));
        StallGuard
    }
}

impl Drop for StallGuard {
    fn drop(&mut self) {
        trellis::dev::interleave::set_stall(None);
    }
}
