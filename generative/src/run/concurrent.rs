//! The concurrent tier's runner (issue #557): a [`ConcurrentPlan`]'s lanes
//! issued from separate tasks, each on its own connection, against a
//! multi-worker engine.

use tokio::task::JoinSet;
use trellis::Pool;

use super::{Outcome, RunError, check_op_outcome, quiesce_snapshot_and_check};
use crate::backend::{ConcurrentBackend, DrainAudit, OpApplier};
use crate::model::{ConcurrentPlan, Op, Program};

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
/// every lane has finished, the engine is quiesced and every definition is
/// checked against the oracle, once per burst.
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
/// # Panics
///
/// If `plan` doesn't cover `program.ops` exactly once, or a lane isn't in
/// program order: both are generator bugs (`crate::generate::concurrent_plan`
/// builds every plan).
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
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();

    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;
    backend
        .start_drain_audit()
        .await
        .map_err(|e| RunError::Install(format!("drain audit: {e:?}")))?;

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

        let mut tasks = JoinSet::new();
        for (lane_index, lane) in burst.lanes.iter().enumerate() {
            let mut applier = appliers[lane_index]
                .take()
                .expect("each lane's applier is returned before the next burst");
            let ops: Vec<(usize, Op)> = lane.iter().map(|&i| (i, program.ops[i].clone())).collect();
            tasks.spawn(async move {
                let mut result = Ok(());
                for (op_index, op) in &ops {
                    let applied = applier.apply(op).await;
                    if let Err(e) = check_op_outcome(*op_index, op, &applied) {
                        result = Err(e);
                        break;
                    }
                }
                (lane_index, applier, result)
            });
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
        if let Some(e) = first_error {
            return Err(e);
        }

        quiesce_snapshot_and_check(
            backend,
            pool,
            program,
            last_op,
            &program.defs,
            timing_enabled,
        )
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
