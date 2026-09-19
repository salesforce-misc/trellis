//! Drives a [`crate::model::Program`] through a [`crate::backend::Backend`]
//! and asserts the convergence property (design doc §4): install, then for
//! each op apply → quiesce → snapshot → compare the materialized target
//! against the [`crate::oracle`].
//!
//! The comparison is **per op**, not just once at the end, so a proptest
//! shrink localizes a failure to the *first* diverging op (design doc §4). An
//! op the backend *rejects* is not swallowed: the loop still quiesces,
//! snapshots, and compares afterward. A rejected DML statement leaves the
//! source unchanged, so the SQL oracle (which recomputes from the real source)
//! and the maintained target must still agree — "an op that errors changed
//! nothing" (design doc §4).
//!
//! This module names no cluster/harness types (`testkit`) and no generator
//! (`proptest`): it ties `backend` + `oracle` + `model` together over the
//! [`Backend`] trait and an [`trellis::Pool`], so any backend and any source of
//! programs can reuse it.

mod coverage;
mod noise;

pub use coverage::Coverage;
pub use noise::run_convergence_with_noise;

use std::collections::HashMap;
use std::fmt;

use trellis::Pool;
use trellis::dev::defs::ast::{TransformDef, ValueType};

use crate::backend::{Backend, Snapshot};
use crate::model::{Op, OpOutcome, Program};
use crate::oracle::{self, ThreeWayReport};

/// Classifies whether a property run counted as evidence at all (design doc
/// §6 "What keeps a green run meaningful"). This is deliberately a *separate*
/// question from whether the property held: [`run_convergence`] answers
/// `Outcome::Ran` for both a converged and a diverged program (the divergence
/// itself still fails the test via `Err(RunError::Diverged)`) — `Outcome`
/// exists to rule out the harness reporting green while never actually
/// exercising the backend.
///
/// [`Outcome::as_pass`] is the single decision point every property must
/// assert through, so a future variant can't quietly become "green" by
/// skipping it.
#[derive(Debug)]
pub enum Outcome {
    /// The backend was provisioned, stood up, and driven through a full
    /// run.
    Ran,
    /// Nothing was available to provision — genuinely inconclusive, and
    /// must never be treated as a pass.
    ///
    /// `testkit` self-provisions its own cluster for every run, so no code
    /// path in this crate currently produces this variant. It is kept so
    /// the classifier stays exhaustive/complete if an external-cluster mode
    /// (point the suite at an existing database instead of a disposable
    /// one) is ever added — see the design doc §6 and issue #4's open
    /// question.
    #[allow(dead_code)]
    Unavailable { reason: String },
    /// The backend was provisioned but could not stand up (e.g. the engine
    /// failed to connect or migrate). Carries the engine's error text; must
    /// never be a pass.
    BackendUnusable { error: String },
}

impl Outcome {
    /// The one place a property decides pass/fail from an `Outcome`. Only
    /// `Ran` passes.
    pub fn as_pass(&self) -> bool {
        matches!(self, Outcome::Ran)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Ran => write!(f, "Ran"),
            Outcome::Unavailable { reason } => write!(f, "Unavailable: {reason}"),
            Outcome::BackendUnusable { error } => write!(f, "BackendUnusable: {error}"),
        }
    }
}

/// Classifies a backend stand-up attempt (e.g. connecting/migrating a fresh
/// backend against a provisioned database) into an [`Outcome`]-shaped
/// result: `Ok` on success, or `Err(Outcome::BackendUnusable)` carrying the
/// engine's error text on failure. Callers that need the value on success
/// (not just whether it stood up) use this directly; callers that only care
/// about the classification can fold the `Result` into an `Outcome` with
/// `unwrap_or_else`/`match`.
pub fn classify_stand_up<T, E: fmt::Debug>(result: Result<T, E>) -> Result<T, Outcome> {
    result.map_err(|error| Outcome::BackendUnusable {
        error: format!("{error:?}"),
    })
}

/// A per-op convergence divergence: the op whose settled state disagreed with
/// the oracle, which definition's target it was, and the localized
/// [`ThreeWayReport`].
#[derive(Debug)]
pub struct Divergence {
    /// Index into `program.ops` of the op after which the divergence was
    /// observed.
    pub op_index: usize,
    /// The diverging definition's target table.
    pub def_target: String,
    pub report: ThreeWayReport,
}

/// Why a convergence run stopped. Backend and oracle errors are rendered to
/// strings so this stays free of the backend's associated error type.
#[derive(Debug)]
pub enum RunError {
    /// A definition or schema install was rejected — a hard failure, never a
    /// skip (design doc §3): the generator emits only valid programs, so a
    /// rejection is a generator or engine bug.
    Install(String),
    Quiesce(String),
    Snapshot(String),
    /// The oracle itself failed to compute (a DB/connection error), distinct
    /// from a divergence it successfully found.
    Oracle(String),
    /// The materialized state disagreed with the oracle after some op.
    Diverged(Divergence),
    /// An op's actual `apply()` outcome did not match what the generator
    /// expected of it (design doc §4 "operation errors are checked, not
    /// swallowed") — e.g. a `DuplicateInsert` the generator built to always
    /// collide with a seeded pk instead succeeded, meaning the backend's
    /// primary-key constraint (or the generator's own assumptions about it)
    /// silently stopped holding.
    UnexpectedOpOutcome {
        op_index: usize,
        expected: OpOutcome,
        actual: OpOutcome,
    },
    /// A scheduled [`Backend::restart`] (improvement-plan task E3) failed.
    Restart(String),
    /// A scheduled [`Backend::scale_out`] (improvement-plan task E3) failed.
    ScaleOut(String),
}

/// Installs `program`, then applies each op and checks convergence after it.
///
/// The `pool` is the oracle's read handle into the same database `backend`
/// drives; it must have its `search_path` pinned to that schema (as
/// `testkit`'s isolated-database pool does).
///
/// Returns `Ok(Outcome::Ran)` once the full loop has executed — whether or
/// not the property held, since a divergence is a separate, still-fatal
/// condition surfaced via `Err(RunError::Diverged)`. There is no path to an
/// `Ok` outside having driven the backend through the whole program: the
/// single decision point every property asserts through
/// ([`Outcome::as_pass`]) can't be satisfied by quietly doing nothing.
pub async fn run_convergence<B: Backend>(
    backend: &mut B,
    pool: &Pool,
    program: &Program,
) -> Result<Outcome, RunError> {
    // Improvement-plan workstream C, task C2: opt-in per-call timing around
    // each phase of the loop below, to find out where the suite's wall-clock
    // time actually goes (install, apply, snapshot, oracle recompute) rather
    // than guessing — see `local_docs/generative-suite-improvement-plan.md`
    // "C2" and its companion §1.3's cautionary tale about tuning before
    // instrumenting. Same convention as C1's `GENERATIVE_QUIESCE_TIMING` in
    // `crate::backend::manual::ManualBackend::quiesce`: an independent env
    // var (`GENERATIVE_COST_TIMING`), checked once per call site, silent and
    // free unless set.
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();

    // Improvement-plan task E2: only the definitions due at op index `n`
    // (`def_install_after_op[i] == n`) — `n == 0` is every pre-E2 program's
    // only value, so this reduces to "every definition" for those.
    let due_defs = |n: usize| -> Vec<TransformDef> {
        program
            .defs
            .iter()
            .zip(program.def_install_after_op.iter())
            .filter(|(_, at)| **at == n)
            .map(|(def, _)| def.clone())
            .collect()
    };

    let start = timing_enabled.then(std::time::Instant::now);
    let initial_defs = due_defs(0);
    let install_result = backend
        .install(&Program {
            tables: program.tables.clone(),
            // Issue #34: every relationship installs once, up front,
            // alongside the tables — ahead of *any* definition, including
            // one deferred to mid-stream (task E2), since a definition
            // naming an undeclared relationship is rejected at validation
            // time and an install rejection is a hard failure, never a skip.
            relationships: program.relationships.clone(),
            defs: initial_defs.clone(),
            def_install_after_op: vec![0; initial_defs.len()],
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        })
        .await;
    if let Some(start) = start {
        eprintln!("COST_TIMING install {}", start.elapsed().as_millis());
    }
    install_result.map_err(|e| RunError::Install(format!("{e:?}")))?;
    // Definitions actually installed so far, in the order they were
    // installed — checked against the oracle below instead of blindly
    // `program.defs` (task E2's whole point: a deferred definition must not
    // be checked, or even looked up in the snapshot, before its own install
    // point is reached).
    let mut installed_defs = initial_defs;

    for (op_index, op) in program.ops.iter().enumerate() {
        if op_index > 0 {
            // Improvement-plan task E2: install whatever became due right
            // before this op runs.
            let due = due_defs(op_index);
            if !due.is_empty() {
                backend
                    .install(&Program {
                        tables: Vec::new(),
                        // Already installed with the tables above.
                        relationships: Vec::new(),
                        defs: due.clone(),
                        def_install_after_op: vec![0; due.len()],
                        ops: Vec::new(),
                        restart_after_ops: Vec::new(),
                        scale_out_after_ops: Vec::new(),
                    })
                    .await
                    .map_err(|e| RunError::Install(format!("{e:?}")))?;
                installed_defs.extend(due);
            }
            // Improvement-plan task E3: a scheduled restart/scale-out fires
            // right before the op it's anchored to, same as a deferred
            // definition install above.
            if program.restart_after_ops.contains(&op_index) {
                backend
                    .restart()
                    .await
                    .map_err(|e| RunError::Restart(format!("{e:?}")))?;
            }
            if program.scale_out_after_ops.contains(&op_index) {
                backend
                    .scale_out()
                    .await
                    .map_err(|e| RunError::ScaleOut(format!("{e:?}")))?;
            }
        }

        apply_and_check_outcome(backend, op_index, op, timing_enabled).await?;
        // Improvement-plan task E2: only the definitions installed so far are
        // checked — a deferred definition must not be looked up in the
        // snapshot (or checked against the oracle) before its own install
        // point is reached.
        quiesce_snapshot_and_check(
            backend,
            pool,
            program,
            op_index,
            &installed_defs,
            timing_enabled,
        )
        .await?;
    }

    Ok(Outcome::Ran)
}

/// Applies one op and checks its actual [`crate::backend::Backend::apply`]
/// outcome against what the generator expected of it (design doc §4 "operation
/// errors are checked, not swallowed"). Shared by [`run_convergence`] (which
/// quiesces and compares after every single call to this) and
/// [`run_convergence_bursty`] (which calls this several times in a row before
/// quiescing/comparing once) — the outcome check itself is identical either
/// way; only *when* the surrounding loop quiesces differs.
///
/// A rejected op is a source no-op, not a skip: the caller still quiesces and
/// compares afterward regardless of what this returns on the `Ok` path.
async fn apply_and_check_outcome<B: Backend>(
    backend: &mut B,
    op_index: usize,
    op: &Op,
    timing_enabled: bool,
) -> Result<(), RunError> {
    let start = timing_enabled.then(std::time::Instant::now);
    let apply_result = backend.apply(op).await;
    if let Some(start) = start {
        eprintln!("COST_TIMING apply {}", start.elapsed().as_millis());
    }
    let actual = match apply_result {
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
    Ok(())
}

/// Quiesces, snapshots, and runs the three-way oracle check against `defs`,
/// reporting a divergence (if any) localized to `op_index` — the shared tail
/// end of both [`run_convergence`]'s per-op loop and
/// [`run_convergence_bursty`]'s per-burst loop. See
/// [`apply_and_check_outcome`]'s doc comment for the division of labor
/// between the two.
///
/// `defs` is taken explicitly, rather than always checking `program.defs` in
/// full, because of improvement-plan task E2: [`run_convergence`] passes only
/// the definitions installed *so far* (a deferred definition must not be
/// checked — or even looked up in the snapshot — before its own install
/// point is reached), while [`run_convergence_bursty`] (which does not
/// support mid-stream installs) always passes `program.defs` in full.
async fn quiesce_snapshot_and_check<B: Backend>(
    backend: &mut B,
    pool: &Pool,
    program: &Program,
    op_index: usize,
    defs: &[TransformDef],
    timing_enabled: bool,
) -> Result<(), RunError> {
    backend
        .quiesce()
        .await
        .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;

    let start = timing_enabled.then(std::time::Instant::now);
    let snapshot = backend.snapshot().await;
    if let Some(start) = start {
        eprintln!("COST_TIMING snapshot {}", start.elapsed().as_millis());
    }
    let snapshot = snapshot.map_err(|e| RunError::Snapshot(format!("{e:?}")))?;

    let start = timing_enabled.then(std::time::Instant::now);
    let checked = check_defs(pool, program, defs, &snapshot).await;
    if let Some(start) = start {
        eprintln!("COST_TIMING oracle {}", start.elapsed().as_millis());
    }
    if let Some((def_target, report)) = checked.map_err(RunError::Oracle)? {
        return Err(RunError::Diverged(Divergence {
            op_index,
            def_target,
            report,
        }));
    }
    Ok(())
}

/// Improvement-plan task D4 ("a second runtime"): like [`run_convergence`],
/// but applies `program`'s ops in fixed-size **bursts** of up to `burst_size`
/// ops back-to-back, with no [`Backend::quiesce`] call between them within a
/// burst — quiescing, snapshotting, and running the three-way oracle check
/// only once per burst, after its last op, instead of after every single op.
///
/// This exists to make a multi-worker [`Backend`] (e.g.
/// `crate::backend::ManualBackend::connect_with_workers`) actually exercise
/// concurrent, multi-row draining: `run_convergence`'s strict apply -> quiesce
/// loop never lets more than one row's change be in flight at once, so a
/// naive N-worker backend driven that way mostly tests "N idle-ish workers
/// don't duplicate/corrupt a single claim" rather than a real batch getting
/// split and drained by several workers at once
/// (`trellis::dev::staging::claim`'s bucket-splitting only kicks in above
/// `MIN_ROWS_TO_SPLIT` rows sealed in one batch). Letting several ops land
/// before the harness ever asks the engine to catch up gives a real batch a
/// chance to accumulate.
///
/// The ops within a burst are still applied strictly in `program.ops`'s own
/// generated order, one at a time, each one's actual outcome still checked
/// against [`Op::expect`] via [`apply_and_check_outcome`] — this is **not**
/// the harness racing/interleaving ops against each other (that's a
/// deliberately out-of-scope follow-up; see
/// `generative/tests/concurrent_convergence.rs`'s module doc comment). It is
/// only a change to *when* `quiesce()` is called, nothing about *what* is
/// applied or in what order.
///
/// **Divergence localization is coarser than `run_convergence`'s.** A
/// divergence found here is only known to have appeared somewhere within the
/// offending burst — the [`Divergence::op_index`] recorded is the burst's
/// *last* op, not necessarily the one that actually caused it, since nothing
/// was checked in between. See the module doc comment on
/// `generative/tests/concurrent_convergence.rs` for the shrink-trust
/// convention this implies: hand-transcribe a burst failure into a
/// `ManualBackend`-driven (single-worker, `burst_size` 1) hand-built pin to
/// find out whether it's a true concurrency bug or reproduces under the
/// manual runtime too.
///
/// Panics if `burst_size` is `0` — there is no such thing as a burst of zero
/// ops, and silently treating it as "never quiesce" would just hang at
/// `quiesce()`'s timeout instead of failing fast.
pub async fn run_convergence_bursty<B: Backend>(
    backend: &mut B,
    pool: &Pool,
    program: &Program,
    burst_size: usize,
) -> Result<Outcome, RunError> {
    assert!(
        burst_size > 0,
        "run_convergence_bursty: burst_size must be at least 1"
    );
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();

    let start = timing_enabled.then(std::time::Instant::now);
    let install_result = backend.install(program).await;
    if let Some(start) = start {
        eprintln!("COST_TIMING install {}", start.elapsed().as_millis());
    }
    install_result.map_err(|e| RunError::Install(format!("{e:?}")))?;

    let mut op_index = 0usize;
    for burst in program.ops.chunks(burst_size) {
        for op in burst {
            apply_and_check_outcome(backend, op_index, op, timing_enabled).await?;
            op_index += 1;
        }
        let last_index_in_burst = op_index - 1;
        quiesce_snapshot_and_check(
            backend,
            pool,
            program,
            last_index_in_burst,
            &program.defs,
            timing_enabled,
        )
        .await?;
    }

    Ok(Outcome::Ran)
}

/// Runs the three-way oracle check for every definition in `program` against
/// `snapshot`, returning the first `(target, report)` that diverged, or `None`
/// if all converged.
///
/// Exposed (not just used by [`run_convergence`]) so a red/control test can
/// feed it a deliberately corrupted snapshot and confirm the harness reports
/// the divergence — proving it tells green from red (design doc §6/§7).
///
/// A thin wrapper around [`check_defs`] for `program.defs` in full — every
/// caller that predates improvement-plan task E2 (every definition installs
/// up front) wants exactly that. [`run_convergence`] itself calls
/// [`check_defs`] directly with only the definitions installed *so far*,
/// since a deferred definition (task E2) must not be checked — or even
/// looked up in the snapshot — before its own install point is reached.
pub async fn check_program(
    pool: &Pool,
    program: &Program,
    snapshot: &Snapshot,
) -> Result<Option<(String, ThreeWayReport)>, String> {
    check_defs(pool, program, &program.defs, snapshot).await
}

/// [`check_program`]'s general form (improvement-plan task E2): runs the
/// three-way oracle check for every definition in `defs` (not necessarily all
/// of `program.defs` — see [`run_convergence`]'s use of this for a partially-
/// installed program) against `snapshot`, returning the first `(target,
/// report)` that diverged, or `None` if all converged. `program` is still
/// needed for its `tables` (to resolve each definition's source schema).
pub async fn check_defs(
    pool: &Pool,
    program: &Program,
    defs: &[TransformDef],
    snapshot: &Snapshot,
) -> Result<Option<(String, ThreeWayReport)>, String> {
    for def in defs {
        let source = program
            .tables
            .iter()
            .find(|t| t.name == def.source)
            .unwrap_or_else(|| {
                panic!(
                    "program references source table {:?} it does not declare — a generator bug",
                    def.source
                )
            });
        let source_columns: HashMap<String, ValueType> = source
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.value_type))
            .collect();

        let target = snapshot.get(&def.target).ok_or_else(|| {
            format!(
                "target table {:?} missing from snapshot — the backend did not install it",
                def.target
            )
        })?;

        let report = oracle::check(pool, program, def, &source.pk_col, &source_columns, target)
            .await
            .map_err(|e| format!("{e:?}"))?;
        if report.diverged() {
            return Ok(Some((def.target.clone(), report)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::Outcome;

    /// Design doc §6 / issue #4: only `Ran` passes. Fast, no database —
    /// this is the classifier logic itself, not the harness sanity check
    /// (that meta-test lives in `tests/meta.rs` and needs a real cluster).
    #[test]
    fn only_ran_passes() {
        assert!(Outcome::Ran.as_pass());
        assert!(
            !Outcome::Unavailable {
                reason: "nothing to provision".to_string(),
            }
            .as_pass()
        );
        assert!(
            !Outcome::BackendUnusable {
                error: "connection refused".to_string(),
            }
            .as_pass()
        );
    }
}
