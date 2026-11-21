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
//! [`Backend`] trait and an [`trellis::dev::pool::Pool`], so any backend and any source of
//! programs can reuse it.

mod concurrent;
mod coverage;
mod db_admin;
mod epoch;
mod held_keys;
mod hidden;
pub mod judge;
mod noise;
mod pause;
pub mod privilege;
mod residue;
mod restore;
mod source_ddl;
mod two_instance;

pub use concurrent::{
    ConcurrentRun, run_convergence_concurrent, run_convergence_concurrent_sampled,
};
pub use coverage::{ConcurrentShape, Coverage, HOT_WRITES, key_space_name};
pub use db_admin::run_convergence_with_db_admin;
pub use epoch::run_epochs;
pub use held_keys::{
    DefModel, HeldExpectation, HeldKeySummary, TRANSFORM_FUSE, run_convergence_with_held_keys,
    run_convergence_with_isolation,
};
pub use hidden::{HiddenReport, SETTLE, check_hidden};
pub use noise::run_convergence_with_noise;
pub use residue::{
    LAP_SEAL_INTERVAL, ResidueReport, TableResidue, check_residue, peak_live_rows,
    run_epochs_lapping,
};
pub use restore::{check_no_pause_after_restore, run_convergence_with_restore};
pub use source_ddl::{
    SourceDdlRun, run_convergence_with_source_ddl, run_convergence_with_source_ddl_allowing,
};
pub use two_instance::{
    InstanceLabel, InstanceRun, TwoInstanceError, chain_off, run_two_instance_convergence,
    share_source,
};

use std::collections::HashMap;
use std::fmt;

use trellis::dev::defs::ast::{TransformDef, ValueType};
use trellis::dev::pool::Pool;

use crate::backend::{Backend, DefinitionState, Snapshot};
use crate::model::{BurstAction, DbAdminAction, Op, OpOutcome, Program};
use crate::oracle::{self, OracleReport};

use self::pause::{RunKind, allowance, check_no_unexpected_pause};

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
    /// one) is ever added — see the design doc §6.
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
/// the oracle, which definitions' targets it was, and each one's localized
/// [`OracleReport`].
#[derive(Debug)]
pub struct Divergence {
    /// Index into `program.ops` of the op after which the divergence was
    /// observed.
    pub op_index: usize,
    /// The first diverging definition's target table, in definition order.
    pub def_target: String,
    /// Boxed so [`RunError`] stays small enough to return by value.
    pub report: Box<OracleReport>,
    /// Every other definition whose target diverged at the same check, in
    /// definition order: a sweep attributing a failure to a planted
    /// bug needs every diverging target, not just the first, or an unplanted
    /// divergence earlier in definition order hides the plant's own.
    pub also_diverged: Vec<(String, OracleReport)>,
    /// Every checked definition's catalog status and pause reason when the
    /// divergence was found, so a target that stopped following its source
    /// shows why. Empty only if the catalog read itself failed.
    pub states: Vec<DefinitionState>,
    /// The diverging targets whose definition reports `live` with nothing
    /// recorded against it: "status tells the truth" fails for them, since
    /// `live` promises an exact target ([`status_lies`]).
    pub status_lies: Vec<String>,
}

impl Divergence {
    /// The divergence [`check_defs`]' result describes at `op_index`, or
    /// `None` if every target converged.
    pub fn from_checked(
        op_index: usize,
        checked: Vec<(String, OracleReport)>,
        states: Vec<DefinitionState>,
    ) -> Option<Self> {
        let lies: Vec<String> = status_lies(&states, |target| {
            checked.iter().any(|(diverged, _)| diverged == target)
        })
        .into_iter()
        .map(|state| state.target.clone())
        .collect();
        let lines: Vec<String> = states
            .iter()
            .map(|state| {
                if lies.contains(&state.target) {
                    format!("{state} <- status lies: reports live, target isn't exact")
                } else {
                    state.to_string()
                }
            })
            .collect();
        let mut checked = checked
            .into_iter()
            .map(|(target, report)| (target, report.with_definition_states(lines.clone())));
        let (def_target, report) = checked.next()?;
        Some(Divergence {
            op_index,
            def_target,
            report: Box::new(report),
            also_diverged: checked.collect(),
            states,
            status_lies: lies,
        })
    }

    /// Every diverging target and its report, the first one included, in
    /// definition order.
    pub fn all(&self) -> impl Iterator<Item = (&str, &OracleReport)> {
        std::iter::once((self.def_target.as_str(), &*self.report)).chain(
            self.also_diverged
                .iter()
                .map(|(target, report)| (target.as_str(), report)),
        )
    }

    /// Every diverging target, the first one included, in definition order.
    pub fn targets(&self) -> impl Iterator<Item = &str> {
        self.all().map(|(target, _)| target)
    }
}

/// "Status tells the truth" (#2185): the definitions of `states` that
/// claim an exact target ([`DefinitionState::claims_exact`]: reported `live`,
/// nothing held, no failure recorded) while `inexact` says their target
/// isn't. Every settled check runs it: [`Divergence::from_checked`] names
/// them in the report, and a run that excuses a target from the oracle
/// (the isolation draw's failing definition) fails on one
/// ([`RunError::StatusLies`]). A check follows a quiesce, so a window in
/// which the harness itself took Postgres or the engine away is over by
/// then; no other window is exempt.
pub fn status_lies(
    states: &[DefinitionState],
    inexact: impl Fn(&str) -> bool,
) -> Vec<&DefinitionState> {
    states
        .iter()
        .filter(|state| state.claims_exact() && inexact(&state.target))
        .collect()
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
        /// The statement's error, when it failed.
        error: Option<String>,
    },
    /// A definition was `paused` at a settled check in a run that never
    /// expects one (the allowlist is `pause::allowance`). `message` is one
    /// `unexpected pause: <definition>: <reason>` line per paused definition.
    UnexpectedPause {
        op_index: usize,
        message: String,
        paused: Vec<DefinitionState>,
    },
    /// After op `op_index` of a held-key run
    /// ([`run_convergence_with_held_keys`]), definition `target` reported
    /// other held keys, or another status, than the held-key model expects.
    HeldKeys {
        op_index: usize,
        target: String,
        expected: String,
        reported: String,
        program: String,
    },
    /// After op `op_index`, a definition whose target the run excuses from
    /// the oracle (it was made to fail) reported `live` with nothing
    /// recorded against it ([`status_lies`]).
    StatusLies {
        op_index: usize,
        state: DefinitionState,
    },
    /// After op `op_index` of an isolation run, the definition made to fail
    /// in combination (`target`) didn't end as the engine's isolation
    /// promises: paused with a T0247 reason naming the page's SQLSTATE and
    /// the pair's keys, its target as it was before the pair. Or, in any
    /// held-key run (`target` `<ring>`), the ring didn't retire once the run
    /// settled. Another definition paused beside it is
    /// [`RunError::UnexpectedPause`].
    Isolation {
        op_index: usize,
        target: String,
        problem: String,
        program: String,
    },
    /// A scheduled [`Backend::restart`] failed.
    Restart(String),
    /// A scheduled [`Backend::scale_out`] failed.
    ScaleOut(String),
    /// Carrying out a database-administration action failed
    /// (the harness's side of it: restarting the server and reconnecting).
    DbAdmin {
        op_index: usize,
        action: DbAdminAction,
        error: String,
    },
    /// Carrying out a backup or restore failed (the harness's
    /// side of it: reconnecting after the backup's stop, stopping the
    /// original engine, starting one on the restored database). `step`
    /// names which.
    Restore {
        op_index: usize,
        step: &'static str,
        error: String,
    },
    /// The restored database's source tables are not what the
    /// original's were after the backup op `op_index`, so the restore itself
    /// lost or kept something it shouldn't have. Only the source tables:
    /// the targets are judged by the oracle.
    RestoreDiffers {
        op_index: usize,
        at_backup: Snapshot,
        restored: Snapshot,
    },
    /// After a cold-copy restore these `(target, status)`
    /// definitions were `paused` once the engine caught up with `op_index`.
    /// Nothing about the restore warrants a pause.
    PausedAfterRestore {
        op_index: usize,
        paused: Vec<(String, String)>,
    },
    /// The engine refused, or the harness failed to
    /// carry out, a mid-burst operator action in burst `burst`. The plan
    /// only draws actions that apply (`crate::generate::add_burst_actions`),
    /// so this is a generator, harness or engine bug.
    BurstAction {
        burst: usize,
        action: BurstAction,
        error: String,
    },
    /// The oracle agreed with every target after burst
    /// `burst`, but a `Strict` `self_check` of a target found a divergence
    /// (a hidden `P`/`M`/`J`/`T` table off) or could not finish.
    Audit {
        burst: usize,
        failures: Vec<String>,
    },
    /// A sample of the in-run hidden-state invariant
    /// ([`crate::invariant`]) during burst `burst` (or, with `settled`,
    /// once it settled and the oracle passed it) found a 1-1 `JOIN`'s hidden
    /// state inconsistent at a committed snapshot, or couldn't read it.
    InRunInvariant {
        burst: usize,
        settled: bool,
        failure: Result<crate::invariant::Failure, String>,
    },
    /// A drawn transaction (#2181) whose first op is `op_index` couldn't
    /// commit: an error outside its ops' own statements, or a serialization
    /// failure every retry hit.
    Transaction {
        op_index: usize,
        error: String,
    },
    /// Transactions still prepared (#2181, 2d) when the oracle was about to
    /// read the source: the harness resolves each as the program says before
    /// it quiesces, so one left is a harness bug, or a prepared transaction
    /// the run lost track of. Their gids.
    LeftPrepared(Vec<String>),
    /// Epoch `epoch` of an epoch chain ([`run_epochs`]) failed with
    /// `error`. Epoch `0` is the chain's first program.
    Epoch {
        epoch: usize,
        error: Box<RunError>,
    },
    /// After op `op_index`, definition `target` ended neither exact, nor
    /// paused with a reason and exact after its `RESUME` (or refused by it
    /// with a code), nor a listed gap the drawn shape names, detected by
    /// `self_check` and made exact by the entry's repair ([`judge`]).
    /// `problem` says which step broke; `detail` holds the statuses and the
    /// oracle's report.
    NeverSilentlyWrong {
        op_index: usize,
        target: String,
        problem: String,
        detail: String,
    },
    /// Carrying out a source-DDL event failed (the harness's side of it: a
    /// statement the generator drew that Postgres refused, or a reconcile
    /// pass the harness stepped).
    SourceDdl {
        op_index: usize,
        event: Box<crate::model::SourceDdlEvent>,
        error: String,
    },
    /// Op `op_index`'s application statement failed with an error raised
    /// inside one of Trellis's capture functions: capture failed a writer
    /// (ADR-0002 I6, #2182). Checked whatever the op was expected to do, so
    /// an op expected to fail can't hide it.
    CaptureFailedWriter {
        op_index: usize,
        error: String,
    },
    /// A residue invariant ([`residue`]) failed at a quiet point: a Trellis
    /// table holds dead tuples after `VACUUM`, or has grown past what its
    /// peak live rows explain. One line per table.
    Residue {
        failures: Vec<String>,
    },
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
    // Opt-in per-call timing around
    // each phase of the loop below, to find out where the suite's wall-clock
    // time actually goes (install, apply, snapshot, oracle recompute) rather
    // than guessing: measure before tuning. Same convention as
    // `GENERATIVE_QUIESCE_TIMING` in
    // `crate::backend::manual::ManualBackend::quiesce`: an independent env
    // var (`GENERATIVE_COST_TIMING`), checked once per call site, silent and
    // free unless set.
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();

    // Only the definitions due at op index `n`
    // (`def_install_after_op[i] == n`) — `n == 0` is the only value for a program
    // that installs everything up front, so this reduces to "every definition" for those.
    let due_defs = |n: usize| -> Vec<TransformDef> {
        program
            .defs
            .iter()
            .zip(program.def_install_after_op.iter())
            .filter(|(_, at)| **at == n)
            .map(|(def, _)| def.clone())
            .collect()
    };

    backend
        .expect(program)
        .map_err(|e| RunError::Install(format!("{e:?}")))?;
    let start = timing_enabled.then(std::time::Instant::now);
    let initial_defs = due_defs(0);
    let install_result = backend
        .install(&Program {
            tables: program.tables.clone(),
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
    // `program.defs` (a deferred definition must not
    // be checked, or even looked up in the snapshot, before its own install
    // point is reached).
    let mut installed_defs = initial_defs;

    for (op_index, op) in program.ops.iter().enumerate() {
        if op_index > 0 {
            // Install whatever became due right
            // before this op runs.
            let due = due_defs(op_index);
            if !due.is_empty() {
                backend
                    .install(&Program {
                        tables: Vec::new(),
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
            // A scheduled restart/scale-out fires
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
        // Only the definitions installed so far are
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
            RunKind::Sequential,
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
    check_op_outcome(op_index, op, &apply_result)
}

/// The outcome check [`apply_and_check_outcome`] makes, given what applying
/// `op` returned: an `Err` is a rejected statement ([`OpOutcome::Fails`]),
/// `Ok(0)` a source no-op. Shared with the concurrent runner, whose lanes
/// apply on their own connections.
fn check_op_outcome<E: std::fmt::Debug>(
    op_index: usize,
    op: &Op,
    apply_result: &Result<u64, E>,
) -> Result<(), RunError> {
    if let Err(error) = apply_result {
        let error = format!("{error:?}");
        if raised_in_capture(&error) {
            return Err(RunError::CaptureFailedWriter { op_index, error });
        }
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
            error: apply_result.as_ref().err().map(|e| format!("{e:?}")),
        });
    }
    Ok(())
}

/// Whether an error's text (a database error's `Debug`, which carries its
/// `where` context) says it was raised inside a Trellis capture function:
/// a PL/pgSQL frame of a function named `cap_<event>_…` in any schema.
/// A quoted name may arrive escaped once or twice (a wrapped transaction's
/// results are `Debug` of a `Debug`), as may a compile error's
/// `function "…"` context.
pub fn raised_in_capture(error: &str) -> bool {
    error.match_indices("function ").any(|(at, _)| {
        let name = error[at + "function ".len()..]
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()
            .unwrap_or_default();
        let bare = name
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .trim_matches(|c| c == '"' || c == '\\');
        ["insert", "update", "delete", "truncate", "begin"]
            .iter()
            .any(|event| bare.starts_with(&format!("cap_{event}_")))
    })
}

/// Quiesces, snapshots, and runs the oracle check against `defs`,
/// reporting a divergence (if any) localized to `op_index` — the shared tail
/// end of both [`run_convergence`]'s per-op loop and
/// [`run_convergence_bursty`]'s per-burst loop. See
/// [`apply_and_check_outcome`]'s doc comment for the division of labor
/// between the two.
///
/// `defs` is taken explicitly, rather than always checking `program.defs` in
/// full, because [`run_convergence`] passes only
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
    kind: RunKind,
) -> Result<(), RunError> {
    check_none_prepared(pool).await?;
    backend
        .quiesce()
        .await
        .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;
    let states = settled_states(backend, program, op_index, defs, kind).await?;

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
    if let Some(divergence) =
        Divergence::from_checked(op_index, checked.map_err(RunError::Oracle)?, states)
    {
        return Err(RunError::Diverged(divergence));
    }
    Ok(())
}

/// Fails with [`RunError::LeftPrepared`] if a transaction of the generative
/// suite is still prepared in the source's database.
pub async fn check_none_prepared(pool: &Pool) -> Result<(), RunError> {
    let client = pool
        .get()
        .await
        .map_err(|e| RunError::Oracle(format!("read prepared transactions: {e:?}")))?;
    let rows = client
        .query(
            "select gid from pg_prepared_xacts \
             where database = current_database() and gid like $1 order by gid",
            &[&format!("{}%", crate::backend::PREPARED_GID_PREFIX)],
        )
        .await
        .map_err(|e| RunError::Oracle(format!("read prepared transactions: {e:?}")))?;
    if rows.is_empty() {
        return Ok(());
    }
    Err(RunError::LeftPrepared(
        rows.iter().map(|r| r.get::<_, String>(0)).collect(),
    ))
}

/// `defs`' [`DefinitionState`]s once the engine has settled, after checking
/// that none is paused in a run of `kind` that never expects a pause (see
/// [`pause::allowance`]). Quiescing counts `paused` as settled, so this is
/// the one place a pause is told apart from a definition that caught up.
async fn settled_states<B: Backend>(
    backend: &mut B,
    program: &Program,
    op_index: usize,
    defs: &[TransformDef],
    kind: RunKind,
) -> Result<Vec<DefinitionState>, RunError> {
    let states = backend
        .definition_states(defs)
        .await
        .map_err(|e| RunError::Snapshot(format!("definition states: {e:?}")))?;
    check_no_unexpected_pause(op_index, &states, allowance(kind, program))?;
    Ok(states)
}

/// The concurrent runtime: like [`run_convergence`],
/// but applies `program`'s ops in fixed-size **bursts** of up to `burst_size`
/// ops back-to-back, with no [`Backend::quiesce`] call between them within a
/// burst — quiescing, snapshotting, and running the oracle check
/// only once per burst, after its last op, instead of after every single op.
///
/// This exists to make a multi-worker [`Backend`] (e.g.
/// `crate::backend::ManualBackend::connect_with_workers`) actually exercise
/// concurrent, multi-row draining: `run_convergence`'s strict apply -> quiesce
/// loop never lets more than one row's change be in flight at once, so a
/// naive N-worker backend driven that way mostly tests "N idle-ish workers
/// don't duplicate/corrupt a single claim" rather than a real batch getting
/// split and drained by several workers at once
/// (`staging::claim`'s bucket-splitting only kicks in above
/// `MIN_ROWS_TO_SPLIT` rows sealed in one batch). Letting several ops land
/// before the harness ever asks the engine to catch up gives a real batch a
/// chance to accumulate.
///
/// The ops within a burst are still applied strictly in `program.ops`'s own
/// generated order, one at a time, each one's actual outcome still checked
/// against [`Op::expect`] via [`apply_and_check_outcome`] — this is **not**
/// the harness racing ops against each other; [`run_convergence_concurrent`]
/// does that. It is only a change to *when* `quiesce()` is
/// called, nothing about *what* is applied or in what order.
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
    for burst in bursts(program, burst_size) {
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
            RunKind::Bursty,
        )
        .await?;
    }

    Ok(Outcome::Ran)
}

/// `program`'s ops in bursts of up to `burst_size` consecutive ops, split
/// early so no burst both brings a leaf into a tree that may hold a `DEFAULT`
/// (a new leaf or middle, or an attach) and removes it again (a detach or drop): the
/// second op starts a new burst. [`run_convergence_bursty`] settles between
/// bursts, which waits for the reconcile pass that records the leaf, so a
/// leaf beside a `DEFAULT` is never created and removed between two passes
/// (a documented gap: `docs/known-correctness-gaps.md`, "A short-lived leaf
/// beside a `DEFAULT` partition"). Any other partition maintenance may share
/// a burst, and so a pass: the engine corrects a tree that changed several
/// times between two passes, a leaf detached and attached again included.
fn bursts(program: &Program, burst_size: usize) -> Vec<&[Op]> {
    use crate::model::{PartitionAction, PartitionStrategy};
    let may_have_default = |table: &str| {
        program.tables.iter().any(|t| {
            t.name == table
                && t.partitioning.as_ref().is_some_and(|p| {
                    matches!(p.strategy, PartitionStrategy::List { default: true, .. })
                })
        })
    };
    let ops = &program.ops;
    let mut out = Vec::new();
    let mut start = 0;
    // `(root, leaf)` of each leaf brought into a tree in the current burst.
    let mut joined: std::collections::HashSet<(&str, &str)> = std::collections::HashSet::new();
    for (i, op) in ops.iter().enumerate() {
        let removes_joined = match op {
            Op::Partition {
                table,
                action: PartitionAction::Detach { leaf, .. } | PartitionAction::Drop { leaf },
                ..
            } => may_have_default(table) && joined.contains(&(table.as_str(), leaf.as_str())),
            _ => false,
        };
        if i - start == burst_size || removes_joined {
            out.push(&ops[start..i]);
            start = i;
            joined.clear();
        }
        if let Op::Partition {
            table,
            action:
                PartitionAction::Attach { leaf, .. }
                | PartitionAction::Create { leaf, .. }
                | PartitionAction::CreateMiddle { middle: leaf, .. },
            ..
        } = op
        {
            joined.insert((table.as_str(), leaf.as_str()));
        }
    }
    if start < ops.len() {
        out.push(&ops[start..]);
    }
    out
}

/// Runs the oracle check for every definition in `program` against
/// `snapshot`, returning every `(target, report)` that diverged, in
/// definition order — empty if all converged.
///
/// Exposed (not just used by [`run_convergence`]) so a red/control test can
/// feed it a deliberately corrupted snapshot and confirm the harness reports
/// the divergence — proving it tells green from red (design doc §6/§7).
///
/// A thin wrapper around [`check_defs`] for `program.defs` in full — every
/// caller whose definitions all install up front wants exactly that. [`run_convergence`] itself
/// calls [`check_defs`] directly with only the definitions installed *so far*,
/// since a deferred definition must not be checked — or even
/// looked up in the snapshot — before its own install point is reached.
pub async fn check_program(
    pool: &Pool,
    program: &Program,
    snapshot: &Snapshot,
) -> Result<Vec<(String, OracleReport)>, String> {
    check_defs(pool, program, &program.defs, snapshot).await
}

/// [`check_program`]'s general form: runs the
/// oracle check for every definition in `defs` (not necessarily all
/// of `program.defs` — see [`run_convergence`]'s use of this for a partially-
/// installed program) against `snapshot`, returning every `(target, report)`
/// that diverged, in definition order — empty if all converged. It checks
/// every definition rather than stopping at the first divergence, so a
/// caller attributing a failure sees each target that went wrong. `program`
/// is still needed for its `tables` (to resolve each definition's source
/// schema).
pub async fn check_defs(
    pool: &Pool,
    program: &Program,
    defs: &[TransformDef],
    snapshot: &Snapshot,
) -> Result<Vec<(String, OracleReport)>, String> {
    let mut diverged = Vec::new();
    for def in defs {
        let report = check_def(pool, program, defs, def, snapshot).await?;
        if report.diverged() {
            diverged.push((def.target.clone(), report));
        }
    }
    Ok(diverged)
}

/// [`check_defs`] for one definition of `defs`: its target in `snapshot`
/// against the oracle's recompute, diverged or not.
pub async fn check_def(
    pool: &Pool,
    program: &Program,
    defs: &[TransformDef],
    def: &TransformDef,
    snapshot: &Snapshot,
) -> Result<OracleReport, String> {
    let source = source_table(program, defs, &def.source).unwrap_or_else(|| {
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

    oracle::check_over(
        pool,
        program,
        defs,
        def,
        &source.pk_col,
        &source_columns,
        target,
    )
    .await
    .map_err(|e| format!("{e:?}"))
}

/// The table `name` a definition reads: one of `program`'s tables, or the
/// target of a plain 1-1 definition among `defs` that reads one
/// ([`oracle::one_to_one_target_table`]), as a chained definition reads it.
pub fn source_table(
    program: &Program,
    defs: &[TransformDef],
    name: &str,
) -> Option<crate::model::Table> {
    if let Some(table) = program.tables.iter().find(|t| t.name == name) {
        return Some(table.clone());
    }
    let upstream = defs.iter().find(|d| {
        d.target == name && matches!(d.key_space, trellis::dev::defs::ast::KeySpace::OneToOne)
    })?;
    let source = program.tables.iter().find(|t| t.name == upstream.source)?;
    Some(oracle::one_to_one_target_table(upstream, source))
}

#[cfg(test)]
mod tests {
    use super::{
        DefinitionState, Divergence, OracleReport, Outcome, bursts, raised_in_capture, status_lies,
    };
    use crate::model::{Op, OpOutcome, PartitionAction, PartitionStrategy, Program};

    /// #2154's error: the capture function's frame in the `where` context
    /// marks it, in any schema and quoted or not; an application's own
    /// trigger function doesn't.
    #[test]
    fn a_capture_function_frame_marks_an_error_raised_in_capture() {
        assert!(raised_in_capture(
            "Db(Error { kind: Db, cause: Some(DbError { message: \"permission denied for table s\", \
             where_: Some(\"SQL statement ...\\nPL/pgSQL function trellis.cap_insert_s_0123456789abcdef() \
             line 88 at SQL statement\") }) })"
        ));
        assert!(raised_in_capture(
            "PL/pgSQL function \"inst_b\".\"cap_update_t_0123\"() line 3"
        ));
        // Escaped by a `Debug` of a `Debug`, and a compile error's context.
        let quoted = "PL/pgSQL function \"inst_b\".\"cap_update_T_0123\"() line 3";
        assert!(raised_in_capture(&format!("{:?}", format!("{quoted:?}"))));
        assert!(raised_in_capture(&format!(
            "{:?}",
            "compilation of PL/pgSQL function \"cap_insert_t_0123\" near line 2"
        )));
        assert!(!raised_in_capture(
            "PL/pgSQL function public.wire_rewrite_t() line 2 at SQL statement"
        ));
        assert!(!raised_in_capture(
            "duplicate key value violates unique constraint"
        ));
    }

    /// A one-table program partitioned by `strategy`, its ops replaced by
    /// `ops`.
    fn partitioned(strategy: PartitionStrategy, ops: Vec<Op>) -> Program {
        use crate::generate::{
            AggregateFn, DefShape, TableSpec, build_program_multi_with_shapes, partition_table,
        };
        let mut program = partition_table(
            build_program_multi_with_shapes(
                &[TableSpec::numeric_only(vec![(Some(1), None)], Vec::new())],
                &[(
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Count],
                    },
                )],
            ),
            0,
            strategy,
        );
        program.ops = ops;
        program
    }

    /// Beside a possible `DEFAULT`, a detach or drop of a leaf
    /// created or attached earlier in the same burst starts a new burst (the
    /// short-lived-leaf gap); every other partition operation shares a burst,
    /// and without a `DEFAULT` nothing splits but `burst_size`.
    #[test]
    fn only_removing_a_leaf_that_joined_in_the_burst_beside_a_default_splits_it() {
        let write = Op::Delete {
            table: "t0".into(),
            pk: "1".into(),
            expect: OpOutcome::AffectsNoRows,
        };
        let ddl = |action| Op::Partition {
            table: "t0".into(),
            action,
            expect: OpOutcome::Succeeds,
        };
        let ops = vec![
            write.clone(),
            ddl(PartitionAction::Create {
                leaf: "t0_n1".into(),
                bound: "for values in (1)".into(),
                rows: Vec::new(),
                standalone: false,
            }),
            write.clone(),
            // A leaf the burst didn't bring in: no split.
            ddl(PartitionAction::Detach {
                leaf: "t0_p0".into(),
                concurrently: false,
            }),
            ddl(PartitionAction::Drop {
                leaf: "t0_n1".into(),
            }),
            ddl(PartitionAction::Attach {
                leaf: "t0_p0".into(),
                bound: "for values in (0)".into(),
            }),
            write.clone(),
            ddl(PartitionAction::Detach {
                leaf: "t0_p0".into(),
                concurrently: false,
            }),
            write.clone(),
            write,
        ];
        let sizes = |strategy, burst_size| -> Vec<usize> {
            let program = partitioned(strategy, ops.clone());
            bursts(&program, burst_size)
                .iter()
                .map(|b| b.len())
                .collect()
        };
        let with_default = PartitionStrategy::List {
            values: 2,
            default: true,
        };
        assert_eq!(sizes(with_default, 20), vec![4, 3, 3]);
        // A leaf brought in by an earlier burst is recorded by the settle
        // between them, so removing it splits nothing.
        assert_eq!(sizes(with_default, 3), vec![3, 3, 3, 1]);
        let without = PartitionStrategy::List {
            values: 2,
            default: false,
        };
        assert_eq!(sizes(without, 20), vec![10]);
        assert_eq!(sizes(without, 4), vec![4, 4, 2]);
        // A new middle joins the tree like a new leaf.
        let middle = vec![
            ddl(PartitionAction::CreateMiddle {
                middle: "t0_m1".into(),
                bound: "for values in (1)".into(),
                leaves: vec![("t0_m1_d".into(), "default".into())],
                rows: Vec::new(),
                standalone: false,
            }),
            ddl(PartitionAction::Detach {
                leaf: "t0_m1".into(),
                concurrently: false,
            }),
        ];
        let program = partitioned(with_default, middle);
        let sizes: Vec<usize> = bursts(&program, 20).iter().map(|b| b.len()).collect();
        assert_eq!(sizes, vec![1, 1]);
    }

    /// Design doc §6: only `Ran` passes. Fast, no database —
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

    use crate::oracle::Divergence as CellDivergence;

    fn missing_row(table: &str) -> OracleReport {
        OracleReport::for_test(vec![CellDivergence::MissingRow {
            table: table.to_string(),
            pk: "1".to_string(),
        }])
    }

    /// The report a failing test prints carries each definition's status,
    /// and a paused one's reason, on every diverging target's report.
    #[test]
    fn a_divergence_report_prints_each_definitions_status_and_pause_reason() {
        let states = vec![
            DefinitionState {
                target: "t0".to_string(),
                status: "live".to_string(),
                reported: "live".to_string(),
                reason: None,
            },
            DefinitionState {
                target: "t1".to_string(),
                status: "paused".to_string(),
                reported: "paused".to_string(),
                reason: Some("disruption: the capture trigger was dropped".to_string()),
            },
        ];
        let divergence = Divergence::from_checked(
            3,
            vec![
                ("t1".to_string(), missing_row("t1")),
                ("t2".to_string(), missing_row("t2")),
            ],
            states.clone(),
        )
        .expect("a divergence");
        assert_eq!(divergence.states, states);
        for (target, report) in divergence.all() {
            let printed = report.to_string();
            assert!(printed.contains("definitions:"), "{target}: {printed}");
            assert!(printed.contains("  t0: live\n"), "{target}: {printed}");
            assert!(
                printed.contains("  t1: paused (disruption: the capture trigger was dropped)\n"),
                "{target}: {printed}"
            );
        }
    }

    fn state(target: &str, status: &str, reported: &str, reason: Option<&str>) -> DefinitionState {
        DefinitionState {
            target: target.to_string(),
            status: status.to_string(),
            reported: reported.to_string(),
            reason: reason.map(str::to_string),
        }
    }

    /// Only a definition reporting `live` with nothing recorded claims an
    /// exact target: `catching_up`, a held key's reason, a pause or a
    /// quarantine each say why it may not be.
    #[test]
    fn status_lies_names_only_inexact_definitions_claiming_live() {
        let states = vec![
            state("a", "live", "live", None),
            state("b", "live", "live", None),
            state("c", "live", "catching_up", None),
            state(
                "d",
                "live",
                "live",
                Some("poisoned 1 key(s), last error: x"),
            ),
            state("e", "paused", "paused", Some("T0247")),
            state("f", "quarantined", "quarantined", None),
        ];
        let lies: Vec<&str> = status_lies(&states, |t| t != "a")
            .into_iter()
            .map(|s| s.target.as_str())
            .collect();
        assert_eq!(lies, ["b"]);

        let divergence = Divergence::from_checked(
            0,
            vec![
                ("b".to_string(), missing_row("b")),
                ("c".to_string(), missing_row("c")),
            ],
            states,
        )
        .expect("a divergence");
        assert_eq!(divergence.status_lies, ["b"]);
        let printed = divergence.report.to_string();
        assert!(
            printed.contains("  b: live <- status lies: reports live, target isn't exact\n"),
            "{printed}"
        );
        assert!(
            printed.contains("  c: live (reported catching_up)\n"),
            "{printed}"
        );
    }

    #[test]
    fn no_divergence_is_none() {
        assert!(Divergence::from_checked(0, Vec::new(), Vec::new()).is_none());
    }
}
