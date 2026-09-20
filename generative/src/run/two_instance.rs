//! Issue #234 (layer-3 bucket 1 remainder, epic #238): drive **two Trellis
//! instances side by side**, each running its own independent program, each
//! converging against its **own** oracle.
//!
//! This is the bug class a single-instance property structurally cannot see.
//! Every other property in this crate stands up exactly one engine against
//! exactly one database, so nothing it does can ever observe a resource one
//! instance was supposed to own privately but actually shares with another —
//! a hardcoded object name that isn't really schema-qualified, an advisory
//! lock keyed by a global constant rather than by the instance, a replication
//! slot or publication whose name collides, a global registry that isn't
//! instance-scoped. A run with one instance in it has nothing to interfere
//! with, so it is green by construction on every one of those.
//!
//! # What makes this an *interaction* test rather than two unrelated runs
//!
//! Running two instances that never overlap in time would prove nothing. This
//! runner deliberately makes them overlap on every axis it can:
//!
//! * **Both engines are live for the whole run.** Both are installed before
//!   either applies its first op, and neither is torn down until the run
//!   ends, so every op of either program is applied while the other
//!   instance's staging worker, intake/CDC consumer, maintenance loop, and
//!   application workers are all running.
//! * **Ops are interleaved, not batched per instance.** Step `n` applies
//!   instance A's op `n` *and* instance B's op `n` before *either* instance
//!   is asked to quiesce, so at the moment the harness asks for convergence
//!   both instances have work genuinely in flight at once, through the same
//!   Postgres WAL.
//! * **They are as close together as the isolation mechanism allows.** The
//!   intended caller (`generative/tests/two_instance_noise.rs`) puts both
//!   instances in the *same database* of the same cluster, separated only by
//!   `docs/instance-identity.md`'s named-schema mechanism — the tightest
//!   coexistence that document claims to support, and therefore the one with
//!   the most shared surface to get wrong.
//!
//! # Each instance is checked against its own oracle, independently
//!
//! [`check_program`] is run once per instance at every checkpoint, against that
//! instance's own [`Pool`] (whose `search_path` is pinned to that instance's
//! own schema) and that instance's own [`Program`]. A divergence carries the
//! [`InstanceLabel`] it was found on, and the runner returns on the first
//! one — so a divergence on A can never be masked by B being fine, or vice
//! versa. There is deliberately no combined/merged comparison anywhere in
//! this file: two instances that are correctly isolated have two entirely
//! separate correctness stories, and merging them would be exactly the way to
//! let one hide the other.

use std::fmt;

use trellis::Pool;

use crate::backend::Backend;
use crate::model::{OpOutcome, Program};

use super::{Divergence, Outcome, RunError, check_program};

/// Which of the two side-by-side instances something happened on. Carried on
/// every error this module produces so a failure message never leaves the
/// reader guessing which instance actually diverged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceLabel {
    A,
    B,
}

impl fmt::Display for InstanceLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstanceLabel::A => write!(f, "A"),
            InstanceLabel::B => write!(f, "B"),
        }
    }
}

/// A [`RunError`] tagged with the instance it happened on.
///
/// Deliberately a wrapper around the existing [`RunError`] rather than a
/// parallel error enum: every failure mode a single-instance run can hit is
/// still exactly a failure mode here, and the *only* new information a
/// two-instance run adds is which instance it was.
#[derive(Debug)]
pub struct TwoInstanceError {
    pub instance: InstanceLabel,
    pub error: RunError,
}

impl fmt::Display for TwoInstanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "instance {}: {:?}", self.instance, self.error)
    }
}

/// One side of a two-instance run: the backend driving it, the oracle's read
/// handle into *that instance's own* schema, and the program it runs.
///
/// The two sides are independent in every respect — different generated
/// programs, different schemas, different pools — which is the point: nothing
/// about either one is supposed to be visible to the other.
pub struct InstanceRun<'a, B: Backend> {
    pub label: InstanceLabel,
    pub backend: &'a mut B,
    /// The oracle's read handle into this instance's schema. Must have its
    /// `search_path` pinned there (build it from a
    /// `trellis::Config::with_schema` carrying this instance's schema, not
    /// from `Config::from_dsn`, which resolves a single process-global
    /// `TRELLIS_SCHEMA` and so cannot give two in-process instances two
    /// different schemas).
    pub pool: &'a Pool,
    pub program: &'a Program,
}

impl<B: Backend> InstanceRun<'_, B> {
    /// Applies this instance's op at `op_index`, checking its actual outcome
    /// against what the generator expected of it — the same "operation errors
    /// are checked, not swallowed" discipline
    /// [`super::run_convergence`] applies, just tagged with an instance.
    ///
    /// A no-op when this instance's program has already run out of ops (the
    /// two programs are independently generated, so they are almost never the
    /// same length — the shorter instance simply goes quiet and stays live,
    /// which is itself a useful state to hold the other instance against).
    async fn apply_step(&mut self, op_index: usize) -> Result<(), TwoInstanceError> {
        let Some(op) = self.program.ops.get(op_index) else {
            return Ok(());
        };
        let actual = match self.backend.apply(op).await {
            Err(_) => OpOutcome::Fails,
            Ok(0) => OpOutcome::AffectsNoRows,
            Ok(_) => OpOutcome::Succeeds,
        };
        let expected = op.expect();
        if !expected.matches(&actual) {
            return Err(self.tag(RunError::UnexpectedOpOutcome {
                op_index,
                expected: expected.clone(),
                actual,
            }));
        }
        Ok(())
    }

    /// Quiesces this instance, snapshots it, and runs the three-way oracle
    /// check for its own program against its own pool — reporting any
    /// divergence localized to `op_index` and tagged with this instance.
    async fn quiesce_and_check(&mut self, op_index: usize) -> Result<(), TwoInstanceError> {
        self.backend
            .quiesce()
            .await
            .map_err(|e| self.tag(RunError::Quiesce(format!("{e:?}"))))?;

        let snapshot = self
            .backend
            .snapshot()
            .await
            .map_err(|e| self.tag(RunError::Snapshot(format!("{e:?}"))))?;

        let checked = check_program(self.pool, self.program, &snapshot)
            .await
            .map_err(|e| self.tag(RunError::Oracle(e)))?;
        if let Some((def_target, report)) = checked {
            return Err(self.tag(RunError::Diverged(Divergence {
                op_index,
                def_target,
                report,
            })));
        }
        Ok(())
    }

    fn tag(&self, error: RunError) -> TwoInstanceError {
        TwoInstanceError {
            instance: self.label,
            error,
        }
    }
}

/// Installs both instances, then interleaves their op streams step by step,
/// checking each instance against its own oracle at up to `max_checks`
/// points spread across the run (always including the last step).
///
/// The ordering within one step is deliberate and load-bearing:
///
/// 1. **A's op, then B's op** — both applied before anything quiesces, so
///    both instances have uncommitted-to-the-target work in flight at the
///    same time rather than taking strict turns.
/// 2. **A quiesces and is checked, then B quiesces and is checked.** A's
///    check runs before B is even asked to quiesce, so A converging cannot
///    depend on B having been given a chance to catch up first — if A only
///    settles once B's workers have drained, that is precisely the
///    cross-instance coupling this property exists to catch, and it shows up
///    here as A timing out or diverging rather than being papered over by a
///    "quiesce everything, then check everything" shape.
///
/// # Why `max_checks`, instead of checking after every single step
///
/// [`super::run_convergence`] checks after every op, and does so cheaply,
/// because a single instance's `quiesce()` almost always returns as soon as
/// its own commit has drained. **That is not true with a co-tenant
/// instance in the same cluster, and the reason is worth writing down.**
///
/// `quiesce()` waits for the engine's own convergence watermark, whose token
/// is `pg_current_wal_lsn()` — a **cluster-wide** LSN. So when instance A
/// asks "have I converged?", it is really asking "has my pipeline confirmed
/// through an LSN that includes instance B's writes?" — writes A's
/// publication filters out and A therefore never receives as data. A's
/// `replication_progress.confirmed_lsn` can only advance past them on a
/// keepalive, and the engine deliberately throttles that persist to once
/// per `intake::KEEPALIVE_PERSIST_INTERVAL` (10s, issue #8's quiet-stream
/// guard (d), because the persist is itself a WAL-generating write). The
/// measured result is ~10s for *every* quiesce in a two-instance run versus
/// ~0.1s in the equivalent single-instance one.
///
/// This is a latency property, not a correctness one — both instances do
/// converge, to exactly what their own oracles say — so it is not treated as
/// a failure here. But it does mean per-op checking would cost roughly
/// `20 * 10s` per case, which no case count makes affordable. Checking at a
/// bounded number of points keeps a case's cost flat in the program's
/// length.
///
/// **This makes divergence localization coarser**, exactly like
/// [`super::run_convergence_bursty`]'s: a divergence is only known to have
/// appeared somewhere within the window since the previous check, and the
/// [`Divergence::op_index`] reported is that window's *last* step. Follow
/// the same convention that file's module doc comment sets out — treat a
/// shrunk case as advisory and hand-transcribe it into a `build_program`
/// pin — before trusting a failure here as a minimal repro.
///
/// Returns `Ok(Outcome::Ran)` once both programs are exhausted — whether or
/// not the property held, since a divergence is a separate, still-fatal
/// condition surfaced via `Err`. Same contract as
/// [`super::run_convergence`]: there is no path to an `Ok` outside having
/// driven both backends through their whole programs *and* checked both at
/// least once.
///
/// Panics if `max_checks` is `0`: a run that never checks anything would
/// report `Outcome::Ran` having proven nothing, which is precisely the
/// "green run that is not evidence" [`Outcome`] exists to rule out.
pub async fn run_two_instance_convergence<A: Backend, B: Backend>(
    mut a: InstanceRun<'_, A>,
    mut b: InstanceRun<'_, B>,
    max_checks: usize,
) -> Result<Outcome, TwoInstanceError> {
    assert!(
        max_checks > 0,
        "run_two_instance_convergence: max_checks must be at least 1"
    );
    // Task E2's deferred definition installs and E3's scheduled
    // restarts/scale-outs are deliberately *not* supported here, and are
    // refused loudly rather than silently ignored — a program carrying one
    // would otherwise be quietly under-tested (every definition installed up
    // front, every lifecycle event dropped) while still reporting
    // `Outcome::Ran`. `trivial_program`, the only strategy this runner is
    // driven with today, never produces either; layering them on top of two
    // live instances is a coherent follow-up, not something to acquire by
    // accident.
    reject_unsupported_schedule(&a)?;
    reject_unsupported_schedule(&b)?;

    // Both instances stand up before either applies an op, so every op below
    // runs against a cluster where *both* engines are live. This is also the
    // first point a shared-resource collision can surface at all: if the two
    // instances contend for something neither is supposed to own globally
    // (a slot name, a publication, an advisory lock), B's install is where
    // that shows up — and in fact is exactly where both of the bugs #234
    // found did show up.
    a.backend
        .install(a.program)
        .await
        .map_err(|e| a.tag(RunError::Install(format!("{e:?}"))))?;
    b.backend
        .install(b.program)
        .await
        .map_err(|e| b.tag(RunError::Install(format!("{e:?}"))))?;

    let steps = a.program.ops.len().max(b.program.ops.len());
    if steps == 0 {
        // Neither program has an op (nothing `trivial_program` draws, but a
        // hand-built `build_program(&[], &[])` would). The freshly-installed
        // state is still a state both oracles have an opinion about, so it
        // is still checked — `Outcome::Ran` must never mean "checked
        // nothing".
        a.quiesce_and_check(0).await?;
        b.quiesce_and_check(0).await?;
        return Ok(Outcome::Ran);
    }

    // Spread `max_checks` checks over `steps` steps. `div_ceil` rounds the
    // stride *up*, so the number of checks never exceeds `max_checks`; the
    // final step always checks regardless, so a run always ends on a check
    // no matter how the arithmetic lands.
    let stride = steps.div_ceil(max_checks).max(1);
    for op_index in 0..steps {
        a.apply_step(op_index).await?;
        b.apply_step(op_index).await?;

        let is_last = op_index + 1 == steps;
        if is_last || (op_index + 1) % stride == 0 {
            a.quiesce_and_check(op_index).await?;
            b.quiesce_and_check(op_index).await?;
        }
    }

    Ok(Outcome::Ran)
}

/// Refuses a program whose schedule this runner does not implement — see the
/// call site in [`run_two_instance_convergence`] for why this is a loud
/// refusal rather than a silent no-op.
fn reject_unsupported_schedule<B: Backend>(
    run: &InstanceRun<'_, B>,
) -> Result<(), TwoInstanceError> {
    let program = run.program;
    if program.def_install_after_op.iter().any(|at| *at != 0) {
        return Err(run.tag(RunError::Install(
            "two-instance runs do not implement task E2's mid-stream definition installs \
             (`def_install_after_op`)"
                .to_string(),
        )));
    }
    if !program.restart_after_ops.is_empty() || !program.scale_out_after_ops.is_empty() {
        return Err(run.tag(RunError::Install(
            "two-instance runs do not implement task E3's scheduled restarts/scale-outs \
             (`restart_after_ops`/`scale_out_after_ops`)"
                .to_string(),
        )));
    }
    Ok(())
}
