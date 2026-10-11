//! A synchronous wrapper around [`Trellis`] for callers that can't assume a
//! `tokio` runtime already exists on the calling thread — chiefly issue
//! #87's future FFI embedding (Rustler/Magnus-style NIF bindings), whose
//! calling convention is fundamentally synchronous: a call blocks until it
//! returns. See `docs/decisions/0008-public-api-design.md`'s decision 1.
//!
//! [`BlockingTrellis`] mirrors the pattern [`crate::client::Client::start`]
//! already uses: a dedicated background thread builds its own `tokio`
//! runtime and owns the real, async [`Trellis`] for as long as the handle
//! lives; every [`BlockingTrellis`] method sends a [`Job`] to that thread
//! and blocks the calling thread on the reply. The calling thread itself
//! never needs a runtime of its own.
//!
//! **Every call returns within 30 seconds** (issue #599). The deadline counts
//! from when the call is submitted, so time a job spends queued for the
//! background thread counts against it, and what the call started on the
//! server is stopped by the server at the deadline (see `crate::deadline`).
//! A call that runs out of time returns [`TrellisError::CallTimeout`]. The
//! background thread runs each job as a task of its own, so a call stuck on a
//! lock holds up none of the calls made after it. A caller's own calls stay in
//! order, because it waits for each reply; calls from different threads have no
//! order. The exceptions are [`BlockingTrellis::self_check`] and
//! [`BlockingTrellis::shutdown`], which `Trellis` runs outside the deadline
//! too (see [`Trellis::self_check`] and [`Trellis::shutdown`]). A shutdown
//! cancels the calls still in flight rather than waiting for them.
//!
//! **Waiting is the caller's to choose.** Each method has a `start_*` twin
//! (`apply` and [`BlockingTrellis::start_apply`], say) that submits the job and
//! returns a [`PendingCall`] at once. The plain method is the twin followed by
//! [`PendingCall::wait`], a bare blocking receive that nothing can cut short.
//! A binding whose host must be able to interrupt the wait (the Ruby
//! extension releases the GVL and wakes on `Thread#kill` or a signal) polls
//! the [`PendingCall`] as a future with a waker of its own, so it needs no
//! thread per call. Abandoning a [`PendingCall`] abandons the reply, not the
//! work: the job runs to its end or its deadline.
//!
//! **Blocks only on registration, never on backfill.**
//! [`BlockingTrellis::apply`] is exactly as fast (or slow) as
//! [`Trellis::apply`] (see `app`'s module doc), which only registers: a new
//! definition is built in the background from `waiting_to_backfill`
//! (ADR-0016), and an `ALTER TRANSFORM` or a column resume starts a
//! background field build (#666, #625 F8b). The one statement that still
//! reads table rows inside the call is a to-one relationship's declaration
//! (or a transform reading through one), which seeds the relationship's
//! projection from its to-side table. It is bound by the deadline like any
//! call: on a to-side table too large to seed in time it returns the timeout
//! error and registers nothing.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{mpsc, oneshot, watch};
use tokio_postgres::types::PgLsn;

use crate::app::{
    Applied, DefinitionStatus, DefinitionSummary, PoisonEntry, PoisonSample, QuarantineEntry,
    RelationshipSummary, Trellis, TrellisError, TrellisOptions,
};
use crate::client::build_runtime;
use crate::config::Config;
use crate::staging::{SelfCheckMode, SelfCheckReport, SelfCheckScope};

/// One [`BlockingTrellis`] method call, carried over a channel to the
/// dedicated background thread that owns the real, async [`Trellis`] (see
/// the module doc comment). Each variant pairs its call's arguments with a
/// one-shot reply sender; [`Job::Shutdown`] is the one variant that ends the
/// background thread's loop.
enum Job {
    Migrate(oneshot::Sender<Result<(), TrellisError>>),
    Apply(String, oneshot::Sender<Result<Applied, TrellisError>>),
    Definitions(oneshot::Sender<Result<Vec<DefinitionSummary>, TrellisError>>),
    Relationships(oneshot::Sender<Result<Vec<RelationshipSummary>, TrellisError>>),
    RequestBackfill(String, oneshot::Sender<Result<(), TrellisError>>),
    PoisonedSince(
        SystemTime,
        oneshot::Sender<Result<Vec<PoisonEntry>, TrellisError>>,
    ),
    Status(
        String,
        oneshot::Sender<Result<Option<DefinitionStatus>, TrellisError>>,
    ),
    Quarantined(oneshot::Sender<Result<Vec<QuarantineEntry>, TrellisError>>),
    QuarantineStatus(
        String,
        oneshot::Sender<Result<QuarantineEntry, TrellisError>>,
    ),
    SampleQuarantined(
        String,
        Option<(String, String)>,
        i64,
        oneshot::Sender<Result<Vec<PoisonSample>, TrellisError>>,
    ),
    ReleaseKey(
        String,
        String,
        String,
        oneshot::Sender<Result<(), TrellisError>>,
    ),
    HasLiveDrainWorkers(oneshot::Sender<Result<bool, TrellisError>>),
    HasLiveStagingWorker(oneshot::Sender<Result<bool, TrellisError>>),
    WatermarkToken(oneshot::Sender<Result<PgLsn, TrellisError>>),
    AwaitConverged(PgLsn, Duration, oneshot::Sender<Result<(), TrellisError>>),
    SelfCheck(
        String,
        SelfCheckScope,
        SelfCheckMode,
        Duration,
        oneshot::Sender<Result<SelfCheckReport, TrellisError>>,
    ),
    Shutdown(oneshot::Sender<Result<(), TrellisError>>),
}

/// A [`Job`] and the instant its caller submitted it, which its deadline
/// counts from.
type Submitted = (Instant, Job);

/// A synchronous facade over [`Trellis`] — see the module doc comment.
///
/// Every method blocks the calling thread until the background thread
/// replies (within the call's 30-second deadline, see the module doc), but none of them (nor [`BlockingTrellis::connect`] itself)
/// require that thread to already be inside a `tokio` runtime. Dropping a
/// `BlockingTrellis` without calling [`BlockingTrellis::shutdown`] closes
/// the job channel — ending the background thread's loop and dropping the
/// `Trellis` it owns, best-effort — but doesn't wait for that thread to
/// exit; call `shutdown` for a clean, joined stop, exactly like
/// [`crate::client::Client`]'s own documented `Drop` discipline.
///
/// The reverse also matters: calling a `BlockingTrellis` method from a
/// thread that *does* already have a `tokio` runtime entered (a
/// `#[tokio::test]` fn, a `tokio::spawn`ed task) is a usage error, not
/// something this type can silently support — it returns
/// [`TrellisError::CalledFromAsyncContext`] rather than blocking, since
/// blocking such a thread would deadlock/panic inside `tokio` itself. Use
/// the async [`Trellis`] directly in that context instead.
///
/// Every public [`Trellis`] method has a twin here except one, left
/// async-only on purpose: [`Trellis::pool`]. The pool hands out async
/// connections, which a caller with no `tokio` runtime of its own (the whole
/// audience of this type) can't drive, and which were never meant to cross
/// an FFI boundary. A caller that needs its own queries against the target
/// tables opens its own connection. The `surface_tests` module below checks
/// that no other method is missing.
pub struct BlockingTrellis {
    job_tx: mpsc::UnboundedSender<Submitted>,
    /// A copy of the configuration the background thread's [`Trellis`]
    /// connected with, so [`BlockingTrellis::config`] can hand out a
    /// reference without a round trip. [`Config`] is immutable once built, so
    /// the copy can't drift from the original.
    config: Config,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl BlockingTrellis {
    /// Spawns the dedicated background thread, builds its `tokio` runtime,
    /// and connects the real [`Trellis`] against `config`/`options` (see
    /// [`Trellis::connect`]) — blocking until that setup finishes or fails.
    ///
    /// [`TrellisOptions::worker_threads`] sizes that runtime's worker-thread
    /// pool, and the background client's runtime too when `staging` or
    /// `drain_threads` starts one; leaving it at `None` keeps `tokio`'s own
    /// per-core default. The cap is per runtime and per handle (see that
    /// option's doc comment).
    pub fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let (job_tx, job_rx) = mpsc::unbounded_channel::<Submitted>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), TrellisError>>();
        let kept_config = config.clone();

        let thread = std::thread::Builder::new()
            .name("trellis-blocking".to_string())
            .spawn(move || {
                let runtime = match build_runtime(options.worker_threads) {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready_tx.send(Err(TrellisError::BlockingSpawn(err)));
                        return;
                    }
                };
                runtime.block_on(run(config, options, job_rx, ready_tx));
            })
            .map_err(TrellisError::BlockingSpawn)?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(BlockingTrellis {
                job_tx,
                config: kept_config,
                thread: Some(thread),
            }),
            Ok(Err(err)) => {
                // Setup failed; the thread is already exiting (or exited) on
                // its own. Best-effort join so we don't leak it, but don't
                // let a join failure mask the real setup error.
                let _ = thread.join();
                Err(err)
            }
            Err(_) => {
                let _ = thread.join();
                Err(TrellisError::BlockingThreadExitedBeforeReady)
            }
        }
    }

    /// The resolved configuration (schema names, DSN) this instance connected
    /// with. See [`Trellis::config`]. Like [`BlockingTrellis::metrics`], this
    /// doesn't round-trip through the background thread: the handle keeps its
    /// own copy of the (immutable) [`Config`] it was connected with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Applies Trellis's schema migrations. See [`Trellis::migrate`].
    pub fn migrate(&self) -> Result<(), TrellisError> {
        self.start_migrate().wait()
    }

    /// Submits [`BlockingTrellis::migrate`] and returns without waiting for it.
    pub fn start_migrate(&self) -> PendingCall<()> {
        self.start(Job::Migrate)
    }

    /// A handle onto this process's in-process metrics registry. See
    /// [`Trellis::metrics`]. Unlike every other method here, this doesn't
    /// round-trip through the background thread's job channel: the registry
    /// is a process-wide global (see `trellis::metrics`'s module doc
    /// comment), not state owned by the background thread's [`Trellis`], and
    /// reading it is synchronous and side-effect-free, so there's nothing to
    /// block on.
    pub fn metrics(&self) -> crate::metrics::Metrics {
        crate::metrics::Metrics::new()
    }

    /// Runs one statement of Trellis's grammar — define a transform, define a
    /// relationship, pause, resume, or drop. The single entrypoint for every
    /// definition-changing operation; see [`Trellis::apply`] for the grammar,
    /// the addressing rules, and each statement's semantics.
    ///
    /// In particular, registering a plain (non-relationship) 1-1 transform
    /// returns before its backfill finishes; poll
    /// [`BlockingTrellis::status`] for [`TransformStatus::Live`](crate::TransformStatus::Live).
    pub fn apply(&self, statement_text: &str) -> Result<Applied, TrellisError> {
        self.start_apply(statement_text).wait()
    }

    /// Submits [`BlockingTrellis::apply`] and returns without waiting for it.
    pub fn start_apply(&self, statement_text: &str) -> PendingCall<Applied> {
        let statement_text = statement_text.to_string();
        self.start(|reply| Job::Apply(statement_text, reply))
    }

    /// Every registered transform definition, oldest first. See
    /// [`Trellis::definitions`].
    pub fn definitions(&self) -> Result<Vec<DefinitionSummary>, TrellisError> {
        self.start_definitions().wait()
    }

    /// Submits [`BlockingTrellis::definitions`] and returns without waiting for it.
    pub fn start_definitions(&self) -> PendingCall<Vec<DefinitionSummary>> {
        self.start(Job::Definitions)
    }

    /// Every registered relationship declaration, oldest first. See
    /// [`Trellis::relationships`].
    pub fn relationships(&self) -> Result<Vec<RelationshipSummary>, TrellisError> {
        self.start_relationships().wait()
    }

    /// Submits [`BlockingTrellis::relationships`] and returns without waiting for it.
    pub fn start_relationships(&self) -> PendingCall<Vec<RelationshipSummary>> {
        self.start(Job::Relationships)
    }

    /// Re-reads `source_table` for every definition applying from it, as a
    /// go-live catch-up. See [`Trellis::request_backfill`].
    pub fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        self.start_request_backfill(source_table).wait()
    }

    /// Submits [`BlockingTrellis::request_backfill`] and returns without
    /// waiting for it.
    pub fn start_request_backfill(&self, source_table: &str) -> PendingCall<()> {
        let source_table = source_table.to_string();
        self.start(|reply| Job::RequestBackfill(source_table, reply))
    }

    /// Poison-quarantine entries recorded since `watermark`, oldest first.
    /// See [`Trellis::poisoned_since`].
    pub fn poisoned_since(&self, watermark: SystemTime) -> Result<Vec<PoisonEntry>, TrellisError> {
        self.start_poisoned_since(watermark).wait()
    }

    /// Submits [`BlockingTrellis::poisoned_since`] and returns without waiting
    /// for it.
    pub fn start_poisoned_since(&self, watermark: SystemTime) -> PendingCall<Vec<PoisonEntry>> {
        self.start(|reply| Job::PoisonedSince(watermark, reply))
    }

    /// One registered transform definition's current [`TransformStatus`](crate::TransformStatus), and
    /// its source's backfill failure if any, by target table name. See
    /// [`Trellis::status`].
    pub fn status(&self, target_table: &str) -> Result<Option<DefinitionStatus>, TrellisError> {
        self.start_status(target_table).wait()
    }

    /// Submits [`BlockingTrellis::status`] and returns without waiting for it.
    pub fn start_status(&self, target_table: &str) -> PendingCall<Option<DefinitionStatus>> {
        let target_table = target_table.to_string();
        self.start(|reply| Job::Status(target_table, reply))
    }

    /// Every currently paused/quarantined target. See [`Trellis::quarantined`].
    pub fn quarantined(&self) -> Result<Vec<QuarantineEntry>, TrellisError> {
        self.start_quarantined().wait()
    }

    /// Submits [`BlockingTrellis::quarantined`] and returns without waiting for it.
    pub fn start_quarantined(&self) -> PendingCall<Vec<QuarantineEntry>> {
        self.start(Job::Quarantined)
    }

    /// The current state of one target (`transform` or `transform.column`).
    /// See [`Trellis::quarantine_status`].
    pub fn quarantine_status(&self, target: &str) -> Result<QuarantineEntry, TrellisError> {
        self.start_quarantine_status(target).wait()
    }

    /// Submits [`BlockingTrellis::quarantine_status`] and returns without
    /// waiting for it.
    pub fn start_quarantine_status(&self, target: &str) -> PendingCall<QuarantineEntry> {
        let target = target.to_string();
        self.start(|reply| Job::QuarantineStatus(target, reply))
    }

    /// A paginated batch of poisoned rows for `target`. See
    /// [`Trellis::sample_quarantined`].
    pub fn sample_quarantined(
        &self,
        target: &str,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<PoisonSample>, TrellisError> {
        self.start_sample_quarantined(target, after, limit).wait()
    }

    /// Submits [`BlockingTrellis::sample_quarantined`] and returns without
    /// waiting for it.
    pub fn start_sample_quarantined(
        &self,
        target: &str,
        after: Option<(String, String)>,
        limit: i64,
    ) -> PendingCall<Vec<PoisonSample>> {
        let target = target.to_string();
        self.start(|reply| Job::SampleQuarantined(target, after, limit, reply))
    }

    /// Releases one key `transform` holds in quarantine, re-deriving it from
    /// its current row. See [`Trellis::release_key`].
    pub fn release_key(
        &self,
        transform: &str,
        source_table: &str,
        key: &str,
    ) -> Result<(), TrellisError> {
        self.start_release_key(transform, source_table, key).wait()
    }

    /// Submits [`BlockingTrellis::release_key`] and returns without waiting
    /// for it.
    pub fn start_release_key(
        &self,
        transform: &str,
        source_table: &str,
        key: &str,
    ) -> PendingCall<()> {
        let (transform, source_table, key) = (
            transform.to_string(),
            source_table.to_string(),
            key.to_string(),
        );
        self.start(|reply| Job::ReleaseKey(transform, source_table, key, reply))
    }

    /// Whether at least one live drain worker is registered anywhere in
    /// this fleet right now — the health-check-shaped read a Phoenix/Rails
    /// host is meant to poll on a timer. See
    /// [`Trellis::has_live_drain_workers`].
    pub fn has_live_drain_workers(&self) -> Result<bool, TrellisError> {
        self.start_has_live_drain_workers().wait()
    }

    /// Submits [`BlockingTrellis::has_live_drain_workers`] and returns without waiting for it.
    pub fn start_has_live_drain_workers(&self) -> PendingCall<bool> {
        self.start(Job::HasLiveDrainWorkers)
    }

    /// Whether this instance's staging worker is running anywhere in the
    /// fleet right now — the other half of the health check. See
    /// [`Trellis::has_live_staging_worker`].
    pub fn has_live_staging_worker(&self) -> Result<bool, TrellisError> {
        self.start_has_live_staging_worker().wait()
    }

    /// Submits [`BlockingTrellis::has_live_staging_worker`] and returns without waiting for it.
    pub fn start_has_live_staging_worker(&self) -> PendingCall<bool> {
        self.start(Job::HasLiveStagingWorker)
    }

    /// A read-your-writes watermark token. See [`Trellis::watermark_token`].
    pub fn watermark_token(&self) -> Result<PgLsn, TrellisError> {
        self.start_watermark_token().wait()
    }

    /// Submits [`BlockingTrellis::watermark_token`] and returns without waiting for it.
    pub fn start_watermark_token(&self) -> PendingCall<PgLsn> {
        self.start(Job::WatermarkToken)
    }

    /// Blocks until every effect committed at or before `token` has been
    /// reflected in its target(s), or `timeout` elapses. See
    /// [`Trellis::await_converged`].
    ///
    /// Like every call, it lasts at most the call's deadline (30 seconds): a
    /// longer `timeout` is cut to what is left of it, and a caller that wants
    /// to wait longer calls again. It holds one pooled connection while it
    /// waits, and no other call on the handle waits behind it.
    pub fn await_converged(&self, token: PgLsn, timeout: Duration) -> Result<(), TrellisError> {
        self.start_await_converged(token, timeout).wait()
    }

    /// Submits [`BlockingTrellis::await_converged`] and returns without
    /// waiting for it.
    pub fn start_await_converged(&self, token: PgLsn, timeout: Duration) -> PendingCall<()> {
        self.start(|reply| Job::AwaitConverged(token, timeout, reply))
    }

    /// Audits one page of `target_table` against an independent recompute of
    /// its definition from the source. See [`Trellis::self_check`] for what
    /// `scope`, `mode` and `timeout` mean and what the report holds.
    ///
    /// Outside the call deadline, like [`Trellis::self_check`]: it can run
    /// for up to `timeout` per convergence await it makes (one under
    /// [`SelfCheckMode::Strict`], up to two under [`SelfCheckMode::Standard`]),
    /// plus the comparison itself. It holds one pooled connection meanwhile,
    /// and no other call on the handle waits behind it.
    pub fn self_check(
        &self,
        target_table: &str,
        scope: SelfCheckScope,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> Result<SelfCheckReport, TrellisError> {
        self.start_self_check(target_table, scope, mode, timeout)
            .wait()
    }

    /// Submits [`BlockingTrellis::self_check`] and returns without waiting for
    /// it.
    pub fn start_self_check(
        &self,
        target_table: &str,
        scope: SelfCheckScope,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> PendingCall<SelfCheckReport> {
        let target_table = target_table.to_string();
        self.start(|reply| Job::SelfCheck(target_table, scope, mode, timeout, reply))
    }

    /// Stops any background work this connection started and waits for the
    /// background thread to exit cleanly. See [`Trellis::shutdown`].
    ///
    /// Calls still in flight are cancelled, not waited for: their callers get
    /// [`TrellisError::CancelledByShutdown`], and what each had started on the
    /// server ends at its own deadline (see the module doc). A call stuck on a
    /// lock therefore doesn't hold the shutdown up.
    pub fn shutdown(self) -> Result<(), TrellisError> {
        self.start_shutdown().wait()
    }

    /// Submits [`BlockingTrellis::shutdown`] and returns without waiting for
    /// it. The returned [`Shutdown`] is the wait: dropping it before it
    /// finishes doesn't stop the shutdown, which completes on its own (the
    /// background thread ends, detached).
    pub fn start_shutdown(mut self) -> Shutdown {
        let reply = self.start(Job::Shutdown);
        Shutdown {
            reply,
            thread: self.thread.take(),
        }
    }

    /// Sends `make_job(reply)` to the background thread and returns the
    /// reply's receiving end, without waiting for it. The one primitive every
    /// method above is built from. A [`TrellisError::BlockingThreadGone`]
    /// means no reply came: the background thread panicked, or the call's own
    /// task did (or the thread was never actually running the loop below,
    /// which can't happen from this module's own `connect`).
    fn start<T: Send + 'static>(
        &self,
        make_job: impl FnOnce(oneshot::Sender<Result<T, TrellisError>>) -> Job,
    ) -> PendingCall<T> {
        // Waiting on the reply from a thread that has a tokio runtime entered
        // panics (not returns an error), so misuse (e.g. a `#[tokio::test]` or
        // a `tokio::spawn`ed task) surfaces here as a normal `TrellisError`
        // instead of an opaque tokio panic, before anything is sent.
        if tokio::runtime::Handle::try_current().is_ok() {
            return PendingCall::failed(TrellisError::CalledFromAsyncContext);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        // The deadline runs from here: the time a job spends queued on the
        // channel counts against it (issue #599).
        match self.job_tx.send((Instant::now(), make_job(reply_tx))) {
            Ok(()) => PendingCall::waiting(reply_rx),
            Err(_) => PendingCall::failed(TrellisError::BlockingThreadGone),
        }
    }
}

/// The reply to a call submitted with one of [`BlockingTrellis`]'s `start_*`
/// methods, which return as soon as the job is on its way.
///
/// It is a [`Future`] that never needs a `tokio` runtime: its waker is woken
/// by the background thread when the reply is sent. A caller that must stay
/// interruptible while it waits (the Ruby binding, which releases the GVL)
/// polls it with a waker of its own, so no thread has to be dedicated to the
/// wait. [`PendingCall::wait`] is the plain blocking wait.
///
/// Dropping it abandons the reply, not the call: the job keeps running until
/// it finishes or reaches its deadline, and its reply is discarded (issue
/// #599).
#[must_use = "a dropped PendingCall discards the call's reply"]
pub struct PendingCall<T> {
    state: Pending<T>,
}

enum Pending<T> {
    Waiting(oneshot::Receiver<Result<T, TrellisError>>),
    /// The call never started, or its answer was already taken.
    Failed(Option<TrellisError>),
}

impl<T> PendingCall<T> {
    fn waiting(reply: oneshot::Receiver<Result<T, TrellisError>>) -> Self {
        PendingCall {
            state: Pending::Waiting(reply),
        }
    }

    fn failed(err: TrellisError) -> Self {
        PendingCall {
            state: Pending::Failed(Some(err)),
        }
    }

    /// Blocks the calling thread until the reply arrives. A thread that has
    /// no `tokio` runtime entered is the only kind that can call this (see
    /// [`TrellisError::CalledFromAsyncContext`], which `start` returns for
    /// the others without sending anything).
    pub fn wait(self) -> Result<T, TrellisError> {
        match self.state {
            Pending::Waiting(reply) => reply
                .blocking_recv()
                .map_err(|_| TrellisError::BlockingThreadGone)?,
            Pending::Failed(err) => Err(err.unwrap_or(TrellisError::BlockingThreadGone)),
        }
    }
}

impl<T> Unpin for PendingCall<T> {}

impl<T> Future for PendingCall<T> {
    type Output = Result<T, TrellisError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            Pending::Waiting(reply) => Pin::new(reply)
                .poll(cx)
                .map(|reply| reply.unwrap_or(Err(TrellisError::BlockingThreadGone))),
            Pending::Failed(err) => {
                Poll::Ready(Err(err.take().unwrap_or(TrellisError::BlockingThreadGone)))
            }
        }
    }
}

/// A [`BlockingTrellis::start_shutdown`] under way. Like [`PendingCall`], a
/// [`Future`] that needs no runtime to be polled; [`Shutdown::wait`] is the
/// plain blocking wait. It is ready once the background thread has replied
/// and exited.
#[must_use = "a dropped Shutdown still completes, but its result is lost"]
pub struct Shutdown {
    reply: PendingCall<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Shutdown {
    /// Blocks until the shutdown has finished and the background thread has
    /// exited.
    pub fn wait(self) -> Result<(), TrellisError> {
        let Shutdown { reply, thread } = self;
        let result = reply.wait();
        if let Some(thread) = thread {
            // Unlike `Client::shutdown`'s `tokio::task::spawn_blocking`
            // join, this call has no surrounding runtime of its own to keep
            // free — that's the entire point of this wrapper — so a plain,
            // directly blocking `join` is exactly right here.
            let _ = thread.join();
        }
        result
    }
}

impl Unpin for Shutdown {}

impl Future for Shutdown {
    type Output = Result<(), TrellisError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::task::ready!(Pin::new(&mut self.reply).poll(cx));
        if let Some(thread) = self.thread.take() {
            // The thread replied as its last act and is winding down, so this
            // join is brief.
            let _ = thread.join();
        }
        Poll::Ready(result)
    }
}

/// Runs entirely inside the background thread's runtime: connects the real
/// [`Trellis`], signals readiness, then services [`Job`]s until the channel
/// closes (every [`BlockingTrellis`] handle dropped without calling
/// `shutdown`) or [`Job::Shutdown`] is received.
///
/// Each job runs as a task of its own (issue #599), so a call blocked on a
/// lock doesn't hold up the ones behind it. The pool and each call's deadline
/// bound how many run at once, and an in-flight job outlives the caller that
/// stopped waiting for it only until its deadline.
async fn run(
    config: Config,
    options: TrellisOptions,
    mut job_rx: mpsc::UnboundedReceiver<Submitted>,
    ready_tx: std::sync::mpsc::Sender<Result<(), TrellisError>>,
) {
    let shared = match Trellis::connect(config, options).await {
        Ok(trellis) => Arc::new(trellis),
        Err(err) => {
            let _ = ready_tx.send(Err(err));
            return;
        }
    };
    if ready_tx.send(Ok(())).is_err() {
        // `BlockingTrellis::connect` gave up waiting on us — can't happen
        // from this crate's own API, but a defensive exit here costs
        // nothing (same reasoning as `Client::start`'s equivalent comment).
        return;
    }

    let (stopping, stop) = watch::channel(false);
    let mut calls = Calls {
        tasks: tokio::task::JoinSet::new(),
        stop,
    };
    while let Some((submitted, job)) = job_rx.recv().await {
        // Reap the finished ones, so the set doesn't grow with the calls made.
        while calls.tasks.try_join_next().is_some() {}
        let trellis = shared.clone();
        // Each arm builds its call's future inside a closure of its own, which
        // boxes it before it returns: the engine's futures are large, and an
        // arm that built them in this function's frame would put every arm's
        // at once on the stack of a thread that keeps the default size.
        match job {
            Job::Migrate(reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.migrate()).await })
            }),
            Job::Apply(text, reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.apply(&text)).await })
            }),
            Job::Definitions(reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.definitions()).await })
            }),
            Job::Relationships(reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.relationships()).await })
            }),
            Job::RequestBackfill(table, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move { t.submitted(at, t.request_backfill(&table)).await })
                })
            }
            Job::PoisonedSince(watermark, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move { t.submitted(at, t.poisoned_since(watermark)).await })
                })
            }
            Job::Status(table, reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.status(&table)).await })
            }),
            Job::Quarantined(reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.quarantined()).await })
            }),
            Job::QuarantineStatus(target, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move { t.submitted(at, t.quarantine_status(&target)).await })
                })
            }
            Job::SampleQuarantined(target, after, limit, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move {
                        t.submitted(at, t.sample_quarantined(&target, after, limit))
                            .await
                    })
                })
            }
            Job::ReleaseKey(transform, source_table, key, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move {
                        t.submitted(at, t.release_key(&transform, &source_table, &key))
                            .await
                    })
                })
            }
            Job::HasLiveDrainWorkers(reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move { t.submitted(at, t.has_live_drain_workers()).await })
                })
            }
            Job::HasLiveStagingWorker(reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(async move { t.submitted(at, t.has_live_staging_worker()).await })
                })
            }
            Job::WatermarkToken(reply) => calls.spawn(trellis, submitted, reply, move |t, at| {
                Box::pin(async move { t.submitted(at, t.watermark_token()).await })
            }),
            Job::AwaitConverged(token, timeout, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, at| {
                    Box::pin(
                        async move { t.submitted(at, t.await_converged(token, timeout)).await },
                    )
                })
            }
            // Exempt from the call deadline, like `Trellis::self_check`.
            Job::SelfCheck(table, scope, mode, timeout, reply) => {
                calls.spawn(trellis, submitted, reply, move |t, _| {
                    Box::pin(async move { t.self_check(&table, scope, mode, timeout).await })
                })
            }
            Job::Shutdown(reply) => {
                // The calls still running are cancelled: the `Trellis` can
                // only be shut down once nothing else holds it, and waiting
                // for a call stuck on a lock would hold the shutdown up for
                // its whole deadline. What a cancelled call started on the
                // server ends at the server's own `statement_timeout` for it
                // (see `crate::deadline`); a `self_check`, which runs outside
                // the deadline, has none, and its statement runs until the
                // server next writes to the closed connection.
                drop(trellis);
                let _ = stopping.send(true);
                while calls.tasks.join_next().await.is_some() {}
                let Ok(owned) = Arc::try_unwrap(shared) else {
                    unreachable!("every task has finished, so none still holds the Trellis");
                };
                let _ = reply.send(owned.shutdown().await);
                return;
            }
        }
    }
    while calls.tasks.join_next().await.is_some() {}
}

/// The calls [`run`] has spawned, each a task of its own, and the signal that
/// stops them all: `Job::Shutdown` sets it.
struct Calls {
    tasks: tokio::task::JoinSet<()>,
    stop: watch::Receiver<bool>,
}

impl Calls {
    /// Spawns the call `make` builds for `trellis`, submitted at `submitted`,
    /// and sends its result to `reply`. A shutdown drops the call where it is
    /// and replies [`TrellisError::CancelledByShutdown`] instead.
    fn spawn<T: Send + 'static>(
        &mut self,
        trellis: Arc<Trellis>,
        submitted: Instant,
        reply: oneshot::Sender<Result<T, TrellisError>>,
        make: impl FnOnce(
            Arc<Trellis>,
            Instant,
        ) -> Pin<Box<dyn Future<Output = Result<T, TrellisError>> + Send>>,
    ) {
        let call = make(trellis, submitted);
        let mut stop = self.stop.clone();
        self.tasks.spawn(async move {
            let result = tokio::select! {
                result = call => result,
                // `Err` only once the sender is gone, which `run` keeps until
                // every task has ended: the branch is then disabled, not taken.
                Ok(_) = stop.wait_for(|stopping| *stopping) => {
                    Err(TrellisError::CancelledByShutdown)
                }
            };
            let _ = reply.send(result);
        });
    }
}

/// Issue #587: a public [`Trellis`] method with no [`BlockingTrellis`] twin
/// is unreachable from every binding, and nothing else notices the gap. This
/// reads the crate's source and fails naming any method missing from the
/// blocking side, so adding one to `Trellis` without bridging it (or listing
/// it in `ASYNC_ONLY` with a reason on [`BlockingTrellis`]'s doc) breaks the
/// build's tests rather than a binding author's afternoon.
///
/// It reads source text rather than a hand-kept list of both method sets, so
/// the only thing maintained by hand is the deliberate exception. `syn` would
/// be exact but is a heavy dev-dependency for one check; the scan instead
/// relies on `rustfmt`'s layout, which `verify`'s fmt check guarantees: an
/// inherent `impl` header on one line ending in `{`, closed by a `}` at the
/// same indent, its methods one level in. It visits every `.rs` file under
/// `src/` and every inherent `impl` block of each type, however the type is
/// pathed, so a method added in a second block or another module is still
/// seen. A method generated by a macro is invisible to it.
#[cfg(test)]
mod surface_tests {
    use std::path::Path;

    /// Public `Trellis` methods deliberately left async-only; see
    /// [`super::BlockingTrellis`]'s doc comment for why.
    const ASYNC_ONLY: &[&str] = &["pool"];

    /// Every `.rs` file under `dir`, recursively, as its text.
    fn sources(dir: &Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("read src dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(std::fs::read_to_string(&path).expect("read source file"));
            }
        }
    }

    /// Whether `line` opens an inherent `impl` block of `type_name`
    /// (`impl Trellis {`, `impl crate::app::Trellis {`, ...), not a trait
    /// impl (`impl Display for Trellis {`).
    fn opens_inherent_impl(line: &str, type_name: &str) -> bool {
        let Some(self_type) = line
            .trim_start()
            .strip_prefix("impl ")
            .and_then(|rest| rest.strip_suffix(" {"))
        else {
            return false;
        };
        !self_type.contains(" for ") && self_type.rsplit("::").next() == Some(type_name)
    }

    /// The name of the `pub` method `line` declares, if it declares one at
    /// `indent`, whatever its qualifiers (`const`, `async`, `unsafe`).
    fn public_method_name(line: &str, indent: &str) -> Option<String> {
        let mut rest = line.strip_prefix(indent)?.strip_prefix("pub ")?;
        for qualifier in ["const ", "async ", "unsafe "] {
            rest = rest.strip_prefix(qualifier).unwrap_or(rest);
        }
        let rest = rest.strip_prefix("fn ")?;
        let end = rest.find(['(', '<'])?;
        Some(rest[..end].to_string())
    }

    /// The names of the `pub` methods in every inherent `impl <type_name>`
    /// block across `sources`, and how many such blocks there were.
    fn public_methods(sources: &[String], type_name: &str) -> (Vec<String>, usize) {
        let mut methods = Vec::new();
        let mut blocks = 0;
        for source in sources {
            let mut lines = source.lines();
            while let Some(line) = lines.next() {
                if !opens_inherent_impl(line, type_name) {
                    continue;
                }
                blocks += 1;
                let outer = &line[..line.len() - line.trim_start().len()];
                let close = format!("{outer}}}");
                let inner = format!("{outer}    ");
                for line in lines.by_ref().take_while(|line| *line != close) {
                    methods.extend(public_method_name(line, &inner));
                }
            }
        }
        (methods, blocks)
    }

    #[test]
    fn every_public_trellis_method_has_a_blocking_twin() {
        let mut all = Vec::new();
        sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut all);
        let (trellis, trellis_blocks) = public_methods(&all, "Trellis");
        let (blocking, blocking_blocks) = public_methods(&all, "BlockingTrellis");
        // Guards the scan itself: finding nothing would pass vacuously. Both
        // names sit near the far end of their blocks, so a scan that stopped
        // early misses them too.
        assert!(
            trellis_blocks > 0 && blocking_blocks > 0,
            "no impl blocks found"
        );
        assert!(trellis.contains(&"self_check".to_string()), "{trellis:?}");
        assert!(blocking.contains(&"self_check".to_string()), "{blocking:?}");

        let missing: Vec<&String> = trellis
            .iter()
            .filter(|name| !ASYNC_ONLY.contains(&name.as_str()) && !blocking.contains(name))
            .collect();
        assert!(
            missing.is_empty(),
            "public Trellis methods with no BlockingTrellis twin: {missing:?}. Bridge each in \
             blocking.rs, or add it to ASYNC_ONLY and say why on BlockingTrellis's doc comment"
        );

        for name in ASYNC_ONLY {
            assert!(
                trellis.iter().any(|method| method == name),
                "ASYNC_ONLY lists `{name}`, which is no longer a public Trellis method"
            );
        }
    }

    /// The scan's own edge cases, on text small enough to read at a glance.
    /// The fixture names a type other than `Trellis` because the real scan
    /// reads this file too.
    #[test]
    fn the_scan_sees_every_inherent_block_and_qualifier() {
        let source = "\
impl Fixture {
    pub async fn first(&self) {}
    pub(crate) fn hidden(&self) {}
    fn private(&self) {}
    pub fn generic<T>(&self) {}
}

impl fmt::Display for Fixture {
    pub fn not_inherent(&self) {}
}

mod nested {
    impl crate::app::Fixture {
        pub const fn pathed(&self) {}
        pub unsafe fn qualified(&self) {}
    }
}

impl OtherFixture {
    pub fn someone_else(&self) {}
}
"
        .to_string();
        let (methods, blocks) = public_methods(&[source], "Fixture");
        assert_eq!(blocks, 2);
        assert_eq!(methods, ["first", "generic", "pathed", "qualified"]);
    }
}
