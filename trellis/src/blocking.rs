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
//! **Blocks only on registration, never on backfill** — for the fast path.
//! [`BlockingTrellis::define`] is exactly as fast (or slow) as
//! [`Trellis::define`] (see `app`'s module doc): a plain (non-relationship)
//! 1-1 transform's backfill work is enumerated and persisted, not executed,
//! before it returns. A relationship-enriched 1-1 or an aggregate transform
//! still builds fully synchronously today, so this wrapper simply blocks
//! longer for those two shapes — consistent with, not a regression from,
//! today's async behavior.

use std::time::SystemTime;

use tokio::sync::{mpsc, oneshot};

use crate::app::{
    DefinitionSummary, PoisonEntry, PoisonSample, QuarantineEntry, RelationshipSummary, Trellis,
    TrellisError, TrellisOptions,
};
use crate::config::Config;
use crate::defs::{Definition, RelationshipDefinition, TransformStatus};

/// One [`BlockingTrellis`] method call, carried over a channel to the
/// dedicated background thread that owns the real, async [`Trellis`] (see
/// the module doc comment). Each variant pairs its call's arguments with a
/// one-shot reply sender; [`Job::Shutdown`] is the one variant that ends the
/// background thread's loop.
enum Job {
    Migrate(oneshot::Sender<Result<(), TrellisError>>),
    Define(String, oneshot::Sender<Result<Definition, TrellisError>>),
    DefineRelationship(
        String,
        oneshot::Sender<Result<RelationshipDefinition, TrellisError>>,
    ),
    Definitions(oneshot::Sender<Result<Vec<DefinitionSummary>, TrellisError>>),
    Relationships(oneshot::Sender<Result<Vec<RelationshipSummary>, TrellisError>>),
    RequestBackfill(String, oneshot::Sender<Result<(), TrellisError>>),
    PoisonedSince(
        SystemTime,
        oneshot::Sender<Result<Vec<PoisonEntry>, TrellisError>>,
    ),
    Status(
        String,
        oneshot::Sender<Result<Option<TransformStatus>, TrellisError>>,
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
    ResumeColumn(
        String,
        oneshot::Sender<Result<Vec<(String, String)>, TrellisError>>,
    ),
    ResumeTransform(String, oneshot::Sender<Result<(), TrellisError>>),
    Shutdown(oneshot::Sender<Result<(), TrellisError>>),
}

/// A synchronous facade over [`Trellis`] — see the module doc comment.
///
/// Every method blocks the calling thread until the background thread
/// replies, but none of them (nor [`BlockingTrellis::connect`] itself)
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
pub struct BlockingTrellis {
    job_tx: mpsc::UnboundedSender<Job>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl BlockingTrellis {
    /// Spawns the dedicated background thread, builds its `tokio` runtime,
    /// and connects the real [`Trellis`] against `config`/`options` (see
    /// [`Trellis::connect`]) — blocking until that setup finishes or fails.
    pub fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let (job_tx, job_rx) = mpsc::unbounded_channel::<Job>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), TrellisError>>();

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

    /// Applies Trellis's schema migrations. See [`Trellis::migrate`].
    pub fn migrate(&self) -> Result<(), TrellisError> {
        self.submit(Job::Migrate)
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

    /// Registers a transform definition and creates its target table. See
    /// [`Trellis::define`] — in particular, this returns before a plain
    /// (non-relationship) 1-1 transform's backfill finishes; poll
    /// [`BlockingTrellis::status`] for [`TransformStatus::Live`].
    pub fn define(&self, definition_text: &str) -> Result<Definition, TrellisError> {
        let definition_text = definition_text.to_string();
        self.submit(|reply| Job::Define(definition_text, reply))
    }

    /// Registers a relationship declaration. See
    /// [`Trellis::define_relationship`].
    pub fn define_relationship(
        &self,
        definition_text: &str,
    ) -> Result<RelationshipDefinition, TrellisError> {
        let definition_text = definition_text.to_string();
        self.submit(|reply| Job::DefineRelationship(definition_text, reply))
    }

    /// Every registered transform definition, oldest first. See
    /// [`Trellis::definitions`].
    pub fn definitions(&self) -> Result<Vec<DefinitionSummary>, TrellisError> {
        self.submit(Job::Definitions)
    }

    /// Every registered relationship declaration, oldest first. See
    /// [`Trellis::relationships`].
    pub fn relationships(&self) -> Result<Vec<RelationshipSummary>, TrellisError> {
        self.submit(Job::Relationships)
    }

    /// Re-stages `source_table`'s current rows for backfill. See
    /// [`Trellis::request_backfill`].
    pub fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        let source_table = source_table.to_string();
        self.submit(|reply| Job::RequestBackfill(source_table, reply))
    }

    /// Poison-quarantine entries recorded since `watermark`, oldest first.
    /// See [`Trellis::poisoned_since`].
    pub fn poisoned_since(&self, watermark: SystemTime) -> Result<Vec<PoisonEntry>, TrellisError> {
        self.submit(|reply| Job::PoisonedSince(watermark, reply))
    }

    /// One registered transform definition's current [`TransformStatus`], by
    /// target table name. See [`Trellis::status`].
    pub fn status(&self, target_table: &str) -> Result<Option<TransformStatus>, TrellisError> {
        let target_table = target_table.to_string();
        self.submit(|reply| Job::Status(target_table, reply))
    }

    /// Every currently paused/quarantined target. See [`Trellis::quarantined`].
    pub fn quarantined(&self) -> Result<Vec<QuarantineEntry>, TrellisError> {
        self.submit(Job::Quarantined)
    }

    /// The current state of one target (`transform` or `transform.column`).
    /// See [`Trellis::quarantine_status`].
    pub fn quarantine_status(&self, target: &str) -> Result<QuarantineEntry, TrellisError> {
        let target = target.to_string();
        self.submit(|reply| Job::QuarantineStatus(target, reply))
    }

    /// A paginated batch of poisoned rows for `target`. See
    /// [`Trellis::sample_quarantined`].
    pub fn sample_quarantined(
        &self,
        target: &str,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<PoisonSample>, TrellisError> {
        let target = target.to_string();
        self.submit(|reply| Job::SampleQuarantined(target, after, limit, reply))
    }

    /// Resumes a paused column. See [`Trellis::resume_column`].
    pub fn resume_column(&self, target: &str) -> Result<Vec<(String, String)>, TrellisError> {
        let target = target.to_string();
        self.submit(|reply| Job::ResumeColumn(target, reply))
    }

    /// Resumes a whole-transform-quarantined transform. See
    /// [`Trellis::resume_transform`].
    pub fn resume_transform(&self, target: &str) -> Result<(), TrellisError> {
        let target = target.to_string();
        self.submit(|reply| Job::ResumeTransform(target, reply))
    }

    /// Stops any background work this connection started and waits for the
    /// background thread to exit cleanly. See [`Trellis::shutdown`].
    pub fn shutdown(mut self) -> Result<(), TrellisError> {
        let result = self.submit(Job::Shutdown);
        if let Some(thread) = self.thread.take() {
            // Unlike `Client::shutdown`'s `tokio::task::spawn_blocking`
            // join, this call has no surrounding runtime of its own to keep
            // free — that's the entire point of this wrapper — so a plain,
            // directly blocking `join` is exactly right here.
            let _ = thread.join();
        }
        result
    }

    /// Sends `make_job(reply)` to the background thread and blocks this
    /// thread on its reply — the one primitive every method above is built
    /// from. A [`TrellisError::BlockingThreadGone`] means the background
    /// thread panicked (or was never actually running the loop below,
    /// which can't happen from this module's own `connect`).
    fn submit<T: Send + 'static>(
        &self,
        make_job: impl FnOnce(oneshot::Sender<Result<T, TrellisError>>) -> Job,
    ) -> Result<T, TrellisError> {
        // `reply_rx.blocking_recv()` below panics (not returns an error) if
        // the calling thread already has a tokio runtime entered — guard it
        // here so misuse (e.g. calling from `#[tokio::test]` or a
        // `tokio::spawn`ed task) surfaces as a normal `TrellisError` instead
        // of an opaque tokio panic.
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(TrellisError::CalledFromAsyncContext);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.job_tx
            .send(make_job(reply_tx))
            .map_err(|_| TrellisError::BlockingThreadGone)?;
        reply_rx
            .blocking_recv()
            .map_err(|_| TrellisError::BlockingThreadGone)?
    }
}

/// Builds the background thread's `tokio` runtime, capping its worker-thread
/// count at `worker_threads` when given (see
/// [`TrellisOptions::worker_threads`]'s doc comment) and leaving `tokio`'s
/// own default (one worker thread per core) otherwise. Split out from
/// [`BlockingTrellis::connect`] so the worker-count wiring itself is
/// unit-testable without needing a real database (see the `tests` module
/// below).
fn build_runtime(worker_threads: Option<usize>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(worker_threads) = worker_threads {
        builder.worker_threads(worker_threads);
    }
    builder.enable_all().build()
}

/// Runs entirely inside the background thread's runtime: connects the real
/// [`Trellis`], signals readiness, then services [`Job`]s until the channel
/// closes (every [`BlockingTrellis`] handle dropped without calling
/// `shutdown`) or [`Job::Shutdown`] is received.
async fn run(
    config: Config,
    options: TrellisOptions,
    mut job_rx: mpsc::UnboundedReceiver<Job>,
    ready_tx: std::sync::mpsc::Sender<Result<(), TrellisError>>,
) {
    let trellis = match Trellis::connect(config, options).await {
        Ok(trellis) => trellis,
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

    while let Some(job) = job_rx.recv().await {
        match job {
            Job::Migrate(reply) => {
                let _ = reply.send(trellis.migrate().await);
            }
            Job::Define(text, reply) => {
                let _ = reply.send(trellis.define(&text).await);
            }
            Job::DefineRelationship(text, reply) => {
                let _ = reply.send(trellis.define_relationship(&text).await);
            }
            Job::Definitions(reply) => {
                let _ = reply.send(trellis.definitions().await);
            }
            Job::Relationships(reply) => {
                let _ = reply.send(trellis.relationships().await);
            }
            Job::RequestBackfill(table, reply) => {
                let _ = reply.send(trellis.request_backfill(&table).await);
            }
            Job::PoisonedSince(watermark, reply) => {
                let _ = reply.send(trellis.poisoned_since(watermark).await);
            }
            Job::Status(table, reply) => {
                let _ = reply.send(trellis.status(&table).await);
            }
            Job::Quarantined(reply) => {
                let _ = reply.send(trellis.quarantined().await);
            }
            Job::QuarantineStatus(target, reply) => {
                let _ = reply.send(trellis.quarantine_status(&target).await);
            }
            Job::SampleQuarantined(target, after, limit, reply) => {
                let _ = reply.send(trellis.sample_quarantined(&target, after, limit).await);
            }
            Job::ResumeColumn(target, reply) => {
                let _ = reply.send(trellis.resume_column(&target).await);
            }
            Job::ResumeTransform(target, reply) => {
                let _ = reply.send(trellis.resume_transform(&target).await);
            }
            Job::Shutdown(reply) => {
                let _ = reply.send(trellis.shutdown().await);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::build_runtime;

    /// Issue #141: an explicit `Some(n)` must actually cap the runtime's
    /// worker-thread count at `n`, not just get accepted and ignored.
    /// `RuntimeMetrics::num_workers` reports the runtime's real worker-thread
    /// count, so this exercises the same `Builder::worker_threads` call
    /// [`super::BlockingTrellis::connect`] makes, without needing a real
    /// database.
    #[test]
    fn worker_threads_some_caps_the_runtime_at_that_count() {
        for n in [1, 2, 3] {
            let runtime = build_runtime(Some(n)).expect("build runtime");
            assert_eq!(
                runtime.handle().metrics().num_workers(),
                n,
                "worker_threads(Some({n})) must produce a runtime with exactly {n} workers"
            );
        }
    }

    /// Issue #141: leaving `worker_threads` at `None` (the default) must
    /// preserve today's behavior untouched — `tokio`'s own per-core default
    /// — rather than this crate silently substituting some other number.
    /// `tokio` computes that default from `std::thread::available_parallelism`
    /// (falling back to 1), so this asserts against that same source rather
    /// than a hardcoded count.
    #[test]
    fn worker_threads_none_preserves_tokios_own_default() {
        // `tokio` lets `TOKIO_WORKER_THREADS` override its default too; skip
        // rather than false-fail if this process happens to run with it set.
        if std::env::var_os("TOKIO_WORKER_THREADS").is_some() {
            return;
        }
        let expected = std::thread::available_parallelism().map_or(1, |n| n.get());
        let runtime = build_runtime(None).expect("build runtime");
        assert_eq!(
            runtime.handle().metrics().num_workers(),
            expected,
            "worker_threads(None) must preserve tokio's own default worker count"
        );
    }
}
