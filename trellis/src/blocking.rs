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
//! [`BlockingTrellis::apply`] is exactly as fast (or slow) as
//! [`Trellis::apply`] (see `app`'s module doc): a plain (non-relationship)
//! 1-1 transform's backfill work is enumerated and persisted, not executed,
//! before it returns. A relationship-enriched 1-1 or an aggregate transform
//! still builds fully synchronously today, so this wrapper simply blocks
//! longer for those two shapes — consistent with, not a regression from,
//! today's async behavior.

use std::time::{Duration, SystemTime};

use tokio::sync::{mpsc, oneshot};
use tokio_postgres::types::PgLsn;

use crate::app::{
    Applied, DefinitionStatus, DefinitionSummary, PoisonEntry, PoisonSample, QuarantineEntry,
    RelationshipSummary, Trellis, TrellisError, TrellisOptions,
};
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
///
/// Every public [`Trellis`] method has a twin here except one, left
/// async-only on purpose: [`Trellis::pool`]. The pool hands out async
/// connections, which a caller with no `tokio` runtime of its own (the whole
/// audience of this type) can't drive, and which were never meant to cross
/// an FFI boundary. A caller that needs its own queries against the target
/// tables opens its own connection. The `surface_tests` module below checks
/// that no other method is missing.
pub struct BlockingTrellis {
    job_tx: mpsc::UnboundedSender<Job>,
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
    /// pool; leaving it at `None` keeps `tokio`'s own per-core default. It's
    /// the one option that only matters here — the async [`Trellis`] never
    /// builds a runtime of its own.
    pub fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let (job_tx, job_rx) = mpsc::unbounded_channel::<Job>();
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

    /// Runs one statement of Trellis's grammar — define a transform, define a
    /// relationship, pause, resume, or drop. The single entrypoint for every
    /// definition-changing operation; see [`Trellis::apply`] for the grammar,
    /// the addressing rules, and each statement's semantics.
    ///
    /// In particular, registering a plain (non-relationship) 1-1 transform
    /// returns before its backfill finishes; poll
    /// [`BlockingTrellis::status`] for [`TransformStatus::Live`](crate::TransformStatus::Live).
    pub fn apply(&self, statement_text: &str) -> Result<Applied, TrellisError> {
        let statement_text = statement_text.to_string();
        self.submit(|reply| Job::Apply(statement_text, reply))
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

    /// Re-reads `source_table` for every definition applying from it, as a
    /// go-live catch-up. See [`Trellis::request_backfill`].
    pub fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        let source_table = source_table.to_string();
        self.submit(|reply| Job::RequestBackfill(source_table, reply))
    }

    /// Poison-quarantine entries recorded since `watermark`, oldest first.
    /// See [`Trellis::poisoned_since`].
    pub fn poisoned_since(&self, watermark: SystemTime) -> Result<Vec<PoisonEntry>, TrellisError> {
        self.submit(|reply| Job::PoisonedSince(watermark, reply))
    }

    /// One registered transform definition's current [`TransformStatus`](crate::TransformStatus), and
    /// its source's backfill failure if any, by target table name. See
    /// [`Trellis::status`].
    pub fn status(&self, target_table: &str) -> Result<Option<DefinitionStatus>, TrellisError> {
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

    /// Whether at least one live drain worker is registered anywhere in
    /// this fleet right now — the health-check-shaped read a Phoenix/Rails
    /// host is meant to poll on a timer. See
    /// [`Trellis::has_live_drain_workers`].
    pub fn has_live_drain_workers(&self) -> Result<bool, TrellisError> {
        self.submit(Job::HasLiveDrainWorkers)
    }

    /// Whether this instance's staging worker is running anywhere in the
    /// fleet right now — the other half of the health check. See
    /// [`Trellis::has_live_staging_worker`].
    pub fn has_live_staging_worker(&self) -> Result<bool, TrellisError> {
        self.submit(Job::HasLiveStagingWorker)
    }

    /// A read-your-writes watermark token. See [`Trellis::watermark_token`].
    pub fn watermark_token(&self) -> Result<PgLsn, TrellisError> {
        self.submit(Job::WatermarkToken)
    }

    /// Blocks until every effect committed at or before `token` has been
    /// reflected in its target(s), or `timeout` elapses. See
    /// [`Trellis::await_converged`].
    ///
    /// **The one method that can occupy the background thread for a
    /// caller-chosen duration.** That thread services jobs strictly one
    /// at a time, so any other `BlockingTrellis` call issued concurrently
    /// from another thread — [`BlockingTrellis::shutdown`] included — queues
    /// behind an in-flight `await_converged` for up to `timeout`. Bounded,
    /// never a hang (`timeout` always expires and the loop moves on), but it
    /// does mean `timeout` sets the worst-case latency of every *other* call
    /// on a shared handle, not just this one. Size it accordingly.
    pub fn await_converged(&self, token: PgLsn, timeout: Duration) -> Result<(), TrellisError> {
        self.submit(|reply| Job::AwaitConverged(token, timeout, reply))
    }

    /// Audits one page of `target_table` against an independent recompute of
    /// its definition from the source. See [`Trellis::self_check`] for what
    /// `scope`, `mode` and `timeout` mean and what the report holds.
    ///
    /// Like [`BlockingTrellis::await_converged`], this can hold the
    /// background thread for up to `timeout` per convergence await it makes
    /// (one under [`SelfCheckMode::Strict`], up to two under
    /// [`SelfCheckMode::Standard`]), plus the comparison itself, so every
    /// other call on a shared handle queues behind it for that long.
    pub fn self_check(
        &self,
        target_table: &str,
        scope: SelfCheckScope,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> Result<SelfCheckReport, TrellisError> {
        let target_table = target_table.to_string();
        self.submit(|reply| Job::SelfCheck(target_table, scope, mode, timeout, reply))
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
///
/// `Some(0)` is rejected here rather than passed through: `tokio`'s
/// `Builder::worker_threads` *panics* on zero, and that panic would fire on
/// the background thread we just spawned — leaving [`BlockingTrellis::connect`]
/// to report the generic
/// [`TrellisError::BlockingThreadExitedBeforeReady`](crate::TrellisError::BlockingThreadExitedBeforeReady)
/// alongside a stray panic on stderr. An FFI caller handing us an integer
/// straight from Elixir or Ruby (issue #141's motivating case) can easily pass
/// zero, so turn it into an ordinary error the caller can read instead.
fn build_runtime(worker_threads: Option<usize>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(worker_threads) = worker_threads {
        if worker_threads == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "TrellisOptions::worker_threads must be at least 1 when set; \
                 leave it as None for tokio's own per-core default",
            ));
        }
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
            Job::Apply(text, reply) => {
                let _ = reply.send(trellis.apply(&text).await);
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
            Job::HasLiveDrainWorkers(reply) => {
                let _ = reply.send(trellis.has_live_drain_workers().await);
            }
            Job::HasLiveStagingWorker(reply) => {
                let _ = reply.send(trellis.has_live_staging_worker().await);
            }
            Job::WatermarkToken(reply) => {
                let _ = reply.send(trellis.watermark_token().await);
            }
            Job::AwaitConverged(token, timeout, reply) => {
                let _ = reply.send(trellis.await_converged(token, timeout).await);
            }
            Job::SelfCheck(table, scope, mode, timeout, reply) => {
                let _ = reply.send(trellis.self_check(&table, scope, mode, timeout).await);
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

    /// `tokio`'s own `Builder::worker_threads` panics on zero, which on the
    /// background thread would surface as the generic
    /// `BlockingThreadExitedBeforeReady` plus a stray panic on stderr. A
    /// zero from an FFI caller must come back as an ordinary error instead.
    #[test]
    fn worker_threads_some_zero_is_an_error_not_a_panic() {
        let err = build_runtime(Some(0)).expect_err("Some(0) must not build a runtime");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("worker_threads"),
            "error should name the offending option, got: {err}"
        );
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
