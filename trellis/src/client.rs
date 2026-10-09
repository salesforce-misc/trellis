//! The Client runtime (issue #11's runtime increment): the one thing an
//! embedder starts to get a live Trellis pipeline — capture-trigger
//! installation (issue #622), ring maintenance (seal/recover/reclaim), and N
//! application workers draining sealed batches into target tables.
//!
//! [`Client::start`] spawns one dedicated `std::thread` owning its own
//! `tokio` runtime; everything else described above runs as tasks inside
//! that runtime. This keeps the calling thread free of any tokio-runtime
//! requirement of its own (an embedder that isn't itself async can still
//! call `Client::start`) while giving every task here one shared runtime to
//! communicate through (the shutdown signal, the pool).
//!
//! Two independent knobs, per the constructor contract:
//!
//! - `staging_worker: bool` — whether this client also owns capture
//!   installation and ring maintenance. Exactly one client in a fleet should
//!   set this; every other client (any number of them, across any number of
//!   processes) sets only `application_threads`.
//! - `application_threads: usize` — how many app-worker tasks this client
//!   runs, each independently claiming and draining sealed batches. Zero is
//!   legal: a staging-only client stages and seals but drains nothing.
//!
//! **Which tables to capture** (issue #427, ADR-0016, issue #622): the
//! staging worker derives them from the catalog alone
//! ([`defs::tables_to_capture`]), at startup and on every reconcile pass
//! ([`capture::reconcile`]). There is no caller-supplied list: a table's
//! capture triggers are installed once a registered definition reads it and
//! uninstalled once none does, and the staging worker is the only process
//! that changes them, including after a `DROP`. A staging worker may start
//! with an empty catalog; it captures nothing until something is registered.
//! Changes are staged by the triggers in the writers' own transactions;
//! nothing reads the WAL.
//!
//! **Wake channel**: [`ClientOptions::wake_channel`], by default
//! [`default_wake_channel`] of the catalog schema, is the one Postgres
//! `LISTEN/NOTIFY` channel the backfill discharge (`run_pending_backfills`),
//! a seal actually completing
//! (`staging::seal_if_active_nonempty`/`staging::recover_stuck_seals`, issue
//! #271 — the transition that makes a batch claimable, as opposed to the
//! others in this list, which fire when rows merely land in the *active*
//! segment), and [`super::staging::apply::drain_once`]'s own
//! downstream-propagation `pg_notify` all wake — and the same name every
//! app-worker task `LISTEN`s on while idle. One name, one channel, shared
//! by construction rather than by convention. Each instance in a database
//! has its own default channel, so it never wakes another instance's workers.

use std::fmt;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::capture::{self, CaptureError};
use crate::config::Config;
use crate::defs::chunk_queue;
use crate::defs::{self, CatalogError};
use crate::error_code::{self, ErrorCode};
use crate::intake::{self, IntakeError};
use crate::pool::{Pool, quote_ident};
use crate::staging::{
    self, ApplyError, HeartbeatDaemon, HeartbeatDaemonConfig, ProducerSession, SealConfig,
    StagingError,
};

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

/// Configuration for [`Client::start`].
///
/// Every field beyond `staging_worker`/`application_threads` has a sensible
/// default (see [`Default`]) — most embedders should only need to set the
/// two the constructor contract calls out.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Whether this client owns capture installation and ring maintenance
    /// (seal/recover/reclaim). Exactly one client in a fleet should set
    /// this.
    pub staging_worker: bool,
    /// How many independent application-worker tasks this client runs.
    /// Zero is legal — a staging-only client.
    pub application_threads: usize,
    /// The `LISTEN/NOTIFY` channel shared by the backfill discharge, seals,
    /// apply's downstream propagation, and every idle app-worker task's
    /// `LISTEN`. `None` (the default) uses [`default_wake_channel`] of the
    /// client's catalog schema, so each instance in a database wakes only
    /// its own workers; `Some` overrides it. Read the channel a client uses
    /// with [`Self::wake_channel_for`].
    pub wake_channel: Option<String>,
    /// How long a claim may sit unrefreshed before it's taken back: ring
    /// segment claims by [`staging::reclaim_stale`] (the maintenance loop,
    /// only when `staging_worker` is set), and backfill-chunk claims by
    /// [`chunk_queue::reclaim_stale_chunks`] (every app-worker task,
    /// regardless of `staging_worker`).
    ///
    /// Must be at least twice [`HeartbeatDaemonConfig::interval`] (see
    /// [`Self::heartbeat`]), or [`Client::start`] rejects the options with
    /// [`ClientError::HeartbeatNotUnderReclaimTtl`]: a live worker only
    /// refreshes its claims once per heartbeat interval, so a TTL at or
    /// under that interval reclaims claims out from under workers that are
    /// still running them.
    pub reclaim_ttl: Duration,
    /// How often the maintenance loop (seal/recover/reclaim) ticks.
    pub maintenance_interval: Duration,
    /// How often the maintenance loop re-derives the desired source-table
    /// set from the catalog (issue #14) and re-runs the capture reconcile
    /// ([`capture::reconcile`]) and
    /// [`intake::markers::run_pending_backfills`] against it — so a
    /// transform registered while the client runs gets captured and
    /// backfilled without a restart, and a table whose last reader was
    /// dropped loses its capture triggers (issue #427). Coarser than
    /// `maintenance_interval` by default: unlike seal/reclaim, this does a
    /// catalog query and (when a table is newly read) a `CREATE TRIGGER`,
    /// neither of which needs sub-second freshness. A freshly
    /// parked backfill marker doesn't wait for it: the maintenance loop runs
    /// the pass early when it sees one (issue #476).
    pub reconcile_interval: Duration,
    /// The window [`staging::count_live_drainers`] uses to size a claim's
    /// share of a batch's buckets.
    pub drainer_window: Duration,
    /// The out-of-band heartbeat daemon's tick interval and idle-exit
    /// timeout, one per app-worker task. The same interval also paces each
    /// in-flight backfill chunk's claim refresh. Its `interval` must be at
    /// most half of `reclaim_ttl` (see that field).
    pub heartbeat: HeartbeatDaemonConfig,
    /// How long an app-worker task's idle `LISTEN` wait sits before polling
    /// `next_claimable_segment` again anyway (a floor under `NOTIFY`
    /// delivery, and the only wake source while the listener connection is
    /// down: the worker reopens it in the background with backoff).
    pub poll_interval: Duration,
    /// The most folded records one drain batch holds at once (issue #620,
    /// ADR-0002): a segment share larger than this drains in pages of at most
    /// this many, each its own compute-and-apply transaction, so a worker's
    /// memory is bounded by this cap rather than by how large a segment grew
    /// (a go-live re-read, or a backlog built while seal was refused).
    /// Several small sealed segments coalesce into one drain only while their
    /// ring rows sum to at most this. Defaults to
    /// [`staging::DEFAULT_DRAIN_BATCH_CAP`] (100,000); peak drain memory is
    /// about `application_threads` × this × bytes per change. Zero is treated
    /// as one.
    ///
    /// A share over the cap is folded once into a session `TEMP` table on a
    /// Postgres connection the drain opens outside the pool and closes when
    /// it finishes, so while paging a client holds up to
    /// `application_threads` connections beyond `pool_max_size`. Leave that
    /// headroom under the server's `max_connections`: without it, an
    /// oversized share fails to connect, is released, and retries each poll.
    pub drain_batch_cap: usize,
    /// Source rows per Re-derive build chunk (#625 Q4), read by the drain
    /// worker that runs a build's plan job. Defaults to
    /// [`staging::build::DEFAULT_CHUNK_ROWS`]. Zero is treated as one.
    pub build_chunk_rows: i64,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            staging_worker: false,
            application_threads: 0,
            wake_channel: None,
            reclaim_ttl: staging::DEFAULT_RECLAIM_TTL,
            maintenance_interval: Duration::from_millis(300),
            reconcile_interval: Duration::from_secs(5),
            drainer_window: staging::DEFAULT_DRAINER_WINDOW,
            heartbeat: HeartbeatDaemonConfig::default(),
            poll_interval: Duration::from_millis(200),
            drain_batch_cap: staging::DEFAULT_DRAIN_BATCH_CAP,
            build_chunk_rows: staging::build::DEFAULT_CHUNK_ROWS,
        }
    }
}

impl ClientOptions {
    /// The wake channel a client of catalog schema `schema` uses: the
    /// explicit [`Self::wake_channel`], else [`default_wake_channel`].
    pub fn wake_channel_for(&self, schema: &str) -> String {
        self.wake_channel
            .clone()
            .unwrap_or_else(|| default_wake_channel(schema))
    }
}

/// The longest a `LISTEN/NOTIFY` channel name may be: Postgres's 63-byte
/// identifier limit, which `pg_notify` rejects an overlong name against.
const MAX_CHANNEL_BYTES: usize = 63;

/// The default wake channel for catalog schema `schema`: `<schema>_wake`, so
/// the default instance's channel is `trellis_wake`. A schema too long for
/// that to fit keeps a prefix of its name plus a hash of the whole name, the
/// way [`capture::sql::trigger_name`] does. Distinct schemas get distinct
/// channels, so a seal, drain or backfill in one instance never wakes
/// another instance's idle workers in the same database.
pub fn default_wake_channel(schema: &str) -> String {
    let suffix = "_wake";
    if schema.len() + suffix.len() <= MAX_CHANNEL_BYTES {
        return format!("{schema}{suffix}");
    }
    let hash = format!("{:016x}", capture::sql::fnv1a64(schema.as_bytes()));
    let prefix =
        capture::sql::truncate_to(schema, MAX_CHANNEL_BYTES - suffix.len() - 1 - hash.len());
    format!("{prefix}_{hash}{suffix}")
}

/// The option checks [`Client::start_with_config`] runs before spawning
/// anything, split out so they're unit-testable without a database.
fn validate_options(options: &ClientOptions) -> Result<(), ClientError> {
    if options.heartbeat.interval.saturating_mul(2) > options.reclaim_ttl {
        return Err(ClientError::HeartbeatNotUnderReclaimTtl {
            heartbeat_interval: options.heartbeat.interval,
            reclaim_ttl: options.reclaim_ttl,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Failure modes for [`Client::start`] and [`Client::shutdown`]. Composes
/// the crate's other error types via `From`, matching
/// [`StagingError`]/[`IntakeError`]/[`ApplyError`]/[`crate::error::Error`]'s
/// own hand-rolled-enum convention. [`ClientError::code`] reports a stable,
/// coarse [`ErrorCode`] category for this error alongside its `Display`
/// message — see `docs/decisions/0008-public-api-design.md`, decision 3.
#[derive(Debug)]
pub enum ClientError {
    /// `heartbeat.interval` is more than half of `reclaim_ttl`. Claims
    /// would go stale between two refreshes of a live worker and be
    /// reclaimed and re-run by a peer while the worker is still running
    /// them. A chunk that takes longer than the TTL would then never finish,
    /// because each run's completion is discarded once the claim has moved.
    HeartbeatNotUnderReclaimTtl {
        /// The configured `heartbeat.interval`.
        heartbeat_interval: Duration,
        /// The configured `reclaim_ttl`.
        reclaim_ttl: Duration,
    },
    /// The background thread failed to spawn.
    Spawn(std::io::Error),
    /// The background thread exited (panicked, or its `run` future dropped
    /// its ready-sender) before ever signalling ready.
    ThreadExitedBeforeReady,
    /// [`Client::shutdown`]'s `spawn_blocking` join of the background
    /// thread panicked.
    ThreadPanicked,
    /// A connection/config-layer failure (see [`crate::error::Error`]).
    Config(crate::error::Error),
    /// A direct Postgres protocol/query error, for statements this module
    /// runs itself.
    Db(tokio_postgres::Error),
    /// Reading the catalog for the tables to capture at startup failed.
    Catalog(CatalogError),
    /// A failure from the staging ring (session guards, seal, liveness).
    Staging(StagingError),
    /// A failure from a backfill marker's park or discharge.
    Intake(IntakeError),
    /// A failure from apply (drain_once).
    Apply(ApplyError),
}

impl ClientError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever
    /// one nests here, so the mapping composes rather than re-deriving a
    /// category this crate already has one for.
    pub fn code(&self) -> ErrorCode {
        match self {
            ClientError::HeartbeatNotUnderReclaimTtl { .. } => ErrorCode::Validation,
            ClientError::Spawn(_)
            | ClientError::ThreadExitedBeforeReady
            | ClientError::ThreadPanicked => ErrorCode::Internal,
            ClientError::Config(err) => err.code(),
            ClientError::Db(err) => error_code::classify_pg_error(err),
            ClientError::Catalog(err) => err.code(),
            ClientError::Staging(err) => err.code(),
            ClientError::Intake(err) => err.code(),
            ClientError::Apply(err) => err.code(),
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::HeartbeatNotUnderReclaimTtl {
                heartbeat_interval,
                reclaim_ttl,
            } => write!(
                f,
                "ClientOptions::heartbeat.interval ({heartbeat_interval:?}) must be at most half of \
                 ClientOptions::reclaim_ttl ({reclaim_ttl:?}); otherwise live workers' claims go \
                 stale between heartbeats and are reclaimed while still in flight"
            ),
            ClientError::Spawn(err) => {
                write!(f, "failed to spawn the client's runtime thread: {err}")
            }
            ClientError::ThreadExitedBeforeReady => write!(
                f,
                "the client's runtime thread exited before signalling that setup completed"
            ),
            ClientError::ThreadPanicked => {
                write!(
                    f,
                    "the client's runtime thread panicked while shutting down"
                )
            }
            ClientError::Config(err) => write!(f, "{err}"),
            ClientError::Db(err) => {
                write!(f, "client setup database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            ClientError::Catalog(err) => write!(f, "{err}"),
            ClientError::Staging(err) => write!(f, "{err}"),
            ClientError::Intake(err) => write!(f, "{err}"),
            ClientError::Apply(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Spawn(err) => Some(err),
            ClientError::Config(err) => Some(err),
            ClientError::Db(err) => Some(err),
            ClientError::Catalog(err) => Some(err),
            ClientError::Staging(err) => Some(err),
            ClientError::Intake(err) => Some(err),
            ClientError::Apply(err) => Some(err),
            ClientError::HeartbeatNotUnderReclaimTtl { .. }
            | ClientError::ThreadExitedBeforeReady
            | ClientError::ThreadPanicked => None,
        }
    }
}

impl From<crate::error::Error> for ClientError {
    fn from(err: crate::error::Error) -> Self {
        ClientError::Config(err)
    }
}

impl From<CatalogError> for ClientError {
    fn from(err: CatalogError) -> Self {
        ClientError::Catalog(err)
    }
}

impl From<tokio_postgres::Error> for ClientError {
    fn from(err: tokio_postgres::Error) -> Self {
        ClientError::Db(err)
    }
}

impl From<StagingError> for ClientError {
    fn from(err: StagingError) -> Self {
        ClientError::Staging(err)
    }
}

impl From<IntakeError> for ClientError {
    fn from(err: IntakeError) -> Self {
        ClientError::Intake(err)
    }
}

impl From<ApplyError> for ClientError {
    fn from(err: ApplyError) -> Self {
        ClientError::Apply(err)
    }
}

// ---------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------

/// A running Trellis client: a dedicated background thread (owning its own
/// `tokio` runtime) plus a shutdown signal to stop it.
///
/// Dropping a `Client` without calling [`Client::shutdown`] signals shutdown
/// (best-effort) but does not wait for the thread to exit — call `shutdown`
/// to observe a clean stop.
pub struct Client {
    shutdown_tx: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Client {
    /// Starts a client against `dsn`. Blocks (synchronously) until the
    /// background thread has finished setup (the staging-worker singleton and
    /// the first capture pass, if `staging_worker`) and every worker task is
    /// spawned, or
    /// until setup fails.
    ///
    /// The instance schema is resolved from the process environment
    /// (`TRELLIS_SCHEMA`, defaulting to [`crate::config::DEFAULT_SCHEMA`]) via
    /// [`Config::from_dsn`]. A caller that already holds a [`Config`] — or
    /// that needs two clients in one process to run in two different schemas
    /// — must use [`Client::start_with_config`] instead; see its doc comment.
    pub fn start(dsn: impl Into<String>, options: ClientOptions) -> Result<Client, ClientError> {
        Self::start_with_config(Config::from_dsn(dsn.into())?, options)
    }

    /// [`Client::start`]'s instance-aware form (issue #234): starts a client
    /// against an already-resolved [`Config`], so the client runs in *that*
    /// config's schema (`docs/instance-identity.md`) rather than whatever a
    /// process-global `TRELLIS_SCHEMA` happens to say.
    ///
    /// # Why this exists
    ///
    /// [`Client::start`] used to take only a DSN and resolve its own `Config`
    /// from the process environment. That made the instance schema a property
    /// of the *process*, not of the client, with two consequences:
    ///
    /// * [`crate::Trellis`] is constructed from an explicit `Config` and
    ///   handed its own `config.dsn()` to `Client::start` — so a `Trellis`
    ///   built with `Config::with_schema(dsn, "some_schema")` silently ran
    ///   its background client in `TRELLIS_SCHEMA`/[`crate::config::DEFAULT_SCHEMA`]
    ///   instead, against a different instance's staging ring entirely. The
    ///   schema was accepted, validated, and then dropped on the floor.
    /// * Two Trellis instances could not coexist *in one process* at all,
    ///   even though `docs/instance-identity.md` describes exactly that
    ///   topology for one cluster — one process-global environment variable
    ///   cannot carry two different schemas.
    ///
    /// Found by `generative/tests/two_instance_noise.rs` (issue #234's
    /// two-instance side-by-side property), which is also its regression
    /// coverage.
    ///
    /// Blocks (synchronously) until the background thread has finished setup
    /// and every worker task is spawned, or until setup fails — same
    /// contract as [`Client::start`].
    pub fn start_with_config(
        config: Config,
        options: ClientOptions,
    ) -> Result<Client, ClientError> {
        let dsn = config.dsn().to_string();
        validate_options(&options)?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), ClientError>>();

        let thread = std::thread::Builder::new()
            .name("trellis-client".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(err) => {
                        let _ = ready_tx.send(Err(ClientError::Spawn(err)));
                        return;
                    }
                };
                runtime.block_on(run(dsn, config, options, shutdown_rx, ready_tx));
            })
            .map_err(ClientError::Spawn)?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Client {
                shutdown_tx,
                thread: Some(thread),
            }),
            Ok(Err(err)) => {
                // Setup failed; the thread is already exiting (or exited)
                // on its own. Best-effort join so we don't leak it, but
                // don't let a join failure mask the real setup error.
                let _ = thread.join();
                Err(err)
            }
            Err(_) => {
                let _ = thread.join();
                Err(ClientError::ThreadExitedBeforeReady)
            }
        }
    }

    /// Signals shutdown and waits for the background thread to exit
    /// cleanly: the maintenance loop and every app-worker task are joined
    /// after cooperatively exiting at their next loop boundary.
    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        let _ = self.shutdown_tx.send(true);
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|_| ClientError::ThreadPanicked)?
                .map_err(|_| ClientError::ThreadPanicked)?;
        }
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Best-effort: wake any still-running tasks so the process doesn't
        // hang on exit even if the caller never awaited `shutdown`. Doesn't
        // join — a `Drop` impl can't be async.
        let _ = self.shutdown_tx.send(true);
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------
// The background thread's runtime: setup + task spawning + shutdown wait
// ---------------------------------------------------------------------

/// Runs entirely inside the dedicated thread's runtime. Performs setup
/// (staging worker only), spawns every task, signals readiness, then waits
/// for shutdown before stopping them.
async fn run(
    dsn: String,
    // Issue #234: resolved by the caller ([`Client::start_with_config`]) and
    // passed in, rather than re-resolved from the process environment here —
    // see that constructor's doc comment for the two bugs the old
    // `Config::from_dsn(dsn)` on this line caused.
    config: Config,
    options: ClientOptions,
    mut shutdown_rx: watch::Receiver<bool>,
    ready_tx: std::sync::mpsc::Sender<Result<(), ClientError>>,
) {
    let pool = match Pool::new(&config) {
        Ok(pool) => pool,
        Err(err) => {
            let _ = ready_tx.send(Err(err.into()));
            return;
        }
    };

    let mut staging_session = None;
    // One resolved name for every notify and every idle worker's `LISTEN`.
    let wake_channel = options.wake_channel_for(config.schema());

    if options.staging_worker {
        match setup_staging(&dsn, &config, &pool).await {
            Ok(session) => staging_session = Some(session),
            Err(err) => {
                let _ = ready_tx.send(Err(err));
                return;
            }
        }
    }

    // Issue #132's guard (a) reads this "staged-through" watermark. Trigger
    // capture (issue #622 C5) stages every change in its writer's own
    // transaction, so every commit a snapshot can see is already in the ring:
    // the watermark is always caught up, in every process of a fleet.
    let watermark = staging::StagedWatermark::saturated();

    let mut maintenance_task = None;
    if let Some(session) = staging_session {
        let maintenance_config = MaintenanceConfig {
            dsn: dsn.clone(),
            schema: config.schema().to_string(),
            pool: pool.clone(),
            session,
            wake_channel: wake_channel.clone(),
            interval: options.maintenance_interval,
            reclaim_ttl: options.reclaim_ttl,
            drainer_window: options.drainer_window,
            reconcile_interval: options.reconcile_interval,
            watermark: watermark.clone(),
            backfill_catch_up_timeout: BACKFILL_CATCH_UP_TIMEOUT,
        };
        maintenance_task = Some(tokio::spawn(maintenance_loop(
            maintenance_config,
            shutdown_rx.clone(),
        )));
    }

    // Unique across processes and restarts (issue #756): see `new_worker_id`.
    let client_id = new_worker_id();

    // Issue #144, ADR-0010 decision 3: register this process in the
    // worker registry the moment it starts running drain workers — before
    // any app-worker task is even spawned, so a health check racing
    // `Client::start` sees this worker as soon as it's real. Deliberately
    // one row per `Client` (keyed by `client_id`, not per app-worker task's
    // own `claimed_by`): the question `Trellis::has_live_drain_workers`
    // answers is "does a live process exist to drain work," not "how many
    // worker tasks does it run." Only when `application_threads > 0` — a
    // staging-only client (capture + ring maintenance, no app workers)
    // does no draining, so it must not register as though it did.
    if options.application_threads > 0
        && let Ok(conn) = pool.get().await
    {
        let _ = staging::register_worker(&**conn, &client_id).await;
    }

    let mut app_worker_tasks = Vec::with_capacity(options.application_threads);
    for i in 0..options.application_threads {
        let claimed_by = format!("{client_id}-app-{i}");
        let worker_config = AppWorkerConfig {
            pool: pool.clone(),
            dsn: dsn.clone(),
            schema: config.schema().to_string(),
            claimed_by,
            worker_id: client_id.clone(),
            wake_channel: wake_channel.clone(),
            drainer_window: options.drainer_window,
            heartbeat_config: options.heartbeat.clone(),
            poll_interval: options.poll_interval,
            reclaim_ttl: options.reclaim_ttl,
            chunk_reclaim_interval: options.maintenance_interval,
            watermark: watermark.clone(),
            drain_batch_cap: options.drain_batch_cap,
            build_chunk_rows: options.build_chunk_rows.max(1),
        };
        app_worker_tasks.push(tokio::spawn(app_worker_loop(
            worker_config,
            shutdown_rx.clone(),
        )));
    }

    if ready_tx.send(Ok(())).is_err() {
        // The calling thread gave up waiting (e.g. it never actually reads
        // the channel because `Client::start` itself was dropped mid-call,
        // which cannot happen from this crate's own API, but a defensive
        // shutdown here costs nothing).
    }

    // Cooperative tasks (maintenance, app workers) watch the shutdown
    // signal themselves and exit at their next loop boundary; wait for the
    // signal here too, then join everything.
    let _ = shutdown_rx.changed().await;

    if let Some(task) = maintenance_task {
        let _ = task.await;
    }
    for task in app_worker_tasks {
        let _ = task.await;
    }

    // Issue #144: clean-shutdown removal, once every app-worker task this
    // process registered under `client_id` has actually stopped — the
    // counterpart to the registration above. An unclean shutdown (a crash,
    // `kill -9`, or a dropped `Client` that never reaches this point at all)
    // leaves the row behind; that's expected, not a bug — see
    // `staging::worker_registry`'s doc comment for how `has_live_workers`
    // and `reclaim_stale_workers` handle that case via the reused reclaim
    // TTL rather than requiring this line to have run.
    if options.application_threads > 0
        && let Ok(conn) = pool.get().await
    {
        let _ = staging::deregister_worker(&**conn, &client_id).await;
    }
}

/// This `Client`'s worker id: its worker-registry key, and the prefix of
/// every app-worker task's `claimed_by` (`{id}-app-{i}`). Shaped
/// `trellis-[{host}-]{pid}-{random}`, with 64 random bits as 16 hex digits.
///
/// It must be unique across every process sharing the database, and across
/// restarts of one process (issue #756). A claim is "mine" by `claimed_by`
/// alone: `held_share` hands a drain every `seg_claims` row carrying its
/// `claimed_by`, and each page's claim check and completion `delete` match on
/// it, as do the chunk queue's `ClaimFence` and `finish_chunk`. Two workers
/// sharing an id both drain the same buckets and both pass every check, so a
/// stale page (a truncate's clear, say) can commit over a newer one. The id
/// used to be the calling thread's `ThreadId`, which only numbers threads
/// within one process: every replica's first `Client` came out the same.
///
/// The host and pid only make the id readable in logs and in the claim
/// tables; the random part alone carries the uniqueness.
fn new_worker_id() -> String {
    let mut bytes = [0u8; 8];
    let random = match getrandom::fill(&mut bytes) {
        Ok(()) => u64::from_le_bytes(bytes),
        // No OS randomness (practically unheard of): std's `RandomState`
        // seeds its own keys per process, so a hash of the clock under it
        // still differs between processes.
        Err(_) => {
            use std::hash::{BuildHasher, Hash, Hasher};
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            std::time::SystemTime::now().hash(&mut hasher);
            std::process::id().hash(&mut hasher);
            hasher.finish()
        }
    };
    worker_id(host_name().as_deref(), std::process::id(), random)
}

/// The machine's host name (a pod name, under Kubernetes), if it's cheap to
/// find: Linux's `/proc`, else `$HOSTNAME`.
fn host_name() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
}

/// [`new_worker_id`]'s format. The host is cut to at most 24 of the
/// characters a host name may use, and dropped if none are left.
fn worker_id(host: Option<&str>, pid: u32, random: u64) -> String {
    let host: String = host
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '.')
        .take(24)
        .collect();
    if host.is_empty() {
        format!("trellis-{pid}-{random:016x}")
    } else {
        format!("trellis-{host}-{pid}-{random:016x}")
    }
}

#[cfg(test)]
mod worker_id_tests {
    use super::*;

    /// Issue #756: the old id was a function of the calling thread alone, so
    /// two processes starting a `Client` from the same-numbered thread got
    /// the same one. Two ids minted on one thread stand in for them: they
    /// must differ, and so must every `claimed_by` derived from them.
    #[test]
    fn ids_minted_on_the_same_thread_differ() {
        let a = new_worker_id();
        let b = new_worker_id();
        assert_ne!(a, b);
        assert_ne!(format!("{a}-app-0"), format!("{b}-app-0"));
    }

    /// The same host and pid (a restarted process that got its old pid back,
    /// or pid 1 in two containers) still differ by the random part.
    #[test]
    fn the_same_host_and_pid_differ_by_the_random_part() {
        assert_ne!(
            worker_id(Some("web-1"), 1, 1),
            worker_id(Some("web-1"), 1, 2)
        );
    }

    #[test]
    fn the_id_is_scannable_and_bounded() {
        assert_eq!(
            worker_id(Some("web-7f9c\n"), 42, 0xab),
            "trellis-web-7f9c-42-00000000000000ab"
        );
        assert_eq!(worker_id(None, 42, 0xab), "trellis-42-00000000000000ab");
        assert_eq!(
            worker_id(Some(" \n"), 42, 0xab),
            "trellis-42-00000000000000ab"
        );
        let long = "a".repeat(200);
        let id = worker_id(Some(&long), u32::MAX, u64::MAX);
        assert_eq!(
            id,
            format!("trellis-{}-4294967295-ffffffffffffffff", "a".repeat(24))
        );
    }
}

// ---------------------------------------------------------------------
// Staging setup: the staging-worker singleton and the first capture pass
// ---------------------------------------------------------------------

/// Takes the staging-worker singleton and runs one capture reconcile pass
/// ([`capture::reconcile::reconcile`], issue #622 C5), parking a marker for
/// every definition the pass finds ready. The maintenance loop's first pass
/// discharges them.
///
/// Returns the [`ProducerSession`] that holds the singleton. The
/// maintenance loop runs every step on it, and re-takes the singleton when it
/// reconnects, so the lock is held for as long as the staging worker runs and
/// `liveness::has_live_staging_worker` reads it from `pg_locks`. A second
/// staging worker for the same instance fails here with
/// [`StagingError::ProducerAlreadyRunning`].
///
/// A table whose capture can't be installed yet (a lock held on it, a
/// missing primary key) doesn't fail the start: the pass logs it, and every
/// maintenance pass tries again.
///
/// A pass that fails as a whole releases the singleton before this returns
/// (#687), as the maintenance loop does when it stops. Dropping the session
/// would free it only once the server noticed the closed connection, so a
/// client restarted at once could fail with `ProducerAlreadyRunning`.
async fn setup_staging(
    dsn: &str,
    config: &Config,
    pool: &Pool,
) -> Result<ProducerSession, ClientError> {
    let mut session = ProducerSession::connect(dsn, config.schema()).await?;
    match first_capture_pass(&mut session, config, pool).await {
        Ok(()) => Ok(session),
        Err(err) => {
            release_singleton(Some(session)).await;
            Err(err)
        }
    }
}

/// [`setup_staging`]'s capture pass, on the session holding the singleton.
async fn first_capture_pass(
    session: &mut ProducerSession,
    config: &Config,
    pool: &Pool,
) -> Result<(), ClientError> {
    let tables = defs::tables_to_capture(pool).await?;
    let deadline = Instant::now() + RECONCILE_DDL_BUDGET;
    let mut outcome =
        capture::reconcile::reconcile(session.client_mut(), config.schema(), &tables, deadline)
            .await
            .map_err(capture_pass_error)?;
    release_retyped_keys(pool).await;
    complete_pause_cascades(pool).await;
    let taken =
        staging::build::start_ready_builds(session.client_mut(), pool, &outcome.ready).await?;
    outcome.ready.retain(|id| !taken.contains(id));
    intake::markers::park_ready_registration_markers(session.client(), &outcome.ready).await?;
    Ok(())
}

/// Releases the keys a capture pass's in-place re-types asked for (#824,
/// [`staging::quarantine::release_retyped_keys`]). A failure is logged, and
/// the next pass tries again: the requests stay until their keys are
/// released.
async fn release_retyped_keys(pool: &Pool) {
    if let Err(err) = staging::quarantine::release_retyped_keys(pool).await {
        tracing::warn!(
            error = %err,
            "couldn't release the keys held before an in-place re-type; retrying next pass"
        );
    }
}

/// Finishes any column pause's cascade that didn't complete (#912,
/// [`staging::quarantine::complete_pause_cascades`]). A walk that fails is
/// logged there, and the next pass tries again: a pause stays marked until
/// its cascade reaches every dependent. Only a failure to read the marks
/// reaches here.
async fn complete_pause_cascades(pool: &Pool) {
    if let Err(err) = staging::quarantine::complete_pause_cascades(pool).await {
        tracing::warn!(
            error = %err,
            "couldn't read the column pauses owing a cascade; retrying next pass"
        );
    }
}

/// The [`ClientError`] for a capture pass that couldn't run at all. Only
/// reading the catalog fails a pass; a table's own failure is in its outcome.
fn capture_pass_error(err: CaptureError) -> ClientError {
    match err {
        CaptureError::Catalog(err) => ClientError::Catalog(err),
        CaptureError::Db(err) => ClientError::Db(err),
        CaptureError::Marker(err) => ClientError::Intake(err),
        // Not raised by a pass as a whole.
        other => ClientError::Config(crate::error::Error::Config(other.to_string())),
    }
}

// ---------------------------------------------------------------------
// Ring maintenance loop: seal / recover / reclaim
// ---------------------------------------------------------------------

/// Everything [`maintenance_loop`] needs — bundled into a struct purely to
/// keep the function signature within clippy's argument-count lint, same as
/// [`AppWorkerConfig`].
struct MaintenanceConfig {
    dsn: String,
    schema: String,
    pool: Pool,
    /// The staging-worker singleton [`setup_staging`] took; the loop's own
    /// connection.
    session: ProducerSession,
    wake_channel: String,
    interval: Duration,
    reclaim_ttl: Duration,
    /// The window a drainer counts as live within
    /// ([`ClientOptions::drainer_window`]): the drainer sweep keeps a row at
    /// least this long.
    drainer_window: Duration,
    reconcile_interval: Duration,
    /// The staged-through watermark a backfill enumeration waits on before
    /// staging (issue #312). Always caught up under trigger capture (issue
    /// #622 C5), so the wait is a no-op; the discharge's fence wait stays.
    watermark: staging::StagedWatermark,
    /// [`BACKFILL_CATCH_UP_TIMEOUT`] outside tests.
    backfill_catch_up_timeout: Duration,
}

/// Runs seal-on-demand, stuck-seal recovery, stale-claim reclaim,
/// drained-segment retirement (stage 06, issue #13/#58), and — on its own,
/// coarser cadence — capture/backfill re-reconciliation (issue #14) on a
/// fixed tick until shutdown. Rides only with the staging worker (see the
/// module doc comment) — application-only clients never run this, since
/// sealing/recovery/reclaim/retirement/reconciliation are ring-wide
/// operations that must not be duplicated across every client in a fleet.
///
/// Holds one dedicated connection across ticks (reconnecting lazily on
/// error) rather than opening a fresh one every tick. That connection is a
/// [`ProducerSession`]: it holds the staging-worker singleton (issue #622
/// C5), and a reconnect takes it again. While another session holds it (this
/// worker's previous backend, until the server notices it is gone, or another
/// staging worker that took over), the reconnect fails and is retried every
/// tick.
async fn maintenance_loop(config: MaintenanceConfig, mut shutdown_rx: watch::Receiver<bool>) {
    let MaintenanceConfig {
        dsn,
        schema,
        pool,
        session,
        wake_channel,
        interval,
        reclaim_ttl,
        drainer_window,
        reconcile_interval,
        watermark,
        backfill_catch_up_timeout,
    } = config;

    let seal_config = SealConfig::default();
    // The capture pass rate-limits its logs in memory under the instance's
    // database and schema; the loop forgets them when it stops.
    let database: Option<String> = session
        .client()
        .query_one("select pg_catalog.current_database()::text", &[])
        .await
        .ok()
        .map(|row| row.get(0));
    let stop = async |session: Option<ProducerSession>| {
        if let Some(database) = &database {
            capture::reconcile::forget_instance(database, &schema);
        }
        release_singleton(session).await;
    };
    let mut session: Option<ProducerSession> = Some(session);
    let mut failures = StepFailures::default();
    // Due immediately on the very first tick rather than waiting a full
    // `reconcile_interval` after startup — `setup_staging` already ran one
    // reconciliation pass at that point, but this makes the loop's own
    // cadence not depend on when it happens to first observe `Instant::now()`.
    let mut next_reconcile = Instant::now();
    // Issue #476: whether a fresh marker may pull the next reconcile pass
    // forward. See [`early_pass_allowed`].
    let mut early_pass = true;

    loop {
        if *shutdown_rx.borrow() {
            stop(session).await;
            return;
        }

        if session.is_none() {
            let connected = ProducerSession::connect(&dsn, &schema).await;
            session = failures.check("connect", connected).ok();
        }

        if let Some(held) = session.as_mut() {
            let c = held.client_mut();
            let sealed = staging::seal_if_active_nonempty(c, &wake_channel).await;
            let mut failed = failures.check("seal", sealed).is_err();
            if !failed {
                let recovered = staging::recover_stuck_seals(c, &seal_config, &wake_channel).await;
                failed = failures.check("recover_stuck_seals", recovered).is_err();
            }
            if !failed {
                let reclaimed = staging::reclaim_stale(c, reclaim_ttl).await;
                failed = failures.check("reclaim_stale", reclaimed).is_err();
            }
            if !failed {
                // Backfill chunks' own reclaim-stale sweep (docs/decisions/0007's
                // amendment): a drain worker that died mid-chunk leaves a
                // stale claim here, freed the same way a stale `seg_claims`
                // row is above.
                let reclaimed = chunk_queue::reclaim_stale_chunks(c, reclaim_ttl).await;
                failed = failures.check("reclaim_stale_chunks", reclaimed).is_err();
            }
            if !failed {
                // Issue #144: table hygiene for `worker_registry` after an
                // unclean shutdown elsewhere in the fleet — a crashed
                // drain-only client's row would otherwise sit forever.
                // `staging::has_live_workers` never depends on this having
                // run (see `staging::worker_registry`'s doc comment); this
                // is purely about bounding the table's size over time.
                let reclaimed = staging::reclaim_stale_workers(c, reclaim_ttl).await;
                failed = failures.check("reclaim_stale_workers", reclaimed).is_err();
            }
            if !failed {
                // Issue #756: the same hygiene for `drainers`. Worker ids are
                // unique per process start, so a restarted or crashed
                // process's drainer rows would otherwise sit forever. Kept at
                // least a drainer window, so no row a claim still counts goes.
                let reclaimed =
                    staging::reclaim_stale_drainers(c, reclaim_ttl.max(drainer_window)).await;
                failed = failures.check("reclaim_stale_drainers", reclaimed).is_err();
            }
            if !failed {
                let retired = staging::retire_drained_segments(c).await;
                failed = failures.check("retire_drained_segments", retired).is_err();
            }
            if !failed {
                let collected = staging::collect_tombstones(c).await;
                failed = failures.check("collect_tombstones", collected).is_err();
            }
            if !failed {
                // ADR-0009 decision 5's staging_segments{state} gauge: cheap
                // to read here since maintenance_loop already ticks on this
                // connection regardless, and a failed read just skips a
                // gauge refresh rather than derailing the tick's other work.
                let counts = staging::segment_state_counts(c).await;
                if let Ok(counts) = failures.check("segment_state_counts", counts) {
                    for (state, count) in counts {
                        crate::metrics::set_staging_segments(state.as_sql(), count as u64);
                    }
                }
            }
            if !failed && early_pass && Instant::now() < next_reconcile {
                // Issue #476: a marker nothing has fenced yet (a finished
                // build's go-live catch-up, say) is discharged on this tick,
                // not a whole `reconcile_interval` later.
                let wanted = intake::markers::discharge_wanted(&*c).await;
                match failures.check("discharge_wanted", wanted) {
                    Ok(true) => next_reconcile = Instant::now(),
                    Ok(false) => {}
                    Err(_) => failed = true,
                }
            }
            if !failed && Instant::now() >= next_reconcile {
                // A discharge can sit waiting for a fresh fence to settle
                // (issue #431). The wait watches the shutdown signal itself
                // and gives up by leaving the marker for the next start (see
                // `run_pending_backfills_until`).
                let shutting_down = || *shutdown_rx.borrow();
                let started = Instant::now();
                let reconciled = reconcile_source_tables(
                    c,
                    &pool,
                    &schema,
                    &wake_channel,
                    &watermark,
                    backfill_catch_up_timeout,
                    &shutting_down,
                )
                .await;
                failed = failures
                    .check("reconcile_source_tables", reconciled)
                    .is_err();
                early_pass = early_pass_allowed(started.elapsed(), backfill_catch_up_timeout);
                next_reconcile = Instant::now() + reconcile_interval;
            }
            if failed {
                // Drop and reconnect next tick rather than spin on a wedged
                // connection; every one of these operations is naturally
                // idempotent/retriable, so skipping a tick costs nothing but
                // latency. The singleton is released first where the
                // connection still works, so the reconnect can take it again
                // at once.
                release_singleton(session.take()).await;
            }
        }

        tokio::select! {
            _ = shutdown_rx.changed() => {
                stop(session).await;
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// One staging-worker reconcile pass, exactly as the maintenance loop runs it
/// ([`reconcile_source_tables`]), for tests that step the staging worker by
/// hand (issue #622 C5). The error is the pass's, as text.
#[cfg(feature = "internals")]
pub async fn reconcile_pass(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    schema: &str,
    wake_channel: &str,
    catch_up_timeout: Duration,
) -> Result<(), String> {
    reconcile_source_tables(
        client,
        pool,
        schema,
        wake_channel,
        &staging::StagedWatermark::saturated(),
        catch_up_timeout,
        &|| false,
    )
    .await
    .map_err(|err| err.to_string())
}

/// Releases the staging-worker singleton on the server, if `session` holds
/// it and its connection still works, so the next session to ask for it (a
/// reconnect, a restarted client) gets it at once instead of waiting for the
/// server to notice the dropped connection.
async fn release_singleton(session: Option<ProducerSession>) {
    if let Some(held) = session
        && !held.client().is_closed()
    {
        let _ = held.release().await;
    }
}

/// How long one [`reconcile_source_tables`] pass spends retrying capture
/// installs, widens and uninstalls on tables whose lock is held (ADR-0002
/// I6, issue #622 C5). Each attempt waits at most
/// `locks::USER_TABLE_DDL_LOCK_TIMEOUT` (50 ms) for the table, so no
/// application writer queues behind one for longer. The first attempt on
/// each table always runs, so one blocked table can't starve another. The
/// maintenance loop is the only sealer, so this is also about the longest a
/// pass holds up sealing on locks.
const RECONCILE_DDL_BUDGET: Duration = Duration::from_secs(1);

/// Whether [`maintenance_loop`] may run its next reconcile pass early, as
/// soon as `intake::markers::discharge_wanted` finds a fresh marker
/// (issue #476), given how long its last pass took.
///
/// A pass that took a whole `catch_up_timeout` spent it waiting on a fresh
/// fence that a long transaction elsewhere in the cluster holds open. The
/// loop is the only sealer, so it seals nothing meanwhile. Issue #431 bounds
/// that stall to one timeout per pass, and passes used to come one
/// `reconcile_interval` apart. Early passes don't, so markers
/// parked one after another (a batch of builds finishing) behind such a
/// transaction would each start a pass that waits it out, back to back, and
/// sealing would all but stop. After a pass like that, the next one waits
/// for the regular interval, as every pass did before #476.
fn early_pass_allowed(last_pass: Duration, catch_up_timeout: Duration) -> bool {
    last_pass < catch_up_timeout
}

/// How often [`StepFailures`] repeats the `warn` for a step that keeps
/// failing tick after tick. The ticks in between log at `debug`.
const STEP_FAILURE_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Issue #408: reports [`maintenance_loop`]'s step failures, which it used
/// to discard. Every step is retried, so a failure is worth a `warn` but not
/// a flood of them: the loop ticks every `maintenance_interval` (300ms by
/// default), and a database outage fails the connect on every one. So each
/// step warns on its first failure and then at most once per
/// [`STEP_FAILURE_WARN_INTERVAL`] while it keeps failing, logging the
/// attempts in between at `debug`, and the step's next success logs one
/// `info` that it has recovered.
///
/// Tracked per step, and only by the step's own outcome: a tick on which a
/// step didn't run (skipped after an earlier step failed, or not due yet,
/// like the reconcile every `reconcile_interval`) says nothing about it. A
/// tick-wide "no step failed" would call a reconcile that fails every pass
/// recovered on each tick in between, and so re-warn every pass.
#[derive(Default)]
struct StepFailures {
    /// Each step whose latest run failed.
    failing: std::collections::HashMap<&'static str, FailingStep>,
}

struct FailingStep {
    /// Consecutive failed runs.
    failures: u64,
    last_warned: Instant,
}

impl StepFailures {
    /// Records `step`'s outcome and hands `result` back.
    fn check<T, E: fmt::Display>(
        &mut self,
        step: &'static str,
        result: Result<T, E>,
    ) -> Result<T, E> {
        match &result {
            Ok(_) => self.succeeded(step),
            Err(error) => self.failed(step, error, Instant::now()),
        }
        result
    }

    fn failed(&mut self, step: &'static str, error: &dyn fmt::Display, now: Instant) {
        let entry = self.failing.entry(step).or_insert(FailingStep {
            failures: 0,
            last_warned: now,
        });
        entry.failures += 1;
        if entry.failures == 1
            || now.duration_since(entry.last_warned) >= STEP_FAILURE_WARN_INTERVAL
        {
            entry.last_warned = now;
            tracing::warn!(
                step,
                error = %error,
                failures = entry.failures,
                "maintenance step failed; retrying"
            );
        } else {
            tracing::debug!(step, error = %error, failures = entry.failures, "maintenance step failed again");
        }
    }

    fn succeeded(&mut self, step: &'static str) {
        if let Some(failing) = self.failing.remove(step) {
            tracing::info!(
                step,
                failures = failing.failures,
                "maintenance step recovered"
            );
        }
    }
}

/// How often [`DrainFailures`] repeats the `warn` for a drain error that
/// keeps recurring. The occurrences in between log at `debug`.
const DRAIN_FAILURE_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Issue #660: reports [`app_worker_loop`]'s failed drain calls, which it
/// used to release and retry without a word. A drain error is worth a
/// `warn`, but a wedged segment fails the same way on every tick (the
/// 200ms poll floor by default), and a drain that can't get a connection
/// fails on every tick too. So each distinct error, by its text, warns the
/// first time and then at most once per [`DRAIN_FAILURE_WARN_INTERVAL`]
/// while it keeps recurring, saying how many repeats it collapsed. The
/// repeats in between log at `debug` with the same fields.
///
/// "By its text" means its first line (see [`drain_failure_key`]): a
/// Postgres error's `DETAIL`/`HINT` lines can differ on every occurrence
/// (a deadlock names the backend pids involved), and keying on them would
/// warn every time. The logged `error` field is always the full text.
///
/// An error that hasn't recurred for a whole interval is forgotten, which
/// keeps this bounded: its next occurrence warns as a new one would.
#[derive(Default)]
struct DrainFailures {
    /// Each error seen within the last interval, by [`drain_failure_key`].
    recent: std::collections::HashMap<String, RecentDrainFailure>,
}

/// The part of a drain error's text that [`DrainFailures`] collapses
/// repeats by: its first line. Every error that nests a Postgres error
/// (`crate::error::write_pg_error`) ends its first line with the server's
/// `severity: message`, and tokio-postgres puts `DETAIL` and `HINT` on the
/// lines after it.
fn drain_failure_key(text: &str) -> &str {
    text.split('\n').next().unwrap_or(text)
}

struct RecentDrainFailure {
    /// Occurrences since the last `warn`, each logged at `debug`.
    collapsed: u64,
    last_warned: Instant,
    last_seen: Instant,
}

impl DrainFailures {
    /// Logs a drain of `seg_seqs` by `claimant` that failed with `error`,
    /// after which the worker released the `released` `(seg_seq, bucket)`
    /// claims, or failed to release them with the `Err` text.
    fn failed(
        &mut self,
        error: &ApplyError,
        seg_seqs: &[i64],
        released: &Result<Vec<(i64, i16)>, String>,
        claimant: &str,
        now: Instant,
    ) {
        self.recent
            .retain(|_, seen| now.duration_since(seen.last_seen) < DRAIN_FAILURE_WARN_INTERVAL);
        let class = staging::quarantine::classify(error);
        let buckets = match released {
            Ok(released) => released_buckets(released),
            // The claims stay until the reclaim sweep takes them. `none`
            // would claim the drain held nothing.
            Err(err) => format!("unknown; release failed: {err}"),
        };
        let text = error.to_string();
        let (recent, first) = match self.recent.entry(drain_failure_key(&text).to_string()) {
            std::collections::hash_map::Entry::Occupied(seen) => (seen.into_mut(), false),
            std::collections::hash_map::Entry::Vacant(new) => (
                new.insert(RecentDrainFailure {
                    collapsed: 0,
                    last_warned: now,
                    last_seen: now,
                }),
                true,
            ),
        };
        recent.last_seen = now;
        if first || now.duration_since(recent.last_warned) >= DRAIN_FAILURE_WARN_INTERVAL {
            tracing::warn!(
                error = %text,
                class = ?class,
                segments = ?seg_seqs,
                buckets = %buckets,
                claimant,
                collapsed = recent.collapsed,
                "drain failed; released its claims to retry"
            );
            recent.collapsed = 0;
            recent.last_warned = now;
        } else {
            recent.collapsed += 1;
            tracing::debug!(
                error = %text,
                class = ?class,
                segments = ?seg_seqs,
                buckets = %buckets,
                claimant,
                collapsed = recent.collapsed,
                "drain failed again"
            );
        }
    }
}

/// `(seg_seq, bucket)` claims, sorted by segment, as `5:[0,1] 6:[3]`, or
/// `none` when the drain held no claim by the time it was released.
fn released_buckets(released: &[(i64, i16)]) -> String {
    if released.is_empty() {
        return "none".to_string();
    }
    let mut out = String::new();
    for (i, &(seg_seq, bucket)) in released.iter().enumerate() {
        if i == 0 || released[i - 1].0 != seg_seq {
            if i > 0 {
                out.push_str("] ");
            }
            out.push_str(&format!("{seg_seq}:["));
        } else {
            out.push(',');
        }
        out.push_str(&bucket.to_string());
    }
    out.push(']');
    out
}

/// Failure modes [`reconcile_source_tables`] composes, purely so its `?`
/// call sites don't have to hand-unwrap two unrelated error enums
/// ([`CatalogError`] from the desired-table-set query, [`IntakeError`] from
/// the reconcile/backfill calls themselves) — [`maintenance_loop`] only
/// needs to know it failed and to log it, so this never needs to be more
/// than that.
#[derive(Debug)]
enum ReconcileError {
    Catalog(CatalogError),
    Intake(IntakeError),
    Capture(CaptureError),
    /// Starting a Re-derive build failed (#625 F2).
    Build(ApplyError),
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReconcileError::Catalog(err) => write!(f, "{err}"),
            ReconcileError::Intake(err) => write!(f, "{err}"),
            ReconcileError::Capture(err) => write!(f, "{err}"),
            ReconcileError::Build(err) => write!(f, "{err}"),
        }
    }
}

impl From<ApplyError> for ReconcileError {
    fn from(err: ApplyError) -> Self {
        ReconcileError::Build(err)
    }
}

impl From<CaptureError> for ReconcileError {
    fn from(err: CaptureError) -> Self {
        ReconcileError::Capture(err)
    }
}

impl From<CatalogError> for ReconcileError {
    fn from(err: CatalogError) -> Self {
        ReconcileError::Catalog(err)
    }
}

impl From<IntakeError> for ReconcileError {
    fn from(err: IntakeError) -> Self {
        ReconcileError::Intake(err)
    }
}

/// How long one discharge pass waits for the fences it just took to settle
/// (issue #431) before leaving their markers for a later pass. It runs this
/// long only while a long transaction is open elsewhere in the cluster. The
/// maintenance loop does no sealing meanwhile, so this is also the longest
/// seal stall one pass can add. The value is a judgement call, not a
/// measured bound.
const BACKFILL_CATCH_UP_TIMEOUT: Duration = Duration::from_secs(5);

/// Issue #14: re-derives the desired source-table set from the catalog
/// ([`defs::tables_to_capture`]), brings every table's capture triggers to
/// it ([`capture::reconcile::reconcile`], issue #622 C5), parks a marker for
/// every newly registered definition whose capture is now current, and
/// discharges the pending markers. Re-run periodically, so a transform
/// registered while this client runs is picked up without a restart.
///
/// Issue #427, ADR-0016: the catalog is the only input. A table's capture is
/// uninstalled on the first pass after its last reader is dropped, and this
/// is the only place that happens: a `DROP` removes catalog rows and nothing
/// more.
///
/// The capture pass runs before the discharge, on the same connection, so a
/// definition is never dispatched before the install or widen its columns
/// need (see `capture::install`'s "Widening and the capture gate"). The
/// discharge dispatches only the definitions the pass found ready.
///
/// The pass gives locked tables [`RECONCILE_DDL_BUDGET`] in all, then leaves
/// them for the next pass, which comes one `reconcile_interval` later. The
/// discharge's watermark wait is a no-op (`watermark` is always caught up
/// under trigger capture); its fence wait stays, because a marker on a
/// seam-fed table still needs it (#622 plan finding 1).
#[allow(clippy::too_many_arguments)]
async fn reconcile_source_tables(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    schema: &str,
    wake_channel: &str,
    watermark: &staging::StagedWatermark,
    catch_up_timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(), ReconcileError> {
    let desired = defs::tables_to_capture(pool).await?;
    let deadline = Instant::now() + RECONCILE_DDL_BUDGET;
    let mut outcome = capture::reconcile::reconcile(client, schema, &desired, deadline).await?;
    release_retyped_keys(pool).await;
    complete_pause_cascades(pool).await;
    // #625 F2/F3: a ready definition the Re-derive build takes gets no
    // registration marker, and the discharge below doesn't dispatch it.
    let taken = staging::build::start_ready_builds(client, pool, &outcome.ready).await?;
    outcome.ready.retain(|id| !taken.contains(id));
    intake::markers::park_ready_registration_markers(&*client, &outcome.ready).await?;
    // The markers whose discharge failed are already logged and backed off on
    // their own rows (issue #407). Only a failure of the pass itself errors,
    // and costs this connection a reconnect.
    intake::markers::run_pending_backfills_for(
        client,
        wake_channel,
        watermark,
        catch_up_timeout,
        stop,
        Some(&outcome.ready),
    )
    .await?;
    Ok(())
}

/// Opens a standalone `tokio_postgres` connection with `search_path`
/// pinned, for the tests that need a concrete `tokio_postgres::Client`.
#[cfg(test)]
async fn connect_plain(
    dsn: &str,
    schema: &str,
) -> Result<tokio_postgres::Client, crate::error::Error> {
    let (client, connection) = crate::pool::connect_dedicated(dsn).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&crate::pool::dedicated_session_setup(schema))
        .await?;
    Ok(client)
}

// ---------------------------------------------------------------------
// Application worker loop: claim / fold / apply
// ---------------------------------------------------------------------

/// Everything one [`app_worker_loop`] task needs — bundled into a struct
/// rather than passed as separate arguments purely to keep the function
/// signature within clippy's argument-count lint; there's no other reason
/// these fields are grouped.
struct AppWorkerConfig {
    pool: Pool,
    dsn: String,
    schema: String,
    claimed_by: String,
    /// This `Client`'s own worker-registry key (issue #144) — the same
    /// `client_id` every app-worker task of this `Client` shares, distinct
    /// from `claimed_by`'s per-task suffix. One row represents the process,
    /// not each individual task, so every task of the same `Client` heartbeats
    /// the same row (a harmless, idempotent upsert either way).
    worker_id: String,
    wake_channel: String,
    drainer_window: Duration,
    heartbeat_config: HeartbeatDaemonConfig,
    poll_interval: Duration,
    /// How long a backfill-chunk claim may sit unrefreshed before it's swept
    /// as stale — the same value [`MaintenanceConfig::reclaim_ttl`] uses for
    /// the ring's own claims. Passed here too (issue: chunk-reclaim sweep
    /// availability) so this sweep runs at the fleet's one configured TTL
    /// regardless of whether this particular client also happens to run the
    /// staging worker.
    reclaim_ttl: Duration,
    /// How often this app-worker task sweeps `backfill_chunks` for a stale
    /// claim (see [`sweep_stale_chunks_if_due`]) — independent of
    /// `ClientOptions::staging_worker`, since a stale backfill-chunk claim
    /// isn't a capture/ring concern the way segment maintenance is (see
    /// this field's own call site's doc comment). Reuses
    /// `ClientOptions::maintenance_interval`'s cadence rather than inventing
    /// a third interval knob.
    chunk_reclaim_interval: Duration,
    /// Issue #132, epic #127, guard (a): this fleet's shared in-process
    /// "staged-through" watermark — the one `run()` constructs, always
    /// caught up under trigger capture — threaded here so
    /// [`staging::drain_many`]'s own Phase 3 apply can check guard (a) for
    /// any relationship reverse record it drains.
    watermark: staging::StagedWatermark,
    /// [`ClientOptions::drain_batch_cap`]: bounds both how many segments one
    /// drain coalesces and how many folded records one page holds.
    drain_batch_cap: usize,
    /// [`ClientOptions::build_chunk_rows`], at least one.
    build_chunk_rows: i64,
}

/// One application-worker task: registers itself as a drainer, runs an
/// out-of-band heartbeat daemon for its ring-segment claims, then loops
/// claiming and draining sealed batches *and* claiming and executing pending
/// direct-build backfill chunks (docs/decisions/0007's amendment) until
/// shutdown — both kinds of claimable work share this one loop/worker pool,
/// per `docs/decisions/0008-public-api-design.md`'s decision 1 ("it's `application_threads`
/// that finishes transform work, backfill included").
///
/// On a non-retryable error from [`staging::drain_once`] (anything
/// `drain_once` itself gave up retrying — a fence miss and a serialization
/// failure are already retried internally up to its own attempt cap), this
/// releases the claim immediately (see [`staging::release_segments`]) rather
/// than leaving it to the reclaim TTL, logs the error (see [`DrainFailures`]),
/// then deregisters the heartbeat and continues: one bad batch never crashes
/// the worker. A backfill chunk that
/// fails to execute is released the same way (see [`drain_backfill_chunks`]),
/// left for the reclaim-stale sweep or a retry by whichever worker claims it
/// next.
async fn app_worker_loop(config: AppWorkerConfig, mut shutdown_rx: watch::Receiver<bool>) {
    let AppWorkerConfig {
        pool,
        dsn,
        schema,
        claimed_by,
        worker_id,
        wake_channel,
        drainer_window,
        heartbeat_config,
        poll_interval,
        reclaim_ttl,
        chunk_reclaim_interval,
        watermark,
        drain_batch_cap,
        build_chunk_rows,
    } = config;

    // Captured before `heartbeat_config` is moved into `HeartbeatDaemon::spawn`
    // below: `drain_backfill_chunks` needs this same cadence for its own
    // per-chunk-claim heartbeat (see its doc comment) — a chunk write and a
    // segment drain should heartbeat at the same margin under `reclaim_ttl`.
    let chunk_heartbeat_interval = heartbeat_config.interval;
    let build_options = staging::build::WorkerOptions {
        chunk_rows: build_chunk_rows,
        drain_batch_cap,
        heartbeat_interval: chunk_heartbeat_interval,
        reclaim_ttl,
    };
    let heartbeat = HeartbeatDaemon::spawn(dsn.clone(), schema.clone(), heartbeat_config);
    let mut wake = WakeListener::spawn(dsn.clone(), schema.clone(), wake_channel.clone());

    // Due immediately on the very first tick, same as `maintenance_loop`'s
    // own `next_reconcile` — see `sweep_stale_chunks_if_due`.
    let mut next_chunk_reclaim = Instant::now();
    // Issue #144: same "due immediately" reasoning — `Client::run` already
    // registers `worker_id` once before this loop starts, but that row's
    // `last_seen` must keep advancing on this same cadence for the whole
    // life of the worker, not just once at startup.
    let mut next_worker_heartbeat = Instant::now();
    let mut drain_failures = DrainFailures::default();
    // The group-delta merges that failed on this worker, backing off (#901).
    let mut merge_failures = staging::build::MergeFailures::default();

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        // Backfill-chunk reclaim sweep (independent of `staging_worker` —
        // see `AppWorkerConfig::chunk_reclaim_interval`'s doc comment): a
        // drain-only fleet (`staging_worker: false`, `application_threads` >
        // 0, no other client running `staging_worker: true` anywhere) would
        // otherwise have nothing to free a crashed drain worker's claimed
        // chunk — `maintenance_loop`'s own sweep only ever runs alongside the
        // staging worker. Cheap/idempotent to also run this when a staging
        // worker *is* present in the same process (its `maintenance_loop`
        // sweeps too): a no-op UPDATE matching zero rows either way.
        sweep_stale_chunks_if_due(
            &pool,
            reclaim_ttl,
            chunk_reclaim_interval,
            &mut next_chunk_reclaim,
        )
        .await;

        // Issue #144: keeps this process's worker-registry row alive on the
        // same cadence as the chunk-reclaim sweep above, for the same
        // "independent of `staging_worker`" reason — a drain-only fleet has
        // no `maintenance_loop` anywhere to do this instead.
        heartbeat_worker_if_due(
            &pool,
            &worker_id,
            chunk_reclaim_interval,
            &mut next_worker_heartbeat,
        )
        .await;

        // `register_drainer` doubles as the liveness refresh
        // `count_live_drainers` reads below (see its own doc comment), so it
        // does need to run on every iteration — `drainers.last_seen` decays
        // over `drainer_window` (30s by default), and this loop's own
        // `poll_interval` floor (200ms by default) is comfortably inside
        // that window even when idle. A drain can outlast the window (a
        // paged one runs for minutes), so while this worker's claims are
        // registered with `heartbeat`, the daemon refreshes the same row on
        // its own tick (issue #654). What's wasteful isn't the cadence,
        // it's checking out a separate pooled connection just for it: one
        // connection serves both this and `next_claimable_segments` below.
        // A failed refresh isn't fatal — it costs this worker one tick of
        // undercounting toward the share denominator, not correctness — so
        // it doesn't block trying `next_claimable_segments` on the same
        // connection.
        //
        // Issue #63 Milestone 2: asks for several segments at once rather
        // than just the lowest one, so a burst of quickly sealing segments
        // (many ready before this worker gets back around to claiming)
        // drains in one coalesced `drain_many` call instead of one full
        // compute-and-apply pass per segment. Issue #620: as many as fit
        // `drain_batch_cap` by row count; a segment over the cap comes back
        // alone and pages.
        let seg_seqs = match pool.get().await {
            Ok(client) => {
                let _ = staging::register_drainer(&**client, &claimed_by).await;
                staging::next_claimable_segments(&**client, drain_batch_cap).await
            }
            Err(err) => Err(err.into()),
        };

        let seg_seqs = match seg_seqs {
            Ok(seqs) if !seqs.is_empty() => seqs,
            Ok(_) | Err(_) => {
                // No claimable segment this tick (or the lookup itself
                // failed): build work comes next (#625 B6). Only actually
                // wait if it made no progress — otherwise loop straight
                // back around without an idle wait.
                let build_progress = build_step(
                    &pool,
                    &claimed_by,
                    chunk_heartbeat_interval,
                    reclaim_ttl,
                    &build_options,
                    &mut merge_failures,
                )
                .await;
                if !build_progress
                    && wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await
                {
                    break;
                }
                continue;
            }
        };

        for &seg_seq in &seg_seqs {
            heartbeat.register(seg_seq, claimed_by.clone()).await;
        }

        let live_workers = match pool.get().await {
            Ok(client) => staging::count_live_drainers(&**client, drainer_window)
                .await
                .unwrap_or(1),
            Err(_) => 1,
        };

        let outcome = staging::drain_many_with_cap(
            &pool,
            &seg_seqs,
            &claimed_by,
            live_workers,
            &wake_channel,
            &watermark,
            drain_batch_cap,
        )
        .await;
        if let Err(error) = &outcome {
            // Release immediately rather than waiting on the reclaim TTL:
            // `drain_many` has already exhausted its own internal retries
            // by the time it returns an error, so nothing about waiting
            // longer helps, and every tick this worker holds a claim
            // un-refreshed is a tick some other worker can't pick it up.
            // The release reports the buckets it freed, so the log line
            // (issue #660) can name them.
            let released = match pool.get().await {
                Ok(client) => staging::release_segments(&**client, &seg_seqs, &claimed_by)
                    .await
                    .map_err(|err| err.to_string()),
                Err(err) => Err(err.to_string()),
            };
            drain_failures.failed(error, &seg_seqs, &released, &claimed_by, Instant::now());
        }

        for &seg_seq in &seg_seqs {
            heartbeat.deregister(seg_seq, &claimed_by).await;
        }

        // Back off before re-looping unless we actually drained something.
        // `next_claimable_segments` picks the lowest sealed/undrained
        // segments by state alone, so any iteration that made no progress on
        // them would otherwise be re-selected and spun on hot across every
        // worker with no sleep. Two no-progress cases:
        //   - `Err(_)`: a batch that fails deterministically (a poison
        //     change); principled quarantine is issue #16.
        //   - `Ok(None)`: this call won no buckets — every bucket of every
        //     requested segment is already claimed by a peer (or already
        //     drained) — but they're still `draining`, so
        //     `next_claimable_segments` hands back the same seqs until the
        //     peer finishes.
        // Only `Ok(Some(_))` re-loops immediately, to grab the next batch
        // promptly. The poll floor (or a wake/shutdown) bounds the idle wait.
        let drained = matches!(outcome, Ok(Some(_)));
        // #625 B6: segments first, for every kind of build work. A worker
        // that drained loops straight back to the ring; one that won nothing
        // there takes build work instead.
        let build_progress = !drained
            && build_step(
                &pool,
                &claimed_by,
                chunk_heartbeat_interval,
                reclaim_ttl,
                &build_options,
                &mut merge_failures,
            )
            .await;
        let made_progress = drained || build_progress;
        if !made_progress && wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await {
            break;
        }
    }
}

/// A drain worker's build work for one pass of its loop, after its segments
/// (#625 B6, global since F3): one old-build chunk (a plain 1-1 range or a
/// direct-build job, [`drain_backfill_chunks`]), then one step of the
/// Re-derive builds' work ([`rederive_build_step`]). Returns whether either
/// did work.
async fn build_step(
    pool: &Pool,
    claimed_by: &str,
    chunk_heartbeat_interval: Duration,
    reclaim_ttl: Duration,
    build_options: &staging::build::WorkerOptions,
    merge_failures: &mut staging::build::MergeFailures,
) -> bool {
    let old = drain_backfill_chunks(pool, claimed_by, chunk_heartbeat_interval, reclaim_ttl).await;
    let rederive = rederive_build_step(pool, claimed_by, build_options, merge_failures).await;
    old || rederive
}

/// One step of the running Re-derive builds' work
/// ([`staging::build::work_once`], #625 F2), after this worker's segments.
/// Returns whether it did work. A failure of the step itself (not of a chunk,
/// which `chunk_queue::fail_chunk` records and logs, nor of a merge, which
/// `merge_failures` backs off and `staging::build` classifies) is logged at
/// warn and retried on the next pass.
async fn rederive_build_step(
    pool: &Pool,
    claimed_by: &str,
    options: &staging::build::WorkerOptions,
    merge_failures: &mut staging::build::MergeFailures,
) -> bool {
    match staging::build::work_once(pool, claimed_by, options, merge_failures).await {
        Ok(step) => step.progressed(),
        Err(error) => {
            tracing::warn!(
                worker = %claimed_by,
                error = %error,
                "re-derive build step failed; retrying on the next pass"
            );
            false
        }
    }
}

/// Runs [`chunk_queue::reclaim_stale_chunks`] if `interval` has elapsed since
/// `next_due` (mutated in place to the next due time, mirroring
/// `maintenance_loop`'s own `next_reconcile` throttle), else does nothing.
///
/// Gap this closes (public-api-design review): `reclaim_stale_chunks` used to
/// only ever run from [`maintenance_loop`], which is only spawned `if
/// options.staging_worker`. A drain-only client (`staging_worker: false`,
/// `application_threads > 0` — a normal, documented fleet topology) had no
/// self-healing for a crashed drain worker's claimed chunk: it would sit
/// stuck at `Backfilling` forever unless some *other* client instance in the
/// fleet happened to also run with `staging_worker: true`. Unlike segment
/// maintenance (legitimately tied to the staging-worker singleton), reclaiming
/// a stale backfill-chunk claim has nothing to do with capture, so it's
/// wired here instead — into the one loop every client with
/// `application_threads > 0` runs regardless of `staging_worker`.
async fn sweep_stale_chunks_if_due(
    pool: &Pool,
    reclaim_ttl: Duration,
    interval: Duration,
    next_due: &mut Instant,
) {
    if Instant::now() < *next_due {
        return;
    }
    if let Ok(mut client) = pool.get().await {
        let _ = chunk_queue::reclaim_stale_chunks(&mut **client, reclaim_ttl).await;
    }
    *next_due = Instant::now() + interval;
}

/// Refreshes `worker_id`'s [`staging::worker_registry`] row (issue #144) if
/// `interval` has elapsed since `next_due`, else does nothing — same
/// due-timer shape as [`sweep_stale_chunks_if_due`], and for the same
/// underlying reason: `Client::run`'s registration at startup is a single
/// point-in-time write, but the row's `last_seen` must keep advancing for
/// [`staging::has_live_workers`] to keep reporting this worker live, on a
/// cadence that has to work whether or not this `Client` also runs
/// `maintenance_loop` (i.e. regardless of `staging_worker`).
async fn heartbeat_worker_if_due(
    pool: &Pool,
    worker_id: &str,
    interval: Duration,
    next_due: &mut Instant,
) {
    if Instant::now() < *next_due {
        return;
    }
    if let Ok(client) = pool.get().await {
        let _ = staging::register_worker(&**client, worker_id).await;
    }
    *next_due = Instant::now() + interval;
}

/// Claims one pending direct-build backfill chunk (`defs::chunk_queue`,
/// docs/decisions/0007's amendment) and executes it, marking it done
/// (flipping its definition `backfilling` -> `live` once every chunk is done
/// — see `chunk_queue::finish_chunk`) or, on a write error, giving it up at
/// once through `chunk_queue::fail_chunk`, which records and logs the
/// failure and retries, narrows or pauses (#616) — the same "release on
/// error rather than wait out the TTL" discipline [`app_worker_loop`]'s
/// segment path uses.
/// Returns whether it claimed anything, so the caller's own idle-wait
/// decision treats a tick that only did backfill work as progress too.
///
/// `heartbeat_interval` is threaded straight through to
/// [`chunk_queue::run_claimed_chunk`] — this app-worker's own
/// [`HeartbeatDaemonConfig::interval`] (the same cadence its segment claims
/// heartbeat at), so a chunk write outliving `reclaim_ttl` isn't falsely
/// reclaimed mid-write (see `chunk_queue::run_claimed_chunk`'s doc comment).
///
/// Exactly one chunk per call, because that heartbeat only covers the chunk
/// that is executing. This used to claim a batch of up to four and run them
/// one after another, so every chunk queued behind the running one sat
/// claimed with no refresh at all. Once the running chunk took longer than
/// `reclaim_ttl`, a peer's sweep reclaimed the queued ones and ran them too,
/// and this worker's own completion of them was discarded (`finish_chunk`
/// is scoped to the current claimant). Claiming one at a time costs nothing:
/// the caller loops straight back around after any backfill progress, and a
/// peer can pick up the next chunk in the meantime.
async fn drain_backfill_chunks(
    pool: &Pool,
    claimed_by: &str,
    heartbeat_interval: Duration,
    reclaim_ttl: Duration,
) -> bool {
    let claimed = match pool.get().await {
        Ok(client) => chunk_queue::claim_chunks(&**client, claimed_by, 1).await,
        Err(err) => Err(err.into()),
    };
    let Some(chunk) = claimed.ok().and_then(|chunks| chunks.into_iter().next()) else {
        return false;
    };

    match chunk_queue::run_claimed_chunk(pool, &chunk, claimed_by, heartbeat_interval, reclaim_ttl)
        .await
    {
        Ok(()) => {
            let _ = chunk_queue::finish_chunk(pool, &chunk, claimed_by).await;
        }
        // `fail_chunk` logs the failure and what it did about it (#616).
        Err(err) => {
            if let Err(fail_err) = chunk_queue::fail_chunk(pool, &chunk, claimed_by, &err).await {
                tracing::warn!(
                    definition_id = chunk.definition_id,
                    chunk_id = chunk.id,
                    error = %err,
                    record_error = %fail_err,
                    "backfill chunk failed, and recording the failure failed too; the \
                     stale-claim sweep frees the chunk for a retry"
                );
            }
        }
    }
    true
}

/// Waits for a wake notification, the poll-interval floor, or shutdown —
/// whichever comes first. Returns `true` if shutdown fired.
///
/// Issue #313: a closed wake channel is *not* a wake. It used to be read as
/// one — `recv()` on a closed channel returns `None` immediately — so an idle
/// worker whose `LISTEN` connection had died looped at full speed with no
/// sleep at all. [`WakeListener`] now reopens its own connection and never
/// closes the channel while it is alive, so `None` here only means its
/// supervisor task is gone. Then the wait falls back to the poll floor, the
/// same as a worker that never had a listener.
async fn wait_for_wake(
    wake: &mut WakeListener,
    shutdown_rx: &mut watch::Receiver<bool>,
    poll_interval: Duration,
) -> bool {
    let notified = async {
        if wake.rx.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    };

    tokio::select! {
        _ = shutdown_rx.changed() => true,
        _ = tokio::time::sleep(poll_interval) => false,
        _ = notified => false,
    }
}

/// First delay before reopening a wake listener's `LISTEN` connection after
/// it closed or failed to open. Doubles on each consecutive failed open, up to
/// [`WAKE_REOPEN_BACKOFF_MAX`], and resets once an open succeeds.
const WAKE_REOPEN_BACKOFF_BASE: Duration = Duration::from_millis(250);
/// Ceiling on [`WAKE_REOPEN_BACKOFF_BASE`]'s doubling, so a database that is
/// down for a long time costs one connection attempt per this interval.
const WAKE_REOPEN_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// A self-healing `LISTEN` on the wake channel, delivering one `()` per
/// notification to [`wait_for_wake`].
///
/// A background task owns the connection. When the connection closes, or
/// can't be opened in the first place, the task backs off and reopens it
/// (see [`keep_listening`]); meanwhile the worker polls on its
/// `poll_interval` floor alone. The channel therefore stays open for as long
/// as this struct lives, and a dead backend never looks like a wake
/// (issue #313).
///
/// Dropping this aborts the task, which drops the connection's
/// `tokio_postgres::Client` and so closes the connection.
struct WakeListener {
    rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    task: tokio::task::JoinHandle<()>,
}

impl WakeListener {
    fn spawn(dsn: String, schema: String, channel: String) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(keep_listening(
            move |tx| open_wake_session(dsn.clone(), schema.clone(), channel.clone(), tx),
            tx,
            WAKE_REOPEN_BACKOFF_BASE,
            WAKE_REOPEN_BACKOFF_MAX,
        ));
        Self { rx, task }
    }
}

impl Drop for WakeListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// [`WakeListener`]'s supervisor. Opens a listening session with `open`,
/// runs it until its connection closes, and opens a new one, sleeping between
/// attempts so a session that keeps dying or a database that keeps refusing
/// connections can't spin: the sleep starts at `base` after a session ends,
/// and doubles (capped at `max`) for each consecutive open that fails.
///
/// Every successful open sends one wake. Any `NOTIFY` sent while no `LISTEN`
/// was active is lost, including the ones before the very first open, so the
/// worker has to take one look at the queue once the `LISTEN` is in place.
/// That wake is sent only after the `LISTEN` has committed, so a commit that
/// lands between the two is still caught by its own notification.
///
/// Returns once the receiving side is gone.
async fn keep_listening<Open, OpenFut, Session, E>(
    mut open: Open,
    tx: tokio::sync::mpsc::UnboundedSender<()>,
    base: Duration,
    max: Duration,
) where
    Open: FnMut(tokio::sync::mpsc::UnboundedSender<()>) -> OpenFut,
    OpenFut: std::future::Future<Output = Result<Session, E>>,
    Session: std::future::Future<Output = ()>,
    E: fmt::Display,
{
    let mut delay = base;
    while !tx.is_closed() {
        match open(tx.clone()).await {
            Ok(session) => {
                delay = base;
                let _ = tx.send(());
                session.await;
                tracing::warn!(
                    retry_in = ?delay,
                    "wake listener's LISTEN connection closed; polling until it reopens"
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    retry_in = ?delay,
                    "wake listener could not open its LISTEN connection; polling until it does"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max);
            }
        }
    }
}

/// Opens one `LISTEN` session on `channel` for [`keep_listening`]: connects,
/// starts forwarding notifications to `tx`, and issues the `LISTEN`. The
/// returned future resolves when the connection closes. It holds the
/// `tokio_postgres::Client` until then, because dropping the client would
/// close the connection it is waiting on.
async fn open_wake_session(
    dsn: String,
    schema: String,
    channel: String,
    tx: tokio::sync::mpsc::UnboundedSender<()>,
) -> Result<impl std::future::Future<Output = ()>, crate::error::Error> {
    let (client, mut connection) = crate::pool::connect_dedicated(&dsn).await?;
    let driver = tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
                    let _ = tx.send(());
                }
                Some(Ok(_)) => continue,
                _ => break,
            }
        }
    });

    if let Err(err) = client
        .batch_execute(&format!(
            "{}; listen {}",
            crate::pool::dedicated_session_setup(&schema),
            quote_ident(&channel)
        ))
        .await
    {
        driver.abort();
        return Err(err.into());
    }

    Ok(async move {
        let _client = client;
        let _ = driver.await;
    })
}

#[cfg(test)]
mod wake_listener_tests {
    //! Issue #313: a dead `LISTEN` connection must neither read as a wake nor
    //! be reopened in a tight loop. Paused-clock tests, no Postgres.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Regression: `recv()` on a closed channel returns `None` at once, and
    /// `wait_for_wake` used to count that as a wake, so every call returned
    /// immediately and the idle worker loop spun with no sleep.
    #[tokio::test(start_paused = true)]
    async fn a_closed_wake_channel_waits_out_the_poll_floor() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        drop(tx);
        let mut wake = WakeListener {
            rx,
            task: tokio::spawn(async {}),
        };
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let poll_interval = Duration::from_millis(200);

        let started = tokio::time::Instant::now();
        for _ in 0..3 {
            assert!(!wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await);
        }
        assert_eq!(
            started.elapsed(),
            poll_interval * 3,
            "each wait on a closed listener must sit out the full poll floor"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_notification_still_ends_the_wait_early() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let mut wake = WakeListener {
            rx,
            task: tokio::spawn(async {}),
        };
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        tx.send(()).unwrap();

        let started = tokio::time::Instant::now();
        assert!(!wait_for_wake(&mut wake, &mut shutdown_rx, Duration::from_secs(60)).await);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    /// The supervisor reopens a closed session after `base`, backs off
    /// exponentially across failed opens, resets after a successful one, and
    /// wakes the worker once per successful open.
    #[tokio::test(start_paused = true)]
    async fn keep_listening_reopens_with_backoff_and_wakes_on_each_open() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(400);
        let opens = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Open #1 succeeds, but its connection dies at once. Opens #2-#4 fail.
        // Open #5 succeeds and stays up.
        let counter = Arc::clone(&opens);
        let open = move |_tx: tokio::sync::mpsc::UnboundedSender<()>| {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                match n {
                    1 => Ok(fake_session(false)),
                    2..=4 => Err(format!("refused #{n}")),
                    _ => Ok(fake_session(true)),
                }
            }
        };
        let supervisor = tokio::spawn(keep_listening(open, tx, base, max));

        // Opens land at t = 0 (ok, closes), 100 (fail), 200 (fail),
        // 400 (fail), 800 (ok, stays up): sleeps of base after the closed
        // session, then 100, 200, 400 = capped doubling after each failure.
        let expected = [
            (50, 1, 1),
            (150, 2, 0),
            (350, 3, 0),
            (750, 4, 0),
            (850, 5, 1),
        ];
        let started = tokio::time::Instant::now();
        for (at_ms, want_opens, want_wakes) in expected {
            tokio::time::sleep_until(started + Duration::from_millis(at_ms)).await;
            assert_eq!(
                opens.load(Ordering::SeqCst),
                want_opens,
                "opens by {at_ms}ms"
            );
            let mut wakes = 0;
            while rx.try_recv().is_ok() {
                wakes += 1;
            }
            assert_eq!(
                wakes, want_wakes,
                "wakes delivered in the window ending {at_ms}ms"
            );
        }

        // The healthy session holds: no further opens, however long we wait.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert_eq!(opens.load(Ordering::SeqCst), 5);

        // Dropping the receiver lets the supervisor stop at its next check.
        drop(rx);
        supervisor.abort();
    }

    /// A stand-in listening session: resolves at once (the connection
    /// closed) unless `stays_up`.
    fn fake_session(
        stays_up: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        if stays_up {
            Box::pin(std::future::pending())
        } else {
            Box::pin(std::future::ready(()))
        }
    }

    /// The issue's repro against a real backend: terminate the listener's
    /// `LISTEN` connection and it reopens, wakes the worker once, and keeps
    /// delivering notifications on the new connection. Each step waits on
    /// the wake channel itself; the timeout is only a hang guard.
    #[tokio::test]
    async fn a_terminated_listen_backend_is_reopened() {
        use crate::config::DEFAULT_SCHEMA;

        const CHANNEL: &str = "wake_313";
        let hang_guard = Duration::from_secs(30);
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut wake = WakeListener::spawn(
            db.dsn().to_string(),
            DEFAULT_SCHEMA.to_string(),
            CHANNEL.to_string(),
        );
        let mut next_wake = async || {
            tokio::time::timeout(hang_guard, wake.rx.recv())
                .await
                .expect("a wake before the hang guard")
                .expect("the wake channel stays open")
        };

        next_wake().await; // the first open's catch-up wake

        let admin = connect_plain(db.dsn(), DEFAULT_SCHEMA)
            .await
            .expect("connect");
        let terminated: i64 = admin
            .query_one(
                "select count(*) from ( \
                   select pg_terminate_backend(pid) from pg_stat_activity \
                   where datname = current_database() \
                     and pid <> pg_backend_pid() \
                     and query like '%listen%' || $1 || '%' \
                 ) t",
                &[&CHANNEL],
            )
            .await
            .expect("terminate the listener's backend")
            .get(0);
        assert_eq!(terminated, 1, "exactly one LISTEN backend to terminate");

        next_wake().await; // the reopen's catch-up wake

        admin
            .batch_execute(&format!("notify {CHANNEL}"))
            .await
            .expect("notify");
        next_wake().await;
    }
}

#[cfg(test)]
pub(crate) mod log_capture {
    //! Captures `tracing` events on the test thread, for the tests that
    //! assert what a failure logs (issues #325, #408).

    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Mutex};

    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[derive(Debug, Clone)]
    pub(crate) struct CapturedEvent {
        pub(crate) level: tracing::Level,
        pub(crate) fields: HashMap<String, String>,
    }

    #[derive(Clone, Default)]
    pub(crate) struct Captured(pub(crate) Arc<Mutex<Vec<CapturedEvent>>>);

    struct FieldVisitor(HashMap<String, String>);

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    struct CaptureLayer(Captured);

    impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor(HashMap::new());
            event.record(&mut visitor);
            self.0.0.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                fields: visitor.0,
            });
        }
    }

    /// Installs a capture as this thread's default subscriber.
    ///
    /// `tracing` caches each callsite's interest globally, and while only
    /// one scoped dispatcher is live it takes that interest from the default
    /// of whichever thread registers the callsite. A test that logs with no
    /// capture (as `consecutive_failures_tracks_the_streak_and_clears_while_healthy`
    /// does) could then cache a shared callsite as "never" while another
    /// test's capture was the only one live, silently dropping that test's
    /// events. A permanently live no-op dispatcher keeps two registered, so
    /// interest is always computed over every live dispatcher.
    pub(crate) fn install_capture() -> (tracing::subscriber::DefaultGuard, Captured) {
        static KEEP_INTEREST_GLOBAL: LazyLock<tracing::Dispatch> =
            LazyLock::new(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        LazyLock::force(&KEEP_INTEREST_GLOBAL);
        let captured = Captured::default();
        let subscriber = tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
        (tracing::subscriber::set_default(subscriber), captured)
    }

    /// A thread with no subscriber is first to hit a callsite while this
    /// test's capture is live.
    /// The capture must still see this thread's event through that callsite.
    #[test]
    fn a_callsite_first_hit_on_an_uncaptured_thread_still_reaches_the_capture() {
        fn log() {
            tracing::error!("callsite shared with an uncaptured thread");
        }

        let (_guard, captured) = install_capture();
        std::thread::spawn(log).join().expect("uncaptured thread");
        log();

        let events = captured.0.lock().unwrap().len();
        assert_eq!(events, 1, "the capture missed its own thread's event");
    }
}

#[cfg(test)]
mod maintenance_failure_tests {
    //! Issue #408: [`maintenance_loop`] used to drop every step's error.

    use super::log_capture::{CapturedEvent, install_capture};
    use super::*;

    /// Issue #476: a pass that ran out a whole catch-up timeout (a fence a
    /// long transaction holds) sends the next one back to the regular
    /// interval, so fresh markers can't start waiting passes back to back
    /// while the loop seals nothing.
    #[test]
    fn a_pass_that_ran_out_its_timeout_suspends_early_passes() {
        let timeout = Duration::from_secs(5);
        assert!(early_pass_allowed(Duration::from_millis(40), timeout));
        assert!(!early_pass_allowed(timeout, timeout));
        assert!(!early_pass_allowed(Duration::from_secs(6), timeout));
    }

    fn step_of(event: &CapturedEvent) -> Option<&str> {
        event.fields.get("step").map(|s| s.trim_matches('"'))
    }

    fn levels(events: &[CapturedEvent]) -> Vec<(tracing::Level, String)> {
        events
            .iter()
            .map(|e| (e.level, step_of(e).unwrap_or("-").to_string()))
            .collect()
    }

    /// A step's first failure warns with its error. While it keeps failing,
    /// only once per interval warns and the rest log at `debug`; another
    /// step's first failure still warns; the step's next success logs one
    /// recovery, after which a new failure warns straight away.
    #[test]
    fn step_failures_warn_once_per_interval_and_report_recovery() {
        let (_guard, captured) = install_capture();
        let mut failures = StepFailures::default();
        let start = Instant::now();

        failures.failed("seal", &"boom", start);
        failures.failed("seal", &"boom", start + Duration::from_secs(1));
        failures.failed("reclaim_stale", &"bang", start + Duration::from_secs(1));
        failures.failed("seal", &"boom", start + STEP_FAILURE_WARN_INTERVAL);
        failures.succeeded("seal");
        // A step that never failed has nothing to recover from.
        failures.succeeded("retire_drained_segments");
        failures.failed("seal", &"boom", start + STEP_FAILURE_WARN_INTERVAL);

        let events = captured.0.lock().unwrap().clone();
        assert_eq!(
            levels(&events),
            vec![
                (tracing::Level::WARN, "seal".to_string()),
                (tracing::Level::DEBUG, "seal".to_string()),
                (tracing::Level::WARN, "reclaim_stale".to_string()),
                (tracing::Level::WARN, "seal".to_string()),
                (tracing::Level::INFO, "seal".to_string()),
                (tracing::Level::WARN, "seal".to_string()),
            ],
            "{events:?}"
        );
        assert!(events[0].fields["error"].contains("boom"), "{events:?}");
        assert_eq!(events[3].fields["failures"], "3", "{events:?}");
        assert_eq!(events[4].fields["failures"], "3", "{events:?}");
        assert_eq!(events[5].fields["failures"], "1", "{events:?}");
    }

    /// Review of #408: the reconcile runs only every `reconcile_interval`,
    /// so the ticks between two failing passes don't run it at all. Those
    /// ticks mustn't count as its recovery, or a reconcile that fails every
    /// pass would log a recovery and a fresh `warn` each pass (every 5s by
    /// default) instead of one `warn` a minute.
    #[test]
    fn a_step_that_did_not_run_has_not_recovered() {
        let (_guard, captured) = install_capture();
        let mut failures = StepFailures::default();
        let start = Instant::now();

        failures.failed("reconcile_source_tables", &"boom", start);
        // The ticks in between run every other step cleanly.
        for _ in 0..3 {
            let _ = failures.check("seal", Ok::<(), &str>(()));
        }
        failures.failed(
            "reconcile_source_tables",
            &"boom",
            start + Duration::from_secs(5),
        );

        let events = captured.0.lock().unwrap().clone();
        assert_eq!(
            levels(&events),
            vec![
                (tracing::Level::WARN, "reconcile_source_tables".to_string()),
                (tracing::Level::DEBUG, "reconcile_source_tables".to_string()),
            ],
            "{events:?}"
        );
    }

    /// The loop itself: pointed at a schema with no Trellis tables, the
    /// first step (seal) fails on its first tick, and that failure must be
    /// logged rather than silently dropped.
    #[tokio::test]
    async fn a_failing_step_is_logged_by_the_loop() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_guard, captured) = install_capture();
        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid config"),
        )
        .expect("build a same-crate pool");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = MaintenanceConfig {
            dsn: db.dsn().to_string(),
            schema: "no_trellis_here".to_string(),
            pool,
            session: ProducerSession::connect(db.dsn(), "no_trellis_here")
                .await
                .expect("take the staging-worker singleton"),
            wake_channel: "wake".to_string(),
            interval: Duration::from_millis(20),
            reclaim_ttl: Duration::from_secs(30),
            drainer_window: staging::DEFAULT_DRAINER_WINDOW,
            reconcile_interval: Duration::from_secs(3600),
            watermark: staging::StagedWatermark::new(),
            backfill_catch_up_timeout: Duration::from_secs(1),
        };
        let seal_warning = || {
            captured.0.lock().unwrap().iter().any(|e| {
                e.level == tracing::Level::WARN
                    && step_of(e) == Some("seal")
                    && e.fields["error"].contains("does not exist")
            })
        };
        let watcher = async {
            // An event, not a convergence budget: the bound only turns a
            // hang into a failure.
            let deadline = Instant::now() + Duration::from_secs(30);
            while !seal_warning() {
                assert!(
                    Instant::now() < deadline,
                    "the seal failure was never logged"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            shutdown_tx.send(true).expect("signal shutdown");
        };
        tokio::join!(maintenance_loop(config, shutdown_rx), watcher);
    }
}

#[cfg(test)]
mod drain_failure_tests {
    //! Issue #660: [`app_worker_loop`] used to release and retry a failed
    //! drain without logging it.

    use super::log_capture::{CapturedEvent, install_capture};
    use super::*;

    fn field<'a>(event: &'a CapturedEvent, name: &str) -> &'a str {
        event
            .fields
            .get(name)
            .map(|v| v.trim_matches('"'))
            .unwrap_or_else(|| panic!("no `{name}` field on {event:?}"))
    }

    fn summary(events: &[CapturedEvent]) -> Vec<(tracing::Level, String, String)> {
        events
            .iter()
            .map(|e| {
                (
                    e.level,
                    field(e, "error").to_string(),
                    field(e, "collapsed").to_string(),
                )
            })
            .collect()
    }

    fn claim_lost() -> ApplyError {
        ApplyError::ClaimLost
    }

    fn hop_bound() -> ApplyError {
        ApplyError::HopBoundExceeded {
            hop_gen: 9,
            tables: vec!["public.a".to_string()],
        }
    }

    /// The first failure warns once, carrying the error, its quarantine
    /// class, the segments, the released buckets and the claimant.
    #[test]
    fn a_failed_drain_warns_once_with_its_context() {
        let (_guard, captured) = install_capture();
        let mut failures = DrainFailures::default();

        failures.failed(
            &claim_lost(),
            &[5, 6],
            &Ok(vec![(5, 0), (5, 3), (6, 1)]),
            "worker-a",
            Instant::now(),
        );

        let events = captured.0.lock().unwrap().clone();
        assert_eq!(events.len(), 1, "{events:?}");
        let event = &events[0];
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(
            field(event, "message"),
            "drain failed; released its claims to retry"
        );
        assert_eq!(field(event, "error"), claim_lost().to_string());
        assert_eq!(
            field(event, "class"),
            format!("{:?}", staging::quarantine::classify(&claim_lost()))
        );
        assert_eq!(field(event, "class"), "Isolate");
        assert_eq!(field(event, "segments"), "[5, 6]");
        assert_eq!(field(event, "buckets"), "5:[0,3] 6:[1]");
        assert_eq!(field(event, "claimant"), "worker-a");
        assert_eq!(field(event, "collapsed"), "0");
    }

    /// Repeats of one error inside the interval log at `debug`; the next
    /// `warn` for it, an interval on, reports how many it collapsed. A
    /// different error still warns straight away, with its own class.
    #[test]
    fn repeats_of_the_same_error_collapse_to_one_warn_per_interval() {
        let (_guard, captured) = install_capture();
        let mut failures = DrainFailures::default();
        let start = Instant::now();
        let lost = claim_lost().to_string();
        let hop = hop_bound().to_string();

        for tick in 0..4 {
            let now = start + Duration::from_millis(200 * tick);
            failures.failed(&claim_lost(), &[5], &Ok(vec![(5, 0)]), "worker-a", now);
        }
        failures.failed(
            &hop_bound(),
            &[5],
            &Ok(vec![(5, 0)]),
            "worker-a",
            start + Duration::from_secs(1),
        );
        failures.failed(
            &claim_lost(),
            &[5],
            &Ok(vec![(5, 0)]),
            "worker-a",
            start + DRAIN_FAILURE_WARN_INTERVAL,
        );

        let events = captured.0.lock().unwrap().clone();
        assert_eq!(
            summary(&events),
            vec![
                (tracing::Level::WARN, lost.clone(), "0".to_string()),
                (tracing::Level::DEBUG, lost.clone(), "1".to_string()),
                (tracing::Level::DEBUG, lost.clone(), "2".to_string()),
                (tracing::Level::DEBUG, lost.clone(), "3".to_string()),
                (tracing::Level::WARN, hop, "0".to_string()),
                (tracing::Level::WARN, lost, "3".to_string()),
            ],
            "{events:?}"
        );
        assert_eq!(field(&events[4], "class"), "Halting");
    }

    /// An error that hasn't recurred for a whole interval is forgotten, so
    /// the map stays bounded; its next occurrence warns as a new one.
    #[test]
    fn an_error_that_stopped_recurring_is_forgotten() {
        let (_guard, captured) = install_capture();
        let mut failures = DrainFailures::default();
        let start = Instant::now();

        failures.failed(&claim_lost(), &[5], &Ok(vec![]), "worker-a", start);
        failures.failed(
            &claim_lost(),
            &[5],
            &Ok(vec![]),
            "worker-a",
            start + Duration::from_secs(1),
        );
        let later = start + Duration::from_secs(1) + DRAIN_FAILURE_WARN_INTERVAL;
        failures.failed(&hop_bound(), &[7], &Ok(vec![]), "worker-a", later);
        assert_eq!(failures.recent.len(), 1, "the stale entry was pruned");
        failures.failed(&claim_lost(), &[5], &Ok(vec![]), "worker-a", later);

        let events = captured.0.lock().unwrap().clone();
        let levels: Vec<_> = summary(&events)
            .into_iter()
            .map(|(level, _, collapsed)| (level, collapsed))
            .collect();
        assert_eq!(
            levels,
            vec![
                (tracing::Level::WARN, "0".to_string()),
                (tracing::Level::DEBUG, "1".to_string()),
                (tracing::Level::WARN, "0".to_string()),
                (tracing::Level::WARN, "0".to_string()),
            ],
            "{events:?}"
        );
        assert_eq!(field(&events[0], "buckets"), "none");
    }

    /// A Postgres error's `DETAIL` can change on every occurrence (a
    /// deadlock names the pids involved). Repeats that differ only there
    /// still collapse, and each log line keeps the full text.
    #[test]
    fn repeats_differing_only_in_detail_still_collapse() {
        let (_guard, captured) = install_capture();
        let mut failures = DrainFailures::default();
        let start = Instant::now();
        let deadlock = |pids: &str| {
            ApplyError::Pool(crate::error::Error::Config(format!(
                "ERROR: deadlock detected\nDETAIL: Process {pids}."
            )))
        };

        let first = deadlock("101 waits for 202");
        let second = deadlock("303 waits for 404");
        failures.failed(&first, &[5], &Ok(vec![(5, 0)]), "worker-a", start);
        failures.failed(
            &second,
            &[5],
            &Ok(vec![(5, 0)]),
            "worker-a",
            start + Duration::from_millis(200),
        );

        let events = captured.0.lock().unwrap().clone();
        assert_eq!(
            summary(&events),
            vec![
                (tracing::Level::WARN, first.to_string(), "0".to_string()),
                (tracing::Level::DEBUG, second.to_string(), "1".to_string()),
            ],
            "{events:?}"
        );
        assert_eq!(failures.recent.len(), 1);
    }

    /// When the release itself fails the claims are still held (until the
    /// reclaim sweep), so the log must not report `none`.
    #[test]
    fn a_failed_release_is_reported_not_shown_as_no_buckets() {
        let (_guard, captured) = install_capture();
        let mut failures = DrainFailures::default();

        failures.failed(
            &claim_lost(),
            &[5],
            &Err("pool timed out".to_string()),
            "worker-a",
            Instant::now(),
        );

        let events = captured.0.lock().unwrap().clone();
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(
            field(&events[0], "buckets"),
            "unknown; release failed: pool timed out"
        );
    }

    #[test]
    fn released_buckets_group_by_segment() {
        assert_eq!(released_buckets(&[]), "none");
        assert_eq!(released_buckets(&[(3, 7)]), "3:[7]");
        assert_eq!(
            released_buckets(&[(3, 0), (3, 1), (4, 2), (9, 0), (9, 5)]),
            "3:[0,1] 4:[2] 9:[0,5]"
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn thread_panicked_is_internal() {
        assert_eq!(ClientError::ThreadPanicked.code(), ErrorCode::Internal);
    }

    /// [`ClientError::Config`] must delegate to [`crate::error::Error::code`]
    /// rather than hardcoding a category — this is the same nested-error a
    /// wrapped [`crate::error::Error::IncompatibleInstance`] should still
    /// report as [`ErrorCode::Conflict`] through the wrapper, not whatever a
    /// blanket "Config variant" category would be.
    #[test]
    fn config_delegates_to_the_wrapped_engine_error() {
        let inner = crate::error::Error::IncompatibleInstance("mismatched marker".to_string());
        let expected = inner.code();
        let wrapped = ClientError::Config(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Conflict);
    }
}

#[cfg(test)]
mod option_validation_tests {
    use super::*;

    fn options_with(heartbeat_interval: Duration, reclaim_ttl: Duration) -> ClientOptions {
        ClientOptions {
            application_threads: 1,
            reclaim_ttl,
            heartbeat: HeartbeatDaemonConfig {
                interval: heartbeat_interval,
                ..HeartbeatDaemonConfig::default()
            },
            ..ClientOptions::default()
        }
    }

    #[test]
    fn the_default_options_pass_validation() {
        assert!(validate_options(&ClientOptions::default()).is_ok());
    }

    /// The configuration behind the `client_e2e` flake this check exists
    /// for: a 200ms `reclaim_ttl` with the default 5s heartbeat. Chunk
    /// claims then went stale 200ms into every chunk run, and a chunk that
    /// took longer than that was reclaimed and re-run by the peer worker
    /// indefinitely.
    #[test]
    fn a_reclaim_ttl_shorter_than_the_heartbeat_interval_is_rejected() {
        let err = validate_options(&options_with(
            Duration::from_secs(5),
            Duration::from_millis(200),
        ))
        .expect_err("a TTL under the heartbeat interval must be rejected");
        assert!(matches!(
            err,
            ClientError::HeartbeatNotUnderReclaimTtl { .. }
        ));
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    #[test]
    fn the_heartbeat_interval_must_be_at_most_half_the_reclaim_ttl() {
        assert!(
            validate_options(&options_with(
                Duration::from_millis(100),
                Duration::from_millis(200)
            ))
            .is_ok(),
            "exactly half is allowed"
        );
        assert!(
            validate_options(&options_with(
                Duration::from_millis(101),
                Duration::from_millis(200)
            ))
            .is_err(),
            "just over half is rejected"
        );
    }
}

#[cfg(test)]
mod backfill_chunk_claim_tests {
    use super::*;
    use std::collections::HashMap;

    use crate::defs::ast::ValueType;

    /// A backfill chunk is heartbeated only while it runs, so a worker must
    /// never hold a claim on a chunk it isn't running yet. The old code
    /// claimed up to four at once and ran them in turn, and the queued ones
    /// went stale and were reclaimed by a peer while the first was still
    /// running (see [`drain_backfill_chunks`]'s doc comment).
    ///
    /// Checked deterministically with an advisory-lock gate rather than a
    /// timing budget: a statement trigger on the target blocks the first
    /// chunk's write on a lock this test holds. While the write is parked
    /// there, exactly one chunk may be claimed.
    #[tokio::test]
    async fn a_worker_claims_only_the_chunk_it_is_running() {
        const GATE: i64 = 0x7472_6c73;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        // Same-crate pool; see apply.rs's metrics test for why testkit's own
        // `db.pool` is a different type here.
        let pool_config =
            crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = Pool::new(&pool_config).expect("build a same-crate pool");
        let (raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("set search_path");

        // One row over the 50k-row chunk size: the smallest table that
        // plans two chunks.
        raw.batch_execute(
            "create table public.gated (id bigint primary key, price numeric); \
             insert into public.gated select g, g from generate_series(1, 50001) g",
        )
        .await
        .expect("seed source");
        // A seam-fed source keeps the old chunked build (#625 F8a).
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.gated")
            .await
            .expect("make gated another definition's target");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("price".to_string(), ValueType::Numeric),
        ]);
        let def = defs::install_definition(
            &pool,
            "TRANSFORM gated_calc FROM gated SELECT price + price AS double_price",
            &columns,
            "public",
        )
        .await
        .expect("install_definition");
        // Registration only records it; the discharge plans the chunks.
        crate::intake::markers::discharge_registrations(&pool)
            .await
            .expect("dispatch the build");
        let chunk_count: i64 = raw
            .query_one(
                "select count(*) from backfill_chunks where definition_id = $1",
                &[&def.id],
            )
            .await
            .expect("count chunks")
            .get(0);
        assert_eq!(chunk_count, 2, "the gate test needs two planned chunks");

        raw.batch_execute(&format!(
            "create function public.gated_calc_gate() returns trigger language plpgsql as $$ \
             begin perform pg_advisory_xact_lock({GATE}); return null; end $$; \
             create trigger gated_calc_gate before insert on public.gated_calc \
             for each statement execute function public.gated_calc_gate(); \
             select pg_advisory_lock({GATE});"
        ))
        .await
        .expect("install the write gate and close it");

        let worker_pool = pool.clone();
        let worker = tokio::spawn(async move {
            drain_backfill_chunks(
                &worker_pool,
                "gated-worker",
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
        });

        // Wait for the chunk write to park on the gate: an event, not a
        // convergence budget. The bound only turns a hang into a failure.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let parked: bool = raw
                .query_one(
                    "select exists(select 1 from pg_locks \
                     where locktype = 'advisory' and not granted)",
                    &[],
                )
                .await
                .expect("read pg_locks")
                .get(0);
            if parked {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the chunk write never reached the gate"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let claimed: Vec<Option<String>> = raw
            .query(
                "select claimed_by from backfill_chunks where definition_id = $1 order by id",
                &[&def.id],
            )
            .await
            .expect("read claims")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            claimed,
            vec![Some("gated-worker".to_string()), None],
            "while one chunk is running, the other must stay unclaimed for a peer to take"
        );

        raw.batch_execute(&format!("select pg_advisory_unlock({GATE})"))
            .await
            .expect("open the gate");
        assert!(
            worker.await.expect("worker task"),
            "the call claimed and ran a chunk"
        );

        let states: Vec<(bool, Option<String>)> = raw
            .query(
                "select done, claimed_by from backfill_chunks where definition_id = $1 order by id",
                &[&def.id],
            )
            .await
            .expect("read chunk states")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(
            states,
            vec![(true, None), (false, None)],
            "one call finishes exactly the chunk it claimed"
        );

        assert!(
            drain_backfill_chunks(
                &pool,
                "gated-worker",
                Duration::from_secs(5),
                Duration::from_secs(60)
            )
            .await
        );
        assert!(
            !drain_backfill_chunks(
                &pool,
                "gated-worker",
                Duration::from_secs(5),
                Duration::from_secs(60)
            )
            .await,
            "nothing is left to claim"
        );
        let status: String = raw
            .query_one(
                "select status from transform_definitions where id = $1",
                &[&def.id],
            )
            .await
            .expect("read status")
            .get(0);
        assert_eq!(status, "catching_up", "the build finished");
    }
}

#[cfg(test)]
mod backfill_chunk_failure_tests {
    //! #616: a backfill chunk that fails says so, and stops retrying blindly.
    //! Each test drives the chunks itself, one [`drain_backfill_chunks`] call
    //! at a time, and makes a backed-off chunk due by hand rather than
    //! waiting for it.

    use super::log_capture::install_capture;
    use super::*;
    use std::collections::HashMap;
    use std::time::SystemTime;

    use crate::app::{Trellis, TrellisOptions};
    use crate::defs::ast::ValueType;
    use crate::defs::model::TransformStatus;
    use crate::integer::IntWidth;

    const WORKER: &str = "issue-616-worker";

    struct Fixture {
        _cluster: testkit::TestCluster,
        _db: testkit::TestDatabase,
        pool: Pool,
        raw: tokio_postgres::Client,
        trellis: Trellis,
        id: i64,
    }

    /// Seeds `public.nums` with `rows` rows (`x = id`, except `x = bad_x` at
    /// `bad_id`) and registers `TRANSFORM doubles FROM nums SELECT x + x`
    /// over it, its build dispatched onto the chunk queue.
    async fn seed(rows: i64, bad: Option<(i64, i32)>) -> Fixture {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let trellis = Trellis::connect(config, TrellisOptions::default())
            .await
            .expect("connect");
        let (raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public; \
             create table public.nums (id bigint primary key, x integer); \
             insert into public.nums select g, g from generate_series(1, {rows}) g",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("seed source");
        if let Some((bad_id, bad_x)) = bad {
            raw.execute(
                "update public.nums set x = $2 where id = $1",
                &[&bad_id, &bad_x],
            )
            .await
            .expect("seed the bad row");
        }
        // The old chunked build these tests exercise is a seam-fed
        // source's since #625 F8a (over a captured table, a plain 1-1 is
        // the Re-derive build's, whose failures `staging::build`'s own
        // tests cover).
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.nums")
            .await
            .expect("make nums another definition's target");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        let def = defs::install_definition(
            &pool,
            "TRANSFORM doubles FROM nums SELECT x + x AS doubled",
            &columns,
            "public",
        )
        .await
        .expect("install_definition");
        crate::intake::markers::discharge_registrations(&pool)
            .await
            .expect("dispatch the build");
        Fixture {
            _cluster: cluster,
            _db: db,
            pool,
            raw,
            trellis,
            id: def.id,
        }
    }

    impl Fixture {
        async fn drain_one(&self) -> bool {
            drain_backfill_chunks(
                &self.pool,
                WORKER,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
        }

        /// Runs chunks until none is claimable, failing the test if that
        /// takes more than `bound` calls.
        async fn drain_all(&self, bound: usize) -> usize {
            for calls in 0..bound {
                if !self.drain_one().await {
                    return calls;
                }
            }
            panic!("the chunk queue still had claimable work after {bound} runs");
        }

        /// Makes every backed-off chunk due now.
        async fn make_due(&self) {
            self.raw
                .execute(
                    "update backfill_chunks set next_attempt_at = now() where definition_id = $1",
                    &[&self.id],
                )
                .await
                .expect("make the chunks due");
        }

        async fn status(&self) -> crate::app::DefinitionStatus {
            self.trellis
                .status("doubles")
                .await
                .expect("status")
                .expect("the definition exists")
        }

        async fn chunk(&self) -> (i32, i32, bool, Option<String>) {
            let row = self
                .raw
                .query_one(
                    "select attempts, charged, next_attempt_at > now(), claimed_by \
                     from backfill_chunks where definition_id = $1 and not done",
                    &[&self.id],
                )
                .await
                .expect("one undone chunk");
            (row.get(0), row.get(1), row.get(2), row.get(3))
        }
    }

    /// #616's repro: a plain 1-1 build over a source larger than one chunk,
    /// whose `x + x` overflows on one row. It used to retry that chunk
    /// forever, silently, with the definition stuck in `backfilling`. Now
    /// the failure is on the definition's status and in a warn line while
    /// the chunk narrows itself to the key, the key is quarantined, and the
    /// build finishes without it.
    #[tokio::test]
    async fn a_chunk_that_fails_on_its_data_quarantines_the_key_and_the_build_finishes() {
        let f = seed(50_001, Some((30_000, i32::MAX))).await;
        let (_guard, captured) = install_capture();

        assert!(f.drain_one().await, "the first chunk ran");
        let status = f.status().await;
        assert_eq!(status.status, TransformStatus::Backfilling);
        let failure = status
            .backfill_failure
            .expect("the failing chunk is on the status");
        assert!(
            failure.last_error.contains("out of range"),
            "{}",
            failure.last_error
        );
        assert_eq!(failure.source_table, "public.nums");
        assert_eq!(failure.attempts, 1);

        let runs = f.drain_all(200).await;
        assert!(runs > 0);

        let status = f.status().await;
        assert_eq!(
            status.status,
            TransformStatus::CatchingUp,
            "the build finished without the key"
        );
        assert_eq!(status.backfill_failure, None, "no chunk is left failing");
        crate::intake::markers::discharge_registrations(&f.pool)
            .await
            .expect("discharge the go-live catch-up");
        assert_eq!(f.status().await.status, TransformStatus::Live);
        let target = f
            .raw
            .query_one(
                "select count(*), count(*) filter (where id = 30000), \
                        count(*) filter (where doubled = 2 * id) \
                 from public.doubles",
                &[],
            )
            .await
            .expect("read the target");
        assert_eq!(target.get::<_, i64>(0), 50_000);
        assert_eq!(target.get::<_, i64>(1), 0, "the failing key is left out");
        assert_eq!(target.get::<_, i64>(2), 50_000);

        let quarantined = f
            .trellis
            .sample_quarantined("doubles", None, 10)
            .await
            .expect("sample the quarantined keys");
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].key, "30000");
        assert!(quarantined[0].error_message.contains("out of range"));
        let parked: Vec<(i64, Option<String>)> = f
            .raw
            .query(
                "select seg_seq, old_image::text from poison_held where key = '30000'",
                &[],
            )
            .await
            .expect("read the parked re-derive")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(
            parked,
            vec![(crate::staging::quarantine::BUILD_PARK_SEG_SEQ, None)],
            "a release re-derives the key"
        );

        let events = captured.0.lock().unwrap().clone();
        let warned = |message: &str| {
            events.iter().any(|event| {
                event.level == tracing::Level::WARN
                    && event
                        .fields
                        .get("message")
                        .is_some_and(|m| m.contains(message))
                    && event.fields.get("definition_id") == Some(&f.id.to_string())
            })
        };
        assert!(warned("split it in two"), "{events:#?}");
        assert!(warned("quarantined the key"), "{events:#?}");
    }

    /// #616: a transient failure (here a serialization failure a trigger on
    /// the target raises) is retried after a backoff and charged nothing.
    #[tokio::test]
    async fn a_transient_chunk_failure_is_retried_after_a_backoff_without_a_charge() {
        let f = seed(3, None).await;
        f.raw
            .batch_execute(
                "create function public.doubles_conflict() returns trigger language plpgsql as $$ \
                 begin raise exception 'conflict' using errcode = 'serialization_failure'; end $$; \
                 create trigger doubles_conflict before insert on public.doubles \
                 for each statement execute function public.doubles_conflict()",
            )
            .await
            .expect("make the write conflict");

        assert!(f.drain_one().await);
        assert_eq!(
            f.chunk().await,
            (1, 0, true, None),
            "one failure, uncharged, backed off, unclaimed"
        );
        assert!(!f.drain_one().await, "the chunk isn't due yet");
        let status = f.status().await;
        assert_eq!(status.status, TransformStatus::Backfilling);
        let failure = status.backfill_failure.expect("the failure is reported");
        assert!(failure.last_error.contains("conflict"));
        assert!(failure.next_attempt_at > SystemTime::now());

        f.raw
            .batch_execute("drop trigger doubles_conflict on public.doubles")
            .await
            .expect("end the conflict");
        f.make_due().await;
        assert_eq!(f.drain_all(5).await, 1);
        let status = f.status().await;
        assert_eq!(status.status, TransformStatus::CatchingUp);
        assert_eq!(status.backfill_failure, None);
    }

    /// #616: a failure that says nothing about any row (here the target
    /// lost the column the build writes) is retried and charged, and its
    /// last charge pauses the definition with the error on its status. The
    /// resume clears it and rebuilds.
    #[tokio::test]
    async fn a_chunk_failure_that_cannot_be_narrowed_pauses_the_definition() {
        let f = seed(3, None).await;
        f.raw
            .batch_execute("alter table public.doubles drop column doubled")
            .await
            .expect("break the target");

        for charge in 1..chunk_queue::MAX_CHARGED_ATTEMPTS {
            f.make_due().await;
            assert!(f.drain_one().await);
            assert_eq!(f.chunk().await, (charge, charge, true, None));
            assert_eq!(f.status().await.status, TransformStatus::Backfilling);
        }
        f.make_due().await;
        assert!(f.drain_one().await);

        let status = f.status().await;
        assert_eq!(status.status, TransformStatus::Paused);
        let failure = status
            .backfill_failure
            .expect("the pause carries the error");
        assert!(
            failure.last_error.contains("doubled"),
            "{}",
            failure.last_error
        );
        assert_eq!(
            failure.attempts,
            u32::try_from(chunk_queue::MAX_CHARGED_ATTEMPTS).unwrap()
        );
        f.make_due().await;
        assert!(
            !f.drain_one().await,
            "a paused definition's chunk isn't claimed"
        );

        crate::staging::quarantine::resume_transform(&f.pool, "doubles")
            .await
            .expect("resume");
        let status = f.status().await;
        assert_eq!(status.status, TransformStatus::WaitingToBackfill);
        assert_eq!(
            status.backfill_failure, None,
            "the resume clears the failure"
        );
    }

    /// #616: a key the build quarantined and the operator released while
    /// the definition is still `backfilling` reaches the target. The
    /// release's re-derive drains before the definition applies anything,
    /// so the drain skips it for this definition, and the chunk that held
    /// the key has already finished without it. The go-live catch-up's
    /// enumeration is what writes it.
    #[tokio::test]
    async fn a_key_released_while_its_build_runs_reaches_the_target() {
        // The failing key is the lowest, so each split keeps it in the
        // first chunk, which is also the next one claimed (by id).
        let mut f = seed(20, Some((1, i32::MAX))).await;
        let mut quarantined = false;
        for _ in 0..20 {
            let chunk = chunk_queue::claim_chunks(&f.raw, WORKER, 1)
                .await
                .expect("claim")
                .pop()
                .expect("a claimable chunk");
            let err = chunk_queue::run_claimed_chunk(
                &f.pool,
                &chunk,
                WORKER,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await;
            match err {
                Ok(()) => chunk_queue::finish_chunk(&f.pool, &chunk, WORKER)
                    .await
                    .expect("finish"),
                Err(err) => {
                    let outcome = chunk_queue::fail_chunk(&f.pool, &chunk, WORKER, &err)
                        .await
                        .expect("fail the chunk");
                    if matches!(outcome, chunk_queue::ChunkFailure::Quarantined { .. }) {
                        quarantined = true;
                        break;
                    }
                }
            }
        }
        assert!(quarantined, "the build narrowed its failure to key 1");
        // The chunk that held the key runs again without it and finishes,
        // while the upper halves of its splits are still to run.
        assert!(f.drain_one().await);
        let held: i64 = f
            .raw
            .query_one(
                "select count(*) from backfill_chunks where definition_id = $1 and not done",
                &[&f.id],
            )
            .await
            .expect("count the chunks left")
            .get(0);
        assert!(held > 0, "the build isn't finished");
        assert_eq!(f.status().await.status, TransformStatus::Backfilling);

        f.raw
            .execute("update public.nums set x = 1 where id = 1", &[])
            .await
            .expect("fix the row");
        crate::staging::quarantine::release_key(&f.pool, "doubles", "public.nums", "1")
            .await
            .expect("release the key");
        drain_ring(&f.pool, &mut f.raw).await;
        assert_eq!(f.status().await.status, TransformStatus::Backfilling);

        f.drain_all(20).await;
        assert_eq!(f.status().await.status, TransformStatus::CatchingUp);
        let built: i64 = f
            .raw
            .query_one("select count(*) from public.doubles where id = 1", &[])
            .await
            .expect("read the target")
            .get(0);
        assert_eq!(built, 0, "neither the release nor the build wrote the key");
        crate::intake::markers::discharge_registrations(&f.pool)
            .await
            .expect("discharge the go-live catch-up");
        assert_eq!(f.status().await.status, TransformStatus::Live);
        drain_ring(&f.pool, &mut f.raw).await;

        let target = f
            .raw
            .query_one(
                "select count(*), count(*) filter (where id = 1 and doubled = 2) \
                 from public.doubles",
                &[],
            )
            .await
            .expect("read the target");
        assert_eq!(target.get::<_, i64>(0), 20);
        assert_eq!(target.get::<_, i64>(1), 1, "the released key is built");
    }

    /// Seals and drains the ring until nothing is pending: a bounded loop,
    /// not a convergence wait.
    async fn drain_ring(pool: &Pool, client: &mut tokio_postgres::Client) {
        let watermark = staging::StagedWatermark::saturated();
        for _ in 0..16 {
            let outcome = staging::seal::seal_phase1(client)
                .await
                .expect("seal phase 1");
            staging::seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
                .await
                .expect("seal phase 2");
            while staging::apply::drain_once(
                pool,
                outcome.sealed_seg_seq,
                WORKER,
                1,
                "wake",
                &watermark,
            )
            .await
            .expect("drain_once")
            .is_some()
            {}
            staging::retire_drained_segments(client)
                .await
                .expect("retire drained segments");
            if !staging::has_pending(client).await.expect("has_pending") {
                return;
            }
        }
        panic!("the ring did not drain within 16 seal/drain rounds");
    }
}

#[cfg(test)]
mod backfill_shutdown_tests {
    use super::*;
    use crate::config::DEFAULT_SCHEMA;
    use crate::defs::ast::ValueType;
    use crate::defs::model::TransformStatus;

    async fn status_of(client: &tokio_postgres::Client) -> TransformStatus {
        let text: String = client
            .query_one(
                "select status from transform_definitions where target_table = 'public.t'",
                &[],
            )
            .await
            .expect("query status")
            .get(0);
        TransformStatus::from_persisted(&text).expect("known status")
    }

    /// Issue #312 review, restated for trigger capture (issue #622 C5):
    /// shutting down while a discharge waits must leave its definition
    /// `waiting_to_backfill` with its marker intact, since only
    /// `waiting_to_backfill` definitions are ever dispatched again. The wait
    /// is now the fresh fence's (issue #431), pinned by a transaction left
    /// open; the catch-up timeout is far longer than the test waits for
    /// shutdown, so only the shutdown signal can end it.
    #[tokio::test]
    async fn shutdown_during_a_backfill_wait_returns_the_definition_to_waiting() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect_plain(db.dsn(), DEFAULT_SCHEMA)
            .await
            .expect("connect");
        raw.batch_execute(
            "create table public.s (id bigint primary key, a numeric, r bigint); \
             insert into public.s (id, a, r) select g, g, 1 from generate_series(1, 5) g; \
             create table public.r (id bigint primary key, w numeric); \
             insert into public.r values (1, 10);",
        )
        .await
        .expect("seed source table");

        // `testkit`'s pool is the published crate's `Pool`, a different type
        // from this `--lib` build's own, so build one from the same DSN.
        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid config"),
        )
        .expect("build a same-crate pool");
        let columns = std::collections::HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("a".to_string(), ValueType::Numeric),
            ("r".to_string(), ValueType::Numeric),
        ]);
        // A relationship-enriched 1-1 still waits on the discharge's fence
        // (until milestone E); a plain 1-1 is the Re-derive build's, which
        // takes no fence (#625 F8a).
        crate::defs::create_relationship(&pool, "RELATIONSHIP rel FROM s.r TO r.id")
            .await
            .expect("create the to-one relationship");
        crate::defs::install_definition(
            &pool,
            "TRANSFORM t FROM s SELECT a + 1 AS f, rel.w AS w",
            &columns,
            "public",
        )
        .await
        .expect("register");
        assert_eq!(status_of(&raw).await, TransformStatus::WaitingToBackfill);

        // An open transaction pins the fence the first pass takes on the
        // join marker its capture install parks.
        let straggler = testkit::crash::OpenTransaction::begin(db.dsn()).await;
        straggler.execute("select txid_current()").await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = MaintenanceConfig {
            dsn: db.dsn().to_string(),
            schema: DEFAULT_SCHEMA.to_string(),
            pool,
            session: ProducerSession::connect(db.dsn(), DEFAULT_SCHEMA)
                .await
                .expect("take the staging-worker singleton"),
            wake_channel: "wake".to_string(),
            interval: Duration::from_millis(50),
            reclaim_ttl: Duration::from_secs(30),
            drainer_window: staging::DEFAULT_DRAINER_WINDOW,
            reconcile_interval: Duration::from_secs(3600),
            watermark: staging::StagedWatermark::saturated(),
            backfill_catch_up_timeout: Duration::from_secs(600),
        };
        let task = tokio::spawn(maintenance_loop(config, shutdown_rx));

        // The first pass installs capture, parks the join marker and fences
        // it, then waits on the fence: an event (the recorded fence), not a
        // convergence budget. The bound only turns a hang into a failure.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let fenced: bool = raw
                .query_one(
                    "select exists(select 1 from pending_backfill \
                     where table_name = 'public.s' and fence_xid is not null)",
                    &[],
                )
                .await
                .expect("read the marker")
                .get(0);
            if fenced {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the first maintenance pass never fenced the join marker"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(status_of(&raw).await, TransformStatus::WaitingToBackfill);

        shutdown_tx.send(true).expect("signal shutdown");
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("the maintenance loop must stop promptly on shutdown")
            .expect("maintenance task");
        straggler.commit().await;

        assert_eq!(
            status_of(&raw).await,
            TransformStatus::WaitingToBackfill,
            "a shutdown mid-wait leaves the definition for the next pass"
        );
        let markers: i64 = raw
            .query_one(
                "select count(*) from pending_backfill where table_name = 'public.s'",
                &[],
            )
            .await
            .expect("count markers")
            .get(0);
        assert_eq!(
            markers, 1,
            "the deferred marker must survive for the next start"
        );
    }
}

#[cfg(test)]
mod reconcile_tests {
    //! Issue #427, ADR-0016, restated for trigger capture (issue #622 C5):
    //! the staging worker's reconcile pass is the only thing that installs
    //! or uninstalls capture, and the catalog is its only source of truth for
    //! what to capture. These call [`reconcile_source_tables`] directly
    //! rather than waiting on a running client (#297).
    use super::*;
    use crate::capture::install::{Installed, installed};
    use crate::config::DEFAULT_SCHEMA;
    use crate::defs::ast::ValueType;

    struct Fixture {
        raw: tokio_postgres::Client,
        pool: Pool,
        _db: testkit::TestDatabase,
        _cluster: testkit::TestCluster,
    }

    /// `public.s`, with one registered definition per name in `readers`,
    /// captured by a first pass (as the worker's startup reconcile leaves
    /// it) when there is any.
    async fn fixture(readers: &[&str]) -> Fixture {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect_plain(db.dsn(), DEFAULT_SCHEMA)
            .await
            .expect("connect");
        raw.batch_execute(
            "create table public.s (id bigint primary key, a numeric); \
             insert into public.s (id, a) select g, g from generate_series(1, 5) g;",
        )
        .await
        .expect("seed a source table");
        // `testkit`'s pool is the published crate's `Pool`, a different type
        // from this `--lib` build's own, so build one from the same DSN.
        let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("valid config"))
            .expect("build a same-crate pool");
        for reader in readers {
            register(&pool, reader).await;
        }
        let mut f = Fixture {
            raw,
            pool,
            _db: db,
            _cluster: cluster,
        };
        if !readers.is_empty() {
            reconcile_pass(&mut f).await;
            assert!(is_captured(&f.raw).await, "the first pass captures `s`");
        }
        f
    }

    async fn register(pool: &Pool, reader: &str) {
        let columns = std::collections::HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("a".to_string(), ValueType::Numeric),
        ]);
        defs::install_definition(
            pool,
            &format!("TRANSFORM {reader} FROM s SELECT a + 1 AS f"),
            &columns,
            "public",
        )
        .await
        .expect("register a reader of `s`");
    }

    async fn drop_reader(pool: &Pool, target: &str) {
        defs::lifecycle::pause_transform(pool, target)
            .await
            .expect("pause");
        let outcome = defs::lifecycle::drop_transform(pool, target)
            .await
            .expect("drop");
        assert_eq!(outcome, defs::lifecycle::DropOutcome::Dropped);
    }

    /// One pass of the staging worker's reconcile, as the maintenance loop
    /// runs it. A freshly fenced marker settles at once here (nothing else
    /// runs in the database), so the timeout only bounds a surprise.
    async fn reconcile_pass(f: &mut Fixture) {
        reconcile_source_tables(
            &mut f.raw,
            &f.pool,
            DEFAULT_SCHEMA,
            "wake",
            &staging::StagedWatermark::saturated(),
            Duration::from_millis(200),
            &|| false,
        )
        .await
        .expect("reconcile pass");
    }

    async fn is_captured(raw: &tokio_postgres::Client) -> bool {
        match installed(raw, DEFAULT_SCHEMA, "public.s")
            .await
            .expect("read the capture")
        {
            Installed::Complete { current, .. } => current,
            Installed::Absent => false,
            partial => panic!("a pass leaves no partial install: {partial:?}"),
        }
    }

    /// A `setup_staging` whose first capture pass fails releases the
    /// staging-worker singleton before it returns (#687), so a restart right
    /// after doesn't fail with `ProducerAlreadyRunning`. Here the pass can't
    /// read the catalog: the schema has no Trellis tables.
    ///
    /// Dropping the session isn't enough: its connection task only closes the
    /// connection once the runtime polls it, and the server only lets the
    /// lock go once it sees that. So the second session is taken on another
    /// thread while this (current-thread) runtime is blocked, which keeps a
    /// merely dropped session holding the lock and makes the test
    /// deterministic.
    #[tokio::test]
    async fn a_failed_setup_releases_the_singleton_before_returning() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let schema = "no_trellis_here";
        let config = Config::with_schema(db.dsn(), schema).expect("valid config");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let err = setup_staging(db.dsn(), &config, &pool)
            .await
            .expect_err("the pass can't read a catalog that isn't there");
        assert!(err.to_string().contains("does not exist"), "{err}");

        let dsn = db.dsn().to_string();
        let retaken = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(async {
                    ProducerSession::connect(&dsn, schema)
                        .await
                        .map(|_| ())
                        .map_err(|err| err.to_string())
                })
        })
        .join()
        .expect("the retake thread");
        assert_eq!(retaken, Ok(()), "the failed setup still held the singleton");
    }

    async fn marker_count(raw: &tokio_postgres::Client) -> i64 {
        raw.query_one(
            "select count(*) from pending_backfill where table_name = 'public.s'",
            &[],
        )
        .await
        .expect("count markers")
        .get(0)
    }

    /// The table was registered (and captured) before the worker started.
    /// Dropping its last reader must uninstall its capture on the next pass
    /// and keep it uninstalled (issue #427's shape: a startup copy of the set
    /// once re-added a dropped table on every pass).
    #[tokio::test]
    async fn a_pass_after_the_last_reader_drops_uninstalls_the_capture_for_good() {
        let mut f = fixture(&["t"]).await;
        drop_reader(&f.pool, "t").await;

        reconcile_pass(&mut f).await;
        assert!(
            !is_captured(&f.raw).await,
            "the pass uninstalls a table nothing reads any more"
        );

        reconcile_pass(&mut f).await;
        assert!(
            !is_captured(&f.raw).await,
            "a later pass must not install it again"
        );
        assert_eq!(
            marker_count(&f.raw).await,
            0,
            "no capture marker is parked for a table nothing reads"
        );
    }

    #[tokio::test]
    async fn a_table_with_a_remaining_reader_stays_captured() {
        let mut f = fixture(&["t", "u"]).await;
        drop_reader(&f.pool, "t").await;

        reconcile_pass(&mut f).await;
        assert!(
            is_captured(&f.raw).await,
            "`u` still reads `s`, so it stays captured"
        );
    }

    /// Issue #427: a staging worker may start with nothing registered, so it
    /// captures nothing. The pass after the first registration must install
    /// the table's capture and leave the registration with a marker (or
    /// already dispatched by the same pass's discharge), not stranded
    /// `waiting_to_backfill` with nothing to discharge it.
    #[tokio::test]
    async fn a_first_registration_after_an_empty_start_installs_capture() {
        let mut f = fixture(&[]).await;

        reconcile_pass(&mut f).await;
        assert!(
            !is_captured(&f.raw).await,
            "nothing reads `s` yet, so the pass leaves it alone"
        );

        register(&f.pool, "t").await;

        reconcile_pass(&mut f).await;
        assert!(
            is_captured(&f.raw).await,
            "the pass after the first registration captures its source"
        );
        let status: String = f
            .raw
            .query_one(
                "select status from transform_definitions where target_table = 'public.t'",
                &[],
            )
            .await
            .expect("read status")
            .get(0);
        assert!(
            status != "waiting_to_backfill" || marker_count(&f.raw).await == 1,
            "the registration is captured: dispatched, or its join marker is still parked \
             (status {status})"
        );
    }
}

#[cfg(test)]
mod wake_channel_tests {
    //! Issue #875: the default wake channel is derived from the catalog
    //! schema. No Postgres.

    use super::*;

    #[test]
    fn the_default_instance_keeps_the_trellis_wake_name() {
        assert_eq!(default_wake_channel("trellis"), "trellis_wake");
    }

    #[test]
    fn distinct_schemas_get_distinct_stable_channels() {
        assert_ne!(default_wake_channel("a"), default_wake_channel("b"));
        // `a` and `a_wake` must not collide through the suffix.
        assert_ne!(default_wake_channel("a"), default_wake_channel("a_wake"));
        assert_eq!(default_wake_channel("a"), default_wake_channel("a"));
    }

    #[test]
    fn a_channel_never_exceeds_postgres_limit_and_long_schemas_keep_a_hash() {
        // Longest schema whose plain name fits, one past it, the validator's
        // 63-byte maximum, and multi-byte names cut on a char boundary.
        for schema in [
            "s".repeat(MAX_CHANNEL_BYTES - "_wake".len()),
            "s".repeat(MAX_CHANNEL_BYTES - "_wake".len() + 1),
            "s".repeat(63),
            "é".repeat(31),
            "😀".repeat(15),
        ] {
            let channel = default_wake_channel(&schema);
            assert!(channel.len() <= MAX_CHANNEL_BYTES, "{channel}");
            assert!(channel.ends_with("_wake"), "{channel}");
        }
        let fits = "s".repeat(MAX_CHANNEL_BYTES - "_wake".len());
        assert_eq!(default_wake_channel(&fits), format!("{fits}_wake"));
        // Two long schemas sharing a long prefix stay apart through the hash.
        let (x, y) = ("p".repeat(62) + "x", "p".repeat(62) + "y");
        assert_ne!(default_wake_channel(&x), default_wake_channel(&y));
    }

    #[test]
    fn an_explicit_wake_channel_overrides_the_derived_one() {
        let derived = ClientOptions::default();
        assert_eq!(derived.wake_channel_for("inst_a"), "inst_a_wake");
        let explicit = ClientOptions {
            wake_channel: Some("custom".to_string()),
            ..Default::default()
        };
        assert_eq!(explicit.wake_channel_for("inst_a"), "custom");
    }
}
