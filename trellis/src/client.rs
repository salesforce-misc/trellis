//! The Client runtime (issue #11's runtime increment): the one thing an
//! embedder starts to get a live Trellis pipeline — publication/slot setup,
//! CDC intake, ring maintenance (seal/recover/reclaim), and N application
//! workers draining sealed batches into target tables.
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
//! - `staging_worker: bool` — whether this client also owns CDC intake and
//!   ring maintenance. Exactly one client in a fleet should set this; every
//!   other client (any number of them, across any number of processes) sets
//!   only `application_threads`.
//! - `application_threads: usize` — how many app-worker tasks this client
//!   runs, each independently claiming and draining sealed batches. Zero is
//!   legal: a staging-only client stages and seals but drains nothing.
//!
//! **Source-table discovery**: the catalog (`defs::catalog`) has no "list
//! every distinct source table" query — only per-table lookups
//! (`transforms_for_source`, `source_table_version`) — so rather than add
//! one speculatively, [`ClientOptions::source_tables`] takes an explicit,
//! fully-qualified (`"schema.table"`) list. An embedder that wants
//! catalog-derived discovery can build that list itself from whatever
//! tracks its own transform definitions and pass it in; this keeps the
//! catalog's read surface exactly what stage 05 needed and no more.
//!
//! **Wake channel**: [`ClientOptions::wake_channel`] is the one Postgres
//! `LISTEN/NOTIFY` channel intake's linchpin (`stage_and_advance`), the
//! backfill discharge (`run_pending_backfills`), a seal actually completing
//! (`staging::seal_if_active_nonempty`/`staging::recover_stuck_seals`, issue
//! #271 — the transition that makes a batch claimable, as opposed to the
//! others in this list, which fire when rows merely land in the *active*
//! segment), and [`super::staging::apply::drain_once`]'s own
//! downstream-propagation `pg_notify` all wake — and the same name every
//! app-worker task `LISTEN`s on while idle. One name, one channel, shared
//! by construction rather than by convention.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_postgres::NoTls;
use tokio_postgres::config::Host;

use crate::config::Config;
use crate::defs::chunk_queue;
use crate::defs::{self, CatalogError};
use crate::error_code::{self, ErrorCode};
use crate::intake::{self, IntakeConfig, IntakeError};
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
/// two the constructor contract calls out, plus `source_tables` if
/// `staging_worker` is set.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Whether this client owns CDC intake and ring maintenance
    /// (seal/recover/reclaim). Exactly one client in a fleet should set
    /// this.
    pub staging_worker: bool,
    /// How many independent application-worker tasks this client runs.
    /// Zero is legal — a staging-only client.
    pub application_threads: usize,
    /// Fully-qualified (`"schema.table"`) source tables intake should
    /// publish and stream. Only consulted when `staging_worker` is set;
    /// required (non-empty) in that case.
    pub source_tables: Vec<String>,
    /// The logical replication slot name intake owns.
    pub slot: String,
    /// The publication name intake reconciles membership against. Created
    /// (empty) if it doesn't already exist.
    pub publication: String,
    /// The `LISTEN/NOTIFY` channel shared by intake's linchpin, backfill
    /// discharge, apply's downstream propagation, and every idle app-worker
    /// task's `LISTEN`.
    pub wake_channel: String,
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
    /// set from `transform_definitions` (issue #14) and re-runs
    /// [`intake::publication::reconcile_publication`] /
    /// [`intake::publication::run_pending_backfills`] against it — so a
    /// transform registered against a table not in `source_tables` at
    /// [`Client::start`] time still gets published and backfilled without a
    /// restart. Coarser than `maintenance_interval` by default: unlike
    /// seal/reclaim, this does a catalog query and (when a table is newly
    /// added) an `ALTER PUBLICATION` plus a full backfill enumeration, none
    /// of which need sub-second freshness.
    pub reconcile_interval: Duration,
    /// The window [`staging::count_live_drainers`] uses to size a claim's
    /// share of a batch's buckets.
    pub drainer_window: Duration,
    /// Intake's txn-buffer spill threshold (bytes) — see
    /// [`intake::spill::TxnBuffer`].
    pub spill_threshold: usize,
    /// Intake's txn-buffer hard cap (bytes).
    pub hard_cap: usize,
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
    /// Issue #274 (epic #269): batches several source transactions into one
    /// ring transaction instead of one ring transaction per source commit —
    /// #266's B4 found the un-grouped path walls at ~17k rows/sec at the
    /// one-row-per-commit shape closest to real application traffic.
    /// Defaults to `Some(GroupCommitConfig::default())` (1,000 rows / 5 ms) —
    /// this is the shipped, default-on behavior. `None` is an explicit
    /// escape hatch back to the original one-ring-transaction-per-source-commit
    /// path, kept for callers who need every source commit to land as its
    /// own ring transaction (e.g. to bound worst-case per-row latency more
    /// tightly than the batch's `max_delay`, at the cost of this option's
    /// whole point). See [`intake::GroupCommitConfig`]'s own doc comment.
    pub group_commit: Option<intake::GroupCommitConfig>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            staging_worker: false,
            application_threads: 0,
            source_tables: Vec::new(),
            slot: "trellis_slot".to_string(),
            publication: "trellis_pub".to_string(),
            wake_channel: "trellis_wake".to_string(),
            reclaim_ttl: staging::DEFAULT_RECLAIM_TTL,
            maintenance_interval: Duration::from_millis(300),
            reconcile_interval: Duration::from_secs(5),
            drainer_window: staging::DEFAULT_DRAINER_WINDOW,
            spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
            hard_cap: intake::spill::DEFAULT_HARD_CAP,
            heartbeat: HeartbeatDaemonConfig::default(),
            poll_interval: Duration::from_millis(200),
            group_commit: Some(intake::GroupCommitConfig::default()),
        }
    }
}

/// The option checks [`Client::start_with_config`] runs before spawning
/// anything, split out so they're unit-testable without a database.
fn validate_options(options: &ClientOptions) -> Result<(), ClientError> {
    if options.staging_worker && options.source_tables.is_empty() {
        return Err(ClientError::NoSourceTables);
    }
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
    /// `staging_worker` was set but `source_tables` was empty — nothing to
    /// publish or stream.
    NoSourceTables,
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
    /// runs itself (publication existence check/creation, the
    /// `replication_progress` existence check).
    Db(tokio_postgres::Error),
    /// A failure from the staging ring (session guards, seal, liveness).
    Staging(StagingError),
    /// A failure from CDC intake (connect, publication/slot setup, run).
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
            // `staging_worker` set with no source tables is a rejected
            // call, same category as any other invalid-configuration error.
            ClientError::NoSourceTables | ClientError::HeartbeatNotUnderReclaimTtl { .. } => {
                ErrorCode::Validation
            }
            ClientError::Spawn(_)
            | ClientError::ThreadExitedBeforeReady
            | ClientError::ThreadPanicked => ErrorCode::Internal,
            ClientError::Config(err) => err.code(),
            ClientError::Db(err) => error_code::classify_pg_error(err),
            ClientError::Staging(err) => err.code(),
            ClientError::Intake(err) => err.code(),
            ClientError::Apply(err) => err.code(),
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::NoSourceTables => write!(
                f,
                "staging_worker is set but ClientOptions::source_tables is empty; nothing to \
                 publish or stream"
            ),
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
            ClientError::Staging(err) => Some(err),
            ClientError::Intake(err) => Some(err),
            ClientError::Apply(err) => Some(err),
            ClientError::NoSourceTables
            | ClientError::HeartbeatNotUnderReclaimTtl { .. }
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
    /// background thread has finished setup (publication/slot/snapshot
    /// handshake, if `staging_worker`) and every worker task is spawned, or
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
    /// cleanly: the intake task (if any) is aborted (it's crash-safe and
    /// resumable — see the module doc comment), and the maintenance loop
    /// and every app-worker task are joined after cooperatively exiting at
    /// their next loop boundary.
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

    if options.staging_worker
        && let Err(err) = setup_staging(&dsn, &config, &options, &pool).await
    {
        let _ = ready_tx.send(Err(err));
        return;
    }

    // Issue #132, epic #127, guard (a): one shared in-process
    // "staged-through" watermark, constructed here — before
    // `intake::Intake::connect` below, per that issue's own wiring note —
    // and cloned into both the intake task (which advances it) and every
    // app-worker task's `AppWorkerConfig` below (which reads it, via the
    // drain path's `check_reverse_guards`).
    //
    // **Known limitation, worth flagging explicitly**: this only advances
    // for real when `options.staging_worker` is set on *this* client — an
    // `Arc<AtomicU64>` is inherently process-local. A fleet topology where
    // `application_threads > 0` clients run in a *different* process from
    // the one `staging_worker: true` client (a legal, documented topology —
    // see this module's own doc comment), a drain-only client's watermark
    // here never advances past its `StagedWatermark::new()` starting point
    // (LSN 0), so guard (a) fails closed for every relationship reverse
    // record such a worker ever processes, falling back to the
    // image-less-recompute stopgap every time rather than ever taking the
    // true-delta fast path. That's always *safe* (guard (a) failing closed
    // never corrupts anything — see `StagedWatermark::new`'s own doc
    // comment), just needlessly conservative for that specific multi-process
    // topology; propagating a cross-process watermark (e.g. by polling
    // `replication_progress` somehow without reintroducing the 10-second
    // sawtooth this issue's own §5 measured as wrong) is out of scope here.
    let watermark = staging::StagedWatermark::new();

    let mut intake_task = None;
    let mut maintenance_task = None;
    if options.staging_worker {
        let intake_config = match build_intake_config(&dsn, &config, &options) {
            Ok(cfg) => cfg,
            Err(err) => {
                let _ = ready_tx.send(Err(err));
                return;
            }
        };
        let intake =
            match intake::Intake::connect(&intake_config, watermark.clone(), pool.clone()).await {
                Ok(intake) => intake,
                Err(err) => {
                    let _ = ready_tx.send(Err(err.into()));
                    return;
                }
            };
        // Intake's replication consumer has no built-in cancellation, but
        // it's crash-safe and resumable (acked LSNs are durable, and a
        // fresh `Intake::connect` resumes from the last confirmed
        // position), so it's safe to `.abort()` outright on shutdown rather
        // than needing a cooperative exit path. The same property lets
        // `supervise_intake` restart it after it stops (issue #325).
        let mut connected = Some(intake);
        let intake_watermark = watermark.clone();
        let intake_pool = pool.clone();
        intake_task = Some(tokio::spawn(async move {
            let slot = intake_config.slot.clone();
            supervise_intake(&slot, INTAKE_RESTART_BACKOFF, move || {
                let connected = connected.take();
                let config = intake_config.clone();
                let watermark = intake_watermark.clone();
                let pool = intake_pool.clone();
                async move {
                    let mut intake = match connected {
                        Some(intake) => intake,
                        None => intake::Intake::connect(&config, watermark, pool).await?,
                    };
                    intake.run().await
                }
            })
            .await;
        }));

        let maintenance_config = MaintenanceConfig {
            dsn: dsn.clone(),
            schema: config.schema().to_string(),
            pool: pool.clone(),
            publication: options.publication.clone(),
            base_source_tables: options.source_tables.clone(),
            wake_channel: options.wake_channel.clone(),
            interval: options.maintenance_interval,
            reclaim_ttl: options.reclaim_ttl,
            reconcile_interval: options.reconcile_interval,
            watermark: watermark.clone(),
            backfill_catch_up_timeout: BACKFILL_CATCH_UP_TIMEOUT,
        };
        maintenance_task = Some(tokio::spawn(maintenance_loop(
            maintenance_config,
            shutdown_rx.clone(),
        )));
    }

    let client_id = format!("trellis-client-{}", uniqueish_id());

    // Issue #144, ADR-0010 decision 3: register this process in the
    // worker registry the moment it starts running drain workers — before
    // any app-worker task is even spawned, so a health check racing
    // `Client::start` sees this worker as soon as it's real. Deliberately
    // one row per `Client` (keyed by `client_id`, not per app-worker task's
    // own `claimed_by`): the question `Trellis::has_live_drain_workers`
    // answers is "does a live process exist to drain work," not "how many
    // worker tasks does it run." Only when `application_threads > 0` — a
    // staging-only client (CDC intake + ring maintenance, no app workers)
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
            wake_channel: options.wake_channel.clone(),
            drainer_window: options.drainer_window,
            heartbeat_config: options.heartbeat.clone(),
            poll_interval: options.poll_interval,
            reclaim_ttl: options.reclaim_ttl,
            chunk_reclaim_interval: options.maintenance_interval,
            watermark: watermark.clone(),
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

    if let Some(task) = intake_task {
        task.abort();
    }
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

/// [`supervise_intake`]'s production backoff: 1s doubling to a 60s cap.
const INTAKE_RESTART_BACKOFF: RestartBackoff =
    RestartBackoff::new(Duration::from_secs(1), Duration::from_secs(60));

/// Exponential delay between intake restarts, capped at `max`. An attempt
/// that stayed up for at least `max` counts as healthy, so the streak (and
/// the delay) resets: a transient blip hours after the last one retries
/// after `initial`, not after whatever a long-past streak escalated to.
#[derive(Debug, Clone, Copy)]
struct RestartBackoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl RestartBackoff {
    const fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial,
        }
    }

    /// How long an attempt must stay up to count as healthy, ending any
    /// failure streak before it.
    fn healthy_after(&self) -> Duration {
        self.max
    }

    /// The delay before the next restart, given how long the attempt that
    /// just ended ran for.
    fn next_delay(&mut self, ran_for: Duration) -> Duration {
        if ran_for >= self.healthy_after() {
            self.current = self.initial;
        }
        let delay = self.current;
        self.current = (self.current * 2).min(self.max);
        delay
    }
}

/// Issue #325: runs CDC intake for the client's whole lifetime, restarting
/// it whenever it stops. Never returns; the client stops it by aborting
/// the task (see [`run`]).
///
/// `attempt` is one full intake lifetime: connect (the client passes its
/// already-connected `Intake` to the first attempt, so a setup failure still
/// fails [`Client::start`]) then `run()`. Intake's future used to be spawned
/// as `let _ = intake.run().await`, so any terminal error ended the task with
/// nothing logged: the process kept running maintenance and looked healthy
/// while it had stopped consuming CDC for good.
///
/// Every stop is now logged at `error!`, counted in
/// `trellis_intake_restarts_total`, and followed by a restart after a
/// [`RestartBackoff`] delay. That includes `run()` returning `Ok(())`: with
/// the pinned `pgwire-replication`, the stream only ends cleanly on a
/// configured stop LSN or a client-side `stop()`, and the client does
/// neither. A server-side close (walsender terminated, Postgres shutting
/// down) surfaces as an `Err`, so a clean end is unexpected, and intake is
/// just as stopped either way. Restarting is
/// safe for the same reason aborting on shutdown is: acked LSNs are durable
/// and `Intake::connect` resumes from the last confirmed position, and a
/// failed attempt's staging transaction never committed (dropping the
/// attempt's `Intake` closes its producer connection, which rolls it back).
/// That `Intake` (its producer session and replication connection) is
/// dropped before the backoff sleep, so the restart doesn't race its own
/// predecessor for the producer lock or the slot. If the server still holds
/// either briefly, the restart fails (`ProducerAlreadyRunning` from
/// `connect`, or "replication slot is active" from the new stream's first
/// `recv`), which is logged and retried like any other failure.
///
/// A deterministic error (one the same WAL will reproduce on every replay)
/// retries forever at the capped delay, logging every time. That's
/// deliberate: it's the loud, actionable signal the issue asks for, and
/// the same log-and-retry-next-tick stance [`maintenance_loop`] takes.
///
/// The exception is a restart refused with `ProducerAlreadyRunning` (issue
/// #341): some producer session holds the staging producer lock, and this
/// client keeps retrying so it takes over once that session goes away. The
/// first refusal in a row logs at `info!` and repeats at `debug!`, rather
/// than `error!` every 60s forever. The `producer_lock_held` restart outcome
/// still counts every one.
///
/// Alongside the lifetime restart counter, `trellis_intake_consecutive_failures`
/// (issue #342) tracks the current streak: attempts in a row that ended
/// without staying up for [`RestartBackoff::healthy_after`], the same
/// window that resets the backoff. It's cleared as soon as a running attempt
/// passes that window, so a recovered intake reads `0` rather than whatever
/// its last streak reached.
///
/// Lock refusals count toward that streak, because this client's intake
/// isn't running during them, and the lock's holder may be nobody live. The
/// first attempt reuses the connection [`Client::start`] made, and that start
/// fails outright if the lock is held, so a refusal here always follows this
/// client's own intake stopping. The usual holder is then this client's own
/// previous producer session, which the server hasn't yet noticed is gone.
/// After a network partition, that can last until the server's TCP
/// keepalive gives up on it (hours, with OS defaults), and all staging is
/// down meanwhile. A second `staging_worker` client that took the lock over
/// is the other possibility; it reads `0` while this one climbs, so a fleet
/// that deliberately runs one aggregates with `min by (slot)`.
async fn supervise_intake<A, Fut>(slot: &str, mut backoff: RestartBackoff, mut attempt: A)
where
    A: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), IntakeError>>,
{
    let mut restarts: u64 = 0;
    let mut consecutive_failures: u64 = 0;
    let mut standing_by = false;
    crate::metrics::set_intake_consecutive_failures(slot, 0);
    loop {
        let started = Instant::now();
        let run = attempt();
        tokio::pin!(run);
        let outcome = tokio::select! {
            outcome = &mut run => outcome,
            () = tokio::time::sleep(backoff.healthy_after()) => {
                consecutive_failures = 0;
                crate::metrics::set_intake_consecutive_failures(slot, 0);
                run.await
            }
        };
        let ran_for = started.elapsed();
        if ran_for >= backoff.healthy_after() {
            consecutive_failures = 0;
        }
        let retry_in = backoff.next_delay(ran_for);
        restarts += 1;
        let lock_held = matches!(
            outcome,
            Err(IntakeError::Staging(StagingError::ProducerAlreadyRunning))
        );
        consecutive_failures += 1;
        crate::metrics::set_intake_consecutive_failures(slot, consecutive_failures);
        match outcome {
            Err(_) if lock_held => {
                if standing_by {
                    tracing::debug!(
                        slot = %slot,
                        retry_in = ?retry_in,
                        restarts,
                        consecutive_failures,
                        "CDC intake still standing by: another producer session holds the \
                         staging producer lock"
                    );
                } else {
                    tracing::info!(
                        slot = %slot,
                        retry_in = ?retry_in,
                        restarts,
                        consecutive_failures,
                        "CDC intake standing by: another producer session holds the staging \
                         producer lock, so this client isn't staging source changes; it will \
                         keep retrying and take over once that session ends. The holder may be \
                         another staging worker, or this client's own previous session that \
                         the server hasn't yet noticed is gone"
                    );
                }
                crate::metrics::increment_intake_restarts("producer_lock_held");
            }
            Err(err) => {
                tracing::error!(
                    slot = %slot,
                    error = %err,
                    code = ?err.code(),
                    ran_for = ?ran_for,
                    retry_in = ?retry_in,
                    restarts,
                    consecutive_failures,
                    "CDC intake stopped with an error; source changes are not being staged \
                     until it restarts"
                );
                crate::metrics::increment_intake_restarts("error");
            }
            Ok(()) => {
                tracing::error!(
                    slot = %slot,
                    ran_for = ?ran_for,
                    retry_in = ?retry_in,
                    restarts,
                    consecutive_failures,
                    "CDC intake's replication stream ended unexpectedly; source changes are \
                     not being staged until it restarts"
                );
                crate::metrics::increment_intake_restarts("stream_ended");
            }
        }
        standing_by = lock_held;
        tokio::time::sleep(retry_in).await;
    }
}

/// A cheap, process-local uniqueness token for `claimed_by` prefixes — not a
/// UUID (no such dependency here), just enough entropy that two clients in
/// the same process (as in a test) don't collide. `thread::current().id()`
/// is unique within a process and available with no extra dependency.
fn uniqueish_id() -> String {
    format!("{:?}", std::thread::current().id())
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}

// ---------------------------------------------------------------------
// Staging setup: publication + slot + snapshot handshake
// ---------------------------------------------------------------------

/// Reconciles the publication's membership against
/// `options.source_tables`, then runs the initial snapshot handshake if the
/// slot is fresh (no `replication_progress` row yet). For an existing slot,
/// first recovers from that slot's loss if it has been lost
/// ([`intake::slot_loss::pause_if_slot_lost`], issue #310); the slot's
/// backfill markers themselves are left for the maintenance loop (issue
/// #312; see the comment in the body). Not safe to call concurrently with
/// another client's own staging setup against the same slot — callers are
/// expected to run exactly one staging worker per fleet, per this module's
/// doc comment.
///
/// Uses a dedicated [`ProducerSession`] (not the pool): the session guards
/// (`synchronous_commit`, the producer singleton advisory lock) are
/// connection-scoped, and this function's session is released before
/// [`intake::Intake::connect`] opens its own — two `ProducerSession`s (or a
/// `ProducerSession` and `Intake::connect`'s internal one) held
/// concurrently on the same database would collide on that lock.
async fn setup_staging(
    dsn: &str,
    config: &Config,
    options: &ClientOptions,
    pool: &Pool,
) -> Result<(), ClientError> {
    let mut session = ProducerSession::connect(dsn, config.schema()).await?;

    ensure_publication_exists(session.client(), &options.publication).await?;
    intake::publication::reconcile_publication(
        session.client_mut(),
        &options.publication,
        &options.source_tables,
    )
    .await?;

    let has_progress: bool = session
        .client()
        .query_one(
            "select exists(select 1 from replication_progress where slot_name = $1)",
            &[&options.slot],
        )
        .await?
        .get(0);

    if has_progress {
        // Issue #310: a slot this instance confirmed work against may be gone
        // (retention-cap invalidation, a pre-PG-17 failover, a restore of the
        // source database). Checked here, before anything else runs against
        // it, so a lost slot pauses every transform it fed and recreates
        // itself instead of `Intake::connect` refusing to start — see
        // `intake::slot_loss`. Nothing resumes until an operator says so.
        intake::slot_loss::pause_if_slot_lost(
            &mut session,
            pool,
            &options.slot,
            &options.publication,
        )
        .await?;
        // An existing slot's pending backfill markers are deliberately *not*
        // discharged here. Intake isn't running yet, so an enumeration now
        // would stage `Recompute` rows ahead of the CDC it is about to
        // replay for changes that enumeration already saw, and an aggregate
        // would count those changes twice (issue #312). The maintenance
        // loop's first pass, which runs as soon as intake is up, discharges
        // them behind `run_pending_backfills`'s wait for intake instead.
    } else {
        intake::publication::initial_snapshot_handshake(
            &mut session,
            &options.slot,
            &options.source_tables,
        )
        .await?;
    }

    // Release the producer singleton on the server before `Intake::connect`
    // takes it on its own connection. Dropping the session would free it
    // only once the backend noticed the closed socket, which can be after
    // `Intake::connect`'s `pg_try_advisory_lock` has already failed.
    session.release().await?;
    Ok(())
}

/// `reconcile_publication` only ever alters an existing publication's
/// membership — it never creates one (see its own doc comment) — so this is
/// the one place that does, if `publication` doesn't already exist.
async fn ensure_publication_exists(
    client: &tokio_postgres::Client,
    publication: &str,
) -> Result<(), tokio_postgres::Error> {
    let exists: bool = client
        .query_one(
            "select exists(select 1 from pg_publication where pubname = $1)",
            &[&publication],
        )
        .await?
        .get(0);
    if !exists {
        client
            .batch_execute(&format!("create publication {}", quote_ident(publication)))
            .await?;
    }
    Ok(())
}

/// Decomposes `dsn` into the discrete host/port/user/password/database
/// fields [`IntakeConfig`] needs for its *replication* connection (the
/// `pgwire_replication` transport takes these fields directly, not a DSN
/// string).
fn build_intake_config(
    dsn: &str,
    config: &Config,
    options: &ClientOptions,
) -> Result<IntakeConfig, ClientError> {
    let pg_config = tokio_postgres::Config::from_str(dsn).map_err(|err| {
        ClientError::Config(crate::error::Error::Config(format!(
            "invalid database connection string: {err}"
        )))
    })?;

    let host = match pg_config.get_hosts().first() {
        Some(Host::Tcp(host)) => host.clone(),
        #[cfg(unix)]
        Some(Host::Unix(path)) => path.display().to_string(),
        None => {
            return Err(ClientError::Config(crate::error::Error::Config(
                "dsn has no host".to_string(),
            )));
        }
    };
    let port = pg_config.get_ports().first().copied().unwrap_or(5432);
    let user = pg_config.get_user().unwrap_or("postgres").to_string();
    let password = pg_config
        .get_password()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .unwrap_or_default();
    let database = pg_config.get_dbname().unwrap_or(&user).to_string();

    Ok(IntakeConfig {
        dsn: dsn.to_string(),
        schema: config.schema().to_string(),
        host,
        port,
        user,
        password,
        database,
        slot: options.slot.clone(),
        publication: options.publication.clone(),
        wake_channel: options.wake_channel.clone(),
        spill_threshold: options.spill_threshold,
        hard_cap: options.hard_cap,
        group_commit: options.group_commit,
    })
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
    publication: String,
    base_source_tables: Vec<String>,
    wake_channel: String,
    interval: Duration,
    reclaim_ttl: Duration,
    reconcile_interval: Duration,
    /// Intake's staged-through watermark, which a backfill enumeration waits
    /// on before staging (issue #312; see
    /// [`intake::publication::run_pending_backfills`]).
    watermark: staging::StagedWatermark,
    /// [`BACKFILL_CATCH_UP_TIMEOUT`] outside tests.
    backfill_catch_up_timeout: Duration,
}

/// Runs seal-on-demand, stuck-seal recovery, stale-claim reclaim,
/// drained-segment retirement (stage 06, issue #13/#58), and — on its own,
/// coarser cadence — publication/backfill re-reconciliation (issue #14) on a
/// fixed tick until shutdown. Rides only with the staging worker (see the
/// module doc comment) — application-only clients never run this, since
/// sealing/recovery/reclaim/retirement/reconciliation are ring-wide
/// operations that must not be duplicated across every client in a fleet.
///
/// Holds one dedicated connection across ticks (reconnecting lazily on
/// error) rather than opening a fresh one every tick.
async fn maintenance_loop(config: MaintenanceConfig, mut shutdown_rx: watch::Receiver<bool>) {
    let MaintenanceConfig {
        dsn,
        schema,
        pool,
        publication,
        base_source_tables,
        wake_channel,
        interval,
        reclaim_ttl,
        reconcile_interval,
        watermark,
        backfill_catch_up_timeout,
    } = config;

    let seal_config = SealConfig::default();
    let mut client: Option<tokio_postgres::Client> = None;
    // Due immediately on the very first tick rather than waiting a full
    // `reconcile_interval` after startup — `setup_staging` already ran one
    // reconciliation pass at that point, but this makes the loop's own
    // cadence not depend on when it happens to first observe `Instant::now()`.
    let mut next_reconcile = Instant::now();
    // Issue #310: due immediately too, so a restart with transforms still
    // paused by an earlier slot loss names them right away rather than a
    // minute in.
    let mut next_slot_loss_reminder = Instant::now();

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        if client.is_none() {
            client = connect_plain(&dsn, &schema).await.ok();
        }

        if let Some(c) = client.as_mut() {
            let mut failed = staging::seal_if_active_nonempty(c, &wake_channel)
                .await
                .is_err();
            if !failed {
                failed = staging::recover_stuck_seals(c, &seal_config, &wake_channel)
                    .await
                    .is_err();
            }
            if !failed {
                failed = staging::reclaim_stale(c, reclaim_ttl).await.is_err();
            }
            if !failed {
                // Backfill chunks' own reclaim-stale sweep (docs/decisions/0007's
                // amendment): a drain worker that died mid-chunk leaves a
                // stale claim here, freed the same way a stale `seg_claims`
                // row is above.
                failed = chunk_queue::reclaim_stale_chunks(c, reclaim_ttl)
                    .await
                    .is_err();
            }
            if !failed {
                // Issue #144: table hygiene for `worker_registry` after an
                // unclean shutdown elsewhere in the fleet — a crashed
                // drain-only client's row would otherwise sit forever.
                // `staging::has_live_workers` never depends on this having
                // run (see `staging::worker_registry`'s doc comment); this
                // is purely about bounding the table's size over time.
                failed = staging::reclaim_stale_workers(c, reclaim_ttl)
                    .await
                    .is_err();
            }
            if !failed {
                failed = staging::retire_drained_segments(c).await.is_err();
            }
            if !failed {
                // ADR-0009 decision 5's staging_segments{state} gauge: cheap
                // to read here since maintenance_loop already ticks on this
                // connection regardless, and a failed read just skips a
                // gauge refresh rather than derailing the tick's other work.
                if let Ok(counts) = staging::segment_state_counts(c).await {
                    for (state, count) in counts {
                        crate::metrics::set_staging_segments(state.as_sql(), count as u64);
                    }
                }
            }
            if !failed && Instant::now() >= next_slot_loss_reminder {
                // Best-effort like the gauge above: a failed read skips one
                // reminder, and the next is at most a minute away.
                let _ = intake::slot_loss::log_slot_loss_reminder(&*c).await;
                next_slot_loss_reminder =
                    Instant::now() + intake::slot_loss::SLOT_LOSS_REMINDER_INTERVAL;
            }
            if !failed && Instant::now() >= next_reconcile {
                // A backfill enumeration can sit waiting for intake to catch
                // up (issue #312), and intake is the first task shutdown
                // stops. The wait therefore watches the shutdown signal
                // itself and gives up through its own revert path. Dropping
                // this future on shutdown instead would strand a definition
                // it had already promoted to `backfilling` (see
                // `run_pending_backfills_until`).
                let shutting_down = || *shutdown_rx.borrow();
                failed = reconcile_source_tables(
                    c,
                    &pool,
                    &publication,
                    &base_source_tables,
                    &wake_channel,
                    &watermark,
                    backfill_catch_up_timeout,
                    &shutting_down,
                )
                .await
                .is_err();
                next_reconcile = Instant::now() + reconcile_interval;
            }
            if failed {
                // Drop and reconnect next tick rather than spin on a wedged
                // connection; every one of these operations is naturally
                // idempotent/retriable, so skipping a tick costs nothing but
                // latency.
                client = None;
            }
        }

        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// Failure modes [`reconcile_source_tables`] composes, purely so its `?`
/// call sites don't have to hand-unwrap two unrelated error enums
/// ([`CatalogError`] from the desired-table-set query, [`IntakeError`] from
/// the reconcile/backfill calls themselves) — [`maintenance_loop`] only ever
/// asks `.is_err()` of the result, so this never needs to be more than that.
#[derive(Debug)]
enum ReconcileError {
    Catalog(CatalogError),
    Intake(IntakeError),
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReconcileError::Catalog(err) => write!(f, "{err}"),
            ReconcileError::Intake(err) => write!(f, "{err}"),
        }
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

/// How long one discharge pass lets a backfill enumeration wait for intake
/// to stage through the enumeration's snapshot before deferring it to the
/// next pass (issue #312; see [`intake::publication::run_pending_backfills`]).
/// Intake normally trails the source by milliseconds. The wait only runs this
/// long when intake is replaying a backlog. The maintenance loop does no
/// sealing while it waits, so this is also the longest seal stall one pass
/// can add. A deferral ends the pass, so the stall is not repeated per marker.
/// The value is a judgement call, not a measured bound. While intake stays
/// further behind than this, markers keep deferring and their definitions
/// stay `waiting_to_backfill`.
const BACKFILL_CATCH_UP_TIMEOUT: Duration = Duration::from_secs(5);

/// Issue #14: re-derives the desired source-table set as the union of
/// `base_source_tables` (whatever [`ClientOptions::source_tables`] was at
/// [`Client::start`] time — kept so an embedder that only ever passes an
/// explicit list, with no `transform_definitions` row for a table, still
/// gets exactly the old, static behavior) and every source table
/// [`defs::all_source_tables`] finds registered in the catalog right now,
/// then reconciles the publication and discharges any resulting backfill
/// against that set, re-run periodically so a transform registered against a
/// new table while this client is already running is picked up without a
/// restart. [`setup_staging`] reconciles once at startup but leaves the
/// discharge to this function's first run, once intake is up.
///
/// Issue #75, ADR-0007: [`defs::all_source_tables`] returns each table's own
/// actual, already-persisted qualified identity — this used to instead
/// return bare suffixes and re-qualify every one of them against one assumed
/// schema (`Config::target_schema`), which was simply wrong for a source
/// living anywhere else (including, since issue #76, a definition's own
/// explicit `FROM <schema>.<source>`): it would either publish/backfill a
/// same-named decoy in the assumed schema instead of the real table, or fail
/// outright if no such decoy existed. No re-qualification happens here
/// anymore — every table `all_source_tables` returns is inserted into
/// `desired` exactly as given.
///
/// Takes a plain `&mut tokio_postgres::Client`, not a [`ProducerSession`]:
/// see [`intake::publication::reconcile_publication`]'s doc comment for why
/// a fresh `ProducerSession` isn't available here (intake's own session
/// holds the producer singleton for the client's whole lifetime).
#[allow(clippy::too_many_arguments)]
async fn reconcile_source_tables(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    publication: &str,
    base_source_tables: &[String],
    wake_channel: &str,
    watermark: &staging::StagedWatermark,
    catch_up_timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(), ReconcileError> {
    let mut desired: std::collections::BTreeSet<String> =
        base_source_tables.iter().cloned().collect();
    desired.extend(defs::publication_tables(pool).await?);
    let desired: Vec<String> = desired.into_iter().collect();

    intake::publication::reconcile_publication(client, publication, &desired).await?;
    intake::publication::run_pending_backfills_until(
        client,
        wake_channel,
        watermark,
        catch_up_timeout,
        stop,
    )
    .await?;
    Ok(())
}

/// Opens a standalone `tokio_postgres` connection with `search_path`
/// pinned, for callers (the maintenance loop, the wake listener) that need
/// a concrete `tokio_postgres::Client` rather than a pooled one —
/// mirroring [`ProducerSession::connect`]'s own connection setup, minus the
/// session guards ProducerSession enforces (this isn't a producer).
async fn connect_plain(
    dsn: &str,
    schema: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public; {}",
            quote_ident(schema),
            crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS
        ))
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
    /// isn't a CDC-intake/ring concern the way segment maintenance is (see
    /// this field's own call site's doc comment). Reuses
    /// `ClientOptions::maintenance_interval`'s cadence rather than inventing
    /// a third interval knob.
    chunk_reclaim_interval: Duration,
    /// Issue #132, epic #127, guard (a): this fleet's shared in-process
    /// "staged-through" watermark — the same `Arc` `run()` constructs and
    /// clones into `intake::Intake::connect` (when this client also runs
    /// `staging_worker: true`), threaded here so [`staging::drain_many`]'s
    /// own Phase 3 apply can check guard (a) for any relationship reverse
    /// record it drains. See `run()`'s own doc comment on this field for
    /// the multi-process-fleet caveat.
    watermark: staging::StagedWatermark,
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
/// releases the claim immediately (see [`staging::release`]) rather than
/// leaving it to the reclaim TTL, then deregisters the heartbeat and
/// continues: one bad batch never crashes the worker. A backfill chunk that
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
    } = config;

    // Captured before `heartbeat_config` is moved into `HeartbeatDaemon::spawn`
    // below: `drain_backfill_chunks` needs this same cadence for its own
    // per-chunk-claim heartbeat (see its doc comment) — a chunk write and a
    // segment drain should heartbeat at the same margin under `reclaim_ttl`.
    let chunk_heartbeat_interval = heartbeat_config.interval;
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

        // Claim and execute pending direct-build backfill chunks before this
        // iteration's ring-segment work, so a fleet running only backfill (no
        // sealed segments yet) still makes progress every tick rather than
        // getting stuck behind the segment path's own early `continue`s
        // below.
        let backfill_progress =
            drain_backfill_chunks(&pool, &claimed_by, chunk_heartbeat_interval).await;

        // `register_drainer` doubles as the liveness refresh
        // `count_live_drainers` reads below (see its own doc comment), so it
        // does need to run on every iteration — `drainers.last_seen` decays
        // over `drainer_window` (30s by default), and this loop's own
        // `poll_interval` floor (200ms by default) is comfortably inside
        // that window even when idle. What's wasteful isn't the cadence,
        // it's checking out a separate pooled connection just for it: one
        // connection serves both this and `next_claimable_segments` below.
        // A failed refresh isn't fatal — it costs this worker one tick of
        // undercounting toward the share denominator, not correctness — so
        // it doesn't block trying `next_claimable_segments` on the same
        // connection.
        //
        // Issue #63 Milestone 2: asks for up to `MAX_COALESCE_SEGMENTS` at
        // once rather than just the lowest one, so a burst of quickly
        // sealing segments (many ready before this worker gets back around
        // to claiming) drains in one coalesced `drain_many` call instead of
        // one `drain_once` call — and one full compute-and-apply pass —
        // per segment.
        let seg_seqs = match pool.get().await {
            Ok(client) => {
                let _ = staging::register_drainer(&**client, &claimed_by).await;
                staging::next_claimable_segments(&**client, staging::MAX_COALESCE_SEGMENTS).await
            }
            Err(err) => Err(err.into()),
        };

        let seg_seqs = match seg_seqs {
            Ok(seqs) if !seqs.is_empty() => seqs,
            Ok(_) | Err(_) => {
                // No claimable segment this tick (or the lookup itself
                // failed): only actually wait if the backfill-chunk claim
                // above also made no progress — otherwise loop straight back
                // around to claim more chunks without an idle wait.
                if !backfill_progress
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

        let outcome = staging::drain_many(
            &pool,
            &seg_seqs,
            &claimed_by,
            live_workers,
            &wake_channel,
            &watermark,
        )
        .await;
        let drain_failed = outcome.is_err();
        if drain_failed {
            // Release immediately rather than waiting on the reclaim TTL:
            // `drain_many` has already exhausted its own internal retries
            // by the time it returns an error, so nothing about waiting
            // longer helps, and every tick this worker holds a claim
            // un-refreshed is a tick some other worker can't pick it up.
            if let Ok(client) = pool.get().await {
                for &seg_seq in &seg_seqs {
                    let _ = staging::release(&**client, seg_seq, &claimed_by).await;
                }
            }
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
        let made_progress = matches!(outcome, Ok(Some(_))) || backfill_progress;
        if !made_progress && wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await {
            break;
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
/// maintenance (legitimately tied to owning the replication slot), reclaiming
/// a stale backfill-chunk claim has nothing to do with CDC intake, so it's
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
/// — see `chunk_queue::finish_chunk`) or, on a write error, releasing the
/// claim immediately for the reclaim-stale sweep or another worker to retry
/// — the same "release on error rather than wait out the TTL" discipline
/// [`app_worker_loop`]'s segment path uses.
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
) -> bool {
    let claimed = match pool.get().await {
        Ok(client) => chunk_queue::claim_chunks(&**client, claimed_by, 1).await,
        Err(err) => Err(err.into()),
    };
    let Some(chunk) = claimed.ok().and_then(|chunks| chunks.into_iter().next()) else {
        return false;
    };

    match chunk_queue::run_claimed_chunk(pool, &chunk, claimed_by, heartbeat_interval).await {
        Ok(()) => {
            let _ = chunk_queue::finish_chunk(pool, &chunk, claimed_by).await;
        }
        Err(_) => {
            if let Ok(mut client) = pool.get().await {
                let _ = chunk_queue::release_chunk(&mut **client, chunk.id, claimed_by).await;
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
) -> Result<impl std::future::Future<Output = ()>, tokio_postgres::Error> {
    let (client, mut connection) = tokio_postgres::connect(&dsn, NoTls).await?;
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
            "set search_path to {}, public; {}; listen {}",
            quote_ident(&schema),
            crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS,
            quote_ident(&channel)
        ))
        .await
    {
        driver.abort();
        return Err(err);
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
mod intake_supervisor_tests {
    //! Issue #325: intake's terminal outcome must never vanish silently.
    //! These drive [`supervise_intake`] with a fake attempt closure (no
    //! Postgres), capturing `tracing` events on the test thread.

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::*;

    #[derive(Debug, Clone)]
    struct CapturedEvent {
        level: tracing::Level,
        fields: HashMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<CapturedEvent>>>);

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

    fn install_capture() -> (tracing::subscriber::DefaultGuard, Captured) {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
        (tracing::subscriber::set_default(subscriber), captured)
    }

    const FAST: RestartBackoff =
        RestartBackoff::new(Duration::from_millis(1), Duration::from_millis(4));

    /// The regression: a failing intake run used to end the task with the
    /// error discarded (`let _ = intake.run().await`) — no log, no retry.
    /// Now every failure is logged at `error!` with the error text and slot,
    /// and intake is restarted rather than left dead.
    #[tokio::test]
    async fn failed_run_is_logged_at_error_and_restarted() {
        let (_guard, captured) = install_capture();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (resumed_tx, resumed_rx) = tokio::sync::oneshot::channel::<()>();
        let mut resumed_tx = Some(resumed_tx);

        let counter = attempts.clone();
        let supervisor = supervise_intake("slot_325", FAST, move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let resumed = if n == 2 { resumed_tx.take() } else { None };
            async move {
                if let Some(tx) = resumed {
                    // Third attempt: a healthy, long-running consumer.
                    let _ = tx.send(());
                    std::future::pending::<()>().await;
                }
                Err(IntakeError::MissingProgressRow {
                    slot: format!("slot_325_attempt_{n}"),
                })
            }
        });

        tokio::select! {
            _ = supervisor => panic!("supervise_intake must never return"),
            got = tokio::time::timeout(Duration::from_secs(10), resumed_rx) => {
                got.expect("intake was never restarted after failing")
                    .expect("sender dropped");
            }
        }

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let events = captured.0.lock().unwrap().clone();
        let errors: Vec<_> = events
            .iter()
            .filter(|e| e.level == tracing::Level::ERROR)
            .collect();
        assert_eq!(errors.len(), 2, "one error! per failed attempt: {events:?}");
        for (i, event) in errors.iter().enumerate() {
            assert_eq!(
                event.fields.get("slot").map(String::as_str),
                Some("slot_325")
            );
            let error = event.fields.get("error").expect("error field");
            assert!(
                error.contains(&format!("slot_325_attempt_{i}")),
                "error field must carry the intake error's text, got {error:?}"
            );
        }

        let rendered = crate::metrics::Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_intake_restarts_total{outcome=\"error\"}"),
            "restart counter missing from:\n{rendered}"
        );
    }

    /// A clean `Ok(())` from `run()` means the replication stream ended —
    /// the client never configures a stop LSN, so that's just as dead as an
    /// error and must be surfaced and restarted too.
    #[tokio::test]
    async fn stream_end_is_logged_and_restarted() {
        let (_guard, captured) = install_capture();
        let (resumed_tx, resumed_rx) = tokio::sync::oneshot::channel::<()>();
        let mut resumed_tx = Some(resumed_tx);
        let mut first = true;

        let supervisor = supervise_intake("slot_325_eos", FAST, move || {
            let resumed = if first { None } else { resumed_tx.take() };
            first = false;
            async move {
                if let Some(tx) = resumed {
                    let _ = tx.send(());
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        });

        tokio::select! {
            _ = supervisor => panic!("supervise_intake must never return"),
            got = tokio::time::timeout(Duration::from_secs(10), resumed_rx) => {
                got.expect("intake was never restarted after its stream ended")
                    .expect("sender dropped");
            }
        }

        let events = captured.0.lock().unwrap().clone();
        assert!(
            events.iter().any(|e| e.level == tracing::Level::ERROR
                && e.fields.get("slot").map(String::as_str) == Some("slot_325_eos")),
            "a stream end must be logged at error: {events:?}"
        );
    }

    /// The supervisor must feed each attempt's real uptime into the backoff:
    /// two quick failures escalate the delay, then a failure after an attempt
    /// that stayed up at least `max` restarts from `initial` again.
    #[tokio::test]
    async fn supervisor_resets_backoff_after_a_long_running_attempt() {
        let (_guard, captured) = install_capture();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (resumed_tx, resumed_rx) = tokio::sync::oneshot::channel::<()>();
        let mut resumed_tx = Some(resumed_tx);

        let counter = attempts.clone();
        let supervisor = supervise_intake("slot_325_reset", FAST, move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let resumed = if n == 3 { resumed_tx.take() } else { None };
            async move {
                if let Some(tx) = resumed {
                    let _ = tx.send(());
                    std::future::pending::<()>().await;
                }
                if n == 2 {
                    // Stays up longer than FAST's 4ms cap before failing.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(IntakeError::MissingProgressRow {
                    slot: "slot_325_reset".to_string(),
                })
            }
        });

        tokio::select! {
            _ = supervisor => panic!("supervise_intake must never return"),
            got = tokio::time::timeout(Duration::from_secs(10), resumed_rx) => {
                got.expect("intake was never restarted").expect("sender dropped");
            }
        }

        let events = captured.0.lock().unwrap().clone();
        let retry_in: Vec<_> = events
            .iter()
            .filter(|e| e.level == tracing::Level::ERROR)
            .map(|e| e.fields.get("retry_in").cloned().expect("retry_in field"))
            .collect();
        assert_eq!(retry_in, ["1ms", "2ms", "1ms"], "{events:?}");
    }

    #[test]
    fn backoff_doubles_to_the_cap_and_resets_after_a_healthy_run() {
        let mut backoff = RestartBackoff::new(Duration::from_secs(1), Duration::from_secs(8));
        let short = Duration::from_millis(10);
        let delays: Vec<_> = (0..5).map(|_| backoff.next_delay(short)).collect();
        assert_eq!(
            delays,
            [1, 2, 4, 8, 8].map(Duration::from_secs).to_vec(),
            "exponential, capped at max"
        );
        // An attempt that stayed up at least `max` counts as healthy: the
        // next failure is treated as a fresh one, not the tail of a streak.
        assert_eq!(
            backoff.next_delay(Duration::from_secs(8)),
            Duration::from_secs(1)
        );
        assert_eq!(backoff.next_delay(short), Duration::from_secs(2));
    }

    /// For tests that assert on the streak gauge: a healthy window wide
    /// enough that an instantly-failing attempt never outlasts it, even on a
    /// loaded box (FAST's 4ms could, which would reset the streak mid-test).
    const STREAK: RestartBackoff =
        RestartBackoff::new(Duration::from_millis(1), Duration::from_millis(500));

    fn lock_held() -> IntakeError {
        IntakeError::Staging(StagingError::ProducerAlreadyRunning)
    }

    /// `slot`'s current `trellis_intake_consecutive_failures` value, if the
    /// series exists yet.
    fn consecutive_failures(slot: &str) -> Option<f64> {
        let rendered = crate::metrics::Metrics::new().render_prometheus();
        let prefix = format!("trellis_intake_consecutive_failures{{slot=\"{slot}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .map(|value| value.parse().expect("numeric gauge value"))
    }

    /// Issue #341: another producer session holding the staging producer
    /// lock must not log `error!` on every retry. The first refusal of a run
    /// is `info!` (visible at the default level, marking the transition),
    /// repeats are `debug!`, and the restart counter records them under their
    /// own outcome. Intake still isn't running, so the streak gauge counts
    /// them.
    #[tokio::test]
    async fn producer_lock_contention_logs_below_error() {
        let (_guard, captured) = install_capture();
        let (resumed_tx, resumed_rx) = tokio::sync::oneshot::channel::<()>();
        let mut resumed_tx = Some(resumed_tx);
        let mut n = 0;

        let supervisor = supervise_intake("slot_341", STREAK, move || {
            n += 1;
            let resumed = if n == 4 { resumed_tx.take() } else { None };
            async move {
                if let Some(tx) = resumed {
                    let _ = tx.send(());
                    std::future::pending::<()>().await;
                }
                Err(lock_held())
            }
        });

        tokio::select! {
            _ = supervisor => panic!("supervise_intake must never return"),
            got = tokio::time::timeout(Duration::from_secs(10), resumed_rx) => {
                got.expect("intake was never restarted").expect("sender dropped");
            }
        }

        let events = captured.0.lock().unwrap().clone();
        let levels: Vec<_> = events
            .iter()
            .filter(|e| e.fields.get("slot").map(String::as_str) == Some("slot_341"))
            .map(|e| e.level)
            .collect();
        assert_eq!(
            levels,
            [
                tracing::Level::INFO,
                tracing::Level::DEBUG,
                tracing::Level::DEBUG
            ],
            "{events:?}"
        );

        let rendered = crate::metrics::Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_intake_restarts_total{outcome=\"producer_lock_held\"}"),
            "lock-held restarts must stay countable:\n{rendered}"
        );
        assert_eq!(
            consecutive_failures("slot_341"),
            Some(3.0),
            "the lock's holder may be this client's own dead session, so refusals must stay \
             visible to a streak alert"
        );
    }

    /// Issue #342: the consecutive-failures gauge climbs with each attempt in
    /// a row that ends (error, stream end or lock refusal), and drops back to
    /// 0 once a running attempt passes the healthy window, without waiting
    /// for that attempt to end.
    #[tokio::test]
    async fn consecutive_failures_tracks_the_streak_and_clears_while_healthy() {
        const SLOT: &str = "slot_342";
        let seen_at_attempt_start = Arc::new(Mutex::new(Vec::new()));
        let (resumed_tx, resumed_rx) = tokio::sync::oneshot::channel::<()>();
        let mut resumed_tx = Some(resumed_tx);
        let mut n = 0;

        let seen = seen_at_attempt_start.clone();
        let supervisor = supervise_intake(SLOT, STREAK, move || {
            seen.lock().unwrap().push(consecutive_failures(SLOT));
            n += 1;
            let resumed = if n == 6 { resumed_tx.take() } else { None };
            async move {
                match n {
                    1 | 2 | 5 => Err(IntakeError::MissingProgressRow {
                        slot: SLOT.to_string(),
                    }),
                    3 => Ok(()),
                    4 => Err(lock_held()),
                    _ => {
                        let _ = resumed.expect("sixth attempt").send(());
                        std::future::pending().await
                    }
                }
            }
        });

        tokio::select! {
            // Polled first, so by the time the second branch's sleep (past
            // STREAK's 500ms healthy window) completes, the supervisor has
            // already been woken for its own, earlier healthy-window timer.
            biased;
            _ = supervisor => panic!("supervise_intake must never return"),
            got = tokio::time::timeout(Duration::from_secs(10), async {
                resumed_rx.await.expect("sender dropped");
                tokio::time::sleep(Duration::from_millis(700)).await;
            }) => got.expect("intake was never restarted"),
        }

        assert_eq!(
            *seen_at_attempt_start.lock().unwrap(),
            [
                Some(0.0),
                Some(1.0),
                Some(2.0),
                Some(3.0),
                Some(4.0),
                Some(5.0)
            ],
            "errors, stream ends and lock refusals all extend the streak"
        );
        assert_eq!(
            consecutive_failures(SLOT),
            Some(0.0),
            "an attempt that stays up past the healthy window clears the streak"
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn no_source_tables_is_validation() {
        assert_eq!(ClientError::NoSourceTables.code(), ErrorCode::Validation);
    }

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
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
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
            drain_backfill_chunks(&worker_pool, "gated-worker", Duration::from_secs(5)).await
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

        assert!(drain_backfill_chunks(&pool, "gated-worker", Duration::from_secs(5)).await);
        assert!(
            !drain_backfill_chunks(&pool, "gated-worker", Duration::from_secs(5)).await,
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
        assert_eq!(status, "live");
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

    /// Issue #312 review: shutting down while a backfill enumeration waits
    /// for intake must hand its definition back to `waiting_to_backfill`.
    /// The wait follows the definition's committed promotion to
    /// `backfilling`, and only `waiting_to_backfill` definitions are ever
    /// promoted again, so a definition left in `backfilling` would never
    /// reach `live`. The catch-up timeout here is far longer than the test
    /// waits for shutdown, so only the shutdown signal can end the wait.
    #[tokio::test]
    async fn shutdown_during_a_backfill_wait_returns_the_definition_to_waiting() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut raw = connect_plain(db.dsn(), DEFAULT_SCHEMA)
            .await
            .expect("connect");
        raw.batch_execute(
            "create table public.s (id bigint primary key, a numeric); \
             insert into public.s (id, a) select g, g from generate_series(1, 5) g; \
             create publication test_pub;",
        )
        .await
        .expect("seed source table and publication");

        // An open transaction pins the marker's fence, so the definition
        // defers to `waiting_to_backfill` instead of enumerating inline.
        let straggler = testkit::crash::OpenTransaction::begin(db.dsn()).await;
        straggler.execute("select txid_current()").await;
        intake::publication::reconcile_publication(&mut raw, "test_pub", &["public.s".to_string()])
            .await
            .expect("reconcile leaves an unsettled marker");
        // `testkit`'s pool is the published crate's `Pool`, a different type
        // from this `--lib` build's own, so build one from the same DSN.
        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid config"),
        )
        .expect("build a same-crate pool");
        let columns = std::collections::HashMap::from([("a".to_string(), ValueType::Numeric)]);
        crate::defs::install_definition(
            &pool,
            "TRANSFORM t FROM s SELECT a + a AS x",
            &columns,
            "public",
        )
        .await
        .expect("install_definition defers");
        assert_eq!(status_of(&raw).await, TransformStatus::WaitingToBackfill);
        straggler.commit().await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = MaintenanceConfig {
            dsn: db.dsn().to_string(),
            schema: DEFAULT_SCHEMA.to_string(),
            pool,
            publication: "test_pub".to_string(),
            base_source_tables: vec!["public.s".to_string()],
            wake_channel: "wake".to_string(),
            interval: Duration::from_millis(50),
            reclaim_ttl: Duration::from_secs(30),
            reconcile_interval: Duration::from_secs(3600),
            // Intake never runs, so the enumeration waits on this forever.
            watermark: staging::StagedWatermark::new(),
            backfill_catch_up_timeout: Duration::from_secs(600),
        };
        let task = tokio::spawn(maintenance_loop(config, shutdown_rx));

        // The first pass promotes the definition, then waits on intake.
        let deadline = Instant::now() + Duration::from_secs(30);
        while status_of(&raw).await != TransformStatus::Backfilling {
            assert!(
                Instant::now() < deadline,
                "the first maintenance pass never started the enumeration"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        shutdown_tx.send(true).expect("signal shutdown");
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("the maintenance loop must stop promptly on shutdown")
            .expect("maintenance task");

        assert_eq!(
            status_of(&raw).await,
            TransformStatus::WaitingToBackfill,
            "a shutdown mid-wait must not strand the definition in backfilling"
        );
        let markers: i64 = raw
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count markers")
            .get(0);
        assert_eq!(
            markers, 1,
            "the deferred marker must survive for the next start"
        );
    }
}
