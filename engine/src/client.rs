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
//! backfill discharge (`run_pending_backfills`), and [`super::staging::apply::drain_once`]'s
//! own downstream-propagation `pg_notify` all wake — and the same name every
//! app-worker task `LISTEN`s on while idle. One name, one channel, shared
//! by construction rather than by convention.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_postgres::NoTls;
use tokio_postgres::config::Host;

use crate::config::Config;
use crate::defs::{self, CatalogError};
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
    /// How long a claim may sit unrefreshed before [`staging::reclaim_stale`]
    /// takes it back. Only consulted when `staging_worker` is set (the
    /// maintenance loop rides only with the staging worker — see the module
    /// doc comment).
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
    /// timeout, one per app-worker task.
    pub heartbeat: HeartbeatDaemonConfig,
    /// How long an app-worker task's idle `LISTEN` wait sits before polling
    /// `next_claimable_segment` again anyway (a floor under `NOTIFY`
    /// delivery, and the only wake source if the listener connection itself
    /// couldn't be opened).
    pub poll_interval: Duration,
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
            reclaim_ttl: Duration::from_secs(30),
            maintenance_interval: Duration::from_millis(300),
            reconcile_interval: Duration::from_secs(5),
            drainer_window: staging::DEFAULT_DRAINER_WINDOW,
            spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
            hard_cap: intake::spill::DEFAULT_HARD_CAP,
            heartbeat: HeartbeatDaemonConfig::default(),
            poll_interval: Duration::from_millis(200),
        }
    }
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Failure modes for [`Client::start`] and [`Client::shutdown`]. Composes
/// the crate's other error types via `From`, matching
/// [`StagingError`]/[`IntakeError`]/[`ApplyError`]/[`crate::error::Error`]'s
/// own hand-rolled-enum convention.
#[derive(Debug)]
pub enum ClientError {
    /// `staging_worker` was set but `source_tables` was empty — nothing to
    /// publish or stream.
    NoSourceTables,
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

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::NoSourceTables => write!(
                f,
                "staging_worker is set but ClientOptions::source_tables is empty; nothing to \
                 publish or stream"
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
    pub fn start(dsn: impl Into<String>, options: ClientOptions) -> Result<Client, ClientError> {
        let dsn = dsn.into();
        if options.staging_worker && options.source_tables.is_empty() {
            return Err(ClientError::NoSourceTables);
        }

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
                runtime.block_on(run(dsn, options, shutdown_rx, ready_tx));
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
    options: ClientOptions,
    mut shutdown_rx: watch::Receiver<bool>,
    ready_tx: std::sync::mpsc::Sender<Result<(), ClientError>>,
) {
    let config = match Config::from_dsn(dsn.clone()) {
        Ok(config) => config,
        Err(err) => {
            let _ = ready_tx.send(Err(err.into()));
            return;
        }
    };
    let pool = match Pool::new(&config) {
        Ok(pool) => pool,
        Err(err) => {
            let _ = ready_tx.send(Err(err.into()));
            return;
        }
    };

    if options.staging_worker
        && let Err(err) = setup_staging(&dsn, &config, &options).await
    {
        let _ = ready_tx.send(Err(err));
        return;
    }

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
        let mut intake = match intake::Intake::connect(&intake_config).await {
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
        // than needing a cooperative exit path.
        intake_task = Some(tokio::spawn(async move {
            let _ = intake.run().await;
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
        };
        maintenance_task = Some(tokio::spawn(maintenance_loop(
            maintenance_config,
            shutdown_rx.clone(),
        )));
    }

    let client_id = format!("trellis-client-{}", uniqueish_id());
    let mut app_worker_tasks = Vec::with_capacity(options.application_threads);
    for i in 0..options.application_threads {
        let claimed_by = format!("{client_id}-app-{i}");
        let worker_config = AppWorkerConfig {
            pool: pool.clone(),
            dsn: dsn.clone(),
            schema: config.schema().to_string(),
            claimed_by,
            wake_channel: options.wake_channel.clone(),
            drainer_window: options.drainer_window,
            heartbeat_config: options.heartbeat.clone(),
            poll_interval: options.poll_interval,
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
/// `options.source_tables`, then either runs the initial snapshot handshake
/// (fresh slot — no `replication_progress` row yet) or discharges any
/// settled backfill markers (a slot this client has already set up, e.g.
/// restarted). Not safe to call concurrently with another client's own
/// staging setup against the same slot — callers are expected to run
/// exactly one staging worker per fleet, per this module's doc comment.
///
/// Uses a dedicated [`ProducerSession`] (not the pool): the session guards
/// (`synchronous_commit`, the producer singleton advisory lock) are
/// connection-scoped, and this function's session is dropped before
/// [`intake::Intake::connect`] opens its own — two `ProducerSession`s (or a
/// `ProducerSession` and `Intake::connect`'s internal one) held
/// concurrently on the same database would collide on that lock.
async fn setup_staging(
    dsn: &str,
    config: &Config,
    options: &ClientOptions,
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
        intake::publication::run_pending_backfills(session.client_mut(), &options.wake_channel)
            .await?;
    } else {
        intake::publication::initial_snapshot_handshake(
            &mut session,
            &options.slot,
            &options.source_tables,
        )
        .await?;
    }

    // `session` drops here, closing its connection and releasing the
    // producer singleton lock before `Intake::connect` opens its own.
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
    } = config;

    let seal_config = SealConfig::default();
    let mut client: Option<tokio_postgres::Client> = None;
    // Due immediately on the very first tick rather than waiting a full
    // `reconcile_interval` after startup — `setup_staging` already ran one
    // reconciliation pass at that point, but this makes the loop's own
    // cadence not depend on when it happens to first observe `Instant::now()`.
    let mut next_reconcile = Instant::now();

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        if client.is_none() {
            client = connect_plain(&dsn, &schema).await.ok();
        }

        if let Some(c) = client.as_mut() {
            let mut failed = staging::seal_if_active_nonempty(c).await.is_err();
            if !failed {
                failed = staging::recover_stuck_seals(c, &seal_config).await.is_err();
            }
            if !failed {
                failed = staging::reclaim_stale(c, reclaim_ttl).await.is_err();
            }
            if !failed {
                failed = staging::retire_drained_segments(c).await.is_err();
            }
            if !failed && Instant::now() >= next_reconcile {
                failed = reconcile_source_tables(
                    c,
                    &pool,
                    &publication,
                    &base_source_tables,
                    &wake_channel,
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

/// Issue #14: re-derives the desired source-table set as the union of
/// `base_source_tables` (whatever [`ClientOptions::source_tables`] was at
/// [`Client::start`] time — kept so an embedder that only ever passes an
/// explicit list, with no `transform_definitions` row for a table, still
/// gets exactly the old, static behavior) and every currently resolvable
/// physical relation [`defs::all_source_relations`] finds in the catalog,
/// then reconciles the publication and discharges any resulting backfill
/// against that set — the same two calls [`setup_staging`] makes once at
/// startup, just re-run periodically so a transform registered against a
/// new table while this client is already running is picked up without a
/// restart.
///
/// Takes a plain `&mut tokio_postgres::Client`, not a [`ProducerSession`]:
/// see [`intake::publication::reconcile_publication`]'s doc comment for why
/// a fresh `ProducerSession` isn't available here (intake's own session
/// holds the producer singleton for the client's whole lifetime).
async fn reconcile_source_tables(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    publication: &str,
    base_source_tables: &[String],
    wake_channel: &str,
) -> Result<(), ReconcileError> {
    let catalog_sources = defs::all_source_relations(pool).await?;
    let mut renamed_explicit = std::collections::HashMap::new();
    for source in &catalog_sources {
        let current = intake::publication::qualify(&source.schema, &source.name)?;
        for original in defs::catalog::source_table_names_for_oid(pool, source.oid).await? {
            // Catalog bindings are authoritative only for the spelling each
            // definition stored; unrelated explicit publication entries stay
            // untouched.
            renamed_explicit.insert(original, current.clone());
        }
    }
    let mut desired: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for base in base_source_tables {
        desired.insert(
            renamed_explicit
                .get(base)
                .cloned()
                .unwrap_or_else(|| base.clone()),
        );
    }
    for source in catalog_sources {
        desired.insert(intake::publication::qualify(&source.schema, &source.name)?);
    }
    let desired: Vec<String> = desired.into_iter().collect();

    intake::publication::reconcile_publication(client, publication, &desired).await?;
    intake::publication::run_pending_backfills(client, wake_channel).await?;
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
            "set search_path to {}, public",
            quote_ident(schema)
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
    wake_channel: String,
    drainer_window: Duration,
    heartbeat_config: HeartbeatDaemonConfig,
    poll_interval: Duration,
}

/// One application-worker task: registers itself as a drainer, runs an
/// out-of-band heartbeat daemon for its claims, then loops claiming and
/// draining sealed batches until shutdown.
///
/// On a non-retryable error from [`staging::drain_once`] (anything
/// `drain_once` itself gave up retrying — a fence miss and a serialization
/// failure are already retried internally up to its own attempt cap), this
/// releases the claim immediately (see [`staging::release`]) rather than
/// leaving it to the reclaim TTL, then deregisters the heartbeat and
/// continues: one bad batch never crashes the worker.
async fn app_worker_loop(config: AppWorkerConfig, mut shutdown_rx: watch::Receiver<bool>) {
    let AppWorkerConfig {
        pool,
        dsn,
        schema,
        claimed_by,
        wake_channel,
        drainer_window,
        heartbeat_config,
        poll_interval,
    } = config;

    let heartbeat = HeartbeatDaemon::spawn(dsn.clone(), schema.clone(), heartbeat_config);
    let mut wake = wake_listener(&dsn, &schema, &wake_channel).await.ok();

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        // `register_drainer` doubles as the liveness refresh
        // `count_live_drainers` reads below (see its own doc comment), so it
        // does need to run on every iteration — `drainers.last_seen` decays
        // over `drainer_window` (30s by default), and this loop's own
        // `poll_interval` floor (200ms by default) is comfortably inside
        // that window even when idle. What's wasteful isn't the cadence,
        // it's checking out a separate pooled connection just for it: one
        // connection serves both this and `next_claimable_segment` below.
        // A failed refresh isn't fatal — it costs this worker one tick of
        // undercounting toward the share denominator, not correctness — so
        // it doesn't block trying `next_claimable_segment` on the same
        // connection.
        let seg_seq = match pool.get().await {
            Ok(client) => {
                let _ = staging::register_drainer(&**client, &claimed_by).await;
                staging::next_claimable_segment(&**client).await
            }
            Err(err) => Err(err.into()),
        };

        let seg_seq = match seg_seq {
            Ok(Some(seq)) => seq,
            Ok(None) => {
                if wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await {
                    break;
                }
                continue;
            }
            Err(_) => {
                if wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await {
                    break;
                }
                continue;
            }
        };

        heartbeat.register(seg_seq, claimed_by.clone()).await;

        let live_workers = match pool.get().await {
            Ok(client) => staging::count_live_drainers(&**client, drainer_window)
                .await
                .unwrap_or(1),
            Err(_) => 1,
        };

        let outcome =
            staging::drain_once(&pool, seg_seq, &claimed_by, live_workers, &wake_channel).await;
        let drain_failed = outcome.is_err();
        if drain_failed {
            // Release immediately rather than waiting on the reclaim TTL:
            // `drain_once` has already exhausted its own internal retries
            // by the time it returns an error, so nothing about waiting
            // longer helps, and every tick this worker holds the claim
            // un-refreshed is a tick some other worker can't pick it up.
            if let Ok(client) = pool.get().await {
                let _ = staging::release(&**client, seg_seq, &claimed_by).await;
            }
        }

        heartbeat.deregister(seg_seq, &claimed_by).await;

        // Back off before re-looping unless we actually drained something.
        // `next_claimable_segment` picks the lowest sealed/undrained segment
        // by state alone, so any iteration that made no progress on it would
        // otherwise be re-selected and spun on hot across every worker with
        // no sleep. Two no-progress cases:
        //   - `Err(_)`: a batch that fails deterministically (a poison
        //     change); principled quarantine is issue #16.
        //   - `Ok(None)`: this call won no buckets — every bucket is already
        //     claimed by a peer (or already drained) — but the segment is
        //     still `draining`, so `next_claimable_segment` hands back the
        //     same seq until the peer finishes.
        // Only `Ok(Some(_))` re-loops immediately, to grab the next segment
        // promptly. The poll floor (or a wake/shutdown) bounds the idle wait.
        let made_progress = matches!(outcome, Ok(Some(_)));
        if !made_progress && wait_for_wake(&mut wake, &mut shutdown_rx, poll_interval).await {
            break;
        }
    }
}

/// Waits for a wake notification, the poll-interval floor, or shutdown —
/// whichever comes first. Returns `true` if shutdown fired.
async fn wait_for_wake(
    wake: &mut Option<WakeListener>,
    shutdown_rx: &mut watch::Receiver<bool>,
    poll_interval: Duration,
) -> bool {
    let notified = async {
        match wake {
            Some(listener) => {
                listener.rx.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    };

    tokio::select! {
        _ = shutdown_rx.changed() => true,
        _ = tokio::time::sleep(poll_interval) => false,
        _ = notified => false,
    }
}

/// A `LISTEN`ing connection on `channel`, delivering one `()` per
/// notification. Best-effort: if opening the connection fails, callers fall
/// back to polling on the timeout alone (see [`wait_for_wake`]).
///
/// Holds the `tokio_postgres::Client` handle for its own connection, not
/// just the polling task: dropping a `tokio_postgres::Client` closes its
/// connection, which would end `_task`'s driver loop and silently stop
/// notifications from ever being delivered (falling all the way back to
/// `poll_interval` polling) even though the struct itself looked alive. The
/// `_client` field exists purely to keep that connection open for as long
/// as this `WakeListener` is.
struct WakeListener {
    rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    _client: tokio_postgres::Client,
    _task: tokio::task::JoinHandle<()>,
}

async fn wake_listener(
    dsn: &str,
    schema: &str,
    channel: &str,
) -> Result<WakeListener, tokio_postgres::Error> {
    let (client, mut connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
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

    client
        .batch_execute(&format!(
            "set search_path to {}, public; listen {}",
            quote_ident(schema),
            quote_ident(channel)
        ))
        .await?;

    Ok(WakeListener {
        rx,
        _client: client,
        _task: task,
    })
}
