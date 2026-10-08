//! The manual backend (design doc §4 "Two runtimes, one oracle"):
//! harness-driven, lockstep apply -> quiesce -> compare, driven by the
//! harness rather than a real subprocess-supervised deployment (that's what
//! "manual" names — the harness itself polls `quiesce()` rather than the
//! engine notifying it — *not* how many application workers the underlying
//! [`EngineClient`] runs). Drives the real engine as far as its current
//! 1-1/numeric-`+` subset allows — a real [`EngineClient`] (one staging
//! worker, one *or more* application workers — see
//! [`ManualBackend::connect_with_workers`]/[`ManualBackend::connect_with_options`],
//! improvement-plan task D4) against a real, already-migrated Postgres
//! database, reached only over raw source DML (never an application-level
//! notify API), matching the production ingestion path
//! (`docs/data-flow.md#capture-by-triggers`).
//!
//! **D4's "second runtime" is this same type, just started with more than one
//! application worker** — not a separate `ConcurrentBackend` type. Nothing in
//! `ManualBackend` (DDL rendering, DML rendering, `quiesce`, `snapshot`)
//! assumes a single worker; the worker count only ever mattered to one line
//! inside [`Backend::install`] that hardcoded `application_threads: 1`. A
//! second type would have had to duplicate this module's substantial
//! rendering logic for zero behavioral difference; a parameterized
//! constructor shares all of it and keeps exactly one implementation of the
//! seam's DDL/DML/read-back logic to maintain. See
//! `generative/tests/concurrent_convergence.rs` for the new, separate
//! property/test file this constructor is meant to be driven from.
//!
//! **Issue #166 update**: that same DDL/DML/read-back rendering logic
//! (`render_definition`/`render_expr`/`create_source_table`/`apply_op`/
//! `read_table`/`read_aggregate_table`, none of which cares whether the
//! engine driving the ring is in-process or a real subprocess) has since
//! moved to [`super::sql`], a module shared with
//! [`super::SubprocessBackend`] — the second `Backend` impl this doc comment
//! used to argue against, but for a genuinely different reason than D4's
//! worker count: a subprocess-supervised engine that `testkit::CrashGuard`
//! can `SIGKILL` mid-drain. What differs between `ManualBackend` and
//! `SubprocessBackend` is *only* how the engine itself is started, stopped,
//! and restarted — never the DDL/DML/read-back shape, which is why sharing
//! [`super::sql`] rather than duplicating it keeps exactly one
//! implementation of the seam's rendering logic, same as this doc comment's
//! original D4 argument.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use tokio_postgres::NoTls;
use trellis::dev::defs::ast::{KeySpace, TransformDef, ValueType};
use trellis::dev::defs::{
    CatalogError, DdlError, create_relationship, install_definition, qualified_target_table,
    source_primary_key,
};
use trellis::dev::staging::{
    StagingError, has_pending as staging_has_pending, retire_drained_segments,
    seal_if_active_nonempty, seal_phase1, seal_phase2,
};
use trellis::{Client as EngineClient, ClientError, ClientOptions, Config, Pool};

use super::Snapshot;
use super::sql::{self, quote_ident};
use crate::model::{
    BurstAction, Column, NoiseAction, NoiseEvent, NoiseEventKind, Op, Program, Table,
};

/// How long [`ManualBackend::quiesce`] waits for convergence before giving
/// up. Generous: this backend targets correctness, not latency, and a
/// genuinely stuck pipeline is exactly what should time out loudly rather
/// than hang the test suite forever.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// The reclaim TTL a run that stops Postgres under a running engine gives
/// its engine clients ([`ManualBackend::set_reclaim_ttl`]).
///
/// A server stop can strand a claim (issue #752). A worker that claimed a
/// build job or a segment just before the stop loses its connection before
/// it can run the work or give the claim back, so the claim stays in the
/// catalog under a worker that isn't running it. The engine's stale-claim
/// sweep frees it once it is a reclaim TTL old. A cold copy carries the same
/// claim into the restored cluster, where its worker doesn't exist at all.
/// At the engine's default TTL (30 s, the same as [`QUIESCE_TIMEOUT`]),
/// the quiesce after the stop timed out on that recovery about half the time.
/// This TTL keeps the recovery well inside the quiesce's budget.
pub const SERVER_STOP_RECLAIM_TTL: Duration = Duration::from_secs(10);

// The sweep frees a stranded claim within a TTL plus one maintenance tick
// (300 ms by default), and the quiesce needs time left after that to
// finish the work. A third of the budget leaves twenty seconds.
const _: () = assert!(SERVER_STOP_RECLAIM_TTL.as_secs() * 3 <= QUIESCE_TIMEOUT.as_secs());

/// A maintenance interval ([`ManualBackend::connect_with_options`]) for a
/// pin that does all sealing itself, with
/// [`ManualBackend::force_seal_active_segment`] and
/// [`ManualBackend::quiesce_forcing_seals`] (issue #453). The engine's
/// start-up capture pass, and the loop's first tick right after it, still
/// run when the engine client starts, so the definitions
/// [`Backend::install`](super::Backend::install) registered before starting
/// it go live as long as those two passes are enough. The next tick is an
/// hour away, past any test. Nothing orders that first tick's seal step
/// against what the caller does after `install`, though (it usually runs
/// within a millisecond of start-up), so a pin that must have no engine seal
/// between two of its own checks that the two sealed segments are
/// consecutive.
///
/// The tick's other jobs don't run after that first one either: retirement
/// (both seal helpers retire on `RingFull` themselves), stuck-seal recovery
/// (every seal here runs both phases at once, so none sticks), reclaiming
/// stale claims (no worker dies), and further reconcile passes. A pin that
/// needs any of those keeps a real interval. A definition that reads a
/// relationship's to-side, for one, may be ready only on a later pass
/// (`capture::reconcile`'s readiness rules), so it would never go live.
pub const SEAL_ON_DEMAND_INTERVAL: Duration = Duration::from_secs(3600);

/// [`ManualBackend::restart`]'s bounded retry budget (issue #251) for the
/// residual `ProducerAlreadyRunning` window `trellis::Client::shutdown`
/// alone doesn't close: up to this many *retries* (so up to
/// `RESTART_PRODUCER_RETRY_ATTEMPTS + 1` total attempts to start the
/// replacement client) before giving up and propagating the error for
/// real. Bounded, not infinite, specifically so a genuinely stuck lock
/// (a real second producer, not just a not-yet-noticed dead connection)
/// still surfaces as an error rather than hanging `restart` forever.
const RESTART_PRODUCER_RETRY_ATTEMPTS: u32 = 8;

/// The first retry's delay in [`RESTART_PRODUCER_RETRY_ATTEMPTS`]'s bounded
/// backoff; each subsequent retry doubles, capped at
/// [`RESTART_PRODUCER_RETRY_MAX_DELAY`]. Short: the gap this is absorbing is
/// "Postgres hasn't yet been scheduled to notice a closed socket," normally
/// sub-millisecond and only stretched into the tens-of-milliseconds range by
/// genuinely heavy CPU contention (confirmed empirically — see `restart`'s
/// own doc comment) — not a multi-second outage this needs to tolerate.
const RESTART_PRODUCER_RETRY_BASE_DELAY: Duration = Duration::from_millis(10);

/// Cap on [`RESTART_PRODUCER_RETRY_BASE_DELAY`]'s exponential growth, so the
/// last few retries in a worst-case run don't balloon the total wait.
const RESTART_PRODUCER_RETRY_MAX_DELAY: Duration = Duration::from_millis(320);

/// Whether `err` is the producer-singleton-lock conflict
/// (`trellis::StagingError::ProducerAlreadyRunning`) [`ManualBackend::restart`]'s
/// bounded retry (issue #251) specifically targets — recognized in both
/// shape it reaches a fresh [`EngineClient::start_with_config`] call
/// through: `setup_staging`'s producer session, inside
/// `ClientError::Staging`. Every other `ClientError` variant is a real
/// failure `restart` should surface immediately, not retry.
fn is_producer_already_running(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Staging(StagingError::ProducerAlreadyRunning)
    )
}

/// [`ManualBackend::restart`]'s layer-2 fix (issue #251): starts a fresh
/// `EngineClient`, retrying with a short bounded backoff
/// ([`RESTART_PRODUCER_RETRY_ATTEMPTS`]/[`RESTART_PRODUCER_RETRY_BASE_DELAY`]/
/// [`RESTART_PRODUCER_RETRY_MAX_DELAY`]) specifically when
/// `EngineClient::start_with_config` fails with
/// [`is_producer_already_running`] — the residual window between the old
/// client's `shutdown()` returning and Postgres actually noticing the
/// closed connection and releasing the advisory lock. Any other
/// `ClientError` (including a `ProducerAlreadyRunning` that's still there
/// after every retry — a genuinely stuck lock, not just a slow one) is
/// returned immediately, unretried.
async fn start_with_producer_retry(
    config: Config,
    options: ClientOptions,
) -> Result<EngineClient, ClientError> {
    let mut delay = RESTART_PRODUCER_RETRY_BASE_DELAY;
    let mut retries_left = RESTART_PRODUCER_RETRY_ATTEMPTS;
    loop {
        match EngineClient::start_with_config(config.clone(), options.clone()) {
            Ok(client) => return Ok(client),
            Err(err) if retries_left > 0 && is_producer_already_running(&err) => {
                retries_left -= 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RESTART_PRODUCER_RETRY_MAX_DELAY);
            }
            Err(err) => return Err(err),
        }
    }
}

/// Opens the backend's own raw session: `search_path` pinned to the instance
/// schema, and the text-output settings `sql::read_table` relies on.
async fn connect_raw(config: &Config) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (raw, connection) = tokio_postgres::connect(config.dsn(), NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    raw.batch_execute(&format!(
        "set search_path to {}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'",
        config.schema()
    ))
    .await?;
    Ok(raw)
}

/// Failure modes across the manual backend's lifecycle. Composes the
/// engine's own error types via `From` rather than re-wrapping their
/// messages, matching `trellis::ClientError`'s own convention.
#[derive(Debug)]
pub enum ManualBackendError {
    /// An op named a table [`ManualBackend::install`] was never given.
    UnknownTable {
        table: String,
    },
    /// [`ManualBackend::connect`] was given no explicitly-named connection
    /// target. Design doc §6: refuse to run against a database the run did
    /// not name, so this stays enforced if a "point at an existing cluster"
    /// mode is ever added.
    UnnamedTarget,
    /// [`ManualBackend::restart`] was called before [`ManualBackend::install`]
    /// ever started a primary engine client — nothing to restart. A generator
    /// bug (improvement-plan task E3's `restart_after_ops` is only ever
    /// nonzero on a program that also has at least one table, so `install`
    /// always starts a client before any restart point is reached), not a
    /// condition a caller should need to handle gracefully.
    NoClientStarted,
    /// [`ManualBackend::quiesce`] waited [`QUIESCE_TIMEOUT`] for every
    /// installed definition to reach a terminal backfill outcome (`live` or
    /// `quarantined`) and at least one never did — `unsettled` names every
    /// target table still short of that. Distinct from
    /// [`StagingError::ConvergenceTimeout`] (surfaced as
    /// [`ManualBackendError::Staging`]): that one covers the ring's own CDC
    /// convergence, this one covers a direct-build 1-1 definition's
    /// `defs::chunk_queue`-driven backfill, which the ring has no visibility
    /// into at all (docs/decisions/0007's amendment).
    DefinitionSettleTimeout {
        unsettled: Vec<String>,
        waited: Duration,
    },
    /// Issue #236: a statement sent through the `Trellis` facade (the
    /// operator's `RESUME TRANSFORM`) failed.
    Facade(trellis::TrellisError),
    Config(trellis::Error),
    Client(ClientError),
    Catalog(CatalogError),
    Ddl(DdlError),
    Staging(StagingError),
    Db(tokio_postgres::Error),
}

impl From<trellis::TrellisError> for ManualBackendError {
    fn from(err: trellis::TrellisError) -> Self {
        ManualBackendError::Facade(err)
    }
}

impl From<trellis::Error> for ManualBackendError {
    fn from(err: trellis::Error) -> Self {
        ManualBackendError::Config(err)
    }
}

impl From<ClientError> for ManualBackendError {
    fn from(err: ClientError) -> Self {
        ManualBackendError::Client(err)
    }
}

impl From<CatalogError> for ManualBackendError {
    fn from(err: CatalogError) -> Self {
        ManualBackendError::Catalog(err)
    }
}

impl From<DdlError> for ManualBackendError {
    fn from(err: DdlError) -> Self {
        ManualBackendError::Ddl(err)
    }
}

impl From<StagingError> for ManualBackendError {
    fn from(err: StagingError) -> Self {
        ManualBackendError::Staging(err)
    }
}

impl From<tokio_postgres::Error> for ManualBackendError {
    fn from(err: tokio_postgres::Error) -> Self {
        ManualBackendError::Db(err)
    }
}

impl From<sql::QuiesceError> for ManualBackendError {
    fn from(err: sql::QuiesceError) -> Self {
        match err {
            sql::QuiesceError::Db(err) => ManualBackendError::Db(err),
            sql::QuiesceError::Staging(err) => ManualBackendError::Staging(err),
            sql::QuiesceError::DefinitionsUnsettled { unsettled, waited } => {
                ManualBackendError::DefinitionSettleTimeout { unsettled, waited }
            }
        }
    }
}

/// Renders one [`NoiseAction`] to the SQL text [`ManualBackend::fire_noise_event`]
/// runs directly (task E1) — the noise-table analog of [`render_definition`]/
/// [`render_expr`], but for plain DDL/DML rather than a `TRANSFORM`
/// definition. `Insert`/`Update` target `table.columns[1]` by name (falling
/// back to the pk column if `table` is somehow columnless) — the one non-pk
/// column every noise table `crate::generate::noise_table` builds gives it —
/// so this works for any single-extra-column noise table shape without
/// needing to know that column's name in advance.
fn render_noise_action(table: &Table, action: &NoiseAction) -> String {
    let value_col = table
        .columns
        .get(1)
        .map(|c| c.name.as_str())
        .unwrap_or(&table.pk_col);
    match action {
        NoiseAction::Insert { pk, value } => format!(
            "insert into {} ({}, {}) values ({pk}, {})",
            quote_ident(&table.name),
            quote_ident(&table.pk_col),
            quote_ident(value_col),
            noise_sql_literal(value),
        ),
        NoiseAction::Update { pk, value } => format!(
            "update {} set {} = {} where {} = {pk}",
            quote_ident(&table.name),
            quote_ident(value_col),
            noise_sql_literal(value),
            quote_ident(&table.pk_col),
        ),
        NoiseAction::Delete { pk } => format!(
            "delete from {} where {} = {pk}",
            quote_ident(&table.name),
            quote_ident(&table.pk_col),
        ),
        NoiseAction::AddColumn { name, value_type } => format!(
            "alter table {} add column {} {}",
            quote_ident(&table.name),
            quote_ident(name),
            sql::pg_type_name(*value_type),
        ),
        NoiseAction::DropColumn { name } => format!(
            "alter table {} drop column {}",
            quote_ident(&table.name),
            quote_ident(name),
        ),
    }
}

/// A SQL literal for a noise [`NoiseAction`] value: `NULL`, or a
/// single-quoted, escaped text literal. Used unconditionally regardless of
/// the target column's declared type: an untyped string literal in an
/// `INSERT`/`UPDATE`'s value position is coerced to whatever the target
/// column's real type is (standard Postgres literal-type inference), so this
/// needs no type dispatch of its own the way [`sql::Assignment`]'s real-op
/// rendering does with its explicit `::text::<type>` casts.
fn noise_sql_literal(value: &Option<String>) -> String {
    match value {
        None => "NULL".to_string(),
        Some(text) => format!("'{}'", text.replace('\'', "''")),
    }
}

/// The manual backend. Owns a raw connection (DDL, DML, watermark reads,
/// snapshot reads) and, once [`ManualBackend::install`] has run, a live
/// [`EngineClient`] draining sealed batches into every installed
/// definition's target table.
pub struct ManualBackend {
    pool: Pool,
    raw: tokio_postgres::Client,
    engine_client: Option<EngineClient>,
    /// The options the primary `engine_client` was started with — remembered
    /// so [`ManualBackend::restart`] (improvement-plan task E3) can start a
    /// fresh client against the exact same target rather than needing the
    /// caller to hand the options back in.
    client_options: Option<ClientOptions>,
    /// Additional application-worker-only clients started by
    /// [`ManualBackend::scale_out`] (improvement-plan task E3). Kept alive for
    /// the backend's own lifetime (dropped, and so best-effort-signalled to
    /// stop, only when `self` is) — nothing here ever reads back out of this
    /// list, it exists purely so these clients keep running and aren't
    /// dropped the instant `scale_out` returns.
    scale_out_clients: Vec<EngineClient>,
    tables: HashMap<String, Table>,
    defs: Vec<TransformDef>,
    /// How many application-worker tasks [`Backend::install`] starts the
    /// underlying [`EngineClient`] with (improvement-plan task D4). `1` for
    /// every existing single-worker caller (unchanged default via
    /// [`ManualBackend::connect`]); `>1` is the "second runtime"
    /// `generative/tests/concurrent_convergence.rs` drives.
    application_threads: usize,
    /// How often the underlying `EngineClient`'s maintenance loop
    /// (seal/recover/reclaim) ticks — see [`ClientOptions::maintenance_interval`].
    /// Left at the engine's own default for every existing caller;
    /// overridable via [`ManualBackend::connect_with_options`] so a
    /// hand-built pin can widen it comfortably past how long a large burst of
    /// raw DML takes to apply, guaranteeing every row of that burst lands in
    /// the *same* sealed batch instead of splitting across an arbitrary
    /// number of 300ms-apart maintenance ticks (see the D4 hand-built
    /// "genuinely exceeds `MIN_ROWS_TO_SPLIT`" pin in
    /// `generative/tests/concurrent_convergence.rs`).
    maintenance_interval: Duration,
    /// The rows per Re-derive build chunk the engine client plans
    /// ([`ClientOptions::build_chunk_rows`]), when a caller sets one with
    /// [`ManualBackend::set_build_chunk_rows`]; the engine's default
    /// otherwise.
    build_chunk_rows: Option<i64>,
    /// How often the engine client's maintenance loop runs its reconcile
    /// pass ([`ClientOptions::reconcile_interval`]), when a caller sets one
    /// with [`ManualBackend::set_reconcile_interval`]; the engine's default
    /// otherwise.
    reconcile_interval: Option<Duration>,
    /// The reclaim TTL the engine client is started with
    /// ([`ClientOptions::reclaim_ttl`]), when a caller sets one with
    /// [`ManualBackend::set_reclaim_ttl`]; the engine's default otherwise.
    reclaim_ttl: Option<Duration>,
    /// The schema this backend's transform *target* tables are created
    /// under (`trellis::Config::target_schema`; issue #234,
    /// `docs/instance-identity.md`): two instances sharing one
    /// database must not both write their targets into `public`, where two
    /// independently-generated programs' identically-named target tables
    /// would collide for reasons that have nothing to do with instance
    /// isolation.
    target_schema: String,
    /// The resolved [`Config`] this backend's [`Pool`] and every
    /// [`EngineClient`] it starts are built from — carrying
    /// the instance schema and [`Self::target_schema`] (issue #234). Kept whole
    /// rather than rebuilt at each use so the engine client, the oracle's
    /// pool, and this backend's own raw connection provably share one
    /// instance identity.
    config: Config,
    /// The public [`trellis::Trellis`] facade [`super::ConcurrentBackend::act`]
    /// takes operator actions through, connected on first use. It runs no
    /// background work of its own.
    operator: Option<trellis::Trellis>,
}

impl ManualBackend {
    /// Connects to `dsn` — an already-migrated Trellis database (see
    /// `testkit::TestCluster::create_isolated_database`) — but installs
    /// nothing yet. One application worker, the engine's default maintenance
    /// cadence — see [`ManualBackend::connect_with_options`] for a backend
    /// that can widen either.
    ///
    /// `dsn` must be given explicitly by the caller (never inferred from an
    /// environment default): design doc §6 wants every run to refuse an
    /// unnamed target, moot today since `testkit` always hands one over
    /// explicitly, but enforced so it stays moot if an external-cluster mode
    /// is ever added. The resolved target is printed so a run's connection
    /// is never silently ambiguous.
    pub async fn connect(dsn: impl Into<String>) -> Result<Self, ManualBackendError> {
        Self::connect_with_options(dsn, 1, None).await
    }

    /// Like [`ManualBackend::connect`], but starts the underlying
    /// [`EngineClient`] with `application_threads` app-worker tasks instead
    /// of a hardcoded `1` (improvement-plan task D4's "second runtime" —
    /// same `Backend` seam, same DDL/DML/quiesce/snapshot code, a real
    /// multi-worker pool underneath). The engine's default maintenance
    /// cadence is unchanged; see [`ManualBackend::connect_with_options`] if a
    /// caller also needs to widen that (e.g. to force a large hand-built
    /// burst into one sealed batch).
    pub async fn connect_with_workers(
        dsn: impl Into<String>,
        application_threads: usize,
    ) -> Result<Self, ManualBackendError> {
        Self::connect_with_options(dsn, application_threads, None).await
    }

    /// [`ManualBackend::connect`]'s general form: `application_threads`
    /// app-worker tasks, and — when `maintenance_interval` is `Some` — the
    /// underlying [`EngineClient`]'s maintenance-loop cadence overridden from
    /// [`ClientOptions`]'s own default (300ms). `None` keeps the engine's
    /// default, exactly like [`ManualBackend::connect`]/
    /// [`ManualBackend::connect_with_workers`].
    ///
    /// `dsn` must be given explicitly by the caller (never inferred from an
    /// environment default): design doc §6 wants every run to refuse an
    /// unnamed target, moot today since `testkit` always hands one over
    /// explicitly, but enforced so it stays moot if an external-cluster mode
    /// is ever added. The resolved target is printed so a run's connection
    /// is never silently ambiguous.
    pub async fn connect_with_options(
        dsn: impl Into<String>,
        application_threads: usize,
        maintenance_interval: Option<Duration>,
    ) -> Result<Self, ManualBackendError> {
        let dsn = dsn.into();
        // Resolved through the engine's own `Config::from_dsn` — i.e. from
        // `TRELLIS_SCHEMA`/`TRELLIS_TARGET_SCHEMA`, defaulting to
        // `DEFAULT_SCHEMA`/`DEFAULT_TARGET_SCHEMA` — so this constructor
        // keeps its exact pre-#234 behavior rather than hardcoding the
        // defaults and quietly ignoring an environment a caller set.
        let resolved = Config::from_dsn(dsn.clone())?;
        let (schema, target_schema) = (
            resolved.schema().to_string(),
            resolved.target_schema().to_string(),
        );
        Self::connect_with_instance(
            dsn,
            schema,
            target_schema,
            application_threads,
            maintenance_interval,
        )
        .await
    }

    /// [`ManualBackend::connect_with_options`]'s instance-aware general form
    /// (issue #234): the same backend, but pinned to an explicitly-named
    /// Trellis **instance schema** and transform **target schema** rather
    /// than whatever `Config::from_dsn` resolves from the process's
    /// `TRELLIS_SCHEMA`/`TRELLIS_TARGET_SCHEMA` environment.
    ///
    /// This exists because #234 runs *two* Trellis instances side by side
    /// inside one test process (see
    /// `generative/tests/two_instance_noise.rs`), and process-global
    /// environment variables structurally cannot give two in-process
    /// instances two different schemas. `docs/instance-identity.md`'s
    /// "several Trellis instances can coexist in one cluster — even one
    /// database — each isolated within its own schema" is exactly the
    /// topology this constructor makes reachable from the harness.
    ///
    /// The caller is responsible for the schemas existing and for `schema`
    /// having been migrated (`trellis::migrate` against a `Config` carrying
    /// the same schema) before connecting — this constructor only pins,
    /// it never creates.
    pub async fn connect_with_instance(
        dsn: impl Into<String>,
        schema: impl Into<String>,
        target_schema: impl Into<String>,
        application_threads: usize,
        maintenance_interval: Option<Duration>,
    ) -> Result<Self, ManualBackendError> {
        let dsn = dsn.into();
        let schema = schema.into();
        let target_schema = target_schema.into();
        if dsn.trim().is_empty() {
            return Err(ManualBackendError::UnnamedTarget);
        }
        println!(
            "generative: connecting ManualBackend to {dsn} (instance schema {schema:?}, target \
             schema {target_schema:?}, {application_threads} application worker(s))"
        );
        let config = Config::with_schema(dsn.clone(), schema.clone())?
            .with_target_schema(target_schema.clone())?;
        let pool = Pool::new(&config)?;

        let raw = connect_raw(&config).await?;

        Ok(Self {
            pool,
            raw,
            engine_client: None,
            client_options: None,
            scale_out_clients: Vec::new(),
            tables: HashMap::new(),
            defs: Vec::new(),
            application_threads,
            maintenance_interval: maintenance_interval
                .unwrap_or_else(|| ClientOptions::default().maintenance_interval),
            build_chunk_rows: None,
            reconcile_interval: None,
            reclaim_ttl: None,
            target_schema,
            config,
            operator: None,
        })
    }

    /// Sets the rows per Re-derive build chunk ([`ClientOptions::build_chunk_rows`])
    /// the engine client is started with. It takes effect at the first
    /// [`Backend::install`](super::Backend::install), which starts the
    /// client. The concurrent tier draws a few rows so that a build over its
    /// small hot table runs as many chunks, spread through the burst, the
    /// way a build over a large table does at the default size (#720).
    pub fn set_build_chunk_rows(&mut self, rows: i64) {
        self.build_chunk_rows = Some(rows);
    }

    /// Sets how often the engine client's reconcile pass runs
    /// ([`ClientOptions::reconcile_interval`]), which is where a Re-derive
    /// build starts. Like [`ManualBackend::set_build_chunk_rows`], it takes
    /// effect at the first [`Backend::install`](super::Backend::install).
    pub fn set_reconcile_interval(&mut self, interval: Duration) {
        self.reconcile_interval = Some(interval);
    }

    /// Sets how long a claim may sit unrefreshed before the engine takes it
    /// back ([`ClientOptions::reclaim_ttl`]), with claims refreshed every
    /// third of it ([`ClientOptions::heartbeat`]). A run that stops Postgres
    /// under the engine sets [`SERVER_STOP_RECLAIM_TTL`]. Like
    /// [`ManualBackend::set_build_chunk_rows`], it takes effect at the first
    /// [`Backend::install`](super::Backend::install), and
    /// [`ManualBackend::connect_to_restore`] carries it over.
    pub fn set_reclaim_ttl(&mut self, ttl: Duration) {
        self.reclaim_ttl = Some(ttl);
    }

    /// The highest `seg_seq` the drain audit has seen sealed, or `0` before
    /// any seal (issue #453): a baseline for
    /// [`ManualBackend::audited_capture_segments`]. Needs
    /// [`super::ConcurrentBackend::start_drain_audit`] first.
    pub async fn latest_audited_seal(&self) -> Result<i64, ManualBackendError> {
        let row = self
            .raw
            .query_one(
                "select coalesce(max(seg_seq), 0) from generative_audit_sealed",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }

    /// The segments sealed after `after_seg` whose slot held, when its fence
    /// was published, a change captured from `table`: a source write's ring
    /// row, not a drain's `Recompute` (issue #453). The drain audit's seal
    /// trigger records them, so this reads where a change actually landed
    /// rather than which seal a caller forced, even once the segment has
    /// drained and retired. `table` is named as the backend's own connection
    /// resolves it. Needs [`super::ConcurrentBackend::start_drain_audit`]
    /// first.
    pub async fn audited_capture_segments(
        &self,
        table: &str,
        after_seg: i64,
    ) -> Result<Vec<i64>, ManualBackendError> {
        let rows = self
            .raw
            .query(
                "select distinct seg_seq from generative_audit_keys \
                 where seg_seq > $2 and op <> 'recompute' \
                   and to_regclass(src_table) = to_regclass($1) \
                 order by seg_seq",
                &[&table, &after_seg],
            )
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Diagnostic-only (improvement-plan task D4): the largest `bucket_count`
    /// across every segment sealed so far, straight from
    /// the engine's own `staging::claim` partition decision (`segments.bucket_count`,
    /// fixed at seal time from row count alone — see
    /// `staging::claim::MIN_ROWS_TO_SPLIT`/`SEG_BUCKETS`). `0` if no
    /// segment has sealed yet.
    ///
    /// This module is the one place the backend seam (its own doc comment)
    /// allows to know the `segments` table exists at all — everything outside
    /// it, including `generative/tests/concurrent_convergence.rs`'s hand-built
    /// "a big batch really gets split across workers" pin, reaches this fact
    /// only through this method, never by querying `segments` itself.
    pub async fn max_bucket_count(&self) -> Result<i64, ManualBackendError> {
        let row = self
            .raw
            .query_one(
                "select coalesce(max(bucket_count)::int8, 0) from segments",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Whether anything committed so far is still sitting in the ring,
    /// un-drained (issue #138, epic #127 phase 2): a thin wrapper around
    /// `trellis::dev::staging::has_pending`, exposed so a caller stressing the
    /// relationship-delta interleaving scenarios
    /// (`crate::generate::build_relationship_interleaving_scenario`) can
    /// confirm "the op I just committed is in the ring" before calling
    /// [`ManualBackend::force_seal_active_segment`] — sealing an empty (or
    /// stale) active segment would silently defeat the deterministic
    /// seal-boundary reproduction the caller is trying to build. A capture
    /// trigger stages a change in the writer's own transaction (#622), so it
    /// is there at commit.
    ///
    /// Not a substitute for [`ManualBackend::quiesce`]: this only reports
    /// whether something is *staged*, not whether it has been
    /// applied/drained.
    pub async fn has_pending(&self) -> Result<bool, ManualBackendError> {
        Ok(staging_has_pending(&self.raw).await?)
    }

    /// Forces whatever is currently active in the ring to seal immediately
    /// (issue #138, epic #127 phase 2), returning the sealed segment's
    /// `seg_seq`: a direct wrapper around
    /// `trellis::dev::staging::seal_phase1`/`seal_phase2`, the same two-phase
    /// seal the engine's own maintenance loop performs on its normal
    /// cadence. Mirrors `trellis/tests/spike_102.rs`'s
    /// `spike_a2_a_from_side_insert_drains_before_the_parents_reverse_work`
    /// (branch `spike/issue-102-validation-v2`): sealing the parent's own
    /// change into its own segment, then sealing a from-side change into a
    /// strictly later one, lands the two in different segments
    /// deterministically instead of hoping a maintenance tick's timing does
    /// it for you.
    ///
    /// Callers **must** first confirm the change they mean to seal is in
    /// the ring ([`ManualBackend::has_pending`]) — see that method's doc
    /// comment.
    ///
    /// `seal_phase1`'s three refusals (`RingFull`, `SealGateBlocked`,
    /// `Raced`) are backpressure the caller must retry through, not
    /// failures (see its doc comment), so this retries them the way the
    /// engine's own `seal_if_active_nonempty` does. On `RingFull` it runs a
    /// retirement pass first. Nothing else in this harness retires a
    /// drained slot: only the engine's maintenance tick does. Without that
    /// pass, whether the next slot is free depends on whether a tick
    /// happened to land between the slot's occupant becoming retirable and
    /// this call. If the ring is really full of undrained work (a
    /// maintenance tick sealed an extra segment just before this call and
    /// the drain workers haven't caught up), the retry waits for the
    /// workers to drain it, bounded by [`QUIESCE_TIMEOUT`] like
    /// [`ManualBackend::quiesce`]. After that the last refusal is returned
    /// as the error.
    ///
    /// This module is the one place the backend seam allows direct access
    /// to `trellis::staging`'s seal machinery — see the module doc comment
    /// and [`ManualBackend::max_bucket_count`]'s own precedent for the same
    /// door.
    pub async fn force_seal_active_segment(&mut self) -> Result<i64, ManualBackendError> {
        const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
        const MAX_BACKOFF: Duration = Duration::from_millis(250);
        let started = std::time::Instant::now();
        let mut backoff = INITIAL_BACKOFF;
        let outcome = loop {
            let refusal = match seal_phase1(&mut self.raw).await {
                Ok(outcome) => break outcome,
                Err(StagingError::Raced) => continue,
                Err(err @ StagingError::RingFull { .. }) => {
                    if !retire_drained_segments(&mut self.raw).await?.is_empty() {
                        continue;
                    }
                    err
                }
                Err(err @ StagingError::SealGateBlocked) => err,
                Err(other) => return Err(other.into()),
            };
            let waited = started.elapsed();
            if waited >= QUIESCE_TIMEOUT {
                return Err(refusal.into());
            }
            tokio::time::sleep(backoff.min(QUIESCE_TIMEOUT - waited)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        };
        let wake_channel = self.wake_channel();
        seal_phase2(&self.raw, outcome.sealed_seg_seq, &wake_channel).await?;
        Ok(outcome.sealed_seg_seq)
    }

    /// [`Backend::quiesce`](super::Backend::quiesce) that seals what the
    /// maintenance tick would have, instead of waiting for the tick (issue
    /// #453): for a backend whose tick is off ([`SEAL_ON_DEMAND_INTERVAL`])
    /// or widened. Draining stages more rows into the active segment (a
    /// relationship's `Recompute`s, say), and only a seal makes them
    /// drainable.
    ///
    /// Each pass seals the active segment if it holds anything, with the
    /// tick's own seal step (`seal_if_active_nonempty`), then checks
    /// [`sql::settled`]. It backs off only after a pass that found nothing
    /// to seal. Once settled, a plain quiesce confirms it, so this never
    /// returns on weaker evidence than [`Backend::quiesce`](super::Backend::quiesce)
    /// does. The whole call is bounded by [`QUIESCE_TIMEOUT`]; past it, that
    /// confirming quiesce checks once and reports what is still outstanding.
    pub async fn quiesce_forcing_seals(&mut self) -> Result<(), ManualBackendError> {
        const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
        const MAX_BACKOFF: Duration = Duration::from_millis(100);
        let started = std::time::Instant::now();
        let wake_channel = self.wake_channel();
        let mut backoff = INITIAL_BACKOFF;
        while started.elapsed() < QUIESCE_TIMEOUT {
            let sealed = match seal_if_active_nonempty(&mut self.raw, &wake_channel).await {
                Ok(outcome) => outcome.is_some(),
                // Every slot holds an undrained segment: the workers free one.
                // Or a tick's seal got there first.
                Err(StagingError::RingFull { .. } | StagingError::Raced) => false,
                Err(other) => return Err(other.into()),
            };
            if sealed {
                backoff = INITIAL_BACKOFF;
                continue;
            }
            if sql::settled(&self.raw, &self.defs).await? {
                break;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
        let remaining = QUIESCE_TIMEOUT.saturating_sub(started.elapsed());
        Ok(sql::quiesce(&self.raw, &self.defs, remaining).await?)
    }

    /// The channel a seal notifies (issue #271: `seal_phase2` `pg_notify`s it
    /// the instant it publishes the fence). Every caller here has always used
    /// `ClientOptions::default()`'s wake channel (never overridden by this
    /// backend), so this falls back to that default when no engine client has
    /// started yet to remember its own options.
    fn wake_channel(&self) -> String {
        self.client_options
            .as_ref()
            .map(|options| options.wake_channel.clone())
            .unwrap_or_else(|| ClientOptions::default().wake_channel)
    }

    /// Delegates to the shared [`sql::create_source_table`] — see that
    /// function's doc comment for the exact DDL shape (`crate::model::PRIMARY_KEY_VALUE_TYPE`
    /// pk, per-column `UNIQUE`).
    async fn create_source_table(&self, table: &Table) -> Result<(), ManualBackendError> {
        Ok(sql::create_source_table(&self.raw, table).await?)
    }

    async fn install_definition(&mut self, def: &TransformDef) -> Result<(), ManualBackendError> {
        let source_table = self
            .tables
            .get(&def.source)
            .ok_or_else(|| ManualBackendError::UnknownTable {
                table: def.source.clone(),
            })?
            .clone();
        let source_columns = sql::source_columns(&source_table);

        let text = sql::render_definition(def);

        // Issue #63 C1: `install_definition` is the same front door real
        // callers use. It creates the target table and records the
        // definition `waiting_to_backfill`; the staging worker's backfill
        // discharge builds it in the background (ADR-0016). Routing the fuzz
        // harness through it, instead of hand-rolling the same
        // create-table/persist sequence, keeps this backend exercising the
        // exact path production traffic takes.
        // Issue #234: `self.target_schema`, not a hardcoded `"public"` — see
        // that field's doc comment. `connect`/`connect_with_workers`/
        // `connect_with_options` all still resolve it to exactly what a
        // literal `"public"` used to mean, so no pre-#234 caller changes.
        install_definition(&self.pool, &text, &source_columns, &self.target_schema).await?;
        Ok(())
    }

    /// Creates a table with the exact same DDL shape [`ManualBackend::install`]
    /// gives a real source table (task E1: untracked-object noise), so it is
    /// indistinguishable from a real one at the DDL level, but never
    /// registers it in `self.tables`, and no definition reads it, so the
    /// engine never captures it (the engine captures exactly the tables
    /// registered definitions read, issue #427). Those are the only ways a table is "tracked" by this backend
    /// or watched by the engine (see [`ManualBackend::install`]/
    /// [`ManualBackend::snapshot`]), and `run::check_program`'s oracle never
    /// looks at "every table in the schema" either — it only ever resolves a
    /// definition's source/target through `Program.tables`/`Program.defs`
    /// (see that function's own doc comment) — so a table installed this way
    /// is structurally invisible to every check this suite runs, regardless
    /// of what DML/DDL later targets it, or what its name/columns happen to
    /// look like.
    pub async fn install_noise_table(&mut self, table: &Table) -> Result<(), ManualBackendError> {
        self.create_source_table(table).await
    }

    /// Runs one arbitrary SQL statement directly against this backend's own
    /// connection, bypassing [`ManualBackend::apply`]'s op-shaped DML
    /// entirely (task E1 noise DDL/DML; task E5's `CHECKPOINT`). Returns the
    /// statement's affected-row count (`0` for a DDL statement or
    /// `CHECKPOINT`, same as any other statement that doesn't affect table
    /// rows).
    pub async fn execute_raw(&self, sql: &str) -> Result<u64, ManualBackendError> {
        Ok(self.raw.execute(sql, &[]).await?)
    }

    /// Fires one [`NoiseEvent`]'s [`NoiseEventKind`]: renders it to SQL text
    /// ([`render_noise_action`] for a `Table` event, used as-is for an
    /// `Admin` one) and runs it via [`ManualBackend::execute_raw`]. Errors
    /// are swallowed (logged to stderr, never returned) — noise/
    /// administration is deliberately allowed to fail (e.g. a `DROP COLUMN`
    /// on a column an earlier event already dropped) without that ever
    /// counting as a run failure; the whole point of tasks E1/E5 is that
    /// nothing here can affect the tracked convergence check regardless of
    /// whether it succeeds.
    pub async fn fire_noise_event(&self, table: Option<&Table>, event: &NoiseEvent) {
        let sql = match &event.kind {
            NoiseEventKind::Admin(sql) => sql.clone(),
            NoiseEventKind::Table(action) => {
                let Some(table) = table else {
                    eprintln!(
                        "generative: noise event {event:?} names a Table(..) action but no \
                         noise table was installed — skipping"
                    );
                    return;
                };
                render_noise_action(table, action)
            }
        };
        if let Err(err) = self.execute_raw(&sql).await {
            eprintln!("generative: noise statement {sql:?} failed (expected/ignored): {err:?}");
        }
    }
}

/// How long [`await_pool_usable`] keeps trying after a Postgres restart.
const POOL_USABLE_TIMEOUT: Duration = Duration::from_secs(15);

/// Blocks until `pool` hands out a connection that can run a query, after a
/// Postgres restart severed every connection it held (issue #236).
///
/// A pooled connection is recycled only once its driver task has noticed the
/// server closed it. One the pool hasn't caught up on yet looks healthy and
/// fails its first query, and the failed attempt is what gets it discarded.
/// So this just retries a trivial query until one succeeds, working through
/// the stale connections.
pub async fn await_pool_usable(pool: &Pool) -> Result<(), ManualBackendError> {
    let started = std::time::Instant::now();
    loop {
        let attempt = match pool.get().await {
            Ok(conn) => conn
                .query_one("select 1", &[])
                .await
                .map(|_| ())
                .map_err(ManualBackendError::from),
            Err(err) => Err(ManualBackendError::from(err)),
        };
        match attempt {
            Ok(()) => return Ok(()),
            Err(err) if started.elapsed() >= POOL_USABLE_TIMEOUT => return Err(err),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Issue #236: the database-administration actions
/// (`crate::model::DbAdminAction`). The harness side of each one lives here,
/// because it has to reach the engine client and the backend's own
/// connections. Restarting the server itself is the cluster's job
/// ([`super::ClusterControl`]), since only the cluster knows its data
/// directory.
impl ManualBackend {
    /// Replaces the backend's raw session and waits for its pool to work
    /// again, after a Postgres restart severed both. The engine client is
    /// left alone: reconnecting is its own job, and that is what a
    /// restart action tests.
    pub async fn reconnect_after_server_restart(&mut self) -> Result<(), ManualBackendError> {
        self.raw = connect_raw(&self.config).await?;
        await_pool_usable(&self.pool).await
    }

    /// Shuts the primary engine client down. [`Self::start_engine`] brings
    /// an equivalent client back.
    pub async fn stop_engine(&mut self) -> Result<(), ManualBackendError> {
        if let Some(client) = self.engine_client.take() {
            client.shutdown().await?;
        }
        Ok(())
    }

    /// Starts a primary engine client with the options [`Backend::install`]
    /// remembered, after [`Self::stop_engine`]. Same producer-lock retry as
    /// [`Backend::restart`].
    pub async fn start_engine(&mut self) -> Result<(), ManualBackendError> {
        let options = self
            .client_options
            .clone()
            .ok_or(ManualBackendError::NoClientStarted)?;
        let client = start_with_producer_retry(self.config.clone(), options).await?;
        self.engine_client = Some(client);
        Ok(())
    }

    /// A second backend over a restored copy of this backend's database at
    /// `dsn` (issue #236), with this one's engine client already started on
    /// it. Nothing is installed: the restored database already holds the
    /// program's tables, catalog, ring and targets as of the backup, so this
    /// only carries over what the harness itself remembers (the tables and
    /// definitions it snapshots, and the client options) and starts an equivalent client, the way
    /// an operator would start Trellis again on a restored server.
    ///
    /// Fails with [`ManualBackendError::NoClientStarted`] if this backend
    /// never installed anything.
    pub async fn connect_to_restore(
        &self,
        dsn: impl Into<String>,
    ) -> Result<Self, ManualBackendError> {
        let mut restored = Self::connect_with_instance(
            dsn,
            self.config.schema(),
            self.target_schema.clone(),
            self.application_threads,
            Some(self.maintenance_interval),
        )
        .await?;
        restored.client_options = self.client_options.clone();
        restored.tables = self.tables.clone();
        restored.defs = self.defs.clone();
        restored.start_engine().await?;
        Ok(restored)
    }

    /// Every installed definition's persisted status, as
    /// `(target, status)` in install order. A definition with no catalog
    /// row at all reads as `"<missing>"`.
    pub async fn definition_statuses(&self) -> Result<Vec<(String, String)>, ManualBackendError> {
        let mut statuses = Vec::with_capacity(self.defs.len());
        for def in &self.defs {
            let status = self
                .raw
                .query_opt(
                    "select status from transform_definitions \
                     where split_part(target_table, '.', 2) = $1",
                    &[&def.target],
                )
                .await?
                .map(|row| row.get::<_, String>(0))
                .unwrap_or_else(|| "<missing>".to_string());
            statuses.push((def.target.clone(), status));
        }
        Ok(statuses)
    }

    /// The operator's half of issue #310's recovery: `RESUME TRANSFORM` on
    /// every installed definition, through the public `Trellis` facade an
    /// operator would use. Each resume is a fresh backfill from current
    /// source data; [`Backend::quiesce`] waits for them to finish.
    pub async fn resume_all(&self) -> Result<(), ManualBackendError> {
        let facade =
            trellis::Trellis::connect(self.config.clone(), trellis::TrellisOptions::default())
                .await?;
        for def in &self.defs {
            facade
                .apply(&format!("RESUME TRANSFORM {}", def.target))
                .await?;
        }
        facade.shutdown().await?;
        Ok(())
    }
}

impl super::Backend for ManualBackend {
    type Error = ManualBackendError;

    async fn install(&mut self, program: &Program) -> Result<(), ManualBackendError> {
        for table in &program.tables {
            self.create_source_table(table).await?;
            self.tables.insert(table.name.clone(), table.clone());
        }
        // Issue #34: every relationship is declared before any definition,
        // since a definition naming an undeclared relationship is rejected
        // at validation time. Both endpoints already exist by now — a
        // program's relationships only ever join two of its own tables, and
        // every one of those was just created above (or, for a mid-stream
        // definition install, in the very first `install` call — see
        // `crate::run::run_convergence`, which passes relationships only
        // alongside the tables).
        for rel in &program.relationships {
            create_relationship(&self.pool, &sql::render_relationship(rel)).await?;
        }
        for def in &program.defs {
            self.install_definition(def).await?;
            self.defs.push(def.clone());
        }

        // The staging worker captures whatever the definitions just
        // registered read, straight from the catalog (issue #427).
        if !program.tables.is_empty() && self.engine_client.is_none() {
            let mut options = ClientOptions {
                staging_worker: true,
                application_threads: self.application_threads,
                maintenance_interval: self.maintenance_interval,
                build_chunk_rows: self
                    .build_chunk_rows
                    .unwrap_or_else(|| ClientOptions::default().build_chunk_rows),
                reconcile_interval: self
                    .reconcile_interval
                    .unwrap_or_else(|| ClientOptions::default().reconcile_interval),
                ..Default::default()
            };
            if let Some(ttl) = self.reclaim_ttl {
                options.reclaim_ttl = ttl;
                options.heartbeat.interval = ttl / 3;
            }
            let client = EngineClient::start_with_config(self.config.clone(), options.clone())?;
            self.engine_client = Some(client);
            // Remembered so `restart` (improvement-plan task E3) can start an
            // equivalent replacement client without the caller needing to
            // hand these options back in.
            self.client_options = Some(options);
        }
        Ok(())
    }

    /// Delegates to the shared [`sql::apply_op`] — see that function's doc
    /// comment. Every op shape's exact SQL rendering (the `$n::text::<type>`
    /// cast discipline, the transactional count-then-truncate) lives there
    /// now, shared verbatim with [`super::SubprocessBackend::apply`].
    async fn apply(&mut self, op: &Op) -> Result<u64, ManualBackendError> {
        sql::apply_op(&mut self.raw, &self.tables, op)
            .await
            .map_err(|err| match err {
                sql::ApplyOpError::UnknownTable(table) => {
                    ManualBackendError::UnknownTable { table }
                }
                sql::ApplyOpError::Db(err) => ManualBackendError::Db(err),
            })
    }

    async fn quiesce(&mut self) -> Result<(), ManualBackendError> {
        // Improvement-plan workstream C, task C1: opt-in per-call timing
        // around the whole quiesce (since #432, the settle and catch-up
        // waits as well as the watermark -> converge round trip), to test the
        // hypothesis (see `local_docs/generative-suite-improvement-plan.md`)
        // that the convergence property's wall-clock variance came from a
        // ~10s pipeline stall. It did: issue #452's keepalive throttle, first
        // blamed on the seal age gate. Silent and free unless
        // `GENERATIVE_QUIESCE_TIMING` is set: the env lookup happens once per
        // call so the default (unset) path pays exactly one `var_os` check
        // and no clock reads.
        let timing_enabled = std::env::var_os("GENERATIVE_QUIESCE_TIMING").is_some();
        let start = timing_enabled.then(std::time::Instant::now);

        // Issue #432: definition status, catch-up markers and the ring, in
        // that order — see `sql::quiesce` for why the order matters.
        let result = sql::quiesce(&self.raw, &self.defs, QUIESCE_TIMEOUT).await;

        if let Some(start) = start {
            eprintln!("QUIESCE_TIMING {}", start.elapsed().as_millis());
        }
        Ok(result?)
    }

    async fn snapshot(&mut self) -> Result<Snapshot, ManualBackendError> {
        let mut snapshot: Snapshot = BTreeMap::new();

        for table in self.tables.values() {
            let rows = sql::read_table(
                &self.raw,
                &quote_ident(&table.name),
                &table.pk_col,
                &table.columns,
            )
            .await?;
            snapshot.insert(table.name.clone(), rows);
        }

        for def in &self.defs {
            // Issue #234: `self.target_schema`, not a hardcoded `"public"` —
            // see `install_definition` above and that field's doc comment.
            let qualified = qualified_target_table(&self.target_schema, def);
            let rows = match &def.key_space {
                KeySpace::OneToOne => {
                    // `source_primary_key` accepts an arbitrary-arity source
                    // primary key (issue #126), and — since issue #121 — the
                    // engine's own 1-1 target DDL mirrors a composite key in
                    // full rather than narrowing it to one column. This
                    // generative suite, though, never itself constructs a
                    // composite-PK 1-1 definition (out of this issue's
                    // scope), and `sql::read_table`'s snapshot reader below
                    // only takes one `pk_col` name — narrowed to the first
                    // (and, for every definition this suite actually
                    // generates, only) column.
                    let pk = source_primary_key(&self.pool, &def.source).await?;
                    let target_columns: Vec<Column> = std::iter::once(Column {
                        name: pk[0].name.clone(),
                        value_type: ValueType::Numeric,
                    })
                    .chain(def.fields.iter().map(|f| Column {
                        name: f.name.clone(),
                        // The physical column type doesn't matter for a
                        // `::text` read; only the name is used below.
                        value_type: ValueType::Text,
                    }))
                    .collect();
                    sql::read_table(&self.raw, &qualified, &pk[0].name, &target_columns).await?
                }
                KeySpace::Aggregate { group_by } => {
                    // The generative suite never constructs a relationship-path
                    // `GROUP BY` key (issue #137 scopes that support out of this
                    // crate) — every key's target column name is read straight
                    // off the target table either way, so mapping to that name
                    // here needs no relationship awareness.
                    let group_by_names: Vec<String> = group_by
                        .iter()
                        .map(|k| k.target_column_name().to_string())
                        .collect();
                    sql::read_aggregate_table(&self.raw, &qualified, &group_by_names, &def.fields)
                        .await?
                }
            };
            snapshot.insert(def.target.clone(), rows);
        }

        Ok(snapshot)
    }

    /// Improvement-plan task E3: tears down the current primary
    /// `trellis::Client` and starts a fresh one against the same dsn/options
    /// [`ManualBackend::install`] remembered. The ring is durable Postgres
    /// state untouched by any of this, so the new client resumes exactly
    /// where the old one left off.
    ///
    /// Issue #251, two layers of fix:
    ///
    /// 1. This used to just drop the old `Option<Client>` and rely on
    ///    `Client`'s `Drop` impl (a best-effort shutdown signal with no
    ///    join) to tear it down, then immediately start the replacement.
    ///    `Drop` never waits for the background thread to exit, so the old
    ///    producer's `pg_try_advisory_lock`-held session could still be open
    ///    when the very next line's producer tried to acquire that same
    ///    lock — a real, sporadic `ProducerAlreadyRunning` race (see the CI
    ///    failure this issue links). Awaiting `trellis::Client::shutdown` on
    ///    the outgoing client first — which sends the same signal *and*
    ///    joins the thread — makes the old lock's release happen-before the
    ///    new client's *thread* fully exiting, closing the client-side half
    ///    of the race.
    /// 2. That alone still narrows rather than closes the window:
    ///    `shutdown` guarantees this process's connection object is torn
    ///    down, not that the Postgres backend serving it has actually been
    ///    scheduled to notice the closed socket and release the advisory
    ///    lock — a kernel/Postgres-scheduling gap, not anything this
    ///    process's own state can observe or wait on directly. Confirmed by
    ///    direct measurement: under sustained heavy CPU contention (dozens
    ///    of runs of the regression test below against real artificial
    ///    load), layer 1 alone still hit `ProducerAlreadyRunning` in roughly
    ///    1 of every 8 restarts — a large reduction from pre-fix, not an
    ///    elimination. [`start_with_producer_retry`] closes that residual
    ///    gap the only way available from this side of the socket: retry
    ///    `EngineClient::start_with_config` with a short bounded backoff
    ///    ([`RESTART_PRODUCER_RETRY_ATTEMPTS`]) specifically when it fails
    ///    with exactly this conflict ([`is_producer_already_running`]),
    ///    rather than propagating it immediately. Bounded so a *genuinely*
    ///    stuck lock (an actual second producer, not just a not-yet-noticed
    ///    dead connection) still surfaces as a real error rather than
    ///    hanging forever.
    ///
    /// Matches [`super::SubprocessBackend::restart`]'s own explicit
    /// `kill`-then-`wait` (never just letting its `CrashGuard` drop) for the
    /// identical layer-1 reason. The process's exit doesn't close layer 2's
    /// gap either: the killed engine's backend still has to be scheduled to
    /// read the EOF (issue #870). `SubprocessBackend` waits for the singleton
    /// to read free in `pg_locks` before spawning instead of retrying, since
    /// a failed start there is a dead subprocess, not a typed error.
    async fn restart(&mut self) -> Result<(), ManualBackendError> {
        let options = self
            .client_options
            .clone()
            .ok_or(ManualBackendError::NoClientStarted)?;
        // Take the old client and await its graceful shutdown — not just
        // drop it — before starting the replacement, so the old producer's
        // advisory lock is released (from this process's point of view)
        // before the new one tries to acquire it (issue #251, layer 1).
        if let Some(old_client) = self.engine_client.take() {
            old_client.shutdown().await?;
        }
        // Layer 2: retry through the residual window `shutdown` alone can't
        // close (see this method's own doc comment).
        let client = start_with_producer_retry(self.config.clone(), options).await?;
        self.engine_client = Some(client);
        Ok(())
    }

    /// Improvement-plan task E3: starts an additional, application-worker-only
    /// (`staging_worker: false`) `trellis::Client` against the same dsn,
    /// alongside whatever primary client `install` already started —
    /// confirming multiple clients can coexist draining the same ring (the
    /// module doc comment on `trellis::Client` claims this is supported; this
    /// is where the generative suite exercises that claim). Needs no state
    /// beyond the dsn.
    async fn scale_out(&mut self) -> Result<(), ManualBackendError> {
        let options = ClientOptions {
            staging_worker: false,
            application_threads: 1,
            ..Default::default()
        };
        let client = EngineClient::start_with_config(self.config.clone(), options)?;
        self.scale_out_clients.push(client);
        Ok(())
    }
}

/// A [`ManualBackend`]'s [`super::OpApplier`] (issue #557): its own raw
/// session, set up exactly like the backend's, and a copy of the installed
/// tables to render ops against.
pub struct ManualApplier {
    raw: tokio_postgres::Client,
    tables: HashMap<String, Table>,
}

impl super::OpApplier for ManualApplier {
    type Error = ManualBackendError;

    /// [`super::Backend::apply`] on this applier's own connection.
    async fn apply(&mut self, op: &Op) -> Result<u64, ManualBackendError> {
        sql::apply_op(&mut self.raw, &self.tables, op)
            .await
            .map_err(|err| match err {
                sql::ApplyOpError::UnknownTable(table) => {
                    ManualBackendError::UnknownTable { table }
                }
                sql::ApplyOpError::Db(err) => ManualBackendError::Db(err),
            })
    }
}

/// The drain audit's tables, triggers and trigger functions (issue #557),
/// created in the instance schema `schema` (already quoted). Two
/// `AFTER` row triggers on the engine's own staging registry: one on a
/// segment's fence being published, which records the batch's bucket count
/// and row count and every `(table, key, op)` in its slot, and one on each
/// `seg_claims` insert, which records which worker claimed which bucket. A
/// retired segment's registry rows are deleted, so the audit keeps its own
/// copy. The seal decides the bucket count and row count in the statement
/// that publishes the fence (issue #598), so the trigger records the
/// registry's own values.
///
/// Each seal is tagged with the burst the harness last announced
/// ([`super::ConcurrentBackend::begin_burst`]). The concurrent runner
/// quiesces between bursts, so every batch sealed after the announcement
/// holds that burst's changes.
fn drain_audit_ddl(schema: &str) -> String {
    format!(
        "create table {schema}.generative_audit_burst (burst int not null); \
         insert into {schema}.generative_audit_burst values (0); \
         create table {schema}.generative_audit_sealed ( \
             seg_seq bigint primary key, burst int not null, \
             bucket_count smallint not null, row_count bigint not null); \
         create table {schema}.generative_audit_keys ( \
             seg_seq bigint not null, burst int not null, \
             src_table text not null, key text not null, op text not null); \
         create table {schema}.generative_audit_claims ( \
             seg_seq bigint not null, bucket smallint not null, claimed_by text not null); \
         create function {schema}.generative_audit_on_seal() returns trigger \
         language plpgsql as $audit$ \
         declare \
             current_burst int; \
             ring text := '{schema}.' || quote_ident('seg_' || new.ring_slot); \
         begin \
             select burst into current_burst from {schema}.generative_audit_burst; \
             insert into {schema}.generative_audit_sealed \
                 values (new.seg_seq, current_burst, new.bucket_count, new.row_count); \
             execute format('insert into {schema}.generative_audit_keys \
                 select distinct $1, $2, src_table, key, op from %s', ring) \
                 using new.seg_seq, current_burst; \
             return null; \
         end $audit$; \
         create trigger generative_audit_on_seal after update of fence_snapshot on {schema}.segments \
             for each row when (old.fence_snapshot is null and new.fence_snapshot is not null) \
             execute function {schema}.generative_audit_on_seal(); \
         create function {schema}.generative_audit_on_claim() returns trigger \
         language plpgsql as $audit$ \
         begin \
             insert into {schema}.generative_audit_claims \
                 values (new.seg_seq, new.bucket, new.claimed_by); \
             return null; \
         end $audit$; \
         create trigger generative_audit_on_claim after insert on {schema}.seg_claims \
             for each row execute function {schema}.generative_audit_on_claim();"
    )
}

impl super::ConcurrentBackend for ManualBackend {
    type Applier = ManualApplier;

    async fn applier(&self) -> Result<ManualApplier, ManualBackendError> {
        Ok(ManualApplier {
            raw: connect_raw(&self.config).await?,
            tables: self.tables.clone(),
        })
    }

    async fn start_drain_audit(&mut self) -> Result<(), ManualBackendError> {
        let schema = quote_ident(self.config.schema());
        self.raw.batch_execute(&drain_audit_ddl(&schema)).await?;
        Ok(())
    }

    async fn begin_burst(&mut self, burst: usize) -> Result<(), ManualBackendError> {
        let burst = i32::try_from(burst).expect("a run has fewer than 2^31 bursts");
        self.raw
            .execute("update generative_audit_burst set burst = $1", &[&burst])
            .await?;
        Ok(())
    }

    async fn drain_audit(&mut self) -> Result<super::DrainAudit, ManualBackendError> {
        let row = self
            .raw
            .query_one(
                "select \
                   (select count(*) from generative_audit_sealed), \
                   (select count(*) from generative_audit_sealed where bucket_count > 1), \
                   (select count(*) from ( \
                       select s.seg_seq from generative_audit_sealed s \
                       join generative_audit_claims c using (seg_seq) \
                       where s.bucket_count > 1 \
                       group by s.seg_seq \
                       having count(distinct c.claimed_by) >= 2) split), \
                   (select coalesce(max(n), 0) from ( \
                       select count(distinct claimed_by) as n from generative_audit_claims \
                       group by seg_seq) workers), \
                   (select coalesce(max(row_count), 0) from generative_audit_sealed), \
                   (select count(*) from ( \
                       select 1 from generative_audit_keys \
                       group by burst, src_table, key \
                       having count(distinct seg_seq) >= 2) keys)",
                &[],
            )
            .await?;
        let count = |i: usize| row.get::<_, i64>(i) as u64;
        Ok(super::DrainAudit {
            sealed: count(0),
            split: count(1),
            split_across_workers: count(2),
            max_workers_per_batch: count(3),
            max_rows_per_batch: count(4),
            keys_in_several_batches: count(5),
        })
    }

    async fn act(
        &mut self,
        program: &Program,
        action: &BurstAction,
    ) -> Result<(), ManualBackendError> {
        if let BurstAction::Install { def } = action {
            // The same front door as an up-front install: the relationships
            // and tables went in with the first `install`.
            let def = program.defs[*def].clone();
            return super::Backend::install(
                self,
                &Program {
                    tables: Vec::new(),
                    relationships: Vec::new(),
                    defs: vec![def],
                    def_install_after_op: vec![0],
                    ops: Vec::new(),
                    restart_after_ops: Vec::new(),
                    scale_out_after_ops: Vec::new(),
                },
            )
            .await;
        }
        if self.operator.is_none() {
            self.operator = Some(
                trellis::Trellis::connect(self.config.clone(), trellis::TrellisOptions::default())
                    .await?,
            );
        }
        let operator = self.operator.as_ref().expect("connected just above");
        let address = |target: &str, column: &Option<String>| match column {
            Some(column) => format!("{target}.{column}"),
            None => target.to_string(),
        };
        match action {
            BurstAction::RequestBackfill { table } => operator.request_backfill(table).await?,
            BurstAction::Pause { target, column } => {
                operator
                    .apply(&format!("PAUSE TRANSFORM {}", address(target, column)))
                    .await?;
            }
            BurstAction::Resume { target, column } => {
                operator
                    .apply(&format!("RESUME TRANSFORM {}", address(target, column)))
                    .await?;
            }
            BurstAction::Install { .. } => unreachable!("handled above"),
        }
        Ok(())
    }
}
