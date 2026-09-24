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
//! (`docs/data-flow.md#ingestion-via-logical-replication`).
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
    StagingError, has_pending as staging_has_pending, retire_drained_segments, seal_phase1,
    seal_phase2,
};
use trellis::{Client as EngineClient, ClientError, ClientOptions, Config, IntakeError, Pool};

use super::Snapshot;
use super::sql::{self, quote_ident};
use crate::model::{
    Column, NoiseAction, NoiseEvent, NoiseEventKind, Op, Program, SlotLossKind, Table,
};

/// How long [`ManualBackend::quiesce`] waits for convergence before giving
/// up. Generous: this backend targets correctness, not latency, and a
/// genuinely stuck pipeline is exactly what should time out loudly rather
/// than hang the test suite forever.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

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
/// shapes it can reach a fresh [`EngineClient::start_with_config`] call
/// through: directly (`setup_staging`'s own producer session, inside
/// `ClientError::Staging`) or via intake's separate internal session
/// (`ClientError::Intake(IntakeError::Staging(..))`, opened after the first
/// session already dropped — see `trellis::client`'s `setup_staging` doc
/// comment). Every other `ClientError` variant is a real failure `restart`
/// should surface immediately, not retry.
fn is_producer_already_running(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Staging(StagingError::ProducerAlreadyRunning)
            | ClientError::Intake(IntakeError::Staging(StagingError::ProducerAlreadyRunning))
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
    /// Issue #432: [`ManualBackend::quiesce`] ran out of [`QUIESCE_TIMEOUT`]
    /// with a `pending_backfill` (catch-up) marker still undischarged for
    /// each of `tables`. A definition's go-live parks one, and its
    /// enumeration can still re-derive a target that is already `live`.
    PendingBackfillTimeout {
        tables: Vec<String>,
        waited: Duration,
    },
    /// Issue #236: [`ManualBackend::stop_engine`] shut the engine down, but
    /// the server still reported its replication slot active after
    /// `waited`, so the slot can't be dropped or invalidated yet.
    SlotStillActive {
        slot: String,
        waited: Duration,
    },
    /// Issue #236: [`ManualBackend::lose_slot`] couldn't get the server to
    /// invalidate the slot within its budget. `wal_status` is the last value
    /// observed.
    SlotNotInvalidated {
        slot: String,
        wal_status: Option<String>,
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
            sql::QuiesceError::BackfillsPending { tables, waited } => {
                ManualBackendError::PendingBackfillTimeout { tables, waited }
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
    /// Overrides `ClientOptions::default()`'s shared `"trellis_slot"`/
    /// `"trellis_pub"` literals for the primary `install`-started
    /// `EngineClient`, when set via [`ManualBackend::set_slot_and_publication`]
    /// before the first [`ManualBackend::install`] call. `None` (every
    /// existing caller) keeps today's shared-literal behavior. See issue
    /// #188: a logical replication slot name is unique cluster-wide, not
    /// scoped per database, so two `ManualBackend`s against different
    /// isolated databases on the *same* shared Postgres cluster (e.g.
    /// `generative/tests/convergence.rs`'s thread-local `TestCluster`) must
    /// not both install against the literal default name.
    slot_and_publication: Option<(String, String)>,
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
            slot_and_publication: None,
            scale_out_clients: Vec::new(),
            tables: HashMap::new(),
            defs: Vec::new(),
            application_threads,
            maintenance_interval: maintenance_interval
                .unwrap_or_else(|| ClientOptions::default().maintenance_interval),
            target_schema,
            config,
        })
    }

    /// Overrides the slot/publication names [`ManualBackend::install`] uses
    /// to start the primary `EngineClient`, instead of
    /// `ClientOptions::default()`'s shared `"trellis_slot"`/`"trellis_pub"`
    /// literals. Must be called before the first `install` (which is the
    /// only call that starts the primary client — see
    /// [`super::Backend::install`]'s impl below); a client already started
    /// ignores a later call.
    ///
    /// Issue #188: a logical replication slot name is unique cluster-wide,
    /// not scoped per database, even though the slot itself is tied to one
    /// database. A caller driving multiple `ManualBackend`s against
    /// separate isolated databases on the *same* shared Postgres cluster
    /// (e.g. `generative/tests/convergence.rs`'s thread-local `TestCluster`,
    /// one per proptest case) must give each a distinct slot/publication
    /// name, or a later case can collide with an earlier case's slot that
    /// hasn't actually been torn down yet (`trellis::Client::drop` is a
    /// best-effort shutdown signal, not a synchronous join — see its own
    /// doc comment) and get misdiagnosed by
    /// the engine's own `intake::publication::create_slot_and_park_markers` as an
    /// orphaned slot.
    pub fn set_slot_and_publication(
        &mut self,
        slot: impl Into<String>,
        publication: impl Into<String>,
    ) {
        self.slot_and_publication = Some((slot.into(), publication.into()));
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
    /// poll for "the op I just committed has actually reached the ring"
    /// before calling [`ManualBackend::force_seal_active_segment`] — sealing
    /// before intake has consumed the change just seals an empty (or stale)
    /// active segment, silently defeating the deterministic seal-boundary
    /// reproduction the caller is trying to build.
    ///
    /// Not a substitute for [`ManualBackend::quiesce`]: this only reports
    /// whether intake has *staged* something, not whether it has been
    /// applied/drained — exactly the distinction #138's "intake-lag window"
    /// scenario needs a caller to be able to observe.
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
    /// Callers **must** first confirm the change they mean to seal has
    /// actually reached the ring (poll [`ManualBackend::has_pending`]) —
    /// see that method's doc comment.
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
        // Issue #271: `seal_phase2` now `pg_notify`s the wake channel the
        // instant it publishes the fence. Every caller here has always used
        // `ClientOptions::default()`'s wake channel (never overridden by
        // this backend), so fall back to that default when no engine client
        // has started yet to remember its own options.
        let wake_channel = self
            .client_options
            .as_ref()
            .map(|options| options.wake_channel.clone())
            .unwrap_or_else(|| ClientOptions::default().wake_channel);
        seal_phase2(&self.raw, outcome.sealed_seg_seq, &wake_channel).await?;
        Ok(outcome.sealed_seg_seq)
    }

    /// Delegates to the shared [`sql::create_source_table`] — see that
    /// function's doc comment for the exact DDL shape (`crate::model::PRIMARY_KEY_VALUE_TYPE`
    /// pk, per-column `UNIQUE`, unconditional `REPLICA IDENTITY FULL`).
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
        // callers use — it creates the target table, then tries the fast,
        // set-based direct build first and falls back to the ring-based
        // `create_definition` only for a shape the direct build can't render
        // yet (`BackfillError::Unsupported`, folded into `CatalogError` —
        // see its doc comment). Routing the fuzz harness through it, instead
        // of hand-rolling the same create-table/backfill/persist sequence,
        // keeps this backend exercising the exact path production traffic
        // takes.
        // Issue #234: `self.target_schema`, not a hardcoded `"public"` — see
        // that field's doc comment. `connect`/`connect_with_workers`/
        // `connect_with_options` all still resolve it to exactly what a
        // literal `"public"` used to mean, so no pre-#234 caller changes.
        install_definition(&self.pool, &text, &source_columns, &self.target_schema).await?;
        Ok(())
    }

    /// Creates a table with the exact same DDL shape [`ManualBackend::install`]
    /// gives a real source table (task E1: untracked-object noise) —
    /// including the same unconditional `replica identity full` (harmless,
    /// and keeps this table indistinguishable from a real one at the DDL
    /// level) — but never registers it in `self.tables`, and no definition
    /// reads it, so the engine never publishes it (the staging worker
    /// publishes exactly the tables registered definitions read, issue
    /// #427). Those are the only ways a table is "tracked" by this backend
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

/// How long [`ManualBackend::stop_engine`] waits for the server to release
/// the replication slot after the engine shut down.
const SLOT_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`ManualBackend::lose_slot`] keeps generating WAL and
/// checkpointing before giving up on getting the slot invalidated.
const SLOT_INVALIDATION_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// The replication slot the primary engine client streams from.
    fn slot_name(&self) -> Result<String, ManualBackendError> {
        self.client_options
            .as_ref()
            .map(|options| options.slot.clone())
            .ok_or(ManualBackendError::NoClientStarted)
    }

    /// Replaces the backend's raw session and waits for its pool to work
    /// again, after a Postgres restart severed both. The engine client is
    /// left alone: reconnecting is its own job, and that is what a
    /// restart action tests.
    pub async fn reconnect_after_server_restart(&mut self) -> Result<(), ManualBackendError> {
        self.raw = connect_raw(&self.config).await?;
        await_pool_usable(&self.pool).await
    }

    /// Shuts the primary engine client down and waits for the server to
    /// release its replication slot, so the slot can be dropped or
    /// invalidated. The walsender can outlive the client's disconnect for a
    /// moment, which is why this polls. [`Self::start_engine`] brings an
    /// equivalent client back.
    pub async fn stop_engine(&mut self) -> Result<(), ManualBackendError> {
        let slot = self.slot_name()?;
        if let Some(client) = self.engine_client.take() {
            client.shutdown().await?;
        }
        let started = std::time::Instant::now();
        loop {
            let active: i64 = self
                .raw
                .query_one(
                    "select count(*) from pg_replication_slots where slot_name = $1 and active",
                    &[&slot],
                )
                .await?
                .get(0);
            if active == 0 {
                return Ok(());
            }
            let waited = started.elapsed();
            if waited >= SLOT_RELEASE_TIMEOUT {
                return Err(ManualBackendError::SlotStillActive { slot, waited });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

    /// Loses the primary client's replication slot, which must not be in use
    /// (call [`Self::stop_engine`] first).
    ///
    /// [`SlotLossKind::Invalidated`] gets the server to invalidate the slot
    /// for real rather than faking the catalog: it drops
    /// `max_slot_wal_keep_size` to zero, writes WAL past it with
    /// non-transactional logical messages (no table, so nothing a
    /// publication could carry), switches segments and checkpoints until the
    /// slot reads `wal_status = 'lost'`. The setting is cluster-wide, so it
    /// is put back before this returns, whether or not the invalidation
    /// took. Any other slot on the cluster that lags gets invalidated
    /// too; on the generative suite's clusters those are only earlier cases'
    /// leftovers.
    pub async fn lose_slot(&self, kind: SlotLossKind) -> Result<(), ManualBackendError> {
        let slot = self.slot_name()?;
        match kind {
            SlotLossKind::Dropped => {
                self.raw
                    .execute("select pg_drop_replication_slot($1)", &[&slot])
                    .await?;
                Ok(())
            }
            SlotLossKind::Invalidated => {
                // `ALTER SYSTEM` refuses to run in a transaction block, so
                // each statement goes on its own. The reset is attempted
                // whatever happened before it, a failed reload after the
                // `set` included: `ALTER SYSTEM` persists in
                // `postgresql.auto.conf`, so a skipped reset would keep the
                // cap at zero for every later case on this cluster, across
                // restarts too. The first error wins.
                let invalidated = async {
                    self.raw
                        .execute("alter system set max_slot_wal_keep_size = '0'", &[])
                        .await?;
                    self.raw.execute("select pg_reload_conf()", &[]).await?;
                    self.invalidate_slot(&slot).await
                }
                .await;
                let reset = async {
                    self.raw
                        .execute("alter system reset max_slot_wal_keep_size", &[])
                        .await?;
                    self.raw.execute("select pg_reload_conf()", &[]).await?;
                    Ok::<(), ManualBackendError>(())
                }
                .await;
                invalidated.and(reset)
            }
        }
    }

    /// [`Self::lose_slot`]'s invalidation loop, run while
    /// `max_slot_wal_keep_size` is zero. Invalidation is decided at
    /// checkpoint time, so each round writes WAL, switches segments and
    /// checkpoints, then looks.
    async fn invalidate_slot(&self, slot: &str) -> Result<(), ManualBackendError> {
        let started = std::time::Instant::now();
        loop {
            self.raw
                .batch_execute(
                    "select pg_logical_emit_message(false, 'generative', repeat('x', 1048576)) \
                     from generate_series(1, 20); \
                     select pg_switch_wal(); \
                     checkpoint;",
                )
                .await?;
            let wal_status: Option<String> = self
                .raw
                .query_opt(
                    "select wal_status from pg_replication_slots where slot_name = $1",
                    &[&slot],
                )
                .await?
                .and_then(|row| row.get(0));
            if wal_status.as_deref() == Some("lost") {
                return Ok(());
            }
            if started.elapsed() >= SLOT_INVALIDATION_TIMEOUT {
                return Err(ManualBackendError::SlotNotInvalidated {
                    slot: slot.to_string(),
                    wal_status,
                });
            }
        }
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
        let facade = trellis::Trellis::connect(self.config.clone(), Default::default()).await?;
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

        // The staging worker publishes whatever the definitions just
        // registered read, straight from the catalog (issue #427).
        if !program.tables.is_empty() && self.engine_client.is_none() {
            let mut options = ClientOptions {
                staging_worker: true,
                application_threads: self.application_threads,
                maintenance_interval: self.maintenance_interval,
                ..Default::default()
            };
            // Issue #188: a caller that needs to coexist with other
            // `ManualBackend`s on the same shared Postgres cluster (see
            // `set_slot_and_publication`'s doc comment) overrides the
            // otherwise-shared default slot/publication names here, at the
            // one place the primary client actually starts.
            if let Some((slot, publication)) = &self.slot_and_publication {
                options.slot = slot.clone();
                options.publication = publication.clone();
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
    /// identical layer-1 reason; `SubprocessBackend` doesn't need layer 2
    /// because waiting on the real OS process's exit status is a stronger,
    /// synchronous guarantee than anything an in-process `Drop`/`shutdown`
    /// pair can give.
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
