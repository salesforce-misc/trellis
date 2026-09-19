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
use trellis::config::DEFAULT_SCHEMA;
use trellis::dev::defs::ast::{KeySpace, TransformDef, ValueType};
use trellis::dev::defs::{
    CatalogError, DdlError, create_relationship, install_definition, qualified_target_table,
    require_single_column_pk, source_primary_key,
};
use trellis::dev::staging::{
    StagingError, await_converged, has_pending as staging_has_pending, seal_phase1, seal_phase2,
    watermark_token,
};
use trellis::{Client as EngineClient, ClientError, ClientOptions, Config, Pool};

use super::Snapshot;
use super::sql::{self, quote_ident};
use crate::model::{Column, NoiseAction, NoiseEvent, NoiseEventKind, Op, Program, Table};

/// How long [`ManualBackend::quiesce`] waits for convergence before giving
/// up. Generous: this backend targets correctness, not latency, and a
/// genuinely stuck pipeline is exactly what should time out loudly rather
/// than hang the test suite forever.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

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
    Config(trellis::Error),
    Client(ClientError),
    Catalog(CatalogError),
    Ddl(DdlError),
    Staging(StagingError),
    Db(tokio_postgres::Error),
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
    dsn: String,
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
        if dsn.trim().is_empty() {
            return Err(ManualBackendError::UnnamedTarget);
        }
        println!(
            "generative: connecting ManualBackend to {dsn} ({application_threads} application \
             worker(s))"
        );
        let config = Config::from_dsn(dsn.clone())?;
        let pool = Pool::new(&config)?;

        let (raw, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!("set search_path to {}, public", config.schema()))
            .await?;

        Ok(Self {
            dsn,
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
    /// `trellis::dev::intake::publication::initial_snapshot_handshake` as an
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
    /// `trellis::dev::staging::claim`'s partition decision (`segments.bucket_count`,
    /// fixed at seal time from row count alone — see
    /// `trellis::dev::staging::claim::MIN_ROWS_TO_SPLIT`/`SEG_BUCKETS`). `0` if no
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
    /// This module is the one place the backend seam allows direct access
    /// to `trellis::staging`'s seal machinery — see the module doc comment
    /// and [`ManualBackend::max_bucket_count`]'s own precedent for the same
    /// door.
    pub async fn force_seal_active_segment(&mut self) -> Result<i64, ManualBackendError> {
        let outcome = seal_phase1(&mut self.raw).await?;
        seal_phase2(&self.raw, outcome.sealed_seg_seq).await?;
        Ok(outcome.sealed_seg_seq)
    }

    /// Delegates to the shared [`sql::create_source_table`] — see that
    /// function's doc comment for the exact DDL shape (`PRIMARY_KEY_PG_TYPE`
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
        install_definition(&self.pool, &text, &source_columns, "public").await?;
        Ok(())
    }

    /// Creates a table with the exact same DDL shape [`ManualBackend::install`]
    /// gives a real source table (task E1: untracked-object noise) —
    /// including the same unconditional `replica identity full` (harmless,
    /// and keeps this table indistinguishable from a real one at the DDL
    /// level) — but never registers it in `self.tables` and never hands its
    /// name to the engine's `ClientOptions.source_tables`. Those are the
    /// only two places a table needs to appear to be "tracked" by this
    /// backend or watched by the engine (see [`ManualBackend::install`]/
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

    /// Polls every installed definition's status
    /// (`transform_definitions.status`) until each has reached a terminal
    /// backfill outcome (`live`/`quarantined`) or `timeout` elapses —
    /// delegates to the shared [`sql::await_definitions_settled`]. See that
    /// function's doc comment for why this closes a real gap in
    /// [`ManualBackend::quiesce`] (public-api-design review): a direct-build
    /// 1-1 definition's backfill runs through `trellis::dev::defs::chunk_queue`'s
    /// durable claim/execute/finish queue entirely outside the ring
    /// (docs/decisions/0007's "Backgrounding and resumability" amendment) —
    /// `await_converged`'s CDC-ring convergence wait has no visibility into
    /// that queue at all.
    async fn await_definitions_settled(&self, timeout: Duration) -> Result<(), ManualBackendError> {
        sql::await_definitions_settled(&self.raw, &self.defs, timeout)
            .await
            // Both arms match pre-#166 `ManualBackend` behavior exactly: a
            // query failure was, and still is, an ordinary `Db` error on
            // `quiesce`'s `Result` — not a panic.
            .map_err(|err| match err {
                sql::DefinitionSettleError::Db(err) => ManualBackendError::Db(err),
                sql::DefinitionSettleError::Timeout { unsettled, waited } => {
                    ManualBackendError::DefinitionSettleTimeout { unsettled, waited }
                }
            })
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

        let source_tables: Vec<String> = program
            .tables
            .iter()
            .map(|t| format!("{DEFAULT_SCHEMA}.{}", t.name))
            .collect();
        if !source_tables.is_empty() && self.engine_client.is_none() {
            let mut options = ClientOptions {
                staging_worker: true,
                application_threads: self.application_threads,
                source_tables,
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
            let client = EngineClient::start(self.dsn.clone(), options.clone())?;
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
        // around the watermark -> converge round trip, to test the
        // hypothesis (see `local_docs/generative-suite-improvement-plan.md`)
        // that the convergence property's wall-clock variance is caused by
        // the same ~10s seal age-gate stall suspected in
        // `local_docs/transit-comparison.md` §3.3. Silent and free unless
        // `GENERATIVE_QUIESCE_TIMING` is set: the env lookup happens once per
        // call so the default (unset) path pays exactly one `var_os` check
        // and no clock reads.
        let timing_enabled = std::env::var_os("GENERATIVE_QUIESCE_TIMING").is_some();
        let start = timing_enabled.then(std::time::Instant::now);

        let token = watermark_token(&self.raw).await?;
        let result = await_converged(&self.raw, token, QUIESCE_TIMEOUT).await;

        if let Some(start) = start {
            eprintln!("QUIESCE_TIMING {}", start.elapsed().as_millis());
        }
        result?;

        // public-api-design review gap: ring convergence alone says nothing
        // about a still-backgrounded direct-build backfill (docs/decisions/0007's
        // amendment) — see `await_definitions_settled`'s own doc comment for
        // why this second wait is load-bearing, not redundant.
        self.await_definitions_settled(QUIESCE_TIMEOUT).await?;

        Ok(())
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
            let qualified = qualified_target_table("public", def);
            let rows = match &def.key_space {
                KeySpace::OneToOne => {
                    // A `KeySpace::OneToOne` target's primary key is always a
                    // single column (`ddl::require_single_column_pk`'s own
                    // doc comment) — `source_primary_key` itself now accepts
                    // an arbitrary-arity source primary key (issue #126), so
                    // narrow it back down here the same way
                    // `catalog::install_definition` does at definition time.
                    let pk = require_single_column_pk(
                        source_primary_key(&self.pool, &def.source).await?,
                        &def.source,
                    )?;
                    let target_columns: Vec<Column> = std::iter::once(Column {
                        name: pk.name.clone(),
                        value_type: ValueType::Numeric,
                    })
                    .chain(def.fields.iter().map(|f| Column {
                        name: f.name.clone(),
                        // The physical column type doesn't matter for a
                        // `::text` read; only the name is used below.
                        value_type: ValueType::Text,
                    }))
                    .collect();
                    sql::read_table(&self.raw, &qualified, &pk.name, &target_columns).await?
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

    /// Improvement-plan task E3: drops the current primary `trellis::Client`
    /// (its own `Drop` impl fires here — best-effort shutdown signal, no
    /// draining, no join: see the `Backend::restart` doc comment) and starts
    /// a fresh one against the same dsn/options [`ManualBackend::install`]
    /// remembered. The ring is durable Postgres state untouched by any of
    /// this, so the new client resumes exactly where the old one left off.
    async fn restart(&mut self) -> Result<(), ManualBackendError> {
        let options = self
            .client_options
            .clone()
            .ok_or(ManualBackendError::NoClientStarted)?;
        // Dropping the old value here — before starting the replacement —
        // is what fires `trellis::Client`'s `Drop` impl (the crash stand-in);
        // reassigning below wouldn't run it any differently, but doing it as
        // its own statement keeps the "crash, then restart" sequencing
        // explicit rather than implicit in the assignment.
        self.engine_client = None;
        let client = EngineClient::start(self.dsn.clone(), options)?;
        self.engine_client = Some(client);
        Ok(())
    }

    /// Improvement-plan task E3: starts an additional, application-worker-only
    /// (`staging_worker: false`) `trellis::Client` against the same dsn,
    /// alongside whatever primary client `install` already started —
    /// confirming multiple clients can coexist draining the same ring (the
    /// module doc comment on `trellis::Client` claims this is supported; this
    /// is where the generative suite exercises that claim). Never touches
    /// `source_tables`: an application-only client doesn't consult it (see
    /// `ClientOptions::source_tables`'s own doc comment), so this needs no
    /// state beyond the dsn.
    async fn scale_out(&mut self) -> Result<(), ManualBackendError> {
        let options = ClientOptions {
            staging_worker: false,
            application_threads: 1,
            ..Default::default()
        };
        let client = EngineClient::start(self.dsn.clone(), options)?;
        self.scale_out_clients.push(client);
        Ok(())
    }
}
