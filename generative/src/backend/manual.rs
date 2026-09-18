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
//! rendering logic (`render_definition`/`render_expr`/`read_table`/
//! `read_aggregate_table`, none of which is worker-count-dependent) for zero
//! behavioral difference; a parameterized constructor shares all of it and
//! keeps exactly one implementation of the seam's DDL/DML/read-back logic to
//! maintain. See `generative/tests/concurrent_convergence.rs` for the new,
//! separate property/test file this constructor is meant to be driven from.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use tokio_postgres::NoTls;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    CatalogError, DdlError, TransformStatus, create_relationship, install_definition,
    qualified_target_table, require_single_column_pk, source_primary_key,
};
use trellis::staging::{
    StagingError, await_converged, has_pending as staging_has_pending, seal_phase1, seal_phase2,
    watermark_token,
};
use trellis::{Client as EngineClient, ClientError, ClientOptions, Config, Pool};

use super::Snapshot;
use crate::model::{
    Column, NoiseAction, NoiseEvent, NoiseEventKind, Op, PRIMARY_KEY_PG_TYPE, Program,
    Relationship, Table, group_key,
};

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

/// Quotes a Postgres identifier for safe interpolation into SQL text,
/// mirroring `trellis::pool`'s own (crate-private) helper of the same name.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The Postgres type name a [`ValueType`] casts to.
fn pg_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
    }
}

/// Renders `def` back to the concrete `TRANSFORM ... FROM ... SELECT ...`
/// syntax [`create_definition`] parses — the manual backend's only reason
/// to exist, since [`crate::model::Program`] stores the parsed AST
/// directly rather than source text. [`KeySpace::OneToOne`] and (as of
/// improvement-plan task B4) [`KeySpace::Aggregate`] are both supported,
/// matching `trellis::defs::parser`'s own `GROUP BY <cols>` clause, which sits
/// directly after `FROM <source>` and before `SELECT` (ADR-0004's reserved
/// slot).
fn render_definition(def: &TransformDef) -> Result<String, ManualBackendError> {
    let fields: Vec<String> = def
        .fields
        .iter()
        .map(|field| format!("{} AS {}", render_expr(&field.expr), field.name))
        .collect();
    debug_assert_eq!(def.predicate, Predicate::True);
    let key_space_clause = match &def.key_space {
        KeySpace::OneToOne => String::new(),
        KeySpace::Aggregate { group_by } => format!(
            " GROUP BY {}",
            group_by
                .iter()
                .map(|k| k.target_column_name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    Ok(format!(
        "TRANSFORM {} FROM {}{key_space_clause} SELECT {}",
        def.target,
        def.source,
        fields.join(", ")
    ))
}

/// Renders a generator-built [`Expr`] back to the source text
/// [`super::install_definition`]/`trellis::defs::parser::parse` re-parses.
///
/// A `BinaryOp`'s operands are *unconditionally* parenthesized (issue #67's
/// reviewer follow-up), not only when the operand is itself a lower-
/// precedence `BinaryOp`: with real operator precedence now in the parser
/// (`trellis::defs::registry::OPERATORS`), a flat render like `a + b > c`
/// silently reconstructs a *different* tree than a nested one the generator
/// might build — e.g. `Add(a, GreaterThan(b, c))` would round-trip as
/// `a + b > c`, which `+`'s tighter binding re-parses as `Add(a,b) >
/// c` — the wrong tree. Always parenthesizing every operand (`(a) + (b > c)`)
/// is simpler than computing whether a given operand's own precedence
/// requires it, and correct regardless of what tree the generator composes,
/// so it's the shape this renderer commits to before the generator ever
/// nests `+` and `>` together (improvement-plan task B2). See
/// `tests::render_expr_parenthesizes_nested_binary_ops_so_they_round_trip`
/// for the regression pin, and `trellis::defs::parser`'s grouping-paren
/// support (issue #67 follow-up) that makes the rendered text re-parseable
/// at all.
fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
        Expr::BinaryOp { op, lhs, rhs } => {
            format!(
                "({}) {} ({})",
                render_expr(lhs),
                render_operator(*op),
                render_expr(rhs)
            )
        }
        // `COUNT(*)` (task B4): the AST carries no argument for this shape
        // (`args` is empty) — `trellis::defs::parser` only ever accepts the
        // literal `*` here, not an empty argument list, so this must render
        // it back explicitly rather than falling through to the generic
        // `name(args)` arm below (which would emit the invalid `COUNT()`).
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "COUNT(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let args: Vec<String> = args.iter().map(render_expr).collect();
            format!("{name}({})", args.join(", "))
        }
        // Issue #34: a `<rel>.<column>` path renders back to exactly the
        // concrete syntax ADR-0006 specifies — the *relationship* name as
        // the head, never a table name or alias. The parser resolves an
        // `ident.ident` in expression position straight to
        // `Expr::RelationshipPath`, so this round-trips.
        Expr::RelationshipPath { rel, column } => format!("{rel}.{column}"),
    }
}

/// Renders a [`Relationship`] back to the concrete
/// `RELATIONSHIP <name> FROM <table>.<col> TO <table>.<col>` syntax
/// [`create_relationship`] parses (ADR-0006) — the relationship analog of
/// [`render_definition`], for the same reason: [`crate::model::Program`]
/// stores plain data, and the engine's front door takes source text.
///
/// Cardinality is deliberately **not** rendered: ADR-0006's grammar has no
/// cardinality keyword, because the engine derives it by introspecting
/// whether the to-side column is provably unique. The model's recorded
/// [`crate::model::Cardinality`] is the generator's claim about what that
/// introspection will conclude; this is the point where the engine gets to
/// disagree, and a disagreement surfaces as a definition-time rejection
/// (a hard failure, never a skip).
fn render_relationship(rel: &Relationship) -> String {
    format!(
        "RELATIONSHIP {} FROM {}.{} TO {}.{}",
        rel.name, rel.from_table, rel.from_col, rel.to_table, rel.to_col
    )
}

fn render_operator(op: Operator) -> &'static str {
    match op {
        Operator::Add => "+",
        Operator::GreaterThan => ">",
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
            pg_type_name(*value_type),
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
/// needs no type dispatch of its own the way [`Assignment`]'s real-op
/// rendering does with its explicit `::text::<type>` casts.
fn noise_sql_literal(value: &Option<String>) -> String {
    match value {
        None => "NULL".to_string(),
        Some(text) => format!("'{}'", text.replace('\'', "''")),
    }
}

/// One row's placeholder assignment for an `INSERT`/`UPDATE` statement:
/// `column = $n::type` (or `column` for the column list), plus the bound
/// text value at that position.
struct Assignment {
    fragment: String,
    value: Option<String>,
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
    /// `trellis::intake::publication::initial_snapshot_handshake` as an
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
    /// `trellis::staging::claim`'s partition decision (`segments.bucket_count`,
    /// fixed at seal time from row count alone — see
    /// `trellis::staging::claim::MIN_ROWS_TO_SPLIT`/`SEG_BUCKETS`). `0` if no
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
    /// `trellis::staging::has_pending`, exposed so a caller stressing the
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
    /// `trellis::staging::seal_phase1`/`seal_phase2`, the same two-phase
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

    async fn create_source_table(&self, table: &Table) -> Result<(), ManualBackendError> {
        let mut sql = format!("create table {} (", quote_ident(&table.name));
        for (i, column) in table.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(&column.name));
            sql.push(' ');
            if column.name == table.pk_col {
                // The PK's declared type comes from `PRIMARY_KEY_PG_TYPE`,
                // never from its `ValueType` — see that constant's doc
                // comment for why a `numeric` PK is rejected at install time.
                sql.push_str(PRIMARY_KEY_PG_TYPE);
                sql.push_str(" primary key");
            } else {
                sql.push_str(pg_type_name(column.value_type));
                if table.unique_cols.iter().any(|c| c == &column.name) {
                    // Issue #34: a real single-column UNIQUE constraint,
                    // which is what makes `trellis::defs::catalog`'s live
                    // `pg_catalog` introspection resolve a relationship whose
                    // *to*-side is this column as to-one (ADR-0006's
                    // cardinality rule). A generated to-one relationship is
                    // otherwise rejected as a bare reference to a to-many
                    // relationship.
                    sql.push_str(" unique");
                }
            }
        }
        sql.push(')');
        self.raw.batch_execute(&sql).await?;

        // Improvement-plan task B4: a `KeySpace::Aggregate` definition needs
        // a changed row's *old* image to know which group a deleted/
        // re-parented row is leaving (`trellis::intake::replica_identity`'s
        // `needs_old_image`), and the engine checks this eagerly — creating
        // an aggregate definition over a table with only the default replica
        // identity (old image limited to the pk) is rejected outright with
        // `CatalogError::ReplicaIdentityRequired`, exactly like
        // `trellis/tests/apply_aggregate.rs`'s own hand-built fixtures always
        // `alter table ... replica identity full` up front. Every table this
        // backend creates gets it unconditionally, rather than only tables
        // an `Aggregate` def happens to source from: it's harmless for a
        // `OneToOne` definition (strictly more WAL detail, never less), and
        // doing it unconditionally means `install` never has to know in
        // advance which of a program's tables end up feeding an aggregate
        // def before any of them are created.
        self.raw
            .batch_execute(&format!(
                "alter table {} replica identity full",
                quote_ident(&table.name)
            ))
            .await?;
        Ok(())
    }

    /// `source_columns` for `table`, as `trellis::defs::install_definition`
    /// wants it.
    fn source_columns(table: &Table) -> HashMap<String, ValueType> {
        table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.value_type))
            .collect()
    }

    async fn install_definition(&mut self, def: &TransformDef) -> Result<(), ManualBackendError> {
        let source_table = self
            .tables
            .get(&def.source)
            .ok_or_else(|| ManualBackendError::UnknownTable {
                table: def.source.clone(),
            })?
            .clone();
        let source_columns = Self::source_columns(&source_table);

        let text = render_definition(def)?;

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

    fn column(&self, table: &str, column: &str) -> Option<&Column> {
        self.tables
            .get(table)
            .and_then(|t| t.columns.iter().find(|c| c.name == column))
    }

    /// The Postgres type `column` was actually declared with in
    /// [`Self::create_source_table`] — the single source of truth every
    /// `$n::text::<type>` cast in this backend renders from.
    ///
    /// A primary-key column resolves to [`PRIMARY_KEY_PG_TYPE`] rather than
    /// to [`pg_type_name`] of its [`ValueType`], because [`ValueType`] cannot
    /// name an integer type and the placeholder it carries (`Numeric`) is not
    /// what the column was declared as. Casting a pk through `numeric` still
    /// *worked* — Postgres assignment-casts `numeric` to `bigint` — but it
    /// round-trips an integer key through an arbitrary-precision type for no
    /// reason, and it is exactly the kind of near-miss that hid the original
    /// `numeric`-pk bug. Routing every site through here keeps the DDL and
    /// the DML casts from drifting apart again.
    fn column_pg_type(&self, table: &str, column: &str) -> &'static str {
        let is_pk = self.tables.get(table).is_some_and(|t| t.pk_col == column);
        if is_pk {
            return PRIMARY_KEY_PG_TYPE;
        }
        pg_type_name(
            self.column(table, column)
                .map(|c| c.value_type)
                .unwrap_or(ValueType::Numeric),
        )
    }

    fn assignment(
        &self,
        table: &str,
        column: &str,
        index: usize,
        value: &Option<String>,
    ) -> Assignment {
        Assignment {
            fragment: format!(
                "{}=${}::text::{}",
                quote_ident(column),
                index,
                self.column_pg_type(table, column)
            ),
            value: value.clone(),
        }
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
    /// backfill outcome — [`TransformStatus::Live`] (the ordinary case) or
    /// [`TransformStatus::Quarantined`] — or `timeout` elapses. Matches
    /// `trellis::staging::await_converged`'s own backoff shape (5ms initial,
    /// doubling to a 250ms ceiling, never resetting within one call) so both
    /// halves of [`ManualBackend::quiesce`] share one polling discipline.
    ///
    /// `Quarantined` stops the wait rather than being treated as "not yet
    /// settled": per `docs/transforms.md`'s "Status" section, a quarantined
    /// transform "is broken and no longer maintained" — it never becomes
    /// `Live` on its own, only by an explicit resume that restarts its
    /// backfill from `waiting_to_backfill`. Waiting past it here would just
    /// hang until `timeout` for no reason; it's as settled as this call can
    /// ever observe it.
    ///
    /// Closes a real gap in [`ManualBackend::quiesce`] (public-api-design
    /// review): a direct-build 1-1 definition's backfill runs through
    /// `trellis::defs::chunk_queue`'s durable claim/execute/finish queue
    /// entirely outside the ring (docs/decisions/0007's "Backgrounding and
    /// resumability" amendment) — `await_converged`'s CDC-ring convergence
    /// wait has no visibility into that queue at all. Before this,
    /// `quiesce` only *appeared* to wait for such a backfill to finish by
    /// accident: a large, unrelated ~10s ring-seal age-gate stall happened
    /// to give drain workers enough real wall-clock time to finish the
    /// suite's small test backfills before the harness ever snapshotted
    /// state. Shortening that stall, or a scenario installing a definition
    /// needing more than one chunk, would have started producing flaky/wrong
    /// convergence results with no real product bug behind them.
    async fn await_definitions_settled(&self, timeout: Duration) -> Result<(), ManualBackendError> {
        const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
        const MAX_BACKOFF: Duration = Duration::from_millis(250);

        let started = std::time::Instant::now();
        let mut backoff = INITIAL_BACKOFF;
        loop {
            let unsettled = self.unsettled_definitions().await?;
            if unsettled.is_empty() {
                return Ok(());
            }
            let waited = started.elapsed();
            if waited >= timeout {
                return Err(ManualBackendError::DefinitionSettleTimeout { unsettled, waited });
            }
            tokio::time::sleep(backoff.min(timeout - waited)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// The target tables of every definition [`ManualBackend::install`] has
    /// installed (`self.defs`) whose current `transform_definitions.status`
    /// is neither `live` nor `quarantined` — i.e. still `waiting_to_backfill`
    /// or `backfilling`. Reads directly against `self.raw` (the same table
    /// `trellis::Trellis::definitions`/`trellis::Trellis::status` query) rather
    /// than through a `Trellis` handle: `ManualBackend` never holds one — it
    /// drives `trellis::defs`/`trellis::Client` directly — so re-running the
    /// same simple by-target-table lookup here is the one query path this
    /// backend already has, not a new one invented for this.
    async fn unsettled_definitions(&self) -> Result<Vec<String>, ManualBackendError> {
        let mut unsettled = Vec::new();
        for def in &self.defs {
            // Issue #73: `target_table` is persisted fully-qualified now,
            // but `def.target` (freshly parsed definition text) is bare —
            // match against `target_table`'s bare table-name suffix, same
            // convention `trellis::app::Trellis::status` itself uses.
            let Some(row) = self
                .raw
                .query_opt(
                    "select status from transform_definitions \
                     where split_part(target_table, '.', 2) = $1",
                    &[&def.target],
                )
                .await?
            else {
                // No row yet for a definition `install_definition` is still
                // in the middle of creating is the same "not settled" case
                // as an explicit non-terminal status — keep waiting rather
                // than treating a momentarily-missing row as vacuously
                // settled.
                unsettled.push(def.target.clone());
                continue;
            };
            let status_text: String = row.get(0);
            let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            });
            if !matches!(status, TransformStatus::Live | TransformStatus::Quarantined) {
                unsettled.push(def.target.clone());
            }
        }
        Ok(unsettled)
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
            create_relationship(&self.pool, &render_relationship(rel)).await?;
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

    async fn apply(&mut self, op: &Op) -> Result<u64, ManualBackendError> {
        let affected = match op {
            Op::Insert { table, row, .. } => {
                let columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
                let assignments: Vec<Assignment> = row
                    .iter()
                    .enumerate()
                    .map(|(i, (col, val))| Assignment {
                        fragment: format!("${}::text::{}", i + 1, self.column_pg_type(table, col)),
                        value: val.clone(),
                    })
                    .collect();
                let column_list = columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = assignments
                    .iter()
                    .map(|a| a.fragment.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "insert into {} ({column_list}) values ({placeholders})",
                    quote_ident(table)
                );
                let params: Vec<Option<String>> =
                    assignments.into_iter().map(|a| a.value).collect();
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                self.raw.execute(&sql, &params).await?
            }
            Op::Update {
                table, pk, changes, ..
            } => {
                let pk_col = self
                    .tables
                    .get(table)
                    .map(|t| t.pk_col.clone())
                    .ok_or_else(|| ManualBackendError::UnknownTable {
                        table: table.clone(),
                    })?;
                let mut assignments = Vec::with_capacity(changes.len());
                for (i, (col, val)) in changes.iter().enumerate() {
                    assignments.push(self.assignment(table, col, i + 1, val));
                }
                let set_clause = assignments
                    .iter()
                    .map(|a| a.fragment.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                // `PRIMARY_KEY_PG_TYPE`, not the pk column's `ValueType`:
                // the cast has to name the type the column was actually
                // declared with in `create_source_table`.
                let sql = format!(
                    "update {} set {set_clause} where {}=${}::text::{}",
                    quote_ident(table),
                    quote_ident(&pk_col),
                    changes.len() + 1,
                    PRIMARY_KEY_PG_TYPE,
                );
                let mut params: Vec<Option<String>> =
                    assignments.into_iter().map(|a| a.value).collect();
                params.push(Some(pk.clone()));
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                self.raw.execute(&sql, &params).await?
            }
            Op::Delete { table, pk, .. } => {
                let pk_col = self
                    .tables
                    .get(table)
                    .map(|t| t.pk_col.clone())
                    .ok_or_else(|| ManualBackendError::UnknownTable {
                        table: table.clone(),
                    })?;
                // Same as the `Update` arm above: the cast names
                // `PRIMARY_KEY_PG_TYPE`, the declared type of the column.
                let sql = format!(
                    "delete from {} where {}=$1::text::{}",
                    quote_ident(table),
                    quote_ident(&pk_col),
                    PRIMARY_KEY_PG_TYPE,
                );
                self.raw.execute(&sql, &[pk]).await?
            }
            Op::Truncate { table, .. } => {
                // Improvement-plan task E6's load-bearing gotcha: Postgres's
                // `TRUNCATE` command tag always reports `0` rows affected,
                // regardless of how many rows actually existed — trusting
                // that raw count into `run_convergence`'s
                // `Ok(0) => AffectsNoRows` classifier would misclassify every
                // non-empty truncate as a no-op, defeating "operation errors
                // are checked, not swallowed" for this op entirely. So the
                // real row count is synthesized here instead: a `SELECT
                // count(*)` in the *same transaction* as the `TRUNCATE`,
                // taken before it runs, so nothing can slip a concurrent
                // write in between the count and the clear (moot for this
                // single-threaded harness, but it's the honest way to make
                // "the count reflects what actually got cleared" true by
                // construction rather than by accident of timing).
                let quoted = quote_ident(table);
                let txn = self.raw.transaction().await?;
                let count_row = txn
                    .query_one(&format!("select count(*) from {quoted}"), &[])
                    .await?;
                let count: i64 = count_row.get(0);
                txn.batch_execute(&format!("truncate table {quoted}"))
                    .await?;
                txn.commit().await?;
                count as u64
            }
            Op::BulkInsert { table, rows, .. } => {
                let Some(first_row) = rows.first() else {
                    // An empty `rows` is a generator bug (there is no valid
                    // SQL "insert zero rows" via a VALUES list) — not a
                    // condition this backend should paper over by silently
                    // doing nothing.
                    panic!(
                        "ManualBackend::apply: Op::BulkInsert against table {table:?} carries no \
                         rows — a generator bug"
                    );
                };
                let columns: Vec<&str> = first_row.iter().map(|(c, _)| c.as_str()).collect();
                let column_list = columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");

                let mut placeholder_groups = Vec::with_capacity(rows.len());
                let mut params: Vec<Option<String>> =
                    Vec::with_capacity(rows.len() * columns.len());
                for row in rows {
                    let row_columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
                    assert_eq!(
                        row_columns, columns,
                        "ManualBackend::apply: every Op::BulkInsert row must carry the same \
                         columns in the same order as the first row — a generator bug (table \
                         {table:?})"
                    );
                    let placeholders: Vec<String> = row
                        .iter()
                        .map(|(col, val)| {
                            params.push(val.clone());
                            format!(
                                "${}::text::{}",
                                params.len(),
                                self.column_pg_type(table, col)
                            )
                        })
                        .collect();
                    placeholder_groups.push(format!("({})", placeholders.join(", ")));
                }

                let sql = format!(
                    "insert into {} ({column_list}) values {}",
                    quote_ident(table),
                    placeholder_groups.join(", ")
                );
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                self.raw.execute(&sql, &params).await?
            }
        };
        Ok(affected)
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
            let rows = read_table(
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
                    read_table(&self.raw, &qualified, &pk.name, &target_columns).await?
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
                    read_aggregate_table(&self.raw, &qualified, &group_by_names, &def.fields)
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

/// Reads `qualified_table` back as text, ordered by `pk_col`, into `pk ->
/// column -> value`.
async fn read_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    pk_col: &str,
    columns: &[Column],
) -> Result<BTreeMap<String, BTreeMap<String, Option<String>>>, ManualBackendError> {
    let select_list = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select {select_list} from {qualified_table} order by {}",
        quote_ident(pk_col)
    );
    let rows = client.query(&sql, &[]).await?;

    let mut result = BTreeMap::new();
    for row in rows {
        let mut by_column = BTreeMap::new();
        let pk_value: Option<String> = row.get(0);
        let pk_value = pk_value.expect("primary key column is never NULL");
        for (i, column) in columns.iter().enumerate() {
            by_column.insert(column.name.clone(), row.get::<_, Option<String>>(i));
        }
        result.insert(pk_value, by_column);
    }
    Ok(result)
}

/// Reads an `Aggregate` key-space target table back as text, keyed by the
/// same composite [`group_key`] convention the SQL oracle and the evaluator
/// oracle key their own rows by (see `crate::oracle`'s module doc comment and
/// [`group_key`]'s own) — so the three-way comparison lines the same group up
/// across all three sources (improvement-plan task B4).
///
/// Unlike [`read_table`]'s single-column primary key (a real Postgres
/// `primary key` constraint on the *source* table, so it's never `NULL`), a
/// `GROUP BY` grouping column genuinely can be `NULL` (the generative suite's
/// grain column deliberately draws one) — so every grouping column here is
/// read as a nullable `Option<String>` rather than `expect`-ed `Some`, and
/// `group_key` is what turns a possibly-`NULL` tuple of them into one
/// [`Rows`]-shaped map key.
///
/// `fields` is `def.fields` — every field whose name matches one of
/// `group_by`'s columns is excluded from the row's own value columns (it
/// contributes no separate target column at all, mirroring
/// `trellis::defs::ddl::create_aggregate_target_table`'s own "a field named
/// after a grouping column is that column's passthrough" rule), leaving only
/// the real aggregate-measure columns.
///
/// [`Rows`]: crate::oracle::Rows
async fn read_aggregate_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    group_by: &[String],
    fields: &[FieldDef],
) -> Result<BTreeMap<String, BTreeMap<String, Option<String>>>, ManualBackendError> {
    let value_fields: Vec<&str> = fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(|name| !group_by.iter().any(|g| g == name))
        .collect();

    let mut select_list: Vec<String> = group_by
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect();
    select_list.extend(
        value_fields
            .iter()
            .map(|c| format!("{}::text", quote_ident(c))),
    );
    let sql = format!("select {} from {qualified_table}", select_list.join(", "));
    let rows = client.query(&sql, &[]).await?;

    let mut result = BTreeMap::new();
    for row in rows {
        let group_values: Vec<Option<String>> = (0..group_by.len())
            .map(|i| row.get::<_, Option<String>>(i))
            .collect();
        let key = group_key(&group_values);
        let mut by_column = BTreeMap::new();
        for (i, name) in value_fields.iter().enumerate() {
            by_column.insert(
                (*name).to_string(),
                row.get::<_, Option<String>>(group_by.len() + i),
            );
        }
        result.insert(key, by_column);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::defs::parse;

    /// The reviewer-flagged follow-up to issue #67 (real operator
    /// precedence): a nested, mixed-operator `Expr` — `Add(Column("a"),
    /// GreaterThan(Column("b"), Column("c")))`, i.e. the tree a source
    /// author would have to spell `a + (b > c)` to get — must round-trip
    /// through `render_expr` and back through the real parser to the exact
    /// same tree, not a reflowed one.
    ///
    /// Before this fix, `render_expr` rendered this tree flat as
    /// `a + b > c`, which the precedence-climbing parser (issue #67) then
    /// re-parses as `GreaterThan(Add(a, b), c)` — `+` binds tighter than
    /// `>`, so it silently reconstructs the *wrong* tree, one that even
    /// type-checks (`Numeric, Numeric -> Boolean`) even though it isn't what
    /// was rendered. Unconditional parenthesization
    /// (`render_expr`'s doc comment) fixes this by always rendering
    /// `(a) + (b > c)`, which only parses one way regardless of any
    /// operator's precedence.
    #[test]
    fn render_expr_parenthesizes_nested_binary_ops_so_they_round_trip() {
        let expr = Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column("b".to_string())),
                rhs: Box::new(Expr::Column("c".to_string())),
            }),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(
            def.fields[0].expr, expr,
            "round-tripping through render_expr -> parse must reproduce the exact original \
             tree; without unconditional parenthesization this would silently come back as \
             `GreaterThan(Add(a, b), c)` instead (`+` binds tighter than `>`, so a flat, \
             unparenthesized render loses the original grouping)"
        );
    }

    /// The mirror shape — `GreaterThan(Add(a, b), c)`, i.e. `(a + b) > c` —
    /// which happens to round-trip correctly even *without* parens (since
    /// `+`'s tighter precedence reconstructs the same grouping by accident).
    /// Pinned anyway so a future change to `render_expr` can't quietly regress
    /// this direction while only testing the other one.
    #[test]
    fn render_expr_round_trips_a_greater_than_wrapping_an_add() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            }),
            rhs: Box::new(Expr::Column("c".to_string())),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(def.fields[0].expr, expr);
    }
}
