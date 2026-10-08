//! The Trellis client facade — the one interface an embedding application is
//! meant to use.
//!
//! Everything an embedder needs to stand up and run a Trellis instance hangs
//! off [`Trellis`]: apply migrations, register relationships and transform
//! definitions, list what's registered, request an ad-hoc backfill, and run
//! the live CDC/apply pipeline. It composes the lower-level building blocks
//! (`defs`, `client`, `intake`, `staging`) into one coherent surface so
//! callers never have to stitch those together themselves — and, critically,
//! never accidentally pick the wrong path: definition registration always
//! goes through [`defs::install_definition`], which creates the target and
//! leaves the build to the staging worker's backfill discharge (ADR-0016),
//! rather than the lower-level primitives it's built from.
//!
//! The individual `defs::*`/`intake::*`/`staging::*` items remain public for
//! internal harnesses (the benchmark and generative-test crates use them as
//! an oracle/testbench), but embedders should treat [`Trellis`] as *the*
//! interface and reach past it only when they genuinely need a primitive it
//! doesn't expose.
//!
//! [`TrellisError::code`] reports a stable, coarse [`ErrorCode`] category
//! for any failure this facade can return, on top of the existing `Display`
//! message — settled ahead of issue #87's FFI embedding work so a host
//! language on the other side of that boundary has something stable to
//! match on instead of every internal Rust error variant (`docs/decisions/0008-public-api-design.md`,
//! decision 3).
//!
//! **Every definition-changing operation goes through one entrypoint.**
//! [`Trellis::apply`] takes a single statement of Trellis's own grammar — the
//! same grammar the CLI speaks — and the parse decides which operation runs:
//! define a transform, define a relationship, pause, resume, drop (issue #227;
//! ADR-0012, ADR-0014). There is deliberately no typed method per operation,
//! so adding one is a grammar addition rather than new surface every
//! host-language binding has to mirror. Read paths (status, `self_check`,
//! quarantine sampling, convergence-await) are unaffected — they stay typed;
//! only the mutation surface unifies.
//!
//! **[`apply`](Trellis::apply)ing a definition doesn't block on backfill.** Per
//! `docs/decisions/0008-public-api-design.md`'s decision 1 and
//! [ADR-0007's amendment](../../docs/decisions/0007-direct-set-based-backfill.md#backgrounding-and-resumability-amendment),
//! and ADR-0016, a transform's initial backfill runs in the background:
//! `apply()` returns once the definition is registered, with
//! [`TransformStatus::WaitingToBackfill`], having read no source rows. The
//! staging worker's backfill discharge then dispatches the build by shape: a
//! plain (non-relationship) 1-1 transform as a durable, claimable queue of
//! chunks, and an aggregate (`GROUP BY`) or relationship-enriched 1-1
//! transform (like the `count(posts.id)` example below) as one direct-build
//! job. Running drain (`application_threads`) workers execute either —
//! anywhere in the fleet, not necessarily on the connection that called
//! `apply()`. Callers that need the target actually populated poll
//! [`Trellis::status`] until it reports [`TransformStatus::Live`] — which
//! requires a staging worker and *some* client in the fleet running with
//! `drain_threads > 0` (a define-only connection, with no such client
//! anywhere, leaves the transform queued indefinitely) — then take a
//! [`Trellis::watermark_token`] and [`Trellis::await_converged`] on it.
//!
//! Nor does an edit block on its rebuild (#666, #625 F8b): an `ALTER
//! TRANSFORM` that adds or changes fields, and a column `RESUME`, register a
//! background field build and return, the definition reading
//! [`TransformStatus::Backfilling`] until it is done. The statements that
//! pause, resume and drop write only the catalog. The one statement that
//! still reads table rows inside the call is a to-one relationship's
//! declaration (or a transform reading through one), which seeds the
//! relationship's projection from its to-side table (milestone E, #624,
//! replaces it).
//!
//! # Lifecycle
//!
//! ```no_run
//! # async fn example() -> Result<(), trellis::TrellisError> {
//! use trellis::{Config, Trellis, TrellisOptions, TransformStatus};
//!
//! // Define transforms with no runtime attached.
//! let trellis = Trellis::connect(Config::resolve(None)?, TrellisOptions::default()).await?;
//! trellis.migrate().await?;
//! trellis
//!     .apply("RELATIONSHIP posts FROM authors.id TO posts.author")
//!     .await?;
//! trellis
//!     .apply("TRANSFORM authors_calc FROM authors SELECT count(posts.id) AS post_count")
//!     .await?;
//!
//! // Separately, run the live pipeline: staging worker + two drain threads.
//! // Drain threads are also what finish any queued backfill chunk work, for
//! // a plain 1-1 transform `apply()` returned before fully building.
//! let running = Trellis::connect(
//!     Config::resolve(None)?,
//!     TrellisOptions {
//!         staging: true,
//!         drain_threads: 2,
//!         ..Default::default()
//!     },
//! )
//! .await?;
//! // Poll until every registered transform is done backfilling.
//! while running.status("authors_calc").await?.map(|s| s.status) != Some(TransformStatus::Live) {
//!     // ... sleep, then re-check ...
//! }
//! // ... run until shutdown ...
//! running.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use tokio_postgres::types::PgLsn;

use crate::client::{Client, ClientError, ClientOptions};
use crate::config::Config;
use crate::defs::{
    self, CatalogError, Definition, ParseError, RelationshipDefinition, TransformStatus, ValueType,
};
use crate::error_code::{self, ErrorCode};
use crate::intake::IntakeError;
use crate::pool::Pool;
use crate::staging::apply::ApplyError;
use crate::staging::holdup::DrainFailure;
use crate::staging::quarantine::{self, HeldKeys};
use crate::staging::self_check::{SelfCheckError, SelfCheckMode, SelfCheckReport, SelfCheckScope};
use crate::staging::{DEFAULT_RECLAIM_TTL, StagingError, converge, worker_registry};

/// Options a client sets when it [`connect`](Trellis::connect)s.
///
/// `staging`/`drain_threads` mirror [`ClientOptions`]'s core contract (see
/// its doc comment): whether this connection owns capture + ring
/// maintenance, and how many drain (application) workers it runs. A
/// connection that only defines transforms leaves both at their defaults
/// (nothing background starts); a connection that runs the live pipeline
/// sets `staging` and a non-zero `drain_threads`. `worker_threads` is
/// unrelated to either — see its own doc comment.
#[derive(Debug, Clone, Default)]
pub struct TrellisOptions {
    /// Whether this connection runs capture reconciliation and ring
    /// maintenance (the staging worker). Exactly one connection in a fleet
    /// should set this. When set, the staging worker installs capture
    /// triggers on whatever tables the registered definitions read, straight
    /// from the catalog, and picks up definitions registered later on its
    /// next reconcile pass; none need be registered before it starts (issue
    /// #427).
    pub staging: bool,
    /// How many drain (application) worker threads this connection runs. Zero
    /// (the default) runs none.
    pub drain_threads: usize,
    /// Caps the worker-thread count of the `tokio` runtime
    /// [`BlockingTrellis::connect`](crate::BlockingTrellis::connect) builds
    /// to own this connection (see its module doc comment) — irrelevant to
    /// [`Trellis::connect`] itself, which never builds a runtime of its own.
    /// `None` (the default) preserves today's behavior: `tokio`'s own
    /// default of one worker thread per core.
    ///
    /// Matters chiefly when Trellis is embedded inside a host VM that
    /// already sized its own scheduler pool to core count — a BEAM node, or
    /// a Ruby process with its own thread pool — where the unbounded
    /// default silently doubles the thread population with threads the host
    /// can't see, account for, or size around. Since the embedded runtime's
    /// own work is I/O, not compute-bound, a small explicit count (2, say)
    /// is enough; see issue #141.
    ///
    /// Must be at least 1 when set: `Some(0)` fails the connect with
    /// [`TrellisError::BlockingSpawn`], since a runtime with no worker
    /// threads couldn't run anything anyway.
    pub worker_threads: Option<usize>,
}

/// A connected Trellis instance — see the [module docs](self).
///
/// Holds a connection pool and, when [`TrellisOptions`] asked for it, a
/// running background [`Client`] (staging worker and/or drain workers). Drop
/// or [`shutdown`](Trellis::shutdown) stops that background work.
pub struct Trellis {
    config: Config,
    pool: Pool,
    /// `Some` iff `options.staging || options.drain_threads > 0` — the live
    /// pipeline this connection started.
    client: Option<Client>,
}

impl Trellis {
    /// Connects to the database `config` names and, if `options` asks for any
    /// background work, starts it before returning.
    ///
    /// With the default options nothing background runs — the returned handle
    /// is purely for defining transforms/relationships and inspecting the
    /// catalog. With `staging` set, the staging worker starts: it installs
    /// capture triggers on whatever tables the registered definitions read
    /// (none yet is fine) and runs ring maintenance; with a non-zero `drain_threads`,
    /// that many application workers start.
    pub async fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let pool = Pool::new(&config)?;
        let client = if options.staging || options.drain_threads > 0 {
            Some(Self::start_client(&config, &options)?)
        } else {
            None
        };
        Ok(Self {
            config,
            pool,
            client,
        })
    }

    /// The connection pool backing this instance. Exposed for callers that
    /// need to run their own queries against target tables; not needed for
    /// anything [`Trellis`]'s own methods already cover. Its connections run
    /// with `row_security = off` (issue #766), so a query that row-level
    /// security would filter for the login role fails instead: read through
    /// your own connection where you rely on policies to filter. They also
    /// run with `jit = off` (issue #794).
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// The resolved configuration (schema names, DSN) this instance connected
    /// with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// A handle onto this process's in-process metrics registry (issue #53),
    /// for Prometheus exposition — see [`crate::metrics::Metrics::render_prometheus`]:
    ///
    /// ```no_run
    /// # async fn example(trellis: &trellis::Trellis) {
    /// let body = trellis.metrics().render_prometheus();
    /// # }
    /// ```
    ///
    /// The registry itself is process-wide, not scoped to this particular
    /// connection (see `trellis::metrics`'s module doc comment) — this
    /// method exists so callers reach it through the same facade as
    /// everything else, matching `docs/observability.md`'s
    /// `trellis.metrics().render_prometheus()` sketch, rather than because
    /// `self` is actually consulted.
    pub fn metrics(&self) -> crate::metrics::Metrics {
        crate::metrics::Metrics::new()
    }

    /// Applies Trellis's schema migrations. Idempotent — safe to call on
    /// every startup.
    pub async fn migrate(&self) -> Result<(), TrellisError> {
        crate::migrate(&self.pool, &self.config)
            .await
            .map_err(TrellisError::Engine)
    }

    /// **The** entrypoint for every definition-changing operation: parse one
    /// statement of Trellis's own grammar and run whatever it says (issue
    /// #227; ADR-0012, ADR-0014).
    ///
    /// Parsing decides the operation, so this signature does not change as
    /// operations are added — a new operation is a grammar addition and a new
    /// [`Applied`] variant, not a new method every binding has to mirror. That
    /// is the whole point: text is the simplest thing to carry across an FFI
    /// boundary (one string in, plain data out), which is why the typed
    /// `define`/`define_relationship`/`pause_transform`/`resume_transform`/
    /// `resume_column`/`drop_transform`/`drop_relationship` methods this
    /// replaces are gone rather than kept alongside it.
    ///
    /// A caller that accepts only some forms (a binding's `define` takes only
    /// `TRANSFORM`) asks [`crate::statement_kind`] first: it parses the text
    /// the same way, applies nothing, and so can refuse the wrong form before
    /// anything happens.
    ///
    /// The statements it accepts (see [`defs::parse_statement`] for the full
    /// grammar, and `docs/transforms.md` for the semantics):
    ///
    /// ```text
    /// TRANSFORM <target> FROM <source> [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]
    /// RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
    ///
    /// PAUSE  TRANSFORM <target>[.<column>]
    /// RESUME TRANSFORM <target>[.<column>]
    /// DROP   TRANSFORM <target>
    /// DROP   RELATIONSHIP [<schema>.]<from_table>.<relationship_name>
    /// ```
    ///
    /// # Addressing
    ///
    /// A transform is addressed by its **bare** target-table name, and a
    /// dotted address means `<transform>.<column>` — not
    /// `<schema>.<transform>`. A relationship is always addressed **scoped to
    /// its from-table** (`posts.author`), because its name is unique only
    /// there. Both follow from how the engine itself identifies a definition;
    /// see [`defs::DefinitionRef`] for the reasoning behind each.
    ///
    /// A schema-qualified relationship address (`blog.posts.author`) is
    /// accepted, and names exactly the relationship declared on that schema's
    /// `posts` — the schema its from-table resolved to when it was declared
    /// (`relationship_definitions.from_schema`, issues #285/#288). A
    /// relationship name is unique per *qualified* from-table, so
    /// `blog.posts.author` and `shop.posts.author` can both exist; a bare
    /// `posts.author` then matches both and is refused as ambiguous rather
    /// than guessed at. An address naming no registered relationship is
    /// `DROP`'s ordinary idempotent no-op.
    ///
    /// # What each statement does
    ///
    /// `TRANSFORM`/`RELATIONSHIP` register a definition, as the retired
    /// `define`/`define_relationship` did — including that **a transform
    /// returns before its backfill starts** (see this module's doc comment):
    /// the returned [`Definition`] reports
    /// [`TransformStatus::WaitingToBackfill`] and the target is populated in
    /// the background, whatever the transform's shape. Poll
    /// [`Trellis::status`] for [`TransformStatus::Live`].
    ///
    /// `PAUSE` freezes a definition at its current value (ADR-0014's
    /// operator-driven half of the pause state whose other half is the poison
    /// fuse) and is **idempotent** — pausing something already frozen, by
    /// either trigger, succeeds as a no-op. Pausing a target that was never
    /// defined is [`CatalogError::TransformNotFound`], since you cannot freeze
    /// what doesn't exist.
    ///
    /// `RESUME` **rebuilds; it does not catch up**. A frozen definition must
    /// not pin the staging ring, so while it is frozen its share of the change
    /// stream is drained for its siblings and is not recoverable by replay —
    /// the recovery is therefore a fresh backfill from source, whose cost
    /// scales with the data rather than with the length of the pause. A whole
    /// transform drops back to [`TransformStatus::WaitingToBackfill`]; a single
    /// column is re-derived across every existing row, and any dependent
    /// column paused *only* by this one's cascade is resumed with it — those
    /// pairs come back in [`Applied::Resumed`].
    ///
    /// `DROP` is the terminal reap. It requires the definition to be frozen
    /// first ([`CatalogError::TransformNotPaused`] otherwise — there is no
    /// live-to-gone edge), **takes the data with it** (the Trellis-owned target
    /// table is dropped unconditionally; source tables are untouched),
    /// **refuses rather than cascades** if a still-registered definition
    /// chains off the subject ([`CatalogError::DependentsBlockDrop`], naming
    /// the blockers, so a chain is retired from the leaves inward), lets the
    /// staging worker's next reconcile uninstall capture nothing reads any
    /// more once it commits, and is
    /// **idempotent** — dropping something already gone succeeds.
    ///
    /// # Pause and resume are transform-only
    ///
    /// The statement list above is exhaustive, and there is deliberately no
    /// `PAUSE`/`RESUME RELATIONSHIP`. A relationship is a reusable *component*
    /// of a transform, not something that does work of its own, so it has
    /// nothing to suspend — which is also why ADR-0014 gives it no lifecycle
    /// status and nothing in the fold gates on one. `PAUSE RELATIONSHIP x.y` is
    /// consequently not a form this grammar knows at all, and fails as an
    /// ordinary parse error about an unexpected keyword, not a special-cased
    /// refusal. Retiring a relationship *is* meaningful — it is a definition —
    /// so `DROP RELATIONSHIP` exists.
    ///
    /// `DROP TRANSFORM <target>.<column>` is likewise not a form: dropping one
    /// calculated field is an `ALTER TRANSFORM ... DROP <field>`, tracked
    /// separately (issues #241/#242), not a `DROP`.
    pub async fn apply(&self, statement_text: &str) -> Result<Applied, TrellisError> {
        match defs::parse_statement(statement_text)? {
            defs::Statement::DefineTransform(parsed) => {
                let source_columns = self
                    .source_columns(&parsed.source, parsed.explicit_source_schema.as_deref())
                    .await?;
                // Hands the *text* on rather than the `TransformDef` just
                // parsed: `install_definition` persists `definition_text` as
                // the definition's own record of itself (every later re-parse
                // reads it back), so the text is the input it needs, not a
                // redundant re-derivation of it.
                let definition = defs::install_definition(
                    &self.pool,
                    statement_text,
                    &source_columns,
                    self.config.target_schema(),
                )
                .await
                .map_err(TrellisError::Catalog)?;
                Ok(Applied::TransformDefined(definition))
            }
            defs::Statement::DefineRelationship(_) => {
                let relationship = defs::create_relationship(&self.pool, statement_text)
                    .await
                    .map_err(TrellisError::Catalog)?;
                Ok(Applied::RelationshipDefined(relationship))
            }
            defs::Statement::Pause(reference) => self.apply_pause(reference).await,
            defs::Statement::Resume(reference) => self.apply_resume(reference).await,
            defs::Statement::Drop(reference) => self.apply_drop(reference).await,
            defs::Statement::AlterTransform(alter) => self.apply_alter(&alter).await,
        }
    }

    /// [`Statement::AlterTransform`](defs::Statement::AlterTransform)'s half
    /// of [`apply`](Trellis::apply) (ADR-0015, issues #241/#242) — a thin
    /// facade wrapper over [`defs::alter_transform`], which does the actual
    /// work (idempotency, validation, DDL, registering the field build,
    /// version fencing); see that function's own doc comment for the full
    /// contract.
    async fn apply_alter(&self, alter: &defs::AlterTransform) -> Result<Applied, TrellisError> {
        let outcome = defs::alter_transform(&self.pool, alter)
            .await
            .map_err(TrellisError::Catalog)?;
        Ok(Applied::Altered {
            definition: outcome.definition,
            added: outcome.added,
            dropped: outcome.dropped,
            altered: outcome.altered,
        })
    }

    /// [`Statement::Pause`](defs::Statement::Pause)'s half of
    /// [`apply`](Trellis::apply).
    async fn apply_pause(&self, reference: defs::TransformRef) -> Result<Applied, TrellisError> {
        let defs::TransformRef { target, column } = reference;
        match column {
            None => defs::lifecycle::pause_transform(&self.pool, &target)
                .await
                .map(|_| Applied::Paused)
                .map_err(TrellisError::Catalog),
            Some(column) => {
                // Checked here rather than inside `quarantine::pause_column`:
                // `column_status` has no foreign key onto either the
                // definition or its field list, so an unchecked pause would
                // silently park a row addressing nothing, and a later
                // `RESUME` of it would be the first sign anything was wrong.
                self.expect_column_exists(&target, &column).await?;
                quarantine::pause_column(&self.pool, &target, &column)
                    .await
                    .map(|()| Applied::Paused)
                    .map_err(TrellisError::Apply)
            }
        }
    }

    /// [`Statement::Resume`](defs::Statement::Resume)'s half of
    /// [`apply`](Trellis::apply).
    async fn apply_resume(&self, reference: defs::TransformRef) -> Result<Applied, TrellisError> {
        let defs::TransformRef { target, column } = reference;
        match column {
            None => quarantine::resume_transform(&self.pool, &target)
                .await
                .map(|()| Applied::Resumed {
                    columns: Vec::new(),
                })
                .map_err(TrellisError::Apply),
            Some(column) => quarantine::resume_column(&self.pool, &target, &column)
                .await
                .map(|columns| Applied::Resumed { columns })
                .map_err(TrellisError::Apply),
        }
    }

    /// [`Statement::Drop`](defs::Statement::Drop)'s half of
    /// [`apply`](Trellis::apply). Only removes catalog rows: it never touches
    /// a source table (issue #427, ADR-0016). The staging worker's next
    /// reconcile pass uninstalls the capture triggers of a table nothing reads
    /// any more, so the process applying a `DROP` needs no privileges on the
    /// source tables.
    async fn apply_drop(&self, reference: defs::DefinitionRef) -> Result<Applied, TrellisError> {
        match reference {
            defs::DefinitionRef::Transform(target) => {
                defs::lifecycle::drop_transform(&self.pool, &target)
                    .await
                    .map_err(TrellisError::Catalog)?;
            }
            defs::DefinitionRef::Relationship {
                schema,
                from_table,
                name,
            } => {
                // Issues #285/#288: a qualified address names exactly the
                // relationship declared on `schema.from_table`; a bare one
                // must match only one schema's `from_table`, and is refused as
                // ambiguous otherwise. See
                // `defs::catalog::relationship_at_address`.
                defs::lifecycle::drop_relationship(
                    &self.pool,
                    schema.as_deref(),
                    &from_table,
                    &name,
                )
                .await
                .map_err(TrellisError::Catalog)?;
            }
        }
        Ok(Applied::Dropped)
    }

    /// Errors unless `target` is a registered transform *and* `column` is one
    /// of the calculated fields its definition declares —
    /// [`TrellisError::TransformNotFound`] or [`TrellisError::ColumnNotFound`]
    /// respectively (both [`ErrorCode::NotFound`]).
    ///
    /// Read from the definition's own persisted text rather than from the
    /// target table's live columns: the definition is what the pause is
    /// *about*, and a field it declares is exactly the set
    /// [`crate::staging::quarantine`]'s pause/resume machinery can act on.
    async fn expect_column_exists(&self, target: &str, column: &str) -> Result<(), TrellisError> {
        let definition = defs::catalog::definition_by_target(&self.pool, target)
            .await
            .map_err(TrellisError::Catalog)?
            .ok_or_else(|| TrellisError::TransformNotFound(target.to_string()))?;
        if definition
            .def
            .fields
            .iter()
            .any(|field| field.name == column)
        {
            return Ok(());
        }
        Err(TrellisError::ColumnNotFound {
            transform: target.to_string(),
            column: column.to_string(),
            declared: definition
                .def
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect(),
        })
    }

    /// Every registered transform definition, oldest first. Each status is
    /// the one [`Trellis::status`] reports: a `live` definition reading an
    /// upstream that isn't `live` shows [`TransformStatus::CatchingUp`]
    /// (issue #497). Each also carries why its build keeps failing, if it
    /// does, and why the drain halted on it, if it did (issue #663), so one
    /// listing shows every stuck definition: a health check that lists them
    /// and finds a [`DefinitionSummary::halt`] has a definition stopped until
    /// an operator fixes the cause and resumes it.
    pub async fn definitions(&self) -> Result<Vec<DefinitionSummary>, TrellisError> {
        let mut client = self.pool.get().await?;
        // One repeatable-read transaction, so the reported statuses are
        // derived from the same catalog state the rows come from.
        let txn = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        // Issue #461: the same `pending_backfill` and failing-chunk joins
        // `status` reads its `backfill_failure` from, in the one listing
        // query (`table_name` is the table's primary key, and the chunk join
        // takes one chunk, so each adds at most one row per definition), and
        // the definition's halt (#663; `capture_failures` is keyed by it).
        let rows = txn
            .query(
                &format!(
                    "select d.id, d.target_table, d.source_table, d.source_version, d.created_at, \
                            pb.table_name as backfill_table, pb.attempts as backfill_attempts, \
                            pb.last_error as backfill_last_error, \
                            pb.next_attempt_at as backfill_next_attempt_at, {CHUNK_FAILURE_COLUMNS}, \
                            cf.source_table as capture_table, cf.columns as capture_columns, \
                            cf.error as capture_error, cf.detected_at as capture_detected_at, \
                            cf.kind as capture_kind \
                     from transform_definitions d \
                     left join pending_backfill pb \
                       on pb.table_name = d.source_table and pb.last_error is not null \
                     {CHUNK_FAILURE_JOIN} \
                     left join capture_failures cf \
                       on cf.transform_id = d.id and cf.kind = 'halt' \
                     order by d.id"
                ),
                &[],
            )
            .await?;
        let reported = crate::defs::catalog::reported_statuses(&*txn).await?;
        txn.commit().await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let id: i64 = row.get(0);
                DefinitionSummary {
                    id,
                    target_table: row.get(1),
                    source_table: row.get(2),
                    source_version: row.get(3),
                    status: reported[&id],
                    created_at: row.get(4),
                    backfill_failure: backfill_failure(&row),
                    halt: capture_failure(&row),
                }
            })
            .collect())
    }

    /// One registered transform definition's current [`TransformStatus`]
    /// (issue #55), by target table name — the read a host-language embedder
    /// polls after [`apply`](Trellis::apply) registers a definition, per
    /// `docs/decisions/0008-public-api-design.md`'s decision 1 ("define, then poll status
    /// until live").
    ///
    /// [`TransformStatus::Live`] means the transform is in its steady state
    /// (ADR-0016, "What `live` promises"): from then on, a
    /// [`Trellis::watermark_token`] taken after a commit and awaited with
    /// [`Trellis::await_converged`] guarantees its target reflects that
    /// commit. A transform whose build has finished but whose go-live
    /// catch-up hasn't run yet reports [`TransformStatus::CatchingUp`]
    /// instead: it is applying changes, but its target may still be missing
    /// some (issue #476). A `live` 1-1 one rebuilding fields (an `ALTER
    /// TRANSFORM` that added or changed fields, a resumed column) reports
    /// [`TransformStatus::Backfilling`] until they are built (#625 F8b). So does
    /// a `live` one reading an upstream (another definition's target, as its
    /// source or through a relationship) that isn't `live` itself: paused,
    /// quarantined, rebuilding or catching up, down the whole chain (issue
    /// #497). That one is derived from the upstream's status when read.
    ///
    /// Also reports why a definition isn't getting there, when the cause is a
    /// failing build ([`DefinitionStatus::backfill_failure`], issues #407,
    /// #616): a failing chunk of it, or a failing backfill of its source
    /// table. Both are retried with backoff, so without this the only sign
    /// would be a warning in a worker's log.
    pub async fn status(
        &self,
        target_table: &str,
    ) -> Result<Option<DefinitionStatus>, TrellisError> {
        let mut client = self.pool.get().await?;
        // Issue #73: `transform_definitions.target_table` is persisted
        // fully-qualified, but every caller here only ever has the bare name
        // their `TRANSFORM <name> FROM ...` text declared — even once issue
        // #76 taught the grammar an explicit `schema.table` spelling,
        // `def.target` itself still always holds just the bare table name
        // (see `defs::ast::TransformDef`'s own doc comment for why), so this
        // API's callers never have anything but the bare name to poll with —
        // match against `target_table`'s bare table-name suffix rather than
        // the qualified column directly.
        let txn = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let row = txn
            .query_opt(
                &format!(
                    "select d.status, d.id, d.source_table, d.definition_text, \
                            pb.table_name as backfill_table, pb.attempts as backfill_attempts, \
                            pb.last_error as backfill_last_error, \
                            pb.next_attempt_at as backfill_next_attempt_at, \
                            {CHUNK_FAILURE_COLUMNS}, \
                            cf.source_table as capture_table, cf.columns as capture_columns, \
                            cf.error as capture_error, cf.detected_at as capture_detected_at, \
                            cf.kind as capture_kind, \
                            exists (select 1 from column_status cs \
                                    where cs.transform_table = $1 and cs.awaiting_capture) \
                              as awaiting_capture, \
                            (select count(*) from poison p where p.transform_id = d.id) \
                              as held_count, \
                            (select min(p.poisoned_at) from poison p \
                             where p.transform_id = d.id) as held_since \
                     from transform_definitions d \
                     left join pending_backfill pb \
                       on pb.table_name = d.source_table and pb.last_error is not null \
                     {CHUNK_FAILURE_JOIN} \
                     left join capture_failures cf on cf.transform_id = d.id \
                     where split_part(d.target_table, '.', 2) = $1"
                ),
                &[&target_table],
            )
            .await?;
        let mut status = None;
        let mut holdup = (None, None);
        let mut drain_failure = None;
        if let Some(row) = &row {
            let status_text: String = row.get(0);
            let stored = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            });
            status = Some(reported_status(&*txn, row.get(1), stored).await?);
            // #687: an edit whose new field awaits a widen is stuck on
            // capture as much as a registration is. Not one a schema change
            // paused (#705): capture stops widening for it, and its field
            // waits for the resume its `capture_failure` asks for, not on
            // the table's lock or install failure.
            let capture_failed = row.get::<_, Option<String>>("capture_table").is_some();
            if !capture_failed
                && (stored == TransformStatus::WaitingToBackfill || row.get("awaiting_capture"))
            {
                holdup = self.capture_holdup(&*txn, row.get(2)).await?;
            }
            // #817: a page the drain keeps failing on, charged to no one,
            // holds back every definition still applying what it reads.
            if !stored.is_frozen() {
                drain_failure = crate::staging::holdup::for_definition(
                    &*txn,
                    row.get("source_table"),
                    row.get("definition_text"),
                )
                .await?;
            }
        }
        txn.commit().await?;
        let (capture_wait, stalled) = holdup;
        Ok(row.zip(status).map(|(row, status)| DefinitionStatus {
            status,
            backfill_failure: backfill_failure(&row),
            capture_wait,
            capture_failure: capture_failure(&row).or(stalled),
            held_keys: quarantine::held_keys_from(row.get("held_count"), row.get("held_since")),
            drain_failure,
        }))
    }

    /// What holds up the capture a definition sourced from `source_table`
    /// waits on (issue #622 C5, #687): the lock wait, then the other
    /// failure, that the staging worker's latest pass recorded in
    /// `capture_holdups` for the source, or for the to-side of a relationship
    /// declared on it. The staging worker may run in any process.
    async fn capture_holdup(
        &self,
        client: &impl tokio_postgres::GenericClient,
        source_table: &str,
    ) -> Result<(Option<CaptureWait>, Option<CaptureFailure>), TrellisError> {
        let rows = client
            .query(
                "select h.table_name, h.since, h.operation, h.lock_mode, h.observed_at, \
                        h.blockers, h.error, h.columns \
                 from capture_holdups h \
                 join (select $1::text as t, 0::bigint as ord \
                       union all \
                       select to_schema || '.' || to_table, id from relationship_definitions \
                       where from_schema || '.' || from_table = $1) read \
                   on read.t = h.table_name \
                 order by read.ord",
                &[&source_table],
            )
            .await?;
        let wait = rows.iter().find_map(|row| {
            let operation: Option<String> = row.get(2);
            operation.map(|operation| CaptureWait {
                table: row.get(0),
                operation,
                lock_mode: row.get(3),
                waiting_since: row.get(1),
                observed_at: row.get(4),
                blockers: row.get(5),
            })
        });
        let failure = rows.iter().find_map(|row| {
            let error: Option<String> = row.get(6);
            error.map(|error| CaptureFailure {
                kind: CaptureFailureKind::Capture,
                source_table: row.get(0),
                columns: row.get(7),
                error,
                detected_at: row.get(1),
            })
        });
        Ok((wait, failure))
    }

    /// Every registered relationship declaration, oldest first.
    pub async fn relationships(&self) -> Result<Vec<RelationshipSummary>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select id, name, from_schema, from_table, from_col, to_schema, to_table, \
                 to_col, cardinality, created_at \
                 from relationship_definitions order by id",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| RelationshipSummary {
                id: row.get(0),
                name: row.get(1),
                from_schema: row.get(2),
                from_table: row.get(3),
                from_col: row.get(4),
                to_schema: row.get(5),
                to_table: row.get(6),
                to_col: row.get(7),
                cardinality: row.get(8),
                created_at: row.get(9),
            })
            .collect())
    }

    /// Re-reads `source_table` via a `pending_backfill` marker, for every
    /// applying definition that reads it, directly or through a
    /// relationship, to re-derive from. A newly registered transform doesn't
    /// need this: the staging worker parks its marker itself (ADR-0016).
    ///
    /// The marker is a go-live catch-up (issue #522, ADR-0016's "A re-read
    /// table's readers"): each `live` reader reports `catching_up` from this
    /// call until the staging worker has discharged it. The discharge
    /// re-derives every row the table still has, deletes each reader's
    /// target rows the table no longer backs (a delete that never reached the
    /// target, say), refreshes the settled projections of a relationship
    /// whose to-side the table is, and flips the readers back `live`.
    ///
    /// Only valid for a table the staging worker captures: one some
    /// registered definition reads, directly or through a relationship, and
    /// isn't one of this instance's own targets (issue #622; the set its
    /// reconcile pass installs capture triggers on). Any other table is
    /// refused ([`TrellisError::TableNotCaptured`]): nothing reads it, and a
    /// table's first reader backfills it in full anyway. The check is the
    /// catalog's, not the triggers': a marker parked before the staging
    /// worker's install lands is harmless, since the table has no applying
    /// reader yet and the discharge dispatches no definition whose capture
    /// isn't current. The running staging worker discharges the marker.
    pub async fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        let mut client = self.pool.get().await?;
        let schema_rows = client
            .query(
                "select table_schema from information_schema.tables \
                 where table_name = $1 and table_schema = any(current_schemas(false))",
                &[&source_table],
            )
            .await?;
        let schema: String = schema_rows
            .first()
            .ok_or_else(|| TrellisError::SourceTableNotFound(source_table.to_string()))?
            .get(0);
        let qualified = format!("{schema}.{source_table}");

        let captured = crate::defs::tables_to_capture(&self.pool)
            .await?
            .contains(&qualified);
        if !captured {
            return Err(TrellisError::TableNotCaptured { table: qualified });
        }

        // A failure to park is a plain `Db` error.
        let txn = client.transaction().await?;
        crate::intake::markers::park_table_catch_ups(&*txn, std::slice::from_ref(&qualified))
            .await
            .map_err(|err| match err {
                IntakeError::Db(err) => TrellisError::Db(err),
                IntakeError::Catalog(err) => TrellisError::Catalog(*err),
                err => TrellisError::Client(ClientError::Intake(err)),
            })?;
        txn.commit().await?;
        Ok(())
    }

    /// Poison-quarantine entries recorded since `watermark`, oldest first —
    /// the keys the apply path gave up on, each for the definition whose
    /// apply failed (#799: every other definition reading the key keeps
    /// applying it). A running client surfaces these so a whole-table
    /// failure (every row poisoned) doesn't sit silently.
    pub async fn poisoned_since(
        &self,
        watermark: SystemTime,
    ) -> Result<Vec<PoisonEntry>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select split_part(d.target_table, '.', 2), p.src_table, p.key, p.last_error, \
                        p.poisoned_at \
                 from poison p join transform_definitions d on d.id = p.transform_id \
                 where p.poisoned_at > $1 order by p.poisoned_at, d.id",
                &[&watermark],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| PoisonEntry {
                transform: row.get(0),
                src_table: row.get(1),
                key: row.get(2),
                last_error: row.get(3),
                poisoned_at: row.get(4),
            })
            .collect())
    }

    /// Every currently paused/quarantined target, across every transform —
    /// `docs/decisions/0003-quarantine-storage-and-api.md`'s amendment,
    /// "Client library API" read 1. Cheap: reads `transform_definitions`'
    /// lifecycle status plus the small, sparse `column_status` table, no
    /// join against poisoned-row detail. The read a dashboard/health-check
    /// polls.
    pub async fn quarantined(&self) -> Result<Vec<QuarantineEntry>, TrellisError> {
        let client = self.pool.get().await?;
        let mut entries = Vec::new();

        // Issue #73: read back the bare table-name suffix, not the persisted
        // fully-qualified `target_table` — `QuarantineTarget::Transform`
        // round-trips through `docs/decisions/0003`'s `transform.column`
        // addressing scheme elsewhere in this API (`quarantine_status`,
        // `resume_column`, `sample_quarantined`, all keyed on the bare name
        // the grammar accepts back), which parses an address on its first
        // `.` — handing it a qualified `"schema.table"` spelling here would
        // make every entry in this list misparse as a column address the
        // moment a target table lived outside the default schema.
        let quarantined_transforms = client
            .query(
                "select split_part(target_table, '.', 2) from transform_definitions \
                 where status = 'quarantined' order by target_table",
                &[],
            )
            .await?;
        entries.extend(
            quarantined_transforms
                .into_iter()
                .map(|row| QuarantineEntry {
                    target: QuarantineTarget::Transform(row.get(0)),
                    state: QuarantineState::Quarantined,
                    paused_at: None,
                    last_error: None,
                }),
        );

        let paused_columns = client
            .query(
                "select transform_table, column_name, paused_at, last_error from column_status \
                 order by transform_table, column_name",
                &[],
            )
            .await?;
        entries.extend(paused_columns.into_iter().map(|row| QuarantineEntry {
            target: QuarantineTarget::Column(row.get(0), row.get(1)),
            state: QuarantineState::Paused,
            paused_at: Some(row.get(2)),
            last_error: row.get(3),
        }));

        Ok(entries)
    }

    /// The current state of one target (`transform` or `transform.column`,
    /// per ADR-0003's amendment addressing scheme) — read 2. Errors with
    /// [`TrellisError::TransformNotFound`] if the named transform doesn't
    /// exist at all; a `transform.column` address for a real transform whose
    /// named column simply isn't paused reports [`QuarantineState::Live`],
    /// not an error (this call doesn't validate that the column name is one
    /// of the transform's actual fields — a paused column always is one, by
    /// construction, but a live one is reported the same way regardless of
    /// whether the name is real, matching "no rows here means live" for
    /// every column not individually tracked).
    pub async fn quarantine_status(&self, target: &str) -> Result<QuarantineEntry, TrellisError> {
        let target = QuarantineTarget::parse(target);
        let client = self.pool.get().await?;
        match &target {
            QuarantineTarget::Transform(t) => {
                // Issue #73: `t` is the bare name `QuarantineTarget::parse`
                // extracted from the caller's address — match against
                // `target_table`'s bare table-name suffix, not the persisted
                // qualified column (see `status`'s own call site for the
                // same reasoning).
                let row = client
                    .query_opt(
                        "select status, id from transform_definitions \
                         where split_part(target_table, '.', 2) = $1",
                        &[t],
                    )
                    .await?
                    .ok_or_else(|| TrellisError::TransformNotFound(t.clone()))?;
                let status_text: String = row.get(0);
                let stored = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                    panic!("transform_definitions.status held unrecognized value '{status_text}'")
                });
                // The status `Trellis::status` reports (issue #497).
                let status = reported_status(&**client, row.get(1), stored).await?;
                Ok(QuarantineEntry {
                    target,
                    state: QuarantineState::from(status),
                    paused_at: None,
                    last_error: None,
                })
            }
            QuarantineTarget::Column(t, c) => {
                // Issue #73: same bare-suffix match as the `Transform` arm
                // above — `t` is bare, `target_table` is qualified.
                let transform_exists: bool = client
                    .query_one(
                        "select exists(select 1 from transform_definitions \
                         where split_part(target_table, '.', 2) = $1)",
                        &[t],
                    )
                    .await?
                    .get(0);
                if !transform_exists {
                    return Err(TrellisError::TransformNotFound(t.clone()));
                }
                let row = client
                    .query_opt(
                        "select paused_at, last_error from column_status \
                         where transform_table = $1 and column_name = $2",
                        &[t, c],
                    )
                    .await?;
                Ok(match row {
                    Some(row) => QuarantineEntry {
                        target,
                        state: QuarantineState::Paused,
                        paused_at: Some(row.get(0)),
                        last_error: row.get(1),
                    },
                    None => QuarantineEntry {
                        target,
                        state: QuarantineState::Live,
                        paused_at: None,
                        last_error: None,
                    },
                })
            }
        }
    }

    /// A paginated batch of `(src_table, key, error_message)` triples for
    /// `target` — read 3, to diagnose and clear a quarantine's cause.
    /// `after` is a keyset cursor (the last row's `(src_table, key)` from a
    /// previous page); `None` starts from the beginning. Ordered by
    /// `(src_table, key)`.
    ///
    /// For a `transform.column` target, pulls from `column_failures` — the
    /// column fuse's own per-row bookkeeping (see
    /// `staging::quarantine`'s module doc comment for why that's a
    /// dedicated table rather than reusing `poison`: a column-level failure
    /// never evicts the row, so it can't live in the same table whose row
    /// presence means "left out of the definition's apply"). For a whole
    /// `transform` target, pulls from `poison` filtered to that transform's
    /// own rows — the coarser, whole-key fuse's own detail.
    pub async fn sample_quarantined(
        &self,
        target: &str,
        after: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<PoisonSample>, TrellisError> {
        let target = QuarantineTarget::parse(target);
        let client = self.pool.get().await?;
        let rows = match &target {
            QuarantineTarget::Column(t, c) => match &after {
                Some((after_src, after_key)) => {
                    client
                        .query(
                            "select src_table, key, error from column_failures \
                             where transform_table = $1 and column_name = $2 \
                               and (src_table, key) > ($3, $4) \
                             order by src_table, key limit $5",
                            &[t, c, after_src, after_key, &limit],
                        )
                        .await?
                }
                None => {
                    client
                        .query(
                            "select src_table, key, error from column_failures \
                             where transform_table = $1 and column_name = $2 \
                             order by src_table, key limit $3",
                            &[t, c, &limit],
                        )
                        .await?
                }
            },
            QuarantineTarget::Transform(t) => {
                // Issue #73: `t` is bare — same `split_part` match as
                // `status`/`quarantine_status`. Whole-key poison is per
                // transform (#799), so the sample is this definition's own
                // rows, whatever spelling of its source they carry.
                let id: i64 = client
                    .query_opt(
                        "select id from transform_definitions \
                         where split_part(target_table, '.', 2) = $1",
                        &[t],
                    )
                    .await?
                    .ok_or_else(|| TrellisError::TransformNotFound(t.clone()))?
                    .get(0);
                match &after {
                    Some((after_src, after_key)) => {
                        client
                            .query(
                                "select src_table, key, last_error from poison \
                                 where transform_id = $1 and (src_table, key) > ($2, $3) \
                                 order by src_table, key limit $4",
                                &[&id, after_src, after_key, &limit],
                            )
                            .await?
                    }
                    None => {
                        client
                            .query(
                                "select src_table, key, last_error from poison \
                                 where transform_id = $1 \
                                 order by src_table, key limit $2",
                                &[&id, &limit],
                            )
                            .await?
                    }
                }
            }
        };
        Ok(rows
            .into_iter()
            .map(|row| PoisonSample {
                src_table: row.get(0),
                key: row.get(1),
                error_message: row.get(2),
            })
            .collect())
    }

    /// Releases one key `transform` holds in quarantine
    /// (`docs/decisions/0003-quarantine-storage-and-api.md`, "Releasing held
    /// keys"), once its cause is fixed: `source_table` and `key` as
    /// [`Trellis::sample_quarantined`] and [`Trellis::poisoned_since`] report
    /// them, the table in either spelling, `schema.table` or bare (#283), and
    /// `transform` the bare target, as [`Trellis::status`] takes it.
    ///
    /// In one transaction, it deletes the key's quarantine rows for
    /// `transform` and stages a recompute of the key, which the drain
    /// applies like any other change: every definition reading the table
    /// re-derives the key from its current row, and the changes held for
    /// `transform` meanwhile are discarded rather than replayed, since the
    /// current row supersedes them. Another definition that holds the same
    /// key keeps holding it. If the cause is still there, the recompute
    /// fails as the changes before it did, and the key is poisoned again.
    ///
    /// Purely operational, like [`Trellis::status`], so not a statement of
    /// [`Trellis::apply`]: releasing a key changes nothing about the
    /// definitions. Resuming the definition releases every key it holds.
    ///
    /// Errors with [`TrellisError::TransformNotFound`] if no transform is
    /// registered as `transform`, and with [`ApplyError::KeyNotHeld`]
    /// (wrapped in [`TrellisError::Apply`], [`ErrorCode::NotFound`]),
    /// changing nothing, if it holds no such key: a table it doesn't read, a
    /// key it never held, or one already released.
    ///
    /// The release first waits for the drain pages in flight on the key's
    /// table to commit, so it never misses a change one of them is parking
    /// for the key. A page that holds it past the session's `lock_timeout`
    /// ([`crate::locks::LOCK_TIMEOUT`], 30 s) makes it error with
    /// [`ApplyError::ReleaseLockTimeout`] (wrapped in
    /// [`TrellisError::Apply`], [`ErrorCode::Timeout`]), changing nothing:
    /// the key is still held, and retrying the call releases it once the
    /// pages commit.
    pub async fn release_key(
        &self,
        transform: &str,
        source_table: &str,
        key: &str,
    ) -> Result<(), TrellisError> {
        match quarantine::release_key(&self.pool, transform, source_table, key).await {
            Ok(_) => Ok(()),
            Err(ApplyError::TransformNotFound { transform }) => {
                Err(TrellisError::TransformNotFound(transform))
            }
            Err(err) => Err(TrellisError::Apply(err)),
        }
    }

    /// Stops any background work this connection started (staging worker and
    /// drain workers) and waits for it to exit cleanly. A no-op for a
    /// connection that started none.
    pub async fn shutdown(self) -> Result<(), TrellisError> {
        if let Some(client) = self.client {
            client.shutdown().await.map_err(TrellisError::Client)?;
        }
        Ok(())
    }

    /// Whether at least one live drain worker (a connection running with
    /// `drain_threads > 0`) is registered anywhere in this fleet right now
    /// (issue #144; `docs/decisions/0010-embeddable-clients.md`, decision 3
    /// — the epic #140 hazard this closes). A single, cheap `exists(...)`
    /// query with no joins — meant to sit behind an application health check
    /// that runs on a timer, not just be called once at boot.
    ///
    /// **What this detects.** The recommended embedded-deployment shape runs
    /// web/migration processes at `drain_threads: 0` and a dedicated worker
    /// process doing drain work. Forget to deploy that process, or scale it
    /// to zero, and every transform this fleet defines sits in
    /// [`TransformStatus::WaitingToBackfill`] forever — nothing errors,
    /// nothing looks broken, the pipeline just never starts. `false` here is
    /// that misconfiguration, directly observable rather than inferred from
    /// a transform that never seems to finish backfilling. It counts drain
    /// workers only: a fleet missing its staging worker stalls the same way
    /// and passes this check, which is what
    /// [`Trellis::has_live_staging_worker`] is for (issue #428). See
    /// `docs/embedding.md`'s health-check section for a worked Phoenix/Rails
    /// example.
    ///
    /// **"Live" reuses the reclaim TTL's own notion of liveness** rather
    /// than inventing a second one, per the issue's explicit instruction:
    /// this compares each worker's last heartbeat against
    /// [`DEFAULT_RECLAIM_TTL`] — the exact threshold
    /// [`crate::staging::liveness::reclaim_stale`] already uses to decide a
    /// *claim* is dead, and the same value [`ClientOptions::default`]'s own
    /// `reclaim_ttl` carries (this facade never exposes a way to override
    /// it — every [`Trellis::connect`] call already gets this one value
    /// today, background `Client` or not). See
    /// [`crate::staging::worker_registry`]'s doc comment for why this is a
    /// read-time comparison rather than something that needs a reclaim pass
    /// to have already run — a design that did would be wrong precisely in
    /// the all-`drain_threads: 0` fleet this method exists to catch, since
    /// nothing in such a fleet would ever run one.
    pub async fn has_live_drain_workers(&self) -> Result<bool, TrellisError> {
        let client = self.pool.get().await?;
        Ok(worker_registry::has_live_workers(&**client, DEFAULT_RECLAIM_TTL).await?)
    }

    /// Whether this instance's staging worker (a connection running with
    /// `staging: true`) is running anywhere in the fleet right now (issue
    /// #428) — [`Trellis::has_live_drain_workers`]'s counterpart, and meant
    /// to sit behind the same timer-driven health check.
    ///
    /// **What this detects.** The staging worker installs change capture on
    /// every source and runs the maintenance loop that seals what capture
    /// stages and dispatches every new transform's backfill. A fleet with
    /// drain workers but no staging worker passes
    /// [`Trellis::has_live_drain_workers`] while nothing seals what capture
    /// stages, so no captured write drains, no new source is captured, and
    /// every new transform sits in [`TransformStatus::WaitingToBackfill`]
    /// forever. `false` here is that misconfiguration. A healthy fleet
    /// needs both checks to be `true`.
    ///
    /// **"Running" means holding the staging-worker singleton**, the
    /// session-scoped advisory lock the staging worker's maintenance loop
    /// holds on its own connection for as long as it runs (see
    /// `staging::ProducerSession`). One `pg_locks` read, no
    /// heartbeat: a crashed worker's connection closes and Postgres frees
    /// the lock with it. It also reads `false` for the tick or so the loop
    /// takes to reconnect after a failed step. Changes are captured by
    /// triggers in the application's own transactions meanwhile (issue #622),
    /// but nothing seals or dispatches a backfill.
    pub async fn has_live_staging_worker(&self) -> Result<bool, TrellisError> {
        let client = self.pool.get().await?;
        Ok(crate::staging::session::producer_is_running(&**client, self.config.schema()).await?)
    }

    /// A read-your-writes watermark (issue #192): `pg_current_wal_insert_lsn()`,
    /// read on a freshly acquired pool connection — see
    /// [`crate::staging::converge::watermark_token`] for the exact
    /// semantics.
    ///
    /// **Call this after the write you want reflected has committed.**
    /// Taken any earlier, the token bounds the write from *below* instead of
    /// above, and [`Trellis::await_converged`] could then return before the
    /// write is actually applied to its target(s)
    /// (docs/staging-and-claiming/07-convergence-and-await.md, "Watermark
    /// tokens"). The write itself doesn't need to go through this same
    /// connection or even through [`Trellis`] at all — any connection works,
    /// as long as this call happens after the write's commit returns:
    /// `pg_current_wal_insert_lsn()` reports the server's current WAL insert
    /// position, not something scoped to one session, so it's guaranteed to
    /// be at or past the write's own commit record by the time this query
    /// runs, even for a writer with `synchronous_commit = off` (issue #697).
    ///
    /// Pairs with [`Trellis::await_converged`]:
    ///
    /// ```no_run
    /// # async fn example(trellis: &trellis::Trellis) -> Result<(), trellis::TrellisError> {
    /// // ... write to a source table (any connection), and let the commit
    /// // return ...
    /// let token = trellis.watermark_token().await?;
    /// trellis
    ///     .await_converged(token, std::time::Duration::from_secs(30))
    ///     .await?;
    /// // Every `live` target fed by that write is now guaranteed to reflect
    /// // it; one still building may not yet (see `Trellis::await_converged`).
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watermark_token(&self) -> Result<PgLsn, TrellisError> {
        let client = self.pool.get().await?;
        Ok(converge::watermark_token(&**client).await?)
    }

    /// Blocks until every effect committed at or before `token` (see
    /// [`Trellis::watermark_token`]) has been reflected in its target(s), or
    /// `timeout` elapses — the read-your-writes primitive an embedder
    /// reaches for after writing to a source table and needing to see that
    /// write's effects in a transform's target table.
    ///
    /// A thin facade over [`crate::staging::converge::await_converged`] (see
    /// its doc comment for the polling/backoff shape and exactly what
    /// "reflected" means). Errors with
    /// [`TrellisError::Staging`]`(`[`StagingError::ConvergenceTimeout`]`)`
    /// on timeout — a named, matchable condition rather than a generic
    /// failure, whose [`TrellisError::code`] is [`ErrorCode::Timeout`].
    /// `timeout` holds even while a poll is blocked, on a lock say: the
    /// server abandons the poll at the deadline, or 250ms into the poll if
    /// that's later, since every poll gets at least that long (issue #596).
    /// So even a zero `timeout` makes one real check.
    ///
    /// Capture triggers write a change's ring rows in the writer's own
    /// transaction (issue #622), so every commit at or below `token` is
    /// already in the ring when the token is read: this waits only for the
    /// ring rows at or below it to drain, and writes nothing.
    ///
    /// This waits for captured changes only. It doesn't read definition
    /// status or backfill progress: a definition that isn't
    /// [`TransformStatus::Live`] yet (including one still
    /// [`TransformStatus::CatchingUp`]) may still be missing rows after this
    /// returns. Check [`Trellis::status`] for that. Once it reports `live`,
    /// a token taken after a commit and awaited here covers that commit
    /// (ADR-0016, "What `live` promises").
    ///
    /// Holds one pooled connection for the whole call (it polls on it), so a
    /// long `timeout` on a small `pool_max_size` is a real, if bounded, draw
    /// on this [`Trellis`]'s own pool — note the background [`Client`]'s
    /// drain workers use a *separate* pool, so this can't starve them.
    pub async fn await_converged(
        &self,
        token: PgLsn,
        timeout: Duration,
    ) -> Result<(), TrellisError> {
        let client = self.pool.get().await?;
        Ok(converge::await_converged(&client, token, timeout).await?)
    }

    /// Audits one target's persisted rows against an independently-rendered
    /// Postgres recompute — issue #174, ADR-0013's production recompute
    /// audit. Read-only.
    ///
    /// `target_table` names a registered transform, same convention as
    /// [`Trellis::status`]/[`Trellis::quarantine_status`] (its bare target
    /// table name). `scope` bounds this call to one keyset page — there is
    /// no unbounded "check everything" convenience method here; a
    /// fleet-wide sweep is a caller-side loop over
    /// [`Trellis::definitions`] and repeated `self_check` calls chained by
    /// [`SelfCheckReport::next_after`]. `mode` picks
    /// [`SelfCheckMode::Standard`] (safe under live load: a divergence must
    /// survive a re-check behind a fresh await before it's reported) or
    /// [`SelfCheckMode::Strict`] (skips the re-check — sound only once the
    /// caller has itself stopped writes to the audited tables). `timeout`
    /// bounds each convergence await this call makes (one under
    /// [`SelfCheckMode::Strict`], up to two under
    /// [`SelfCheckMode::Standard`]) — see [`Trellis::await_converged`]'s own
    /// doc comment for how to size it; a target that's merely still
    /// catching up reports [`crate::staging::self_check::SelfCheckOutcome::NotCaughtUp`],
    /// never a divergence.
    ///
    /// Only a [`crate::defs::ast::KeySpace::OneToOne`] target is supported
    /// this issue (see the module doc comment on
    /// [`crate::staging::self_check`]); an aggregate target's audit errors
    /// with [`SelfCheckError::UnsupportedKeySpace`], wrapped in
    /// [`TrellisError::SelfCheck`].
    ///
    /// A currently-paused column (`docs/decisions/0003-quarantine-storage-and-api.md`)
    /// is excluded from the comparison entirely — its persisted value is
    /// deliberately stale, so comparing it would report a false divergence.
    ///
    /// Every report carries the keys the definition holds in quarantine
    /// ([`SelfCheckReport::held_keys`], #759), whatever the outcome, so a
    /// held key isn't hidden behind a `Converged` page that didn't reach it
    /// or a `NotCaughtUp` its parked changes cause.
    pub async fn self_check(
        &self,
        target_table: &str,
        scope: SelfCheckScope,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> Result<SelfCheckReport, TrellisError> {
        crate::staging::self_check::self_check(&self.pool, target_table, scope, mode, timeout)
            .await
            .map_err(TrellisError::SelfCheck)
    }

    /// Starts the background [`Client`] for a `staging`/`drain_threads`
    /// connection. The staging worker reads the tables to publish from the
    /// catalog itself (issue #427), so an empty catalog is fine.
    fn start_client(config: &Config, options: &TrellisOptions) -> Result<Client, TrellisError> {
        let client_options = ClientOptions {
            staging_worker: options.staging,
            application_threads: options.drain_threads,
            ..Default::default()
        };
        // Issue #234: `start_with_config`, not `start(config.dsn(), ..)` —
        // the latter threw this `Config`'s schema away and re-resolved one
        // from the process environment, so a `Trellis` explicitly configured
        // into a named schema ran its background client against the *default*
        // instance's staging ring instead. See `Client::start_with_config`.
        Client::start_with_config(config.clone(), client_options).map_err(TrellisError::Client)
    }

    /// Introspects `source_table`'s column names and types for the definition
    /// validator, the same `information_schema` read
    /// [`defs::install_definition`] expects its caller to supply.
    ///
    /// Broader sweep, reviewer follow-up to issue #74 (epic #78's own
    /// whole-branch review): this used to query `information_schema.columns`
    /// with a bare `table_name = $1 and table_schema = any(current_schemas(false))`
    /// `search_path` walk — the same no-fallback shape every other fixed gap
    /// in this round had — fed straight from [`defs::ast::TransformDef::source`],
    /// which also completely ignored
    /// [`defs::ast::TransformDef::explicit_source_schema`] (issue #76).
    /// So *this*, the very first thing [`Trellis::apply`] does with a
    /// parsed definition, rejected both an explicitly-qualified `FROM
    /// <schema>.<table>` naming a table outside this connection's
    /// `search_path`, and a bare `FROM <table>` chaining off another
    /// definition's target explicitly qualified into a non-default schema
    /// (issue #76) — before `install_definition`'s own, already-fixed
    /// resolution (`resolve_source_for_install`) was ever reached: this call
    /// happens first, in [`Trellis::apply`], and returns
    /// [`TrellisError::SourceTableNotFound`] eagerly on an empty result, so
    /// `install_definition` was never even called. Now resolves the source
    /// the same way `resolve_source_for_install` does — an explicit schema
    /// names that exact relation directly, a bare name goes through
    /// [`defs::catalog::resolve_graph_identity`]'s two-step (physical
    /// `search_path` lookup, falling back to a live definition's own bare
    /// target-suffix) — then queries `information_schema.columns` by the
    /// resolved `table_schema`/`table_name` pair directly, rather than
    /// walking `search_path` a second time.
    async fn source_columns(
        &self,
        source_table: &str,
        explicit_source_schema: Option<&str>,
    ) -> Result<HashMap<String, ValueType>, TrellisError> {
        let qualified = match explicit_source_schema {
            Some(schema) => {
                crate::intake::markers::qualify(schema, source_table).map_err(CatalogError::from)?
            }
            None => defs::catalog::resolve_graph_identity(&self.pool, source_table).await?,
        };
        debug_assert!(
            qualified.contains('.'),
            "resolve_graph_identity/qualify always return a schema.table-shaped string"
        );

        // The same introspection a resume re-validates against
        // (`defs::catalog::revalidate`).
        let client = self.pool.get().await?;
        let columns = defs::catalog::live_source_columns(&**client, &qualified).await?;
        if columns.is_empty() {
            return Err(TrellisError::SourceTableNotFound(source_table.to_string()));
        }
        Ok(columns)
    }
}

/// The status an operator reads for definition `id`, persisted as `stored`
/// (issue #497, `defs::catalog::reported_statuses`). Only a `live` one can
/// differ, so any other skips the catalog read.
async fn reported_status(
    client: &impl tokio_postgres::GenericClient,
    id: i64,
    stored: TransformStatus,
) -> Result<TransformStatus, TrellisError> {
    if stored != TransformStatus::Live {
        return Ok(stored);
    }
    Ok(crate::defs::catalog::reported_statuses(client)
        .await?
        .get(&id)
        .copied()
        .unwrap_or(stored))
}

/// The columns [`backfill_failure`] reads a failing build chunk from, joined
/// by [`CHUNK_FAILURE_JOIN`] (#616).
const CHUNK_FAILURE_COLUMNS: &str = "bcf.attempts as chunk_attempts, \
     bcf.last_error as chunk_last_error, bcf.next_attempt_at as chunk_next_attempt_at";

/// The definition's current build chunk that has failed, if any, aliased
/// `bcf` for [`CHUNK_FAILURE_COLUMNS`] (#616): the one that has failed most,
/// leaving out finished chunks and those of a build a resume superseded.
const CHUNK_FAILURE_JOIN: &str = "left join lateral ( \
         select bc.attempts, bc.last_error, bc.next_attempt_at from backfill_chunks bc \
         where bc.definition_id = d.id and not bc.done and bc.last_error is not null \
           and bc.fuse_rearmed_at is not distinct from d.fuse_rearmed_at \
         order by bc.attempts desc, bc.id limit 1 \
     ) bcf on true";

/// The [`BackfillFailure`] in a row of [`Trellis::status`]'s or
/// [`Trellis::definitions`]' query. The definition's own failing build chunk
/// comes first ([`CHUNK_FAILURE_COLUMNS`], #616): its `attempts`,
/// `last_error` and `next_attempt_at` on the definition's `source_table`.
/// Otherwise `pending_backfill`'s `table_name`, `attempts`, `last_error` and
/// `next_attempt_at`, selected as `backfill_table`, `backfill_attempts`,
/// `backfill_last_error` and `backfill_next_attempt_at` from a left join on
/// the definition's source table where `last_error` is set. `None` when
/// neither found a failure. Read by name, so the two queries can order their
/// columns freely.
fn backfill_failure(row: &tokio_postgres::Row) -> Option<BackfillFailure> {
    if let Some(last_error) = row.get::<_, Option<String>>("chunk_last_error") {
        return Some(BackfillFailure {
            source_table: row.get("source_table"),
            attempts: u32::try_from(row.get::<_, i32>("chunk_attempts")).unwrap_or(0),
            last_error,
            next_attempt_at: row.get("chunk_next_attempt_at"),
        });
    }
    row.get::<_, Option<String>>("backfill_table")
        .map(|source_table| BackfillFailure {
            source_table,
            attempts: u32::try_from(row.get::<_, i32>("backfill_attempts")).unwrap_or(0),
            last_error: row.get("backfill_last_error"),
            next_attempt_at: row.get("backfill_next_attempt_at"),
        })
}

/// The [`CaptureFailure`] in a row of [`Trellis::status`]'s or
/// [`Trellis::definitions`]' query: `capture_failures`' `kind`,
/// `source_table`, `columns`, `error` and `detected_at`, selected as
/// `capture_kind`, `capture_table`, `capture_columns`, `capture_error` and
/// `capture_detected_at` from a left join on the definition's id.
fn capture_failure(row: &tokio_postgres::Row) -> Option<CaptureFailure> {
    row.get::<_, Option<String>>("capture_table")
        .map(|source_table| CaptureFailure {
            kind: CaptureFailureKind::from_persisted(row.get("capture_kind")),
            source_table,
            columns: row.get("capture_columns"),
            error: row.get("capture_error"),
            detected_at: row.get("capture_detected_at"),
        })
}

/// One registered transform definition's status, as [`Trellis::status`]
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionStatus {
    /// Where the transform is in its lifecycle (issue #55).
    pub status: TransformStatus,
    /// Set while the definition's build keeps failing. Either:
    ///
    /// - one of its build chunks failed (#616), and is waiting out a backoff
    ///   before it is retried, or is being narrowed to the key it fails on.
    ///   It clears once the chunk finishes. A key the narrowing quarantines
    ///   is reported by [`Trellis::quarantined`] instead, and the build
    ///   finishes without it. A definition `paused` with this set was paused
    ///   by its build: a chunk kept failing in a way no retry or narrowing
    ///   gets past (a missing table or column, say). Fix the cause and resume
    ///   it, which rebuilds it and clears this;
    /// - or the backfill marker parked on the definition's source table
    ///   keeps failing to discharge (issue #407, ADR-0016), or carries the
    ///   error of a direct build that failed and was handed back to it
    ///   (issue #419). That discharge runs every build of a
    ///   `waiting_to_backfill` definition on the table and the catch-up of
    ///   every `catching_up` one, so its failure is reported on each
    ///   definition that reads the table, whatever its status. A definition
    ///   stuck in `waiting_to_backfill` with this set is waiting on the cause
    ///   named in [`BackfillFailure::last_error`], not on the discharge's
    ///   turn. It clears once the discharge succeeds or the table is parked
    ///   again.
    ///
    /// The definition's own chunk is reported when both are set.
    pub backfill_failure: Option<BackfillFailure>,
    /// Set while the definition waits on capture, because the staging worker
    /// can't yet take the lock it needs to install or widen the capture
    /// triggers on its source, or on the to-side of a relationship declared
    /// on it (issue #622). It clears on its own once the lock holder lets
    /// go. A definition waits on capture while it is `waiting_to_backfill`,
    /// or while an `ALTER TRANSFORM` field it gained is paused until the
    /// capture images the column it reads (it is `backfilling` then, its
    /// field build waiting to start, #687, #625 F8b).
    /// One a schema change paused doesn't: it waits for the resume its
    /// `capture_failure` asks for (#705). The staging worker's latest pass records it in the catalog
    /// (`capture_holdups`), so every process reports it, wherever the worker
    /// runs.
    pub capture_wait: Option<CaptureWait>,
    /// Set while capture of a table the definition reads is broken, and
    /// only fixing the cause gets the definition going again. Either:
    ///
    /// - a schema change paused it (issue #622 C6): a column it reads was
    ///   renamed or dropped, or its source's primary key was redefined
    ///   (#687). The application's writes go on succeeding, and the other
    ///   definitions on the table keep applying. Recorded in the catalog
    ///   and cleared by resuming the definition, which rebuilds it;
    /// - or row-level security on a table it reads came to apply to the
    ///   Trellis role after it was defined (issue #745): its policies would
    ///   filter every read of the table. The staging worker's capture pass
    ///   pauses it, with no `columns`. Exempt the role and resume the
    ///   definition, which rebuilds it and clears this;
    /// - or row-level security on its own target came to apply to the role
    ///   the workers write it as (issue #765): its policies would filter
    ///   apply's writes. The staging worker's capture pass pauses it, with
    ///   no `columns` and the target as `source_table`. Exempt the role and
    ///   resume the definition, which rebuilds it and clears this;
    /// - or a logical-replication subscription came to replicate into a
    ///   table it reads after it was defined (issue #751): capture never
    ///   sees the subscription's changes. The staging worker's capture pass
    ///   pauses it, with no `columns`. Stop replicating into the table and
    ///   resume the definition, which rebuilds it and clears this;
    /// - or, while it waits on capture as for `capture_wait`, the staging
    ///   worker's install or widen fails for a reason other than a lock (no
    ///   primary key, a statement that fails, #687). Every pass retries it,
    ///   and it clears once one succeeds. Recorded like `capture_wait`;
    /// - or a column it keys by changed type or collation, or a column it
    ///   keeps a typed copy of widened (issues #760, #767): define would now
    ///   refuse the column, a relationship's join columns no longer match,
    ///   the stored keys render differently, or a copy can't hold the new
    ///   type. The staging worker's capture pass pauses it, naming the
    ///   columns, their types and what to do. Resume it once define would
    ///   accept it (a resume refuses until then), which re-types Trellis's
    ///   copies and rebuilds it and clears this. While the staging worker
    ///   re-types the copies for a resume, it stays paused with a record
    ///   whose `error` starts `resuming:`; if a re-type fails, the record
    ///   says why;
    /// - or the drain halted on it (issue #663), with
    ///   [`CaptureFailureKind::Halt`]: a failure no retry or quarantine gets
    ///   past (a key the drain can't use, a propagation wave past the hop
    ///   bound, an aggregate off the ledger) reached it, so the drain paused
    ///   it, everything else reading what it reads, and everything
    ///   downstream of them, and drained the rest. Fix the cause and resume
    ///   the definition, which rebuilds it and clears this;
    /// - or its Re-derive build's group-delta merge halted on it (issue
    ///   #901), also with [`CaptureFailureKind::Halt`]: the merge into its
    ///   target was refused, or kept failing in a way no retry gets past (a
    ///   check or deferred constraint on the target that a merged group
    ///   breaks), so it paused, with everything downstream of its target.
    ///   `source_table` names the target. Fix the cause and resume it, which
    ///   rebuilds it and clears this.
    ///
    /// Every case but the halts has [`CaptureFailureKind::Capture`]. A
    /// definition an operator paused has none.
    pub capture_failure: Option<CaptureFailure>,
    /// Set while the definition holds keys in quarantine (#759): source keys
    /// whose changes its apply leaves out, each because a change to it kept
    /// failing in this definition's apply, so their target rows stay as
    /// they were. Every other definition reading the same keys applies them
    /// as usual. A definition holds them whatever its status, `live`
    /// included, until each is released ([`Trellis::release_key`]) or the
    /// definition is resumed or dropped. [`Trellis::sample_quarantined`]
    /// pages the keys themselves, with each one's error.
    pub held_keys: Option<HeldKeys>,
    /// Set while the drain keeps failing on a page holding changes to a
    /// table the definition reads, as its source or through a relationship,
    /// with nothing charged or paused (#817): a refused read or write the
    /// catalog can't pin on a table (a column grant, a function's
    /// `EXECUTE`, one of Trellis's own tables), records that fail only
    /// together, isolation stopped at its probe limit, or an
    /// isolate-eligible failure whose retries ran out. Every drain pass
    /// retries the page, and the definition's target stops short of it until
    /// one succeeds: fix the cause the error names. Reported on every
    /// definition that isn't paused or quarantined, the oldest such page
    /// first, and cleared when the page commits. Separate from
    /// `capture_failure`: it pauses nothing.
    pub drain_failure: Option<DrainFailure>,
}

/// Why capture of a table a definition reads is broken (issue #622 C6,
/// #687); see [`DefinitionStatus::capture_failure`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureFailure {
    /// Whether capture broke, or the drain halted on the definition (#663).
    pub kind: CaptureFailureKind,
    /// The qualified captured table. For a halt, the table (or tables,
    /// joined with `", "`) the halting failure named, or the target, for an
    /// aggregate off the ledger or a build's merge that kept failing.
    pub source_table: String,
    /// The columns the failure is about: the renamed or dropped ones the
    /// definition reads (every missing column of the table when a
    /// primary-key column went), the old key columns when the key was
    /// redefined, the key columns whose type or collation changed (a type
    /// define would refuse, a re-rendering, a widening of a typed copy, a
    /// broken join pairing), or the missing column an install names. Empty
    /// for a failure that isn't about a column, and for a halt.
    pub columns: Vec<String>,
    /// A sentence naming the cause and, for a pause, what to do.
    pub error: String,
    /// When the drain or the staging worker first found it.
    pub detected_at: SystemTime,
}

/// What a [`CaptureFailure`] is (issue #663): `capture_failures.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CaptureFailureKind {
    /// Capture of a table the definition reads is broken: a schema change,
    /// row-level security, a subscription, or a failing install or widen.
    Capture,
    /// The drain, or a Re-derive build's group-delta merge, halted on the
    /// definition: a failure no retry or quarantine gets past reached it, so
    /// it was paused with the rest of the failure's closure
    /// (`staging::halt`).
    Halt,
}

impl CaptureFailureKind {
    /// Every kind, in declaration order.
    pub const ALL: [CaptureFailureKind; 2] =
        [CaptureFailureKind::Capture, CaptureFailureKind::Halt];

    /// The word `capture_failures.kind` stores: `capture` or `halt`.
    pub fn as_str(self) -> &'static str {
        match self {
            CaptureFailureKind::Capture => "capture",
            CaptureFailureKind::Halt => "halt",
        }
    }

    /// The kind `capture_failures.kind` stores. Its check constraint (V70)
    /// admits only these two.
    fn from_persisted(kind: &str) -> Self {
        match kind {
            "halt" => CaptureFailureKind::Halt,
            "capture" => CaptureFailureKind::Capture,
            other => panic!("capture_failures.kind held unrecognized value '{other}'"),
        }
    }
}

/// What a definition's capture is waiting on (issue #622 C5): the staging
/// worker's install or widen of the capture triggers on a table the
/// definition reads couldn't take the table's lock, because another session
/// holds or is queued for a conflicting one. Nothing cancels that session
/// (an autovacuum included), so the definition waits until it lets go; this
/// names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureWait {
    /// The qualified table whose lock the capture operation waits for.
    pub table: String,
    /// `install`, `widen` or `uninstall`.
    pub operation: String,
    /// The lock mode it asks for, as `pg_locks` spells it.
    pub lock_mode: String,
    /// When the staging worker first found the table locked.
    pub waiting_since: SystemTime,
    /// When it last read who holds the lock.
    pub observed_at: SystemTime,
    /// One line per session holding or queued for a conflicting lock: its
    /// pid (or prepared transaction), backend type (`autovacuum worker`,
    /// say), lock mode, for how long, and the start of its query.
    pub blockers: Vec<String>,
}

/// The retry state of a definition's failing build (see
/// [`DefinitionStatus::backfill_failure`]): one of its build chunks (#616),
/// or the backfill marker on its source table whose discharge has failed
/// (issue #407). Both are retried with capped exponential backoff. A chunk
/// that fails on its data is narrowed to the key that causes it, which is
/// quarantined; one that fails otherwise pauses the definition after a few
/// charged attempts. The marker's discharge never gives up on its own: fix
/// the cause (or drop the definitions reading the table) and the next
/// attempt goes through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillFailure {
    /// The qualified source table the build reads.
    pub source_table: String,
    /// How many times the chunk (counting the chunk it was split from), or
    /// the marker's discharge since it was parked, has failed.
    pub attempts: u32,
    /// The latest failure's error, as the worker's log reported it.
    pub last_error: String,
    /// The earliest time the build tries it again. For a definition its
    /// build paused, when it paused: nothing retries it until it is resumed.
    pub next_attempt_at: SystemTime,
}

/// One registered transform definition, as [`Trellis::definitions`] reports
/// it.
#[derive(Debug, Clone)]
pub struct DefinitionSummary {
    pub id: i64,
    pub target_table: String,
    pub source_table: String,
    pub source_version: i64,
    /// Where this transform is in its lifecycle (issue #55) — see
    /// [`TransformStatus`].
    pub status: TransformStatus,
    pub created_at: SystemTime,
    /// Why this definition's build keeps failing, if
    /// it does: the same value [`DefinitionStatus::backfill_failure`] reports
    /// (issue #461).
    pub backfill_failure: Option<BackfillFailure>,
    /// Set while the drain has halted on this definition (issue #663): the
    /// [`DefinitionStatus::capture_failure`] [`Trellis::status`] reports for
    /// it, always with [`CaptureFailureKind::Halt`]. The definition is
    /// `paused`. Listing every definition is the health read for halts: any
    /// with this set is stopped until the cause is fixed and it is resumed.
    pub halt: Option<CaptureFailure>,
}

/// One registered relationship declaration, as [`Trellis::relationships`]
/// reports it. `cardinality` is the persisted `"to_one"`/`"to_many"` string.
#[derive(Debug, Clone)]
pub struct RelationshipSummary {
    pub id: i64,
    pub name: String,
    /// The schema `from_table` resolved to when the relationship was declared
    /// — part of the relationship's identity alongside `from_table` and
    /// `name` (issue #288).
    pub from_schema: String,
    pub from_table: String,
    pub from_col: String,
    /// The schema `to_table` resolved to when the relationship was declared
    /// (issue #372): the to-side every reader uses.
    pub to_schema: String,
    pub to_table: String,
    pub to_col: String,
    pub cardinality: String,
    pub created_at: SystemTime,
}

/// What [`Trellis::apply`] did — the one return type every
/// definition-changing statement shares (issue #227).
///
/// One variant per operation, carrying only what that operation has to report
/// that the caller couldn't already know from the statement it wrote: the
/// registered [`Definition`]/[`RelationshipDefinition`] for the two defining
/// forms (an id, and the status a caller then polls), the resumed
/// `(transform, column)` pairs for a column resume, and nothing at all for a
/// pause or a drop.
///
/// `#[non_exhaustive]`, for the same reason [`ErrorCode`] is: this is a type
/// the caller is *meant* to match on from outside — eventually from outside
/// Rust — while the set of operations behind `apply` keeps growing (`AMEND`
/// next, once it has its own ADR — issue #228, decision 3). A match on it has
/// to tolerate a variant it doesn't know rather than assume the set is closed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Applied {
    /// A `TRANSFORM ...` statement registered a transform definition. Reports
    /// [`TransformStatus::WaitingToBackfill`]: its backfill runs in the
    /// background — see [`Trellis::apply`].
    TransformDefined(Definition),
    /// A `RELATIONSHIP ...` statement registered a relationship declaration.
    RelationshipDefined(RelationshipDefinition),
    /// A `PAUSE ...` statement froze its subject — or found it already frozen,
    /// which ADR-0014 makes the same success.
    Paused,
    /// A `RESUME ...` statement unfroze its subject.
    ///
    /// `columns` holds every `(transform, column)` pair a **column** resume
    /// actually resumed: the addressed column first, then any dependent whose
    /// pause was purely this one's cascade (a dependent with an independent
    /// reason to stay paused is deliberately left alone). Empty for a
    /// whole-transform resume, which has no per-column result to report — the
    /// transform drops to [`TransformStatus::WaitingToBackfill`] and rebuilds.
    Resumed { columns: Vec<(String, String)> },
    /// A `DROP ...` statement removed its subject — or found it already gone,
    /// which ADR-0014 makes the same success.
    Dropped,
    /// An `ALTER TRANSFORM ...` statement edited its subject's calculated
    /// fields (ADR-0015, issues #241/#242). `definition` is the edited
    /// definition's new state; `added`/`dropped`/`altered` name exactly the
    /// fields this call actually changed, excluding any clause that turned
    /// out to be an idempotent no-op (re-adding an existing field with the
    /// same formula, re-altering to the formula it already had, dropping an
    /// already-absent field) — all three lists are empty for a statement
    /// whose every clause was one of those.
    Altered {
        definition: Definition,
        added: Vec<String>,
        dropped: Vec<String>,
        altered: Vec<String>,
    },
}

impl Applied {
    /// The [`Definition`] a `TRANSFORM ...` statement registered, or `None`
    /// for any other outcome.
    ///
    /// For the common case where the caller wrote the statement itself and so
    /// already knows which form it was: `apply(...)?.into_transform().expect(...)`
    /// reads better than a `match` with an unreachable arm — and much better
    /// than the `let ... else` that an `#[non_exhaustive]` enum otherwise
    /// forces on every such call site.
    pub fn into_transform(self) -> Option<Definition> {
        match self {
            Applied::TransformDefined(definition) => Some(definition),
            _ => None,
        }
    }

    /// The [`RelationshipDefinition`] a `RELATIONSHIP ...` statement
    /// registered, or `None` for any other outcome — see
    /// [`into_transform`](Applied::into_transform).
    pub fn into_relationship(self) -> Option<RelationshipDefinition> {
        match self {
            Applied::RelationshipDefined(relationship) => Some(relationship),
            _ => None,
        }
    }
}

/// One poison-quarantine entry, as [`Trellis::poisoned_since`] reports it.
#[derive(Debug, Clone)]
pub struct PoisonEntry {
    /// The bare target of the definition the key is held for (#799).
    pub transform: String,
    pub src_table: String,
    pub key: String,
    pub last_error: String,
    pub poisoned_at: SystemTime,
}

/// A quarantine/status address (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// amendment, "Addressing"): either a whole transform (its whole keyspace,
/// from the transform-wide lifecycle/fuse) or one of its columns (from the
/// column-status table). Reuses the `table.column` shape the grammar already
/// has elsewhere for qualified column references, rather than inventing a
/// new addressing idiom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineTarget {
    /// The whole transform, named by its target table.
    Transform(String),
    /// One column of a transform, named `(target_table, column_name)`.
    Column(String, String),
}

impl QuarantineTarget {
    /// Parses `"transform"` or `"transform.column"`, splitting on the *first*
    /// `.`: everything after it is the column name.
    ///
    /// Transform and column names are grammar identifiers
    /// (`[A-Za-z_][A-Za-z0-9_]*`), so neither half of any target this crate
    /// reports contains a dot, and `parse(&target.to_string())` gives back
    /// `target`. A hand-built target whose *transform* half contains a dot
    /// does not round-trip: `Column("a.b", "c")` displays as `"a.b.c"`, which
    /// parses as `Column("a", "b.c")`. The address has no quoting rule to
    /// tell the two apart. A dot in the column half does survive.
    ///
    /// An address with either half empty (`"."`, `"orders."`, `".total"`) is
    /// treated as a bare transform name. [`fmt::Display`] never produces such
    /// a string from identifier names, so this only matters for a
    /// caller-supplied one.
    pub fn parse(address: &str) -> Self {
        match address.split_once('.') {
            Some((transform, column)) if !transform.is_empty() && !column.is_empty() => {
                QuarantineTarget::Column(transform.to_string(), column.to_string())
            }
            _ => QuarantineTarget::Transform(address.to_string()),
        }
    }

    /// The transform this target names, regardless of whether it addresses
    /// the whole thing or one column.
    pub fn transform(&self) -> &str {
        match self {
            QuarantineTarget::Transform(t) | QuarantineTarget::Column(t, _) => t,
        }
    }
}

impl fmt::Display for QuarantineTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuarantineTarget::Transform(t) => write!(f, "{t}"),
            QuarantineTarget::Column(t, c) => write!(f, "{t}.{c}"),
        }
    }
}

/// A target's current state, as [`Trellis::quarantined`]/[`Trellis::quarantine_status`]
/// report it — [`TransformStatus`]'s four lifecycle states for a
/// [`QuarantineTarget::Transform`] address, plus [`QuarantineState::Paused`]
/// for a [`QuarantineTarget::Column`] address (a state [`TransformStatus`]
/// has no equivalent of, since column pausing doesn't touch a transform's
/// overall lifecycle status at all).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineState {
    Live,
    WaitingToBackfill,
    Backfilling,
    /// Applied like a live transform, with a go-live catch-up still pending
    /// (mirrors [`TransformStatus::CatchingUp`], issue #476).
    CatchingUp,
    /// A whole transform's keyspace fuse has tripped (mirrors
    /// [`TransformStatus::Quarantined`]).
    Quarantined,
    /// Frozen without a whole-keyspace fuse having tripped.
    ///
    /// At **column** granularity: one column's own fuse has tripped, or it's
    /// paused only because an upstream column it reads is (decision #5's
    /// cascade) — both look the same from this read; see
    /// [`Trellis::quarantine_status`] for whether a caller needs to
    /// distinguish them (today, [`QuarantineEntry::last_error`] is `None` for
    /// a purely cascaded pause, since it never itself failed).
    ///
    /// At **whole-transform** granularity (issue #142): an operator
    /// deliberately froze it via `PAUSE TRANSFORM` ([`Trellis::apply`]), mirroring
    /// [`TransformStatus::Paused`] — the same freeze
    /// [`QuarantineState::Quarantined`] is, reached by the other of
    /// ADR-0014's two triggers.
    Paused,
}

impl QuarantineState {
    /// Every variant, once — the closed set an embedding binding allocates
    /// its host-side names (Elixir atoms, Ruby symbols) from at load time
    /// (`docs/decisions/0010-embeddable-clients.md`, decision 4), so it never
    /// has to hard-code the set itself. A new variant fails
    /// `every_variant_is_listed_in_all` below until it is added here too.
    pub const ALL: [QuarantineState; 6] = [
        QuarantineState::Live,
        QuarantineState::WaitingToBackfill,
        QuarantineState::Backfilling,
        QuarantineState::CatchingUp,
        QuarantineState::Quarantined,
        QuarantineState::Paused,
    ];

    /// A stable, lowercase `snake_case` name for this state — the form that
    /// crosses an FFI boundary. Each state [`TransformStatus`] mirrors uses
    /// that status's own [`TransformStatus::as_str`] word, so a host sees one
    /// name for one state whichever read reported it.
    pub fn as_str(self) -> &'static str {
        match self {
            QuarantineState::Live => "live",
            QuarantineState::WaitingToBackfill => "waiting_to_backfill",
            QuarantineState::Backfilling => "backfilling",
            QuarantineState::CatchingUp => "catching_up",
            QuarantineState::Quarantined => "quarantined",
            QuarantineState::Paused => "paused",
        }
    }
}

impl From<TransformStatus> for QuarantineState {
    fn from(status: TransformStatus) -> Self {
        match status {
            TransformStatus::WaitingToBackfill => QuarantineState::WaitingToBackfill,
            TransformStatus::Backfilling => QuarantineState::Backfilling,
            TransformStatus::CatchingUp => QuarantineState::CatchingUp,
            TransformStatus::Live => QuarantineState::Live,
            TransformStatus::Quarantined => QuarantineState::Quarantined,
            TransformStatus::Paused => QuarantineState::Paused,
        }
    }
}

/// One target's quarantine/status entry, as [`Trellis::quarantined`] (a
/// whole list) and [`Trellis::quarantine_status`] (one target) both report
/// it.
#[derive(Debug, Clone)]
pub struct QuarantineEntry {
    pub target: QuarantineTarget,
    pub state: QuarantineState,
    /// When this target's pause tripped — `Some` only for a
    /// [`QuarantineTarget::Column`] currently in [`QuarantineState::Paused`].
    pub paused_at: Option<SystemTime>,
    /// The most recent failure's message — `Some` only for a
    /// [`QuarantineTarget::Column`] currently in [`QuarantineState::Paused`]
    /// whose pause has an error of its own (a purely cascaded pause has
    /// none).
    pub last_error: Option<String>,
}

/// One sampled quarantined row, as [`Trellis::sample_quarantined`] reports
/// it — the ADR's `(src_table, key, error_message)` triple.
#[derive(Debug, Clone)]
pub struct PoisonSample {
    pub src_table: String,
    pub key: String,
    pub error_message: String,
}

/// Why a [`Trellis`] operation failed. Composes the crate's lower-level error
/// types via `From`, matching the hand-rolled-enum convention the rest of the
/// crate uses. [`TrellisError::code`] reports a stable, coarse [`ErrorCode`]
/// category for this error alongside its `Display` message — see
/// `docs/decisions/0008-public-api-design.md`, decision 3.
#[derive(Debug)]
pub enum TrellisError {
    /// A definition failed to parse before it could be registered.
    Parse(ParseError),
    /// A catalog operation (define/relationship/install/pause/drop) failed.
    Catalog(CatalogError),
    /// Starting or stopping the background client failed.
    Client(ClientError),
    /// A connection/config/migration-layer failure.
    Engine(crate::error::Error),
    /// A direct Postgres query this facade runs itself (introspection,
    /// listing, backfill request) failed.
    Db(tokio_postgres::Error),
    /// A named source table doesn't resolve on the connection's search path.
    SourceTableNotFound(String),
    /// [`Trellis::request_backfill`] was asked to backfill a table no
    /// registered definition reads, so the staging worker doesn't capture it
    /// (issue #622).
    TableNotCaptured { table: String },
    /// [`crate::blocking::BlockingTrellis::connect`]'s background thread
    /// failed to spawn.
    BlockingSpawn(std::io::Error),
    /// [`crate::blocking::BlockingTrellis::connect`]'s background thread
    /// exited (panicked, or its setup future dropped its ready-sender)
    /// before ever signalling ready.
    BlockingThreadExitedBeforeReady,
    /// A [`crate::blocking::BlockingTrellis`] method's background thread was
    /// no longer there to service the call (it panicked after connecting
    /// successfully) — the job channel send or reply recv failed.
    BlockingThreadGone,
    /// A [`crate::blocking::BlockingTrellis`] method was called from a thread
    /// that already has a `tokio` runtime entered (e.g. from inside
    /// `#[tokio::test]` or a `tokio::spawn`ed task). Blocking such a thread
    /// on the reply would panic inside `tokio`, so this is reported as a
    /// normal error instead — call the async [`Trellis`] directly in that
    /// context, `BlockingTrellis` is only for threads with no runtime of
    /// their own.
    CalledFromAsyncContext,
    /// A quarantine/status/resume call failed inside the staging layer's
    /// column-fuse machinery (`staging::quarantine`) — resuming a column
    /// that isn't paused, or a lower-level DB/catalog failure encountered
    /// while listing, reading, sampling, or resuming.
    Apply(ApplyError),
    /// A [`QuarantineTarget`] named a transform (whole or `.column`) that
    /// doesn't exist in `transform_definitions` at all.
    TransformNotFound(String),
    /// A `PAUSE TRANSFORM <target>.<column>` statement named a column that
    /// isn't one of `transform`'s declared calculated fields (issue #227) —
    /// most often a typo, or a `<schema>.<transform>` address written where
    /// this grammar reads `<transform>.<column>` (see [`Trellis::apply`]'s
    /// "Addressing"). `declared` lists the fields the definition does declare,
    /// so the message can show what was available.
    ColumnNotFound {
        transform: String,
        column: String,
        declared: Vec<String>,
    },
    /// [`Trellis::watermark_token`]/[`Trellis::await_converged`] hit a
    /// failure inside the staging ring's convergence machinery
    /// (`staging::converge`) — most commonly
    /// [`StagingError::ConvergenceTimeout`], but also a lower-level DB
    /// failure encountered while polling.
    Staging(StagingError),
    /// [`Trellis::self_check`] failed outright (as opposed to succeeding and
    /// reporting a divergence, which is a successful audit) — see
    /// [`SelfCheckError`].
    SelfCheck(SelfCheckError),
}

impl TrellisError {
    /// This error's stable, coarse [`ErrorCode`] category
    /// (`docs/decisions/0008-public-api-design.md`, decision 3). Delegates to the wrapped
    /// error's own `code()` wherever one nests here
    /// ([`TrellisError::Parse`], [`TrellisError::Catalog`],
    /// [`TrellisError::Client`], [`TrellisError::Engine`]) rather than
    /// hardcoding one category for a whole variant, so the mapping composes
    /// through nesting instead of re-deriving a category this crate already
    /// has one for.
    pub fn code(&self) -> ErrorCode {
        match self {
            TrellisError::Parse(err) => err.code(),
            TrellisError::Catalog(err) => err.code(),
            TrellisError::Client(err) => err.code(),
            TrellisError::Engine(err) => err.code(),
            TrellisError::Db(err) => error_code::classify_pg_error(err),
            // A backfill request against an unpublished table is a rejected
            // call given the connection's current state — same category as
            // any other invalid-configuration error.
            TrellisError::TableNotCaptured { .. } => ErrorCode::Validation,
            TrellisError::SourceTableNotFound(_) => ErrorCode::NotFound,
            // Same category as `ClientError`'s equivalent thread-lifecycle
            // variants — an embedder can't do anything about these beyond
            // retrying the whole connection.
            TrellisError::BlockingSpawn(_)
            | TrellisError::BlockingThreadExitedBeforeReady
            | TrellisError::BlockingThreadGone => ErrorCode::Internal,
            // Caller misuse (wrong calling context), not an engine fault.
            TrellisError::CalledFromAsyncContext => ErrorCode::Validation,
            TrellisError::Apply(err) => err.code(),
            TrellisError::TransformNotFound(_) | TrellisError::ColumnNotFound { .. } => {
                ErrorCode::NotFound
            }
            TrellisError::Staging(err) => err.code(),
            TrellisError::SelfCheck(err) => err.code(),
        }
    }
}

impl std::fmt::Display for TrellisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrellisError::Parse(err) => write!(f, "{err}"),
            TrellisError::Catalog(err) => write!(f, "{err}"),
            TrellisError::Client(err) => write!(f, "{err}"),
            TrellisError::Engine(err) => write!(f, "{err}"),
            TrellisError::Db(err) => {
                write!(f, "database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            TrellisError::SourceTableNotFound(table) => {
                write!(f, "source table \"{table}\" not found on the search path")
            }
            TrellisError::TableNotCaptured { table } => write!(
                f,
                "\"{table}\" isn't captured: no registered transform reads it, and a table's \
                 first reader backfills it automatically"
            ),
            TrellisError::BlockingSpawn(err) => {
                write!(f, "failed to spawn BlockingTrellis's runtime thread: {err}")
            }
            TrellisError::BlockingThreadExitedBeforeReady => write!(
                f,
                "BlockingTrellis's runtime thread exited before signalling that setup completed"
            ),
            TrellisError::BlockingThreadGone => write!(
                f,
                "BlockingTrellis's runtime thread was no longer running to service this call"
            ),
            TrellisError::CalledFromAsyncContext => write!(
                f,
                "BlockingTrellis was called from a thread that already has a tokio runtime \
                 entered; call the async Trellis directly in that context instead"
            ),
            TrellisError::Apply(err) => write!(f, "{err}"),
            TrellisError::TransformNotFound(target) => {
                write!(f, "no transform named \"{target}\" is registered")
            }
            TrellisError::ColumnNotFound {
                transform,
                column,
                declared,
            } => write!(
                f,
                "transform \"{transform}\" declares no column \"{column}\"; it declares: {}",
                if declared.is_empty() {
                    "(none)".to_string()
                } else {
                    declared.join(", ")
                }
            ),
            TrellisError::Staging(err) => write!(f, "{err}"),
            TrellisError::SelfCheck(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for TrellisError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TrellisError::Parse(err) => Some(err),
            TrellisError::Catalog(err) => Some(err),
            TrellisError::Client(err) => Some(err),
            TrellisError::Engine(err) => Some(err),
            TrellisError::Db(err) => Some(err),
            TrellisError::SourceTableNotFound(_)
            | TrellisError::TableNotCaptured { .. }
            | TrellisError::BlockingThreadExitedBeforeReady
            | TrellisError::BlockingThreadGone
            | TrellisError::CalledFromAsyncContext => None,
            TrellisError::BlockingSpawn(err) => Some(err),
            TrellisError::Apply(err) => Some(err),
            TrellisError::TransformNotFound(_) | TrellisError::ColumnNotFound { .. } => None,
            TrellisError::Staging(err) => Some(err),
            TrellisError::SelfCheck(err) => Some(err),
        }
    }
}

impl From<ApplyError> for TrellisError {
    fn from(err: ApplyError) -> Self {
        TrellisError::Apply(err)
    }
}

impl From<ParseError> for TrellisError {
    fn from(err: ParseError) -> Self {
        TrellisError::Parse(err)
    }
}

impl From<CatalogError> for TrellisError {
    fn from(err: CatalogError) -> Self {
        TrellisError::Catalog(err)
    }
}

impl From<ClientError> for TrellisError {
    fn from(err: ClientError) -> Self {
        TrellisError::Client(err)
    }
}

impl From<crate::error::Error> for TrellisError {
    fn from(err: crate::error::Error) -> Self {
        TrellisError::Engine(err)
    }
}

impl From<tokio_postgres::Error> for TrellisError {
    fn from(err: tokio_postgres::Error) -> Self {
        TrellisError::Db(err)
    }
}

impl From<StagingError> for TrellisError {
    fn from(err: StagingError) -> Self {
        TrellisError::Staging(err)
    }
}

impl From<SelfCheckError> for TrellisError {
    fn from(err: SelfCheckError) -> Self {
        TrellisError::SelfCheck(err)
    }
}

#[cfg(test)]
mod quarantine_target_tests {
    use super::*;

    fn column(transform: &str, column: &str) -> QuarantineTarget {
        QuarantineTarget::Column(transform.to_string(), column.to_string())
    }

    /// Every target this crate reports has identifier names, and those
    /// round-trip through their address.
    #[test]
    fn identifier_targets_round_trip_through_their_address() {
        for target in [
            QuarantineTarget::Transform("order_totals".to_string()),
            column("order_totals", "total"),
            column("_t2", "_c9"),
            // A dot in the column half survives: the split is on the first dot.
            column("order_totals", "a.b"),
        ] {
            assert_eq!(QuarantineTarget::parse(&target.to_string()), target);
        }
    }

    /// The limit `parse`'s doc states: a dot in the transform half is
    /// indistinguishable from the separator.
    #[test]
    fn a_dotted_transform_name_does_not_round_trip() {
        let target = column("a.b", "c");
        assert_eq!(target.to_string(), "a.b.c");
        assert_eq!(QuarantineTarget::parse("a.b.c"), column("a", "b.c"));
    }
}

#[cfg(test)]
mod quarantine_state_tests {
    use super::*;

    /// [`QuarantineState::ALL`] is exactly the enum's variants, each with its
    /// own `as_str` word: a state missing from it would reach a host as a
    /// name outside the set it allocated at load time.
    #[test]
    fn every_variant_is_listed_in_all() {
        crate::error_code::assert_all_is_every_variant!(
            QuarantineState: Live,
            WaitingToBackfill,
            Backfilling,
            CatchingUp,
            Quarantined,
            Paused,
        );
        let names: std::collections::HashSet<&str> =
            QuarantineState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(names.len(), QuarantineState::ALL.len());
    }

    /// A state that mirrors a [`TransformStatus`] is named exactly as that
    /// status is, so a host binding sees one word per state across
    /// [`Trellis::status`] and [`Trellis::quarantine_status`].
    #[test]
    fn mirrored_states_share_the_status_word() {
        for status in TransformStatus::ALL {
            assert_eq!(QuarantineState::from(status).as_str(), status.as_str());
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn source_table_not_found_is_not_found() {
        assert_eq!(
            TrellisError::SourceTableNotFound("widgets".to_string()).code(),
            ErrorCode::NotFound
        );
    }

    /// [`TrellisError::Catalog`] must delegate to [`CatalogError::code`]
    /// rather than hardcoding a category — the exact composition-through-
    /// nesting case `docs/decisions/0008-public-api-design.md`'s decision 3 calls out.
    #[test]
    fn catalog_delegates_to_the_wrapped_catalog_error() {
        let inner = CatalogError::SourceTableNotFound("orders".to_string());
        let expected = inner.code();
        let wrapped = TrellisError::Catalog(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::NotFound);
    }

    /// Two layers of nesting: [`TrellisError::Client`] wraps
    /// [`ClientError::Config`], which itself wraps [`crate::error::Error`] —
    /// the code must survive both hops unchanged.
    #[test]
    fn client_delegates_through_two_layers_of_nesting() {
        let inner = crate::error::Error::IncompatibleInstance("mismatched marker".to_string());
        let expected = inner.code();
        let wrapped = TrellisError::Client(ClientError::Config(inner));

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Conflict);
    }

    /// The same composition-through-nesting contract for
    /// [`TrellisError::Staging`] (issue #192's new arm): it must delegate to
    /// [`StagingError::code`] rather than hardcoding a category. Checked with
    /// a variant whose code is *not* [`ErrorCode::Internal`], so a hardcoded
    /// "staging failures are internal" would fail this test rather than
    /// coincidentally pass it.
    #[test]
    fn staging_delegates_to_the_wrapped_staging_error() {
        let inner = StagingError::ProducerAlreadyRunning;
        let expected = inner.code();
        let wrapped = TrellisError::Staging(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Conflict);
    }

    /// [`StagingError::ConvergenceTimeout`] is the one variant
    /// [`Trellis::await_converged`] actually surfaces, so its category is
    /// pinned separately from the delegation test above — an embedder
    /// branching on [`TrellisError::code`] shouldn't see it drift silently.
    /// It is [`ErrorCode::Timeout`], not [`ErrorCode::Internal`]: the
    /// caller's deadline expiring is expected and retryable, and a host maps
    /// `internal` to "a Trellis bug" (issue #586).
    #[test]
    fn a_convergence_timeout_surfaces_as_timeout_through_the_facade() {
        let err = TrellisError::Staging(StagingError::ConvergenceTimeout {
            token: PgLsn::from(0),
            waited: Duration::from_secs(1),
        });

        assert_eq!(err.code(), ErrorCode::Timeout);
    }
}
