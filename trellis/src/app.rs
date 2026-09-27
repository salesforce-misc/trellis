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
use crate::staging::quarantine;
use crate::staging::self_check::{SelfCheckError, SelfCheckMode, SelfCheckReport, SelfCheckScope};
use crate::staging::{DEFAULT_RECLAIM_TTL, StagingError, converge, worker_registry};

/// Options a client sets when it [`connect`](Trellis::connect)s.
///
/// `staging`/`drain_threads` mirror [`ClientOptions`]'s core contract (see
/// its doc comment): whether this connection owns CDC intake + ring
/// maintenance, and how many drain (application) workers it runs. A
/// connection that only defines transforms leaves both at their defaults
/// (nothing background starts); a connection that runs the live pipeline
/// sets `staging` and a non-zero `drain_threads`. `worker_threads` is
/// unrelated to either — see its own doc comment.
#[derive(Debug, Clone, Default)]
pub struct TrellisOptions {
    /// Whether this connection runs the CDC subscriber and ring maintenance
    /// (the staging worker). Exactly one connection in a fleet should set
    /// this. When set, the staging worker publishes whatever tables the
    /// registered definitions read, straight from the catalog, and picks up
    /// definitions registered later on its next reconcile pass; none need be
    /// registered before it starts (issue #427).
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
    /// catalog. With `staging` set, CDC intake + ring maintenance start, and
    /// the staging worker publishes whatever tables the registered
    /// definitions read (none yet is fine); with a non-zero `drain_threads`,
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
    /// anything [`Trellis`]'s own methods already cover.
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
    /// the blockers, so a chain is retired from the leaves inward), shrinks the
    /// replication publication by reconciliation once it commits, and is
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
    /// work (idempotency, validation, DDL, single-pass backfill, version
    /// fencing); see that function's own doc comment for the full contract.
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
    /// the publication (issue #427, ADR-0016). The staging worker's next
    /// reconcile pass drops a table nothing reads any more, so the process
    /// applying a `DROP` needs no publication privileges.
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
    /// (issue #497).
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
        let rows = txn
            .query(
                "select id, target_table, source_table, source_version, created_at \
                 from transform_definitions order by id",
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
    /// instead, as does a `live` one given a catch-up of its own (an
    /// `ALTER TRANSFORM` that added columns, a resumed column): it is applying
    /// changes, but its target may still be missing some (issue #476). So does
    /// a `live` one reading an upstream (another definition's target, as its
    /// source or through a relationship) that isn't `live` itself: paused,
    /// quarantined, rebuilding or catching up, down the whole chain (issue
    /// #497). That one is derived from the upstream's status when read.
    ///
    /// Also reports why a definition isn't getting there, when the cause is a
    /// failing backfill of its source table
    /// ([`DefinitionStatus::backfill_failure`], issue #407): the staging
    /// worker retries it with backoff forever, so without this the only
    /// sign would be a warning in that worker's log.
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
                "select d.status, pb.table_name, pb.attempts, pb.last_error, pb.next_attempt_at, \
                        d.id \
                 from transform_definitions d \
                 left join pending_backfill pb \
                   on pb.table_name = d.source_table and pb.last_error is not null \
                 where split_part(d.target_table, '.', 2) = $1",
                &[&target_table],
            )
            .await?;
        let mut status = None;
        if let Some(row) = &row {
            let status_text: String = row.get(0);
            let stored = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            });
            status = Some(reported_status(&*txn, row.get(5), stored).await?);
        }
        txn.commit().await?;
        Ok(row.zip(status).map(|(row, status)| {
            let backfill_failure =
                row.get::<_, Option<String>>(1)
                    .map(|source_table| BackfillFailure {
                        source_table,
                        attempts: u32::try_from(row.get::<_, i32>(2)).unwrap_or(0),
                        last_error: row.get(3),
                        next_attempt_at: row.get(4),
                    });
            DefinitionStatus {
                status,
                backfill_failure,
            }
        }))
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
    /// Only valid for a table that's already a publication member: a
    /// never-published table is backfilled in full on first contact by the
    /// running staging worker, so this is refused there
    /// ([`TrellisError::TableNotPublished`]). The running staging worker
    /// discharges the marker.
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

        let publication = ClientOptions::default().publication;
        let already_published: bool = client
            .query_one(
                "select exists(select 1 from pg_publication_tables \
                 where pubname = $1 and schemaname = $2 and tablename = $3)",
                &[&publication, &schema, &source_table],
            )
            .await?
            .get(0);
        if !already_published {
            return Err(TrellisError::TableNotPublished {
                table: qualified,
                publication,
            });
        }

        // A failure to park is a plain `Db` error.
        let txn = client.transaction().await?;
        crate::intake::publication::park_table_catch_ups(&*txn, std::slice::from_ref(&qualified))
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
    /// the keys the apply path gave up on. A running client surfaces these so
    /// a whole-table failure (every row poisoned) doesn't sit silently.
    pub async fn poisoned_since(
        &self,
        watermark: SystemTime,
    ) -> Result<Vec<PoisonEntry>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select src_table, key, last_error, poisoned_at \
                 from poison where poisoned_at > $1 order by poisoned_at",
                &[&watermark],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| PoisonEntry {
                src_table: row.get(0),
                key: row.get(1),
                last_error: row.get(2),
                poisoned_at: row.get(3),
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
    /// presence means "excluded from folding entirely"). For a whole
    /// `transform` target, pulls from `poison` filtered to that transform's
    /// own source table — the coarser, whole-key fuse's own detail.
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
                // `status`/`quarantine_status`.
                let source_row = client
                    .query_opt(
                        "select source_table from transform_definitions \
                         where split_part(target_table, '.', 2) = $1",
                        &[t],
                    )
                    .await?
                    .ok_or_else(|| TrellisError::TransformNotFound(t.clone()))?;
                // Issue #72: `transform_definitions.source_table` is fully
                // qualified, and as of issue #267 so is every `src_table`
                // `staging::apply` emits — a definition chained off another's
                // target table used to stage its downstream trigger (and so
                // its poisoned rows) under `ddl::neighbor_table_name`'s
                // deliberately bare name, which this query's `bare` arm
                // existed to catch. Both arms are kept regardless: `poison`
                // rows are durable, so rows recorded before that fix still
                // carry the bare spelling, and this crate's own integration
                // fixtures stage bare names by hand. Matching against both
                // forms keeps this query correct either way rather than
                // picking one and silently going empty for the other.
                //
                // Issue #283 has since made the qualified spelling `poison`'s
                // canonical *key*, not just what new rows happen to carry:
                // `staging::quarantine` resolves `src_table` once and both
                // writes and reads it canonically, and
                // `V33__quarantine_canonical_src_table.sql` folded the
                // pre-existing bare rows into their qualified counterpart. The
                // bare arm below is therefore no longer load-bearing for
                // ordinary installations and is kept only for the spellings
                // that fold deliberately declines (a bare suffix ambiguous
                // across two schemas, or a source nothing in the catalog can
                // resolve) plus fixtures that hand-stage bare rows after the
                // migration ran. Unlike the counting/charging sites that issue
                // fixed, this is a read-only operator sample with no budget
                // behind it, so matching a set of spellings here cannot re-split
                // anything — see `quarantine::canonical_and_raw`'s doc comment
                // for that same distinction on the delete paths.
                let qualified: String = source_row.get(0);
                let bare = qualified
                    .split_once('.')
                    .map(|(_, table)| table.to_string())
                    .unwrap_or_else(|| qualified.clone());
                match &after {
                    Some((after_src, after_key)) => {
                        client
                            .query(
                                "select src_table, key, last_error from poison \
                                 where src_table in ($1, $2) and (src_table, key) > ($3, $4) \
                                 order by src_table, key limit $5",
                                &[&qualified, &bare, after_src, after_key, &limit],
                            )
                            .await?
                    }
                    None => {
                        client
                            .query(
                                "select src_table, key, last_error from poison \
                                 where src_table in ($1, $2) \
                                 order by src_table, key limit $3",
                                &[&qualified, &bare, &limit],
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
    /// **What this detects.** The staging worker captures source changes
    /// and runs the maintenance loop that dispatches every new transform's
    /// backfill. A fleet with drain workers but no staging worker passes
    /// [`Trellis::has_live_drain_workers`] while no change is captured and
    /// every new transform sits in [`TransformStatus::WaitingToBackfill`]
    /// forever. `false` here is that misconfiguration. A healthy fleet
    /// needs both checks to be `true`.
    ///
    /// **"Running" means holding the producer singleton**, the
    /// session-scoped advisory lock the staging worker's intake holds on its
    /// own connection for as long as it streams (see
    /// `staging::ProducerSession`). One `pg_locks` read, no
    /// heartbeat: a crashed worker's connection closes and Postgres frees
    /// the lock with it. It also reads `false` while a failed intake waits
    /// to restart (up to a minute between attempts), during which nothing
    /// is captured either.
    pub async fn has_live_staging_worker(&self) -> Result<bool, TrellisError> {
        let client = self.pool.get().await?;
        Ok(crate::staging::session::producer_is_running(&**client, self.config.schema()).await?)
    }

    /// A read-your-writes watermark (issue #192): `pg_current_wal_lsn()`,
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
    /// `pg_current_wal_lsn()` reports the server's current WAL position, not
    /// something scoped to one session, so it's guaranteed to be at or past
    /// the write's own commit LSN by the time this query runs.
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
    /// // Every target fed by that write is now guaranteed to reflect it.
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
    /// `token` is `pg_current_wal_lsn()`, which normally sits *ahead* of the
    /// caller's own commit: any unrelated WAL (another backend, a write to an
    /// unpublished table, the engine's own bookkeeping) advances it, and none
    /// of it gives intake a change to confirm. When intake is behind the
    /// token, this writes one `trellis.converge` logical decoding message
    /// (`pg_logical_emit_message`, executable by `PUBLIC` by default), which
    /// intake confirms through as soon as it decodes it, so a quiet stream
    /// converges as fast as a busy one (issue #452).
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
            Some(schema) => crate::intake::publication::qualify(schema, source_table)
                .map_err(CatalogError::from)?,
            None => defs::catalog::resolve_graph_identity(&self.pool, source_table).await?,
        };
        debug_assert!(
            qualified.contains('.'),
            "resolve_graph_identity/qualify always return a schema.table-shaped string"
        );

        // Issue #108: queries `pg_attribute` directly for each column's raw
        // `atttypid` OID, classified via `defs::pg_type::value_type_for_oid`
        // — the same `to_regclass`-bound introspection `defs::catalog` uses
        // — rather than `information_schema.columns.data_type` text matched
        // against a small hardcoded list (`pg_value_type`, since removed).
        // That old mapping silently *dropped* every column whose type it
        // didn't recognize, `uuid` included, so referencing a `uuid` source
        // column in a definition failed before validation ever saw it, and
        // any other Postgres type (`bytea`, `jsonb`, `timestamptz`, ...) was
        // invisible to the validator entirely rather than being an honestly
        // typed passthrough column.
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select a.attname::text, a.atttypid \
                 from pg_attribute a \
                 where a.attrelid = pg_catalog.to_regclass($1) \
                   and a.attnum > 0 \
                   and not a.attisdropped",
                &[&defs::ddl::regclass_arg(&qualified)],
            )
            .await?;

        if rows.is_empty() {
            return Err(TrellisError::SourceTableNotFound(source_table.to_string()));
        }

        let mut columns = HashMap::with_capacity(rows.len());
        for row in rows {
            let column_name: String = row.get(0);
            let type_oid: u32 = row.get(1);
            // Issue #117: `value_type_for_oid` now also recognizes a
            // user-defined enum type (a connection is only ever used for an
            // OID this process hasn't already classified as a fixed
            // builtin — see that function's own doc comment).
            match defs::pg_type::value_type_for_oid(&**client, type_oid).await? {
                // Issue #108 review: a column whose OID the registry still
                // can't place at all (an array, a range, a composite, a
                // domain, `citext`, ...) stays *out* of the validator's
                // view, exactly as the old `pg_value_type` dropped it.
                // `Other(Unrecognized)` is an honest label but not an
                // actionable one: everything downstream is keyed on knowing
                // the column's real Postgres type, and this doesn't.
                // Admitting it made `GROUP BY <col>` fail at `create table`
                // with a raw `type "unrecognized" does not exist`, and let a
                // bare passthrough `define()` succeed only to fail later in
                // `apply_target`'s `$n::text::<type>` cast — both strictly
                // worse than the clean `ValidationError::UnknownColumn` this
                // drop preserves. Promoting the remaining families is
                // `docs/type-support.md`'s deferred work (#122); enums
                // themselves left this arm as of #117.
                ValueType::Other(defs::PgType::Unrecognized) => {}
                value_type => {
                    columns.insert(column_name, value_type);
                }
            }
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

/// One registered transform definition's status, as [`Trellis::status`]
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionStatus {
    /// Where the transform is in its lifecycle (issue #55).
    pub status: TransformStatus,
    /// Set while the backfill marker parked on the definition's source table
    /// keeps failing to discharge (issue #407, ADR-0016), or carries the error
    /// of a direct build that failed and was handed back to it (issue #419).
    /// That discharge runs
    /// every build of a `waiting_to_backfill` definition on the table and the
    /// catch-up of every `catching_up` one, so its failure is reported on each
    /// definition that reads the table, whatever its status. A definition
    /// stuck in `waiting_to_backfill` with this set is waiting on the cause
    /// named in [`BackfillFailure::last_error`], not on the discharge's turn.
    /// It clears once the discharge succeeds or the table is parked again.
    pub backfill_failure: Option<BackfillFailure>,
}

/// The retry state of a source table's backfill marker whose discharge has
/// failed (issue #407). The discharge retries it with capped exponential
/// backoff, and never gives up on its own: fix the cause (or drop the
/// definitions reading the table) and the next attempt goes through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillFailure {
    /// The qualified source table the marker is parked on.
    pub source_table: String,
    /// How many discharges of the marker have failed since it was parked.
    pub attempts: u32,
    /// The latest failure's error, as the staging worker's log reported it.
    pub last_error: String,
    /// The earliest time the staging worker's discharge tries it again.
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
    /// [`Trellis::request_backfill`] was asked to backfill a table that isn't
    /// a member of the publication yet.
    TableNotPublished { table: String, publication: String },
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
            TrellisError::TableNotPublished { .. } => ErrorCode::Validation,
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
            TrellisError::TableNotPublished { table, publication } => write!(
                f,
                "\"{table}\" isn't in publication \"{publication}\" yet; run with staging enabled \
                 and it will be backfilled automatically on first contact"
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
            | TrellisError::TableNotPublished { .. }
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
