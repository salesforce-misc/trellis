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
//! goes through [`defs::install_definition`], the fast direct-build entry
//! point that also records backfill coverage, rather than the lower-level
//! `create_definition`/`create_target_table` primitives it's built from.
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
//! **[`define`](Trellis::define) doesn't block on backfill.** Per
//! `docs/decisions/0008-public-api-design.md`'s decision 1 and
//! [ADR-0007's amendment](../../docs/decisions/0007-direct-set-based-backfill.md#backgrounding-and-resumability-amendment),
//! a plain (non-relationship) 1-1 transform's initial backfill runs as a
//! durable, claimable queue of chunks that running drain
//! (`application_threads`) workers execute — anywhere in the fleet, not
//! necessarily on the connection that called `define()`. `define()` itself
//! returns once the definition is registered and that chunk work is
//! enumerated/persisted, with [`TransformStatus::Backfilling`]; callers that
//! need the target actually populated poll [`Trellis::status`] until it
//! reports [`TransformStatus::Live`] — which requires *some* client in the
//! fleet to be running with `drain_threads > 0` (a `define`-only connection,
//! with no such client anywhere, leaves the transform queued indefinitely).
//! A relationship-enriched 1-1 transform (like the `count(posts.id)` example
//! below) or an aggregate (`GROUP BY`) transform still builds fully
//! synchronously in-call today — see `trellis::defs::backfill`'s module docs
//! for why those two shapes aren't chunked into the durable queue yet.
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
//!     .define_relationship("RELATIONSHIP posts FROM authors.id TO posts.author")
//!     .await?;
//! trellis
//!     .define("TRANSFORM authors_calc FROM authors SELECT count(posts.id) AS post_count")
//!     .await?;
//!
//! // Separately, run the live pipeline: staging worker + two drain threads.
//! // Drain threads are also what finish any queued backfill chunk work, for
//! // a plain 1-1 transform `define()` returned before fully building.
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
//! while running.status("authors_calc").await? != Some(TransformStatus::Live) {
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
    /// this. When set, [`Trellis::connect`] derives the source-table set to
    /// publish from the catalog, so at least one definition must already be
    /// registered.
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
    /// catalog. With `staging` set, the source-table set is derived from the
    /// registered definitions and CDC intake + ring maintenance start; with a
    /// non-zero `drain_threads`, that many application workers start.
    pub async fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let pool = Pool::new(&config)?;
        let client = if options.staging || options.drain_threads > 0 {
            Some(Self::start_client(&config, &pool, &options).await?)
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

    /// Registers a transform definition and creates its target table.
    ///
    /// Introspects the source table's columns for the validator, then routes
    /// through [`defs::install_definition`] — the fast direct-build path.
    ///
    /// **Returns before backfill finishes** for a plain (non-relationship)
    /// 1-1 transform (see this module's doc comment): the returned
    /// [`Definition`] reports [`TransformStatus::Backfilling`], and the
    /// target is populated in the background by whichever drain
    /// (`application_threads`) workers are running in the fleet, not by this
    /// call. Poll [`Trellis::status`] for [`TransformStatus::Live`] once you
    /// need the target's contents. A relationship-enriched 1-1 or an
    /// aggregate (`GROUP BY`) transform still builds synchronously — the
    /// returned [`Definition`] already reports [`TransformStatus::Live`] for
    /// those two shapes.
    pub async fn define(&self, definition_text: &str) -> Result<Definition, TrellisError> {
        let parsed = defs::parse(definition_text)?;
        let source_columns = self
            .source_columns(&parsed.source, parsed.explicit_source_schema.as_deref())
            .await?;
        defs::install_definition(
            &self.pool,
            definition_text,
            &source_columns,
            self.config.target_schema(),
        )
        .await
        .map_err(TrellisError::Catalog)
    }

    /// Registers a relationship declaration (ADR-0006) — the standalone
    /// `RELATIONSHIP <name> FROM <table>.<col> TO <table>.<col>` form a later
    /// [`define`](Trellis::define) can reference in a calculated field.
    pub async fn define_relationship(
        &self,
        definition_text: &str,
    ) -> Result<RelationshipDefinition, TrellisError> {
        defs::create_relationship(&self.pool, definition_text)
            .await
            .map_err(TrellisError::Catalog)
    }

    /// Every registered transform definition, oldest first.
    pub async fn definitions(&self) -> Result<Vec<DefinitionSummary>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select id, target_table, source_table, source_version, status, created_at \
                 from transform_definitions order by id",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let status_text: String = row.get(4);
                let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                    panic!("transform_definitions.status held unrecognized value '{status_text}'")
                });
                DefinitionSummary {
                    id: row.get(0),
                    target_table: row.get(1),
                    source_table: row.get(2),
                    source_version: row.get(3),
                    status,
                    created_at: row.get(5),
                }
            })
            .collect())
    }

    /// One registered transform definition's current [`TransformStatus`]
    /// (issue #55), by target table name — the read a host-language embedder
    /// polls after [`define`](Trellis::define) returns, per
    /// `docs/decisions/0008-public-api-design.md`'s decision 1 ("define, then poll status
    /// until live"). A thin convenience over [`definitions`](Trellis::definitions)
    /// for callers that only want one row rather than the full list.
    pub async fn status(
        &self,
        target_table: &str,
    ) -> Result<Option<TransformStatus>, TrellisError> {
        let client = self.pool.get().await?;
        // Issue #73: `transform_definitions.target_table` is persisted
        // fully-qualified, but every caller here only ever has the bare name
        // their `TRANSFORM <name> FROM ...` text declared — even once issue
        // #76 taught the grammar an explicit `schema.table` spelling,
        // `def.target` itself still always holds just the bare table name
        // (see `defs::ast::TransformDef`'s own doc comment for why), so this
        // API's callers never have anything but the bare name to poll with —
        // match against `target_table`'s bare table-name suffix rather than
        // the qualified column directly.
        let row = client
            .query_opt(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target_table],
            )
            .await?;
        Ok(row.map(|row| {
            let status_text: String = row.get(0);
            TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            })
        }))
    }

    /// Every registered relationship declaration, oldest first.
    pub async fn relationships(&self) -> Result<Vec<RelationshipSummary>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select id, name, from_table, from_col, to_table, to_col, cardinality, created_at \
                 from relationship_definitions order by id",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| RelationshipSummary {
                id: row.get(0),
                name: row.get(1),
                from_table: row.get(2),
                from_col: row.get(3),
                to_table: row.get(4),
                to_col: row.get(5),
                cardinality: row.get(6),
                created_at: row.get(7),
            })
            .collect())
    }

    /// Re-stages `source_table`'s current rows via a `pending_backfill`
    /// marker, so a transform registered *after* the table already joined the
    /// replication publication gets its own backfill. Only valid for a table
    /// that's already a publication member — a never-published table is
    /// backfilled in full on first contact by the running staging worker, so
    /// this is refused there (and the running staging worker must discharge
    /// the marker).
    pub async fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        let client = self.pool.get().await?;
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

        client
            .execute(
                "insert into pending_backfill (table_name, fence_snapshot) \
                 values ($1, pg_current_snapshot()) \
                 on conflict (table_name) \
                 do update set fence_snapshot = excluded.fence_snapshot, added_at = now()",
                &[&qualified],
            )
            .await?;
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
                        "select status from transform_definitions \
                         where split_part(target_table, '.', 2) = $1",
                        &[t],
                    )
                    .await?
                    .ok_or_else(|| TrellisError::TransformNotFound(t.clone()))?;
                let status_text: String = row.get(0);
                let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                    panic!("transform_definitions.status held unrecognized value '{status_text}'")
                });
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
                // Issue #72: `transform_definitions.source_table` is now
                // fully qualified, but `poison.src_table` (what CDC intake
                // actually stages) isn't uniformly so — a raw/CDC-sourced
                // definition's poisoned rows are staged qualified (matching
                // `qualified` directly), while a definition chained off
                // another's target table are staged bare. Landing issue #73
                // (which persists `transform_definitions.target_table`
                // qualified too) doesn't close this gap: a chained
                // definition's poisoned rows get their `src_table` from
                // `ddl::neighbor_table_name`, which #73 deliberately leaves
                // bare (see `defs::source_table_version`'s doc comment for
                // the full rationale, and why closing this for good is
                // issue #75's emission-audit territory instead). Matching
                // against both forms keeps this query correct either way
                // rather than picking one and silently going empty for the
                // other.
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

    /// Resumes a paused column: clears its pause, re-derives its value
    /// across every existing row, and un-cascades any dependent transform
    /// that was only paused because of this one (see
    /// [`crate::staging::quarantine::resume_column`] for the full contract,
    /// including why a dependent with its own independent reason to stay
    /// paused is left alone). `target` must address a column
    /// (`transform.column`) — [`TrellisError::ColumnAddressRequired`] if
    /// given a bare transform name — a `quarantined` transform's
    /// whole-transform remedy is [`Trellis::resume_transform`], a different
    /// operation entirely (drops back to `waiting_to_backfill` and re-runs
    /// the full backfill — see `docs/transforms.md#status`), not this call.
    ///
    /// Returns every `(transform, column)` pair actually resumed —
    /// `target` itself first, then any dependents whose pause was purely
    /// this one's cascade.
    pub async fn resume_column(&self, target: &str) -> Result<Vec<(String, String)>, TrellisError> {
        match QuarantineTarget::parse(target) {
            QuarantineTarget::Column(transform, column) => {
                quarantine::resume_column(&self.pool, &transform, &column)
                    .await
                    .map_err(TrellisError::Apply)
            }
            QuarantineTarget::Transform(_) => Err(TrellisError::ColumnAddressRequired),
        }
    }

    /// Resumes a frozen transform — either trigger (issue #55, issue #142;
    /// ADR-0003's coarser, transform-wide fuse tier and ADR-0014's operator
    /// pause share one recovery path, because the state they produce is the
    /// same state). `target` must be a bare transform's target table, not a
    /// `transform.column` address — see
    /// [`crate::staging::quarantine::resume_transform`] for the full
    /// contract, including why this drops the transform to
    /// [`TransformStatus::WaitingToBackfill`] and re-runs its backfill
    /// through the same `xmin`-fence-respecting path a fresh transform's own
    /// initial backfill uses, rather than a shortcut.
    ///
    /// **Resume rebuilds; it does not catch up** (ADR-0014). A frozen
    /// definition must not pin the staging ring — holding ring segments open
    /// for it would wedge the ring for every sibling reading the same source
    /// — so while it is frozen its share of the change stream is drained for
    /// those siblings and is *not* recoverable by replay. There are no
    /// buffered changes to apply on the way back, which is why the recovery
    /// is a fresh backfill from source. The cost of resuming therefore scales
    /// with the data, not with the length of the pause.
    pub async fn resume_transform(&self, target: &str) -> Result<(), TrellisError> {
        quarantine::resume_transform(&self.pool, target)
            .await
            .map_err(TrellisError::Apply)
    }

    /// Freezes a transform deliberately (issue #142, ADR-0014) — the
    /// operator-driven half of the pause state whose other half is the
    /// poison fuse's auto-pause.
    ///
    /// The target stops being written to and holds its current, now-stale
    /// value: [`TransformStatus::Paused`] fails the one gate every
    /// claim-time fold dispatch already resolves targets through, so this
    /// reuses the existing freeze rather than adding a second one. Its share
    /// of the change stream is drained for its siblings meanwhile, so it
    /// never pins the staging ring — and [`resume_transform`](Trellis::resume_transform)
    /// consequently rebuilds by a fresh backfill rather than catching up.
    ///
    /// Pausing an already-frozen transform — whether by an earlier pause or
    /// by the poison fuse — **succeeds as a no-op**. Pause runs on Trellis's
    /// own connections, not inside a caller's migration transaction, so a
    /// migration that is replayed or interleaved with a rollback has to be
    /// safe to re-run; "did the pause land?" resolves to success either way.
    ///
    /// A `target` that was never defined is
    /// [`CatalogError::TransformNotFound`] — unlike
    /// [`drop_transform`](Trellis::drop_transform), whose absent case *is* the
    /// outcome its caller wanted.
    pub async fn pause_transform(&self, target: &str) -> Result<(), TrellisError> {
        defs::lifecycle::pause_transform(&self.pool, target)
            .await
            .map(|_| ())
            .map_err(TrellisError::Catalog)
    }

    /// Removes a transform definition (issue #142, ADR-0014) — the terminal
    /// reap of a paused definition, and the inverse of
    /// [`define`](Trellis::define).
    ///
    /// **Pause it first.** There is no direct live-to-gone edge in the
    /// lifecycle: a definition that isn't frozen is refused with
    /// [`CatalogError::TransformNotPaused`]. Quiescing through the pause is
    /// what lets the removal skip reasoning about a fold still dispatching to
    /// the target.
    ///
    /// **The data goes with it.** Dropping a definition drops its target
    /// table, unconditionally — there is no option to retire the definition
    /// while keeping its rows. Keeping derived rows after removing the
    /// definition that explains them has no use worth naming, and the paused
    /// state already serves the caller who wants the data to stick around
    /// unmaintained: leave it paused rather than dropping it. Only the
    /// Trellis-owned target table is ever dropped; source tables are
    /// user-owned and untouched.
    ///
    /// **Refuses rather than cascades.** If a live definition still chains
    /// off this target, the drop fails with
    /// [`CatalogError::DependentsBlockDrop`] naming the blockers, so the
    /// order to retire them in is explicit. Work from the leaves inward.
    ///
    /// **Shrinks the publication inline.** Once the drop commits, the
    /// replication publication is reconciled against the definitions that
    /// remain, so a source table leaves replication exactly when nothing
    /// derives from it any longer — by reconciliation, not by hand-editing,
    /// and at drop time rather than deferred to a maintenance pass.
    ///
    /// Dropping a definition that isn't registered **succeeds as a no-op**,
    /// for the same replayed-migration reason [`pause_transform`](Trellis::pause_transform)
    /// is idempotent: "is it already gone?" resolves to success.
    pub async fn drop_transform(&self, target: &str) -> Result<(), TrellisError> {
        let outcome = defs::lifecycle::drop_transform(&self.pool, target)
            .await
            .map_err(TrellisError::Catalog)?;

        if outcome == defs::lifecycle::DropOutcome::Dropped {
            self.reconcile_publication_after_drop().await?;
        }
        Ok(())
    }

    /// Removes a relationship declaration (issue #142, ADR-0014) — the
    /// inverse of [`define_relationship`](Trellis::define_relationship).
    ///
    /// Addressed by `(from_table, name)` because a relationship name is
    /// unique per from-table rather than globally — the same pair a
    /// calculated field's `<rel>.<column>` head resolves against. Drops the
    /// Trellis-owned parent projection table the declaration created along
    /// with it; both endpoint tables are the user's and are untouched.
    ///
    /// **Refuses rather than cascades**, like
    /// [`drop_transform`](Trellis::drop_transform): any live transform whose
    /// text still references this relationship blocks the drop and is named
    /// in [`CatalogError::DependentsBlockDrop`].
    ///
    /// There is deliberately no `pause_relationship`: a relationship carries
    /// no lifecycle status and nothing in the fold gates on one, so freezing
    /// it would mean building the second freezing mechanism ADR-0014 rules
    /// out. A relationship is dropped outright, once nothing live reads it.
    ///
    /// Dropping an unregistered relationship **succeeds as a no-op**.
    pub async fn drop_relationship(
        &self,
        from_table: &str,
        name: &str,
    ) -> Result<(), TrellisError> {
        let outcome = defs::lifecycle::drop_relationship(&self.pool, from_table, name)
            .await
            .map_err(TrellisError::Catalog)?;

        if outcome == defs::lifecycle::DropOutcome::Dropped {
            self.reconcile_publication_after_drop().await?;
        }
        Ok(())
    }

    /// Reconciles the replication publication against whatever definitions
    /// are left, immediately after a drop (ADR-0014, "The publication shrinks
    /// by reconciliation").
    ///
    /// Lives here rather than in `defs::lifecycle` for two reasons the engine
    /// layer can't supply on its own:
    /// [`crate::intake::publication::reconcile_publication`] needs a concrete
    /// `tokio_postgres::Client` (not a pooled one — see its own doc comment
    /// for why intake's session isn't available), and it needs the configured
    /// publication name, which only the facade knows. It mirrors
    /// [`crate::client`]'s own periodic `reconcile_source_tables` exactly:
    /// derive the desired set from the catalog, then diff. Because
    /// `defs::all_source_tables` walks the definitions that still exist, a
    /// source table leaves the publication precisely when its last reader
    /// does — never while a sibling definition still reads it.
    ///
    /// A publication that doesn't exist yet is skipped rather than an error:
    /// a define-only connection that never ran with `staging: true` has no
    /// publication to shrink, and refusing its drop over that would be
    /// gratuitous.
    async fn reconcile_publication_after_drop(&self) -> Result<(), TrellisError> {
        let publication = ClientOptions::default().publication;
        let exists: bool = self
            .pool
            .get()
            .await?
            .query_one(
                "select exists(select 1 from pg_publication where pubname = $1)",
                &[&publication],
            )
            .await?
            .get(0);
        if !exists {
            return Ok(());
        }

        let desired = defs::all_source_tables(&self.pool)
            .await
            .map_err(TrellisError::Catalog)?;

        let (mut client, connection) =
            tokio_postgres::connect(self.config.dsn(), tokio_postgres::NoTls).await?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        // A bare `tokio_postgres::connect` lands on the *default*
        // `search_path`, not the Trellis schema — unlike every pooled
        // connection, which `pool::session_bootstrap` pins. Without this,
        // `reconcile_publication`'s add path (which parks a
        // `pending_backfill` marker for a table joining the publication)
        // fails with a `42P01` "relation \"pending_backfill\" does not
        // exist" against any non-`public` Trellis schema, so a drop that
        // leaves the desired set *larger* than the published one reports
        // `TrellisError::Publication` after the definition is already gone.
        // Mirrors `client::connect_plain`'s own bootstrap.
        client
            .batch_execute(&format!(
                "set search_path to {}, public; {}",
                crate::pool::quote_ident(self.config.schema()),
                crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS
            ))
            .await?;
        let result =
            crate::intake::publication::reconcile_publication(&mut client, &publication, &desired)
                .await;
        drop(client);
        handle.abort();
        result.map_err(TrellisError::Publication)
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
    /// a transform that never seems to finish backfilling. See
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
    /// failure.
    ///
    /// **Size `timeout` in seconds, not milliseconds.** `token` is
    /// `pg_current_wal_lsn()`, which normally sits *ahead* of the caller's
    /// own commit (any unrelated WAL — another backend, a checkpoint, the
    /// engine's own bookkeeping — advances it), and the convergence
    /// predicate's first condition is `replication_progress.confirmed_lsn >=
    /// token`. On a busy pipeline that clears almost immediately, but on a
    /// *quiet* stream `confirmed_lsn` only catches up to a token past the
    /// last decoded change when intake's keepalive-driven advance persists —
    /// throttled to once per `intake::KEEPALIVE_PERSIST_INTERVAL` (10s), and
    /// itself paced by the server's own walsender keepalive cadence. A
    /// sub-second budget can therefore report
    /// [`StagingError::ConvergenceTimeout`] on a pipeline that is in fact
    /// fully caught up. The one real in-tree caller
    /// (`generative`'s `ManualBackend::quiesce`) uses 30s; that's the right
    /// order of magnitude.
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
        Ok(converge::await_converged(&**client, token, timeout).await?)
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
    /// connection. When staging, derives the source-table set from the
    /// catalog (a staging worker needs it non-empty).
    async fn start_client(
        config: &Config,
        pool: &Pool,
        options: &TrellisOptions,
    ) -> Result<Client, TrellisError> {
        let source_tables = if options.staging {
            let tables = qualified_source_tables(pool).await?;
            if tables.is_empty() {
                return Err(TrellisError::NoDefinitions);
            }
            tables
        } else {
            Vec::new()
        };

        let client_options = ClientOptions {
            staging_worker: options.staging,
            application_threads: options.drain_threads,
            source_tables,
            ..Default::default()
        };
        Client::start(config.dsn(), client_options).map_err(TrellisError::Client)
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
    /// So *this*, the very first thing [`Trellis::define`] does with a
    /// parsed definition, rejected both an explicitly-qualified `FROM
    /// <schema>.<table>` naming a table outside this connection's
    /// `search_path`, and a bare `FROM <table>` chaining off another
    /// definition's target explicitly qualified into a non-default schema
    /// (issue #76) — before `install_definition`'s own, already-fixed
    /// resolution (`resolve_source_for_install`) was ever reached: this call
    /// happens first, in [`Trellis::define`], and returns
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
                &[&qualified],
            )
            .await?;

        if rows.is_empty() {
            return Err(TrellisError::SourceTableNotFound(source_table.to_string()));
        }

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let column_name: String = row.get(0);
                let type_oid: u32 = row.get(1);
                match defs::pg_type::value_type_for_oid(type_oid) {
                    // Issue #108 review: a column whose OID the registry
                    // can't place at all (an enum — enum OIDs are assigned
                    // per `CREATE TYPE`, not fixed builtins — an array, a
                    // range, a composite, a domain, `citext`, ...) stays
                    // *out* of the validator's view, exactly as the old
                    // `pg_value_type` dropped it. `Other(Unrecognized)` is an
                    // honest label but not an actionable one: everything
                    // downstream is keyed on knowing the column's real
                    // Postgres type, and this doesn't. Admitting it made
                    // `GROUP BY <enum col>` fail at `create table` with a raw
                    // `type "unrecognized" does not exist`, and let a bare
                    // enum passthrough `define()` succeed only to fail later
                    // in `apply_target`'s `$n::text::<type>` cast — both
                    // strictly worse than the clean
                    // `ValidationError::UnknownColumn` this drop preserves.
                    // Promoting these families is `docs/type-support.md`'s
                    // deferred work (#117 for enums, #122 for the rest).
                    ValueType::Other(defs::PgType::Unrecognized) => None,
                    value_type => Some((column_name, value_type)),
                }
            })
            .collect())
    }
}

/// The full transitive closure of source tables reachable from every
/// registered definition — each definition's direct anchor table plus every
/// relationship `to_table` reachable from one — each already schema-qualified
/// for [`ClientOptions::source_tables`].
///
/// Issue #75, ADR-0007: [`defs::all_source_tables`] itself now returns each
/// table's actual, already-persisted qualified identity, so this is a thin
/// pass-through — it used to re-resolve every bare result against
/// `information_schema`/`current_schemas(false)` (a `search_path` walk of
/// exactly the kind ADR-0007 forbids downstream of definition-acceptance
/// time), which broke for a source living outside this connection's
/// `search_path` (e.g. an issue #76 explicit-schema source).
///
/// `pub(crate)`: [`Trellis::connect`]'s `start_client` is its only caller, and
/// ADR-0012 keeps the facade's public surface to tier 1 and tier 2. What it
/// passes through to — the transitive source-table closure — is covered
/// directly in `trellis/tests/app.rs` against `defs::all_source_tables`.
pub(crate) async fn qualified_source_tables(pool: &Pool) -> Result<Vec<String>, TrellisError> {
    Ok(defs::all_source_tables(pool).await?)
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
    pub from_table: String,
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
    pub cardinality: String,
    pub created_at: SystemTime,
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
    /// Parses `"transform"` or `"transform.column"`. A dotted address splits
    /// on the *first* `.`, so a target table name that itself contains a dot
    /// (unusual, but not forbidden by this crate) still parses as intended:
    /// everything after the first dot is the column name, matching how a
    /// calculated field's own qualified references work elsewhere in this
    /// crate. An address with either half empty (`"."`, `"orders."`,
    /// `".total"`) is treated as a bare transform name — [`fmt::Display`]
    /// never produces such a string, so this only matters for a
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
    /// deliberately froze it via [`Trellis::pause_transform`], mirroring
    /// [`TransformStatus::Paused`] — the same freeze
    /// [`QuarantineState::Quarantined`] is, reached by the other of
    /// ADR-0014's two triggers.
    Paused,
}

impl From<TransformStatus> for QuarantineState {
    fn from(status: TransformStatus) -> Self {
        match status {
            TransformStatus::WaitingToBackfill => QuarantineState::WaitingToBackfill,
            TransformStatus::Backfilling => QuarantineState::Backfilling,
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
    /// A catalog operation (define/relationship/install) failed.
    Catalog(CatalogError),
    /// Starting or stopping the background client failed.
    Client(ClientError),
    /// A connection/config/migration-layer failure.
    Engine(crate::error::Error),
    /// A direct Postgres query this facade runs itself (introspection,
    /// listing, backfill request) failed.
    Db(tokio_postgres::Error),
    /// `staging` was requested but no transform definitions are registered,
    /// so there is nothing to publish or stream.
    NoDefinitions,
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
    /// [`Trellis::resume_column`] was given a bare transform address
    /// (no `.column`) — a `quarantined` transform's whole-transform remedy
    /// is [`Trellis::resume_transform`], not this call.
    ColumnAddressRequired,
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
    /// Reconciling the replication publication after a
    /// [`Trellis::drop_transform`]/[`Trellis::drop_relationship`] failed
    /// (issue #142). The definition is already gone when this surfaces — the
    /// drop and the reconcile are deliberately not one transaction, since
    /// `alter publication` is its own DDL and the drop must not be held open
    /// across it — so the recovery is to re-run the reconcile (the running
    /// client's own periodic `reconcile_source_tables` pass will, unprompted),
    /// not to re-run the drop.
    Publication(crate::intake::IntakeError),
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
            // `staging` requested with nothing registered, or a backfill
            // request against an unpublished table, are both rejected calls
            // given the connection's current state — same category as any
            // other invalid-configuration error.
            TrellisError::NoDefinitions | TrellisError::TableNotPublished { .. } => {
                ErrorCode::Validation
            }
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
            TrellisError::TransformNotFound(_) => ErrorCode::NotFound,
            TrellisError::ColumnAddressRequired => ErrorCode::Validation,
            TrellisError::Staging(err) => err.code(),
            TrellisError::SelfCheck(err) => err.code(),
            TrellisError::Publication(err) => err.code(),
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
            TrellisError::NoDefinitions => write!(
                f,
                "no transform definitions registered; define one before running with staging \
                 enabled"
            ),
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
            TrellisError::ColumnAddressRequired => write!(
                f,
                "resume_column needs a \"transform.column\" address; to resume a whole \
                 quarantined transform, call resume_transform instead"
            ),
            TrellisError::Staging(err) => write!(f, "{err}"),
            TrellisError::SelfCheck(err) => write!(f, "{err}"),
            TrellisError::Publication(err) => write!(
                f,
                "the definition was dropped, but reconciling the replication publication \
                 afterwards failed: {err}"
            ),
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
            TrellisError::NoDefinitions
            | TrellisError::SourceTableNotFound(_)
            | TrellisError::TableNotPublished { .. }
            | TrellisError::BlockingThreadExitedBeforeReady
            | TrellisError::BlockingThreadGone
            | TrellisError::CalledFromAsyncContext => None,
            TrellisError::BlockingSpawn(err) => Some(err),
            TrellisError::Apply(err) => Some(err),
            TrellisError::TransformNotFound(_) | TrellisError::ColumnAddressRequired => None,
            TrellisError::Staging(err) => Some(err),
            TrellisError::SelfCheck(err) => Some(err),
            TrellisError::Publication(err) => Some(err),
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
mod error_code_tests {
    use super::*;

    #[test]
    fn no_definitions_is_validation() {
        assert_eq!(TrellisError::NoDefinitions.code(), ErrorCode::Validation);
    }

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
    #[test]
    fn a_convergence_timeout_surfaces_as_internal_through_the_facade() {
        let err = TrellisError::Staging(StagingError::ConvergenceTimeout {
            token: PgLsn::from(0),
            waited: Duration::from_secs(1),
        });

        assert_eq!(err.code(), ErrorCode::Internal);
    }
}
