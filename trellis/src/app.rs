//! The Trellis client facade — the one interface an embedding application is
//! meant to use.
//!
//! Everything an embedder needs to stand up and run a Trellis instance hangs
//! off [`Trellis`]: apply migrations, register transform definitions, list
//! what's registered, request an ad-hoc backfill, and run the live CDC/apply
//! pipeline. It composes the lower-level building blocks
//! (`defs`, `client`, `intake`, `staging`) into one coherent surface so
//! callers never have to stitch those together themselves — and, critically,
//! never accidentally pick the wrong path: definition registration always
//! goes through [`defs::install_definition`], which creates the target and
//! leaves the build to the staging worker's capture pass (ADR-0002),
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
//! message — settled ahead of FFI embedding so a host
//! language on the other side of that boundary has something stable to
//! match on instead of every internal Rust error variant (`docs/decisions/0008-public-api-design.md`,
//! decision 3).
//!
//! **Every definition-changing operation goes through one entrypoint.**
//! [`Trellis::apply`] takes a single statement of Trellis's own grammar — the
//! same grammar the CLI speaks — and the parse decides which operation runs:
//! define a transform, alter, pause, resume, drop (ADR-0012,
//! ADR-0014). There is deliberately no typed method per operation,
//! so adding one is a grammar addition rather than new surface every
//! host-language binding has to mirror. Read paths (status, `self_check`,
//! quarantine sampling, convergence-await) are unaffected — they stay typed;
//! only the mutation surface unifies.
//!
//! **[`apply`](Trellis::apply)ing a definition doesn't block on backfill.** Per
//! `docs/decisions/0008-public-api-design.md`'s decision 1 and
//! [ADR-0002](../../docs/decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk),
//! a transform's initial backfill runs in the background:
//! `apply()` returns once the definition is registered, with
//! [`TransformStatus::WaitingToBackfill`], having read no source rows. The
//! staging worker's capture pass then starts the build, and running
//! drain (`application_threads`) workers execute it — anywhere in the fleet, not necessarily on the connection that called
//! `apply()`. Callers that need the target actually populated poll
//! [`Trellis::status`] until it reports [`TransformStatus::Live`] — which
//! requires a staging worker and *some* client in the fleet running with
//! `drain_threads > 0` (a define-only connection, with no such client
//! anywhere, leaves the transform queued indefinitely) — then take a
//! [`Trellis::watermark_token`] and [`Trellis::await_converged`] on it.
//!
//! Nor does an edit block on its rebuild: an `ALTER
//! TRANSFORM` that adds or changes fields, and a column `RESUME`, register a
//! background field build and return, the definition reading
//! [`TransformStatus::Backfilling`] until it is done. The statements that
//! pause, resume and drop write only the catalog.
//!
//! **Every call returns within a deadline** (issue #599, `crate::deadline`):
//! 30 seconds ([`DEFAULT_CALL_BUDGET`](crate::deadline::DEFAULT_CALL_BUDGET)) from when
//! the call is made, a wait for a pooled connection included. What it started
//! on the server is stopped by the server's own `statement_timeout`, not
//! abandoned, and its transaction rolls back; the caller gets
//! [`TrellisError::CallTimeout`], whose code is [`ErrorCode::Timeout`]. One
//! method runs outside it: [`Trellis::shutdown`], which ends background work
//! rather than serving a request. [`Trellis::self_check`], a full comparison
//! of a target, is inside it because it only starts a background job, which
//! [`Trellis::self_check_job`] polls. [`Trellis::connect`], [`Trellis::pool`],
//! [`Trellis::config`] and [`Trellis::metrics`] make no call to the database
//! that could wait.
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
//!     .apply("TRANSFORM post_counts FROM posts GROUP BY author_id SELECT count(*) AS post_count")
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
//! while running.status("post_counts").await?.map(|s| s.status) != Some(TransformStatus::Live) {
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
use crate::config::{Config, DEFAULT_CALL_POOL_MAX_SIZE, DEFAULT_CALL_POOL_WAIT_TIMEOUT};
use crate::defs::source_index::SourceIndexGap;
use crate::defs::{self, CatalogError, ParseError, StoredStatus, TransformStatus, ValueType};
use crate::error_code::{self, ErrorCode};
use crate::intake::IntakeError;
use crate::pool::Pool;
use crate::staging::apply::ApplyError;
use crate::staging::holdup::DrainFailure;
use crate::staging::join_status::JoinStatus;
use crate::staging::partition_correction::CorrectingNode;
use crate::staging::quarantine::{self, HeldKeyRetry, HeldKeys};
use crate::staging::self_check::{SelfCheckError, SelfCheckMode};
use crate::staging::self_check_job::{self, SelfCheckJob};
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
#[derive(Debug, Clone)]
pub struct TrellisOptions {
    /// Whether this connection runs capture reconciliation and ring
    /// maintenance (the staging worker). Any number of connections may set
    /// this: one takes the producer lock and leads, the rest stand by and take
    /// over when the leader goes away (see `docs/embedding.md`, "Rolling
    /// deploys"). When set, the leader installs capture
    /// triggers on whatever tables the registered definitions read, straight
    /// from the catalog, and picks up definitions registered later on its
    /// next reconcile pass; none need be registered before it starts.
    pub staging: bool,
    /// How many drain (application) worker threads this connection runs. Zero
    /// (the default) runs none. Each is a task on the background client's
    /// runtime, not an OS thread. With `n` of them the background pool holds
    /// up to `2n + 5` connections (`2n + 4` without `staging`), each thread
    /// holding at most two at once; they share one `LISTEN` connection and one
    /// claim keep-alive, and a paged drain opens a session of its own.
    /// `docs/embedding.md`, "What a handle uses", adds up a handle's
    /// connections, threads and memory.
    pub drain_threads: usize,
    /// Caps the worker-thread count of each `tokio` runtime this connection
    /// owns: the background [`Client`]'s (built when `staging` or
    /// `drain_threads` starts one) and, for
    /// [`BlockingTrellis::connect`](crate::BlockingTrellis::connect), the
    /// runtime that services the handle's calls (see its module doc
    /// comment). [`Trellis::connect`] itself never builds a runtime of its
    /// own, so a connection that starts no client is unaffected. `None` (the
    /// default) leaves `tokio`'s own default of one worker thread per core.
    ///
    /// The cap is per runtime and per handle, with no budget shared between
    /// handles. A [`BlockingTrellis`](crate::BlockingTrellis) that runs a
    /// client holds up to two runtimes, so up to `2 * n` worker threads; a
    /// process with H such handles holds up to `2 * n * H`. Size `n` for the
    /// handle count you open. The cap counts worker threads only: the thread
    /// driving each runtime and each runtime's on-demand blocking pool are
    /// outside it. The client's staging and drain workers are tasks on its
    /// runtime, so `n` also bounds how many of them compute at once, however
    /// large `drain_threads` is.
    ///
    /// Matters chiefly when Trellis is embedded inside a host VM that
    /// already sized its own scheduler pool to core count — a BEAM node, or
    /// a Ruby process with its own thread pool — where the unbounded
    /// default silently doubles the thread population with threads the host
    /// can't see, account for, or size around. Since the embedded runtime's
    /// own work is I/O, not compute-bound, a small explicit count (2, say)
    /// is enough.
    ///
    /// Must be at least 1 when set, since a runtime with no worker threads
    /// couldn't run anything: `Some(0)` fails a connect that builds a runtime,
    /// with [`TrellisError::BlockingSpawn`] from `BlockingTrellis::connect` or
    /// [`TrellisError::Client`] from a [`Trellis::connect`] that starts a
    /// client. A `Trellis::connect` that starts no client builds no runtime,
    /// so it never reads the cap.
    pub worker_threads: Option<usize>,
    /// The deadline of each public call, in place of
    /// the default 30-second budget. Not part of the
    /// contract: it exists so tests can exercise the deadline without waiting
    /// out 30 seconds.
    #[doc(hidden)]
    pub call_deadline: Option<Duration>,
    /// Retries held quarantine keys automatically. `None` (the
    /// default) retries nothing: a key stays held until an operator releases
    /// it ([`Trellis::release_key`]) or the definition is resumed.
    ///
    /// With a [`HeldKeyRetry`], the staging worker (`staging` set)
    /// releases each held key once its backoff is up, the same release as
    /// [`Trellis::release_key`], so a key whose cause went away (a parent row
    /// arriving later, a constraint that was fixed, a value that fits again)
    /// leaves quarantine on its own. A retried key that fails again is
    /// poisoned again on its first failure, and its backoff doubles, up to
    /// the policy's cap. The retries yield to the drain: none is released
    /// while the ring's backlog is past the bound a build chunk waits on.
    ///
    /// [`Trellis::status`]'s [`HeldKeys`](crate::HeldKeys) and
    /// [`Trellis::sample_quarantined`] report the next retry time and the
    /// attempt count; the next retry time is reported only by a connection
    /// with this option set.
    pub held_key_retry: Option<HeldKeyRetry>,
    /// The most physical connections the handle's call pool opens: the pool
    /// that serves the handle's API calls (`status`, `apply`, `define`,
    /// `migrate`, ...). Default 20 (`DEFAULT_CALL_POOL_MAX_SIZE`).
    /// Connections open as calls need them and close after 60 seconds idle,
    /// so a handle that makes few calls holds none. Most calls hold one
    /// connection, a few hold two or three while they run.
    ///
    /// Calls never use the background pool, which serves the background
    /// client (`staging`, `drain_threads`) and is sized by those roles, not
    /// by this option. Must be at least 1.
    pub max_call_pool_size: usize,
    /// How long a call waits for a free call-pool connection before failing
    /// with a connectivity error. Default 30 seconds
    /// (`DEFAULT_CALL_POOL_WAIT_TIMEOUT`). A call's own deadline can cut the
    /// wait shorter.
    pub call_pool_wait_timeout: Duration,
}

impl Default for TrellisOptions {
    fn default() -> Self {
        Self {
            staging: false,
            drain_threads: 0,
            worker_threads: None,
            call_deadline: None,
            held_key_retry: None,
            max_call_pool_size: DEFAULT_CALL_POOL_MAX_SIZE,
            call_pool_wait_timeout: DEFAULT_CALL_POOL_WAIT_TIMEOUT,
        }
    }
}

/// A connected Trellis instance — see the [crate docs](crate).
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
    /// The name this handle's log events carry as `trellis_instance`: its
    /// database and catalog schema (see [`crate::instance_log::name_of`]).
    instance: std::sync::Arc<str>,
    /// How long each public call has to finish (issue #599,
    /// `crate::deadline`).
    call_budget: Duration,
    /// [`TrellisOptions::held_key_retry`], which `status` and
    /// `sample_quarantined` read the next retry time under.
    held_key_retry: Option<HeldKeyRetry>,
    /// Keeps `trellis_instance_up` at 1 for this instance until the handle
    /// is dropped or shut down.
    _running: crate::metrics::RunningInstance,
}

impl Trellis {
    /// Connects to the database `config` names and, if `options` asks for any
    /// background work, starts it before returning.
    ///
    /// With the default options nothing background runs — the returned handle
    /// is purely for defining transforms and inspecting the
    /// catalog. With `staging` set, the staging worker starts: it installs
    /// capture triggers on whatever tables the registered definitions read
    /// (none yet is fine) and runs ring maintenance; with a non-zero `drain_threads`,
    /// that many application workers start.
    pub async fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let pool = Pool::with_limits(
            &config,
            options.max_call_pool_size,
            options.call_pool_wait_timeout,
        )?;
        let client = if options.staging || options.drain_threads > 0 {
            Some(Self::start_client(&config, &options)?)
        } else {
            None
        };
        let instance = crate::instance_log::name_of(&config);
        Ok(Self {
            _running: crate::metrics::RunningInstance::new(&instance),
            instance,
            call_budget: options
                .call_deadline
                .unwrap_or(crate::deadline::DEFAULT_CALL_BUDGET),
            held_key_retry: options.held_key_retry,
            config,
            pool,
            client,
        })
    }

    /// Runs one public call: under this instance's name in the log, and
    /// within its deadline (issue #599, `crate::deadline`). The wrapper of
    /// every public method that touches the database except the one
    /// documented on [`Trellis::shutdown`].
    fn call<'a, T: 'a>(
        &'a self,
        call: impl std::future::Future<Output = Result<T, TrellisError>> + Send + 'a,
    ) -> impl std::future::Future<Output = Result<T, TrellisError>> + Send + 'a {
        self.submitted(std::time::Instant::now(), call)
    }

    /// [`Trellis::call`] for a call that was submitted at `submitted`:
    /// [`BlockingTrellis`](crate::BlockingTrellis) hands a job over a channel,
    /// and the time it spends queued counts against the deadline.
    ///
    /// Boxes the call first, so the wrappers around it hold a pointer rather
    /// than a copy of it each: the futures of `apply` and its kin are large,
    /// and the deadline's layers would otherwise put several copies on the
    /// stack of whatever polls them.
    pub(crate) fn submitted<'a, T: 'a>(
        &'a self,
        submitted: std::time::Instant,
        call: impl std::future::Future<Output = Result<T, TrellisError>> + Send + 'a,
    ) -> impl std::future::Future<Output = Result<T, TrellisError>> + Send + 'a {
        let call: std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send + 'a>> =
            Box::pin(call);
        crate::deadline::bounded(
            submitted,
            self.call_budget,
            crate::instance_log::scoped(self.instance.clone(), call),
        )
    }

    /// The connection pool backing this instance. Exposed for callers that
    /// need to run their own queries against target tables; not needed for
    /// anything [`Trellis`]'s own methods already cover. Its connections run
    /// with `row_security = off`, so a query that row-level
    /// security would filter for the login role fails instead: read through
    /// your own connection where you rely on policies to filter. They also
    /// run with `jit = off`.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// The resolved configuration (schema names, DSN) this instance connected
    /// with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// A handle onto this process's in-process metrics registry,
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
        self.call(async {
            crate::migrate(&self.pool, &self.config)
                .await
                .map_err(TrellisError::Engine)
        })
        .await
    }

    /// **The** entrypoint for every definition-changing operation: parse one
    /// statement of Trellis's own grammar and run whatever it says (ADR-0012,
    /// ADR-0014).
    ///
    /// Parsing decides the operation, so this signature does not change as
    /// operations are added — a new operation is a grammar addition and a new
    /// [`Applied`] variant, not a new method every binding has to mirror. That
    /// is the whole point: text is the simplest thing to carry across an FFI
    /// boundary (one string in, plain data out), which is why the typed
    /// `define`/`pause_transform`/`resume_transform`/`resume_column`/
    /// `drop_transform` methods this replaces are gone rather than kept
    /// alongside it.
    ///
    /// A caller that accepts only some forms (a binding's `define` takes only
    /// `TRANSFORM`) asks [`crate::statement_kind`] first: it parses the text
    /// the same way, applies nothing, and so can refuse the wrong form before
    /// anything happens.
    ///
    /// The statements it accepts (see `defs::parse_statement` for the full
    /// grammar, and `docs/transforms.md` for the semantics):
    ///
    /// ```text
    /// TRANSFORM <target> FROM <source> [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]
    /// TRANSFORM <target> FROM <source> [LEFT|INNER] JOIN <other> ON <equi-join>
    ///   GROUP BY <keys> SELECT <fields> [WHERE <predicate>]
    ///
    /// PAUSE  TRANSFORM <target>[.<column>]
    /// RESUME TRANSFORM <target>[.<column>]
    /// DROP   TRANSFORM <target>
    ///
    /// ALTER TRANSFORM <target> <clause>[, <clause> ...]
    /// ```
    ///
    /// # Addressing
    ///
    /// A transform is addressed by its **bare** target-table name, and a
    /// dotted address means `<transform>.<column>` — not
    /// `<schema>.<transform>`, except under `DROP`, which removes a whole
    /// definition and so reads `a.b` as its qualified name.
    ///
    /// # What each statement does
    ///
    /// `TRANSFORM` registers a definition, as the retired `define` did —
    /// including that **a transform
    /// returns before its backfill starts** (see this module's doc comment):
    /// the new transform's status is
    /// [`TransformStatus::WaitingToBackfill`] and the target is populated in
    /// the background, whatever the transform's shape. Poll
    /// [`Trellis::status`] with the bare table name for [`TransformStatus::Live`].
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
    /// `DROP TRANSFORM <target>.<column>` is not a form: dropping one
    /// calculated field is an `ALTER TRANSFORM ... DROP <field>`, tracked
    /// separately, not a `DROP`.
    pub async fn apply(&self, statement_text: &str) -> Result<Applied, TrellisError> {
        self.call(async {
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
                    Ok(Applied::TransformDefined(definition.target_table))
                }
                defs::Statement::Pause(reference) => self.apply_pause(reference).await,
                defs::Statement::Resume(reference) => self.apply_resume(reference).await,
                defs::Statement::Drop(reference) => self.apply_drop(reference).await,
                defs::Statement::AlterTransform(alter) => self.apply_alter(&alter).await,
            }
        })
        .await
    }

    /// [`Statement::AlterTransform`](defs::Statement::AlterTransform)'s half
    /// of [`apply`](Trellis::apply) (ADR-0015) — a thin
    /// facade wrapper over [`defs::alter_transform`], which does the actual
    /// work (idempotency, validation, DDL, registering the field build,
    /// version fencing); see that function's own doc comment for the full
    /// contract.
    async fn apply_alter(&self, alter: &defs::AlterTransform) -> Result<Applied, TrellisError> {
        let outcome = defs::alter_transform(&self.pool, alter)
            .await
            .map_err(TrellisError::Catalog)?;
        Ok(Applied::Altered {
            target_table: outcome.definition.target_table,
            added: outcome.added,
            dropped: outcome.dropped,
            altered: outcome.altered,
        })
    }

    /// The grammar reads `a.b` as transform `a`, column `b`. When `a.b` is
    /// instead a registered transform's qualified name (the spelling `apply`
    /// returns), it names that whole transform: the reference with the
    /// qualified name as its target and no column. See
    /// [`Trellis::whole_transform_named`] for which reading wins when both
    /// resolve.
    async fn qualified_or_column(
        &self,
        reference: defs::TransformRef,
    ) -> Result<defs::TransformRef, TrellisError> {
        let Some(column) = &reference.column else {
            return Ok(reference);
        };
        Ok(
            match self
                .whole_transform_named(&reference.target, column)
                .await?
            {
                Some(_) => defs::TransformRef {
                    target: format!("{}.{column}", reference.target),
                    column: None,
                },
                None => reference,
            },
        )
    }

    /// Resolves the `first.second` shape, which is both `<transform>.<column>`
    /// (ADR-0003) and a qualified `<schema>.<table>` name: the bare name of
    /// the transform registered as `first.second`, or `None` when the address
    /// is a column one. Column addressing wins when transform `first` declares
    /// a field `second`, since a column has no other spelling while the
    /// transform can still be named bare.
    async fn whole_transform_named(
        &self,
        first: &str,
        second: &str,
    ) -> Result<Option<String>, TrellisError> {
        let bare = {
            let client = self.pool.get().await?;
            defs::catalog::bare_target_name(&**client, &format!("{first}.{second}"))
                .await
                .map_err(TrellisError::Catalog)?
        };
        let Some(bare) = bare else {
            return Ok(None);
        };
        let declares_column = defs::catalog::definition_by_target(&self.pool, first)
            .await
            .map_err(TrellisError::Catalog)?
            .is_some_and(|definition| {
                definition
                    .def
                    .fields
                    .iter()
                    .any(|field| field.name == second)
            });
        Ok((!declares_column).then_some(bare))
    }

    /// Parses a quarantine address for the facade's lookups: as
    /// [`QuarantineTarget::parse`] does, except that a `first.second` address
    /// naming a registered transform's qualified name
    /// ([`Trellis::whole_transform_named`]) is that whole transform, as its
    /// bare [`QuarantineTarget::Transform`]. A qualified name with the wrong
    /// schema stays a column address, of a transform that isn't there.
    async fn resolve_quarantine_address(
        &self,
        address: &str,
    ) -> Result<QuarantineTarget, TrellisError> {
        let parsed = QuarantineTarget::parse(address);
        if let QuarantineTarget::Column(first, second) = &parsed
            && let Some(bare) = self.whole_transform_named(first, second).await?
        {
            return Ok(QuarantineTarget::Transform(bare));
        }
        Ok(parsed)
    }

    /// [`Statement::Pause`](defs::Statement::Pause)'s half of
    /// [`apply`](Trellis::apply).
    async fn apply_pause(&self, reference: defs::TransformRef) -> Result<Applied, TrellisError> {
        let defs::TransformRef { target, column } = self.qualified_or_column(reference).await?;
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
        let defs::TransformRef { target, column } = self.qualified_or_column(reference).await?;
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
    /// a source table (ADR-0002). The staging worker's next
    /// reconcile pass uninstalls the capture triggers of a table nothing reads
    /// any more, so the process applying a `DROP` needs no privileges on the
    /// source tables.
    async fn apply_drop(&self, reference: defs::DefinitionRef) -> Result<Applied, TrellisError> {
        match reference {
            defs::DefinitionRef::Transform(target) => {
                // A dotted address is a qualified name. One that names no
                // transform while its first half does reads as
                // `<transform>.<column>`: refuse it rather than report a
                // successful drop of nothing.
                if let Some((first, second)) = target.split_once('.') {
                    let client = self.pool.get().await?;
                    let names_none = defs::catalog::bare_target_name(&**client, &target)
                        .await
                        .map_err(TrellisError::Catalog)?
                        .is_none();
                    if names_none
                        && defs::catalog::bare_target_name(&**client, first)
                            .await
                            .map_err(TrellisError::Catalog)?
                            .is_some()
                    {
                        return Err(TrellisError::Parse(
                            defs::ParseError::MalformedDefinitionAddress {
                                statement: "DROP TRANSFORM".to_string(),
                                address: target.clone(),
                                detail: format!(
                                    "DROP removes a whole definition, named bare or \
                                     '<schema>.<table>', and no transform is '{target}'; \
                                     dropping the calculated field '{second}' of '{first}' \
                                     is ALTER TRANSFORM {first} DROP {second}"
                                ),
                            },
                        ));
                    }
                }
                defs::lifecycle::drop_transform(&self.pool, &target)
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
    /// upstream that isn't `live` shows [`TransformStatus::CatchingUp`].
    /// Each also carries why its build keeps failing, if it
    /// does, and why the drain halted on it, if it did, so one
    /// listing shows every stuck definition: a health check that lists them
    /// and finds a [`DefinitionSummary::halt`] has a definition stopped until
    /// an operator fixes the cause and resumes it.
    pub async fn definitions(&self) -> Result<Vec<DefinitionSummary>, TrellisError> {
        self.call(async {
            let mut client = self.pool.get().await?;
            // One repeatable-read transaction, so the reported statuses are
            // derived from the same catalog state the rows come from.
            let txn = client
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await?;
            // The same failing-chunk join `status` reads its
            // `backfill_failure` from, in the one listing query (the chunk join
            // takes one chunk, so it adds at most one row per definition), and
            // the definition's halt (`capture_failures` is keyed by it).
            let rows = txn
                .query(
                    &format!(
                        "select d.id, d.target_table, d.created_at, d.source_table, \
                            {CHUNK_FAILURE_COLUMNS}, \
                            cf.source_table as capture_table, cf.columns as capture_columns, \
                            cf.error as capture_error, cf.detected_at as capture_detected_at, \
                            cf.kind as capture_kind \
                     from transform_definitions d \
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
                        target_table: row.get(1),
                        status: reported[&id],
                        created_at: row.get(2),
                        backfill_failure: backfill_failure(&row),
                        halt: capture_failure(&row),
                    }
                })
                .collect())
        })
        .await
    }

    /// SQL that loads this instance's definitions into a database built from
    /// a schema-only dump: `INSERT`s for the Trellis migration
    /// history, the instance marker and the transform
    /// definitions with the catalog rows they need, and for nothing else.
    /// Append it to a schema-only `pg_dump` of the Trellis schema and the
    /// schemas it maintains, the way Rails appends `schema_migrations`.
    ///
    /// Every definition loads "defined, not built": the state a define
    /// commits. Runtime state (the ledger, the ring, build chunks,
    /// quarantine, claims) is never dumped, because it describes rows a
    /// schema-only dump doesn't carry. The worker that starts on the loaded
    /// database builds the definitions.
    pub async fn dump_definitions_sql(&self) -> Result<String, TrellisError> {
        self.call(crate::dump::dump_definitions_sql(&self.pool, &self.config))
            .await
    }

    /// One registered transform definition's current [`TransformStatus`],
    /// by target table name — the read a host-language embedder
    /// polls after [`apply`](Trellis::apply) registers a definition, per
    /// `docs/decisions/0008-public-api-design.md`'s decision 1 ("define, then poll status
    /// until live").
    ///
    /// [`TransformStatus::Live`] means the transform is in its steady state
    /// (ADR-0002, "What `live` promises"): from then on, a
    /// [`Trellis::watermark_token`] taken after a commit and awaited with
    /// [`Trellis::await_converged`] guarantees its target reflects that
    /// commit. A `live` 1-1 one rebuilding fields (an `ALTER
    /// TRANSFORM` that added or changed fields, a resumed column) reports
    /// [`TransformStatus::Backfilling`] until they are built, and so
    /// does an aggregate over a partitioned source while it is corrected for a
    /// partition that was attached, detached, dropped or truncated (ADR-0017),
    /// each listed in [`DefinitionStatus::correcting`]. A `live`
    /// one reading an upstream (another definition's target, as its source or
    /// as a `JOIN`'s joined table) that isn't `live` itself (paused,
    /// quarantined, rebuilding or catching up, down the whole chain) reports
    /// [`TransformStatus::CatchingUp`]: it keeps applying, but its
    /// target doesn't reflect the source yet. That status is derived from the
    /// upstream's status when read, never stored.
    ///
    /// Also reports why a definition isn't getting there, when the cause is a
    /// failing build ([`DefinitionStatus::backfill_failure`]): a failing
    /// chunk of it. It is retried with backoff, so without this the only sign
    /// would be a warning in a worker's log.
    ///
    /// `target_table` is the name `apply` returns (`schema.table`) or the bare table
    /// name; a qualified name with another schema matches nothing.
    pub async fn status(
        &self,
        target_table: &str,
    ) -> Result<Option<DefinitionStatus>, TrellisError> {
        self.call(async {
            let mut client = self.pool.get().await?;
            // `target_table` is either spelling of the transform's name, the
            // qualified one `apply` returns or the bare one its text declared
            // (`defs::catalog::target_match_sql`).
            let txn = client
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await?;
            let row = txn
                .query_opt(
                    &format!(
                        "select d.status, d.id, d.source_table, d.definition_text, d.grammar_version, \
                            d.join_table, \
                            {CHUNK_FAILURE_COLUMNS}, \
                            cf.source_table as capture_table, cf.columns as capture_columns, \
                            cf.error as capture_error, cf.detected_at as capture_detected_at, \
                            cf.kind as capture_kind, \
                            exists (select 1 from column_status cs \
                                    where cs.transform_table = split_part(d.target_table, '.', 2) \
                                    and cs.awaiting_capture) \
                              as awaiting_capture, \
                            (select bc.fence_xid::text::bigint from backfill_chunks bc \
                             where bc.definition_id = d.id and bc.kind = 'plan' \
                               and not bc.done and bc.fence_xid is not null \
                               and pg_snapshot_xmin(pg_current_snapshot()) <= bc.fence_xid \
                             limit 1) as build_fence, \
                            (select count(*) from poison p where p.transform_id = d.id) \
                              as held_count, \
                            (select min(p.poisoned_at) from poison p \
                             where p.transform_id = d.id) as held_since, \
                            (select coalesce(max(p.retry_attempts), 0) from poison p \
                             where p.transform_id = d.id) as held_retries, \
                            (select min({due}) from poison p \
                             where p.transform_id = d.id) as held_retry_at \
                     from transform_definitions d \
                     {CHUNK_FAILURE_JOIN} \
                     left join capture_failures cf on cf.transform_id = d.id \
                     where {matches}",
                        due = quarantine::retry_due_sql(2, 3),
                        matches = defs::catalog::target_match_sql("d.target_table", 1),
                    ),
                    &[
                        &target_table,
                        &self.held_key_retry.map(|r| r.sql_backoffs().0),
                        &self.held_key_retry.map(|r| r.sql_backoffs().1),
                    ],
                )
                .await?;
            let mut status = None;
            let mut paused_upstream_columns = Vec::new();
            let mut holdup = (None, None);
            let mut drain_failure = None;
            let mut source_index_gaps = Vec::new();
            let mut join = None;
            let mut correcting = Vec::new();
            if let Some(row) = &row {
                let status_text: String = row.get(0);
                let stored = StoredStatus::from_row(&status_text);
                let (reported, causes) =
                    reported_status_with_causes(&*txn, row.get(1), stored).await?;
                status = Some(reported);
                paused_upstream_columns = causes;
                correcting =
                    crate::staging::partition_correction::correcting(&*txn, row.get(1)).await?;
                // An edit whose new field awaits a widen is stuck on
                // capture as much as a registration is. Not one a schema change
                // paused: capture stops widening for it, and its field
                // waits for the resume its `capture_failure` asks for, not on
                // the table's lock or install failure.
                let capture_failed = row.get::<_, Option<String>>("capture_table").is_some();
                if !capture_failed
                    && (stored == StoredStatus::WaitingToBackfill || row.get("awaiting_capture"))
                {
                    holdup = self.capture_holdup(&*txn, row.get(1), row.get(2)).await?;
                }
                // A page the drain keeps failing on, charged to no one,
                // holds back every definition still applying what it reads.
                if !stored.is_frozen() {
                    drain_failure =
                        crate::staging::holdup::for_definition(
                            &*txn,
                            row.get("source_table"),
                            row.get::<_, Option<&str>>("join_table"),
                        )
                        .await?;
                }
                // A stored definition always parses.
                if let Ok(def) = crate::defs::grammar::load(
                    row.get("definition_text"),
                    row.get("grammar_version"),
                ) {
                    // Only an aggregate's requirement reads the recorded
                    // source columns (whether the group basis takes it).
                    let source_columns =
                        if matches!(def.key_space, defs::ast::KeySpace::Aggregate { .. }) {
                            defs::catalog::definition_by_id_in(&*txn, row.get(1))
                                .await?
                                .map(|d| d.source_columns)
                                .unwrap_or_default()
                        } else {
                            Default::default()
                        };
                    source_index_gaps = defs::source_index::gaps(
                        &*txn,
                        &def,
                        &source_columns,
                        row.get("source_table"),
                        row.get::<_, Option<String>>("join_table").as_deref(),
                    )
                    .await?;
                    join = crate::staging::join_status::for_definition(
                        &*txn,
                        self.config.schema(),
                        row.get(1),
                        row.get("source_table"),
                        row.get::<_, Option<String>>("join_table").as_deref(),
                        &def,
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
                held_keys: quarantine::held_keys_from(
                    row.get("held_count"),
                    row.get("held_since"),
                    row.get("held_retries"),
                    row.get("held_retry_at"),
                ),
                drain_failure,
                source_index_gaps,
                join,
                correcting,
                paused_upstream_columns,
                build_wait: row
                    .get::<_, Option<i64>>("build_fence")
                    .map(|xid| BuildWait::Fence { xid }),
            }))
        })
        .await
    }

    /// Pauses, now, the readers of every captured table whose Trellis
    /// capture was changed outside Trellis or whose table is gone, and
    /// returns those tables. The staging worker's reconcile pass does
    /// the same on its own cadence (`capture_failure` of kind
    /// [`CaptureFailureKind::Disruption`]); this is for a caller that has
    /// just done something that disrupts capture (a fixture or bulk load that
    /// ends with `ENABLE TRIGGER ALL`) and needs the readers paused before it
    /// goes on, rather than a pass later. It installs, repairs and rebuilds
    /// nothing, and runs beside a worker. A `RESUME` of each paused
    /// definition is still the caller's.
    pub async fn detect_capture_disruptions(&self) -> Result<Vec<String>, TrellisError> {
        self.call(async {
            let desired = crate::defs::tables_to_capture(&self.pool).await?;
            let mut client = self.pool.get().await?;
            crate::capture::reconcile::detect_disruptions(
                &mut client,
                self.config.schema(),
                &desired,
            )
            .await
            .map_err(|err| TrellisError::Client(crate::client::capture_pass_error(err)))
        })
        .await
    }

    /// What holds up the capture a definition sourced from `source_table`
    /// waits on: the lock wait, then the other
    /// failure, that the staging worker's latest pass recorded in
    /// `capture_holdups` for the source, or for a `JOIN`'s joined table.
    /// The staging worker may run in any process.
    async fn capture_holdup(
        &self,
        client: &impl tokio_postgres::GenericClient,
        definition_id: i64,
        source_table: &str,
    ) -> Result<(Option<CaptureWait>, Option<CaptureFailure>), TrellisError> {
        let rows = client
            .query(
                "select h.table_name, h.since, h.operation, h.lock_mode, h.observed_at, \
                        h.blockers, h.error, h.columns \
                 from capture_holdups h \
                 join (select $1::text as t, 0::bigint as ord \
                       union all \
                       select join_table, 0 from transform_definitions \
                       where id = $2 and join_table is not null) read \
                   on read.t = h.table_name \
                 order by read.ord, read.t",
                &[&source_table, &definition_id],
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

    /// Rebuilds every applying definition that reads `source_table`: this call's own
    /// transaction moves each `live -> backfilling` and enqueues its plan and sweep
    /// jobs, one row each, so the call returns quickly and the status already reads
    /// `backfilling` when it does. The definition keeps applying. Its plan re-derives
    /// every row the table has, and its sweep deletes what the table no longer backs,
    /// and then it is `live` again. A newly registered transform doesn't need this: the
    /// staging worker starts its build itself.
    ///
    /// `source_table` is the captured table's `schema.table` name, matched
    /// exactly, or a bare name that exactly one captured table carries (any
    /// schema); a bare name two captured tables share is refused
    /// ([`TrellisError::AmbiguousSourceTable`]).
    ///
    /// Only valid for a table the staging worker captures: one some
    /// registered definition reads, and that isn't one of this instance's own
    /// targets (the set its
    /// reconcile pass installs capture triggers on). Any other table is
    /// refused ([`TrellisError::TableNotCaptured`]): nothing reads it, and a
    /// table's first reader backfills it in full anyway. The check is the
    /// catalog's, not the triggers'.
    pub async fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        self.call(async {
            let mut client = self.pool.get().await?;
            let captured = crate::defs::tables_to_capture(&self.pool).await?;
            let qualified = match resolve_backfill_table(&captured, source_table)? {
                Some(qualified) => qualified,
                None => {
                    // Not captured. Tell a table that exists (nothing reads it)
                    // from one that doesn't. A bare name is looked up the way
                    // define resolves it; a table the role can't use is refused
                    // as such rather than as not found.
                    let (schema, table) = match source_table.split_once('.') {
                        Some((schema, table)) => (Some(schema), table),
                        None => (None, source_table),
                    };
                    let found = client
                        .query_opt(
                            "select table_schema from information_schema.tables \
                             where table_name = $2 \
                               and (table_schema = $1::text \
                                    or ($1::text is null \
                                        and table_schema = any(current_schemas(false)))) \
                             order by array_position(current_schemas(false), table_schema) \
                             limit 1",
                            &[&schema, &table],
                        )
                        .await?;
                    let Some(row) = found else {
                        defs::catalog::reject_inaccessible_table(&**client, schema, table).await?;
                        return Err(TrellisError::SourceTableNotFound(source_table.to_string()));
                    };
                    let found_schema: String = row.get(0);
                    return Err(TrellisError::TableNotCaptured {
                        table: format!("{found_schema}.{table}"),
                    });
                }
            };

            // A failure to park is a plain `Db` error.
            let txn = client.transaction().await?;
            let rebuilt = crate::intake::catch_up::park_table_catch_ups(
                &*txn,
                std::slice::from_ref(&qualified),
                false,
            )
            .await
            .map_err(|err| match err {
                IntakeError::Db(err) => TrellisError::Db(err),
                IntakeError::Catalog(err) => TrellisError::Catalog(*err),
                err => TrellisError::Client(ClientError::Intake(err)),
            })?;
            crate::staging::build::stamp_rebuild_seg(&*txn, &rebuilt).await?;
            txn.commit().await?;
            Ok(())
        })
        .await
    }

    /// Poison-quarantine entries recorded since `watermark`, oldest first —
    /// the keys the apply path gave up on, each for the definition whose
    /// apply failed (every other definition reading the key keeps
    /// applying it). A running client surfaces these so a whole-table
    /// failure (every row poisoned) doesn't sit silently.
    pub async fn poisoned_since(
        &self,
        watermark: SystemTime,
    ) -> Result<Vec<PoisonEntry>, TrellisError> {
        self.call(async {
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
        })
        .await
    }

    /// Every currently paused/quarantined target, across every transform —
    /// `docs/decisions/0003-quarantine-storage-and-api.md`'s "The client API
    /// is three reads, separated by cost, and one release," read 1. Cheap:
    /// reads `transform_definitions`' lifecycle status plus the small, sparse
    /// `column_status` table, no join against poisoned-row detail. The read a
    /// dashboard/health-check polls.
    pub async fn quarantined(&self) -> Result<Vec<QuarantineEntry>, TrellisError> {
        self.call(async {
            let client = self.pool.get().await?;
            let mut entries = Vec::new();

            // Read back the bare table-name suffix, not the persisted
            // fully-qualified `target_table` — `QuarantineTarget::Transform`
            // round-trips through
            // `docs/decisions/0003-quarantine-storage-and-api.md`'s
            // `transform.column` addressing scheme elsewhere in this API
            // (`quarantine_status`, `resume_column`, `sample_quarantined`, all
            // keyed on the bare name the grammar accepts back), which parses an
            // address on its first `.` — handing it a qualified
            // `"schema.table"` spelling here would make every entry in this
            // list misparse as a column address the moment a target table
            // lived outside the default schema.
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
        })
        .await
    }

    /// The current state of one target (`transform` or `transform.column`,
    /// per ADR-0003's addressing scheme) — read 2. Errors with
    /// [`TrellisError::TransformNotFound`] if the named transform doesn't
    /// exist at all; a `transform.column` address for a real transform whose
    /// named column simply isn't paused reports [`QuarantineState::Live`],
    /// not an error (this call doesn't validate that the column name is one
    /// of the transform's actual fields — a paused column always is one, by
    /// construction, but a live one is reported the same way regardless of
    /// whether the name is real, matching "no rows here means live" for
    /// every column not individually tracked).
    ///
    /// The transform is named qualified (`schema.table`, as `apply` returns it) or
    /// bare; `transform.column` takes the bare transform.
    pub async fn quarantine_status(&self, target: &str) -> Result<QuarantineEntry, TrellisError> {
        self.call(async {
            let target = self.resolve_quarantine_address(target).await?;
            let client = self.pool.get().await?;
            match &target {
                QuarantineTarget::Transform(t) => {
                    // `t` is the bare name `QuarantineTarget::parse`
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
                    let stored = StoredStatus::from_row(&status_text);
                    // The status `Trellis::status` reports.
                    let status = reported_status(&**client, row.get(1), stored).await?;
                    Ok(QuarantineEntry {
                        target,
                        state: QuarantineState::from(status),
                        paused_at: None,
                        last_error: None,
                    })
                }
                QuarantineTarget::Column(t, c) => {
                    // Same bare-suffix match as the `Transform` arm
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
        })
        .await
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
        self.call(async {
            let target = self.resolve_quarantine_address(target).await?;
            let client = self.pool.get().await?;
            let rows = match &target {
                QuarantineTarget::Column(t, c) => match &after {
                    Some((after_src, after_key)) => {
                        client
                            .query(
                                "select src_table, key, error, 0, null::timestamptz from column_failures \
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
                                "select src_table, key, error, 0, null::timestamptz from column_failures \
                             where transform_table = $1 and column_name = $2 \
                             order by src_table, key limit $3",
                                &[t, c, &limit],
                            )
                            .await?
                    }
                },
                QuarantineTarget::Transform(t) => {
                    // `t` is bare — same `split_part` match as
                    // `status`/`quarantine_status`. Whole-key poison is per
                    // transform, so the sample is this definition's own
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
                                    "select src_table, key, last_error, retry_attempts, poisoned_at from poison \
                                 where transform_id = $1 and (src_table, key) > ($2, $3) \
                                 order by src_table, key limit $4",
                                    &[&id, after_src, after_key, &limit],
                                )
                                .await?
                        }
                        None => {
                            client
                                .query(
                                    "select src_table, key, last_error, retry_attempts, poisoned_at from poison \
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
                .map(|row| {
                    let retry_attempts = u32::try_from(row.get::<_, i32>(3)).unwrap_or(0);
                    let poisoned_at: Option<SystemTime> = row.get(4);
                    PoisonSample {
                        src_table: row.get(0),
                        key: row.get(1),
                        error_message: row.get(2),
                        retry_attempts,
                        next_retry_at: self
                            .held_key_retry
                            .zip(poisoned_at)
                            .and_then(|(retry, at)| at.checked_add(retry.backoff(retry_attempts))),
                    }
                })
                .collect())
        })
        .await
    }

    /// Releases one key `transform` holds in quarantine
    /// (`docs/decisions/0003-quarantine-storage-and-api.md`, "Releasing held
    /// keys"), once its cause is fixed: `source_table` and `key` as
    /// [`Trellis::sample_quarantined`] and [`Trellis::poisoned_since`] report
    /// them, the table in either spelling, `schema.table` or bare, and
    /// `transform` the target table, bare or qualified, as [`Trellis::status`] takes it.
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
    /// (`crate::locks::LOCK_TIMEOUT`, 30 s) makes it error with
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
        self.call(async {
            match quarantine::release_key(&self.pool, transform, source_table, key).await {
                Ok(_) => Ok(()),
                Err(ApplyError::TransformNotFound { transform }) => {
                    Err(TrellisError::TransformNotFound(transform))
                }
                Err(err) => Err(TrellisError::Apply(err)),
            }
        })
        .await
    }

    /// Stops any background work this connection started (staging worker and
    /// drain workers) and waits for it to exit cleanly. A no-op for a
    /// connection that started none.
    ///
    /// Outside the call deadline (issue #599): it ends background work rather
    /// than serving a request, and each worker's own lock waits are bounded by
    /// the lock timeout.
    pub async fn shutdown(self) -> Result<(), TrellisError> {
        crate::instance_log::scoped(self.instance, async move {
            if let Some(client) = self.client {
                client.shutdown().await.map_err(TrellisError::Client)?;
            }
            Ok(())
        })
        .await
    }

    /// Whether at least one live drain worker (a connection running with
    /// `drain_threads > 0`) is registered anywhere in this fleet right now
    /// (`docs/decisions/0010-embeddable-clients.md`, decision 3). A single, cheap `exists(...)`
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
    /// [`Trellis::has_live_staging_worker`] is for. See
    /// `docs/embedding.md`'s health-check section for a worked Phoenix/Rails
    /// example.
    ///
    /// **"Live" reuses the reclaim TTL's own notion of liveness** rather
    /// than inventing a second one, per the issue's explicit instruction:
    /// this compares each worker's last heartbeat against
    /// `DEFAULT_RECLAIM_TTL` — the exact threshold
    /// `crate::staging::liveness::reclaim_stale` already uses to decide a
    /// *claim* is dead, and the same value [`ClientOptions::default`]'s own
    /// `reclaim_ttl` carries (this facade never exposes a way to override
    /// it — every [`Trellis::connect`] call already gets this one value
    /// today, background `Client` or not). See
    /// `crate::staging::worker_registry`'s doc comment for why this is a
    /// read-time comparison rather than something that needs a reclaim pass
    /// to have already run — a design that did would be wrong precisely in
    /// the all-`drain_threads: 0` fleet this method exists to catch, since
    /// nothing in such a fleet would ever run one.
    pub async fn has_live_drain_workers(&self) -> Result<bool, TrellisError> {
        self.call(async {
            let client = self.pool.get().await?;
            Ok(worker_registry::has_live_workers(&**client, DEFAULT_RECLAIM_TTL).await?)
        })
        .await
    }

    /// Whether this instance's staging worker (a connection running with
    /// `staging: true`) is running anywhere in the fleet right now —
    /// [`Trellis::has_live_drain_workers`]'s counterpart, and meant
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
    /// triggers in the application's own transactions meanwhile,
    /// but nothing seals or dispatches a backfill. A connection that stands
    /// by (`staging: true`, but another worker holds the singleton) does not
    /// count: the check is `true` only while some worker leads.
    pub async fn has_live_staging_worker(&self) -> Result<bool, TrellisError> {
        self.call(async {
            let client = self.pool.get().await?;
            Ok(
                crate::staging::session::producer_is_running(&**client, self.config.schema())
                    .await?,
            )
        })
        .await
    }

    /// A read-your-writes watermark: `pg_current_wal_insert_lsn()`,
    /// read on a freshly acquired pool connection — see
    /// `crate::staging::converge::watermark_token` for the exact
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
    /// runs, even for a writer with `synchronous_commit = off`.
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
        self.call(async {
            let client = self.pool.get().await?;
            Ok(converge::watermark_token(&**client).await?)
        })
        .await
    }

    /// Blocks until every effect committed at or before `token` (see
    /// [`Trellis::watermark_token`]) has been reflected in its target(s), or
    /// `timeout` elapses — the read-your-writes primitive an embedder
    /// reaches for after writing to a source table and needing to see that
    /// write's effects in a transform's target table.
    ///
    /// A thin facade over `crate::staging::converge::await_converged` (see
    /// its doc comment for the polling/backoff shape and exactly what
    /// "reflected" means). Errors with
    /// [`TrellisError::Staging`]`(`[`StagingError::ConvergenceTimeout`]`)`
    /// on timeout — a named, matchable condition rather than a generic
    /// failure, whose [`TrellisError::code`] is [`ErrorCode::Timeout`].
    /// `timeout` holds even while a poll is blocked, on a lock say: the
    /// server abandons the poll at the deadline, or 250ms into the poll if
    /// that's later, since every poll gets at least that long.
    /// So even a zero `timeout` makes one real check.
    ///
    /// Capture triggers write a change's ring rows in the writer's own
    /// transaction, so every commit at or below `token` is
    /// already in the ring when the token is read: this waits only for the
    /// ring rows at or below it to drain, and writes nothing.
    ///
    /// This waits for captured changes only. It doesn't read definition
    /// status or backfill progress: a definition that isn't
    /// [`TransformStatus::Live`] yet (including one reporting
    /// [`TransformStatus::CatchingUp`]) may still be missing rows after this
    /// returns. Check [`Trellis::status`] for that. Once it reports `live`,
    /// a token taken after a commit and awaited here covers that commit
    /// (ADR-0002, "What `live` promises"). That includes a repair that
    /// rebuilds a definition ([`Trellis::request_backfill`]): the definition
    /// reads `backfilling` from the call's return, so polling for `live`
    /// first waits out the rebuild.
    ///
    /// Like every call, this lasts at most the call's deadline (30 seconds,
    /// issue #599): a longer `timeout` is cut to what is left of it, and a
    /// caller that wants to wait longer calls again.
    ///
    /// Holds one pooled connection for the whole call (it polls on it), so a
    /// long `timeout` on a small `max_call_pool_size` is a real, if bounded, draw
    /// on this [`Trellis`]'s own pool — note the background [`Client`]'s
    /// drain workers use a *separate* pool, so this can't starve them.
    pub async fn await_converged(
        &self,
        token: PgLsn,
        timeout: Duration,
    ) -> Result<(), TrellisError> {
        self.call(async {
            let client = self.pool.get().await?;
            // A call lasts at most its deadline (issue #599), so a longer wait
            // is capped to what is left of it. A caller that wants to wait
            // longer calls again: each call is one bounded wait.
            let timeout = match crate::deadline::current() {
                Some(deadline) => timeout.min(deadline.remaining()),
                None => timeout,
            };
            Ok(converge::await_converged(&client, token, timeout).await?)
        })
        .await
    }

    /// Starts a check of one target's persisted rows against an
    /// independently-rendered Postgres recompute — ADR-0013's
    /// production recompute audit — and returns the job at once. Read-only.
    ///
    /// The comparison is a background job: this call registers
    /// it and returns, and a process that runs drain workers
    /// (`drain_threads > 0`, any process of the fleet) walks the target a
    /// keyset page at a time on a task beside them, saving
    /// its progress as it goes. Poll [`Trellis::self_check_job`] with the
    /// returned job's `id` until its state is finished
    /// ([`SelfCheckJobState::is_finished`](crate::SelfCheckJobState::is_finished)); [`SelfCheckJob::report`](crate::SelfCheckJob::report) is then
    /// the verdict. With no drain worker anywhere the job stays
    /// [`Queued`](crate::SelfCheckJobState::Queued), as a define does. Nothing in this call
    /// reads the target's rows.
    ///
    /// `target_table` names a registered transform, same convention as
    /// [`Trellis::status`] (its target table, qualified or bare). An unknown
    /// one, and one `self_check` can't walk ([`SelfCheckError`]), are refused
    /// here, wrapped in [`TrellisError::SelfCheck`]. A fleet-wide sweep is a
    /// caller-side loop over [`Trellis::definitions`]. `mode` picks
    /// [`SelfCheckMode::Standard`] (safe under live load: a divergence must
    /// survive a re-check behind a fresh await before it's reported) or
    /// [`SelfCheckMode::Strict`] (skips the re-check — sound only once the
    /// caller has itself stopped writes to the audited tables). `timeout`
    /// (at most an hour) bounds each convergence await a page of the job makes (one per page
    /// under [`SelfCheckMode::Strict`], up to two under
    /// [`SelfCheckMode::Standard`]) — see [`Trellis::await_converged`]'s own
    /// doc comment for how to size it; a target that's merely still
    /// catching up reports
    /// [`crate::staging::self_check::SelfCheckOutcome::NotCaughtUp`], never a
    /// divergence.
    ///
    /// **One job per target at a time.** A target whose job is still
    /// `queued` or `running` gets that job back, whatever `mode` and
    /// `timeout` this call passed, so a second caller (or a retry) joins the
    /// check in progress instead of starting another. A finished job stays
    /// until the next call for its target replaces it.
    ///
    /// Only a `live` definition is compared: any other ends the job with
    /// [`crate::staging::self_check::SelfCheckOutcome::NotLive`] and its
    /// status, and nothing is awaited or compared. A rebuild
    /// ([`Trellis::request_backfill`]) is `backfilling` from the call's
    /// return, so "poll `live`, then check" checks after the repair. A job
    /// whose target stops being live, or whose capture breaks, partway ends
    /// there and reports what it had found, `truncated`.
    ///
    /// A currently-paused column (`docs/decisions/0003-quarantine-storage-and-api.md`)
    /// is excluded from the comparison entirely — its persisted value is
    /// deliberately stale, so comparing it would report a false divergence.
    ///
    /// The report carries the keys the definition holds in quarantine
    /// ([`SelfCheckReport::held_keys`](crate::SelfCheckReport::held_keys)), read when it is
    /// polled whatever the outcome, so a held key isn't hidden behind a `Converged`
    /// job that didn't reach it or a `NotCaughtUp` its parked changes cause.
    ///
    /// The job ends when its transform is dropped (and a poll then finds
    /// nothing) and when the worker running it shuts down (it is then
    /// [`Cancelled`](crate::SelfCheckJobState::Cancelled)).
    pub async fn self_check(
        &self,
        target_table: &str,
        mode: SelfCheckMode,
        timeout: Duration,
    ) -> Result<SelfCheckJob, TrellisError> {
        self.call(async {
            self_check_job::start(&self.pool, target_table, mode, timeout)
                .await
                .map_err(TrellisError::SelfCheck)
        })
        .await
    }

    /// The `self_check` job `id` ([`SelfCheckJob::id`](crate::SelfCheckJob::id), from
    /// [`Trellis::self_check`]): its state, how many keys it has compared so
    /// far, and, once it is [`Done`](crate::SelfCheckJobState::Done), its report. `None`
    /// when there is no such job: a newer `self_check` of its target replaced
    /// it, or its transform was dropped.
    pub async fn self_check_job(&self, id: i64) -> Result<Option<SelfCheckJob>, TrellisError> {
        self.call(async {
            self_check_job::get(&self.pool, id)
                .await
                .map_err(TrellisError::SelfCheck)
        })
        .await
    }

    /// The [`ClientOptions`] a connection's background [`Client`] runs with:
    /// the role flags and the runtime's thread cap, everything else default.
    fn client_options(options: &TrellisOptions) -> ClientOptions {
        ClientOptions {
            staging_worker: options.staging,
            application_threads: options.drain_threads,
            worker_threads: options.worker_threads,
            held_key_retry: options.held_key_retry,
            ..Default::default()
        }
    }

    /// Starts the background [`Client`] for a `staging`/`drain_threads`
    /// connection. The staging worker reads the tables to publish from the
    /// catalog itself, so an empty catalog is fine.
    fn start_client(config: &Config, options: &TrellisOptions) -> Result<Client, TrellisError> {
        let client_options = Self::client_options(options);
        // `start_with_config`, not `start(config.dsn(), ..)` —
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
    /// This is the very first thing [`Trellis::apply`] does with a parsed
    /// definition, and it returns [`TrellisError::SourceTableNotFound`]
    /// eagerly on an empty result, so it must accept both an
    /// explicitly-qualified `FROM <schema>.<table>` naming a table outside
    /// this connection's `search_path` and a bare `FROM <table>` chaining off
    /// another definition's target explicitly qualified into a non-default
    /// schema, before `install_definition`'s own resolution
    /// (`resolve_source_for_install`) is reached. It resolves the source
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
        // A table the role can't use is refused as such, naming
        // the missing privilege, rather than as not found (or, without
        // `USAGE` on a qualified name's schema, as Postgres's own refusal of
        // the column read below).
        let qualified = match explicit_source_schema {
            Some(schema) => {
                let client = self.pool.get().await?;
                defs::catalog::reject_inaccessible_table(&**client, Some(schema), source_table)
                    .await?;
                crate::intake::qualify(schema, source_table).map_err(CatalogError::from)?
            }
            None => match defs::catalog::resolve_graph_identity(&self.pool, source_table).await {
                Err(err @ CatalogError::SourceTableNotFound(_)) => {
                    let client = self.pool.get().await?;
                    defs::catalog::reject_inaccessible_table(&**client, None, source_table).await?;
                    return Err(err.into());
                }
                result => result?,
            },
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

/// Which captured table `request_backfill` names. A dotted name is the exact
/// `schema.table`; a bare one matches the table part of the captured set and
/// must match exactly one. `None` means no captured table matches.
fn resolve_backfill_table(captured: &[String], name: &str) -> Result<Option<String>, TrellisError> {
    if name.contains('.') {
        return Ok(captured.iter().find(|t| t.as_str() == name).cloned());
    }
    let mut matches: Vec<&String> = captured
        .iter()
        .filter(|t| t.rsplit_once('.').map_or(t.as_str(), |(_, bare)| bare) == name)
        .collect();
    matches.sort();
    match matches.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some((*one).clone())),
        many => Err(TrellisError::AmbiguousSourceTable {
            table: name.to_string(),
            tables: many.iter().map(|t| (*t).clone()).collect(),
        }),
    }
}

/// The status an operator reads for definition `id`, persisted as `stored`
/// (`defs::catalog::reported_statuses`). Only a `live` one can
/// differ, so any other skips the catalog read.
async fn reported_status(
    client: &impl tokio_postgres::GenericClient,
    id: i64,
    stored: StoredStatus,
) -> Result<TransformStatus, TrellisError> {
    Ok(reported_status_with_causes(client, id, stored).await?.0)
}

/// [`reported_status`] and the paused upstream columns behind a reported
/// `catching_up` ([`DefinitionStatus::paused_upstream_columns`]).
async fn reported_status_with_causes(
    client: &impl tokio_postgres::GenericClient,
    id: i64,
    stored: StoredStatus,
) -> Result<(TransformStatus, Vec<String>), TrellisError> {
    if stored != StoredStatus::Live {
        return Ok((stored.into(), Vec::new()));
    }
    let (statuses, mut causes) =
        crate::defs::catalog::reported_statuses_with_causes(client).await?;
    Ok((
        statuses.get(&id).copied().unwrap_or(stored.into()),
        causes.remove(&id).unwrap_or_default(),
    ))
}

/// The columns [`backfill_failure`] reads a failing build chunk from, joined
/// by [`CHUNK_FAILURE_JOIN`].
const CHUNK_FAILURE_COLUMNS: &str = "bcf.attempts as chunk_attempts, \
     bcf.last_error as chunk_last_error, bcf.next_attempt_at as chunk_next_attempt_at";

/// The definition's current build chunk that has failed, if any, aliased
/// `bcf` for [`CHUNK_FAILURE_COLUMNS`]: the one that has failed most,
/// leaving out finished chunks and those of a build a resume superseded.
const CHUNK_FAILURE_JOIN: &str = "left join lateral ( \
         select bc.attempts, bc.last_error, bc.next_attempt_at from backfill_chunks bc \
         where bc.definition_id = d.id and not bc.done and bc.last_error is not null \
           and bc.fuse_rearmed_at is not distinct from d.fuse_rearmed_at \
         order by bc.attempts desc, bc.id limit 1 \
     ) bcf on true";

/// The [`BackfillFailure`] in a row of [`Trellis::status`]'s or
/// [`Trellis::definitions`]' query: the definition's own failing build chunk
/// ([`CHUNK_FAILURE_COLUMNS`]), with its `attempts`, `last_error` and
/// `next_attempt_at` on the definition's `source_table`. `None` when no chunk
/// is failing.
fn backfill_failure(row: &tokio_postgres::Row) -> Option<BackfillFailure> {
    let last_error = row.get::<_, Option<String>>("chunk_last_error")?;
    Some(BackfillFailure {
        source_table: row.get("source_table"),
        attempts: u32::try_from(row.get::<_, i32>("chunk_attempts")).unwrap_or(0),
        last_error,
        next_attempt_at: row.get("chunk_next_attempt_at"),
    })
}

/// What a Re-derive build waits on, as [`DefinitionStatus::build_wait`]
/// reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildWait {
    /// The build's source is another definition's target, fed by the
    /// target-mutation seam, and no chunk of the build runs until every
    /// transaction that was open when its fence was taken has ended: a
    /// writer of that target could otherwise commit after a chunk's snapshot
    /// without the build seeing its rows. `xid` is the fence's transaction id
    /// (`pg_current_xact_id()`): the wait ends once the oldest running
    /// transaction in the cluster, `pg_snapshot_xmin(pg_current_snapshot())`,
    /// is past it, so the transaction to look for in `pg_stat_activity` is
    /// one with a `backend_xid` at or below `xid` (a transaction gets one
    /// when it first writes or locks a row; a read-only one holds no fence).
    /// It clears on its own when that transaction ends.
    Fence {
        /// The fence's transaction id.
        xid: i64,
    },
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
#[non_exhaustive]
pub struct DefinitionStatus {
    /// Where the transform is in its lifecycle.
    pub status: TransformStatus,
    /// Set while one of the definition's build chunks keeps failing:
    /// it is waiting out a backoff before it is retried, or is being narrowed
    /// to the key it fails on. It clears once the chunk finishes. A key the
    /// narrowing quarantines is reported by [`Trellis::quarantined`] instead,
    /// and the build finishes without it. A definition `paused` with this set
    /// was paused by its build: a chunk kept failing in a way no retry or
    /// narrowing gets past (a missing table or column, say). Fix the cause
    /// and resume it, which rebuilds it and clears this.
    pub backfill_failure: Option<BackfillFailure>,
    /// Set while the definition waits on capture, because the staging worker
    /// can't yet take the lock it needs to install or widen the capture
    /// triggers on its source or a `JOIN`'s joined table. It
    /// clears on its own once the lock holder lets go. A definition waits on capture while it is `waiting_to_backfill`,
    /// or while an `ALTER TRANSFORM` field it gained is paused until the
    /// capture images the column it reads (it is `backfilling` then, its
    /// field build waiting to start).
    /// One a schema change paused doesn't: it waits for the resume its
    /// `capture_failure` asks for. The staging worker's latest pass records it in the catalog
    /// (`capture_holdups`), so every process reports it, wherever the worker
    /// runs.
    pub capture_wait: Option<CaptureWait>,
    /// Set while capture of a table the definition reads is broken, and
    /// only fixing the cause gets the definition going again. Either:
    ///
    /// - a schema change paused it: a column it reads was
    ///   renamed or dropped, or its source's primary key was redefined.
    ///   The application's writes go on succeeding, and the other
    ///   definitions on the table keep applying. Recorded in the catalog
    ///   and cleared by resuming the definition, which rebuilds it;
    /// - or row-level security on a table it reads came to apply to the
    ///   Trellis role after it was defined: its policies would
    ///   filter every read of the table. The staging worker's capture pass
    ///   pauses it, with no `columns`. Exempt the role and resume the
    ///   definition, which rebuilds it and clears this;
    /// - or row-level security on its own target came to apply to the role
    ///   the workers write it as: its policies would filter
    ///   apply's writes. The staging worker's capture pass pauses it, with
    ///   no `columns` and the target as `source_table`. Exempt the role and
    ///   resume the definition, which rebuilds it and clears this;
    /// - or a logical-replication subscription came to replicate into a
    ///   table it reads after it was defined: capture never
    ///   sees the subscription's changes. The staging worker's capture pass
    ///   pauses it, with no `columns`. Stop replicating into the table and
    ///   resume the definition, which rebuilds it and clears this;
    /// - or, while it waits on capture as for `capture_wait`, the staging
    ///   worker's install or widen fails for a reason other than a lock (no
    ///   primary key, a statement that fails). Every pass retries it,
    ///   and it clears once one succeeds. Recorded like `capture_wait`;
    /// - or a column it keys by changed type or collation, or a column it
    ///   keeps a typed copy of widened: define would now
    ///   refuse the column, a `JOIN`'s `ON` columns no longer match,
    ///   the stored keys render differently, or a copy can't hold the new
    ///   type. The staging worker's capture pass pauses it, naming the
    ///   columns, their types and what to do. Resume it once define would
    ///   accept it (a resume refuses until then), which re-types Trellis's
    ///   copies and rebuilds it and clears this. While the staging worker
    ///   re-types the copies for a resume, it stays paused with a record
    ///   whose `error` starts `resuming:`; if a re-type fails, the record
    ///   says why;
    /// - or the drain halted on it, with
    ///   [`CaptureFailureKind::Halt`]: a failure no retry or quarantine gets
    ///   past (a key the drain can't use, a propagation wave past the hop
    ///   bound, an aggregate the group basis can't apply) reached it, so the drain paused
    ///   it, everything else reading what it reads, and everything
    ///   downstream of them, and drained the rest. Fix the cause and resume
    ///   the definition, which rebuilds it and clears this;
    /// - or its Re-derive build couldn't start, also with
    ///   [`CaptureFailureKind::Halt`]: a failure no retry gets past (its
    ///   ledger table was dropped, say), so it paused, with everything
    ///   downstream of its target. `source_table` names the target. Fix the
    ///   cause and resume it, which rebuilds it and clears this.
    ///
    /// - or Trellis's capture of a table it reads was changed outside
    ///   Trellis, with [`CaptureFailureKind::Disruption`]: a
    ///   capture trigger or function was altered, disabled (`ENABLE TRIGGER
    ///   ALL` after a bulk load leaves `ORIGIN`), recreated or replaced, or
    ///   the table is gone. Changes made meanwhile may be missing, so resume
    ///   the definition, which reinstalls capture and rebuilds it. A
    ///   definition already frozen when the disruption is seen (an
    ///   operator's `PAUSE`) gets the same record with
    ///   [`CaptureFailureKind::Capture`]: it stays the operator's to resume.
    ///
    /// The halts have [`CaptureFailureKind::Halt`], the disruptions of a
    /// definition that wasn't frozen [`CaptureFailureKind::Disruption`], and
    /// every other case [`CaptureFailureKind::Capture`]. A definition an
    /// operator paused has none until one of these is seen.
    pub capture_failure: Option<CaptureFailure>,
    /// Set while the definition holds keys in quarantine: source keys
    /// whose changes its apply leaves out, each because a change to it kept
    /// failing in this definition's apply, so their target rows stay as
    /// they were. Every other definition reading the same keys applies them
    /// as usual. A definition holds them whatever its status, `live`
    /// included, until each is released ([`Trellis::release_key`]) or the
    /// definition is resumed or dropped. [`Trellis::sample_quarantined`]
    /// pages the keys themselves, with each one's error.
    pub held_keys: Option<HeldKeys>,
    /// Set while the drain keeps failing on a page holding changes to a
    /// table the definition reads, as its source or a `JOIN`'s joined table,
    /// with nothing charged or paused: a refused read or write the
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
    /// The source indexes a `GROUP BY` or `JOIN` target needs and lacks now:
    /// one entry per table and columns. Define and a resume refuse
    /// a definition without them, so an entry here is an index dropped or
    /// made invalid since, or one the definition opted out of with
    /// `WITHOUT SOURCE INDEX` ([`SourceIndexGap::opted_out`]). A warning:
    /// nothing is paused and nothing is wrong, but `self_check` refuses to
    /// walk the target (unless it opted out, when the walk scans the source
    /// once per page). [`SourceIndexGap::fix`] is a `create index` to run;
    /// Trellis never creates one. Read live on each call. Empty for every
    /// other target, and when the indexes exist.
    pub source_index_gaps: Vec<SourceIndexGap>,
    /// What a `JOIN` transform's hidden tables look like now: their
    /// sizes, a warning when `P` approaches the fact table's size, and the step
    /// its build runs while one is under way. `None` for a
    /// definition that isn't a `JOIN`. Read live on each call; a warning
    /// only, it never changes the definition's status.
    pub join: Option<JoinStatus>,
    /// The partitions of its partitioned source the definition is being
    /// corrected for (ADR-0017): one entry per node whose attach, detach,
    /// drop or truncate (or rows routing to no current leaf) the staging
    /// worker's pass detected and queued a correction for, with when it was
    /// queued and the keys left to re-derive. The definition reports
    /// [`TransformStatus::Backfilling`] while one is listed, and once each
    /// node's keys are re-derived and their changes folded into its target
    /// it is `live` again. An entry leaves once its node's keys are
    /// re-derived. Empty when nothing is being corrected, and for a
    /// definition that is paused or quarantined: a correction that kept
    /// failing is reported in [`DefinitionStatus::backfill_failure`].
    pub correcting: Vec<CorrectingNode>,
    /// The paused columns of upstream 1-1 targets that make a `live`
    /// definition report [`TransformStatus::CatchingUp`], each
    /// `target.column` as [`Trellis::quarantine_status`] addresses it, sorted:
    /// the columns it reads, and those read by the definitions it reads
    /// (transitively). A column is paused when its fuse tripped or `PAUSE
    /// TRANSFORM t.col` froze it. A reader that is not a 1-1 definition (an
    /// aggregate, a `JOIN`) keeps applying over the column's frozen values,
    /// exact against the upstream target and stale against the source.
    /// Nothing is paused or rebuilt: resuming the column writes through to
    /// the reader, the list empties and the status returns to `live`. Empty
    /// when no such column is read.
    pub paused_upstream_columns: Vec<String>,
    /// Set while the definition's Re-derive build waits on something other
    /// than its own work: see [`BuildWait`]. Cleared once the
    /// wait is over.
    pub build_wait: Option<BuildWait>,
}

/// Why capture of a table a definition reads is broken; see [`DefinitionStatus::capture_failure`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CaptureFailure {
    /// Whether capture broke, or the drain halted on the definition.
    pub kind: CaptureFailureKind,
    /// The qualified captured table. For a halt, the table (or tables,
    /// joined with `", "`) the halting failure named, or the target, for an
    /// aggregate the group basis can't apply or a build that couldn't start.
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

/// What a [`CaptureFailure`] is: `capture_failures.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CaptureFailureKind {
    /// Capture of a table the definition reads is broken: a schema change,
    /// row-level security, a subscription, or a failing install or widen.
    /// Also a [`CaptureFailureKind::Disruption`] seen while the definition
    /// was already frozen (an operator's `PAUSE`), which stays the
    /// operator's to resume.
    Capture,
    /// The drain, or a Re-derive build's start, halted on the
    /// definition: a failure no retry or quarantine gets past reached it, so
    /// it was paused with the rest of the failure's closure
    /// (`staging::halt`).
    Halt,
    /// Trellis's capture of a table the definition reads was changed outside
    /// Trellis, or the table is gone: a capture trigger
    /// or function was altered, disabled, recreated or replaced, or the
    /// table was dropped, renamed away or moved. Bulk loaders leave this
    /// (`ENABLE TRIGGER ALL` puts the triggers in `ORIGIN` mode). The
    /// definition waits for a resume, which re-validates, reinstalls
    /// capture and rebuilds it. Also every definition of a database restored
    /// from a logical dump, whose resume first resets the ordering
    /// state the dump brought. A test helper that resumes after a fixture
    /// load selects on this kind. A definition that was already frozen when
    /// the disruption was seen gets [`CaptureFailureKind::Capture`] instead.
    Disruption,
}

impl CaptureFailureKind {
    /// Every kind, in declaration order.
    pub const ALL: [CaptureFailureKind; 3] = [
        CaptureFailureKind::Capture,
        CaptureFailureKind::Halt,
        CaptureFailureKind::Disruption,
    ];

    /// The word `capture_failures.kind` stores: `capture`, `halt` or
    /// `disruption`.
    pub fn as_str(self) -> &'static str {
        match self {
            CaptureFailureKind::Capture => "capture",
            CaptureFailureKind::Halt => "halt",
            CaptureFailureKind::Disruption => "disruption",
        }
    }

    /// The kind `capture_failures.kind` stores. Its check constraint admits
    /// only these.
    fn from_persisted(kind: &str) -> Self {
        match kind {
            "halt" => CaptureFailureKind::Halt,
            "capture" => CaptureFailureKind::Capture,
            "disruption" => CaptureFailureKind::Disruption,
            other => panic!("capture_failures.kind held unrecognized value '{other}'"),
        }
    }
}

/// What a definition's capture is waiting on: the staging
/// worker's install or widen of the capture triggers on a table the
/// definition reads couldn't take the table's lock, because another session
/// holds or is queued for a conflicting one. Nothing cancels that session
/// (an autovacuum included), so the definition waits until it lets go; this
/// names it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
/// [`DefinitionStatus::backfill_failure`]): one of its build chunks,
/// retried with capped exponential backoff. A chunk that fails on its data is
/// narrowed to the key that causes it, which is quarantined; one that fails
/// otherwise pauses the definition after a few charged attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BackfillFailure {
    /// The qualified source table the build reads.
    pub source_table: String,
    /// How many times the chunk (counting the chunk it was split from) has
    /// failed.
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
#[non_exhaustive]
pub struct DefinitionSummary {
    /// The schema-qualified target table, which identifies the transform.
    pub target_table: String,
    /// Where this transform is in its lifecycle — see
    /// [`TransformStatus`].
    pub status: TransformStatus,
    pub created_at: SystemTime,
    /// Why this definition's build keeps failing, if
    /// it does: the same value [`DefinitionStatus::backfill_failure`] reports.
    pub backfill_failure: Option<BackfillFailure>,
    /// Set while the drain has halted on this definition: the
    /// [`DefinitionStatus::capture_failure`] [`Trellis::status`] reports for
    /// it, always with [`CaptureFailureKind::Halt`]. The definition is
    /// `paused`. Listing every definition is the health read for halts: any
    /// with this set is stopped until the cause is fixed and it is resumed.
    pub halt: Option<CaptureFailure>,
}

/// What [`Trellis::apply`] did — the one return type every
/// definition-changing statement shares.
///
/// One variant per operation, carrying only what that operation has to report
/// that the caller couldn't already know from the statement it wrote: the
/// registered transform's target table for the defining form, the resumed
/// `(transform, column)` pairs for a column resume, and nothing at all for a
/// pause or a drop.
///
/// `#[non_exhaustive]`, for the same reason [`ErrorCode`] is: this is a type
/// the caller is *meant* to match on from outside — eventually from outside
/// Rust — while the set of operations behind `apply` keeps growing (`AMEND`
/// next, once it has its own ADR). A match on it has
/// to tolerate a variant it doesn't know rather than assume the set is closed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Applied {
    /// A `TRANSFORM ...` statement registered the transform writing this
    /// schema-qualified target table, at [`TransformStatus::WaitingToBackfill`]:
    /// its backfill runs in the background — see [`Trellis::apply`].
    TransformDefined(String),
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
    /// fields (ADR-0015). `target_table` is the edited
    /// transform's schema-qualified target table; `added`/`dropped`/`altered` name exactly the
    /// fields this call actually changed, excluding any clause that turned
    /// out to be an idempotent no-op (re-adding an existing field with the
    /// same formula, re-altering to the formula it already had, dropping an
    /// already-absent field) — all three lists are empty for a statement
    /// whose every clause was one of those.
    Altered {
        target_table: String,
        added: Vec<String>,
        dropped: Vec<String>,
        altered: Vec<String>,
    },
}

impl Applied {
    /// The target table a `TRANSFORM ...` statement registered, or `None`
    /// for any other outcome.
    ///
    /// For the common case where the caller wrote the statement itself and so
    /// already knows which form it was: `apply(...)?.into_transform().expect(...)`
    /// reads better than a `match` with an unreachable arm — and much better
    /// than the `let ... else` that an `#[non_exhaustive]` enum otherwise
    /// forces on every such call site.
    pub fn into_transform(self) -> Option<String> {
        match self {
            Applied::TransformDefined(target_table) => Some(target_table),
            _ => None,
        }
    }
}

/// One poison-quarantine entry, as [`Trellis::poisoned_since`] reports it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PoisonEntry {
    /// The bare target of the definition the key is held for.
    pub transform: String,
    pub src_table: String,
    pub key: String,
    pub last_error: String,
    pub poisoned_at: SystemTime,
}

/// A quarantine/status address (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// "The client API is three reads, separated by cost, and one release"): either a whole transform (its whole keyspace,
/// from the transform-wide lifecycle/fuse) or one of its columns (from the
/// column-status table). Reuses the `table.column` shape the grammar already
/// has elsewhere for qualified column references, rather than inventing a
/// new addressing idiom.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
#[non_exhaustive]
pub enum QuarantineState {
    Live,
    WaitingToBackfill,
    Backfilling,
    /// Applied like a live transform, but reading an upstream that isn't
    /// `live` (mirrors [`TransformStatus::CatchingUp`]).
    CatchingUp,
    /// A whole transform's keyspace fuse has tripped (mirrors
    /// [`TransformStatus::Quarantined`]).
    Quarantined,
    /// Frozen without a whole-keyspace fuse having tripped.
    ///
    /// At **column** granularity: one column's own fuse has tripped, or it's
    /// paused only because an upstream column it reads is (the
    /// cascade) — both look the same from this read; see
    /// [`Trellis::quarantine_status`] for whether a caller needs to
    /// distinguish them (today, [`QuarantineEntry::last_error`] is `None` for
    /// a purely cascaded pause, since it never itself failed).
    ///
    /// At **whole-transform** granularity: an operator
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct PoisonSample {
    pub src_table: String,
    pub key: String,
    pub error_message: String,
    /// How many times the automatic retry ([`TrellisOptions::held_key_retry`])
    /// has released this key since it last applied cleanly. 0 for a column
    /// target's rows and where none has.
    pub retry_attempts: u32,
    /// When the key's next automatic retry is due: `None` for a column
    /// target's rows and where this connection has no
    /// [`TrellisOptions::held_key_retry`].
    pub next_retry_at: Option<SystemTime>,
}

/// Why a [`Trellis`] operation failed. Composes the crate's lower-level error
/// types via `From`, matching the hand-rolled-enum convention the rest of the
/// crate uses. [`TrellisError::code`] reports a stable, coarse [`ErrorCode`]
/// category for this error alongside its `Display` message — see
/// `docs/decisions/0008-public-api-design.md`, decision 3.
#[derive(Debug)]
#[non_exhaustive]
pub enum TrellisError {
    /// A definition failed to parse before it could be registered.
    Parse(ParseError),
    /// A catalog operation (define/alter/install/pause/drop) failed.
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
    /// registered definition reads, so the staging worker doesn't capture it.
    TableNotCaptured { table: String },
    /// [`Trellis::request_backfill`] was given a bare table name that more than
    /// one captured source table carries, one per entry of `tables`
    /// (schema-qualified). The caller qualifies the name to say which it means.
    AmbiguousSourceTable { table: String, tables: Vec<String> },
    /// [`crate::blocking::BlockingTrellis::connect`]'s background thread
    /// failed to spawn.
    BlockingSpawn(std::io::Error),
    /// [`crate::blocking::BlockingTrellis::connect`]'s background thread
    /// exited (panicked, or its setup future dropped its ready-sender)
    /// before ever signalling ready.
    BlockingThreadExitedBeforeReady,
    /// A [`crate::blocking::BlockingTrellis`] call got no reply: the
    /// background thread was no longer there to service it (it panicked after
    /// connecting successfully), or the call's own task panicked — the job
    /// channel send or reply recv failed.
    BlockingThreadGone,
    /// A [`crate::blocking::BlockingTrellis`] call was still in flight when
    /// the handle was shut down, and the shutdown cancelled it rather than
    /// wait for it. What the call had started on the server ends at the
    /// call's deadline, where the server stops it.
    CancelledByShutdown,
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
    /// isn't one of `transform`'s declared calculated fields —
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
    /// The call did not finish within its deadline (issue #599): 30 seconds
    /// from when it was submitted, queueing and the wait for a pooled
    /// connection included. Whatever the call had running on the server was
    /// stopped by the server's own `statement_timeout`, and its transaction
    /// rolled back, so the call changed nothing it did not commit before the
    /// deadline: a multi-transaction call such as [`Trellis::migrate`] keeps
    /// each migration that committed. Expected and retryable, like any other
    /// [`ErrorCode::Timeout`]: look at what the call waits on (a lock a long
    /// application transaction holds, a database that is behind) and call
    /// again.
    CallTimeout {
        /// The deadline the call had.
        budget: Duration,
        /// `false` when the call heard back: the server's `statement_timeout`
        /// (or a lock wait, or the wait for a pooled connection) ended it,
        /// and what it had running was stopped and rolled back. `true` when
        /// nothing had answered a second past the deadline
        /// (`deadline::BACKSTOP_GRACE`) and the call stopped waiting: a
        /// connection that stopped delivering, or a host too loaded to read
        /// the server's answer in time. What it left running is still
        /// bounded by the same `statement_timeout` and rolls back when that
        /// fires, but the call did not see it happen.
        abandoned: bool,
    },
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
            TrellisError::TableNotCaptured { .. } | TrellisError::AmbiguousSourceTable { .. } => {
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
            // A call racing its handle's shutdown: the same category the
            // bindings give a call made after it.
            TrellisError::CancelledByShutdown => ErrorCode::Validation,
            TrellisError::Apply(err) => err.code(),
            TrellisError::TransformNotFound(_) | TrellisError::ColumnNotFound { .. } => {
                ErrorCode::NotFound
            }
            TrellisError::Staging(err) => err.code(),
            TrellisError::SelfCheck(err) => err.code(),
            TrellisError::CallTimeout { .. } => ErrorCode::Timeout,
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
            TrellisError::AmbiguousSourceTable { table, tables } => write!(
                f,
                "source table \"{table}\" is ambiguous: these captured tables share the name: \
                 {}; qualify it as <schema>.{table}",
                tables.join(", ")
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
            TrellisError::CancelledByShutdown => write!(
                f,
                "this call was cancelled because its Trellis handle was shut down while it ran"
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
            TrellisError::CallTimeout {
                budget,
                abandoned: false,
            } => write!(
                f,
                "the call did not finish within its {}s deadline; the work it started on the \
                 server was stopped and rolled back",
                budget.as_secs_f64()
            ),
            TrellisError::CallTimeout {
                budget,
                abandoned: true,
            } => write!(
                f,
                "the call did not finish within its {}s deadline and the database had not \
                 answered a second later; the server's statement timeout stops and rolls back \
                 the work it started",
                budget.as_secs_f64()
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
            TrellisError::SourceTableNotFound(_)
            | TrellisError::TableNotCaptured { .. }
            | TrellisError::AmbiguousSourceTable { .. }
            | TrellisError::BlockingThreadExitedBeforeReady
            | TrellisError::BlockingThreadGone
            | TrellisError::CancelledByShutdown
            | TrellisError::CalledFromAsyncContext
            | TrellisError::CallTimeout { .. } => None,
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
mod client_options_tests {
    use super::*;

    /// A connection's `worker_threads` reaches the background
    /// client's runtime. It used to stop at the blocking wrapper, so the
    /// client thread built its own per-core runtime whatever the caller set.
    #[test]
    fn worker_threads_reaches_the_client_options() {
        let options = TrellisOptions {
            staging: true,
            drain_threads: 3,
            worker_threads: Some(2),
            ..Default::default()
        };
        let client = Trellis::client_options(&options);
        assert_eq!(client.worker_threads, Some(2));
        assert!(client.staging_worker);
        assert_eq!(client.application_threads, 3);
        assert_eq!(
            Trellis::client_options(&TrellisOptions::default()).worker_threads,
            None
        );
    }

    /// The cap reaches the runtime the client thread actually
    /// builds, not just the options struct. A zero cap fails that build
    /// before the client opens any connection, so this needs no database;
    /// a client thread that built its runtime any other way would get past
    /// it and fail on the unreachable DSN instead.
    #[tokio::test]
    async fn connect_builds_the_client_runtime_with_worker_threads() {
        let config =
            Config::with_schema("postgres://trellis@127.0.0.1:1/none", "trellis").expect("config");
        let options = TrellisOptions {
            staging: true,
            drain_threads: 0,
            worker_threads: Some(0),
            ..Default::default()
        };
        match Trellis::connect(config, options).await {
            Err(TrellisError::Client(ClientError::Spawn(err))) => {
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            }
            Err(err) => panic!("expected the client runtime build to fail, got: {err}"),
            Ok(_) => panic!("a zero worker_threads cap must not start a client"),
        }
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

    #[test]
    fn backfill_names_resolve_exactly_or_unambiguously() {
        let captured = vec!["shop.orders".to_string(), "public.items".to_string()];
        let resolve = |n| resolve_backfill_table(&captured, n);
        assert_eq!(
            resolve("shop.orders").unwrap().as_deref(),
            Some("shop.orders")
        );
        assert_eq!(resolve("orders").unwrap().as_deref(), Some("shop.orders"));
        assert_eq!(resolve("public.orders").unwrap(), None);
        assert_eq!(resolve("nope").unwrap(), None);

        let both = vec!["shop.orders".to_string(), "blog.orders".to_string()];
        let err = resolve_backfill_table(&both, "orders").unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation);
        let msg = err.to_string();
        assert!(
            msg.contains("blog.orders") && msg.contains("shop.orders"),
            "{msg}"
        );
        assert_eq!(
            resolve_backfill_table(&both, "blog.orders")
                .unwrap()
                .as_deref(),
            Some("blog.orders")
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
    /// [`TrellisError::Staging`]: it must delegate to
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
    /// `internal` to "a Trellis bug".
    #[test]
    fn a_convergence_timeout_surfaces_as_timeout_through_the_facade() {
        let err = TrellisError::Staging(StagingError::ConvergenceTimeout {
            token: PgLsn::from(0),
            waited: Duration::from_secs(1),
        });

        assert_eq!(err.code(), ErrorCode::Timeout);
    }
}
