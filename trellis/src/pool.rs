//! Connection pool management, built on `deadpool-postgres`.
//!
//! [`Pool::new`] wires up a `deadpool_postgres::Pool` from a resolved
//! [`Config`] and attaches a per-connection session bootstrap hook (via
//! deadpool's `post_create` hook). That hook pins `search_path` to the
//! configured Trellis schema followed by the configured target schema (see
//! `Config::target_schema`), so migrations and staging objects land in the
//! Trellis schema and transform target tables resolve without every query
//! needing to qualify table names, while [`crate::defs::ddl`] still
//! schema-qualifies target-table DDL explicitly (`search_path` only decides
//! where an *unqualified* `CREATE TABLE` lands, and that must be the
//! Trellis schema, not the target schema, for the rest of this module's
//! unqualified references to Trellis's own objects to keep resolving
//! correctly).
//!
//! Beyond `search_path`, the hook also pins the output GUCs that make a
//! value's text rendering depend on the value alone rather than on the
//! session reading it (see [`DETERMINISTIC_TEXT_OUTPUT_GUCS`]), turns
//! `row_security` off so a read row-level security would filter raises
//! instead (see [`ROW_SECURITY_OFF`]), turns JIT compilation
//! off (see [`JIT_OFF`]), starts every transaction at
//! `READ COMMITTED` (see [`READ_COMMITTED`]), and turns on
//! TCP keepalives at both ends so a partitioned connection's locks don't
//! outlive it by hours (see [`tcp_keepalive_gucs`]). The
//! dedicated, non-pooled connections get the same through
//! [`connect_dedicated`] and [`dedicated_session_setup`].

use crate::config::{
    Config, DEFAULT_CALL_POOL_MAX_SIZE, DEFAULT_CALL_POOL_WAIT_TIMEOUT, POOL_IDLE_TIMEOUT,
};
use crate::error::Error;
use deadpool::managed::QueueMode;
use deadpool_postgres::{
    Hook, HookError, Manager, ManagerConfig, Pool as DeadpoolPool, RecyclingMethod, Runtime,
};
use std::sync::Arc;
use std::time::Duration;

/// Closes `pool`'s idle connections unused for `idle` or longer.
fn close_idle(pool: &DeadpoolPool, idle: Duration) {
    pool.retain(|_, metrics| metrics.last_used() < idle);
}

/// The size of the background pool for a client's roles, or `None` when it
/// has neither: a pool that never waits on its own tasks.
///
/// A drain thread holds at most 2 pooled connections at once
/// ([`MAX_BACKGROUND_HELD`]; a column-fuse trip releases its connection
/// first), and so does `self_check` (1 when it runs a single query). On top of
/// those:
///
/// - a worker (`staging` with `N` drain threads): `2N` for the drain threads,
///   plus `self_check` 2, the upkeep task 1, the staging maintenance loop 1 and
///   the registration/startup task 1: `2N + 5`;
/// - drain only: the same without maintenance, `2N + 4`;
/// - staging only: the maintenance loop's one pooled connection, `1`.
pub(crate) fn background_pool_size(staging: bool, drain_threads: usize) -> Option<usize> {
    match (staging, drain_threads) {
        (false, 0) => None,
        (true, 0) => Some(1),
        (true, n) => Some(2 * n + 5),
        (false, n) => Some(2 * n + 4),
    }
}

/// A pooled connection, handed out by [`Pool::get`].
///
/// Derefs to the pooled connection exactly as `deadpool_postgres::Client`
/// does, and behaves the same, with one difference for a checkout made inside
/// a public call (issue #599, `crate::deadline`): it was handed out with a
/// session `statement_timeout` of the call's remaining budget, and
/// [`PooledClient::transaction`]/[`PooledClient::build_transaction`] set the budget then
/// remaining for each transaction. When it drops, the setting is reset
/// *before* the connection goes back to the pool, on a task of its own, so a
/// later borrower (a background worker) never inherits a call's timeout. That
/// reset queues behind whatever the connection is still doing, so a statement
/// an abandoned call left running keeps its connection out of the pool until
/// the server has finished with it.
pub struct PooledClient {
    inner: Option<deadpool_postgres::Client>,
    /// `Some` when this checkout ran under a call's deadline: how long to
    /// wait for the connection to settle before giving up on it.
    settle: Option<Duration>,
    /// Counts this checkout against its task's background-connection depth
    /// ([`depth`]); debug builds only.
    #[cfg(debug_assertions)]
    _depth: Option<depth::Held>,
}

impl PooledClient {
    fn plain(inner: deadpool_postgres::Client) -> Self {
        Self {
            inner: Some(inner),
            settle: None,
            #[cfg(debug_assertions)]
            _depth: None,
        }
    }

    fn wrapper_mut(&mut self) -> &mut deadpool_postgres::ClientWrapper {
        self.inner.as_mut().expect("present until dropped")
    }

    /// `deadpool_postgres::ClientWrapper::transaction`, bounded by the
    /// running call's remaining budget if there is one.
    pub async fn transaction(
        &mut self,
    ) -> Result<deadpool_postgres::Transaction<'_>, tokio_postgres::Error> {
        let txn = self.wrapper_mut().transaction().await?;
        bound_transaction(&txn).await?;
        Ok(txn)
    }

    /// `deadpool_postgres::ClientWrapper::build_transaction`; its
    /// [`TransactionBuilder::start`] bounds the transaction like
    /// [`PooledClient::transaction`].
    pub fn build_transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder {
            inner: self.wrapper_mut().build_transaction(),
        }
    }
}

/// The most background-pool connections one task may hold at once: the
/// background pools are sized on it (`2N + ...`, see [`Pool::background`]).
/// A debug build aborts the process when a task holds more.
pub const MAX_BACKGROUND_HELD: usize = 2;

/// Debug-build accounting of how many background-pool connections each task
/// holds. A task that wants its checkouts counted runs inside
/// [`depth::scope`]; checkouts outside any scope (a test's own pool use, a
/// momentary `ChunkHeartbeat` task) are not counted.
///
/// A violation aborts the process rather than panicking: the counted tasks
/// are the background client's, whose panics end only the task (`client::run`
/// joins them and moves on), so a panic would pass a test unnoticed. An abort
/// fails the whole test binary, and a binding's test run with it.
mod depth {
    #[cfg(debug_assertions)]
    use std::sync::Arc;
    #[cfg(debug_assertions)]
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One scope's count of held checkouts.
    #[cfg(debug_assertions)]
    pub(super) struct Count {
        held: AtomicUsize,
        /// Panic on a violation instead of aborting: only this module's own
        /// test, which checks that a violation is caught.
        panic: bool,
    }

    #[cfg(debug_assertions)]
    tokio::task_local! {
        static HELD: Arc<Count>;
    }

    /// One counted checkout, released on drop.
    #[cfg(debug_assertions)]
    pub(super) struct Held(Arc<Count>);

    #[cfg(debug_assertions)]
    impl Drop for Held {
        fn drop(&mut self) {
            self.0.held.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Counts one more checkout against the running task, if it is scoped.
    /// Aborts the process (debug builds) when that is more than
    /// [`super::MAX_BACKGROUND_HELD`].
    #[cfg(debug_assertions)]
    pub(super) fn enter() -> Option<Held> {
        HELD.try_with(|count| {
            let guard = Held(count.clone());
            let now = count.held.fetch_add(1, Ordering::SeqCst) + 1;
            if now > super::MAX_BACKGROUND_HELD {
                let message = format!(
                    "a task holds {now} background-pool connections at once (the pools are \
                     sized for at most {}): release one before taking the next",
                    super::MAX_BACKGROUND_HELD
                );
                if count.panic {
                    panic!("{message}");
                }
                eprintln!("{message}\n{}", std::backtrace::Backtrace::force_capture());
                std::process::abort();
            }
            guard
        })
        .ok()
    }

    /// Runs `fut` with its own background-connection count.
    pub(crate) async fn scope<F: std::future::Future>(fut: F) -> F::Output {
        #[cfg(debug_assertions)]
        {
            let count = Count {
                held: AtomicUsize::new(0),
                panic: false,
            };
            HELD.scope(Arc::new(count), fut).await
        }
        #[cfg(not(debug_assertions))]
        {
            fut.await
        }
    }

    /// [`scope`], but a violation panics the task instead of aborting.
    #[cfg(all(test, debug_assertions))]
    pub(super) async fn panicking_scope<F: std::future::Future>(fut: F) -> F::Output {
        let count = Count {
            held: AtomicUsize::new(0),
            panic: true,
        };
        HELD.scope(Arc::new(count), fut).await
    }
}

pub(crate) use depth::scope as count_background_connections;

/// `SET LOCAL statement_timeout` for the running call's remaining budget, as
/// a transaction's first statement. Nothing outside a call.
async fn bound_transaction(
    txn: &deadpool_postgres::Transaction<'_>,
) -> Result<(), tokio_postgres::Error> {
    if let Some(deadline) = crate::deadline::current() {
        txn.simple_query(&deadline.set_statement_timeout_sql(true))
            .await?;
    }
    Ok(())
}

/// [`PooledClient::build_transaction`]'s builder: `deadpool_postgres`'s, whose
/// `start` also bounds the transaction by the running call's budget.
pub struct TransactionBuilder<'a> {
    inner: deadpool_postgres::TransactionBuilder<'a>,
}

impl<'a> TransactionBuilder<'a> {
    /// The transaction's isolation level.
    pub fn isolation_level(mut self, isolation_level: tokio_postgres::IsolationLevel) -> Self {
        self.inner = self.inner.isolation_level(isolation_level);
        self
    }

    /// Whether the transaction is read-only.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.inner = self.inner.read_only(read_only);
        self
    }

    /// Whether the transaction is deferrable.
    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.inner = self.inner.deferrable(deferrable);
        self
    }

    /// Begins the transaction.
    pub async fn start(self) -> Result<deadpool_postgres::Transaction<'a>, tokio_postgres::Error> {
        let txn = self.inner.start().await?;
        bound_transaction(&txn).await?;
        Ok(txn)
    }
}

impl std::fmt::Debug for PooledClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledClient").finish_non_exhaustive()
    }
}

impl std::ops::Deref for PooledClient {
    type Target = deadpool_postgres::ClientWrapper;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("present until dropped")
    }
}

impl std::ops::DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.wrapper_mut()
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        let Some(settle) = self.settle else {
            return;
        };
        let Some(inner) = self.inner.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(reset_session(Unreset(Some(inner)), settle));
            }
            // Nothing to reset it on: close it rather than pool it still
            // carrying a call's timeout.
            Err(_) => drop(deadpool_postgres::Object::take(inner)),
        }
    }
}

/// How long past its settle time a released call connection gets to answer
/// the reset before it is closed instead of pooled.
const RESET_GRACE: Duration = Duration::from_secs(5);

/// Clears the call's `statement_timeout` and returns the connection to the
/// pool, or closes it if it won't answer. The connection is closed too if
/// this task is dropped before the reset lands, even before its first poll
/// (its runtime shutting down): only a connection the reset reached goes
/// back to the pool.
async fn reset_session(mut unreset: Unreset, settle: Duration) {
    let reset = tokio::time::timeout(
        settle + RESET_GRACE,
        unreset
            .0
            .as_ref()
            .expect("held until reset")
            .simple_query("reset statement_timeout"),
    )
    .await;
    if let Ok(Ok(_)) = reset {
        drop(unreset.0.take());
    }
}

/// A released call connection that still carries the call's
/// `statement_timeout`: closed when dropped, unless [`reset_session`] took
/// it back out once its reset landed.
struct Unreset(Option<deadpool_postgres::Client>);

impl Drop for Unreset {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}

/// Trellis's connection pool.
///
/// Connections use TLS as the DSN's `sslmode` says (default `prefer`); see
/// `crate::tls` and the TLS section of `docs/embedding.md`. The connector
/// is built once, in [`Pool::new`], so a rotated client certificate or CA
/// file takes a new `Pool`.
///
/// `Clone` is cheap: `deadpool_postgres::Pool` is itself `Arc`-backed, so a
/// clone shares the same underlying pool of physical connections rather than
/// opening a second one. [`crate::Client`]'s app-worker tasks each get their
/// own clone rather than a `&Pool` reference, since every worker runs in its
/// own spawned task with its own lifetime.
#[derive(Debug, Clone)]
pub struct Pool {
    inner: DeadpoolPool,
    /// The connection settings the pool's connections are opened with,
    /// client keepalives included, kept so [`Pool::connect_unpooled`] can
    /// open one more the same way. `tokio_postgres::Config`'s `Debug`
    /// redacts the password.
    pg_config: tokio_postgres::Config,
    /// The TLS connector the pool's connections use, kept for the same
    /// reason as `pg_config`.
    tls: crate::tls::Tls,
    /// A copy of the [`Config::schema`] this pool was built from — see
    /// [`Pool::schema`].
    schema: String,
    /// A copy of the [`Config::target_schema`] this pool was built from —
    /// see [`Pool::target_schema`]. [`Pool::connect_unpooled`] bootstraps
    /// its connection's `search_path` with it.
    target_schema: String,
    /// The name this pool's instance logs under ([`crate::instance_log`]),
    /// for the drivers [`Pool::connect_unpooled`] spawns.
    instance: std::sync::Arc<str>,
    /// How long a checkout waits for a free connection, which a call's
    /// remaining budget can shorten ([`Pool::get`]).
    wait_timeout: Duration,
    /// Whether this is a background pool ([`Pool::background`]), whose
    /// checkouts are counted per task in debug builds.
    background: bool,
    /// Stops the idle sweep when the last clone of the pool goes.
    _sweep: Arc<SweepTask>,
}

/// The pool's idle sweep; aborted when the pool's last clone drops.
#[derive(Debug)]
struct SweepTask(Option<tokio::task::JoinHandle<()>>);

impl Drop for SweepTask {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl Pool {
    /// Builds a call pool from `config`: `DEFAULT_CALL_POOL_MAX_SIZE`
    /// connections, a `DEFAULT_CALL_POOL_WAIT_TIMEOUT` wait. See
    /// [`Pool::with_limits`].
    pub fn new(config: &Config) -> Result<Self, Error> {
        Self::with_limits(
            config,
            DEFAULT_CALL_POOL_MAX_SIZE,
            DEFAULT_CALL_POOL_WAIT_TIMEOUT,
        )
    }

    /// Builds a pool of at most `max_size` connections, each checkout waiting
    /// up to `wait_timeout` for a free one. Fails only if `config.dsn` can't
    /// be parsed as a Postgres connection string, `max_size` is `0` (a pool
    /// that can never open a connection is broken, not smaller), or
    /// deadpool's builder rejects the resulting configuration; no network I/O
    /// happens here: the pool connects lazily on first `get()`.
    ///
    /// Sized and timed out explicitly rather than by deadpool's defaults
    /// (`2 * num_cpus` connections, no wait timeout at all): with no timeout a
    /// checkout on an exhausted pool would wait forever, with no error and no
    /// log.
    ///
    /// A connection unused for `POOL_IDLE_TIMEOUT` is closed: the pool hands
    /// out its most recently used connection first, and, when built inside a
    /// tokio runtime, a task sweeps the rest.
    pub fn with_limits(
        config: &Config,
        max_size: usize,
        wait_timeout: Duration,
    ) -> Result<Self, Error> {
        Self::build(config, max_size, wait_timeout, false)
    }

    /// Builds the pool a handle's background client runs on, of exactly
    /// `max_size` connections (see [`background_pool_size`]). Its checkouts
    /// wait `DEFAULT_CALL_POOL_WAIT_TIMEOUT`, and in debug builds a task
    /// that holds more than [`MAX_BACKGROUND_HELD`] of them at once aborts the
    /// process.
    pub(crate) fn background(config: &Config, max_size: usize) -> Result<Self, Error> {
        Self::build(config, max_size, DEFAULT_CALL_POOL_WAIT_TIMEOUT, true)
    }

    fn build(
        config: &Config,
        max_size: usize,
        wait_timeout: Duration,
        background: bool,
    ) -> Result<Self, Error> {
        if max_size == 0 {
            return Err(Error::Config(
                "a pool must allow at least 1 connection (max_call_pool_size)".to_string(),
            ));
        }
        let (mut pg_config, tls_spec) = crate::config::parse_dsn_tls(config.dsn())?;
        let tls = tls_spec.connector()?;
        with_client_keepalives(&mut pg_config, DeadPeerDetection::KeepalivesOnly);

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config.clone(), tls.clone(), manager_config);

        let schema = config.schema().to_string();
        let target_schema = config.target_schema().to_string();
        let inner = DeadpoolPool::builder(manager)
            .post_create(Hook::async_fn(move |client, _metrics| {
                let schema = schema.clone();
                let target_schema = target_schema.clone();
                Box::pin(async move {
                    session_bootstrap(client, &schema, &target_schema)
                        .await
                        .map_err(HookError::Backend)
                })
            }))
            .max_size(max_size)
            // Reuse the hottest connection, so a burst's extra connections go
            // unused and the idle sweep can close them (first-in-first-out
            // would rotate through all of them and none would look idle).
            .queue_mode(QueueMode::Lifo)
            // A `Runtime` is required for `wait_timeout` to take effect at
            // all — without one, `deadpool`'s `PoolBuilder::build` rejects
            // the config outright (`BuildError::NoRuntimeSpecified`) rather
            // than silently ignoring the timeout. `Runtime::Tokio1` matches
            // deadpool-postgres's own default feature (`rt_tokio_1`) and
            // every other async runtime this crate already assumes
            // (tokio_postgres::Client, the post_create hook above).
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(wait_timeout))
            .build()?;

        let sweep = tokio::runtime::Handle::try_current().ok().map(|runtime| {
            let pool = inner.clone();
            runtime.spawn(async move {
                let mut tick = tokio::time::interval(POOL_IDLE_TIMEOUT / 2);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    close_idle(&pool, POOL_IDLE_TIMEOUT);
                }
            })
        });

        Ok(Self {
            inner,
            pg_config,
            tls,
            schema: config.schema().to_string(),
            target_schema: config.target_schema().to_string(),
            instance: crate::instance_log::name_of(config),
            wait_timeout,
            background,
            _sweep: Arc::new(SweepTask(sweep)),
        })
    }

    /// Closes every idle connection unused for `idle` or longer. The pool's
    /// own sweep calls this every half `POOL_IDLE_TIMEOUT`; tests call it
    /// directly. A connection's last-use time is stamped at checkout, so one
    /// held for a long transaction counts as idle the moment it comes back,
    /// and may be closed by the next sweep (one reconnect).
    pub fn close_idle(&self, idle: Duration) {
        close_idle(&self.inner, idle);
    }

    /// The pool's `(size, available)` connection counts: connections open,
    /// and how many of them are idle.
    pub fn counts(&self) -> (usize, usize) {
        let status = self.inner.status();
        (status.size, status.available)
    }

    /// The most connections this pool opens at once.
    pub fn max_size(&self) -> usize {
        self.inner.status().max_size
    }

    /// How long a checkout waits for a free connection before failing (a
    /// call's remaining budget can cut one checkout's wait shorter).
    pub fn wait_timeout(&self) -> Duration {
        self.wait_timeout
    }

    /// Acquires a connection, waiting up to the pool's wait timeout
    /// (see [`Pool::with_limits`]) for one to become available if the pool is at
    /// capacity. Once that timeout elapses, returns
    /// `Err(Error::Pool(deadpool_postgres::PoolError::Timeout(_)))` — a
    /// clear, typed, loggable failure (categorized [`crate::ErrorCode::Connectivity`]
    /// via [`Error::code`]) instead of hanging indefinitely.
    ///
    /// Inside a public call (issue #599, `crate::deadline`) it waits no
    /// longer than the call has left, and hands the connection out with its
    /// `statement_timeout` set to that, so the server abandons a statement
    /// the call can no longer wait for.
    pub async fn get(&self) -> Result<PooledClient, Error> {
        let Some(deadline) = crate::deadline::current() else {
            return Ok(self.counted(PooledClient::plain(self.inner.get().await?)));
        };
        let remaining = deadline.remaining();
        if remaining.is_zero() {
            return Err(Error::Pool(deadpool_postgres::PoolError::Timeout(
                deadpool_postgres::TimeoutType::Wait,
            )));
        }
        let timeouts = deadpool_postgres::Timeouts {
            wait: Some(self.wait_timeout.min(remaining)),
            create: Some(remaining),
            recycle: Some(remaining),
        };
        let mut client = self.counted(PooledClient::plain(
            self.inner.timeout_get(&timeouts).await?,
        ));
        client.settle = Some(deadline.settle_time());
        client
            .simple_query(&deadline.set_statement_timeout_sql(false))
            .await?;
        Ok(client)
    }

    /// Counts a background checkout against its task (debug builds).
    #[cfg_attr(not(debug_assertions), allow(unused_mut))]
    fn counted(&self, mut client: PooledClient) -> PooledClient {
        #[cfg(debug_assertions)]
        if self.background {
            client._depth = depth::enter();
        }
        client
    }

    /// Opens one connection outside the pool, set up exactly like a pooled
    /// one ([`session_bootstrap`]), and spawns its driver. It closes when the
    /// returned client is dropped, and it never counts against
    /// the pool's size or waits on it.
    ///
    /// For session state that must never reach a pooled connection: a paged
    /// drain's `TEMP` table ([`crate::staging::page`]) lives on
    /// one of these, so however the drain ends (finished, failed, or its
    /// future dropped mid-page) the table goes with the session, and no later
    /// borrower of a pooled connection can inherit it.
    pub(crate) async fn connect_unpooled(&self) -> Result<tokio_postgres::Client, Error> {
        let (mut client, connection) = self.pg_config.connect(self.tls.clone()).await?;
        // The driver may run on a thread of the host's runtime, so it names
        // its own instance.
        tokio::spawn(crate::instance_log::scoped(
            self.instance.clone(),
            async move {
                if let Err(err) = connection.await {
                    crate::instance_log::debug!(
                        error = %err,
                        "unpooled connection ended with an error"
                    );
                }
            },
        ));
        session_bootstrap(&mut client, &self.schema, &self.target_schema).await?;
        // Closed when dropped, so nothing to reset: a call's deadline simply
        // ends with the connection.
        if let Some(deadline) = crate::deadline::current() {
            client
                .simple_query(&deadline.set_statement_timeout_sql(false))
                .await?;
        }
        Ok(client)
    }

    /// The [`Config::schema`] this pool was built with: the instance's own
    /// catalog schema, first on every connection's `search_path`. Trellis's
    /// internal tables that DDL creates at runtime rather than a migration
    /// (an aggregate's or a `JOIN`'s hidden tables) are qualified with it
    /// explicitly.
    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    /// The [`Config::target_schema`] this pool was built with (ADR-0011) — the same
    /// value baked into every connection's own `search_path` by [`session_bootstrap`]
    /// above, and the same value every real caller of
    /// [`crate::defs::install_definition`] already passes as its own `target_schema`
    /// argument (that argument and this one are never meant to diverge;
    /// `install_definition` still takes its own explicit parameter rather than reading
    /// this one, so its target-table DDL and
    /// [`crate::defs::catalog::create_definition_inner`]'s persisted qualification are
    /// threaded from one guaranteed-identical value instead of two that merely happen
    /// to agree). Exposed so [`crate::defs::catalog::create_definition`]/
    /// [`crate::defs::catalog::create_definition_without_backfill`] — the test-fixture
    /// entry points, which take no `target_schema` parameter of their own — can qualify
    /// a definition's target table the same way, without widening their public
    /// signature. Gated like its only callers.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn target_schema(&self) -> &str {
        &self.target_schema
    }
}

/// The GUCs that make a value's **text rendering** a function of the value
/// alone rather than of the session it's read on.
///
/// On the **output** side every one of these is already what a stock server
/// produces, so this changes nothing an unconfigured Postgres does — it makes
/// a guarantee Trellis was previously *assuming* into one it actually
/// enforces. A server, database or role with `ALTER ... SET datestyle`
/// applied (or `PGDATESTYLE` in the environment) silently breaks that
/// assumption today.
///
/// One caveat, because the claim above is not quite "these are the boot
/// values": `DateStyle`'s boot value is `ISO, MDY`, and the second field is
/// an *input* field-order preference that has no effect on output at all.
/// Pinning `YMD` rather than `MDY` therefore does change one thing on a
/// stock server — how an *ambiguous* all-numeric date literal typed into a
/// session is read (`'01/02/2024'::date` is 2024-01-02 under `MDY` and an
/// out-of-range error under `YMD`). That is deliberate and safe here: every
/// date text Trellis itself puts into SQL is canonical leading-4-digit ISO
/// (see the input note below), which parses identically under either field
/// order, and application SQL runs on the application's own connections,
/// not on these. `YMD` is chosen so that an ambiguous spelling *fails loudly*
/// on an engine connection instead of being silently reinterpreted.
///
/// Why Trellis needs it, concretely: a computed value travels through the
/// engine as **text** ([`crate::defs::eval::Value`]), and correctness is
/// established by comparing two independently-authored renderings of it —
/// the Rust evaluator's and Postgres's own (ADR-0013's continuous
/// cross-check; `docs/generative-test-suite.md`). For a
/// [`crate::defs::ValueType::Other`] value that comparison is **byte-exact**
/// (the generative suite's `Comparison::Exact`), so any GUC that changes how
/// Postgres spells a value turns a perfectly converged target into a
/// reported divergence. Under `DateStyle = 'SQL, MDY'`, for instance,
/// `('2024-01-01'::date)::text` renders `01/01/2024`, and nothing is
/// actually wrong.
///
/// The first four pinned here cover what typed literals can
/// produce (`date`/`timestamp` under `DateStyle`, `bytea` under
/// `bytea_output`), what the float split added (`real`/`double
/// precision` under `extra_float_digits`), and what the temporal
/// families added (`interval` under `IntervalStyle`). **Every future type
/// family hits this same wall and should extend this one
/// constant** rather than pinning a GUC at its own call site. Keeping them
/// in a single list is also what lets the non-pooled connect sites below,
/// and every capture function's `SET` clauses
/// ([`crate::capture::sql::pinned_output_settings`]), stay in step with the
/// pooled connection: every one of them derives from or interpolates this
/// same text, so a GUC added here is pinned everywhere else too, with
/// nothing to remember to update in a second place.
///
/// # `TimeZone`
///
/// `timestamptz_out` renders in the session's `TimeZone`, so pinning it to
/// `'UTC'` gives `timestamptz` the same text-stability `DateStyle` gives
/// `date`/`timestamp`. Trellis renders a value in two places: on its own
/// connections, and in a capture function running inside the application's
/// session. Both pin this constant, so they agree whatever the server,
/// database or role default is.
/// [`crate::temporal::is_bijective_under_text`]/
/// [`crate::temporal::is_render_consistent`] pick the story up from here.
///
/// `extra_float_digits` deserves a word, because `1` is already Postgres
/// 12+'s default and pinning a default can look like a no-op. It isn't: the
/// GUC is settable per-database and per-role, and the *old* default (`0`,
/// pre-12) means "round to `FLT_DIG`/`DBL_DIG` significant digits", which
/// loses information — `0.1::float8 + 0.2::float8` renders as `0.3` under
/// `0` and `0.30000000000000004` under `1`. A value that renders lossily on
/// one connection and exactly on another cannot round-trip through the
/// text-carried staging ring, and [`crate::float::render`] reproduces the
/// `>= 1` spelling specifically.
///
/// Note these are *output*-side settings. Input is a separate question and
/// is handled separately: `crate::defs::typed_literal` requires a literal's
/// text to be in a spelling that parses to the same value under **any**
/// `DateStyle` (leading-4-digit-year ISO is unambiguous), so a definition
/// installed against one server stays correct if read on another.
/// `IntervalStyle` joins them and is the clearest case of
/// all: `interval_out` renders the *same* value as `1 year 2 mons 3 days
/// 04:05:06` under `postgres`, `+1-2 +3 +4:05:06` under `sql_standard`,
/// `@ 1 year 2 mons 3 days 4 hours 5 mins 6 secs` under `postgres_verbose`
/// and `P1Y2M3DT4H5M6S` under `iso_8601` (all four read off a live server).
/// `crate::temporal::Interval::render` reproduces the `postgres` spelling
/// specifically, which is both Postgres's default and what an interval
/// `SUM` has to write back.
///
/// `TimeZone` joined the other four (see the "`TimeZone`"
/// section above): `timestamptz_out` renders in `'UTC'` everywhere Trellis
/// renders a value, making it a bijection on the instant exactly the way
/// `DateStyle` makes `date_out` a bijection on the day number.
///
/// `lc_monetary` joined too. `cash_out` spells a `money` value
/// in the session's monetary locale: `1234.56::money` is `$1,234.56` under
/// `C`, `£1,234.56` under `en_GB.UTF-8` and `kr.1.234,56` under
/// `en_DK.UTF-8`, and `cash_in` reads input under the same locale. A
/// `money` column is a passthrough value (`docs/type-support.md`), so an
/// operator's `ALTER DATABASE ... SET lc_monetary` used to change its
/// staged text. `'C'` is the value every libc has, and it renders the same
/// `$1,234.56` a server initialized with `C.UTF-8` (the test clusters)
/// or `en_US.UTF-8` defaults to.
///
/// `money` stores an integer count of the monetary locale's smallest unit,
/// scaled by its `frac_digits` at *input* time, so under a database whose
/// `lc_monetary` has `frac_digits` other than 2 (`ja_JP`'s is 0) the text
/// Trellis stages (`$12.35` for a value the application wrote as `¥1,235`)
/// is not what the application reads back. It is still a function of the
/// stored value alone, which is all this constant promises.
///
/// # `standard_conforming_strings`, the one *input* setting pinned here
///
/// Everything else here pins how a value is *rendered*. This one pins how
/// SQL text Trellis writes is *parsed*: [`quote_literal`],
/// [`crate::defs::typed_literal::render_sql`] and every `Expr::StringLiteral`
/// renderer escape a literal by doubling `'` and nothing else, which is
/// correct only when `standard_conforming_strings` is `on`. Under `off` a
/// backslash in an ordinary `'...'` literal starts an escape, so a column
/// named `a\b` quotes as the `jsonb_build_object` key `a<backspace>`, one
/// ending in `\` swallows the closing quote, and the canonical `bytea`
/// literal `'\x0102'` reads as a control character followed by `02`. `on`
/// has been the server default since Postgres 9.1, but an operator's
/// `ALTER DATABASE ... SET standard_conforming_strings = off` still reaches
/// every session that doesn't set it. It sits in this constant
/// because this is the one list every Trellis session applies. A capture
/// function pins it too, where it is harmless: its body's literals never
/// contain a backslash.
///
/// # `xmloption`, an input setting a stored value's text depends on
///
/// `xml_in` parses under `xmloption`: `document` accepts only a
/// well-formed document, `content` also accepts a fragment (`a<b/>`, plain
/// text). An application can store a fragment under its own `content`
/// session, and the group basis casts that value's text back into an `xml`
/// value through `jsonb_populate_record` ([`crate::staging::group_basis`]),
/// so an operator's `ALTER DATABASE ... SET xmloption = document` used to
/// fail every page carrying one. `content` accepts everything `document`
/// does, so it is pinned. A capture function
/// pins it too, where it is harmless: `xml_out` ignores it.
///
/// # Output settings deliberately *not* pinned
///
/// The rest of Postgres's locale and formatting settings were audited.
/// None of them changes what a type's output function (the
/// `::text` cast, capture's `format('%s', col)`, `to_jsonb`) prints:
///
/// * `lc_numeric` and `lc_time` only feed `to_char`'s locale patterns (`D`,
///   `G`, `L`, the `TM` prefix). `numeric_out`, `float8out` and the
///   date/time output functions ignore them. Trellis never calls `to_char`,
///   and its grammar can't express it.
/// * `lc_messages` changes error message text only. Trellis classifies
///   errors by SQLSTATE, never by message.
/// * `lc_collate`/`lc_ctype` are fixed per database, not settable per
///   session. Ordering is handled with explicit `collate "C"` and the
///   deterministic-collation checks on key columns.
/// * `client_encoding` is fixed to `UTF8` by `tokio-postgres` itself.
/// * `xmlbinary` and `timezone_abbreviations` change how *input* is parsed
///   (or, for `xmlbinary`, what the `xml*` constructor functions build),
///   not how a stored value is rendered, and no SQL Trellis writes depends
///   on them.
/// * `escape_string_warning` and `backslash_quote` only matter when
///   `standard_conforming_strings` is `off`, which it never is here.
pub(crate) const DETERMINISTIC_TEXT_OUTPUT_GUCS: &str = "set datestyle to 'ISO, YMD'; \
     set bytea_output to 'hex'; set extra_float_digits to 1; set intervalstyle to 'postgres'; \
     set timezone to 'UTC'; set lc_monetary to 'C'; set standard_conforming_strings to 'on'; \
     set xmloption to 'content'";

/// Makes a read or write that row-level security would filter **raise**
/// instead.
///
/// Under `row_security = off`, Postgres doesn't apply a table's policies to
/// the session at all. When they would apply (the table has RLS enabled and
/// the session's role is neither a superuser, nor has `BYPASSRLS`, nor owns
/// the table without `FORCE ROW LEVEL SECURITY`), the statement fails with
/// `42501` ("query would be affected by row-level security policy for
/// table ...") rather than silently seeing only the rows the policies allow.
/// A role that is exempt sees no difference: the setting only matters to a
/// role the policies apply to.
///
/// Trellis doesn't support policies that apply to the roles it reads or
/// writes as (`docs/transforms.md`, "Supported sources and targets"), and
/// checks the catalog for them at define, on every capture pass, and in
/// `self_check` ([`crate::defs::row_security`]). Those checks only
/// see the ring's owner and the role of the session running them, so a drain
/// or build logged in as any other role would otherwise read past them and
/// write a silently wrong target. This setting is the defense in depth behind them:
/// whichever role a Trellis connection logs in as, a filtered read fails
/// loudly, and the drain pauses the definitions it reaches
/// ([`crate::staging::halt`]) instead of charging keys for it.
///
/// It is applied by [`session_bootstrap`] and [`dedicated_session_setup`],
/// so every connection Trellis opens itself carries it. It isn't part of
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`], because that list is also pinned on
/// every capture function, which runs inside the *application's* write:
/// there it would turn a policy that applies to the ring's owner into a
/// failed application write. The capture pass pauses those readers instead.
pub(crate) const ROW_SECURITY_OFF: &str = "set row_security to off";

/// Turns JIT compilation off.
///
/// Postgres JIT-compiles a statement whose estimated cost is above
/// `jit_above_cost`, and inlines and optimizes it above
/// `jit_inline_above_cost` and `jit_optimize_above_cost`, whenever the server
/// is built with LLVM (as the PGDG and most distribution packages are) and
/// `jit` is on, its default. Inlining and optimizing one of Trellis's
/// statements costs 100–200 ms, and most of them are bounded by a page, a
/// chunk or a `limit`, so they never earn that back. Their cost estimates
/// are what cross the thresholds, not their work:
///
/// * A statement run with a node type turned off (the build's merge,
///   `staging::build::merge_deltas`, and the other plan pins) carries the
///   planner's `1e10` penalty for a disabled node wherever no other node
///   can do the job, before PostgreSQL 18. The merge's claimed count joins
///   its result `on true`, which only a nested loop can, so every merge
///   clears the thresholds: a merge of 35 rows does about 0.4 ms of work
///   and spends about 137 ms compiling.
/// * A build chunk's insert joins its keys (an `unnest`) to the source by
///   an expression, whose row estimate is the planner's default guess: at
///   10,000 keys it expects 21.7M rows, and spends 168 ms compiling a
///   statement that runs in 40 ms.
///
/// The statements that do read a whole table (a group-basis build's or
/// rebuild's absolute write, a 1-1 build's chunk and the resume's sweep) gain
/// nothing
/// from JIT either: at 5M rows each takes the same time, within 2%, with it
/// on or off, since its time goes to writing, sorting and hashing rows, not
/// to evaluating expressions.
///
/// It is applied by [`session_bootstrap`] and [`dedicated_session_setup`],
/// so every connection Trellis opens itself carries it, whatever the
/// server's, database's or role's default.
pub(crate) const JIT_OFF: &str = "set jit to off";

/// Starts every transaction at `READ COMMITTED`.
///
/// ADR-0002's I1 (read after lock) depends on it: a statement that reads
/// after the entry lock has to see what committed before it, including the
/// writer the lock waited on. At `READ COMMITTED` each statement takes a
/// fresh snapshot, so it does. At `REPEATABLE READ` or `SERIALIZABLE` the
/// whole transaction reads from the snapshot its first statement took,
/// before the lock, and a page could write a value derived from a row
/// another transaction had already replaced. Where that writer also
/// touched a row the page locks or writes, the page fails with `40001`
/// instead and is retried; where it didn't, nothing would notice.
///
/// It is applied by [`session_bootstrap`] and [`dedicated_session_setup`],
/// so every connection Trellis opens itself carries it, whatever the
/// server's, database's or role's default. A transaction that asks for a
/// level of its own still gets it: the capture pass reads its catalog in
/// one `REPEATABLE READ` snapshot.
pub(crate) const READ_COMMITTED: &str = "set default_transaction_isolation to 'read committed'";

/// Runs once per physical connection, right after it's established and
/// before it's returned to any caller.
///
/// Pins `search_path` to `schema` (first, so unqualified references to
/// Trellis's own objects always resolve there) followed by `target_schema`
/// (so unqualified reads/writes against a transform target table resolve
/// even when it lives outside both `schema` and `public`) and `public`
/// (Postgres's own default, kept last as a fallback for anything that
/// depends on it today), plus [`DETERMINISTIC_TEXT_OUTPUT_GUCS`],
/// [`ROW_SECURITY_OFF`], [`JIT_OFF`], [`READ_COMMITTED`], the server-side
/// TCP keepalives ([`tcp_keepalive_gucs`], without the user timeout: see
/// [`DeadPeerDetection::KeepalivesOnly`]) and the session's
/// `lock_timeout` cap ([`crate::locks::session_lock_timeout_sql`], ADR-0002
/// I7).
async fn session_bootstrap(
    client: &mut tokio_postgres::Client,
    schema: &str,
    target_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!(
            "set search_path to {}, {}, public; {DETERMINISTIC_TEXT_OUTPUT_GUCS}; \
             {ROW_SECURITY_OFF}; {JIT_OFF}; {READ_COMMITTED}; {}; {}",
            quote_ident(schema),
            quote_ident(target_schema),
            tcp_keepalive_gucs(DeadPeerDetection::KeepalivesOnly),
            crate::locks::session_lock_timeout_sql()
        ))
        .await
}

// ---------------------------------------------------------------------
// Dead-peer detection: TCP keepalives on both ends
// ---------------------------------------------------------------------

/// How long a connection sits with no traffic before the first keepalive
/// probe goes out. Applied on both ends; see [`tcp_keepalive_gucs`] for the
/// server's half and [`with_client_keepalives`] for the client's.
///
/// # Why every Trellis connection gets these, and why these numbers
///
/// Postgres frees a session's locks only when the backend learns its
/// client is gone. A process that exits closes its socket and the server
/// notices at once. A network partition closes nothing: the backend sits
/// idle on a socket whose peer can no longer answer until the kernel gives
/// up on it, which with Linux defaults (`tcp_keepalive_time` 7200s, then 9
/// probes 75s apart) is about 2h11m.
///
/// The producer session is where this hurt: its backend holds the
/// instance's producer-singleton advisory lock
/// ([`crate::staging::session::producer_singleton_lock_key`]), so a partitioned
/// producer kept every restart standing by, unable to take the lock, for the
/// whole two hours. But any connection partitioned mid-transaction pins its row
/// and advisory locks the same way, so the pool and every dedicated connection
/// get the keepalives too. Only the dedicated ones also get
/// [`TCP_USER_TIMEOUT`]; see [`DeadPeerDetection`] for why the pool doesn't.
///
/// Idle 10s, then up to 3 probes 5s apart, is a dead peer declared about 25s
/// after it last answered ([`TCP_USER_TIMEOUT`] matches that). The cost is one
/// probe per idle connection per 10s, and a connection is only given up on
/// after it has missed three probes in a row. A live peer's kernel answers
/// probes even when its application is busy or stalled, so keepalives never end
/// a healthy connection.
///
/// # Explicit settings win
///
/// There's no Trellis knob for these, but an operator's own settings are
/// respected, all-or-nothing per end (mixing schedules can leave a user
/// timeout shorter than the probing budget, which ends a connection on a
/// single lost probe):
///
/// - **Client end:** if the DSN sets any of `keepalives`, `keepalives_idle`,
///   `keepalives_interval`, `keepalives_retries` or `tcp_user_timeout`,
///   the client socket is left exactly as the DSN says.
/// - **Server end:** if any of the four `tcp_*` GUCs was set for this
///   connection specifically (the DSN's `options`/`PGOPTIONS`, or `ALTER
///   ROLE`/`ALTER DATABASE ... SET`), none of them is touched. A
///   server-wide value from `postgresql.conf` or the command line is
///   overridden, since it wasn't chosen with this client in mind.
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(10);

/// The gap between keepalive probes once [`TCP_KEEPALIVE_IDLE`] has passed.
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Unanswered probes before the connection is declared dead.
const TCP_KEEPALIVE_COUNT: u32 = 3;

/// How long data sent on the connection may go unacknowledged before the
/// kernel drops it: [`TCP_KEEPALIVE_IDLE`] + [`TCP_KEEPALIVE_COUNT`] ×
/// [`TCP_KEEPALIVE_INTERVAL`], the same 25s.
///
/// The keepalives alone don't cover a busy connection. Linux sends no
/// keepalive probe while the socket has unacknowledged data in flight, so a
/// partition that lands just after one end sent something is left to the
/// retransmission timeout instead, about 15 minutes under the default
/// `tcp_retries2`. `TCP_USER_TIMEOUT` bounds that case too, and when it's
/// set Linux also uses it in place of the probe count to decide when
/// keepalives have failed, which is why it matches the keepalive total.
///
/// Unlike the keepalives, it can end a healthy connection. See
/// [`DeadPeerDetection::KeepalivesOnly`].
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(25);

/// `tokio_postgres`'s default keepalive idle time. A parsed DSN reporting
/// anything else set `keepalives_idle` itself.
const TOKIO_POSTGRES_DEFAULT_KEEPALIVE_IDLE: Duration = Duration::from_secs(2 * 60 * 60);

/// How much of the dead-peer schedule a connection gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeadPeerDetection {
    /// Keepalives only, for the pool. [`TCP_USER_TIMEOUT`] also fires on a
    /// live peer that stops reading: once the reader's receive buffer is
    /// full it advertises a zero window, and Linux (5.11+) aborts a socket
    /// whose window has stayed shut for the user timeout even though the
    /// peer keeps acknowledging the window probes. On the server end that
    /// kills a pooled connection whose caller streams a large result
    /// (`query_raw`, `copy_out`) and pauses more than 25s between rows. The
    /// pool is handed to embedders ([`crate::Trellis::pool`]) and Trellis
    /// can't bound how they read, so it goes without. A partition that
    /// strikes a pooled connection with data in flight is then noticed at
    /// the retransmission timeout (~15 minutes) as before; the idle case,
    /// the usual one, still drops from about 2h to 25s.
    KeepalivesOnly,
    /// Keepalives plus [`TCP_USER_TIMEOUT`], for Trellis's own dedicated
    /// connections (producer session, maintenance loop, heartbeat daemon,
    /// wake listener). Trellis runs every query on them, reads every result
    /// eagerly and never pipelines a bulk write, so neither end sits on a
    /// shut window.
    KeepalivesAndUserTimeout,
}

impl DeadPeerDetection {
    fn user_timeout(self) -> Option<Duration> {
        match self {
            Self::KeepalivesOnly => None,
            Self::KeepalivesAndUserTimeout => Some(TCP_USER_TIMEOUT),
        }
    }
}

/// The statement that makes the **server** probe this connection's client
/// and drop it once the client stops answering. This is the half that
/// frees a partitioned session's locks. They're user-settable GUCs, so no
/// privilege is needed. It leaves all four alone if any was set for this
/// connection specifically (see [`TCP_KEEPALIVE_IDLE`], "Explicit settings
/// win"); `pg_settings.source` tells those apart from a server-wide default.
///
/// Postgres ignores them on a unix-socket connection, which has no network
/// to partition, and reads them back as `0` there. On a server without
/// `TCP_USER_TIMEOUT` support (macOS) it logs that it can't set that one,
/// and the statement still succeeds.
pub(crate) fn tcp_keepalive_gucs(detection: DeadPeerDetection) -> String {
    let mut settings = vec![
        (
            "tcp_keepalives_idle",
            TCP_KEEPALIVE_IDLE.as_secs().to_string(),
        ),
        (
            "tcp_keepalives_interval",
            TCP_KEEPALIVE_INTERVAL.as_secs().to_string(),
        ),
        ("tcp_keepalives_count", TCP_KEEPALIVE_COUNT.to_string()),
    ];
    if let Some(user_timeout) = detection.user_timeout() {
        settings.push(("tcp_user_timeout", user_timeout.as_millis().to_string()));
    }
    let values = settings
        .iter()
        .map(|(name, value)| format!("('{name}', '{value}')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "select pg_catalog.set_config(w.name, w.setting, false) \
         from (values {values}) as w(name, setting) \
         where not exists (\
             select from pg_catalog.pg_settings s \
             where s.name in ('tcp_keepalives_idle', 'tcp_keepalives_interval', \
                              'tcp_keepalives_count', 'tcp_user_timeout') \
               and s.source in ('client', 'database', 'user', 'database user'))"
    )
}

/// The **client's** half of [`tcp_keepalive_gucs`]: `tokio_postgres`'s own
/// keepalives, which default to 2h idle with OS-default probing. Leaves
/// `config` untouched if its DSN set any keepalive option itself (see
/// [`TCP_KEEPALIVE_IDLE`], "Explicit settings win").
///
/// Freeing the server's locks doesn't need this, but noticing the partition
/// does. A client waiting on a reply from a partitioned server sees nothing
/// arrive and, without its own probes, keeps waiting for the same two
/// hours. On the producer session that's the maintenance loop stuck
/// mid-step instead of failing, so it never reconnects. With these, the wait
/// fails after about 25s and the reconnect path takes over.
///
/// `tokio_postgres` sets the user timeout on Linux only and ignores it
/// elsewhere; the keepalive options work on Linux, macOS and Windows.
pub(crate) fn with_client_keepalives(
    config: &mut tokio_postgres::Config,
    detection: DeadPeerDetection,
) {
    let dsn_set_any = !config.get_keepalives()
        || config.get_keepalives_idle() != TOKIO_POSTGRES_DEFAULT_KEEPALIVE_IDLE
        || config.get_keepalives_interval().is_some()
        || config.get_keepalives_retries().is_some()
        || config.get_tcp_user_timeout().is_some();
    if dsn_set_any {
        return;
    }
    config
        .keepalives(true)
        .keepalives_idle(TCP_KEEPALIVE_IDLE)
        .keepalives_interval(TCP_KEEPALIVE_INTERVAL)
        .keepalives_retries(TCP_KEEPALIVE_COUNT);
    if let Some(user_timeout) = detection.user_timeout() {
        config.tcp_user_timeout(user_timeout);
    }
}

/// Opens a standalone (non-pooled) connection to `dsn` with the client-side
/// keepalives and user timeout set. The caller spawns or polls the returned
/// connection future, then runs [`dedicated_session_setup`] (or its own
/// superset of it) before using the client.
///
/// A DSN that doesn't parse is an [`Error::Config`] from
/// [`crate::config::parse_dsn`], never a `tokio_postgres::Error`, whose
/// `Debug` can quote part of the password.
///
/// The TLS options in `dsn` apply as they do to the pool; the connector is
/// rebuilt on each call, so a replaced certificate file is picked up.
pub(crate) async fn connect_dedicated(
    dsn: &str,
) -> Result<(tokio_postgres::Client, DedicatedConnection), Error> {
    let (mut config, tls) = crate::config::parse_dsn_tls(dsn)?;
    let tls = tls.connector()?;
    with_client_keepalives(&mut config, DeadPeerDetection::KeepalivesAndUserTimeout);
    Ok(config.connect(tls).await?)
}

/// The connection half of [`connect_dedicated`]: the future to spawn or poll.
pub(crate) type DedicatedConnection = tokio_postgres::Connection<
    tokio_postgres::Socket,
    <crate::tls::Tls as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::Stream,
>;

/// The session setup a dedicated connection runs straight after
/// [`connect_dedicated`], mirroring what [`session_bootstrap`] does for a
/// pooled one: `search_path` pinned to `schema` then `public`,
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`], [`ROW_SECURITY_OFF`], [`JIT_OFF`],
/// [`READ_COMMITTED`], [`tcp_keepalive_gucs`] with the user timeout, and
/// the `lock_timeout` cap ([`crate::locks::session_lock_timeout_sql`]).
pub(crate) fn dedicated_session_setup(schema: &str) -> String {
    format!(
        "set search_path to {}, public; {DETERMINISTIC_TEXT_OUTPUT_GUCS}; {ROW_SECURITY_OFF}; \
         {JIT_OFF}; {READ_COMMITTED}; {}; {}",
        quote_ident(schema),
        tcp_keepalive_gucs(DeadPeerDetection::KeepalivesAndUserTimeout),
        crate::locks::session_lock_timeout_sql()
    )
}

/// Quotes a Postgres identifier for safe interpolation into SQL text.
///
/// `config.schema` is operator-supplied (an environment variable), not
/// end-user input, but it still flows into SQL as text rather than a bind
/// parameter (identifiers can't be bound), so it's quoted defensively.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quotes a plain string as a Postgres string *literal* for safe
/// interpolation into SQL text — [`quote_ident`]'s counterpart for a value
/// rather than an identifier (doubling embedded `'` characters, the
/// `standard_conforming_strings = on` escaping that
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`] pins on every connection this crate
/// opens, so a backslash needs no escaping).
///
/// A column name can't be bound as an ordinary query parameter
/// (it isn't a value), but it still needs to appear as a `jsonb_build_object`
/// *key* — a text literal, not a bare identifier — when
/// `staging::apply`/`staging::quarantine` build an explicit per-column
/// `jsonb_build_object('<col>', <col>::text, ...)` in place of `to_jsonb(t.*)`
/// (see `staging::apply::row_as_text_jsonb_sql`). The column names it quotes
/// there come from live `pg_catalog` introspection, not end-user input, but
/// they're quoted defensively anyway, matching [`quote_ident`]'s own stance
/// on `config.schema`.
pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio_postgres::NoTls;

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("trellis"), "\"trellis\"");
        assert_eq!(quote_ident("weird\"schema"), "\"weird\"\"schema\"");
    }

    /// The client probes on the same schedule the server does,
    /// with the user timeout only on dedicated connections. The server half
    /// is checked against a live TCP session in `tests/tcp_keepalives.rs`;
    /// `tokio_postgres` exposes no way to read the client's socket options
    /// back, so this checks the config instead.
    #[test]
    fn client_keepalives_match_the_server_schedule() {
        for (detection, user_timeout) in [
            (DeadPeerDetection::KeepalivesOnly, None),
            (
                DeadPeerDetection::KeepalivesAndUserTimeout,
                Some(&TCP_USER_TIMEOUT),
            ),
        ] {
            let mut config = tokio_postgres::Config::from_str("host=127.0.0.1").expect("parse dsn");
            with_client_keepalives(&mut config, detection);
            assert!(config.get_keepalives(), "{detection:?}");
            assert_eq!(config.get_keepalives_idle(), TCP_KEEPALIVE_IDLE);
            assert_eq!(
                config.get_keepalives_interval(),
                Some(TCP_KEEPALIVE_INTERVAL)
            );
            assert_eq!(config.get_keepalives_retries(), Some(TCP_KEEPALIVE_COUNT));
            assert_eq!(config.get_tcp_user_timeout(), user_timeout, "{detection:?}");
        }
        // The user timeout replaces the probe count on Linux once set, so
        // it has to equal the probing budget or it changes the schedule.
        assert_eq!(
            TCP_USER_TIMEOUT,
            TCP_KEEPALIVE_IDLE + TCP_KEEPALIVE_INTERVAL * TCP_KEEPALIVE_COUNT
        );
        // The "did the DSN set it?" check compares against this default.
        assert_eq!(
            tokio_postgres::Config::new().get_keepalives_idle(),
            TOKIO_POSTGRES_DEFAULT_KEEPALIVE_IDLE
        );
    }

    /// A DSN that sets any client keepalive option keeps its own settings,
    /// all of them: Trellis doesn't fill in the rest around it.
    #[test]
    fn client_keepalives_leave_a_dsn_that_sets_any_alone() {
        for dsn in [
            "host=127.0.0.1 keepalives=0",
            "host=127.0.0.1 keepalives_idle=60",
            "host=127.0.0.1 keepalives_interval=20",
            "host=127.0.0.1 keepalives_retries=7",
            "host=127.0.0.1 tcp_user_timeout=90",
            "postgresql://postgres@127.0.0.1/db?keepalives_idle=60",
        ] {
            let parsed = tokio_postgres::Config::from_str(dsn).expect("parse dsn");
            let mut config = parsed.clone();
            with_client_keepalives(&mut config, DeadPeerDetection::KeepalivesAndUserTimeout);
            assert_eq!(config.get_keepalives(), parsed.get_keepalives(), "{dsn}");
            assert_eq!(
                config.get_keepalives_idle(),
                parsed.get_keepalives_idle(),
                "{dsn}"
            );
            assert_eq!(
                config.get_keepalives_interval(),
                parsed.get_keepalives_interval(),
                "{dsn}"
            );
            assert_eq!(
                config.get_keepalives_retries(),
                parsed.get_keepalives_retries(),
                "{dsn}"
            );
            assert_eq!(
                config.get_tcp_user_timeout(),
                parsed.get_tcp_user_timeout(),
                "{dsn}"
            );
        }
    }

    #[test]
    fn quote_literal_escapes_embedded_quotes() {
        assert_eq!(quote_literal("created_at"), "'created_at'");
        assert_eq!(quote_literal("weird'column"), "'weird''column'");
    }

    #[test]
    fn unparsable_dsn_is_a_typed_config_error() {
        let config =
            Config::from_dsn("not a valid dsn").expect("schema is valid; only the DSN is bogus");
        match Pool::new(&config) {
            Err(Error::Config(_)) => {}
            other => panic!("expected a typed config error, got {other:?}"),
        }
    }

    /// A DSN that doesn't parse gets a fixed message, not
    /// tokio_postgres's error, which can name part of the password.
    #[test]
    fn unparsable_dsn_error_carries_no_password_fragment() {
        for dsn in crate::config::malformed_dsn_fixtures::DSNS {
            let config = Config::from_dsn(dsn).expect("schema is valid; only the DSN is bogus");
            let err = Pool::new(&config).expect_err("the DSN doesn't parse");
            assert!(matches!(err, Error::Config(_)), "{err:?}");
            crate::config::malformed_dsn_fixtures::assert_no_password_fragment(dsn, &err);
        }
    }

    /// The dedicated connections (producer session, maintenance
    /// loop, wake listener, liveness daemon) parse the DSN themselves.
    #[tokio::test]
    async fn dedicated_connection_dsn_error_carries_no_password_fragment() {
        for dsn in crate::config::malformed_dsn_fixtures::DSNS {
            let err = connect_dedicated(dsn)
                .await
                .map(|_| ())
                .expect_err("the DSN doesn't parse");
            crate::config::malformed_dsn_fixtures::assert_no_password_fragment(dsn, &err);
        }
    }

    /// `connect_unpooled` opens its session with the pool's TLS settings
    /// (the pooled and dedicated sessions are covered in
    /// `tests/tls_connect.rs`, which cannot reach this crate-private call).
    #[tokio::test]
    async fn an_unpooled_connection_is_encrypted_as_the_dsn_asks() {
        let cluster = testkit::TestCluster::start_with_tls();
        let db = cluster.create_isolated_database().await;
        let ca = cluster.tls_dir().join("ca.crt");
        let dsn = format!(
            "host=127.0.0.1 port={} user=postgres dbname={} sslmode=verify-full sslrootcert='{}'",
            cluster.port(),
            db.name(),
            ca.display()
        );
        let pool = Pool::new(&Config::from_dsn(dsn).expect("config")).expect("pool");
        let client = pool.connect_unpooled().await.expect("unpooled connection");
        let encrypted: bool = client
            .query_one(
                "select ssl from pg_stat_ssl where pid = pg_backend_pid()",
                &[],
            )
            .await
            .expect("pg_stat_ssl")
            .get(0);
        assert!(encrypted);
    }

    /// `money` renders the same on every kind of session Trellis
    /// opens itself (pooled, unpooled, dedicated) even when the database's
    /// own `lc_monetary` default says otherwise.
    #[tokio::test]
    async fn money_renders_the_same_on_every_session_kind_under_a_hostile_lc_monetary() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let hostile = testkit::locale::hostile_lc_monetary(&raw).await;
        raw.batch_execute(&format!(
            "alter database {} set lc_monetary to '{hostile}'",
            quote_ident(db.name())
        ))
        .await
        .expect("apply hostile database lc_monetary");

        const PROBE: &str = "select 1234.56::money::text, (-1234.56)::money::text";
        let expected = (
            testkit::locale::PINNED_MONEY_TEXT.to_string(),
            "-$1,234.56".to_string(),
        );
        async fn render(client: &tokio_postgres::Client) -> (String, String) {
            let row = client.query_one(PROBE, &[]).await.expect("render money");
            (row.get(0), row.get(1))
        }

        // Control: a session started after the `ALTER DATABASE` that pins
        // nothing sees the hostile locale, or this test proves nothing.
        let (unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        assert_ne!(
            render(&unpinned).await,
            expected,
            "hostile lc_monetary {hostile} must be in effect"
        );

        // A fresh pool: `db.pool` may hold connections opened before the
        // `ALTER DATABASE`.
        let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("pool");
        let pooled = pool.get().await.expect("pooled connection");
        assert_eq!(render(&pooled).await, expected, "pooled");

        let unpooled = pool.connect_unpooled().await.expect("unpooled connection");
        assert_eq!(render(&unpooled).await, expected, "unpooled");

        let (dedicated, connection) = connect_dedicated(db.dsn()).await.expect("dedicated");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        dedicated
            .batch_execute(&dedicated_session_setup(crate::config::DEFAULT_SCHEMA))
            .await
            .expect("dedicated session setup");
        assert_eq!(render(&dedicated).await, expected, "dedicated");
    }

    /// Every kind of session Trellis opens itself (pooled, unpooled, dedicated) runs
    /// with `row_security` and `jit` off and starts its transactions at `read
    /// committed`, even when the database's defaults say otherwise: a read that
    /// row-level security would filter raises instead, no statement is JIT-compiled,
    /// and a read after a lock sees what committed before it.
    #[tokio::test]
    async fn session_settings_are_pinned_on_every_session_kind() {
        /// Each setting, the database default this test sets, and the value
        /// every Trellis session pins.
        const SETTINGS: [(&str, &str, &str); 3] = [
            ("row_security", "on", "off"),
            ("jit", "on", "off"),
            (
                "default_transaction_isolation",
                "repeatable read",
                "read committed",
            ),
        ];
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        for (name, default, _) in SETTINGS {
            raw.batch_execute(&format!(
                "alter database {} set {name} to {}",
                quote_ident(db.name()),
                quote_literal(default)
            ))
            .await
            .expect("an explicit database default");
        }

        /// Each setting's value, then the isolation level of a transaction
        /// opened with no level of its own.
        async fn settings(client: &mut tokio_postgres::Client) -> Vec<String> {
            let mut values = Vec::new();
            for (name, _, _) in SETTINGS {
                values.push(
                    client
                        .query_one("select current_setting($1)", &[&name])
                        .await
                        .expect("read the setting")
                        .get(0),
                );
            }
            let txn = client.transaction().await.expect("begin");
            values.push(
                txn.query_one("select current_setting('transaction_isolation')", &[])
                    .await
                    .expect("read the transaction's level")
                    .get(0),
            );
            txn.rollback().await.expect("rollback");
            values
        }
        let defaults: Vec<String> = SETTINGS
            .iter()
            .map(|(_, default, _)| default)
            .chain([&"repeatable read"])
            .map(|value| value.to_string())
            .collect();
        let pinned: Vec<String> = SETTINGS
            .iter()
            .map(|(_, _, pinned)| pinned)
            .chain([&"read committed"])
            .map(|value| value.to_string())
            .collect();

        // Control: a session that pins nothing has the database's defaults.
        let (mut unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        assert_eq!(settings(&mut unpinned).await, defaults, "unpinned");

        let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("pool");
        let mut pooled = pool.get().await.expect("pooled connection");
        assert_eq!(settings(&mut pooled).await, pinned, "pooled");

        // A transaction that asks for a level of its own still gets it, as
        // the capture pass's catalog read does.
        let txn = pooled
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await
            .expect("begin repeatable read");
        let level: String = txn
            .query_one("select current_setting('transaction_isolation')", &[])
            .await
            .expect("read the transaction's level")
            .get(0);
        assert_eq!(level, "repeatable read", "an explicit level");
        txn.rollback().await.expect("rollback");

        let mut unpooled = pool.connect_unpooled().await.expect("unpooled connection");
        assert_eq!(settings(&mut unpooled).await, pinned, "unpooled");

        let (mut dedicated, connection) = connect_dedicated(db.dsn()).await.expect("dedicated");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        dedicated
            .batch_execute(&dedicated_session_setup(crate::config::DEFAULT_SCHEMA))
            .await
            .expect("dedicated session setup");
        assert_eq!(settings(&mut dedicated).await, pinned, "dedicated");
    }

    /// The SQL Trellis writes escapes a string literal by
    /// doubling `'` only ([`quote_literal`],
    /// [`crate::defs::typed_literal::render_sql`]), which assumes
    /// `standard_conforming_strings = on`. A database default of `off` must
    /// not reach any session Trellis opens itself: here a column named
    /// `a\b` would otherwise become the image key `a<backspace>`, one named
    /// `trail\` would swallow its closing quote, and the canonical `bytea`
    /// literal `'\x0102'` would read as three other bytes.
    #[tokio::test]
    async fn backslashes_in_generated_literals_survive_a_database_default_of_nonstandard_strings() {
        use crate::defs::ast::ValueType;
        use crate::defs::pg_type::PgType;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "create table t (id int primary key, \"a\\b\" text, \"trail\\\" text); \
             insert into t values (1, 'x', 'y'); \
             alter database {} set standard_conforming_strings to off",
            quote_ident(db.name())
        ))
        .await
        .expect("seed the table and apply a hostile database default");

        let probe = |columns: &[&str]| {
            let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
            format!(
                "select array_agg(e.key || '=' || e.value order by e.key collate \"C\"), \
                        ({})::text \
                 from t, jsonb_each_text({}) e",
                crate::defs::typed_literal::render_sql(ValueType::Other(PgType::Bytea), "\\x0102"),
                crate::staging::apply::row_as_text_jsonb_sql("t", &columns)
            )
        };
        async fn render(client: &tokio_postgres::Client, sql: &str) -> (Vec<String>, String) {
            let row = client.query_one(sql, &[]).await.expect("render the probe");
            (row.get(0), row.get(1))
        }

        // Control: an unpinned session started after the `ALTER DATABASE`
        // really does read backslashes as escapes. `trail\` is left out of
        // this probe because under `off` it doesn't parse at all.
        let (unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let (keys, bytea) = render(&unpinned, &probe(&["id", "a\\b"])).await;
        assert_ne!(
            keys,
            ["a\\b=x", "id=1"],
            "hostile default must be in effect"
        );
        assert_ne!(bytea, "\\x0102", "hostile default must be in effect");

        let full = probe(&["id", "a\\b", "trail\\"]);
        let expected = (
            vec![
                "a\\b=x".to_string(),
                "id=1".to_string(),
                "trail\\=y".to_string(),
            ],
            "\\x0102".to_string(),
        );

        let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("pool");
        let pooled = pool.get().await.expect("pooled connection");
        assert_eq!(render(&pooled, &full).await, expected, "pooled");

        let unpooled = pool.connect_unpooled().await.expect("unpooled connection");
        assert_eq!(render(&unpooled, &full).await, expected, "unpooled");

        let (dedicated, connection) = connect_dedicated(db.dsn()).await.expect("dedicated");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        dedicated
            .batch_execute(&dedicated_session_setup(crate::config::DEFAULT_SCHEMA))
            .await
            .expect("dedicated session setup");
        assert_eq!(render(&dedicated, &full).await, expected, "dedicated");
    }

    /// `Pool`'s derived `Debug` reaches the DSN only through
    /// `deadpool_postgres::Manager`'s `tokio_postgres::Config`, whose own
    /// Issue #599: a call connection whose reset never ran (its runtime shut
    /// down with the reset task still queued) is closed, not pooled with the
    /// call's `statement_timeout` still set. The connection's driver runs on
    /// a runtime that stays up, and the call runs on a current-thread runtime
    /// that never polls the queued reset before it is dropped. With one
    /// connection in the pool, the next checkout would be that session.
    #[test]
    fn a_call_connection_whose_reset_never_ran_is_not_pooled() {
        let setup = tokio::runtime::Runtime::new().expect("setup runtime");
        let cluster = testkit::TestCluster::start();
        let db = setup.block_on(cluster.create_isolated_database());
        let config = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        let pool = Pool::with_limits(&config, 1, DEFAULT_CALL_POOL_WAIT_TIMEOUT).expect("pool");
        // Opened here, so its driver lives on `setup`.
        setup.block_on(async {
            drop(pool.get().await.expect("open the connection"));
        });

        let call = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("call runtime");
        call.block_on(crate::deadline::bounded(
            std::time::Instant::now(),
            Duration::from_secs(30),
            async {
                let client = pool.get().await.map_err(crate::TrellisError::Engine)?;
                // Released at the end of the call: its reset is queued on
                // `call`, which returns without running it.
                drop(client);
                Ok(())
            },
        ))
        .expect("the call");
        drop(call);

        let timeout: String = setup.block_on(async {
            pool.get()
                .await
                .expect("check out again")
                .query_one("show statement_timeout", &[])
                .await
                .expect("show")
                .get(0)
        });
        assert_eq!(
            timeout, "0",
            "the call's statement_timeout came back to the pool"
        );
    }

    /// Issue #599: a transaction opened inside a call gets the budget left
    /// when it begins, not the one its connection was checked out with.
    #[tokio::test]
    async fn a_calls_transaction_gets_the_budget_left_when_it_begins() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("dsn")).expect("pool");
        let (session, local) =
            crate::deadline::bounded(std::time::Instant::now(), Duration::from_secs(3), async {
                let mut client = pool.get().await.map_err(crate::TrellisError::Engine)?;
                let setting = "select setting::bigint from pg_settings \
                               where name = 'statement_timeout'";
                let session: i64 = client.query_one(setting, &[]).await.expect("read").get(0);
                // Stands for the call's earlier work.
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let txn = client.transaction().await.expect("begin");
                let local: i64 = txn.query_one(setting, &[]).await.expect("read").get(0);
                Ok((session, local))
            })
            .await
            .expect("the call");
        assert!((2500..=3000).contains(&session), "at checkout: {session}");
        assert!((1..=1500).contains(&local), "at begin: {local}");
    }

    /// `Debug` masks the password. This pins that, so a dependency bump
    /// that changes it fails here rather than in someone's logs.
    #[test]
    fn pool_debug_never_prints_the_password() {
        let config = Config::from_dsn("postgresql://alice:s3cret@db.example.com/app")
            .expect("schema is valid");
        let pool = Pool::new(&config).expect("the DSN parses; no I/O happens here");
        let printed = format!("{pool:?}");
        assert!(!printed.contains("s3cret"), "leaked: {printed}");
        assert!(printed.contains("db.example.com"), "{printed}");
    }

    /// The background pool is sized by the client's roles: `2N + 5` for a
    /// worker, `2N + 4` drain-only, `1` staging-only, none otherwise.
    #[test]
    fn the_background_pool_is_sized_by_role() {
        assert_eq!(background_pool_size(false, 0), None);
        assert_eq!(background_pool_size(true, 0), Some(1));
        assert_eq!(background_pool_size(true, 1), Some(7));
        assert_eq!(background_pool_size(true, 2), Some(9));
        assert_eq!(background_pool_size(true, 4), Some(13));
        assert_eq!(background_pool_size(false, 1), Some(6));
        assert_eq!(background_pool_size(false, 2), Some(8));
    }

    #[test]
    fn an_empty_pool_is_refused() {
        let config = Config::from_dsn("postgresql://example/db").expect("config");
        let err = Pool::with_limits(&config, 0, DEFAULT_CALL_POOL_WAIT_TIMEOUT).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
    }

    /// Idle connections close and a busy one stays: `close_idle` is what the
    /// pool's sweep runs, driven here directly.
    #[tokio::test]
    async fn idle_connections_close_and_the_most_recent_one_is_reused() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        let pool = Pool::new(&config).expect("pool");

        // A burst opens three connections.
        let held = [
            pool.get().await.expect("one"),
            pool.get().await.expect("two"),
            pool.get().await.expect("three"),
        ];
        drop(held);
        assert_eq!(pool.counts(), (3, 3));

        // Last in, first out: serial checkouts keep reusing one connection,
        // so the other two age while it stays warm.
        async fn backend(pool: &Pool) -> i32 {
            let client = pool.get().await.expect("get");
            client
                .query_one("select pg_backend_pid()", &[])
                .await
                .expect("pid")
                .get(0)
        }
        let first = backend(&pool).await;
        for _ in 0..3 {
            assert_eq!(backend(&pool).await, first);
        }

        // Nothing is idle for an hour yet.
        pool.close_idle(Duration::from_secs(3600));
        assert_eq!(pool.counts(), (3, 3));
        // Anything unused for a moment goes: the two that were not reused.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let warm = pool.get().await.expect("get");
        drop(warm);
        pool.close_idle(Duration::from_millis(15));
        assert_eq!(pool.counts(), (1, 1), "only the warm connection is left");
    }

    /// A task that holds more than [`MAX_BACKGROUND_HELD`] background
    /// connections is caught (debug builds; outside this test, by aborting the
    /// process); two are fine, and a released one is not counted.
    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn a_third_background_connection_in_one_task_is_caught() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        let pool = Pool::background(&config, 4).expect("pool");

        count_background_connections(async {
            let a = pool.get().await.expect("a");
            let b = pool.get().await.expect("b");
            drop(a);
            let c = pool
                .get()
                .await
                .expect("a released one, so this is the second");
            drop((b, c));
        })
        .await;

        let pool2 = pool.clone();
        let result = tokio::spawn(depth::panicking_scope(async move {
            let _a = pool2.get().await.expect("a");
            let _b = pool2.get().await.expect("b");
            let _c = pool2.get().await;
        }))
        .await;
        assert!(result.is_err(), "the third held connection must panic");

        // A pool of calls, or a task outside any scope, is not counted.
        let call_pool = Pool::new(&config).expect("pool");
        count_background_connections(async {
            let _held: Vec<_> = futures_three(&call_pool).await;
        })
        .await;
    }

    #[cfg(debug_assertions)]
    async fn futures_three(pool: &Pool) -> Vec<PooledClient> {
        vec![
            pool.get().await.expect("a"),
            pool.get().await.expect("b"),
            pool.get().await.expect("c"),
        ]
    }
}
