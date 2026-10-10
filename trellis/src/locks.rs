//! Lock discipline: ADR-0002 invariants I6 and I7 (issue #621).
//!
//! **I7: no Trellis transaction waits for a lock while it holds a snapshot
//! open.** Every connection Trellis opens (pooled, unpooled and dedicated)
//! starts its session with `lock_timeout` capped at [`LOCK_TIMEOUT`]
//! ([`session_lock_timeout_sql`]), so every lock wait in every Trellis
//! transaction ends by then. That covers the explicit lock statements
//! (`for update`, `for share`, advisory locks, DDL) and the implicit ones: an
//! `insert ... on conflict` waiting on another transaction's uncommitted key,
//! an `update` waiting on a locked row. #617's drain waited 1 h 50 min on the
//! second kind. A session setting, rather than a `set local` per transaction,
//! is what makes this hold for a transaction nobody audited, including one
//! written after this module. On `55P03` the transaction rolls back and the
//! caller retries from outside it: the drain in
//! `staging::apply::drain_batch`, the background loops on their next tick.
//!
//! **I6: never block an application writer.** DDL on a user table runs in
//! its own short transactions, each under a per-attempt `lock_timeout`, retried
//! once per [`USER_TABLE_DDL_RETRY_INTERVAL`] until it lands ([`DdlRetry`]).
//! That DDL is `CREATE`/`DROP TRIGGER` (#622), whose `SHARE ROW EXCLUSIVE`
//! conflicts with writers' `ROW EXCLUSIVE`: a DDL statement queued for it
//! makes every later writer queue behind it, so the queue must be short and
//! keep emptying: [`USER_TABLE_DDL_LOCK_TIMEOUT`]. [#565 E7]: a bare `CREATE
//! TRIGGER` stalled every writer for 25 s behind one open transaction; a
//! 50 ms `lock_timeout` retried every 200 ms held the worst writer wait to
//! 52 ms. An autovacuum holding the table is waited out, never cancelled
//! (#622 plan, Q1).
//!
//! [#565 E7]: https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119

use std::time::{Duration, Instant};

/// The longest any Trellis statement waits for a lock (I7). A session whose
/// `lock_timeout` is already set shorter (a DSN's `options`, a role or
/// database default) keeps its own; anything longer, or unset, is capped
/// here.
///
/// Long enough that a lock wait between Trellis's own transactions never
/// reaches it: a claim waiting behind a page's `segments` update, and drain
/// pages queued on the same ledger entries and group rows. The retries it
/// forces are for a transaction that holds a lock for much longer than that:
/// a long application transaction, a stuck chunk, an operator's `LOCK
/// TABLE`. #617's drain waited 1 h 50 min; this bounds that to 30 seconds.
///
/// **Sized by #623 D9**, once D5 had deleted the aggregate group pre-lock
/// (#326) that the interim 120 s was sized for. The shape that queued worst
/// on the pre-lock, `bench --disk fold-in-ratio --ratios 10` (40k groups at
/// 400k rows/s, eight workers, every page touching most groups), now has
/// its longest page transaction, wait included, at 6.6 s (it was 89 s), and
/// no page waits out the cap at either 30 s or 10 s. D9's benchmark round,
/// with a checkpoint every 30 s, saw 7.6 s at most. The rule was the
/// smallest of 30 s and 10 s with no timeouts and the longest page under a
/// third of the cap, which 10 s misses (6.6 s against 3.3 s): a cap that
/// close to a normal page would turn a slow checkpoint into retries, and
/// each retry loses its place in the queue. Going lower needs #629's
/// measurements first.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// The per-attempt `lock_timeout` for DDL on a user table whose lock
/// conflicts with writers' `ROW EXCLUSIVE` (I6): `CREATE`/`DROP TRIGGER`
/// (#622). See [`DdlRetry`].
pub const USER_TABLE_DDL_LOCK_TIMEOUT: Duration = Duration::from_millis(50);

/// How long [`DdlRetry`] waits between two attempts at DDL on a user table.
pub const USER_TABLE_DDL_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Milliseconds for a `lock_timeout` setting: at least 1, since 0 disables
/// the timeout, and at most the GUC's `int` ceiling.
fn millis(timeout: Duration) -> u128 {
    timeout.as_millis().clamp(1, i32::MAX as u128)
}

/// The statement a session runs at connect to cap its `lock_timeout` at
/// [`LOCK_TIMEOUT`]: keeps a shorter setting already in force, replaces a
/// longer one or `0` (no timeout, Postgres's default).
pub(crate) fn session_lock_timeout_sql() -> String {
    let ms = millis(LOCK_TIMEOUT);
    format!(
        "select set_config('lock_timeout', \
             case when setting::bigint between 1 and {ms} then setting else '{ms}' end, \
             false) \
         from pg_settings where name = 'lock_timeout'"
    )
}

/// Sets the current transaction's `lock_timeout` to `timeout` (`set local`,
/// so it ends with the transaction).
pub async fn set_local_lock_timeout(
    txn: &impl tokio_postgres::GenericClient,
    timeout: Duration,
) -> Result<(), tokio_postgres::Error> {
    txn.batch_execute(&format!("set local lock_timeout = {}", millis(timeout)))
        .await
}

/// Whether `err`, or any error on its [`std::error::Error::source`] chain, is
/// Postgres's `55P03` (`lock_not_available`): a `lock_timeout` firing or a
/// `NOWAIT` lock refused. The first `tokio_postgres::Error` on the chain
/// decides, the same walk `staging::quarantine`'s transient classification
/// makes.
pub fn is_lock_not_available(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut link = Some(err);
    while let Some(err) = link {
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return pg.code() == Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE);
        }
        link = err.source();
    }
    false
}

/// The first int of the column-pause lock's key (#922).
const COLUMN_PAUSE_LOCK_CLASS: i32 = 922;

/// The advisory lock key of `schema`'s column-pause lock, the one lock every
/// read-to-decide and every write of that instance's column-pause state takes
/// (#922, ADR-0014 "Locking"): `column_status` rows, the cascade edges
/// (`column_pause_cascades`) and the pending marks on them.
///
/// Advisory locks are keyed by `(database, key)` and know nothing of schemas,
/// so the instance schema is hashed into the key, as the producer singleton's
/// is (`staging::session::producer_singleton_lock_key`, #234): instances
/// sharing a database don't serialize each other's pauses (#978). A two-int
/// key, so it shares nothing with the single-`bigint` session locks. Two
/// schemas whose hashes collide would share the lock, which costs those two
/// instances the coupling this avoids and nothing else.
pub fn column_pause_lock_key(schema: &str) -> (i32, i32) {
    let hash = crate::staging::session::fnv1a_64(schema);
    // Fold the 64 bits onto 32, then reinterpret as the signed `int4` the
    // lock function takes.
    (COLUMN_PAUSE_LOCK_CLASS, ((hash >> 32) ^ hash) as u32 as i32)
}

/// How a transaction holds the column-pause lock ([`lock_column_pauses`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnPauseLock {
    /// A pause, fuse trip, cascade pair, `RESUME`, `DROP TRANSFORM`, an
    /// `ALTER TRANSFORM` that builds or drops a field, and a build start's
    /// release of `awaiting_capture` rows: transactions that write the
    /// pause state. They serialize with each other and with a define.
    Exclusive,
    /// A define: it reads the pause state of what it reads and writes the
    /// rows of its own new target. Defines run together.
    Shared,
}

/// Who takes the column-pause lock, the `op` label of
/// `trellis_column_pause_lock_timeouts_total` and the name in
/// [`ColumnPauseLockTimeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnPauseOp {
    /// `PAUSE TRANSFORM t.col`.
    Pause,
    /// `RESUME TRANSFORM t.col`.
    Resume,
    /// A column fuse trip.
    Fuse,
    /// One pair of a pause's cascade walk.
    Cascade,
    /// A define.
    Define,
    /// `DROP TRANSFORM`.
    Drop,
    /// `ALTER TRANSFORM` that builds or drops a field.
    Alter,
    /// A build's release of the `awaiting_capture` pauses an `ALTER`
    /// made.
    Capture,
}

impl ColumnPauseOp {
    /// The metric label.
    pub fn label(self) -> &'static str {
        match self {
            ColumnPauseOp::Pause => "pause",
            ColumnPauseOp::Resume => "resume",
            ColumnPauseOp::Fuse => "fuse",
            ColumnPauseOp::Cascade => "cascade",
            ColumnPauseOp::Define => "define",
            ColumnPauseOp::Drop => "drop",
            ColumnPauseOp::Alter => "alter",
            ColumnPauseOp::Capture => "capture",
        }
    }
}

/// The column-pause lock wasn't granted before the transaction's
/// `lock_timeout` ran out (`55P03`). The transaction has failed and rolls
/// back with nothing written; the call is retryable. A fuse trip and the
/// capture pass's callers retry on their next pass.
///
/// A call made of several such transactions can have committed some before
/// one times out: a `PAUSE` whose own row committed and whose cascade pair
/// timed out (the walk stays owed, and the capture pass finishes it), or a
/// `RESUME` that resumed one target before the next timed out. Each is
/// idempotent, so a retry finishes the rest.
#[derive(Debug)]
pub struct ColumnPauseLockTimeout {
    /// Which operation was waiting.
    pub op: ColumnPauseOp,
    source: tokio_postgres::Error,
}

impl std::fmt::Display for ColumnPauseLockTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: timed out waiting for the column-pause lock, which another pause, resume, \
             define, alter or drop holds; this step wrote nothing: retry",
            self.op.label()
        )
    }
}

impl std::error::Error for ColumnPauseLockTimeout {
    /// The `55P03` itself, so a retry classifier that walks the chain
    /// (`staging::quarantine::is_transient_error`) sees a lock timeout.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// [`lock_column_pauses`]'s failure.
#[derive(Debug)]
pub enum ColumnPauseLockError {
    /// The wait ran out ([`ColumnPauseLockTimeout`]).
    Timeout(ColumnPauseLockTimeout),
    /// Any other database error.
    Db(tokio_postgres::Error),
}

/// Takes the transaction-scoped column-pause lock of the instance whose
/// schema is `schema` in `mode` for `op`: the only place a column-pause
/// advisory lock is taken (rule 8 of #922; the guard
/// `tests::only_the_helper_takes_the_column_pause_lock` greps for it).
///
/// **Lock order.** A transaction that needs it takes these in this order,
/// and holds each to its commit:
///
/// 1. its source's version fence (`staging::build::bump_version_fence`),
///    when it needs one: the wait there can last the whole `lock_timeout`
///    behind busy writers, so nothing else is held for it;
/// 2. this lock;
/// 3. the fuse gate (`transform_fuse_gate`), when it needs one. No column-pause
///    site takes the gate today; `evict_for` takes it and then the definition
///    row without this lock;
/// 4. definition rows and `column_status` / cascade-edge rows.
///
/// With one lock for every writer and reader of the pause state, no two of
/// these transactions can each hold a row the other waits for: the second to
/// arrive waits here, holding only what comes before this in the order.
///
/// It waits as long as the transaction's `lock_timeout` in force: the
/// session's ([`LOCK_TIMEOUT`], 30 s) unless the caller narrowed it (a
/// capture-pass cascade pair uses one second). On `55P03` it counts
/// `trellis_column_pause_lock_timeouts_total{op}` and returns
/// [`ColumnPauseLockError::Timeout`]; the caller's transaction is aborted.
pub async fn lock_column_pauses(
    txn: &impl tokio_postgres::GenericClient,
    schema: &str,
    mode: ColumnPauseLock,
    op: ColumnPauseOp,
) -> Result<(), ColumnPauseLockError> {
    let sql = match mode {
        ColumnPauseLock::Exclusive => "select pg_advisory_xact_lock($1, $2)",
        ColumnPauseLock::Shared => "select pg_advisory_xact_lock_shared($1, $2)",
    };
    let (class, object) = column_pause_lock_key(schema);
    match txn.execute(sql, &[&class, &object]).await {
        Ok(_) => Ok(()),
        Err(err) if is_lock_not_available(&err) => {
            crate::metrics::increment_column_pause_lock_timeouts(op.label());
            Err(ColumnPauseLockError::Timeout(ColumnPauseLockTimeout {
                op,
                source: err,
            }))
        }
        Err(err) => Err(ColumnPauseLockError::Db(err)),
    }
}

/// The retry loop for DDL on a user table (I6): `CREATE`/`DROP TRIGGER`
/// (#622), under [`USER_TABLE_DDL_LOCK_TIMEOUT`] per attempt (the module
/// doc). Each attempt is its own transaction that runs its DDL under
/// `set_local_lock_timeout(txn, retry.lock_timeout())`; the caller loops:
///
/// ```ignore
/// let mut retry = DdlRetry::new("create trigger", USER_TABLE_DDL_LOCK_TIMEOUT, None);
/// loop {
///     match attempt(client, retry.lock_timeout()).await {
///         Err(err) if retry.again(&err).await => continue,
///         other => return other,
///     }
/// }
/// ```
///
/// [`DdlRetry::again`] says whether to go round again: only for a lock
/// timeout, after sleeping [`USER_TABLE_DDL_RETRY_INTERVAL`], and only if the
/// next attempt, timeout included, could end by the deadline. The first
/// attempt always runs, whatever the deadline. With no deadline the loop runs
/// until the DDL lands. A caller that must not stall for that long (the
/// maintenance loop, which is also the only sealer) passes one and treats
/// the returned lock timeout as "not yet".
///
/// A loop rather than a closure-taking function: an attempt borrows the
/// caller's `&mut` client, and a closure that lends it out again per call
/// can't name its future's `Send`-ness for the spawned loops that run it.
#[derive(Debug)]
pub struct DdlRetry {
    what: &'static str,
    lock_timeout: Duration,
    deadline: Option<Instant>,
    attempts: u32,
    started: Instant,
    last_logged: Option<Instant>,
}

/// How often a [`DdlRetry`] still waiting logs that it is.
const DDL_RETRY_LOG_INTERVAL: Duration = Duration::from_secs(5);

impl DdlRetry {
    /// A retry loop whose attempts each run under `lock_timeout`, retried
    /// until the DDL lands or, with a `deadline`, until the next attempt
    /// could no longer end by it.
    pub fn new(what: &'static str, lock_timeout: Duration, deadline: Option<Instant>) -> Self {
        Self {
            what,
            lock_timeout,
            deadline,
            attempts: 0,
            started: Instant::now(),
            last_logged: None,
        }
    }

    /// The `lock_timeout` each attempt runs its DDL under.
    pub fn lock_timeout(&self) -> Duration {
        self.lock_timeout
    }

    /// Whether the attempt that just failed with `err` should be retried.
    /// When it should, this has already slept the retry interval.
    ///
    /// Decides before it returns the future, so the future doesn't borrow
    /// `err` (a `dyn Error` isn't `Sync`, and the loops that run this are
    /// spawned).
    pub fn again(
        &mut self,
        err: &(dyn std::error::Error + 'static),
    ) -> impl std::future::Future<Output = bool> + Send + 'static {
        let retry = self.decide(err);
        async move {
            if retry {
                tokio::time::sleep(USER_TABLE_DDL_RETRY_INTERVAL).await;
            }
            retry
        }
    }

    fn decide(&mut self, err: &(dyn std::error::Error + 'static)) -> bool {
        if !is_lock_not_available(err) {
            return false;
        }
        self.attempts += 1;
        let now = Instant::now();
        let next_ends = now + USER_TABLE_DDL_RETRY_INTERVAL + self.lock_timeout;
        if self.deadline.is_some_and(|deadline| next_ends > deadline) {
            crate::instance_log::info!(
                what = self.what,
                attempts = self.attempts,
                "user-table DDL still waiting for a lock at its deadline; leaving it for the \
                 next pass"
            );
            return false;
        }
        // One line per `DDL_RETRY_LOG_INTERVAL` of waiting, not per attempt.
        if self
            .last_logged
            .is_none_or(|logged| now.duration_since(logged) >= DDL_RETRY_LOG_INTERVAL)
        {
            self.last_logged = Some(now);
            crate::instance_log::info!(
                what = self.what,
                attempts = self.attempts,
                waited_ms = now.duration_since(self.started).as_millis() as u64,
                lock_timeout_ms = self.lock_timeout.as_millis() as u64,
                "user-table DDL waiting for a lock; retrying every {}ms",
                USER_TABLE_DDL_RETRY_INTERVAL.as_millis()
            );
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_sql_caps_at_the_lock_timeout_and_keeps_a_shorter_one() {
        let sql = session_lock_timeout_sql();
        let ms = LOCK_TIMEOUT.as_millis();
        assert!(
            sql.contains(&format!("between 1 and {ms} then setting else '{ms}'")),
            "{sql}"
        );
    }

    #[test]
    fn millis_never_disables_the_timeout() {
        assert_eq!(millis(Duration::ZERO), 1);
        assert_eq!(millis(Duration::from_micros(10)), 1);
        assert_eq!(millis(Duration::from_millis(50)), 50);
        assert_eq!(millis(Duration::from_secs(1 << 40)), i32::MAX as u128);
    }

    /// A session's `lock_timeout` in milliseconds, as `pg_settings` shows it.
    const SETTING_MS: &str = "select setting from pg_settings where name = 'lock_timeout'";

    /// [`LOCK_TIMEOUT`] as [`SETTING_MS`] shows it.
    fn cap_ms() -> String {
        LOCK_TIMEOUT.as_millis().to_string()
    }

    async fn lock_timeout_of(pool: &crate::pool::Pool) -> String {
        let client = pool.get().await.expect("connect");
        let unpooled = pool.connect_unpooled().await.expect("unpooled");
        let pooled: String = client
            .query_one(SETTING_MS, &[])
            .await
            .expect("show")
            .get(0);
        let other: String = unpooled
            .query_one(SETTING_MS, &[])
            .await
            .expect("show")
            .get(0);
        assert_eq!(pooled, other, "pooled and unpooled sessions agree");
        pooled
    }

    /// I7's floor: every session Trellis opens caps its `lock_timeout` at
    /// [`LOCK_TIMEOUT`], and keeps a shorter one it was given.
    #[tokio::test]
    async fn every_session_caps_its_lock_timeout() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let pool_with = |options: &str| {
            crate::pool::Pool::new(
                &crate::config::Config::from_dsn(format!("{} {options}", db.dsn()))
                    .expect("valid dsn"),
            )
            .expect("pool")
        };
        assert_eq!(lock_timeout_of(&pool_with("")).await, cap_ms());
        assert_eq!(
            lock_timeout_of(&pool_with("options='-c lock_timeout=200'")).await,
            "200"
        );
        assert_eq!(
            lock_timeout_of(&pool_with("options='-c lock_timeout=1h'")).await,
            cap_ms()
        );

        let (dedicated, connection) = crate::pool::connect_dedicated(db.dsn())
            .await
            .expect("dedicated");
        tokio::spawn(connection);
        dedicated
            .batch_execute(&crate::pool::dedicated_session_setup("public"))
            .await
            .expect("setup");
        let shown: String = dedicated
            .query_one(SETTING_MS, &[])
            .await
            .expect("show")
            .get(0);
        assert_eq!(shown, cap_ms());
    }

    #[tokio::test]
    async fn ddl_retry_gives_up_on_anything_but_a_lock_timeout() {
        let err = "port=not-a-port"
            .parse::<tokio_postgres::Config>()
            .expect_err("an unparseable port is a config error");
        let mut retry = DdlRetry::new("test", USER_TABLE_DDL_LOCK_TIMEOUT, None);
        assert!(!retry.again(&err).await);
        assert_eq!(retry.attempts, 0);
    }

    /// A deadline stops the retries once the next attempt, timeout
    /// included, could no longer end by it; the first attempt isn't asked.
    #[tokio::test]
    async fn ddl_retry_stops_before_an_attempt_that_would_overrun_its_deadline() {
        let err = lock_not_available().await;
        let long = Duration::from_secs(2);
        let mut retry = DdlRetry::new("test", long, Some(Instant::now() + Duration::from_secs(1)));
        assert!(
            !retry.again(&err).await,
            "a 2 s attempt can't end within 1 s"
        );

        let mut retry = DdlRetry::new(
            "test",
            USER_TABLE_DDL_LOCK_TIMEOUT,
            Some(Instant::now() + Duration::from_secs(1)),
        );
        assert!(retry.again(&err).await, "a 50 ms attempt fits");
    }

    /// How many timeouts `trellis_column_pause_lock_timeouts_total` has
    /// counted for `op` so far in this process.
    fn timeouts_counted(op: ColumnPauseOp) -> u64 {
        let prefix = format!(
            "trellis_column_pause_lock_timeouts_total{{trellis_instance=\"unknown\",op=\"{}\"}} ",
            op.label()
        );
        crate::metrics::Metrics::new()
            .render_prometheus()
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .map_or(0, |count| count.trim().parse().expect("a counter value"))
    }

    /// Rule 5 and 6 of #922: a wait that outlasts the transaction's
    /// `lock_timeout` returns the named, retryable error, counts the metric
    /// under its caller, and leaves the lock free for a retry. Two shared
    /// holders (defines) don't wait on each other; an exclusive one waits
    /// for them, and they wait for it.
    #[tokio::test]
    async fn a_held_lock_times_out_with_the_retryable_error_and_counts_the_metric() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let schema = crate::config::DEFAULT_SCHEMA;
        let mut holder = db.pool.get().await.expect("connect");
        let mut waiter = db.pool.get().await.expect("connect");
        let hold = holder.transaction().await.expect("begin");
        lock_column_pauses(
            &*hold,
            schema,
            ColumnPauseLock::Exclusive,
            ColumnPauseOp::Pause,
        )
        .await
        .expect("an idle lock is granted at once");

        let before = timeouts_counted(ColumnPauseOp::Resume);
        let attempt = waiter.transaction().await.expect("begin");
        set_local_lock_timeout(&*attempt, Duration::from_millis(100))
            .await
            .expect("set the timeout");
        let err = lock_column_pauses(
            &*attempt,
            schema,
            ColumnPauseLock::Exclusive,
            ColumnPauseOp::Resume,
        )
        .await
        .expect_err("the lock is held");
        let ColumnPauseLockError::Timeout(timeout) = &err else {
            panic!("a named timeout, got {err:?}");
        };
        assert_eq!(timeout.op, ColumnPauseOp::Resume);
        assert!(is_lock_not_available(timeout), "the 55P03 is its source");
        assert_eq!(timeouts_counted(ColumnPauseOp::Resume), before + 1);

        // Through the operator-facing errors: retryable, and still a
        // transient lock failure for the drain's and the build's retry rules.
        let apply: crate::staging::apply::ApplyError = err.into();
        assert_eq!(apply.code(), crate::error_code::ErrorCode::Timeout);
        assert!(crate::staging::quarantine::is_transient_error(&apply));
        drop(attempt);

        // A shared taker waits for the exclusive holder too.
        let attempt = waiter.transaction().await.expect("begin");
        set_local_lock_timeout(&*attempt, Duration::from_millis(100))
            .await
            .expect("set the timeout");
        let before = timeouts_counted(ColumnPauseOp::Define);
        let err = lock_column_pauses(
            &*attempt,
            schema,
            ColumnPauseLock::Shared,
            ColumnPauseOp::Define,
        )
        .await
        .expect_err("the lock is held exclusive");
        assert!(matches!(err, ColumnPauseLockError::Timeout(_)));
        assert_eq!(timeouts_counted(ColumnPauseOp::Define), before + 1);
        drop(attempt);

        hold.commit().await.expect("release");
        let retry = waiter.transaction().await.expect("begin");
        lock_column_pauses(
            &*retry,
            schema,
            ColumnPauseLock::Exclusive,
            ColumnPauseOp::Resume,
        )
        .await
        .expect("the retry finds it free");
        retry.commit().await.expect("commit");

        // Two shared holders at once, then an exclusive one that has to wait.
        let shared_a = holder.transaction().await.expect("begin");
        lock_column_pauses(
            &*shared_a,
            schema,
            ColumnPauseLock::Shared,
            ColumnPauseOp::Define,
        )
        .await
        .expect("shared");
        let shared_b = waiter.transaction().await.expect("begin");
        set_local_lock_timeout(&*shared_b, Duration::from_millis(100))
            .await
            .expect("set the timeout");
        lock_column_pauses(
            &*shared_b,
            schema,
            ColumnPauseLock::Shared,
            ColumnPauseOp::Define,
        )
        .await
        .expect("defines don't wait on each other");
        let mut third = db.pool.get().await.expect("connect");
        let exclusive = third.transaction().await.expect("begin");
        set_local_lock_timeout(&*exclusive, Duration::from_millis(100))
            .await
            .expect("set the timeout");
        assert!(
            lock_column_pauses(
                &*exclusive,
                schema,
                ColumnPauseLock::Exclusive,
                ColumnPauseOp::Alter
            )
            .await
            .is_err(),
            "an exclusive taker waits for the shared holders"
        );
    }

    /// #978: the lock is one per instance, not per database. Instance A holds
    /// its column-pause lock in an open transaction; instance B, in the same
    /// database, takes its own at once (a `lock_timeout` of a few
    /// milliseconds turns any wait into a failure, so no polling), and a
    /// second taker of A's still waits.
    #[tokio::test]
    async fn instances_in_one_database_take_their_own_column_pause_lock() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut a = db.pool.get().await.expect("connect");
        let mut b = db.pool.get().await.expect("connect");
        let mut a_again = db.pool.get().await.expect("connect");
        let hold_a = a.transaction().await.expect("begin");
        lock_column_pauses(
            &*hold_a,
            "instance_a",
            ColumnPauseLock::Exclusive,
            ColumnPauseOp::Pause,
        )
        .await
        .expect("an idle lock is granted at once");

        let txn_b = b.transaction().await.expect("begin");
        set_local_lock_timeout(&*txn_b, Duration::from_millis(5))
            .await
            .expect("set the timeout");
        for mode in [ColumnPauseLock::Exclusive, ColumnPauseLock::Shared] {
            lock_column_pauses(&*txn_b, "instance_b", mode, ColumnPauseOp::Pause)
                .await
                .expect("another instance's lock is not held");
        }

        let txn_a = a_again.transaction().await.expect("begin");
        set_local_lock_timeout(&*txn_a, Duration::from_millis(5))
            .await
            .expect("set the timeout");
        // An op of its own: the timeout metric is process-wide, and
        // another test counts `resume`'s.
        let err = lock_column_pauses(
            &*txn_a,
            "instance_a",
            ColumnPauseLock::Exclusive,
            ColumnPauseOp::Fuse,
        )
        .await
        .expect_err("the same instance's lock is held");
        assert!(matches!(err, ColumnPauseLockError::Timeout(_)));
    }

    /// The key is a pure function of the schema: the same schema always
    /// takes the same lock, and distinct schemas take distinct ones, in the
    /// two-int key space (so never a single-`bigint` session lock).
    #[test]
    fn the_column_pause_lock_key_follows_the_schema() {
        assert_eq!(column_pause_lock_key("a"), column_pause_lock_key("a"));
        assert_ne!(column_pause_lock_key("a"), column_pause_lock_key("b"));
        assert_ne!(
            column_pause_lock_key("public"),
            column_pause_lock_key("trellis_b")
        );
        assert_eq!(column_pause_lock_key("a").0, COLUMN_PAUSE_LOCK_CLASS);
    }

    /// Rule 8 of #922: one function takes the column-pause lock, so a new
    /// call site can't skip the order or the timeout handling. Any
    /// `pg_advisory_xact_lock` in `src` outside that function's file fails
    /// this, except the test-only gates (`staging::interleave`, the `__pause`
    /// column `staging::ledger` adds for it, and a gate function in
    /// `client.rs`'s tests), which hold a lock key of the test's choosing and
    /// never the pause lock's.
    #[test]
    fn only_the_helper_takes_the_column_pause_lock() {
        const ALLOWED: [&str; 4] = [
            "locks.rs",
            "client.rs",
            "staging/interleave.rs",
            "staging/ledger.rs",
        ];
        fn visit(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("read src") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    visit(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        visit(&src, &mut files);
        assert!(files.len() > 50, "the walk found the sources");
        let offenders: Vec<String> = files
            .iter()
            .filter_map(|path| {
                let rel = path.strip_prefix(&src).expect("under src");
                let rel = rel.to_string_lossy().replace('\\', "/");
                if ALLOWED.contains(&rel.as_str()) {
                    return None;
                }
                let text = std::fs::read_to_string(path).expect("read a source");
                text.contains("pg_advisory_xact_lock").then_some(rel)
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "take the column-pause lock through locks::lock_column_pauses: {offenders:?}"
        );
    }

    /// A real `55P03`, from a `NOWAIT` lock another session holds.
    async fn lock_not_available() -> tokio_postgres::Error {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut holder = db.pool.get().await.expect("connect");
        let mut waiter = db.pool.get().await.expect("connect");
        holder
            .batch_execute("create table held (id int)")
            .await
            .expect("create");
        let hold = holder.transaction().await.expect("begin");
        hold.batch_execute("lock table held in access exclusive mode")
            .await
            .expect("hold");
        let attempt = waiter.transaction().await.expect("begin");
        let err = attempt
            .batch_execute("lock table held in access exclusive mode nowait")
            .await
            .expect_err("held");
        assert!(is_lock_not_available(&err));
        err
    }
}
