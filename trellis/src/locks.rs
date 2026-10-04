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
            tracing::info!(
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
            tracing::info!(
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
