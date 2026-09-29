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
//! The per-attempt timeout depends on the lock the DDL takes:
//!
//! - **A lock that conflicts with `ROW EXCLUSIVE`** (`CREATE`/`DROP TRIGGER`'s
//!   `SHARE ROW EXCLUSIVE`, #622): a DDL statement queued for it makes every
//!   later writer queue behind it, so the queue must be short and keep
//!   emptying: [`USER_TABLE_DDL_LOCK_TIMEOUT`]. [#565 E7]: a bare `CREATE
//!   TRIGGER` stalled every writer for 25 s behind one open transaction; a
//!   50 ms `lock_timeout` retried every 200 ms held the worst writer wait to
//!   52 ms.
//! - **`SHARE UPDATE EXCLUSIVE`** (ALTER PUBLICATION): writers don't queue
//!   behind it, so it needs no such bound, only I7's.
//!   [`share_update_exclusive_ddl_timeout`] waits past `deadlock_timeout`
//!   instead, because that is when Postgres cancels an autovacuum holding
//!   the lock the waiter wants. Under a 50 ms timeout it never cancels one,
//!   and the DDL waits out the whole vacuum, hours on a large table.
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
/// pages queued on the same aggregate groups. The retries it forces are for a
/// transaction that holds a lock for much longer than that: a long
/// application transaction, a stuck chunk, an operator's `LOCK TABLE`. #617's
/// drain waited 1 h 50 min; this bounds that to two minutes.
///
/// **Interim value, sized for the aggregate group pre-lock (#326).** Drain
/// pages that touch the same groups queue on it one behind another, so the
/// last of eight workers waits out seven pages. `bench fold-in-ratio` at
/// ratio 10 (40k groups, every page touching most of them) measured the
/// longest page transaction, wait included, at 89 s (eight ~28k-record pages
/// queued together) and 100k-record pages at 47 s in steady state; a 5 s
/// timeout fired 75 times there, each retry losing its place in the queue.
/// #623 deletes the pre-lock, and should bring this back down.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(120);

/// The per-attempt `lock_timeout` for DDL on a user table whose lock
/// conflicts with writers' `ROW EXCLUSIVE` (I6): `CREATE`/`DROP TRIGGER`
/// (#622). See [`DdlRetry`].
pub const USER_TABLE_DDL_LOCK_TIMEOUT: Duration = Duration::from_millis(50);

/// The least [`share_update_exclusive_ddl_timeout`] returns.
// Only the publication's reconcile used this; C8 deletes it (issue #622).
#[allow(dead_code)]
const SHARE_UPDATE_EXCLUSIVE_DDL_FLOOR: Duration = Duration::from_secs(2);

/// The per-attempt `lock_timeout` for DDL on a user table that takes `SHARE
/// UPDATE EXCLUSIVE` (ALTER PUBLICATION), given the waiting session's
/// `deadlock_timeout`: twice it, at least
/// [`SHARE_UPDATE_EXCLUSIVE_DDL_FLOOR`], at most [`LOCK_TIMEOUT`].
///
/// A waiter runs the deadlock check once it has waited `deadlock_timeout`
/// (1 s by default), and that check is what cancels an autovacuum worker
/// holding the lock. A `lock_timeout` below `deadlock_timeout` gives up
/// before the check runs, so the DDL waits out the whole vacuum, however long
/// it takes. Twice `deadlock_timeout` leaves the cancelled worker as long
/// again to abort and release the lock. The floor covers a server whose
/// `deadlock_timeout` is tuned so low that its double would leave the worker
/// only a few milliseconds. The cap keeps I7's bound. The cap only binds past
/// a 60 s `deadlock_timeout`, and it still lets the check run until
/// `deadlock_timeout` reaches the cap itself.
///
/// Waiting this long costs no application writer anything: `ROW EXCLUSIVE`
/// doesn't conflict with `SHARE UPDATE EXCLUSIVE`, so writers don't queue
/// behind the waiting DDL. What does queue is other `SHARE UPDATE EXCLUSIVE`
/// or stronger requests (a manual `VACUUM`, `CREATE INDEX`, other DDL), for
/// at most this long. An anti-wraparound autovacuum is never cancelled; the
/// retry waits that one out.
// Only the publication's reconcile used this; C8 deletes it (issue #622).
#[allow(dead_code)]
pub fn share_update_exclusive_ddl_timeout(deadlock_timeout: Duration) -> Duration {
    deadlock_timeout
        .saturating_mul(2)
        .max(SHARE_UPDATE_EXCLUSIVE_DDL_FLOOR)
        .min(LOCK_TIMEOUT)
}

/// [`share_update_exclusive_ddl_timeout`] for `client`'s session, reading its
/// `deadlock_timeout`: the deadlock check runs in the waiting backend, on that
/// backend's own setting.
// Only the publication's reconcile used this; C8 deletes it (issue #622).
#[allow(dead_code)]
pub async fn read_share_update_exclusive_ddl_timeout(
    client: &impl tokio_postgres::GenericClient,
) -> Result<Duration, tokio_postgres::Error> {
    let ms: i64 = client
        .query_one(
            "select setting::bigint from pg_settings where name = 'deadlock_timeout'",
            &[],
        )
        .await?
        .get(0);
    Ok(share_update_exclusive_ddl_timeout(Duration::from_millis(
        u64::try_from(ms).unwrap_or(0),
    )))
}

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

/// Starts a stretch of user-table DDL inside `txn` (I6): sets the
/// transaction's `lock_timeout` to `timeout` (the attempt's, from
/// [`DdlRetry::lock_timeout`]) and returns the setting it replaced, for
/// [`end_user_table_ddl`] to put back. Only the DDL runs under the attempt's
/// timeout, so the rest of the transaction (catalog rows another Trellis
/// transaction may hold for a moment) keeps the session's.
// Only the publication's reconcile used this; C8 deletes it (issue #622).
#[allow(dead_code)]
pub async fn begin_user_table_ddl(
    txn: &impl tokio_postgres::GenericClient,
    timeout: Duration,
) -> Result<String, tokio_postgres::Error> {
    let previous: String = txn
        .query_one("select current_setting('lock_timeout')", &[])
        .await?
        .get(0);
    set_local_lock_timeout(txn, timeout).await?;
    Ok(previous)
}

/// Ends the stretch [`begin_user_table_ddl`] started, restoring `previous`
/// for the rest of the transaction.
// Only the publication's reconcile used this; C8 deletes it (issue #622).
#[allow(dead_code)]
pub async fn end_user_table_ddl(
    txn: &impl tokio_postgres::GenericClient,
    previous: &str,
) -> Result<(), tokio_postgres::Error> {
    txn.execute("select set_config('lock_timeout', $1, true)", &[&previous])
        .await?;
    Ok(())
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

/// The retry loop for DDL on a user table (I6), shared by ALTER PUBLICATION
/// today and `CREATE`/`DROP TRIGGER` from #622. The caller picks the
/// per-attempt `lock_timeout` by the lock its DDL takes (the module doc):
/// [`USER_TABLE_DDL_LOCK_TIMEOUT`] for one writers queue behind,
/// [`share_update_exclusive_ddl_timeout`] for ALTER PUBLICATION. Each attempt
/// is its own transaction that runs its DDL between [`begin_user_table_ddl`]
/// and [`end_user_table_ddl`] (or under
/// `set_local_lock_timeout(txn, retry.lock_timeout())` when the DDL is all it
/// does); the caller loops:
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
    // For an attempt that is only its DDL (#622's triggers, the tests);
    // `reconcile_publication` passes its timeout along itself.
    #[allow(dead_code)]
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

    /// Only the DDL stretch runs under the short timeout; the rest of the
    /// transaction gets the session's back.
    #[tokio::test]
    async fn a_user_table_ddl_stretch_restores_the_session_timeout() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = db.pool.get().await.expect("connect");
        let txn = client.transaction().await.expect("begin");
        let show = async |txn: &tokio_postgres::Transaction<'_>| -> String {
            txn.query_one(SETTING_MS, &[]).await.expect("show").get(0)
        };
        let previous = begin_user_table_ddl(&*txn, USER_TABLE_DDL_LOCK_TIMEOUT)
            .await
            .expect("begin ddl");
        assert_eq!(show(&txn).await, "50");
        end_user_table_ddl(&*txn, &previous).await.expect("end ddl");
        assert_eq!(show(&txn).await, cap_ms());
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

    /// ALTER PUBLICATION's attempts wait past `deadlock_timeout`, so the
    /// deadlock check runs and cancels an autovacuum holding the table.
    #[test]
    fn a_share_update_exclusive_attempt_outwaits_the_deadlock_check() {
        let ms = Duration::from_millis;
        for deadlock_timeout in [ms(1), ms(10), ms(100), ms(1000), ms(5000), ms(59_000)] {
            let timeout = share_update_exclusive_ddl_timeout(deadlock_timeout);
            assert!(
                timeout >= deadlock_timeout * 2 && timeout >= SHARE_UPDATE_EXCLUSIVE_DDL_FLOOR,
                "{deadlock_timeout:?} -> {timeout:?}"
            );
            assert!(timeout <= LOCK_TIMEOUT);
        }
        assert_eq!(share_update_exclusive_ddl_timeout(ms(1000)), ms(2000));
        assert_eq!(share_update_exclusive_ddl_timeout(ms(5000)), ms(10_000));
        // Past half the cap, I7's bound wins, and still outwaits the check
        // until `deadlock_timeout` reaches the cap itself.
        assert_eq!(share_update_exclusive_ddl_timeout(ms(90_000)), LOCK_TIMEOUT);
        assert!(share_update_exclusive_ddl_timeout(ms(90_000)) > ms(90_000));
        assert_eq!(
            share_update_exclusive_ddl_timeout(Duration::from_secs(3600)),
            LOCK_TIMEOUT
        );
    }

    /// The timeout reads the session's own `deadlock_timeout`: the server
    /// default, and a session's override.
    #[tokio::test]
    async fn the_share_update_exclusive_timeout_reads_the_sessions_deadlock_timeout() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("connect");
        let server: String = client
            .query_one(
                "select setting from pg_settings where name = 'deadlock_timeout'",
                &[],
            )
            .await
            .expect("show")
            .get(0);
        let server = Duration::from_millis(server.parse().expect("ms"));
        let timeout = read_share_update_exclusive_ddl_timeout(&**client)
            .await
            .expect("read");
        assert_eq!(timeout, share_update_exclusive_ddl_timeout(server));
        assert!(timeout > server, "{timeout:?} must outwait {server:?}");

        client
            .batch_execute("set deadlock_timeout = '7s'")
            .await
            .expect("set deadlock_timeout");
        assert_eq!(
            read_share_update_exclusive_ddl_timeout(&**client)
                .await
                .expect("read"),
            Duration::from_secs(14)
        );
        client
            .batch_execute("reset deadlock_timeout")
            .await
            .expect("reset");
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
