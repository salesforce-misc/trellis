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
//! **I6: never block an application writer.** DDL on a user table runs under
//! the much shorter [`USER_TABLE_DDL_LOCK_TIMEOUT`] and is retried once per
//! [`USER_TABLE_DDL_RETRY_INTERVAL`] until it lands ([`DdlRetry`]). A DDL
//! statement queued for a lock that conflicts with `ROW EXCLUSIVE` makes every
//! later writer queue behind it, so the queue must be short and must keep
//! emptying. ALTER PUBLICATION takes `SHARE UPDATE EXCLUSIVE`, which writers
//! don't queue behind, but its transaction still holds a snapshot while it
//! waits; `CREATE`/`DROP TRIGGER` (#622) take `SHARE ROW EXCLUSIVE`, which
//! they do. [#565 E7]: a bare `CREATE TRIGGER` stalled every writer for 25 s
//! behind one open transaction; a 50 ms `lock_timeout` retried every 200 ms
//! held the worst writer wait to 52 ms.
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

/// The `lock_timeout` DDL on a user table runs under (I6): see [`DdlRetry`].
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

/// Starts a stretch of user-table DDL inside `txn` (I6): sets the
/// transaction's `lock_timeout` to [`USER_TABLE_DDL_LOCK_TIMEOUT`] and
/// returns the setting it replaced, for [`end_user_table_ddl`] to put back.
/// Only the DDL runs under the short timeout, so the rest of the transaction
/// (catalog rows another Trellis transaction may hold for a moment) doesn't
/// turn every brief wait into a retry.
pub async fn begin_user_table_ddl(
    txn: &impl tokio_postgres::GenericClient,
) -> Result<String, tokio_postgres::Error> {
    let previous: String = txn
        .query_one("select current_setting('lock_timeout')", &[])
        .await?
        .get(0);
    set_local_lock_timeout(txn, USER_TABLE_DDL_LOCK_TIMEOUT).await?;
    Ok(previous)
}

/// Ends the stretch [`begin_user_table_ddl`] started, restoring `previous`
/// for the rest of the transaction.
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
/// today and `CREATE`/`DROP TRIGGER` from #622. Each attempt is its own
/// transaction that runs its DDL between [`begin_user_table_ddl`] and
/// [`end_user_table_ddl`] (or under
/// `set_local_lock_timeout(txn, USER_TABLE_DDL_LOCK_TIMEOUT)` when the DDL is
/// all it does); the caller loops:
///
/// ```ignore
/// let mut retry = DdlRetry::new("alter publication", None);
/// loop {
///     match attempt(client).await {
///         Err(err) if retry.again(&err).await => continue,
///         other => return other,
///     }
/// }
/// ```
///
/// [`DdlRetry::again`] says whether to go round again: only for a lock
/// timeout, and only before the deadline, after sleeping
/// [`USER_TABLE_DDL_RETRY_INTERVAL`]. With no deadline the loop runs until
/// the DDL lands. A caller that must not stall for that long (the
/// maintenance loop, which is also the only sealer) passes one and treats
/// the returned lock timeout as "not yet".
///
/// A loop rather than a closure-taking function: an attempt borrows the
/// caller's `&mut` client, and a closure that lends it out again per call
/// can't name its future's `Send`-ness for the spawned loops that run it.
#[derive(Debug)]
pub struct DdlRetry {
    what: &'static str,
    deadline: Option<Instant>,
    attempts: u32,
}

impl DdlRetry {
    pub fn new(what: &'static str, deadline: Option<Instant>) -> Self {
        Self {
            what,
            deadline,
            attempts: 0,
        }
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
        let resume = Instant::now() + USER_TABLE_DDL_RETRY_INTERVAL;
        if self.deadline.is_some_and(|deadline| resume > deadline) {
            tracing::info!(
                what = self.what,
                attempts = self.attempts,
                "user-table DDL still waiting for a lock at its deadline; leaving it for the \
                 next pass"
            );
            return false;
        }
        // One line per 5 s of waiting (25 attempts), not per attempt.
        if self.attempts % 25 == 1 {
            tracing::info!(
                what = self.what,
                attempts = self.attempts,
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
        let previous = begin_user_table_ddl(&*txn).await.expect("begin ddl");
        assert_eq!(show(&txn).await, "50");
        end_user_table_ddl(&*txn, &previous).await.expect("end ddl");
        assert_eq!(show(&txn).await, cap_ms());
    }

    #[tokio::test]
    async fn ddl_retry_gives_up_on_anything_but_a_lock_timeout() {
        let err = "port=not-a-port"
            .parse::<tokio_postgres::Config>()
            .expect_err("an unparseable port is a config error");
        let mut retry = DdlRetry::new("test", None);
        assert!(!retry.again(&err).await);
        assert_eq!(retry.attempts, 0);
    }
}
