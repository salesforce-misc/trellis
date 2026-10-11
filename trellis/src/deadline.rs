//! The 30-second call contract (issue #599): every public [`crate::Trellis`]
//! and [`crate::BlockingTrellis`] call returns within a deadline, and the
//! work it started on the server is killed at that deadline rather than
//! abandoned.
//!
//! A call's deadline is a task-local ([`CallDeadline`]) installed by
//! [`bounded`], once, at the facade. Nothing between the facade and the
//! database takes it as an argument; the pool reads it when it hands out a
//! connection ([`crate::pool::Pool::get`]), and the three layers then bound
//! the call:
//!
//! 1. **The server kills the work.** A connection checked out inside a call
//!    gets a session `statement_timeout` of the budget remaining at checkout,
//!    and every transaction opened through the pooled client sets a
//!    `SET LOCAL statement_timeout` for the budget remaining when it begins
//!    (see [`crate::pool::Client::transaction`]). Postgres starts a fresh
//!    timer per statement and the timer covers lock waits, so a statement
//!    stuck on a lock is abandoned by the server and its transaction rolls
//!    back. No `cancel_query`: a cancel request can land on the connection's
//!    next query (#596, #599 rule 1).
//! 2. **The pool wait is part of the budget.** A checkout waits no longer
//!    than what is left of it.
//! 3. **A client-side backstop.** [`bounded`] drops the call's future
//!    [`BACKSTOP_GRACE`] after the deadline, for a connection that stopped
//!    answering altogether. The abandoned statement is still bounded on the
//!    server by (1), and the connection it was on stays out of the pool until
//!    the server has finished with it (see [`crate::pool::Client`]'s `Drop`).
//!
//! A call that runs out of budget rolls back and returns
//! [`TrellisError::CallTimeout`] ([`crate::ErrorCode::Timeout`]).
//!
//! Background work (the staging worker, drain workers) runs outside any call,
//! so none of this applies to it: [`current`] is `None` there. A step of it
//! that must be bounded the same way, a `self_check` page, runs under
//! [`within`].

use std::future::Future;
use std::time::{Duration, Instant};

use tokio_postgres::error::SqlState;

use crate::app::TrellisError;

/// The default budget of one public call.
pub const DEFAULT_CALL_BUDGET: Duration = Duration::from_secs(30);

/// How far past the deadline [`bounded`] waits for the server to report the
/// timeout itself before it gives up on the call client-side. Only a
/// connection that has stopped delivering anything at all (a network stall)
/// ever reaches it. The same grace as `staging::converge`'s #596 poll.
pub const BACKSTOP_GRACE: Duration = Duration::from_secs(1);

/// One public call's deadline, in scope for everything the call awaits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CallDeadline {
    at: Instant,
    budget: Duration,
}

tokio::task_local! {
    static CURRENT: CallDeadline;
}

impl CallDeadline {
    /// The time left, zero once the deadline has passed.
    pub(crate) fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    pub(crate) fn passed(&self) -> bool {
        Instant::now() >= self.at
    }

    /// `set_config('statement_timeout', ..., local)` for the time left now,
    /// as one statement. Rounded up, so the server can't fire before the
    /// deadline, and never below 1 ms, since 0 disables the timeout. A
    /// `statement_timeout` already in force that is shorter is kept: a DSN's
    /// `options`, a role or a database default.
    ///
    /// `local` is `SET LOCAL` (inside a transaction); otherwise the setting
    /// lasts for the session.
    ///
    /// The statement itself is [`crate::locks::cap_timeout_sql`], which reads
    /// the setting in force with `current_setting`, not `pg_settings` (#1010);
    /// this adds the time left, as [`timeout_ms`].
    pub(crate) fn set_statement_timeout_sql(&self, local: bool) -> String {
        crate::locks::cap_timeout_sql("statement_timeout", timeout_ms(self.remaining()), local)
    }

    /// How long a connection that ran under this deadline may still be busy
    /// with a statement an abandoned call left behind: the budget, since a
    /// statement started at the very end of the call is given what was left
    /// when its connection was checked out.
    pub(crate) fn settle_time(&self) -> Duration {
        self.budget
    }
}

/// `remaining` as a `statement_timeout` in ms: rounded up, so the server
/// can't fire before the deadline, at least 1 (0 disables the timeout) and at
/// most `i32::MAX` (the setting's own ceiling).
fn timeout_ms(remaining: Duration) -> u128 {
    remaining
        .as_nanos()
        .div_ceil(1_000_000)
        .clamp(1, i32::MAX as u128)
}

/// The deadline of the call this task is running, if it is running one.
pub(crate) fn current() -> Option<CallDeadline> {
    CURRENT.try_with(|deadline| *deadline).ok()
}

/// Runs `call` under a deadline of `budget` from `started`. Time already
/// spent queued counts: a call handed over from another thread passes the
/// instant it was submitted.
///
/// A call already inside a deadline (the blocking facade sets one when it
/// takes the job, and the public method it then calls would set another) keeps
/// the outer one.
pub(crate) async fn bounded<T>(
    started: Instant,
    budget: Duration,
    call: impl Future<Output = Result<T, TrellisError>>,
) -> Result<T, TrellisError> {
    if current().is_some() {
        return call.await;
    }
    let deadline = CallDeadline {
        at: started + budget,
        budget,
    };
    CURRENT
        .scope(deadline, async move {
            let backstop = tokio::time::Instant::from_std(deadline.at + BACKSTOP_GRACE);
            match tokio::time::timeout_at(backstop, call).await {
                Ok(Err(err)) if deadline.passed() && is_budget_error(&err) => {
                    Err(TrellisError::CallTimeout { budget })
                }
                Ok(result) => result,
                Err(_) => Err(TrellisError::CallTimeout { budget }),
            }
        })
        .await
}

/// A background step that ran past its budget ([`within`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetExpired {
    pub(crate) budget: Duration,
}

/// Runs a step of background work (outside any public call) under a deadline
/// of `budget` from now, so everything it asks of the database is bounded the
/// way a call's is: a connection it checks out gets a session
/// `statement_timeout` of the budget left, every transaction it opens a
/// `SET LOCAL` of the same, and a checkout waits no longer than what is left.
/// The server stops the work at the deadline. Past it by [`BACKSTOP_GRACE`]
/// the step's future is dropped, for a connection that stopped answering, and
/// the connection stays out of the pool until the server has finished with
/// what it left (see [`crate::pool::Client`]'s `Drop`).
///
/// A step already inside a deadline keeps it, like [`bounded`].
pub(crate) async fn within<T>(
    budget: Duration,
    work: impl Future<Output = T>,
) -> Result<T, BudgetExpired> {
    if current().is_some() {
        return Ok(work.await);
    }
    let deadline = CallDeadline {
        at: Instant::now() + budget,
        budget,
    };
    CURRENT
        .scope(deadline, async move {
            let backstop = tokio::time::Instant::from_std(deadline.at + BACKSTOP_GRACE);
            tokio::time::timeout_at(backstop, work)
                .await
                .map_err(|_| BudgetExpired { budget })
        })
        .await
}

/// Whether `err` is what running out of budget looks like: the server's
/// `statement_timeout` (`57014`, `query_canceled`), a lock wait that outlasted
/// its `lock_timeout` (`55P03`) at about the same moment, or a pool checkout
/// that waited out what was left of the budget. Only ever consulted once the
/// deadline has passed (like #596's poll): the budget's timer can't fire
/// before it, so an earlier `57014` is someone else's (a shorter session
/// `statement_timeout`, a `pg_cancel_backend`) and stays what it was.
fn is_budget_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut link = Some(err);
    while let Some(err) = link {
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return matches!(
                pg.code(),
                Some(code) if *code == SqlState::QUERY_CANCELED
                    || *code == SqlState::LOCK_NOT_AVAILABLE
            );
        }
        if let Some(pool) = err.downcast_ref::<deadpool_postgres::PoolError>() {
            return matches!(pool, deadpool_postgres::PoolError::Timeout(_));
        }
        link = err.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_deadline_outside_a_call() {
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn a_nested_call_keeps_the_outer_deadline() {
        let outer = Duration::from_secs(5);
        let seen = bounded(Instant::now(), outer, async {
            let inner = bounded(Instant::now(), Duration::from_secs(60), async {
                Ok(current().expect("in a call").budget)
            })
            .await?;
            Ok(inner)
        })
        .await
        .expect("call");
        assert_eq!(seen, outer);
    }

    #[tokio::test]
    async fn time_spent_queued_counts() {
        let queued = Instant::now() - Duration::from_secs(3);
        let remaining = bounded(queued, Duration::from_secs(5), async {
            Ok(current().expect("in a call").remaining())
        })
        .await
        .expect("call");
        assert!(remaining <= Duration::from_secs(2), "{remaining:?}");
    }

    #[tokio::test]
    async fn a_call_that_never_answers_times_out_at_the_backstop() {
        let started = Instant::now();
        let err = bounded(started, Duration::from_millis(50), async {
            std::future::pending::<Result<(), TrellisError>>().await
        })
        .await
        .expect_err("times out");
        assert!(matches!(err, TrellisError::CallTimeout { .. }), "{err}");
        assert!(started.elapsed() >= Duration::from_millis(50) + BACKSTOP_GRACE);
    }

    #[tokio::test]
    async fn the_set_sql_keeps_a_shorter_setting_and_rounds_up() {
        let deadline = CallDeadline {
            at: Instant::now() + Duration::from_millis(1500),
            budget: Duration::from_secs(2),
        };
        let sql = deadline.set_statement_timeout_sql(true);
        assert!(sql.contains("between 1 and 15") || sql.contains("between 1 and 14"));
        assert!(sql.contains("true)"));
        // `pg_settings` builds every setting on each read: a third of a
        // millisecond on every call.
        assert!(!sql.contains("pg_settings"), "{sql}");
        let past = CallDeadline {
            at: Instant::now() - Duration::from_secs(1),
            budget: Duration::from_secs(2),
        };
        assert!(
            past.set_statement_timeout_sql(false)
                .contains("between 1 and 1 ")
        );
    }

    #[test]
    fn the_timeout_rounds_up_and_stays_within_what_postgres_accepts() {
        assert_eq!(timeout_ms(Duration::ZERO), 1, "0 would disable it");
        assert_eq!(timeout_ms(Duration::from_nanos(1)), 1);
        assert_eq!(timeout_ms(Duration::from_millis(1500)), 1500);
        assert_eq!(timeout_ms(Duration::from_nanos(1_500_000_001)), 1501);
        assert_eq!(
            timeout_ms(Duration::from_secs(30 * 24 * 3600)),
            i32::MAX as u128
        );
        let month = CallDeadline {
            at: Instant::now() + Duration::from_secs(30 * 24 * 3600),
            budget: Duration::from_secs(30 * 24 * 3600),
        };
        let sql = month.set_statement_timeout_sql(false);
        assert!(
            sql.contains(&format!("between 1 and {} ", i32::MAX)),
            "{sql}"
        );
    }
}
