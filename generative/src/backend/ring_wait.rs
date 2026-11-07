//! The ring wait inside a quiesce ([`super::sql::quiesce`]): the engine's
//! [`await_converged`], given up only after a window in which the ring made
//! no progress at all, or at a hard cap however much it progressed.
//!
//! A fixed budget can't tell a slow drain from a stuck one. A drain page
//! that sleeps at a commit-time pause point while it holds the `segments`
//! row lock makes every other page wait for it, so a run with many stalled
//! pages converges in about the sum of its stalls: seconds each, finite, and
//! over any fixed budget often enough to fail runs that would have passed.
//! A stuck drain (a page that never commits, a livelock that retries the
//! same page, a held key that never clears) commits nothing. So the wait
//! restarts its window whenever the ring's drain state moves
//! ([`progress_mark`]) and fails when it hasn't moved for a whole window.
//!
//! Every timeout prints the database's state to stderr first
//! ([`dump_state`]), so a real hang fails with the evidence of what it was
//! waiting on.

use std::time::{Duration, Instant};

use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::types::PgLsn;
use trellis::dev::staging::{StagingError, await_converged};

/// A quiesce's time budget.
#[derive(Clone, Copy, Debug)]
pub(super) struct QuiesceBudget {
    /// How long the quiesce may go without the ring advancing. The status
    /// and partition waits get this much from the last advance too.
    pub no_progress: Duration,
    /// The ceiling on the whole quiesce, however often the ring advances.
    pub cap: Duration,
}

/// How often the ring wait samples [`progress_mark`]: one bounded poll slice
/// of [`await_converged`]. A sample costs one small query over the ring's
/// bookkeeping tables, and a quiesce that converges within the first slice
/// takes none.
const PROGRESS_SAMPLE: Duration = Duration::from_secs(1);

/// The `statement_timeout` (ms) on each [`progress_mark`] and [`dump_state`]
/// query, so neither can hang on a lock the stuck state holds.
const PROBE_TIMEOUT_MS: u32 = 2_000;

/// One quiesce's deadline: [`QuiesceBudget::no_progress`] after the last
/// time the ring advanced (or the quiesce started), never past
/// [`QuiesceBudget::cap`] after the start. Pure bookkeeping over the instants
/// it is given, so the logic is tested without a database or a clock.
#[derive(Debug)]
pub(super) struct ProgressDeadline {
    budget: QuiesceBudget,
    started: Instant,
    window_start: Instant,
    mark: Option<String>,
    restarts: u32,
    ring_waited: Duration,
}

/// Why a [`ProgressDeadline`] passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Expiry {
    /// The ring did not advance for a whole no-progress window.
    NoProgress,
    /// The cap passed while the ring was still advancing.
    Cap,
}

impl ProgressDeadline {
    pub(super) fn new(budget: QuiesceBudget, now: Instant) -> Self {
        ProgressDeadline {
            budget,
            started: now,
            window_start: now,
            mark: None,
            restarts: 0,
            ring_waited: Duration::ZERO,
        }
    }

    /// When the quiesce started.
    pub(super) fn started(&self) -> Instant {
        self.started
    }

    /// When the current no-progress window started: the quiesce's start, or
    /// the sample that last saw the ring advance.
    pub(super) fn window_start(&self) -> Instant {
        self.window_start
    }

    /// The budget measured from [`Self::window_start`], in the
    /// `(started, timeout)` form `sql::poll_until_empty` takes.
    pub(super) fn window_budget(&self) -> Duration {
        let cap_end = self.started + self.budget.cap;
        self.budget
            .no_progress
            .min(cap_end.saturating_duration_since(self.window_start))
    }

    fn deadline(&self) -> Instant {
        self.window_start + self.window_budget()
    }

    /// Time left before the deadline, zero once it has passed.
    pub(super) fn remaining(&self, now: Instant) -> Duration {
        self.deadline().saturating_duration_since(now)
    }

    /// Whether the deadline has passed at `now`, and which limit it was.
    pub(super) fn expired(&self, now: Instant) -> Option<Expiry> {
        if now < self.deadline() {
            None
        } else if self.window_start + self.budget.no_progress <= now {
            Some(Expiry::NoProgress)
        } else {
            Some(Expiry::Cap)
        }
    }

    /// Records a [`progress_mark`] sampled at `now`. A mark that differs
    /// from the previous one means the ring advanced: the window restarts at
    /// `now`, and this returns `true`. The first mark is the baseline, and a
    /// failed sample (`None`) shows nothing; neither restarts.
    pub(super) fn observe(&mut self, mark: Option<String>, now: Instant) -> bool {
        let Some(mark) = mark else {
            return false;
        };
        let advanced = self.mark.as_ref().is_some_and(|previous| *previous != mark);
        self.mark = Some(mark);
        if advanced {
            self.window_start = now;
            self.restarts += 1;
        }
        advanced
    }

    /// How many times the ring's progress restarted the window.
    pub(super) fn restarts(&self) -> u32 {
        self.restarts
    }

    /// The time spent in [`await_ring`], summed over the quiesce's passes.
    pub(super) fn ring_waited(&self) -> Duration {
        self.ring_waited
    }
}

/// Waits for the ring to converge through `token` within `deadline`, in
/// [`PROGRESS_SAMPLE`] slices of [`await_converged`], sampling
/// [`progress_mark`] after each slice that didn't converge. Polls at least
/// once even with the deadline already passed, like [`await_converged`].
///
/// On expiry, prints [`dump_state`] and returns the error [`await_converged`]
/// itself returns on timeout, [`StagingError::ConvergenceTimeout`], with the
/// time this call waited.
pub(super) async fn await_ring(
    raw: &tokio_postgres::Client,
    token: PgLsn,
    deadline: &mut ProgressDeadline,
) -> Result<(), StagingError> {
    let started = Instant::now();
    let mut expired = None;
    let result = loop {
        let slice = deadline.remaining(Instant::now()).min(PROGRESS_SAMPLE);
        match await_converged(raw, token, slice).await {
            Ok(()) => break Ok(()),
            Err(StagingError::ConvergenceTimeout { .. }) => {}
            Err(other) => break Err(other),
        }
        deadline.observe(progress_mark(raw).await, Instant::now());
        expired = deadline.expired(Instant::now());
        if expired.is_some() {
            break Err(StagingError::ConvergenceTimeout {
                token,
                waited: started.elapsed(),
            });
        }
    };
    deadline.ring_waited += started.elapsed();
    if let Some(expiry) = expired {
        dump_state(raw, Some(token), &expiry_reason(expiry, deadline)).await;
    }
    result
}

/// The line [`dump_state`] opens with for an expired `deadline`.
pub(super) fn expiry_reason(expiry: Expiry, deadline: &ProgressDeadline) -> String {
    let what = match expiry {
        Expiry::NoProgress => format!("no ring progress for {:?}", deadline.budget.no_progress),
        Expiry::Cap => format!("cap of {:?} reached", deadline.budget.cap),
    };
    format!(
        "{what}; quiesce ran {:?}, ring wait {:?}, {} progress restarts",
        deadline.started.elapsed(),
        deadline.ring_waited,
        deadline.restarts
    )
}

/// A fingerprint of the ring's drain state, or `None` if the query failed
/// or timed out (a sample that shows nothing).
///
/// Every committed drain page writes one of the first two parts: a page
/// with more of its buckets left moves those buckets' `drain_cursor` rows
/// to its last key, and a bucket's last page sets its bit in its segment's
/// `drained_mask` (and the segment's last bucket flips it `drained`). A seal
/// adds or changes a `segments` row and a retire deletes one. The other two
/// parts cover the convergence conditions outside the segments: a 1-1
/// `JOIN`'s pending fan-outs (each pass moves `gen` or `cursor`, or deletes
/// the marker) and the keys held in quarantine.
///
/// Left out on purpose, because they move while a drain is stuck: claims'
/// `claimed_at` and `drainers.last_seen` (heartbeats refresh them), and WAL
/// position (any write moves it). The ring tables' row counts don't move
/// per page (a slot empties only when its segment retires). These tables
/// hold a few rows per segment in flight, so the sample is cheap.
pub(super) async fn progress_mark(raw: &tokio_postgres::Client) -> Option<String> {
    const MARK: &str = "select md5(concat_ws('|', \
         (select string_agg(concat_ws(':', seg_seq, state, drained_mask), ',' order by seg_seq) \
            from segments), \
         (select string_agg(concat_ws(':', seg_seq, bucket, after_route, after_src_table, after_key), \
                            ',' order by seg_seq, bucket) \
            from drain_cursor), \
         (select string_agg(concat_ws(':', transform_id, join_key::text, gen, cursor), \
                            ',' order by transform_id, join_key) \
            from join1_fan_out), \
         (select count(*) from poison_held)))";
    let rows = bounded_query(raw, MARK).await.ok()?;
    rows.into_iter().next()?.into_iter().next()?
}

/// Runs `sql` (one statement) under [`PROBE_TIMEOUT_MS`], as rows of
/// nullable text.
async fn bounded_query(
    raw: &tokio_postgres::Client,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, tokio_postgres::Error> {
    // `set_config(..., true)` lasts until the end of the implicit
    // transaction a multi-statement simple query runs in: this message only.
    let message =
        format!("select set_config('statement_timeout', '{PROBE_TIMEOUT_MS}', true); {sql}");
    let messages = raw.simple_query(&message).await?;
    Ok(messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .skip(1)
        .collect())
}

/// Prints what a timed-out quiesce was waiting on to stderr, one line per
/// row, each prefixed `QUIESCE_DUMP <part>:`: the ring (per-slot row counts
/// and how many gate `token`), the segments, claims, cursors and holdups,
/// the drainers' heartbeats, the definitions' status, quarantine and
/// pending fan-outs, and every other backend of this database with its
/// wait event and the locks it waits for. Each query is bounded by
/// [`PROBE_TIMEOUT_MS`]; one that fails prints its error instead.
pub(super) async fn dump_state(raw: &tokio_postgres::Client, token: Option<PgLsn>, reason: &str) {
    let gate = match token {
        // `PgLsn`'s `Display` is Postgres's own `X/Y` hex form.
        Some(token) => format!("origin_lsn is null or origin_lsn <= '{token}'::pg_lsn"),
        None => "origin_lsn is null".to_string(),
    };
    let mut parts: Vec<(String, String)> = (0..4)
        .map(|slot| {
            (
                format!("seg_{slot}"),
                format!(
                    "select count(*) as rows, count(*) filter (where {gate}) as gating, \
                            min(origin_lsn)::text, max(origin_lsn)::text \
                       from seg_{slot}"
                ),
            )
        })
        .collect();
    let fixed: [(&str, &str); 13] = [
        (
            "segment_pointer",
            "select active_seq, ring_slot from segment_pointer",
        ),
        (
            "segments",
            "select seg_seq, ring_slot, state, bucket_count, drained_mask, row_count, \
                    late_rows, has_truncate, now() - created_at as age, now() - sealed_at as sealed_for \
               from segments order by seg_seq",
        ),
        (
            "seg_claims",
            "select seg_seq, bucket, claimed_by, now() - claimed_at as claim_age \
               from seg_claims order by seg_seq, bucket",
        ),
        (
            "drain_cursor",
            "select seg_seq, bucket, after_route, after_src_table, after_key from drain_cursor order by 1, 2",
        ),
        (
            "drain_holdups",
            "select seg_seq, buckets, tables, error, attempts, now() - since as held_for from drain_holdups",
        ),
        (
            "drainers",
            "select drainer_id, now() - last_seen as since_heartbeat from drainers order by 1",
        ),
        (
            "definitions",
            "select id, target_table, status from transform_definitions order by id",
        ),
        (
            "poison_held",
            "select transform_id, count(*), count(*) filter (where origin_lsn is null) as no_origin \
               from poison_held group by 1 order by 1",
        ),
        (
            "join1_fan_out",
            "select transform_id, count(*), min(origin_lsn)::text, max(gen) from join1_fan_out group by 1 order by 1",
        ),
        (
            "activity",
            "select pid, application_name, state, wait_event_type, wait_event, \
                    now() - xact_start as xact_age, now() - state_change as state_age, \
                    left(regexp_replace(query, '\\s+', ' ', 'g'), 160) \
               from pg_stat_activity \
              where datname = current_database() and pid <> pg_backend_pid() \
              order by xact_start nulls last",
        ),
        (
            "locks_waiting",
            "select l.pid, l.locktype, l.mode, l.relation::regclass::text, l.transactionid::text, \
                    l.page, l.tuple, pg_blocking_pids(l.pid)::text as blocked_by \
               from pg_locks l where not l.granted order by l.pid",
        ),
        (
            "locks_strong",
            "select l.pid, l.mode, l.relation::regclass::text from pg_locks l \
              where l.granted and l.locktype = 'relation' and l.pid <> pg_backend_pid() \
                and l.mode in ('ShareLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock') \
              order by l.pid",
        ),
        (
            "locks_summary",
            "select locktype, mode, granted, count(*) from pg_locks \
              where pid <> pg_backend_pid() group by 1, 2, 3 order by 1, 2, 3",
        ),
    ];
    parts.extend(
        fixed
            .into_iter()
            .map(|(name, sql)| (name.to_string(), sql.to_string())),
    );
    eprintln!("QUIESCE_DUMP begin: {reason}");
    for (name, sql) in parts {
        match bounded_query(raw, &sql).await {
            Ok(rows) if rows.is_empty() => eprintln!("QUIESCE_DUMP {name}: (no rows)"),
            Ok(rows) => {
                for row in rows {
                    let cols: Vec<&str> =
                        row.iter().map(|c| c.as_deref().unwrap_or("NULL")).collect();
                    eprintln!("QUIESCE_DUMP {name}: {}", cols.join(" | "));
                }
            }
            Err(err) => match err.as_db_error() {
                Some(db) => eprintln!(
                    "QUIESCE_DUMP {name}: ERROR {}: {}",
                    db.code().code(),
                    db.message()
                ),
                None => eprintln!("QUIESCE_DUMP {name}: ERROR {err}"),
            },
        }
    }
    eprintln!("QUIESCE_DUMP end");
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: QuiesceBudget = QuiesceBudget {
        no_progress: Duration::from_secs(30),
        cap: Duration::from_secs(180),
    };

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn mark(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    /// With no sample ever differing, the deadline is the no-progress
    /// window from the start: a stuck quiesce fails exactly when a fixed
    /// 30 s budget did.
    #[test]
    fn without_progress_the_deadline_is_the_window_from_the_start() {
        let t0 = Instant::now();
        let mut deadline = ProgressDeadline::new(BUDGET, t0);
        for s in 1..30 {
            assert!(!deadline.observe(mark("same"), t0 + secs(s)));
            assert_eq!(deadline.expired(t0 + secs(s)), None);
        }
        assert_eq!(deadline.remaining(t0 + secs(29)), secs(1));
        assert_eq!(deadline.expired(t0 + secs(30)), Some(Expiry::NoProgress));
        assert_eq!(deadline.restarts(), 0);
        assert_eq!(deadline.window_budget(), secs(30));
    }

    /// The first sample is the baseline and a failed sample shows nothing:
    /// neither counts as progress.
    #[test]
    fn the_baseline_and_failed_samples_never_restart_the_window() {
        let t0 = Instant::now();
        let mut deadline = ProgressDeadline::new(BUDGET, t0);
        assert!(!deadline.observe(mark("a"), t0 + secs(1)));
        assert!(!deadline.observe(None, t0 + secs(2)));
        assert!(!deadline.observe(mark("a"), t0 + secs(3)));
        assert_eq!(deadline.window_start(), t0);
        assert_eq!(deadline.expired(t0 + secs(30)), Some(Expiry::NoProgress));
    }

    /// A changed mark restarts the window at the sample, and the deadline
    /// is then a full window after that sample, not after the start.
    #[test]
    fn progress_restarts_the_window_at_the_sample() {
        let t0 = Instant::now();
        let mut deadline = ProgressDeadline::new(BUDGET, t0);
        deadline.observe(mark("a"), t0 + secs(1));
        assert!(deadline.observe(mark("b"), t0 + secs(25)));
        assert_eq!(deadline.window_start(), t0 + secs(25));
        assert_eq!(deadline.expired(t0 + secs(54)), None);
        assert_eq!(deadline.expired(t0 + secs(55)), Some(Expiry::NoProgress));
        // A mark that changes back is progress too.
        assert!(deadline.observe(mark("a"), t0 + secs(40)));
        assert_eq!(deadline.restarts(), 2);
        assert_eq!(deadline.expired(t0 + secs(69)), None);
        assert_eq!(deadline.expired(t0 + secs(70)), Some(Expiry::NoProgress));
    }

    /// However often the ring advances, the quiesce ends at the cap.
    #[test]
    fn progress_never_extends_past_the_cap() {
        let t0 = Instant::now();
        let mut deadline = ProgressDeadline::new(BUDGET, t0);
        for s in 0..=179 {
            deadline.observe(Some(s.to_string()), t0 + secs(s));
            assert_eq!(deadline.expired(t0 + secs(s)), None, "at {s}s");
        }
        assert_eq!(deadline.remaining(t0 + secs(179)), secs(1));
        assert_eq!(deadline.expired(t0 + secs(180)), Some(Expiry::Cap));
        // The window the status waits get shrinks to what the cap leaves.
        assert_eq!(deadline.window_budget(), secs(1));
    }

    /// A cap below the window (a caller that spent most of its budget
    /// already) bounds even a quiesce that makes no progress.
    #[test]
    fn a_cap_shorter_than_the_window_bounds_the_first_window() {
        let t0 = Instant::now();
        let deadline = ProgressDeadline::new(
            QuiesceBudget {
                no_progress: secs(30),
                cap: secs(10),
            },
            t0,
        );
        assert_eq!(deadline.window_budget(), secs(10));
        assert_eq!(deadline.expired(t0 + secs(9)), None);
        assert_eq!(deadline.expired(t0 + secs(10)), Some(Expiry::Cap));
    }
}
