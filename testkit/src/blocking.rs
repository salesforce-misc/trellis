//! Whether one session was stuck behind another's lock, read from the
//! server's lock table rather than from how long the session's statements
//! took (issue #893).
//!
//! The user-table DDL tests (ADR-0002 I6) hold a lock the DDL needs for a
//! few seconds while a writer writes the table throughout. A DDL attempt that
//! waits for the lock makes every writer that arrives meanwhile queue behind
//! it, so the property is that each attempt gives up quickly, letting the
//! queue drain, rather than waiting out the whole hold. Timing the writer's
//! inserts tells those apart only on an idle box: under CPU load an insert
//! that never queued can take most of a second of wall time.
//!
//! [`watch_blocked`] asks Postgres instead. It samples
//! `pg_blocking_pids(writer)` throughout the hold and keys each blocker by
//! its backend and virtual transaction id, so it can tell one DDL attempt
//! from the next: every attempt is a transaction of its own. A writer that
//! queues behind a short attempt is blocked by a transaction that is gone
//! within one `lock_timeout`, while a writer that queues behind an attempt
//! waiting out the hold is blocked by the *same* transaction from the
//! hold's start to its end. [`BlockedWatch::longest`] is the longest any one
//! transaction was seen blocking the writer, and a test bounds it well below
//! the hold.
//!
//! Short attempts alone aren't enough: attempts retried back to back would
//! each be brief and still keep the writer queued nearly all the time
//! (issue #911). So a test also bounds [`BlockedWatch::blocked_fraction`],
//! the share of samples that found the writer blocked, below half. With the
//! real retry interval that share is about a fifth (a 50 ms `lock_timeout`
//! per 250 ms cycle); with no interval it is two thirds or more.
//! [`BlockedWatch::assert_brief_blocks`] checks both. A slow observer takes
//! fewer samples, but a sample's timing doesn't depend on whether the writer
//! is blocked, so the share stays an estimate of the time blocked. It only
//! gets noisy if samples grow as far apart as the retry cycle itself, which
//! even a heavily loaded box doesn't come near.
//!
//! The span is a lower bound: from when the first sample that saw the
//! blocker *returned* to when the last one was *issued*. A slow observer
//! (fewer samples, late replies) only shortens it, so CPU load can't turn a
//! short attempt into a long span; only a blocker that really lasted that
//! long produces one.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use tokio_postgres::Client;

/// How often [`watch_blocked`] samples the watched session's blockers.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(10);

/// One transaction that blocked the watched session.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blocker {
    /// The blocking backend's pid.
    pub pid: i32,
    /// Its virtual transaction id at the time, which tells one of its
    /// transactions from the next.
    pub vxid: String,
}

impl fmt::Display for Blocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pid {} transaction {}", self.pid, self.vxid)
    }
}

/// The transactions blocking `pid` right now: those holding a lock that
/// conflicts with the one it waits for, and those queued ahead of it for
/// one (`pg_blocking_pids`' hard and soft blocks).
pub async fn blockers_of(observer: &Client, pid: i32) -> Vec<Blocker> {
    observer
        .query(
            "select distinct l.pid, l.virtualtransaction \
             from pg_catalog.pg_locks l \
             where l.pid = any(pg_catalog.pg_blocking_pids($1)) \
               and l.virtualtransaction is not null",
            &[&pid],
        )
        .await
        .expect("read the session's blockers")
        .iter()
        .map(|row| Blocker {
            pid: row.get(0),
            vxid: row.get(1),
        })
        .collect()
}

/// What [`watch_blocked`] saw.
#[derive(Debug)]
pub struct BlockedWatch {
    /// How long it watched.
    pub watched: Duration,
    /// How many samples it took, and how many of them found the session
    /// blocked.
    pub samples: usize,
    pub blocked_samples: usize,
    /// Each transaction seen blocking the session, with the span it was seen
    /// doing so (a lower bound, see the [module docs](self)).
    pub spans: BTreeMap<Blocker, Duration>,
}

impl BlockedWatch {
    /// The transaction seen blocking the session the longest, and for how
    /// long, or `None` if nothing ever blocked it.
    pub fn longest(&self) -> Option<(&Blocker, Duration)> {
        self.spans
            .iter()
            .max_by_key(|(_, span)| **span)
            .map(|(blocker, span)| (blocker, *span))
    }

    /// The share of samples that found the session blocked, from 0 to 1
    /// (0 if no sample was taken).
    pub fn blocked_fraction(&self) -> f64 {
        if self.samples == 0 {
            return 0.0;
        }
        self.blocked_samples as f64 / self.samples as f64
    }

    /// Asserts that, over a watch of a `hold`, nothing kept the session
    /// blocked for long: no one transaction blocked it for half the hold or
    /// more, and it was blocked in fewer than half the samples. A watch that
    /// took no sample proves neither, so it fails too ([`watch_blocked`]
    /// always takes at least one over a nonzero hold). `what` names the DDL
    /// in the failure message.
    #[track_caller]
    pub fn assert_brief_blocks(&self, hold: Duration, what: &str) {
        assert!(self.samples > 0, "{what}: the watch took no sample: {self}");
        if let Some((blocker, span)) = self.longest() {
            assert!(
                span < hold / 2,
                "{what}: one transaction ({blocker}) blocked the writer for {span:?} of a \
                 {hold:?} hold: {self}"
            );
        }
        assert!(
            self.blocked_fraction() < 0.5,
            "{what}: the writer was blocked in half the samples or more: {self}"
        );
    }
}

impl fmt::Display for BlockedWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} of {} samples ({:.1}%) over {:?} blocked, by {} transactions",
            self.blocked_samples,
            self.samples,
            100.0 * self.blocked_fraction(),
            self.watched,
            self.spans.len()
        )?;
        if let Some((blocker, span)) = self.longest() {
            write!(f, "; the longest, {blocker}, for at least {span:?}")?;
        }
        Ok(())
    }
}

/// When `blocker` was first and last seen blocking the watched session.
struct Seen {
    /// When the first sample that saw it returned.
    first_returned: Instant,
    /// When the last sample that saw it was issued.
    last_issued: Instant,
}

/// Samples which transactions block `pid` every few milliseconds for
/// `during`, the span of a known hold, and returns how long each was seen
/// blocking it. Takes `during` whatever it sees: a fixed window, not a wait
/// for anything to converge.
pub async fn watch_blocked(observer: &Client, pid: i32, during: Duration) -> BlockedWatch {
    let started = Instant::now();
    let mut seen: BTreeMap<Blocker, Seen> = BTreeMap::new();
    let mut samples = 0;
    let mut blocked_samples = 0;
    while started.elapsed() < during {
        let issued = Instant::now();
        let blockers = blockers_of(observer, pid).await;
        let returned = Instant::now();
        samples += 1;
        if !blockers.is_empty() {
            blocked_samples += 1;
        }
        for blocker in blockers {
            seen.entry(blocker)
                .and_modify(|s| s.last_issued = issued)
                .or_insert(Seen {
                    first_returned: returned,
                    last_issued: issued,
                });
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
    BlockedWatch {
        watched: started.elapsed(),
        samples,
        blocked_samples,
        spans: seen
            .into_iter()
            .map(|(blocker, s)| {
                let span = s.last_issued.saturating_duration_since(s.first_returned);
                (blocker, span)
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOLD: Duration = Duration::from_secs(2);

    fn watch(samples: usize, blocked_samples: usize, longest_ms: u64) -> BlockedWatch {
        let mut spans = BTreeMap::new();
        if blocked_samples > 0 {
            spans.insert(
                Blocker {
                    pid: 1,
                    vxid: "3/7".into(),
                },
                Duration::from_millis(longest_ms),
            );
        }
        BlockedWatch {
            watched: HOLD,
            samples,
            blocked_samples,
            spans,
        }
    }

    #[test]
    fn brief_blocks_a_fifth_of_the_time_pass() {
        watch(175, 32, 45).assert_brief_blocks(HOLD, "ddl");
        watch(175, 0, 0).assert_brief_blocks(HOLD, "ddl");
    }

    #[test]
    #[should_panic(expected = "half the samples or more")]
    fn brief_blocks_half_the_time_fail() {
        watch(176, 88, 45).assert_brief_blocks(HOLD, "ddl");
    }

    #[test]
    #[should_panic(expected = "one transaction (pid 1 transaction 3/7) blocked the writer")]
    fn one_block_for_half_the_hold_fails() {
        watch(175, 30, 1_000).assert_brief_blocks(HOLD, "ddl");
    }

    #[test]
    #[should_panic(expected = "the watch took no sample")]
    fn a_watch_with_no_sample_fails() {
        watch(0, 0, 0).assert_brief_blocks(HOLD, "ddl");
    }
}
