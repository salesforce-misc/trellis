//! Load generators against a scenario's source table. There are two, and
//! they answer different questions:
//!
//! - [`run_controlled_load`]: **one connection, precisely paced.** The
//!   latency ladder's generator, where the offered rate is an independent
//!   variable held low and steady, not the thing being measured. Its
//!   behaviour is frozen (issue #276: "existing paced scenarios —
//!   byte-identical behaviour"), because the latency battery's numbers are
//!   only comparable across runs while the instrument doesn't move.
//! - [`run_parallel_load`]: **N connections, paced on a shared schedule or
//!   unpaced** ([`Pace`]). The throughput generator (issue #276): able to
//!   offer more than the engine can absorb, so a throughput scenario measures
//!   the engine's ceiling rather than one connection's. Each commit is one
//!   prepared `INSERT ... SELECT FROM generate_series(..)` whose rows Postgres
//!   builds server-side, so the client does no per-row work and the
//!   statement costs the same to parse at 1 row as at 10,000.
//!
//! **Either generator can still be the limit, and the harness must say so
//! when it is.** A single connection saturates at a few tens of thousands of
//! commits/sec; N connections saturate somewhere far higher, but somewhere.
//! Whenever a scenario's achieved rate materially undershoots its target
//! while the engine kept up with everything it *was* offered, the number is a
//! statement about the generator, not about Trellis — [`generator_bound`] is
//! the one place that verdict is decided, and every scenario reports it next
//! to its achieved and target rates.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;

/// How far `achieved_rows_per_sec` may fall below the target before
/// [`generator_bound`] calls it a material undershoot: the same 2% the
/// scenarios' prose warnings used before issue #276 made it a verdict.
pub const GENERATOR_UNDERSHOOT_TOLERANCE: f64 = 0.02;

/// Issue #276's self-check: whether a result measured the *generator* rather
/// than the engine.
///
/// True when the generator materially undershot the offered target
/// (`achieved < target * (1 - GENERATOR_UNDERSHOOT_TOLERANCE)`) **and** the
/// engine still kept up with everything it was actually offered
/// (`engine_kept_up`: no backlog left, and — for the scenarios that fit an
/// in-window rate — processed at the achieved rate while it was offered, see
/// [`crate::streaming::rate::kept_up_with_offer`]). That combination means nothing on
/// the engine side ever pushed back, so the only thing the result bounds is
/// the generator.
///
/// `target_rows_per_sec: None` is a max-rate ([`Pace::Max`]) run — there is
/// no target to undershoot, because the generator was asked for everything it
/// had. There, the engine keeping up is *by itself* the generator-bound
/// verdict: it absorbed the generator's full output, so the engine's ceiling
/// is somewhere above the achieved rate, not at it.
///
/// An undershoot *with* a backlog is deliberately not generator-bound: the
/// engine failed to sustain even the lower rate it actually received, which
/// is a real (and conservative) engine measurement.
pub fn generator_bound(
    target_rows_per_sec: Option<f64>,
    achieved_rows_per_sec: f64,
    engine_kept_up: bool,
) -> bool {
    if !engine_kept_up {
        return false;
    }
    match target_rows_per_sec {
        Some(target) => achieved_rows_per_sec < target * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE),
        None => true,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LoadConfig {
    pub commits_per_sec: f64,
    pub rows_per_commit: usize,
    pub duration: Duration,
    /// When `Some(n)`, each row also carries a group key `id % n` in a `grp`
    /// column — the fold-in axis [`crate::streaming::fold_in`] sweeps. When
    /// `None`, rows are `(id, val)` only.
    pub groups: Option<usize>,
}

impl LoadConfig {
    /// A `(id, val)` 1-1 load.
    pub fn plain(commits_per_sec: f64, rows_per_commit: usize, duration: Duration) -> Self {
        Self {
            commits_per_sec,
            rows_per_commit,
            duration,
            groups: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LoadSummary {
    pub commits_issued: u64,
    pub rows_issued: u64,
    pub elapsed: Duration,
}

impl LoadSummary {
    pub fn achieved_rows_per_sec(&self) -> f64 {
        self.rows_issued as f64 / self.elapsed.as_secs_f64()
    }
}

/// Builds and runs one `INSERT` of `rows_per_commit` values against
/// `public.<source_table>`, starting at `*next_id` and advancing it past the
/// ids used. Postgres commits a single-statement `execute` as its own
/// implicit transaction, giving exactly the "N rows per commit" shape the
/// transaction-shape sweep wants with no explicit `BEGIN`/`COMMIT` round
/// trip.
async fn insert_batch(
    raw: &RawClient,
    source_table: &str,
    next_id: &mut i64,
    rows_per_commit: usize,
    groups: Option<usize>,
) {
    let mut values = Vec::with_capacity(rows_per_commit);
    for _ in 0..rows_per_commit {
        let id = *next_id;
        match groups {
            Some(groups) => values.push(format!("({id},{},{id})", id % groups as i64)),
            None => values.push(format!("({id},{id})")),
        }
        *next_id += 1;
    }
    let columns = match groups {
        Some(_) => "(id, grp, val)",
        None => "(id, val)",
    };
    let sql = format!(
        "insert into public.{source_table} {columns} values {}",
        values.join(",")
    );
    raw.execute(sql.as_str(), &[])
        .await
        .expect("insert load batch");
}

/// Offers `cfg` against `public.<source_table>` at a precise, controlled
/// rate, starting ids at `first_id` — callers reserve negative ids (see
/// [`super::chain::WARM_UP_ID`]) for out-of-band rows so they never collide
/// with, or get miscounted among, this generator's own.
///
/// Deliberately does not try to push as hard as the connection allows —
/// that's [`run_parallel_load`]'s job. This holds the rate steady and
/// self-throttles rather than bursting to catch up if a single insert
/// overruns one tick period (`MissedTickBehavior::Delay`), so a slow insert
/// shows up as a lower achieved rate instead of as a latency spike the engine
/// didn't cause.
pub async fn run_controlled_load(
    raw: &RawClient,
    source_table: &str,
    first_id: i64,
    cfg: &LoadConfig,
) -> LoadSummary {
    assert!(
        cfg.commits_per_sec > 0.0,
        "commits_per_sec must be positive"
    );
    assert!(
        cfg.rows_per_commit >= 1,
        "rows_per_commit must be at least 1"
    );

    let period = Duration::from_secs_f64(1.0 / cfg.commits_per_sec);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let start = Instant::now();
    let deadline = start + cfg.duration;
    let mut commits = 0u64;
    let mut next_id = first_id;

    loop {
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        insert_batch(
            raw,
            source_table,
            &mut next_id,
            cfg.rows_per_commit,
            cfg.groups,
        )
        .await;
        commits += 1;
    }

    LoadSummary {
        commits_issued: commits,
        rows_issued: commits * cfg.rows_per_commit as u64,
        elapsed: start.elapsed(),
    }
}

/// [`run_parallel_load`]'s writer count when a scenario isn't told otherwise.
///
/// Measured with `generator-reach` (no engine running) on the 16-core dev
/// box: 8 connections offer ~470k commits/sec at 1 row/commit and ~4.5M
/// rows/sec at 1,000 rows/commit — past both of issue #276's reach gates
/// (200k and 1M) with margin — and adding more barely moves the 1-row number
/// (16: ~490k, 32: ~520k) while taking more cores from the engine the
/// generator shares the box with.
pub const DEFAULT_CONNECTIONS: usize = 8;

/// How [`run_parallel_load`] paces its commits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Pace {
    /// Unpaced: every connection issues back-to-back commits for the whole
    /// window. The intake ceiling's mode, and the way to find the generator's
    /// own reach.
    Max,
    /// Offer this many rows/sec in aggregate, across every connection, on one
    /// shared schedule: commit `k` is due at `k * rows_per_commit / rate`
    /// seconds into the window, whichever connection is free to take it. A
    /// connection that falls behind (a slow insert) doesn't hold the schedule
    /// back — the next free connection issues the overdue commit
    /// immediately — so the achieved rate only undershoots when *every*
    /// connection is saturated, which is exactly what [`generator_bound`]
    /// then flags.
    RowsPerSec(f64),
}

/// A [`run_parallel_load`] configuration.
#[derive(Debug, Clone, Copy)]
pub struct ParallelLoad {
    /// Writer connections, each its own Postgres backend. Every one is
    /// connected (and its statement prepared) before the window opens, so
    /// connection setup never counts against the achieved rate.
    pub connections: usize,
    pub rows_per_commit: usize,
    pub duration: Duration,
    /// As [`LoadConfig::groups`]: `Some(n)` adds a `grp = id % n` column.
    pub groups: Option<usize>,
    pub pace: Pace,
}

/// When commit `k` (0-based) is due, as an offset from the window's start,
/// under [`Pace::RowsPerSec`]: `k` commits of `rows_per_commit` rows at
/// `rows_per_sec` take exactly this long to offer.
fn commit_due_offset(k: u64, rows_per_commit: usize, rows_per_sec: f64) -> Duration {
    Duration::from_secs_f64(k as f64 * rows_per_commit as f64 / rows_per_sec)
}

/// The inclusive id range commit `k` (0-based) inserts. Ranges are disjoint
/// and contiguous in `k`, so a run that issued commits `0..n` inserted exactly
/// the ids `first_id..first_id + n * rows_per_commit` — whichever connection
/// issued which commit, and in whatever order they landed.
fn commit_id_range(first_id: i64, k: u64, rows_per_commit: usize) -> (i64, i64) {
    let lo = first_id + (k * rows_per_commit as u64) as i64;
    (lo, lo + rows_per_commit as i64 - 1)
}

/// The one statement [`run_parallel_load`] prepares per connection. Rows
/// carry the same values [`insert_batch`] writes — `(id, val = id)`, or
/// `(id, grp = id % groups, val = id)` — but Postgres generates them from the
/// `$1..=$2` id range, so a commit costs one small bind regardless of its row
/// count. What CDC decodes is identical either way: the same row images in
/// one transaction per commit.
pub(crate) fn parallel_insert_sql(source_table: &str, groups: Option<usize>) -> String {
    match groups {
        Some(groups) => format!(
            "insert into public.{source_table} (id, grp, val) \
             select g, g % {groups}, g from generate_series($1::bigint, $2::bigint) g"
        ),
        None => format!(
            "insert into public.{source_table} (id, val) \
             select g, g from generate_series($1::bigint, $2::bigint) g"
        ),
    }
}

/// Offers `cfg` against `public.<source_table>` from `cfg.connections`
/// connections to `dsn`, with ids starting at `first_id` (as
/// [`run_controlled_load`]). Every commit is a single-statement implicit
/// transaction of exactly `cfg.rows_per_commit` rows.
///
/// Ids are handed out by commit index from one shared counter, so the rows
/// issued are always exactly `first_id..first_id + rows_issued` with no gaps
/// — a scenario's oracle and its `rows_issued` agree without having to know
/// how many connections there were.
pub async fn run_parallel_load(
    dsn: &str,
    source_table: &str,
    first_id: i64,
    cfg: &ParallelLoad,
) -> LoadSummary {
    assert!(cfg.connections >= 1, "connections must be at least 1");
    assert!(
        cfg.rows_per_commit >= 1,
        "rows_per_commit must be at least 1"
    );
    if let Pace::RowsPerSec(rate) = cfg.pace {
        assert!(rate > 0.0, "a paced rate must be positive");
    }

    let sql = parallel_insert_sql(source_table, cfg.groups);
    let mut writers = Vec::with_capacity(cfg.connections);
    for _ in 0..cfg.connections {
        let raw = connect_raw(dsn).await;
        let statement = raw
            .prepare(&sql)
            .await
            .expect("prepare parallel load insert");
        writers.push((raw, statement));
    }

    let next_commit = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let deadline = start + cfg.duration;
    let cfg = *cfg;

    let tasks: Vec<_> = writers
        .into_iter()
        .map(|(raw, statement)| {
            let next_commit = Arc::clone(&next_commit);
            tokio::spawn(async move {
                let mut issued = 0u64;
                loop {
                    // Check the wall clock *before* claiming, so every claimed
                    // commit is issued and the ids stay gapless. A paced run
                    // needs this too: one that has fallen behind its schedule
                    // would otherwise keep issuing overdue commits long after
                    // the window closed, rather than reporting the undershoot.
                    if Instant::now() >= deadline {
                        break;
                    }
                    let k = next_commit.fetch_add(1, Ordering::Relaxed);
                    if let Pace::RowsPerSec(rate) = cfg.pace {
                        // Paced: due times rise with `k`, so the first commit
                        // due past the deadline ends the run for every
                        // connection — every smaller `k` was claimed before
                        // the deadline and is issued.
                        let due = start + commit_due_offset(k, cfg.rows_per_commit, rate);
                        if due >= deadline {
                            break;
                        }
                        tokio::time::sleep_until(due.into()).await;
                    }
                    let (lo, hi) = commit_id_range(first_id, k, cfg.rows_per_commit);
                    raw.execute(&statement, &[&lo, &hi])
                        .await
                        .expect("insert parallel load batch");
                    issued += 1;
                }
                issued
            })
        })
        .collect();

    let mut commits = 0u64;
    for task in tasks {
        commits += task.await.expect("load writer task panicked");
    }

    LoadSummary {
        commits_issued: commits,
        rows_issued: commits * cfg.rows_per_commit as u64,
        elapsed: start.elapsed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_undershoot_the_engine_kept_up_with_is_generator_bound() {
        // The deliberately under-offered run: 400k asked for, 150k offered,
        // no backlog — nothing on the engine side ever pushed back.
        assert!(generator_bound(Some(400_000.0), 150_000.0, true));
    }

    #[test]
    fn hitting_the_target_is_an_engine_measurement() {
        assert!(!generator_bound(Some(400_000.0), 400_000.0, true));
        // Inside the tolerance is still "hit".
        assert!(!generator_bound(Some(400_000.0), 395_000.0, true));
        assert!(generator_bound(Some(400_000.0), 390_000.0, true));
    }

    #[test]
    fn an_undershoot_with_a_backlog_is_still_an_engine_measurement() {
        // The engine couldn't sustain even the lower rate it received: a real
        // (if conservative) "not sustained".
        assert!(!generator_bound(Some(400_000.0), 150_000.0, false));
    }

    #[test]
    fn a_max_rate_run_is_generator_bound_exactly_when_the_engine_kept_up() {
        assert!(generator_bound(None, 548_000.0, true));
        assert!(!generator_bound(None, 548_000.0, false));
    }

    #[test]
    fn commit_ids_are_contiguous_and_disjoint_across_commits() {
        assert_eq!(commit_id_range(1, 0, 1000), (1, 1000));
        assert_eq!(commit_id_range(1, 1, 1000), (1001, 2000));
        assert_eq!(commit_id_range(1, 0, 1), (1, 1));
        assert_eq!(commit_id_range(1, 7, 1), (8, 8));
    }

    #[test]
    fn the_shared_schedule_spaces_commits_at_the_aggregate_rate() {
        // 400k rows/sec in 200-row commits: 2000 commits/sec, one every 500us.
        assert_eq!(commit_due_offset(0, 200, 400_000.0), Duration::ZERO);
        assert_eq!(
            commit_due_offset(1, 200, 400_000.0),
            Duration::from_micros(500)
        );
        assert_eq!(
            commit_due_offset(2000, 200, 400_000.0),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn parallel_rows_match_the_controlled_generators_values() {
        assert_eq!(
            parallel_insert_sql("s", None),
            "insert into public.s (id, val) select g, g from generate_series($1::bigint, $2::bigint) g"
        );
        assert_eq!(
            parallel_insert_sql("s", Some(40)),
            "insert into public.s (id, grp, val) select g, g % 40, g from generate_series($1::bigint, $2::bigint) g"
        );
    }

    /// Runs [`run_parallel_load`] against a fresh cluster's `(id, val)` table
    /// and returns its summary plus the landed rows' `(count, min id, max id)`.
    fn run_against_postgres(cfg: ParallelLoad) -> (LoadSummary, (i64, i64, i64)) {
        tokio::runtime::Runtime::new()
            .expect("build tokio runtime")
            .block_on(async {
                let cluster = testkit::TestCluster::start();
                let db = cluster.create_isolated_database().await;
                let raw = connect_raw(db.dsn()).await;
                raw.batch_execute("create table public.gen (id bigint primary key, val numeric)")
                    .await
                    .expect("create table");
                let summary = run_parallel_load(db.dsn(), "gen", 1, &cfg).await;
                let row = raw
                    .query_one(
                        "select count(*), coalesce(min(id), 0), coalesce(max(id), 0) \
                         from public.gen where val = id",
                        &[],
                    )
                    .await
                    .expect("count landed rows");
                (summary, (row.get(0), row.get(1), row.get(2)))
            })
    }

    #[test]
    fn a_paced_parallel_run_offers_its_target_with_gapless_ids() {
        // 20k rows/sec in 100-row commits from 4 connections for 1s: well
        // inside one connection's reach, so it must hit the target.
        let (summary, (landed, min_id, max_id)) = run_against_postgres(ParallelLoad {
            connections: 4,
            rows_per_commit: 100,
            duration: Duration::from_secs(1),
            groups: None,
            pace: Pace::RowsPerSec(20_000.0),
        });
        assert_eq!(summary.rows_issued, summary.commits_issued * 100);
        assert_eq!(landed as u64, summary.rows_issued);
        assert_eq!((min_id, max_id), (1, summary.rows_issued as i64));
        // Exactly the commits due inside the window: 20,000 rows.
        assert_eq!(summary.rows_issued, 20_000);
        assert!(
            !generator_bound(Some(20_000.0), summary.achieved_rows_per_sec(), true),
            "a run that hit its target is not generator-bound: {summary:?}"
        );
    }

    #[test]
    fn a_deliberately_under_offered_run_is_flagged_generator_bound() {
        // One connection, one row per commit, asked for 10M rows/sec: no
        // connection gets near that, and with nothing downstream to fall
        // behind, the self-check must call it generator-bound.
        let (summary, (landed, min_id, max_id)) = run_against_postgres(ParallelLoad {
            connections: 1,
            rows_per_commit: 1,
            duration: Duration::from_millis(500),
            groups: None,
            pace: Pace::RowsPerSec(10_000_000.0),
        });
        assert_eq!(landed as u64, summary.rows_issued);
        assert_eq!((min_id, max_id), (1, summary.rows_issued as i64));
        assert!(
            summary.achieved_rows_per_sec() < 10_000_000.0 * 0.5,
            "the premise: the generator can't offer this: {summary:?}"
        );
        // A generator behind its schedule still stops when the window
        // closes — it must not keep issuing overdue commits until it has
        // offered the whole 5M-row target.
        assert!(
            summary.elapsed < Duration::from_secs(5),
            "the run must end with its window: {summary:?}"
        );
        assert!(generator_bound(
            Some(10_000_000.0),
            summary.achieved_rows_per_sec(),
            true
        ));
    }

    #[test]
    fn a_max_rate_parallel_run_lands_every_row_it_reports() {
        let (summary, (landed, min_id, max_id)) = run_against_postgres(ParallelLoad {
            connections: 4,
            rows_per_commit: 10,
            duration: Duration::from_millis(500),
            groups: None,
            pace: Pace::Max,
        });
        assert!(summary.commits_issued > 0);
        assert_eq!(landed as u64, summary.rows_issued);
        assert_eq!((min_id, max_id), (1, summary.rows_issued as i64));
    }
}
