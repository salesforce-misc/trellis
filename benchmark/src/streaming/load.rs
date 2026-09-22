//! Load generators against a scenario's source table: a
//! controlled-offered-rate one for the latency and ramp scenarios (where the
//! offered rate is an independent variable, not the thing being measured) and
//! a max-rate one for the intake ceiling, which pushes as fast as one
//! connection allows instead of holding a rate.
//!
//! **The generator is itself a measurement limit, and every caller must
//! report it as one.** Both generators here are a single connection issuing
//! sequential awaited `execute` calls. Past a few tens of thousands of
//! commits/sec that connection saturates before the engine does, at which
//! point `achieved_rows_per_sec` undershoots the target and a "sustained"
//! verdict says more about the generator than about Trellis. Every result
//! struct in this module's callers therefore carries the achieved rate next
//! to the target, never the target alone. (#269's own child #276 exists to
//! replace this with a generator that can offer T3's 400k rows/sec; until
//! then the ceiling numbers here are floors.)

use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

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
/// that's [`run_max_rate_load`]'s job. This holds the rate steady and
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

/// Runs back-to-back `INSERT`s of `rows_per_commit` values each, unpaced, for
/// `duration` — as many commits as one connection can physically push. The
/// achieved rate this reports is a *floor* on the engine's ceiling, not a
/// measurement of it: see this module's doc comment.
pub async fn run_max_rate_load(
    raw: &RawClient,
    source_table: &str,
    first_id: i64,
    rows_per_commit: usize,
    duration: Duration,
) -> LoadSummary {
    assert!(rows_per_commit >= 1, "rows_per_commit must be at least 1");

    let start = Instant::now();
    let deadline = start + duration;
    let mut commits = 0u64;
    let mut next_id = first_id;

    while Instant::now() < deadline {
        insert_batch(raw, source_table, &mut next_id, rows_per_commit, None).await;
        commits += 1;
    }

    LoadSummary {
        commits_issued: commits,
        rows_issued: commits * rows_per_commit as u64,
        elapsed: start.elapsed(),
    }
}
