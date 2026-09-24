//! Single-hop throughput scenarios: the ramp (#266's B2, epic #269's
//! **V-THRU** / target T2) and the transaction-shape sweep (#266's B4, #269's
//! **V-SHAPE**). Both go through the same [`run_probe`] — the ramp fixes the
//! commit cadence and derives `rows_per_commit` from the target rate, the
//! shape sweep fixes `rows_per_commit` (the axis it's sweeping) and derives
//! the cadence instead.
//!
//! ## What "sustained" means here, and what it doesn't
//!
//! T2 (#266) defines sustained as "held for >= 10 minutes with ring depth and
//! replication lag both flat". A ramp searching several candidate rates at 10
//! minutes each is a multi-hour run before it has found anything. Each probe
//! here instead offers load for a short window and checks whether the backlog
//! it created (`rows offered - rows applied`) fully drains within a bounded
//! grace period. That is a sound *proxy* — a rate whose backlog won't drain in
//! a bounded grace after a short window certainly won't hold for 10 minutes —
//! but it is not a T2 confirmation. Once the ramp identifies a knee,
//! confirming it over T2's real window is a separate, longer run.
//!
//! Load comes from the multi-connection generator
//! ([`crate::streaming::load::run_parallel_load`]) paced at the target on a
//! shared schedule, so a high target is actually offered rather than capped
//! by one connection. Every probe still reports `generator_bound`
//! ([`generator_bound`]): a `sustained: true` whose achieved rate undershot
//! the target says nothing about the engine at that target.

use std::time::{Duration, Instant};

use testkit::TestCluster;

use crate::scenario::connect_raw;
use crate::streaming::chain::{
    ChainOracle, check_chain_oracle, create_chain_source_table, install_chain_hops,
    wait_for_catch_up_discharged, wait_for_chain_live, warm_up,
};
use crate::streaming::load::{Pace, ParallelLoad, generator_bound, run_parallel_load};
use crate::streaming::scrape::{
    CHANGES_APPLIED_METRIC, END_TO_END_LATENCY_METRIC, HistogramSnapshot, LE_MAX, LE_P50, LE_P99,
    T1_BOUNDS, counter_value, scrape,
};
use crate::streaming::tuning::EngineTuning;

/// Drain parallelism for the throughput scenarios: more than the latency
/// ladder's, because #266's H4 expects `SEG_BUCKETS = 8` worth of useful
/// claim parallelism per batch and these scenarios are measuring throughput,
/// not per-hop latency.
pub const THROUGHPUT_APPLICATION_THREADS: usize = 8;

/// The ramp's fixed generator cadence. The ramp varies `rows_per_commit` to
/// hit each target rate so the sweep isn't confounded by a changing commit
/// rate — transaction shape is the *other* scenario's axis.
pub const RAMP_COMMITS_PER_SEC: f64 = 50.0;

/// The three commit shapes #266's B4 names.
pub const DEFAULT_SHAPES: &[usize] = &[1, 100, 10_000];

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

/// One probe: a target rate delivered with one specific transaction shape.
#[derive(Debug)]
pub struct ThroughputProbe {
    pub target_rows_per_sec: f64,
    pub rows_per_commit: usize,
    pub commits_per_sec: f64,
    /// Generator writer connections.
    pub connections: usize,
    pub offered_duration_secs: f64,
    pub rows_issued: u64,
    /// What the generator actually managed — compare against
    /// `target_rows_per_sec` before reading anything else here.
    pub achieved_rows_per_sec: f64,
    /// Issue #276's self-check: the generator undershot the target and the
    /// engine drained everything it did get, so this probe measured the
    /// generator. Its `sustained` says nothing about the engine at
    /// `target_rows_per_sec`.
    pub generator_bound: bool,
    pub changes_applied: u64,
    pub backlog_after_grace: i64,
    pub sustained: bool,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
    pub e2e_max_frac: f64,
    pub oracle: ChainOracle,
}

impl ThroughputProbe {
    pub fn to_json(&self, scenario: &str) -> String {
        format!(
            "{{\"scenario\":\"{}\",\"target_rows_per_sec\":{},\"rows_per_commit\":{},\
             \"commits_per_sec\":{:.2},\"connections\":{},\"offered_duration_secs\":{},\
             \"rows_issued\":{},\"achieved_rows_per_sec\":{:.1},\"generator_bound\":{},\
             \"changes_applied\":{},\
             \"backlog_after_grace\":{},\"sustained\":{},\"e2e_count\":{},\
             \"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4},\
             \"e2e_max_bucket_frac\":{:.4},\"oracle_ok\":{},\"oracle_source_rows\":{},\
             \"oracle_terminal_rows\":{},\"oracle_mismatched_rows\":{},\
             \"oracle_fully_converged\":{}}}",
            scenario,
            self.target_rows_per_sec,
            self.rows_per_commit,
            self.commits_per_sec,
            self.connections,
            self.offered_duration_secs,
            self.rows_issued,
            self.achieved_rows_per_sec,
            self.generator_bound,
            self.changes_applied,
            self.backlog_after_grace,
            self.sustained,
            self.e2e_count,
            self.e2e_p50_bucket_frac,
            self.e2e_p99_bucket_frac,
            self.e2e_max_frac,
            self.oracle.ok(),
            self.oracle.source_rows,
            self.oracle.terminal_rows,
            self.oracle.mismatched_rows,
            self.oracle.fully_converged,
        )
    }
}

/// How a probe's load is offered — the generator's writer count, the offer
/// window, and the drain grace after it — shared by every probe in a ramp or
/// sweep (and by [`crate::streaming::fold_in`]'s).
#[derive(Debug, Clone, Copy)]
pub struct Offer {
    pub connections: usize,
    pub duration: Duration,
    pub grace: Duration,
}

/// Runs one probe against a fresh, isolated single-hop chain: offers
/// `rows_per_commit`-shaped commits at `commits_per_sec` (target rate =
/// their product) for `offer.duration`, waits up to `offer.grace` for the
/// backlog to drain, and reports whether it did plus the latency fractions
/// observed over the window.
pub async fn run_probe(
    rows_per_commit: usize,
    commits_per_sec: f64,
    offer: Offer,
    tuning: &EngineTuning,
) -> ThroughputProbe {
    let target_rows_per_sec = commits_per_sec * rows_per_commit as f64;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let source = create_chain_source_table(&raw, "shp").await;
    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");

    let chain = install_chain_hops(&db.pool, &source, 1).await;
    wait_for_chain_live(&raw, &chain, SETUP_TIMEOUT).await;
    wait_for_catch_up_discharged(
        &raw,
        Instant::now() + SETUP_TIMEOUT + tuning.reconcile_interval,
    )
    .await;
    warm_up(&raw, &chain, SETUP_TIMEOUT).await;

    let terminal = chain.terminal();
    let before = scrape();
    let e2e_baseline =
        HistogramSnapshot::capture(&before, END_TO_END_LATENCY_METRIC, terminal, &T1_BOUNDS);
    let changes_before = counter_value(&before, CHANGES_APPLIED_METRIC, terminal);

    let load = run_parallel_load(
        db.dsn(),
        &chain.source,
        1,
        &ParallelLoad {
            connections: offer.connections,
            rows_per_commit,
            duration: offer.duration,
            groups: None,
            pace: Pace::RowsPerSec(target_rows_per_sec),
        },
    )
    .await;

    // Wait for the backlog to drain, polling the cheap counter for progress
    // and confirming completion with an authoritative row count.
    //
    // Neither signal works alone here:
    //
    // * `trellis_changes_applied_total` counts staged rows (#409), which
    //   matches rows committed only while nothing stages a source row a second
    //   time. A catch-up backfill does exactly that: its discharge stages every
    //   row the source holds as a `Recompute`. Setup waits out the one that
    //   going live parks (see `wait_for_catch_up_discharged`), and with it
    //   gone the counter has measured exact. But if one ran mid-window, the
    //   counter would reach `rows_issued` with rows still in flight and end
    //   the grace period early. Issue #423 measured that before setup waited:
    //   at 20k rows/sec in 10,000-row commits the counter read 500,001 against
    //   400,000 committed rows.
    // * a `count(*)` every poll is exact but, at the millions-of-rows end of
    //   the ramp, a repeated seq scan competes with the drain it is watching
    //   and changes the answer. Measured at 200k rows/sec: polling the count
    //   every 500 ms turned a rate that otherwise drains fully into a 1.08M
    //   row backlog.
    //
    // So: poll the counter (no scan), and only once it claims completion spend
    // one row count to confirm — at most one per `CONFIRM_INTERVAL` if the
    // counter is running ahead, rather than one per poll.
    const CONFIRM_INTERVAL: Duration = Duration::from_secs(2);
    let expected_rows = load.rows_issued as i64 + 1; // + the warm-up row
    let grace_deadline = Instant::now() + offer.grace;
    let mut next_confirm = Instant::now();
    while Instant::now() < grace_deadline {
        let applied = counter_value(&scrape(), CHANGES_APPLIED_METRIC, terminal)
            .saturating_sub(changes_before);
        if applied >= load.rows_issued && Instant::now() >= next_confirm {
            let landed: i64 = raw
                .query_one(&format!("select count(*) from public.{terminal}"), &[])
                .await
                .expect("count terminal rows while draining")
                .get(0);
            if landed >= expected_rows {
                break;
            }
            next_confirm = Instant::now() + CONFIRM_INTERVAL;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let after = scrape();
    let changes_applied =
        counter_value(&after, CHANGES_APPLIED_METRIC, terminal).saturating_sub(changes_before);
    let window =
        HistogramSnapshot::capture(&after, END_TO_END_LATENCY_METRIC, terminal, &T1_BOUNDS)
            .since(&e2e_baseline);
    let oracle = check_chain_oracle(&raw, &chain).await;

    client.shutdown().await.expect("client shutdown");

    // The backlog comes from the target table's own row count, not from
    // `changes_applied`: the counter is an in-process tally that a re-staged
    // row can push past rows committed (see the drain loop above), which
    // would understate the backlog and could report a saturated rate as
    // sustained. A row count can't overcount. `oracle.terminal_rows`
    // includes the warm-up row, which the generator's `rows_issued` doesn't.
    let landed = oracle.terminal_rows - 1;
    let backlog = load.rows_issued as i64 - landed;
    let achieved_rows_per_sec = load.achieved_rows_per_sec();
    ThroughputProbe {
        target_rows_per_sec,
        rows_per_commit,
        commits_per_sec,
        connections: offer.connections,
        offered_duration_secs: offer.duration.as_secs_f64(),
        rows_issued: load.rows_issued,
        achieved_rows_per_sec,
        generator_bound: generator_bound(
            Some(target_rows_per_sec),
            achieved_rows_per_sec,
            backlog <= 0,
        ),
        changes_applied,
        backlog_after_grace: backlog,
        sustained: backlog <= 0,
        e2e_count: window.count,
        e2e_p50_bucket_frac: window.fraction(LE_P50),
        e2e_p99_bucket_frac: window.fraction(LE_P99),
        e2e_max_frac: window.fraction(LE_MAX),
        oracle,
    }
}

/// Ramps through `candidate_rates` (ascending) until a probe fails to drain
/// its backlog within `grace`, or the list is exhausted. Returns every probe
/// run, in order: the last `sustained: true` entry is the candidate knee, the
/// first `false` one the rate that broke it.
pub async fn run_ramp(
    candidate_rates: &[f64],
    offer: Offer,
    tuning: &EngineTuning,
) -> Vec<ThroughputProbe> {
    let mut probes = Vec::new();
    for &target_rows_per_sec in candidate_rates {
        let rows_per_commit =
            ((target_rows_per_sec / RAMP_COMMITS_PER_SEC).round() as usize).max(1);
        let probe = run_probe(rows_per_commit, RAMP_COMMITS_PER_SEC, offer, tuning).await;
        let sustained = probe.sustained;
        probes.push(probe);
        if !sustained {
            break;
        }
    }
    probes
}

/// Runs one probe per `rows_per_commit` in `shapes`, all at the same
/// `target_rows_per_sec` — every probe's `commits_per_sec` differs
/// (`target_rows_per_sec / rows_per_commit`), which is the whole point: 1
/// row/commit at a high aggregate rate means a very high *commit* rate, and
/// this measures what that costs relative to a few large commits.
///
/// Unlike [`run_ramp`] this does not stop at the first failure — the shapes
/// aren't ordered by difficulty, and the 1-row/commit case failing says
/// nothing about the 10,000-row one.
pub async fn run_shape_sweep(
    shapes: &[usize],
    target_rows_per_sec: f64,
    offer: Offer,
    tuning: &EngineTuning,
) -> Vec<ThroughputProbe> {
    let mut probes = Vec::with_capacity(shapes.len());
    for &rows_per_commit in shapes {
        probes.push(
            run_probe(
                rows_per_commit,
                target_rows_per_sec / rows_per_commit as f64,
                offer,
                tuning,
            )
            .await,
        );
    }
    probes
}
