//! The hop-depth latency ladder — issue #266's B1, epic #269's **V-LAT**.
//!
//! Depths 1/2/3/5/10 of plain 1-1 transforms at a low offered rate, reporting
//! T1's boundary-fraction evaluation per depth plus `per_hop_mean_ms` for
//! every hop. This is both the T1 measurement and the direct test of H1 —
//! whether the seal cadence (the only place a claimable batch is ever created,
//! `client.rs`'s `maintenance_loop`) puts a hard floor under per-hop latency
//! roughly 6x T1's ~50 ms-per-hop budget.

use std::time::{Duration, Instant};

use testkit::TestCluster;

use crate::scenario::connect_raw;
use crate::streaming::chain::{
    ChainOracle, check_chain_oracle, create_chain_source_table, install_chain_hops,
    wait_for_chain_live, warm_up,
};
use crate::streaming::load::{LoadConfig, generator_bound, run_controlled_load};
use crate::streaming::scrape::{
    CHANGES_APPLIED_METRIC, END_TO_END_LATENCY_METRIC, HistogramSnapshot, T1_BOUNDS, T1Evaluation,
    counter_value, per_hop_baseline, per_hop_json, per_hop_mean_ms, scrape,
};
use crate::streaming::tuning::EngineTuning;

/// The depths #266's ladder and #269's V-LAT both name.
pub const DEFAULT_DEPTHS: &[usize] = &[1, 2, 3, 5, 10];

/// How long a hop's chain gets to reach `live` and to prove it's flowing
/// end to end before the measurement window opens.
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

/// One depth's full measurement. Hand-rolled `to_json`, matching
/// [`crate::scenario::BenchResult`]'s convention (every field is an
/// int/float/bool, or a literal name needing no escaping).
#[derive(Debug)]
pub struct HopLatencyResult {
    pub depth: usize,
    pub maintenance_interval_ms: u64,
    pub poll_interval_ms: u64,
    pub reconcile_interval_ms: u64,
    pub application_threads: usize,
    pub offered_commits_per_sec: f64,
    pub duration_secs: f64,
    pub commits_issued: u64,
    pub rows_issued: u64,
    pub actual_elapsed_secs: f64,
    /// `trellis_changes_applied_total` for the terminal hop over the window.
    pub changes_applied_terminal: u64,
    /// The #266 metric sanity check: did the terminal transform apply at
    /// least as many changes as the generator committed rows? A latency
    /// number that looks fast because most changes were never observed fails
    /// here rather than passing quietly.
    pub changes_match_committed: bool,
    /// Issue #276's self-check ([`generator_bound`]): the single-connection
    /// generator undershot `offered_commits_per_sec` while the chain still
    /// converged. At the ladder's low default rate this should never fire; if
    /// it does, the depth was measured at a lower rate than it claims.
    pub generator_bound: bool,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
    pub e2e_max_under_1s: bool,
    pub t1_p50_pass: bool,
    pub t1_p99_pass: bool,
    pub t1_all_pass: bool,
    /// Exact mean latency (ms), cumulative from source commit, for *every*
    /// hop — not just the terminal one (#268). `None` for a hop with no
    /// samples. Index 0 is hop 1.
    pub per_hop_mean_ms: Vec<Option<f64>>,
    /// The mean per-hop *increment*: `(hop N mean - hop 1 mean) / (N - 1)`.
    /// This is the number #268 reports as "per-hop mean" and the one #270's
    /// expected bracket (300-315 ms at the stock tick) is stated against.
    /// `None` for depth 1, or if either endpoint has no samples.
    pub per_hop_increment_ms: Option<f64>,
    pub oracle: ChainOracle,
}

impl HopLatencyResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"hop-ladder\",\"depth\":{},\"maintenance_interval_ms\":{},\
             \"poll_interval_ms\":{},\"reconcile_interval_ms\":{},\"application_threads\":{},\
             \"offered_commits_per_sec\":{},\"duration_secs\":{},\"commits_issued\":{},\
             \"rows_issued\":{},\"actual_elapsed_secs\":{:.3},\"changes_applied_terminal\":{},\
             \"changes_match_committed\":{},\"generator_bound\":{},\"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\
             \"e2e_p99_bucket_frac\":{:.4},\"e2e_max_under_1s\":{},\"t1_p50_pass\":{},\
             \"t1_p99_pass\":{},\"t1_all_pass\":{},\"per_hop_mean_ms\":[{}],\
             \"per_hop_increment_ms\":{},\"oracle_ok\":{},\"oracle_source_rows\":{},\
             \"oracle_terminal_rows\":{},\"oracle_mismatched_rows\":{},\
             \"oracle_fully_converged\":{}}}",
            self.depth,
            self.maintenance_interval_ms,
            self.poll_interval_ms,
            self.reconcile_interval_ms,
            self.application_threads,
            self.offered_commits_per_sec,
            self.duration_secs,
            self.commits_issued,
            self.rows_issued,
            self.actual_elapsed_secs,
            self.changes_applied_terminal,
            self.changes_match_committed,
            self.generator_bound,
            self.e2e_count,
            self.e2e_p50_bucket_frac,
            self.e2e_p99_bucket_frac,
            self.e2e_max_under_1s,
            self.t1_p50_pass,
            self.t1_p99_pass,
            self.t1_all_pass,
            per_hop_json(&self.per_hop_mean_ms),
            match self.per_hop_increment_ms {
                Some(ms) => format!("{ms:.3}"),
                None => "null".to_string(),
            },
            self.oracle.ok(),
            self.oracle.source_rows,
            self.oracle.terminal_rows,
            self.oracle.mismatched_rows,
            self.oracle.fully_converged,
        )
    }
}

/// `(hop N mean - hop 1 mean) / (N - 1)` — the per-hop increment #268
/// reports. Uses the first and last hop with samples, so a run where an
/// intermediate hop happened to observe nothing still yields a number rather
/// than `None`.
fn increment_ms(per_hop: &[Option<f64>]) -> Option<f64> {
    let mut present = per_hop
        .iter()
        .enumerate()
        .filter_map(|(i, v)| v.map(|ms| (i, ms)));
    let (first_i, first_ms) = present.next()?;
    let (last_i, last_ms) = present.next_back()?;
    Some((last_ms - first_ms) / (last_i - first_i) as f64)
}

/// Runs one depth against a fresh, isolated cluster: builds a `depth`-hop 1-1
/// chain through the real front door, starts a real streaming client, proves
/// the chain is flowing, then offers `commits_per_sec` for `duration` and
/// evaluates T1 against the terminal hop's end-to-end histogram — diffed
/// against a pre-load baseline scrape (see [`crate::streaming::scrape`]).
pub async fn run_depth(
    depth: usize,
    commits_per_sec: f64,
    duration: Duration,
    tuning: &EngineTuning,
) -> HopLatencyResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let source = create_chain_source_table(&raw, &format!("d{depth}")).await;
    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");

    let chain = install_chain_hops(&db.pool, &source, depth).await;
    wait_for_chain_live(&raw, &chain, SETUP_TIMEOUT).await;
    warm_up(&raw, &chain, SETUP_TIMEOUT).await;

    let terminal = chain.terminal();
    let before = scrape();
    let e2e_baseline =
        HistogramSnapshot::capture(&before, END_TO_END_LATENCY_METRIC, terminal, &T1_BOUNDS);
    let changes_before = counter_value(&before, CHANGES_APPLIED_METRIC, terminal);
    let hop_baseline = per_hop_baseline(&before, &chain.hops);

    let load = run_controlled_load(
        &raw,
        &chain.source,
        1,
        &LoadConfig::plain(commits_per_sec, 1, duration),
    )
    .await;

    // Let the tail of the offered load drain before scraping: the seal cadence
    // means the last handful of commits can still be in flight for up to
    // ~depth * maintenance_interval after the generator stops, plus normal
    // claim/fold/apply latency on top.
    //
    // The completion signal is the terminal table's own row count, not
    // `trellis_changes_applied_total`: a change can legitimately be applied
    // more than once for one source row (the same write arriving both as an
    // in-transaction `Recompute` and as a CDC-decoded copy — see
    // `tuning::ISOLATED_PROPAGATION_RECONCILE_INTERVAL`), so the counter can
    // reach the row count while rows are still in flight, and waiting on it
    // would cut the window short. A row count can't overshoot.
    let expected_rows = load.rows_issued as i64 + 1; // + the warm-up row
    let grace_deadline =
        Instant::now() + tuning.maintenance_interval * depth as u32 * 2 + Duration::from_secs(5);
    while Instant::now() < grace_deadline {
        let landed: i64 = raw
            .query_one(&format!("select count(*) from public.{terminal}"), &[])
            .await
            .expect("count terminal rows while draining")
            .get(0);
        if landed >= expected_rows {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let after = scrape();
    let window =
        HistogramSnapshot::capture(&after, END_TO_END_LATENCY_METRIC, terminal, &T1_BOUNDS)
            .since(&e2e_baseline);
    let changes_applied =
        counter_value(&after, CHANGES_APPLIED_METRIC, terminal).saturating_sub(changes_before);
    let eval = T1Evaluation::evaluate(&window);
    let per_hop = per_hop_mean_ms(&after, &chain.hops, &hop_baseline);
    let oracle = check_chain_oracle(&raw, &chain).await;

    client.shutdown().await.expect("client shutdown");

    HopLatencyResult {
        depth,
        maintenance_interval_ms: tuning.maintenance_interval.as_millis() as u64,
        poll_interval_ms: tuning.poll_interval.as_millis() as u64,
        reconcile_interval_ms: tuning.reconcile_interval.as_millis() as u64,
        application_threads: tuning.application_threads,
        offered_commits_per_sec: commits_per_sec,
        duration_secs: duration.as_secs_f64(),
        commits_issued: load.commits_issued,
        rows_issued: load.rows_issued,
        actual_elapsed_secs: load.elapsed.as_secs_f64(),
        changes_applied_terminal: changes_applied,
        changes_match_committed: changes_applied >= load.rows_issued,
        // 1 row per commit, so the target rows/sec is the commit rate.
        generator_bound: generator_bound(
            Some(commits_per_sec),
            load.achieved_rows_per_sec(),
            oracle.fully_converged,
        ),
        e2e_count: eval.count,
        e2e_p50_bucket_frac: eval.p50_frac,
        e2e_p99_bucket_frac: eval.p99_frac,
        e2e_max_under_1s: eval.max_ok,
        t1_p50_pass: eval.p50_pass,
        t1_p99_pass: eval.p99_pass,
        t1_all_pass: eval.all_pass(),
        per_hop_increment_ms: increment_ms(&per_hop),
        per_hop_mean_ms: per_hop,
        oracle,
    }
}

/// [`run_depth`] for every depth in `depths`, in order.
pub async fn run_ladder(
    depths: &[usize],
    commits_per_sec: f64,
    duration: Duration,
    tuning: &EngineTuning,
) -> Vec<HopLatencyResult> {
    let mut results = Vec::with_capacity(depths.len());
    for &depth in depths {
        results.push(run_depth(depth, commits_per_sec, duration, tuning).await);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::increment_ms;

    #[test]
    fn increment_is_the_slope_between_first_and_last_observed_hop() {
        let per_hop = vec![Some(26.0), Some(332.0), Some(638.0)];
        assert_eq!(increment_ms(&per_hop), Some(306.0));
    }

    #[test]
    fn increment_skips_hops_with_no_samples() {
        let per_hop = vec![Some(10.0), None, Some(30.0)];
        assert_eq!(increment_ms(&per_hop), Some(10.0));
    }

    #[test]
    fn a_single_hop_has_no_increment() {
        assert_eq!(increment_ms(&[Some(26.0)]), None);
        assert_eq!(increment_ms(&[None]), None);
        assert_eq!(increment_ms(&[]), None);
    }
}
