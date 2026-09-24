//! Aggregate throughput by fold-in ratio — issue #266's B3, epic #269's
//! **V-AGG** / target T3 (400k rows/sec at fold-in >= 10:1).
//!
//! Unlike a 1-1 chain, where every source row becomes exactly one target
//! write, an aggregate's target write count is bounded by its *group* count:
//! several source rows landing in one sealed batch for the same group key
//! collapse into one target write at claim-time fold. T3 rests entirely on
//! that premise ("fold-in collapses the write side"), so the ratio is swept as
//! an explicit axis rather than asserted at a single value.
//!
//! `fold_in_ratio` and the target rate together derive
//! `groups = target_rows_per_sec / fold_in_ratio`: at 10:1 offering 400k
//! rows/sec that's 40,000 groups each taking ~10 rows/sec; at 1000:1, 400
//! groups each taking ~1,000 rows/sec.
//!
//! Load comes from the multi-connection generator, paced at the target
//! ([`run_parallel_load`]): before issue #276 a single connection offered
//! this scenario's 400k rows/sec target, undershot it at every ratio, and
//! still reported `sustained: true` — a verdict on the generator that read
//! like one on T3. `generator_bound` now flags exactly that combination.
//!
//! The same probe backs issue #277's `group-contention` scenario, which takes
//! the group count directly (`--groups`) and crosses it with the drain-worker
//! count (`--threads`). Every probe also samples where the engine's backends
//! spent the window ([`contention`]), so a slow row can be attributed —
//! row-lock waits in the claim or in the aggregate's target-row pre-lock, the
//! per-group source probes, the fold — rather than guessed at.
//!
//! Offer above the ceiling when characterizing: `in_window_folded_rows_per_sec`
//! of a pipeline that can't keep up *is* its capacity at that shape, which is
//! the number #277's curves are made of.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;
use crate::streaming::chain::{
    numeric_columns, wait_for_catch_up_discharged, wait_for_live, warm_up_aggregate,
};
use crate::streaming::contention::{self, ContentionSummary};
use crate::streaming::load::{
    GENERATOR_UNDERSHOOT_TOLERANCE, Pace, ParallelLoad, generator_bound, run_parallel_load,
};
use crate::streaming::scrape::{
    CHANGES_APPLIED_METRIC, END_TO_END_LATENCY_METRIC, HistogramSnapshot, LE_MAX, LE_P50, LE_P99,
    T1_BOUNDS, counter_value, scrape,
};
use crate::streaming::throughput::Offer;
use crate::streaming::tuning::EngineTuning;

/// A mid-sized commit shape — deliberately neither of the transaction-shape
/// sweep's extremes, so this scenario's axis is the fold-in ratio and nothing
/// else.
const ROWS_PER_COMMIT: usize = 200;

const SOURCE_TABLE: &str = "agg_src";
const GROUP_COLUMN: &str = "grp";
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

/// The ratios #266 names.
pub const DEFAULT_RATIOS: &[usize] = &[10, 100, 1000];

#[derive(Debug)]
pub struct FoldInResult {
    /// `target_rows_per_sec / groups`: source rows per group per second.
    pub fold_in_ratio: f64,
    pub groups: usize,
    pub target_rows_per_sec: f64,
    /// Drain workers ([`EngineTuning::application_threads`]) — issue #277's
    /// second axis.
    pub application_threads: usize,
    /// Generator writer connections.
    pub connections: usize,
    pub offered_duration_secs: f64,
    pub rows_issued: u64,
    pub achieved_rows_per_sec: f64,
    /// Issue #276's self-check: the generator undershot the target and the
    /// aggregate still drained everything it got — `sustained` then says
    /// nothing about T3 at `target_rows_per_sec`.
    pub generator_bound: bool,
    pub changes_applied: u64,
    /// Whether apply activity went quiet before the grace deadline — see
    /// [`run_probe`] on why this scenario can't use a row-count backlog.
    pub sustained: bool,
    /// Source rows over the time from the offer window opening until the
    /// target had folded in every one of them. `None` when the target never
    /// caught up within the grace period.
    ///
    /// Informational, and biased low: its denominator includes the
    /// pipeline's end-to-end latency on the last rows plus the drain poll's
    /// granularity, so even a pipeline that keeps up exactly reads
    /// `duration / (duration + tail)` of the target — ~93% on a 5s window at
    /// a T1-compliant few hundred ms. T3's verdict uses
    /// `in_window_folded_rows_per_sec` instead.
    pub folded_rows_per_sec: Option<f64>,
    /// The rate the aggregate folded rows at *while* load was arriving: the
    /// least-squares slope of `sum(row_count)` sampled across the offer
    /// window after its first [`FOLD_RATE_SETTLE_FRACTION`]. A pipeline that
    /// keeps up tracks the offered rate at a constant lag, so the slope equals
    /// the offered rate whatever that lag is; one that falls behind folds at
    /// its own lower rate. `None` if too few samples landed to fit a line.
    pub in_window_folded_rows_per_sec: Option<f64>,
    /// T3's yes-or-no at this ratio: the target drained (`sustained`) **and**
    /// `in_window_folded_rows_per_sec` kept up with the target within
    /// [`GENERATOR_UNDERSHOOT_TOLERANCE`]. `sustained` alone can't answer
    /// T3: it only says the backlog drained within `grace`, and a long grace
    /// lets a pipeline running at a fraction of the target pass. Only
    /// meaningful when `generator_bound` is false.
    pub kept_target_rate: bool,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
    pub e2e_max_frac: f64,
    /// `Some(true/false)` when the oracle could be evaluated, `None` when it
    /// couldn't — see [`check_aggregate_oracle`].
    pub oracle_ok: Option<bool>,
    pub oracle_groups: i64,
    pub oracle_mismatched_groups: i64,
    /// Issue #277: where the engine's backends spent the offer window, over
    /// the same span the in-window fold rate is fitted to — see
    /// [`contention`].
    pub contention: ContentionSummary,
    /// `pg_stat_database` deadlocks over the offer window.
    pub deadlocks: i64,
    /// `pg_stat_database` rolled-back transactions over the offer window —
    /// see [`contention::deadlocks_and_rollbacks`].
    pub xact_rollbacks: i64,
}

impl FoldInResult {
    pub fn to_json(&self, scenario: &str) -> String {
        format!(
            "{{\"scenario\":\"{}\",\"fold_in_ratio\":{:.1},\"groups\":{},\
             \"target_rows_per_sec\":{},\"application_threads\":{},\"connections\":{},\
             \"offered_duration_secs\":{:.3},\
             \"rows_issued\":{},\"achieved_rows_per_sec\":{:.1},\"generator_bound\":{},\
             \"changes_applied\":{},\"sustained\":{},\"folded_rows_per_sec\":{},\
             \"in_window_folded_rows_per_sec\":{},\"kept_target_rate\":{},\
             \"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4},\
             \"e2e_max_bucket_frac\":{:.4},\"oracle_ok\":{},\"oracle_groups\":{},\
             \"oracle_mismatched_groups\":{},\"deadlocks\":{},\"xact_rollbacks\":{},\
             \"contention\":{}}}",
            scenario,
            self.fold_in_ratio,
            self.groups,
            self.target_rows_per_sec,
            self.application_threads,
            self.connections,
            self.offered_duration_secs,
            self.rows_issued,
            self.achieved_rows_per_sec,
            self.generator_bound,
            self.changes_applied,
            self.sustained,
            json_rate(self.folded_rows_per_sec),
            json_rate(self.in_window_folded_rows_per_sec),
            self.kept_target_rate,
            self.e2e_count,
            self.e2e_p50_bucket_frac,
            self.e2e_p99_bucket_frac,
            self.e2e_max_frac,
            match self.oracle_ok {
                Some(ok) => ok.to_string(),
                None => "null".to_string(),
            },
            self.oracle_groups,
            self.oracle_mismatched_groups,
            self.deadlocks,
            self.xact_rollbacks,
            self.contention.to_json(),
        )
    }
}

/// Compares the aggregate target against the engine's own rendered oracle
/// `SELECT` over the source table — the same shape of check
/// [`crate::scenario`] runs, via [`trellis::dev::defs::oracle`], rather than a
/// hand-duplicated `GROUP BY`.
///
/// **Only meaningful when the pipeline has fully caught up.** A partially
/// drained aggregate legitimately holds partial sums: rows already committed
/// at the source but not yet folded are missing from the target's `SUM`, so
/// every group would "mismatch" for a reason that is not a correctness
/// failure. `drained` is therefore passed in, and a non-drained run reports
/// `None` rather than a misleading `false`.
async fn check_aggregate_oracle(
    raw: &RawClient,
    terminal: &str,
    def: &trellis::dev::defs::ast::TransformDef,
    drained: bool,
) -> (Option<bool>, i64, i64) {
    let oracle_sql = trellis::dev::defs::oracle::render_aggregate_select_sql(def);
    let oracle_groups: i64 = raw
        .query_one(&format!("select count(*) from ({oracle_sql}) o"), &[])
        .await
        .expect("count aggregate oracle groups")
        .get(0);

    if !drained {
        return (None, oracle_groups, 0);
    }

    // A full outer join catches all three ways the target can disagree: a
    // group present in one side only, or present in both with a different
    // value.
    let mismatched: i64 = raw
        .query_one(
            &format!(
                "select count(*) from ({oracle_sql}) o \
                 full outer join public.{terminal} t on t.{GROUP_COLUMN} = o.{GROUP_COLUMN} \
                 where t.{GROUP_COLUMN} is null or o.{GROUP_COLUMN} is null \
                    or t.val is distinct from o.val \
                    or t.row_count is distinct from o.row_count"
            ),
            &[],
        )
        .await
        .expect("compare aggregate target against oracle")
        .get(0);

    (Some(mismatched == 0), oracle_groups, mismatched)
}

/// How many source rows have been folded into `terminal` so far:
/// `sum(row_count)` across its groups. Every source row is counted into
/// exactly one group's `COUNT(*)`, so this equals the number of source rows
/// the pipeline has caught up with — the aggregate analogue of a 1-1 chain's
/// target row count.
async fn folded_rows(raw: &RawClient, terminal: &str) -> i64 {
    let folded: Option<i64> = raw
        .query_one(
            &format!("select sum(row_count)::bigint from public.{terminal}"),
            &[],
        )
        .await
        .expect("sum folded row_count")
        .get(0);
    folded.unwrap_or(0)
}

/// How much of the offer window to skip before sampling the fold rate: long
/// enough for the pipeline's start-up lag (first seal, first claim) to pass,
/// short enough to leave most of the window to fit over.
const FOLD_RATE_SETTLE_FRACTION: f64 = 0.25;
const FOLD_RATE_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

/// Samples [`folded_rows`] every [`FOLD_RATE_SAMPLE_INTERVAL`] from
/// `FOLD_RATE_SETTLE_FRACTION` of the way into the offer window until it
/// closes, as `(seconds since offer_start, rows folded)` pairs. Runs
/// alongside the generator, on `raw` — never on a generator connection.
async fn sample_fold_progress(
    raw: &RawClient,
    terminal: &str,
    offer_start: Instant,
    duration: Duration,
) -> Vec<(f64, f64)> {
    tokio::time::sleep_until((offer_start + duration.mul_f64(FOLD_RATE_SETTLE_FRACTION)).into())
        .await;
    let window_end = offer_start + duration;
    let mut samples = Vec::new();
    while Instant::now() < window_end {
        // The query reads one snapshot as of its start, so stamp it then.
        let at = offer_start.elapsed().as_secs_f64();
        samples.push((at, folded_rows(raw, terminal).await as f64));
        tokio::time::sleep(FOLD_RATE_SAMPLE_INTERVAL).await;
    }
    samples
}

/// Least-squares slope of `samples`' `y` over `x` — rows folded per second.
/// A fit over many samples rather than a two-point difference, because the
/// aggregate folds in batches: `sum(row_count)` is a step function, and two
/// points can each land anywhere in a step. `None` with fewer than two
/// distinct `x`s.
fn fold_rate(samples: &[(f64, f64)]) -> Option<f64> {
    let n = samples.len() as f64;
    if samples.len() < 2 {
        return None;
    }
    let mean_x = samples.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = samples.iter().map(|(_, y)| y).sum::<f64>() / n;
    let (mut sxy, mut sxx) = (0.0, 0.0);
    for (x, y) in samples {
        sxy += (x - mean_x) * (y - mean_y);
        sxx += (x - mean_x) * (x - mean_x);
    }
    (sxx > 0.0).then(|| sxy / sxx)
}

/// T3's verdict: drained, and folded at the target rate while it was offered.
fn kept_target_rate(drained: bool, in_window_rate: Option<f64>, target_rows_per_sec: f64) -> bool {
    drained
        && in_window_rate.is_some_and(|rate| {
            rate >= target_rows_per_sec * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE)
        })
}

fn json_rate(rate: Option<f64>) -> String {
    match rate {
        Some(rate) => format!("{rate:.1}"),
        None => "null".to_string(),
    }
}

/// One probe: installs a single `SUM`/`COUNT` aggregate over a fresh source
/// table, offers `target_rows_per_sec` for `offer.duration` from
/// `offer.connections` writers, spread across `groups` group keys, then waits
/// up to `offer.grace` for the target to fold in every row.
///
/// "Caught up" is the target's own `sum(row_count)` reaching the committed
/// row count (see the comment at the drain loop below), not
/// `changes_applied`. That counter counts staged source rows too (#409), but
/// it's an in-process tally recorded after each apply commits, not the
/// target's own state, so the target is the authoritative signal.
pub async fn run_probe(
    groups: usize,
    target_rows_per_sec: f64,
    offer: Offer,
    tuning: &EngineTuning,
) -> FoldInResult {
    assert!(groups >= 1, "an aggregate needs at least one group");

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    // Issue #277's sampler gets its own connection: `raw` is busy sampling
    // the fold rate over the same window.
    let sampler = connect_raw(db.dsn()).await;

    raw.batch_execute(&format!(
        "create table public.{SOURCE_TABLE} \
             (id bigint primary key, {GROUP_COLUMN} bigint not null, val numeric); \
         alter table public.{SOURCE_TABLE} replica identity full;"
    ))
    .await
    .expect("create aggregate source table");

    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");

    let columns: HashMap<_, _> = numeric_columns(&["id", GROUP_COLUMN, "val"]);
    let source_text = format!(
        "TRANSFORM agg_totals FROM public.{SOURCE_TABLE} GROUP BY {GROUP_COLUMN} \
         SELECT {GROUP_COLUMN} AS {GROUP_COLUMN}, SUM(val) AS val, COUNT(*) AS row_count"
    );
    let def = trellis::dev::defs::install_definition(&db.pool, &source_text, &columns, "public")
        .await
        .expect("install aggregate definition");
    let terminal = def.def.target.clone();

    wait_for_live(&raw, &terminal, Instant::now() + SETUP_TIMEOUT).await;
    wait_for_catch_up_discharged(
        &raw,
        Instant::now() + SETUP_TIMEOUT + tuning.reconcile_interval,
    )
    .await;
    warm_up_aggregate(&raw, SOURCE_TABLE, &terminal, GROUP_COLUMN, SETUP_TIMEOUT).await;

    let before = scrape();
    let e2e_baseline =
        HistogramSnapshot::capture(&before, END_TO_END_LATENCY_METRIC, &terminal, &T1_BOUNDS);
    let changes_before = counter_value(&before, CHANGES_APPLIED_METRIC, &terminal);

    let load_cfg = ParallelLoad {
        connections: offer.connections,
        rows_per_commit: ROWS_PER_COMMIT,
        duration: offer.duration,
        groups: Some(groups),
        pace: Pace::RowsPerSec(target_rows_per_sec),
    };
    let (deadlocks_before, rollbacks_before) = contention::deadlocks_and_rollbacks(&sampler).await;
    let offer_start = Instant::now();
    let (load, fold_samples, contention) = tokio::join!(
        run_parallel_load(db.dsn(), SOURCE_TABLE, 1, &load_cfg),
        sample_fold_progress(&raw, &terminal, offer_start, offer.duration),
        contention::sample(
            &sampler,
            SOURCE_TABLE,
            offer_start + offer.duration.mul_f64(FOLD_RATE_SETTLE_FRACTION),
            offer_start + offer.duration,
        ),
    );
    let (deadlocks_after, rollbacks_after) = contention::deadlocks_and_rollbacks(&sampler).await;
    let in_window_folded_rows_per_sec = fold_rate(&fold_samples);

    // An aggregate has an exact convergence signal that a 1-1 chain's row
    // count is the analogue of: every source row is counted into exactly one
    // group's `COUNT(*)`, so `sum(row_count)` across the target equals the
    // number of source rows that have been folded in. When that reaches the
    // number committed, the pipeline has caught up — precisely, with no
    // heuristic.
    //
    // The reference harness instead declared "drained" the first time two
    // consecutive polls of `trellis_changes_applied_total` were equal. That
    // fires on any momentary lull mid-run, and measurably did: it reported
    // `sustained: true` with the target still holding partial sums, which is
    // also why its aggregate runs carried no oracle check that could have
    // caught it.
    let expected_rows = load.rows_issued as i64 + 1; // + the warm-up row
    let grace_deadline = Instant::now() + offer.grace;
    let mut drained_after = None;
    while Instant::now() < grace_deadline {
        if folded_rows(&raw, &terminal).await >= expected_rows {
            drained_after = Some(offer_start.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let drained = drained_after.is_some();

    let after = scrape();
    let changes_now = counter_value(&after, CHANGES_APPLIED_METRIC, &terminal);
    let window =
        HistogramSnapshot::capture(&after, END_TO_END_LATENCY_METRIC, &terminal, &T1_BOUNDS)
            .since(&e2e_baseline);
    let (oracle_ok, oracle_groups, oracle_mismatched_groups) =
        check_aggregate_oracle(&raw, &terminal, &def.def, drained).await;

    client.shutdown().await.expect("client shutdown");

    let achieved_rows_per_sec = load.achieved_rows_per_sec();
    let folded_rows_per_sec =
        drained_after.map(|elapsed| load.rows_issued as f64 / elapsed.as_secs_f64());
    FoldInResult {
        fold_in_ratio: target_rows_per_sec / groups as f64,
        groups,
        target_rows_per_sec,
        application_threads: tuning.application_threads,
        connections: offer.connections,
        offered_duration_secs: load.elapsed.as_secs_f64(),
        rows_issued: load.rows_issued,
        achieved_rows_per_sec,
        generator_bound: generator_bound(Some(target_rows_per_sec), achieved_rows_per_sec, drained),
        changes_applied: changes_now.saturating_sub(changes_before),
        sustained: drained,
        folded_rows_per_sec,
        in_window_folded_rows_per_sec,
        kept_target_rate: kept_target_rate(
            drained,
            in_window_folded_rows_per_sec,
            target_rows_per_sec,
        ),
        e2e_count: window.count,
        e2e_p50_bucket_frac: window.fraction(LE_P50),
        e2e_p99_bucket_frac: window.fraction(LE_P99),
        e2e_max_frac: window.fraction(LE_MAX),
        oracle_ok,
        oracle_groups,
        oracle_mismatched_groups,
        contention,
        deadlocks: deadlocks_after - deadlocks_before,
        xact_rollbacks: rollbacks_after - rollbacks_before,
    }
}

/// The group count a fold-in `ratio` gives at `target_rows_per_sec`.
pub fn groups_for_ratio(ratio: usize, target_rows_per_sec: f64) -> usize {
    ((target_rows_per_sec / ratio as f64).round() as usize).max(1)
}

/// [`run_probe`] for every ratio in `ratios`, in order.
pub async fn run_sweep(
    ratios: &[usize],
    target_rows_per_sec: f64,
    offer: Offer,
    tuning: &EngineTuning,
) -> Vec<FoldInResult> {
    let mut results = Vec::with_capacity(ratios.len());
    for &ratio in ratios {
        let groups = groups_for_ratio(ratio, target_rows_per_sec);
        results.push(run_probe(groups, target_rows_per_sec, offer, tuning).await);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `sum(row_count)` for a pipeline offered `rate` rows/sec that folds in
    /// batches every `batch_secs`, each batch landing `lag_secs` after the
    /// rows it holds were committed — sampled every 200ms across `span`.
    fn batched_fold(
        rate: f64,
        lag_secs: f64,
        batch_secs: f64,
        span: (f64, f64),
    ) -> Vec<(f64, f64)> {
        let mut samples = Vec::new();
        let mut t = span.0;
        while t < span.1 {
            let visible_through = t - lag_secs;
            let batches = (visible_through / batch_secs).floor().max(0.0);
            samples.push((t, rate * batches * batch_secs));
            t += 0.2;
        }
        samples
    }

    #[test]
    fn a_pipeline_that_keeps_up_at_a_lag_folds_at_the_offered_rate() {
        // 400k rows/sec over a 5s window, folded in 150ms batches half a
        // second behind: T1-compliant, and keeping up exactly. The drain-time
        // rate reads this as 5 / 5.5 of the target — a false T3 NO — but the
        // in-window slope doesn't depend on the lag.
        let samples = batched_fold(400_000.0, 0.5, 0.15, (1.25, 5.0));
        let rate = fold_rate(&samples).expect("enough samples");
        assert!(
            (rate - 400_000.0).abs() < 400_000.0 * 0.01,
            "slope {rate} should track the offered 400k"
        );
        assert!(kept_target_rate(true, Some(rate), 400_000.0));
    }

    #[test]
    fn a_pipeline_that_falls_behind_folds_at_its_own_rate() {
        let samples = batched_fold(85_000.0, 0.5, 0.15, (5.0, 20.0));
        let rate = fold_rate(&samples).expect("enough samples");
        assert!((rate - 85_000.0).abs() < 85_000.0 * 0.01, "slope {rate}");
        assert!(!kept_target_rate(true, Some(rate), 400_000.0));
    }

    #[test]
    fn kept_target_rate_needs_a_drain_and_a_fit() {
        assert!(!kept_target_rate(false, Some(400_000.0), 400_000.0));
        assert!(!kept_target_rate(true, None, 400_000.0));
        assert!(kept_target_rate(true, Some(393_000.0), 400_000.0));
        assert!(!kept_target_rate(true, Some(390_000.0), 400_000.0));
    }

    #[test]
    fn too_few_samples_fit_no_line() {
        assert_eq!(fold_rate(&[]), None);
        assert_eq!(fold_rate(&[(1.0, 10.0)]), None);
        assert_eq!(fold_rate(&[(1.0, 10.0), (1.0, 20.0)]), None);
        assert_eq!(fold_rate(&[(1.0, 10.0), (2.0, 30.0)]), Some(20.0));
    }
}
