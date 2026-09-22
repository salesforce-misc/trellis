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

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;
use crate::streaming::chain::{numeric_columns, wait_for_live, warm_up_aggregate};
use crate::streaming::load::{LoadConfig, run_controlled_load};
use crate::streaming::scrape::{
    CHANGES_APPLIED_METRIC, END_TO_END_LATENCY_METRIC, HistogramSnapshot, LE_MAX, LE_P50, LE_P99,
    T1_BOUNDS, counter_value, scrape,
};
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
    pub fold_in_ratio: usize,
    pub groups: usize,
    pub target_rows_per_sec: f64,
    pub offered_duration_secs: f64,
    pub rows_issued: u64,
    pub achieved_rows_per_sec: f64,
    pub changes_applied: u64,
    /// Whether apply activity went quiet before the grace deadline — see
    /// [`run_probe`] on why this scenario can't use a row-count backlog.
    pub sustained: bool,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
    pub e2e_max_frac: f64,
    /// `Some(true/false)` when the oracle could be evaluated, `None` when it
    /// couldn't — see [`check_aggregate_oracle`].
    pub oracle_ok: Option<bool>,
    pub oracle_groups: i64,
    pub oracle_mismatched_groups: i64,
}

impl FoldInResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"fold-in-ratio\",\"fold_in_ratio\":{},\"groups\":{},\
             \"target_rows_per_sec\":{},\"offered_duration_secs\":{:.3},\"rows_issued\":{},\
             \"achieved_rows_per_sec\":{:.1},\"changes_applied\":{},\"sustained\":{},\
             \"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4},\
             \"e2e_max_bucket_frac\":{:.4},\"oracle_ok\":{},\"oracle_groups\":{},\
             \"oracle_mismatched_groups\":{}}}",
            self.fold_in_ratio,
            self.groups,
            self.target_rows_per_sec,
            self.offered_duration_secs,
            self.rows_issued,
            self.achieved_rows_per_sec,
            self.changes_applied,
            self.sustained,
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

/// One probe: installs a single `SUM`/`COUNT` aggregate over a fresh source
/// table, offers `target_rows_per_sec` for `offered_duration` spread across
/// `groups` group keys, then waits up to `grace` for apply activity to go
/// quiet.
///
/// "Caught up" here can't wait for `changes_applied` to reach `rows_issued`
/// the way the 1-1 probes do: an aggregate's applied-change count is bounded
/// by group touches per batch, not by row count, so it converges to something
/// at or *below* the row count. This instead polls and calls the ring drained
/// once a poll finds no new applied changes since the previous one.
pub async fn run_probe(
    fold_in_ratio: usize,
    target_rows_per_sec: f64,
    offered_duration: Duration,
    grace: Duration,
    tuning: &EngineTuning,
) -> FoldInResult {
    let groups = ((target_rows_per_sec / fold_in_ratio as f64).round() as usize).max(1);
    let commits_per_sec = target_rows_per_sec / ROWS_PER_COMMIT as f64;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(&format!(
        "create table public.{SOURCE_TABLE} \
             (id bigint primary key, {GROUP_COLUMN} bigint not null, val numeric); \
         alter table public.{SOURCE_TABLE} replica identity full;"
    ))
    .await
    .expect("create aggregate source table");

    let client = trellis::Client::start(
        db.dsn(),
        tuning.client_options(vec![format!("public.{SOURCE_TABLE}")]),
    )
    .expect("client start");

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
    warm_up_aggregate(&raw, SOURCE_TABLE, &terminal, GROUP_COLUMN, SETUP_TIMEOUT).await;

    let before = scrape();
    let e2e_baseline =
        HistogramSnapshot::capture(&before, END_TO_END_LATENCY_METRIC, &terminal, &T1_BOUNDS);
    let changes_before = counter_value(&before, CHANGES_APPLIED_METRIC, &terminal);

    let load = run_controlled_load(
        &raw,
        SOURCE_TABLE,
        1,
        &LoadConfig {
            commits_per_sec,
            rows_per_commit: ROWS_PER_COMMIT,
            duration: offered_duration,
            groups: Some(groups),
        },
    )
    .await;

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
    let grace_deadline = Instant::now() + grace;
    let mut drained = false;
    while Instant::now() < grace_deadline {
        if folded_rows(&raw, &terminal).await >= expected_rows {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let after = scrape();
    let changes_now = counter_value(&after, CHANGES_APPLIED_METRIC, &terminal);
    let window =
        HistogramSnapshot::capture(&after, END_TO_END_LATENCY_METRIC, &terminal, &T1_BOUNDS)
            .since(&e2e_baseline);
    let (oracle_ok, oracle_groups, oracle_mismatched_groups) =
        check_aggregate_oracle(&raw, &terminal, &def.def, drained).await;

    client.shutdown().await.expect("client shutdown");

    FoldInResult {
        fold_in_ratio,
        groups,
        target_rows_per_sec,
        offered_duration_secs: load.elapsed.as_secs_f64(),
        rows_issued: load.rows_issued,
        achieved_rows_per_sec: load.achieved_rows_per_sec(),
        changes_applied: changes_now.saturating_sub(changes_before),
        sustained: drained,
        e2e_count: window.count,
        e2e_p50_bucket_frac: window.fraction(LE_P50),
        e2e_p99_bucket_frac: window.fraction(LE_P99),
        e2e_max_frac: window.fraction(LE_MAX),
        oracle_ok,
        oracle_groups,
        oracle_mismatched_groups,
    }
}

/// [`run_probe`] for every ratio in `ratios`, in order.
pub async fn run_sweep(
    ratios: &[usize],
    target_rows_per_sec: f64,
    offered_duration: Duration,
    grace: Duration,
    tuning: &EngineTuning,
) -> Vec<FoldInResult> {
    let mut results = Vec::with_capacity(ratios.len());
    for &ratio in ratios {
        results.push(run_probe(ratio, target_rows_per_sec, offered_duration, grace, tuning).await);
    }
    results
}
