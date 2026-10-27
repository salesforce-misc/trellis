//! `one-to-one-apply`: what a 1-1 target's drain costs per row, by field
//! shape.
//!
//! Each shape gets a fresh cluster and one 1-1 definition over
//! `public.oto_src (id bigint primary key, a integer, b numeric, t text)`.
//! Once it is `live`, one connection inserts `--rows` rows (1,000,000) in
//! commits of `--commit-rows` (10,000) as fast as it can, and the run waits
//! for the target to hold every row. Capture, seal and fold cost the same
//! whatever the fields are, so the shapes differ in what evaluating and
//! writing the fields costs:
//!
//! - `passthrough`: `a AS a, b AS b, t AS t`;
//! - `numeric`: integer and `numeric` arithmetic, a comparison and a
//!   `COALESCE`;
//! - `regex`: `REGEXP_COUNT`, `STRPOS` and `CHAR_LENGTH` over `t`.
//!
//! Reported per shape: rows/s from the first insert to the last row landing,
//! Postgres CPU over that window (`server_cost`), and the page
//! transactions' durations (`trellis_page_transaction_seconds`: the span a
//! page holds its entry and target-row locks), as count, mean and bucket
//! upper bounds for p50, p99 and the maximum. An oracle compares the target
//! with the definition's own SQL over the source afterwards.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::dev::defs::{IntWidth, ValueType, install_definition};

use crate::scenario::connect_raw;
use crate::streaming::chain::wait_for_live;
use crate::streaming::scrape::scrape;
use crate::streaming::server_cost;
use crate::streaming::tuning::EngineTuning;

/// The page-transaction histogram.
pub const PAGE_TRANSACTION_METRIC: &str = "trellis_page_transaction_seconds";

const TARGET: &str = "oto_tgt";

/// The shapes, by name.
pub const SHAPES: &[&str] = &["passthrough", "numeric", "regex"];

/// One shape's fields, and the oracle's SQL for them over `public.oto_src`.
fn shape_fields(shape: &str) -> (&'static str, &'static str) {
    match shape {
        "passthrough" => ("a AS a, b AS b, t AS t", "a, b, t"),
        "numeric" => (
            "(a + a + 1) AS i, (b + b + 1.25) AS n, ((b + 1) > 1000) AS big, \
             (COALESCE(b, 0) + a) AS c",
            "(a + a + 1), (b + b + 1.25), ((b + 1) > 1000), (coalesce(b, 0) + a)",
        ),
        "regex" => (
            "REGEXP_COUNT(t, '[0-9]') AS digits, STRPOS(t, 'x') AS pos, \
             CHAR_LENGTH(t) AS len",
            "regexp_count(t, '[0-9]'), strpos(t, 'x'), char_length(t)",
        ),
        other => panic!("unknown --shapes entry {other:?}; expected one of {SHAPES:?}"),
    }
}

/// The run's knobs.
#[derive(Debug, Clone)]
pub struct OneToOneApply {
    pub shapes: Vec<String>,
    pub rows: i64,
    pub commit_rows: i64,
    pub timeout: Duration,
}

/// One shape's numbers.
#[derive(Debug, Clone)]
pub struct ShapeResult {
    pub shape: String,
    pub rows: i64,
    pub drain_secs: f64,
    pub rows_per_sec: f64,
    pub pg_cpu_secs: f64,
    pub pg_cpu_us_per_row: f64,
    pub pages: u64,
    pub page_mean_ms: f64,
    pub page_p50_le_ms: f64,
    pub page_p99_le_ms: f64,
    pub page_max_le_ms: f64,
    pub drained: bool,
    pub oracle_mismatches: i64,
}

impl ShapeResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"one-to-one-apply\",\"shape\":\"{}\",\"rows\":{},\
             \"drain_secs\":{:.3},\"rows_per_sec\":{:.1},\"pg_cpu_secs\":{:.3},\
             \"pg_cpu_us_per_row\":{:.3},\"pages\":{},\"page_mean_ms\":{:.3},\
             \"page_p50_le_ms\":{:.3},\"page_p99_le_ms\":{:.3},\"page_max_le_ms\":{:.3},\
             \"drained\":{},\"oracle_mismatches\":{}}}",
            self.shape,
            self.rows,
            self.drain_secs,
            self.rows_per_sec,
            self.pg_cpu_secs,
            self.pg_cpu_us_per_row,
            self.pages,
            self.page_mean_ms,
            self.page_p50_le_ms,
            self.page_p99_le_ms,
            self.page_max_le_ms,
            self.drained,
            self.oracle_mismatches,
        )
    }
}

/// The page histogram's cumulative buckets (upper bound in seconds, count),
/// its sum and its count, summed over every instance in one scrape.
#[derive(Debug, Clone, Default)]
struct PageHistogram {
    buckets: Vec<(f64, u64)>,
    sum: f64,
    count: u64,
}

impl PageHistogram {
    fn capture(rendered: &str) -> Self {
        let mut buckets: Vec<(f64, u64)> = Vec::new();
        let mut sum = 0.0;
        let mut count = 0;
        for line in rendered.lines() {
            let Some(rest) = line.strip_prefix(PAGE_TRANSACTION_METRIC) else {
                continue;
            };
            let value = line.rsplit(' ').next().unwrap_or("0");
            if rest.starts_with("_bucket") {
                let le = rest
                    .split("le=\"")
                    .nth(1)
                    .and_then(|r| r.split('"').next())
                    .unwrap_or("+Inf");
                let le: f64 = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().unwrap_or(f64::INFINITY)
                };
                let n: u64 = value.parse().unwrap_or(0);
                match buckets.iter_mut().find(|(b, _)| *b == le) {
                    Some((_, c)) => *c += n,
                    None => buckets.push((le, n)),
                }
            } else if rest.starts_with("_sum") {
                sum += value.parse::<f64>().unwrap_or(0.0);
            } else if rest.starts_with("_count") {
                count += value.parse::<u64>().unwrap_or(0);
            }
        }
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        Self {
            buckets,
            sum,
            count,
        }
    }

    fn since(&self, base: &PageHistogram) -> PageHistogram {
        let buckets = self
            .buckets
            .iter()
            .map(|(le, n)| {
                let b = base
                    .buckets
                    .iter()
                    .find(|(l, _)| l == le)
                    .map_or(0, |(_, c)| *c);
                (*le, n.saturating_sub(b))
            })
            .collect();
        PageHistogram {
            buckets,
            sum: self.sum - base.sum,
            count: self.count.saturating_sub(base.count),
        }
    }

    /// The smallest bucket bound holding at least `q` of the samples, in ms.
    fn quantile_le_ms(&self, q: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let want = (q * self.count as f64).ceil() as u64;
        self.buckets
            .iter()
            .find(|(_, c)| *c >= want.max(1))
            .map_or(f64::INFINITY, |(le, _)| le * 1e3)
    }
}

async fn target_rows(raw: &RawClient) -> i64 {
    raw.query_one(&format!("select count(*) from public.{TARGET}"), &[])
        .await
        .expect("count target rows")
        .get(0)
}

/// Runs one shape on a fresh cluster.
pub async fn run_shape(shape: &str, cfg: &OneToOneApply, tuning: &EngineTuning) -> ShapeResult {
    let (fields, oracle_fields) = shape_fields(shape);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.oto_src (id bigint primary key, a integer, b numeric, t text)",
    )
    .await
    .expect("create the source");
    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    let columns = std::collections::HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("a".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("b".to_string(), ValueType::Numeric),
        ("t".to_string(), ValueType::Text),
    ]);
    let text = format!("TRANSFORM {TARGET} FROM public.oto_src SELECT {fields}");
    install_definition(&db.pool, &text, &columns, "public")
        .await
        .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    wait_for_live(&raw, TARGET, Instant::now() + Duration::from_secs(120)).await;

    let pages_before = PageHistogram::capture(&scrape());
    let cost = server_cost::start(&cluster);
    let started = Instant::now();
    let mut lo = 1_i64;
    while lo <= cfg.rows {
        let hi = (lo + cfg.commit_rows - 1).min(cfg.rows);
        raw.execute(
            "insert into public.oto_src \
             select g, (g % 100000)::integer, g * 1.5, 'item-' || g || ' x é' \
             from generate_series($1::bigint, $2::bigint) g",
            &[&lo, &hi],
        )
        .await
        .expect("insert a commit");
        lo = hi + 1;
    }
    let deadline = Instant::now() + cfg.timeout;
    let mut drained = false;
    while Instant::now() < deadline {
        if target_rows(&raw).await >= cfg.rows {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let drain_secs = started.elapsed().as_secs_f64();
    let cost = cost.finish(&raw).await;
    let pages = PageHistogram::capture(&scrape()).since(&pages_before);

    let oracle_mismatches: i64 = raw
        .query_one(
            &format!(
                "select count(*) from \
                   (select id, row({oracle_fields})::text as v from public.oto_src) s \
                 full join (select id, {} as v from public.{TARGET}) g using (id) \
                 where s.v is distinct from g.v",
                target_columns(shape)
            ),
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(-1);
    client.shutdown().await.expect("client shutdown");

    ShapeResult {
        shape: shape.to_string(),
        rows: cfg.rows,
        drain_secs,
        rows_per_sec: cfg.rows as f64 / drain_secs.max(f64::EPSILON),
        pg_cpu_secs: cost.pg_cpu_secs,
        pg_cpu_us_per_row: cost.pg_cpu_secs * 1e6 / cfg.rows as f64,
        pages: pages.count,
        page_mean_ms: if pages.count == 0 {
            0.0
        } else {
            pages.sum * 1e3 / pages.count as f64
        },
        page_p50_le_ms: pages.quantile_le_ms(0.5),
        page_p99_le_ms: pages.quantile_le_ms(0.99),
        page_max_le_ms: pages.quantile_le_ms(1.0),
        drained,
        oracle_mismatches,
    }
}

/// The target's field columns for `shape`, in definition order.
fn target_columns(shape: &str) -> &'static str {
    match shape {
        "passthrough" => "row(a, b, t)::text",
        "numeric" => "row(i, n, big, c)::text",
        "regex" => "row(digits, pos, len)::text",
        _ => unreachable!(),
    }
}

/// Runs every shape in `cfg`, one fresh cluster each.
pub async fn run(cfg: &OneToOneApply, tuning: &EngineTuning) -> Vec<ShapeResult> {
    let mut results = Vec::new();
    for shape in &cfg.shapes {
        let result = run_shape(shape, cfg, tuning).await;
        println!("{}", result.to_json());
        results.push(result);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_histogram_reads_cumulative_buckets_and_quantiles() {
        let rendered = "\
trellis_page_transaction_seconds_bucket{trellis_instance=\"a\",le=\"0.001\"} 1\n\
trellis_page_transaction_seconds_bucket{trellis_instance=\"a\",le=\"0.01\"} 9\n\
trellis_page_transaction_seconds_bucket{trellis_instance=\"a\",le=\"0.1\"} 10\n\
trellis_page_transaction_seconds_bucket{trellis_instance=\"a\",le=\"+Inf\"} 10\n\
trellis_page_transaction_seconds_sum{trellis_instance=\"a\"} 0.05\n\
trellis_page_transaction_seconds_count{trellis_instance=\"a\"} 10\n";
        let h = PageHistogram::capture(rendered).since(&PageHistogram::default());
        assert_eq!(h.count, 10);
        assert_eq!(h.quantile_le_ms(0.5), 10.0);
        assert_eq!(h.quantile_le_ms(1.0), 100.0);
    }
}
