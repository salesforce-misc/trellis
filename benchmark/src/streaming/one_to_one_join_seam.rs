//! `one-to-one-join-seam`: what a 1-1 `JOIN` that reads an upstream's target
//! costs the upstream (ADR-0021 "A side that is another transform's target is
//! read from its change feed", ADR-0002 "Multi-hop", #1547, moved from #1487).
//!
//! A transform's target that no other transform reads is written with no
//! record for a reader. When a 1-1 `JOIN` reads it, every write of the
//! upstream's pages also stages the row's prior image and a ring row for the
//! reader (the target-mutation seam), inside the upstream's own transaction.
//! This scenario puts a number on that.
//!
//! One paced writer updates `asrc` (`id, k, x`) in commits of `--commit-rows`
//! rows, `--rate` rows/s in all, for `--duration-secs`, over `--rows` rows. The
//! upstream is `TRANSFORM ta FROM public.asrc SELECT k AS k, x AS x`. Per
//! variant, a fresh cluster:
//!
//! - `none`: the upstream alone (the control);
//! - `a`: plus `jr`, `ta` as `a` of a 1-1 `JOIN` of `bsrc`
//!   (`FROM public.ta LEFT JOIN public.bsrc ON ta.k = bsrc.id`);
//! - `b`: plus `jb`, `ta` as `b` of a 1-1 `JOIN` whose `a` is a captured
//!   `csrc`, one row per `ta` row (`ON csrc.k = ta.id`), so each change of
//!   `ta` re-derives one `jb` row;
//! - `ab`: both readers.
//!
//! Once every transform is `live`, an idle window of `--idle-secs` measures the
//! cluster's WAL and CPU with nothing flowing; the writer then runs, and the
//! window closes when every transform's applied-change counter has been still
//! for 1.5 s. Reported per variant:
//!
//! - the upstream's own latency (`trellis_transform_latency_seconds{ta}`:
//!   mean and the bucket bounds holding p50 and p99);
//! - the page transaction histogram (`trellis_page_transaction_seconds`),
//!   which is per engine instance, so with readers it counts their pages too;
//! - WAL bytes and Postgres CPU per source row, with the idle window's rate
//!   over the same time taken off;
//! - the seconds from the writer's end to the last applied change, and an
//!   oracle for each target.
//!
//! The readers' own drain is in the totals, so the upstream's share of the
//! difference between `none` and the other variants is read from the upstream's
//! own latency, not from the WAL.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::dev::defs::{IntWidth, ValueType, install_definition, set_one_to_one_join};
use trellis::dev::metrics::LATENCY_BUCKETS;

use crate::scenario::connect_raw;
use crate::streaming::chain::wait_for_live;
use crate::streaming::disk_tier;
use crate::streaming::one_to_one_apply::PageHistogram;
use crate::streaming::scrape::{
    CHANGES_APPLIED_METRIC, HistogramSnapshot, SumCountSnapshot, TRANSFORM_LATENCY_METRIC,
    counter_value, scrape,
};
use crate::streaming::server_cost;
use crate::streaming::tuning::EngineTuning;

/// The variants, by name.
pub const VARIANTS: &[&str] = &["none", "a", "b", "ab"];

/// The run's knobs.
#[derive(Debug, Clone)]
pub struct Seam {
    pub variants: Vec<String>,
    pub rows: i64,
    pub rate: f64,
    pub commit_rows: i64,
    pub connections: usize,
    pub duration: Duration,
    pub idle: Duration,
    pub timeout: Duration,
}

/// One variant's numbers.
#[derive(Debug, Clone)]
pub struct SeamResult {
    pub variant: String,
    pub rows_written: i64,
    pub achieved_rows_per_sec: f64,
    pub window_secs: f64,
    pub drain_tail_secs: f64,
    pub upstream_mean_ms: f64,
    pub upstream_p50_le_ms: f64,
    pub upstream_p99_le_ms: f64,
    pub pages: u64,
    pub page_mean_ms: f64,
    pub page_p50_le_ms: f64,
    pub page_p99_le_ms: f64,
    pub wal_bytes_per_row: f64,
    pub pg_cpu_us_per_row: f64,
    pub ambient_wal_bytes_per_sec: f64,
    pub ambient_pg_cpu_cores: f64,
    pub applied: Vec<(String, u64)>,
    pub oracle_mismatches: i64,
}

impl SeamResult {
    pub fn to_json(&self) -> String {
        let applied: Vec<String> = self
            .applied
            .iter()
            .map(|(t, n)| format!("\"{t}\":{n}"))
            .collect();
        format!(
            "{{\"scenario\":\"one-to-one-join-seam\",\"variant\":\"{}\",\"rows_written\":{},\
             \"achieved_rows_per_sec\":{:.0},\"window_secs\":{:.2},\"drain_tail_secs\":{:.2},\
             \"upstream_mean_ms\":{:.2},\"upstream_p50_le_ms\":{:.1},\"upstream_p99_le_ms\":{:.1},\
             \"pages\":{},\"page_mean_ms\":{:.3},\"page_p50_le_ms\":{:.1},\"page_p99_le_ms\":{:.1},\
             \"wal_bytes_per_row\":{:.1},\"pg_cpu_us_per_row\":{:.2},\
             \"ambient_wal_bytes_per_sec\":{:.0},\"ambient_pg_cpu_cores\":{:.3},\
             \"applied\":{{{}}},\"oracle_mismatches\":{}}}",
            self.variant,
            self.rows_written,
            self.achieved_rows_per_sec,
            self.window_secs,
            self.drain_tail_secs,
            self.upstream_mean_ms,
            self.upstream_p50_le_ms,
            self.upstream_p99_le_ms,
            self.pages,
            self.page_mean_ms,
            self.page_p50_le_ms,
            self.page_p99_le_ms,
            self.wal_bytes_per_row,
            self.pg_cpu_us_per_row,
            self.ambient_wal_bytes_per_sec,
            self.ambient_pg_cpu_cores,
            applied.join(","),
            self.oracle_mismatches,
        )
    }
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// The paced writer: `connections` tasks take commit indexes from one counter,
/// each due at `index * commit_rows / rate` into the window, and update
/// `commit_rows` consecutive ids, cycling over `rows`. Returns the rows
/// updated.
async fn write(dsn: &str, cfg: &Seam) -> i64 {
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let commits_total = (cfg.duration.as_secs_f64() * cfg.rate / cfg.commit_rows as f64) as u64;
    let mut tasks = Vec::new();
    for _ in 0..cfg.connections {
        let raw = connect_raw(dsn).await;
        let statement = raw
            .prepare("update public.asrc set x = x + 1 where id between $1 and $2")
            .await
            .expect("prepare the update");
        let next = next.clone();
        let (rows, commit_rows, rate) = (cfg.rows, cfg.commit_rows, cfg.rate);
        tasks.push(tokio::spawn(async move {
            loop {
                let k = next.fetch_add(1, Ordering::Relaxed);
                if k >= commits_total {
                    return;
                }
                let due = Duration::from_secs_f64(k as f64 * commit_rows as f64 / rate);
                if let Some(wait) = due.checked_sub(started.elapsed()) {
                    tokio::time::sleep(wait).await;
                }
                let lo = ((k as i64 * commit_rows) % rows) as i32 + 1;
                let hi = lo + commit_rows as i32 - 1;
                raw.execute(&statement, &[&lo, &hi])
                    .await
                    .expect("update a commit");
            }
        }));
    }
    for t in tasks {
        t.await.expect("writer task");
    }
    commits_total as i64 * cfg.commit_rows
}

/// A transform's target rows that differ from their query.
async fn oracle(raw: &RawClient, variant_target: &str) -> i64 {
    let sql = match variant_target {
        "ta" => {
            "select count(*) from (select id, k, x from public.asrc) s \
             full join public.ta t using (id) \
             where s.k is distinct from t.k or s.x is distinct from t.x"
        }
        "jr" => {
            "select count(*) from (select a.id, a.x + coalesce(b.w, 0) as total from public.asrc a \
              left join public.bsrc b on a.k = b.id) s \
             full join public.jr t using (id) where s.total is distinct from t.total"
        }
        "jb" => {
            "select count(*) from (select c.id, c.x + coalesce(a.x, 0) as total from public.csrc c \
              left join public.asrc a on c.k = a.id) s \
             full join public.jb t using (id) where s.total is distinct from t.total"
        }
        other => panic!("no oracle for {other}"),
    };
    raw.query_one(sql, &[]).await.expect("oracle").get(0)
}

async fn applied_counts(targets: &[&str]) -> Vec<(String, u64)> {
    let rendered = scrape();
    targets
        .iter()
        .map(|t| {
            (
                t.to_string(),
                counter_value(&rendered, CHANGES_APPLIED_METRIC, t),
            )
        })
        .collect()
}

/// The smallest bucket bound (ms) of `snapshot` holding at least `q` of its samples.
fn quantile_le_ms(snapshot: &HistogramSnapshot, bounds: &[String], q: f64) -> f64 {
    bounds
        .iter()
        .find(|le| snapshot.fraction(le) >= q)
        .map_or(f64::INFINITY, |le| le.parse::<f64>().unwrap_or(0.0) * 1e3)
}

/// Runs one variant on a fresh cluster.
pub async fn run_variant(cfg: &Seam, tuning: &EngineTuning, variant: &str) -> SeamResult {
    assert!(
        VARIANTS.contains(&variant),
        "unknown variant {variant:?}; expected one of {VARIANTS:?}"
    );
    set_one_to_one_join(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let with_a = variant.contains('a');
    let with_b = variant.contains('b');
    raw.batch_execute(
        "create table public.asrc (id integer primary key, k integer, x integer); \
         create table public.bsrc (id integer primary key, w integer); \
         create table public.csrc (id integer primary key, k integer, x integer)",
    )
    .await
    .expect("create the sources");
    raw.execute(
        "insert into public.asrc select g, 1 + g % 1000, g % 1000 from generate_series(1, $1::bigint) g",
        &[&cfg.rows],
    )
    .await
    .expect("seed asrc");
    raw.batch_execute("insert into public.bsrc select g, g from generate_series(1, 1000) g")
        .await
        .expect("seed bsrc");
    raw.execute(
        "insert into public.csrc select g, g, g % 100 from generate_series(1, $1::bigint) g",
        &[&cfg.rows],
    )
    .await
    .expect("seed csrc");
    raw.batch_execute("analyze").await.expect("analyze");

    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    let deadline = || Instant::now() + cfg.timeout;
    let mut targets = vec!["ta"];
    install_definition(
        &db.pool,
        "TRANSFORM ta FROM public.asrc SELECT k AS k, x AS x",
        &HashMap::from([("k".to_string(), int4()), ("x".to_string(), int4())]),
        "public",
    )
    .await
    .expect("define the upstream");
    wait_for_live(&raw, "ta", deadline()).await;
    let total = HashMap::from([("total".to_string(), int4())]);
    if with_a {
        install_definition(
            &db.pool,
            "TRANSFORM jr FROM public.ta LEFT JOIN public.bsrc ON ta.k = bsrc.id \
             SELECT ta.x + COALESCE(bsrc.w, 0) AS total",
            &total,
            "public",
        )
        .await
        .expect("define the a-side reader");
        wait_for_live(&raw, "jr", deadline()).await;
        targets.push("jr");
    }
    if with_b {
        install_definition(
            &db.pool,
            "TRANSFORM jb FROM public.csrc LEFT JOIN public.ta ON csrc.k = ta.id \
             SELECT csrc.x + COALESCE(ta.x, 0) AS total",
            &total,
            "public",
        )
        .await
        .expect("define the b-side reader");
        wait_for_live(&raw, "jb", deadline()).await;
        targets.push("jb");
    }

    // Ambient: what the cluster and engine cost with nothing flowing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let disk = disk_tier::sample(&raw).await;
    let cost = server_cost::start(&cluster);
    tokio::time::sleep(cfg.idle).await;
    let idle_cost = cost.finish(&raw).await;
    let idle_disk = disk_tier::since(&raw, &disk).await;
    let ambient_wal = idle_disk.wal_bytes as f64 / idle_cost.window_secs;
    let ambient_cpu = idle_cost.pg_cpu_secs / idle_cost.window_secs;

    let rendered = scrape();
    let bounds: Vec<String> = LATENCY_BUCKETS.iter().map(|b| b.to_string()).collect();
    let bound_refs: Vec<&str> = bounds.iter().map(String::as_str).collect();
    let latency_before =
        HistogramSnapshot::capture(&rendered, TRANSFORM_LATENCY_METRIC, "ta", &bound_refs);
    let mean_before = SumCountSnapshot::capture(&rendered, TRANSFORM_LATENCY_METRIC, "ta");
    let pages_before = PageHistogram::capture(&rendered);
    let applied_before = applied_counts(&targets).await;

    checkpoint(&raw).await;
    let disk = disk_tier::sample(&raw).await;
    let cost = server_cost::start(&cluster);
    let started = Instant::now();
    let rows_written = write(db.dsn(), cfg).await;
    let write_secs = started.elapsed().as_secs_f64();
    // Done when every counter has been still for 1.5 s.
    let mut last = applied_counts(&targets).await;
    let mut still_since = Instant::now();
    let give_up = Instant::now() + cfg.timeout;
    let tail_start = Instant::now();
    let mut tail_end = tail_start;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = applied_counts(&targets).await;
        if now != last {
            last = now;
            still_since = Instant::now();
            tail_end = still_since;
        } else if still_since.elapsed() > Duration::from_millis(1500) {
            break;
        }
        assert!(Instant::now() < give_up, "the pipeline never settled");
    }
    let window_secs = started.elapsed().as_secs_f64() - 1.5;
    let drain_tail_secs = tail_end.duration_since(tail_start).as_secs_f64();
    let cost = cost.finish(&raw).await;
    let disk = disk_tier::since(&raw, &disk).await;

    let rendered = scrape();
    let latency =
        HistogramSnapshot::capture(&rendered, TRANSFORM_LATENCY_METRIC, "ta", &bound_refs)
            .since(&latency_before);
    let mean = SumCountSnapshot::capture(&rendered, TRANSFORM_LATENCY_METRIC, "ta")
        .since(&mean_before)
        .mean_ms()
        .unwrap_or(f64::NAN);
    let pages = PageHistogram::capture(&rendered).since(&pages_before);
    let applied: Vec<(String, u64)> = applied_counts(&targets)
        .await
        .into_iter()
        .zip(applied_before)
        .map(|((t, after), (_, before))| (t, after - before))
        .collect();

    let mut oracle_mismatches = 0;
    for t in &targets {
        oracle_mismatches += oracle(&raw, t).await;
    }
    let _ = client.shutdown().await;

    let rows = rows_written as f64;
    SeamResult {
        variant: variant.to_string(),
        rows_written,
        achieved_rows_per_sec: rows / write_secs,
        window_secs,
        drain_tail_secs,
        upstream_mean_ms: mean,
        upstream_p50_le_ms: quantile_le_ms(&latency, &bounds, 0.5),
        upstream_p99_le_ms: quantile_le_ms(&latency, &bounds, 0.99),
        pages: pages.count,
        page_mean_ms: if pages.count == 0 {
            0.0
        } else {
            pages.sum * 1e3 / pages.count as f64
        },
        page_p50_le_ms: pages.quantile_le_ms(0.5),
        page_p99_le_ms: pages.quantile_le_ms(0.99),
        wal_bytes_per_row: (disk.wal_bytes as f64 - ambient_wal * window_secs) / rows,
        pg_cpu_us_per_row: (cost.pg_cpu_secs - ambient_cpu * window_secs) * 1e6 / rows,
        ambient_wal_bytes_per_sec: ambient_wal,
        ambient_pg_cpu_cores: ambient_cpu,
        applied,
        oracle_mismatches,
    }
}

async fn checkpoint(raw: &RawClient) {
    raw.batch_execute("checkpoint").await.expect("checkpoint");
}

/// Runs every variant in `cfg`, one fresh cluster each.
pub async fn run(cfg: &Seam, tuning: &EngineTuning) -> Vec<SeamResult> {
    let mut results = Vec::new();
    for variant in &cfg.variants {
        let r = run_variant(cfg, tuning, variant).await;
        println!("{}", r.to_json());
        results.push(r);
    }
    results
}
