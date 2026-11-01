//! `one-to-one-join-fan-out` and `one-to-one-join-poison`: what a change of a
//! 1-1 `JOIN`'s `b` row costs when it rewrites many rows of `a` (ADR-0021,
//! #1547). Both are on a captured pair of tables:
//!
//! ```text
//! asrc (id integer pk, k integer, x integer)   -- F rows on k = 1, plus background keys
//! bsrc (id integer pk, w integer, tag text)    -- the b row id = 1 is the hot key
//! TRANSFORM jt FROM public.asrc LEFT JOIN public.bsrc ON asrc.k = bsrc.id
//!     SELECT asrc.x + COALESCE(bsrc.w, 0) AS total, ...
//! ```
//!
//! ## `one-to-one-join-fan-out`
//!
//! For each fan-out size `F` (`--fan-outs`), a fresh cluster holds `F` rows of
//! `asrc` on key 1. The definition is built (the build is timed too), then
//! `--reps` times one `update bsrc set w = w + 1 where id = 1` commits and the
//! run waits for its one page to land. A page's transaction is the span in
//! which an `a` change on the key waits ([`PAGE_TRANSACTION_METRIC`]), so its
//! duration is the stall. Reported per size: that duration (mean, min, max),
//! its cost per row, the wall time from the `bsrc` commit to the page landing,
//! WAL bytes and Postgres CPU per row, the engine process's peak RSS, and the
//! **control**: the same rewrite as one hand-written set-based `UPDATE` of a
//! copy of the target (`create table ctl as select * from jt`), run on the
//! same cluster with the engine stopped, under the same checkpoint-first
//! rule. The engine's cost over the control is what the ledger (`R`, `M`,
//! `J`), the rendered upsert and the page's own bookkeeping add to a plain
//! `UPDATE ... FROM`.
//!
//! `--probe-reps` more reps each issue a change of an `a` row on the key 30 ms
//! after the `bsrc` commit and time it to landing on the target
//! (`a_probe_ms`), against the same probe on an idle key (`a_probe_idle_ms`):
//! what a writer on a hot key waits.
//!
//! ## `one-to-one-join-poison`
//!
//! A fan-out in which `P` rows of `a` fail their fields. The poisoned rows hold
//! `x = 2147483646`, so `asrc.x + COALESCE(bsrc.w, 0)` is in range for `w = 1` and an
//! `integer` overflow for `w = 2`, which the one `bsrc` update sets. The page
//! rolls the fan-out back and bisects its rows under savepoints down to the
//! failing ones (at most 256 attempts), holds each in quarantine and writes the
//! rest. Reported per `P`: the page's duration against the same fan-out with
//! `P = 0`, the held rows, whether the target matches its query for every row
//! but the held ones, and, when the bisection runs out of attempts, the halt.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::dev::defs::{IntWidth, ValueType, install_definition, set_one_to_one_join};

use crate::scenario::connect_raw;
use crate::streaming::chain::wait_for_live;
use crate::streaming::disk_tier;
use crate::streaming::idle_cost;
use crate::streaming::one_to_one_apply::PageHistogram;
use crate::streaming::process_memory::RssSampler;
use crate::streaming::scrape::{CHANGES_APPLIED_METRIC, counter_value, scrape};
use crate::streaming::server_cost;
use crate::streaming::tuning::EngineTuning;

const TARGET: &str = "jt";

/// The fields a run's definition selects, by name.
pub const SHAPES: &[&str] = &["light", "wide"];

/// `(select list, target column types, oracle select list over a and b)`.
///
/// `light` is one `integer` sum. `wide` adds a `text` pass-through, a
/// product and a comparison: four columns, the shape of a typical derived row.
fn shape(name: &str) -> (&'static str, Vec<(&'static str, ValueType)>, &'static str) {
    let int4 = ValueType::Integer(IntWidth::Int4);
    match name {
        "light" => (
            "asrc.x + COALESCE(bsrc.w, 0) AS total",
            vec![("total", int4)],
            "a.x + coalesce(b.w, 0)",
        ),
        "wide" => (
            "asrc.x + COALESCE(bsrc.w, 0) AS total, bsrc.tag AS tag, \
             asrc.x * 2 + COALESCE(bsrc.w, 0) AS dbl, (asrc.x + COALESCE(bsrc.w, 0)) > 500 AS big",
            vec![
                ("total", int4),
                ("tag", ValueType::Text),
                ("dbl", int4),
                ("big", ValueType::Boolean),
            ],
            "a.x + coalesce(b.w, 0), b.tag, a.x * 2 + coalesce(b.w, 0), \
             (a.x + coalesce(b.w, 0)) > 500",
        ),
        other => panic!("unknown --shape {other:?}; expected one of {SHAPES:?}"),
    }
}

/// Rows of `a` on keys other than the hot one, by default: enough for `J` to
/// hold more than a key.
pub const BACKGROUND_ROWS: i64 = 1_000;

/// The run's knobs, for both scenarios.
#[derive(Debug, Clone)]
pub struct OneToOneJoin {
    /// Fan-out sizes, in rows of `a` on the hot key.
    pub fan_outs: Vec<i64>,
    pub reps: usize,
    pub probe_reps: usize,
    pub shape: String,
    /// `one-to-one-join-poison`: failing rows per run.
    pub poisoned: Vec<i64>,
    /// `one-to-one-join-poison`: the fan-out size every run shares.
    pub poison_rows: i64,
    pub timeout: Duration,
    /// `--statements`: after the reps, print the busiest statements by total
    /// time (`pg_stat_statements`; see `idle-cost --statements` for the cluster
    /// settings). Use `--reps 1 --probe-reps 0` so only the fan-out is in it.
    pub statements: bool,
    /// `--background-rows`: rows of `a` on other keys (spread over 98 keys), which
    /// make `R` larger than the fan-out.
    pub background_rows: i64,
    /// `--analyze-hidden`: `ANALYZE` the definition's hidden tables once it is
    /// live, before the reps.
    pub analyze_hidden: bool,
}

/// A page's numbers, from `bsrc`'s commit to the page landing.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rep {
    pub page_secs: f64,
    pub wall_secs: f64,
    pub wal_bytes: i64,
    pub pg_cpu_secs: f64,
}

/// Creates the tables and fills them. `poison` rows of the hot key hold
/// `x = 2147483646`, spread evenly over the key's ids.
async fn load(raw: &RawClient, fan_out: i64, poison: i64, background_rows: i64) {
    raw.batch_execute(
        "create table public.asrc (id integer primary key, k integer, x integer); \
         create table public.bsrc (id integer primary key, w integer, tag text); \
         insert into public.bsrc select g, 1, 'b' || g from generate_series(1, 100) g",
    )
    .await
    .expect("create the sources");
    let stride = if poison > 0 { fan_out / poison } else { 0 };
    raw.execute(
        "insert into public.asrc \
         select g, 1, case when $2::bigint > 0 and g % $2::bigint = 0 then 2147483646 \
                           else (g % 1000)::integer end \
         from generate_series(1, $1::bigint) g",
        &[&fan_out, &stride],
    )
    .await
    .expect("load the hot key");
    raw.execute(
        "insert into public.asrc \
         select $1::bigint + g, 2 + (g % 98), g from generate_series(1, $2::bigint) g",
        &[&fan_out, &background_rows],
    )
    .await
    .expect("load the background keys");
    raw.batch_execute("analyze public.asrc; analyze public.bsrc")
        .await
        .expect("analyze");
}

/// What a fresh cluster with the sources, the engine and a live definition
/// holds.
struct Fixture {
    cluster: TestCluster,
    db: testkit::TestDatabase,
    raw: RawClient,
    client: trellis::Client,
    build_secs: f64,
}

async fn fixture(cfg: &OneToOneJoin, tuning: &EngineTuning, fan_out: i64, poison: i64) -> Fixture {
    set_one_to_one_join(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    load(&raw, fan_out, poison, cfg.background_rows).await;
    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    let (fields, columns, _) = shape(&cfg.shape);
    let columns: HashMap<String, ValueType> = columns
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect();
    let text = format!(
        "TRANSFORM {TARGET} FROM public.asrc LEFT JOIN public.bsrc ON asrc.k = bsrc.id \
         SELECT {fields}"
    );
    let started = Instant::now();
    install_definition(&db.pool, &text, &columns, "public")
        .await
        .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    wait_for_live(&raw, TARGET, Instant::now() + cfg.timeout).await;
    let build_secs = started.elapsed().as_secs_f64();
    if cfg.analyze_hidden {
        let tables: Vec<String> = raw
            .query(
                "select format('%I.%I', n.nspname, c.relname) from pg_class c \
                 join pg_namespace n on n.oid = c.relnamespace \
                 where c.relname ~ '^join1_[0-9]+_[rmj]$'",
                &[],
            )
            .await
            .expect("list the hidden tables")
            .iter()
            .map(|r| r.get(0))
            .collect();
        for table in tables {
            raw.batch_execute(&format!("analyze {table}"))
                .await
                .expect("analyze a hidden table");
        }
    }
    Fixture {
        cluster,
        db,
        raw,
        client,
        build_secs,
    }
}

/// The definition's status text.
async fn status(raw: &RawClient) -> String {
    raw.query_one(
        "select status::text from transform_definitions where target_table = $1",
        &[&format!("public.{TARGET}")],
    )
    .await
    .expect("read status")
    .get(0)
}

/// Rows of the target that differ from the join written over the sources.
async fn mismatches(raw: &RawClient, shape_name: &str) -> i64 {
    let (_, columns, oracle) = shape(shape_name);
    let names: Vec<&str> = columns.iter().map(|(n, _)| *n).collect();
    raw.query_one(
        &format!(
            "select count(*) from \
               (select a.id, row({oracle})::text as v from public.asrc a \
                left join public.bsrc b on a.k = b.id) s \
             full join (select id, row({})::text as v from public.{TARGET}) t using (id) \
             where s.v is distinct from t.v",
            names.join(", ")
        ),
        &[],
    )
    .await
    .expect("oracle")
    .get(0)
}

/// Rows of the target whose `total` differs from the `light` join written in
/// `bigint`, which a poisoned row cannot overflow.
async fn total_mismatches(raw: &RawClient) -> i64 {
    raw.query_one(
        &format!(
            "select count(*) from public.asrc a \
             left join public.bsrc b on a.k = b.id \
             full join public.{TARGET} t on t.id = a.id \
             where t.total::bigint is distinct from a.x::bigint + coalesce(b.w, 0)"
        ),
        &[],
    )
    .await
    .expect("bigint oracle")
    .get(0)
}

/// What a page landing needs: one page more than `before`, or the definition
/// leaving `live`.
enum Landed {
    Page,
    Left(String),
    TimedOut,
}

async fn wait_page(
    raw: &RawClient,
    before: &PageHistogram,
    applied: u64,
    timeout: Duration,
) -> Landed {
    let deadline = Instant::now() + timeout;
    let mut last_status_check = Instant::now();
    loop {
        // A page that failed (a halt rolls the page back) records no applied change.
        let rendered = scrape();
        if PageHistogram::capture(&rendered).since(before).count >= 1
            && counter_value(&rendered, CHANGES_APPLIED_METRIC, TARGET) > applied
        {
            return Landed::Page;
        }
        if last_status_check.elapsed() > Duration::from_millis(100) {
            last_status_check = Instant::now();
            let s = status(raw).await;
            if s != "live" {
                return Landed::Left(s);
            }
        }
        if Instant::now() > deadline {
            return Landed::TimedOut;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// `checkpoint`, so every rep starts with the same full-page-write state.
async fn checkpoint(raw: &RawClient) {
    raw.batch_execute("checkpoint").await.expect("checkpoint");
}

/// One `update bsrc` and the wait for its page.
async fn one_rep(f: &Fixture, timeout: Duration) -> (Rep, Landed) {
    checkpoint(&f.raw).await;
    let rendered = scrape();
    let before = PageHistogram::capture(&rendered);
    let applied = counter_value(&rendered, CHANGES_APPLIED_METRIC, TARGET);
    let disk = disk_tier::sample(&f.raw).await;
    let cost = server_cost::start(&f.cluster);
    let started = Instant::now();
    f.raw
        .batch_execute("update public.bsrc set w = w + 1 where id = 1")
        .await
        .expect("update b");
    let landed = wait_page(&f.raw, &before, applied, timeout).await;
    let wall_secs = started.elapsed().as_secs_f64();
    let pages = PageHistogram::capture(&scrape()).since(&before);
    let cost = cost.finish(&f.raw).await;
    let disk = disk_tier::since(&f.raw, &disk).await;
    (
        Rep {
            page_secs: pages.sum,
            wall_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        },
        landed,
    )
}

/// The wall time of one change of an `a` row, committed to landing on the
/// target, polling the one target row.
async fn a_probe(raw: &RawClient, id: i32, timeout: Duration) -> f64 {
    let expected: i32 = raw
        .query_one(
            "update public.asrc a set x = x + 1 where id = $1 returning \
             a.x + coalesce((select w from public.bsrc where id = a.k), 0)",
            &[&id],
        )
        .await
        .expect("update a")
        .get(0);
    let started = Instant::now();
    loop {
        let total: Option<i32> = raw
            .query_opt(
                &format!("select total from public.{TARGET} where id = $1"),
                &[&id],
            )
            .await
            .expect("read the target row")
            .map(|r| r.get(0));
        if total == Some(expected) {
            return started.elapsed().as_secs_f64();
        }
        assert!(
            started.elapsed() < timeout,
            "the probe change of a row never landed"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The ten busiest statements since the last reset, one JSON line: calls,
/// total and mean milliseconds, rows, WAL bytes and the first 300 characters.
async fn top_statements(raw: &RawClient, fan_out: i64) -> String {
    raw.query_one(
        "select jsonb_build_object('scenario', 'one-to-one-join-fan-out-statements', \
                'fan_out', $1::bigint, 'rows', coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb))::text \
         from (select calls, round(total_exec_time::numeric, 1)::float8 as total_ms, \
                      round(mean_exec_time::numeric, 2)::float8 as mean_ms, rows, \
                      wal_bytes::bigint as wal_bytes, left(regexp_replace(query, '\\s+', ' ', 'g'), 300) as query \
               from pg_stat_statements where query not like '%pg_stat_statements%' \
               order by total_exec_time desc limit 10) t",
        &[&fan_out],
    )
    .await
    .expect("read pg_stat_statements")
    .get(0)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    if v.is_empty() {
        f64::NAN
    } else {
        v[v.len() / 2]
    }
}

/// The set-based control: `reps` timed `UPDATE ... FROM` of a copy of the
/// target's hot-key rows, the engine stopped.
async fn control(
    cluster: &TestCluster,
    raw: &RawClient,
    fan_out: i64,
    reps: usize,
    shape_name: &str,
) -> Vec<Rep> {
    let (_, columns, _) = shape(shape_name);
    let sets: Vec<String> = match shape_name {
        "light" => vec!["total = a.x + coalesce(b.w, 0)".to_string()],
        _ => vec![
            "total = a.x + coalesce(b.w, 0)".into(),
            "tag = b.tag".into(),
            "dbl = a.x * 2 + coalesce(b.w, 0)".into(),
            "big = (a.x + coalesce(b.w, 0)) > 500".into(),
        ],
    };
    assert_eq!(sets.len(), columns.len());
    raw.batch_execute(&format!(
        "create table public.ctl as select * from public.{TARGET}; \
             alter table public.ctl add primary key (id); analyze public.ctl"
    ))
    .await
    .expect("copy the target");
    let mut out = Vec::new();
    for _ in 0..reps {
        checkpoint(raw).await;
        let disk = disk_tier::sample(raw).await;
        let cost = server_cost::start(cluster);
        let started = Instant::now();
        let n = raw
            .execute(
                &format!(
                    "update public.ctl c set {} from public.asrc a \
                     left join public.bsrc b on a.k = b.id \
                     where a.id = c.id and a.k = 1",
                    sets.join(", ")
                ),
                &[],
            )
            .await
            .expect("control update");
        assert_eq!(n as i64, fan_out, "the control rewrites the fan-out");
        let wall_secs = started.elapsed().as_secs_f64();
        let cost = cost.finish(raw).await;
        let disk = disk_tier::since(raw, &disk).await;
        out.push(Rep {
            page_secs: wall_secs,
            wall_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        });
    }
    out
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len().max(1) as f64
}

fn min(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn max(v: &[f64]) -> f64 {
    v.iter().cloned().fold(0.0, f64::max)
}

/// One fan-out size's numbers.
#[derive(Debug, Clone)]
pub struct FanOutResult {
    pub fan_out: i64,
    pub shape: String,
    pub build_secs: f64,
    pub reps: Vec<Rep>,
    pub control: Vec<Rep>,
    pub a_probe_secs: Vec<f64>,
    pub a_probe_idle_secs: Vec<f64>,
    pub peak_rss_mb: f64,
    pub oracle_mismatches: i64,
    pub landed: bool,
    pub background_rows: i64,
    pub analyze_hidden: bool,
}

impl FanOutResult {
    pub fn to_json(&self) -> String {
        let f = self.fan_out as f64;
        let col = |reps: &[Rep], g: fn(&Rep) -> f64| reps.iter().map(g).collect::<Vec<_>>();
        let page = col(&self.reps, |r| r.page_secs);
        let wall = col(&self.reps, |r| r.wall_secs);
        let wal = col(&self.reps, |r| r.wal_bytes as f64);
        let cpu = col(&self.reps, |r| r.pg_cpu_secs);
        let c_page = col(&self.control, |r| r.page_secs);
        let c_wal = col(&self.control, |r| r.wal_bytes as f64);
        let c_cpu = col(&self.control, |r| r.pg_cpu_secs);
        format!(
            "{{\"scenario\":\"one-to-one-join-fan-out\",\"shape\":\"{}\",\"fan_out\":{},\"background_rows\":{},\"analyze_hidden\":{},\
             \"build_secs\":{:.2},\"build_us_per_row\":{:.2},\"reps\":{},\
             \"page_secs_mean\":{:.4},\"page_secs_min\":{:.4},\"page_secs_max\":{:.4},\
             \"page_us_per_row\":{:.3},\"wall_secs_mean\":{:.4},\
             \"wal_bytes_per_row\":{:.1},\"pg_cpu_us_per_row\":{:.3},\
             \"control_secs_mean\":{:.4},\"control_us_per_row\":{:.3},\
             \"control_wal_bytes_per_row\":{:.1},\"control_pg_cpu_us_per_row\":{:.3},\
             \"engine_over_control\":{:.2},\
             \"a_probe_secs_median\":{:.4},\"a_probe_idle_secs_median\":{:.4},\
             \"peak_rss_mb\":{:.1},\"landed\":{},\"oracle_mismatches\":{}}}",
            self.shape,
            self.fan_out,
            self.background_rows,
            self.analyze_hidden,
            self.build_secs,
            self.build_secs * 1e6 / (f + self.background_rows as f64),
            self.reps.len(),
            mean(&page),
            min(&page),
            max(&page),
            mean(&page) * 1e6 / f,
            mean(&wall),
            mean(&wal) / f,
            mean(&cpu) * 1e6 / f,
            mean(&c_page),
            mean(&c_page) * 1e6 / f,
            mean(&c_wal) / f,
            mean(&c_cpu) * 1e6 / f,
            mean(&page) / mean(&c_page).max(f64::EPSILON),
            median(self.a_probe_secs.clone()),
            median(self.a_probe_idle_secs.clone()),
            self.peak_rss_mb,
            self.landed,
            self.oracle_mismatches,
        )
    }
}

/// One fan-out size on a fresh cluster.
pub async fn run_fan_out(cfg: &OneToOneJoin, tuning: &EngineTuning, fan_out: i64) -> FanOutResult {
    let f = fixture(cfg, tuning, fan_out, 0).await;
    let rss = RssSampler::start(Duration::from_millis(100), None);
    let idle: Vec<f64> = {
        let mut v = Vec::new();
        for _ in 0..cfg.probe_reps.max(3) {
            // A row off the hot key: no fan-out is anywhere near it.
            v.push(a_probe(&f.raw, (fan_out + 1) as i32, cfg.timeout).await);
        }
        v
    };
    if cfg.statements {
        idle_cost::reset_statements(&f.raw).await;
    }
    // The last probe's page records its duration just after its row is visible.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut reps = Vec::new();
    let mut landed = true;
    for _ in 0..cfg.reps {
        let (rep, l) = one_rep(&f, cfg.timeout).await;
        landed &= matches!(l, Landed::Page);
        reps.push(rep);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // The a-writer's wait: a change of a row on the hot key 30 ms into a fan-out.
    let mut probes = Vec::new();
    for _ in 0..cfg.probe_reps {
        checkpoint(&f.raw).await;
        let before = PageHistogram::capture(&scrape());
        f.raw
            .batch_execute("update public.bsrc set w = w + 1 where id = 1")
            .await
            .expect("update b");
        tokio::time::sleep(Duration::from_millis(30)).await;
        probes.push(a_probe(&f.raw, 2, cfg.timeout).await);
        // Both pages (the fan-out and the probe) are done before the next rep.
        let deadline = Instant::now() + cfg.timeout;
        while PageHistogram::capture(&scrape()).since(&before).count < 2 {
            assert!(
                Instant::now() < deadline,
                "the probe reps' pages never landed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    if cfg.statements {
        println!("{}", top_statements(&f.raw, fan_out).await);
    }
    let peak_rss_mb = rss.finish().peak_bytes as f64 / 1e6;
    let oracle_mismatches = mismatches(&f.raw, &cfg.shape).await;
    let Fixture {
        cluster,
        db: _db,
        raw,
        client,
        build_secs,
    } = f;
    client.shutdown().await.expect("client shutdown");
    let control = control(&cluster, &raw, fan_out, cfg.reps, &cfg.shape).await;
    FanOutResult {
        fan_out,
        shape: cfg.shape.clone(),
        build_secs,
        reps,
        control,
        a_probe_secs: probes,
        a_probe_idle_secs: idle,
        peak_rss_mb,
        oracle_mismatches,
        landed,
        background_rows: cfg.background_rows,
        analyze_hidden: cfg.analyze_hidden,
    }
}

/// Runs every fan-out size in `cfg`, one fresh cluster each.
pub async fn run_fan_outs(cfg: &OneToOneJoin, tuning: &EngineTuning) -> Vec<FanOutResult> {
    let mut results = Vec::new();
    for &fan_out in &cfg.fan_outs {
        let r = run_fan_out(cfg, tuning, fan_out).await;
        println!("{}", r.to_json());
        results.push(r);
    }
    results
}

/// One poisoned fan-out's numbers.
#[derive(Debug, Clone)]
pub struct PoisonResult {
    pub rows: i64,
    pub poisoned: i64,
    pub shape: String,
    pub rep: Rep,
    pub outcome: String,
    pub held: i64,
    pub mismatches: i64,
    pub build_secs: f64,
}

impl PoisonResult {
    pub fn to_json(&self) -> String {
        // A page that did not commit has no duration worth reporting.
        let committed = self.outcome == "committed";
        let or_null = |v: f64, places: usize| {
            if committed {
                format!("{v:.places$}")
            } else {
                "null".to_string()
            }
        };
        format!(
            "{{\"scenario\":\"one-to-one-join-poison\",\"shape\":\"{}\",\"fan_out\":{},\
             \"poisoned\":{},\"outcome\":\"{}\",\"page_secs\":{},\"wall_secs\":{:.4},\
             \"page_us_per_row\":{},\"wal_bytes_per_row\":{:.1},\"pg_cpu_us_per_row\":{:.3},\
             \"held_rows\":{},\"target_rows_off\":{},\"build_secs\":{:.2}}}",
            self.shape,
            self.rows,
            self.poisoned,
            self.outcome,
            or_null(self.rep.page_secs, 4),
            self.rep.wall_secs,
            or_null(self.rep.page_secs * 1e6 / self.rows as f64, 3),
            self.rep.wal_bytes as f64 / self.rows as f64,
            self.rep.pg_cpu_secs * 1e6 / self.rows as f64,
            self.held,
            self.mismatches,
            self.build_secs,
        )
    }
}

/// One poisoned fan-out on a fresh cluster: `poisoned` rows of `cfg.poison_rows`
/// fail when `w` goes from 1 to 2.
pub async fn run_poison(cfg: &OneToOneJoin, tuning: &EngineTuning, poisoned: i64) -> PoisonResult {
    let rows = cfg.poison_rows;
    // `wide`'s `a.x * 2` overflows on a poisoned row at the build.
    let cfg = &OneToOneJoin {
        shape: "light".to_string(),
        ..cfg.clone()
    };
    let f = fixture(cfg, tuning, rows, poisoned).await;
    checkpoint(&f.raw).await;
    let rendered = scrape();
    let before = PageHistogram::capture(&rendered);
    let applied = counter_value(&rendered, CHANGES_APPLIED_METRIC, TARGET);
    let disk = disk_tier::sample(&f.raw).await;
    let cost = server_cost::start(&f.cluster);
    let started = Instant::now();
    f.raw
        .batch_execute("update public.bsrc set w = 2 where id = 1")
        .await
        .expect("update b");
    let landed = wait_page(&f.raw, &before, applied, cfg.timeout).await;
    let wall_secs = started.elapsed().as_secs_f64();
    let pages = PageHistogram::capture(&scrape()).since(&before);
    let cost = cost.finish(&f.raw).await;
    let disk = disk_tier::since(&f.raw, &disk).await;
    let outcome = match landed {
        Landed::Page => "committed".to_string(),
        Landed::Left(s) => format!("left live: {s}"),
        Landed::TimedOut => "timed out".to_string(),
    };
    // The held rows keep the total they had; every other row has the new one.
    let held: i64 = f
        .raw
        .query_one("select count(*) from poison", &[])
        .await
        .map_or(-1, |r| r.get(0));
    let off = total_mismatches(&f.raw).await;
    let build_secs = f.build_secs;
    let _ = f.client.shutdown().await;
    PoisonResult {
        rows,
        poisoned,
        shape: cfg.shape.clone(),
        rep: Rep {
            page_secs: pages.sum,
            wall_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        },
        outcome,
        held,
        mismatches: off,
        build_secs,
    }
}

/// Runs every poison count in `cfg`, one fresh cluster each.
pub async fn run_poisons(cfg: &OneToOneJoin, tuning: &EngineTuning) -> Vec<PoisonResult> {
    let mut results = Vec::new();
    for &p in &cfg.poisoned {
        let r = run_poison(cfg, tuning, p).await;
        println!("{}", r.to_json());
        results.push(r);
    }
    results
}
