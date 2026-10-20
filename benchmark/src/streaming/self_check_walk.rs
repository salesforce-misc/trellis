//! `self-check-walk`: what one `self_check` walk costs as the table grows
//! (#1178).
//!
//! Seeds a `bigint`-keyed source of `--rows` rows, defines either a 1-1
//! transform (`--kind one-to-one`) or a group-basis aggregate over
//! `--groups` groups (`--kind aggregate`), waits for it to go live, then
//! runs the real `Trellis::self_check` job to its end and reports wall time,
//! pages (`ceil(rows / page)`) and the sequential tuples Postgres read from
//! the source and the target (`pg_stat_user_tables.seq_tup_read` deltas), so
//! rows read per page is `seq_tup_read / pages`.
//!
//! `--kind join` (#1198) instead walks the `P` and `M` parts of a JOIN target
//! (`FROM a JOIN b ON a.bid = b.id GROUP BY a.cat, b.region`, `a` indexed on
//! `bid`, `--rows` rows of `a`, a tenth as many of `b`) with `Standard`
//! pages of 10 000 keys, each part timed apart, and reports the rows read
//! from the hidden table and the source of each (sequential tuples plus index
//! fetches) per page. `--hidden-only-old` is not a flag: run it on a commit.
//!
//! `--probe-pages K` (1-1 only) instead times K single pages of today's
//! statement shape directly (the source side's keyset read and the target
//! side's, spread across the keyspace), without running the job: for a table
//! too big for a whole walk to finish. A page is `where key-text > $cursor
//! order by key-text limit 10000`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;
use crate::streaming::chain::{numeric_columns, wait_for_live};
use crate::streaming::tuning::EngineTuning;

const PAGE_KEYS: i64 = 10_000;

#[derive(Debug, Clone)]
pub struct Config {
    pub rows: i64,
    pub aggregate: bool,
    /// `--kind join` (#1198): walk a JOIN target's `P` and `M` parts.
    pub join: bool,
    pub groups: i64,
    pub probe_pages: usize,
    /// Run the walk too after probing (`--walk-after-probe`).
    pub walk_after_probe: bool,
    /// Index the aggregate's source on its group column (`--index`).
    pub index: bool,
    pub build_timeout: Duration,
    pub walk_timeout: Duration,
}

async fn seq_tup_read(raw: &RawClient, table: &str) -> i64 {
    // Stats are flushed lazily; give the collector a moment.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    raw.query_one(
        "select coalesce(sum(seq_tup_read), 0)::bigint from pg_stat_user_tables where relname = $1",
        &[&table],
    )
    .await
    .expect("read seq_tup_read")
    .get(0)
}

pub async fn run(cfg: Config, tuning: &EngineTuning) {
    if cfg.join {
        return run_join(cfg, tuning).await;
    }
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let seed_start = Instant::now();
    if cfg.aggregate {
        raw.batch_execute(
            "create table public.sc_src (id bigint primary key, grp integer not null, amt bigint not null)",
        )
        .await
        .expect("seed aggregate source");
    } else {
        raw.batch_execute(&format!(
            "create table public.sc_src (id bigint primary key, val numeric); \
             insert into public.sc_src select g, g from generate_series(1, {}::bigint) g",
            cfg.rows
        ))
        .await
        .expect("seed source");
    }
    eprintln!(
        "seeded {} rows in {:.1}s",
        cfg.rows,
        seed_start.elapsed().as_secs_f64()
    );

    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    let (text, columns): (String, HashMap<_, _>) = if cfg.aggregate {
        (
            "TRANSFORM sc_tgt FROM public.sc_src GROUP BY grp \
             SELECT grp AS grp, SUM(amt) AS total, COUNT(*) AS n"
                .to_string(),
            {
                use trellis::IntWidth::{Int4, Int8};
                use trellis::dev::defs::ValueType::Integer;
                HashMap::from([
                    ("id".to_string(), Integer(Int8)),
                    ("grp".to_string(), Integer(Int4)),
                    ("amt".to_string(), Integer(Int8)),
                ])
            },
        )
    } else {
        (
            "TRANSFORM sc_tgt FROM public.sc_src SELECT val AS val".to_string(),
            numeric_columns(&["id", "val"]),
        )
    };
    if cfg.aggregate && cfg.index {
        raw.batch_execute("create index sc_src_grp on public.sc_src (grp)")
            .await
            .expect("index the group column");
    }
    let build_start = Instant::now();
    trellis::dev::defs::install_definition(&db.pool, &text, &columns, "public")
        .await
        .expect("install definition");
    wait_for_live(&raw, "sc_tgt", Instant::now() + cfg.build_timeout).await;
    if cfg.aggregate {
        // A group-basis build over a populated source isn't built yet (#1101):
        // the rows arrive through capture instead.
        let batch = 100_000i64;
        let mut lo = 1i64;
        while lo <= cfg.rows {
            let hi = (lo + batch - 1).min(cfg.rows);
            raw.batch_execute(&format!(
                "insert into public.sc_src select g, (g % {})::integer, g from generate_series({lo}::bigint, {hi}::bigint) g",
                cfg.groups
            ))
            .await
            .expect("seed aggregate source");
            lo = hi + 1;
        }
        loop {
            let n: i64 = raw
                .query_one("select coalesce(sum(n), 0)::bigint from public.sc_tgt", &[])
                .await
                .expect("read target")
                .get(0);
            if n == cfg.rows {
                break;
            }
            assert!(
                build_start.elapsed() < cfg.build_timeout,
                "aggregate never converged"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    let build_secs = build_start.elapsed().as_secs_f64();
    eprintln!("built and live in {build_secs:.1}s");
    // Every table, the hidden ones too: autovacuum would get to them, and a
    // plan that changes under the walk when it does is no measurement.
    raw.batch_execute("vacuum analyze")
        .await
        .expect("vacuum analyze");

    if cfg.probe_pages > 0 {
        assert!(!cfg.aggregate, "--probe-pages is 1-1 only");
        probe(&raw, &cfg).await;
        if !cfg.walk_after_probe {
            client.shutdown().await.expect("shutdown");
            return;
        }
    }

    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(db.dsn().to_string()).expect("dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect Trellis");

    settle(&raw).await;
    let src_before = seq_tup_read(&raw, "sc_src").await;
    let tgt_before = seq_tup_read(&raw, "sc_tgt").await;
    let start = Instant::now();
    let job = trellis
        .self_check(
            "sc_tgt",
            trellis::SelfCheckMode::Standard,
            Duration::from_secs(120),
        )
        .await
        .expect("start self_check");
    let mut last_log = Instant::now();
    let job = loop {
        let j = trellis
            .self_check_job(job.id)
            .await
            .expect("poll")
            .expect("job exists");
        if j.state.is_finished() {
            break j;
        }
        assert!(
            start.elapsed() < cfg.walk_timeout,
            "walk exceeded --walk-timeout-secs"
        );
        if last_log.elapsed() > Duration::from_secs(30) {
            eprintln!(
                "  {:.0}s: {} keys compared",
                start.elapsed().as_secs_f64(),
                j.rows_compared
            );
            last_log = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let walk_secs = start.elapsed().as_secs_f64();
    let src_delta = seq_tup_read(&raw, "sc_src").await - src_before;
    let tgt_delta = seq_tup_read(&raw, "sc_tgt").await - tgt_before;
    let keys = job.rows_compared;
    let pages = (keys + PAGE_KEYS - 1) / PAGE_KEYS + 1;
    println!(
        "{{\"scenario\":\"self-check-walk\",\"indexed\":{},\"kind\":\"{}\",\"rows\":{},\"groups\":{},\
         \"keys_compared\":{},\"state\":\"{}\",\"outcome\":\"{:?}\",\"walk_secs\":{:.2},\
         \"pages_est\":{},\"src_seq_tup_read\":{},\"tgt_seq_tup_read\":{},\
         \"src_rows_per_page\":{},\"build_secs\":{:.1}}}",
        cfg.index,
        if cfg.aggregate {
            "aggregate"
        } else {
            "one-to-one"
        },
        cfg.rows,
        if cfg.aggregate { cfg.groups } else { 0 },
        keys,
        job.state.as_str(),
        job.report.as_ref().map(|r| &r.outcome),
        walk_secs,
        pages,
        src_delta,
        tgt_delta,
        src_delta / pages.max(1),
        build_secs,
    );
    client.shutdown().await.expect("shutdown");
}

/// Waits until the engine's own background work (draining, retiring ring
/// segments, vacuum) has been idle for several seconds, so the walk is
/// measured alone.
async fn settle(raw: &RawClient) {
    let mut quiet = 0;
    for _ in 0..600 {
        let busy: i64 = raw
            .query_one(
                "select count(*)::bigint from pg_stat_activity \
                 where state = 'active' and pid <> pg_backend_pid() \
                   and backend_type in ('client backend', 'autovacuum worker')",
                &[],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        quiet = if busy == 0 { quiet + 1 } else { 0 };
        if quiet >= 10 {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Times `cfg.probe_pages` pages of the 1-1 statement shape, evenly spread.
async fn probe(raw: &RawClient, cfg: &Config) {
    let page = |table: &str, val: &str| {
        format!(
            "select (id::text) collate \"C\" as k, {val}::text as v from {table} \
             where ($1::text is null or (id::text) collate \"C\" > $1) \
             order by (id::text) collate \"C\" limit {PAGE_KEYS}"
        )
    };
    let src_sql = page("public.sc_src", "val");
    let tgt_sql = page("public.sc_tgt", "val");
    let total_pages = (cfg.rows + PAGE_KEYS - 1) / PAGE_KEYS;
    let mut times = Vec::new();
    for i in 0..cfg.probe_pages {
        // The key text a page starts after: the id at that fraction of the table.
        let id = 1 + (cfg.rows * i as i64) / cfg.probe_pages as i64;
        let after: Option<String> = if i == 0 { None } else { Some(id.to_string()) };
        let t = Instant::now();
        let a = raw
            .query(src_sql.as_str(), &[&after])
            .await
            .expect("src page");
        let s = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let b = raw
            .query(tgt_sql.as_str(), &[&after])
            .await
            .expect("tgt page");
        let g = t.elapsed().as_secs_f64();
        eprintln!(
            "probe page {i}: source {s:.2}s ({} rows), target {g:.2}s ({} rows)",
            a.len(),
            b.len()
        );
        times.push(s + g);
    }
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    println!(
        "{{\"scenario\":\"self-check-walk-probe\",\"rows\":{},\"probes\":{},\"mean_page_secs\":{:.3},\
         \"pages\":{},\"projected_walk_secs\":{:.0}}}",
        cfg.rows,
        times.len(),
        mean,
        total_pages,
        mean * total_pages as f64
    );
}

/// `--kind join`: the `P` and `M` walks of a JOIN target (the module doc).
async fn run_join(cfg: Config, tuning: &EngineTuning) {
    use trellis::IntWidth::{Int4, Int8};
    use trellis::dev::defs::ValueType::Integer;
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let keys = (cfg.rows / 10).max(1);
    raw.batch_execute(
        "create table public.a (id bigint primary key, bid integer not null, \
             cat integer not null, amt bigint not null); \
         create table public.b (id integer primary key, region integer not null, w bigint); \
         create index a_bid on public.a (bid)",
    )
    .await
    .expect("create the tables");
    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    let columns: HashMap<_, _> = HashMap::from([
        ("id".to_string(), Integer(Int8)),
        ("bid".to_string(), Integer(Int4)),
        ("cat".to_string(), Integer(Int4)),
        ("amt".to_string(), Integer(Int8)),
    ]);
    trellis::dev::defs::install_definition(
        &db.pool,
        "TRANSFORM jt FROM public.a JOIN public.b ON a.bid = b.id GROUP BY a.cat, b.region \
         SELECT a.cat AS cat, b.region AS region, COUNT(*) AS n, SUM(a.amt) AS sa",
        &columns,
        "public",
    )
    .await
    .expect("install definition");
    wait_for_live(&raw, "jt", Instant::now() + cfg.build_timeout).await;
    // The rows arrive through capture, `b` first. Every `a` row matches one
    // `b` row; `a` has about `rows / 2` distinct `(bid, cat)` cells.
    let load_start = Instant::now();
    raw.batch_execute(&format!(
        "insert into public.b select g, (g % 5)::integer, g from generate_series(1, {keys}) g"
    ))
    .await
    .expect("load b");
    let batch = 100_000i64;
    let mut lo = 1i64;
    while lo <= cfg.rows {
        let hi = (lo + batch - 1).min(cfg.rows);
        raw.batch_execute(&format!(
            "insert into public.a select g, (g % {keys} + 1)::integer, \
                 ((g / {keys}) % 5)::integer, g from generate_series({lo}::bigint, {hi}::bigint) g"
        ))
        .await
        .expect("load a");
        lo = hi + 1;
    }
    loop {
        let n: i64 = raw
            .query_one("select coalesce(sum(n), 0)::bigint from public.jt", &[])
            .await
            .expect("read target")
            .get(0);
        if n == cfg.rows {
            break;
        }
        assert!(
            load_start.elapsed() < cfg.build_timeout,
            "the join never converged"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    eprintln!("loaded in {:.1}s", load_start.elapsed().as_secs_f64());
    raw.batch_execute("vacuum analyze")
        .await
        .expect("vacuum analyze");
    let id: i64 = raw
        .query_one(
            "select id from trellis.transform_definitions where target_table = 'public.jt'",
            &[],
        )
        .await
        .expect("definition id")
        .get(0);
    settle(&raw).await;

    // Rows a table has served so far: sequential tuples and index fetches.
    let rows_read = |table: String| {
        let raw = &raw;
        // `~` against the name: a table name is anchored, `seg_` is the ring's
        // segments.
        let table = if table == "ring" {
            "^seg_[0-9]+$".to_string()
        } else {
            format!("^{table}$")
        };
        async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            let r: i64 = raw
                .query_one(
                    "select coalesce(sum(seq_tup_read + idx_tup_fetch), 0)::bigint \
                     from pg_stat_user_tables where relname ~ $1",
                    &[&table],
                )
                .await
                .expect("read the table stats")
                .get(0);
            r
        }
    };
    for (part, hidden, source, next) in [
        ("p", format!("join_{id}_p"), "a", "m"),
        ("m", format!("join_{id}_m"), "b", "j"),
    ] {
        let (hidden_before, source_before, ring_before) = (
            rows_read(hidden.clone()).await,
            rows_read(source.to_string()).await,
            rows_read("ring".to_string()).await,
        );
        let start = Instant::now();
        let (mut after, mut pages, mut compared) = (Some(part.to_string()), 0i64, 0i64);
        while let Some(cursor) = after.take() {
            let page = trellis::dev::staging::self_check_page(
                &db.pool,
                "jt",
                trellis::dev::staging::SelfCheckScope {
                    after: Some(cursor),
                    limit: PAGE_KEYS,
                },
                trellis::SelfCheckMode::Standard,
                Duration::from_secs(120),
            )
            .await
            .expect("a page of the check");
            assert!(
                matches!(page.outcome, trellis::SelfCheckOutcome::Converged),
                "{:?}",
                page.outcome
            );
            pages += 1;
            compared += page.rows_compared;
            assert!(
                start.elapsed() < cfg.walk_timeout,
                "walk exceeded --walk-timeout-secs"
            );
            after = match page.next_after {
                Some(c) if c.starts_with(&format!("{part}:")) => Some(c),
                Some(c) => {
                    assert!(c == next, "the part ended at {c:?}");
                    None
                }
                None => None,
            };
        }
        let secs = start.elapsed().as_secs_f64();
        let hidden_read = rows_read(hidden).await - hidden_before;
        let source_read = rows_read(source.to_string()).await - source_before;
        let ring_read = rows_read("ring".to_string()).await - ring_before;
        println!(
            "{{\"scenario\":\"self-check-walk\",\"kind\":\"join\",\"part\":\"{part}\",\
             \"rows\":{},\"keys_compared\":{compared},\"pages\":{pages},\"walk_secs\":{secs:.2},\
             \"hidden_rows_read\":{hidden_read},\"source_rows_read\":{source_read},\"ring_rows_read\":{ring_read},\
             \"hidden_rows_per_page\":{},\"source_rows_per_page\":{}}}",
            cfg.rows,
            hidden_read / pages.max(1),
            source_read / pages.max(1),
        );
    }
    client.shutdown().await.expect("shutdown");
}
