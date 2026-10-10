//! Idle cost — epic #269's **V-IDLE** (issue #268's X6): what a fully idle
//! install costs the database. A staging worker plus N drain workers, a
//! captured source table that is never written to, the one 1-1 transform
//! that makes the staging worker capture it (issue #427), and nothing else —
//! sampled for transactions/sec, WAL bytes/sec and seals/sec.
//!
//! All three come **from Postgres directly**, not from new engine metrics:
//!
//! * transactions/sec — `pg_stat_database.xact_commit` delta,
//! * WAL bytes/sec — `pg_current_wal_lsn()` delta, and
//! * seals/sec — `segment_pointer.active_seq`, which advances by exactly 1 per
//!   successful seal (`seal_phase1`'s `next_seg_seq = active_seq + 1`), so a
//!   before/after delta is an exact count rather than an estimate.
//!
//! This is the measurement that catches a latency fix which quietly pays for
//! itself in background load: if a change improves latency but this gets
//! worse, that's a trade and should be reported as one.
//!
//! **On "queries/sec"**: #268's prediction is phrased in queries/sec, but
//! there is no query counter without `pg_stat_statements`, which this
//! harness's ephemeral cluster doesn't preload unless asked to (see
//! "Per-statement attribution" below). In this regime almost every
//! statement the wake path issues (`register_drainer`,
//! `next_claimable_segments`, the maintenance tick's reads) runs as its own
//! autocommit round trip rather than batched inside an explicit transaction,
//! so `xact_commit`'s delta is a reasonable stand-in — which is why this
//! reports `xact_commit_per_sec` and nothing that pretends to be a second,
//! independently measured number.
//!
//! ## Per-statement attribution (issue #350)
//!
//! `--statements` turns the window into an attribution run: it creates
//! `pg_stat_statements`, resets it right before the window and reads every
//! statement's calls, rows and WAL right after, as one extra JSON line
//! (`"scenario":"idle-cost-statements"`). The cluster must preload the
//! library, which the testkit passes through from its environment:
//!
//! ```text
//! TRELLIS_TESTKIT_PG_OPTIONS='shared_preload_libraries=pg_stat_statements \
//!   pg_stat_statements.track=all track_activity_query_size=4096' \
//!   bench idle-cost --statements
//! ```
//!
//! `--no-staging-worker` and `--application-threads 0` split the idle cost
//! into its halves. A definition only goes live with a staging worker *and* a
//! drain thread, so for either the install runs under a full client first and
//! the measured client replaces it before the warm-up.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;
use crate::streaming::chain::{create_chain_source_table, install_chain_hops, wait_for_chain_live};
use crate::streaming::tuning::EngineTuning;

/// How long installing the one transform and capturing its (empty) source may
/// take before the warm-up starts.
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

async fn xact_commit(raw: &RawClient) -> i64 {
    raw.query_one(
        "select xact_commit from pg_stat_database where datname = current_database()",
        &[],
    )
    .await
    .expect("read pg_stat_database.xact_commit")
    .get(0)
}

pub(crate) async fn wal_lsn(raw: &RawClient) -> String {
    raw.query_one("select pg_current_wal_lsn()::text", &[])
        .await
        .expect("read pg_current_wal_lsn")
        .get(0)
}

/// WAL bytes written since `start_lsn`. `pg_wal_lsn_diff` returns `numeric`,
/// which `tokio_postgres` has no built-in `FromSql` for — cast to `bigint`
/// explicitly (one idle window's WAL delta is nowhere near `i64`'s range).
pub(crate) async fn wal_bytes_since(raw: &RawClient, start_lsn: &str) -> i64 {
    raw.query_one(
        "select pg_wal_lsn_diff(pg_current_wal_lsn(), $1::text::pg_lsn)::bigint",
        &[&start_lsn],
    )
    .await
    .expect("compute wal lsn diff")
    .get(0)
}

/// The active segment pointer's sequence number — see the module doc comment
/// on why its delta is an exact seal count.
async fn active_seg_seq(raw: &RawClient) -> i64 {
    raw.query_one("select active_seq from segment_pointer", &[])
        .await
        .expect("read segment_pointer.active_seq")
        .get(0)
}

#[derive(Debug)]
pub struct IdleCostResult {
    pub application_threads: usize,
    pub maintenance_interval_ms: u64,
    pub poll_interval_ms: u64,
    pub duration_secs: f64,
    pub xact_commit_delta: i64,
    pub xact_commit_per_sec: f64,
    pub wal_bytes_delta: i64,
    pub wal_bytes_per_sec: f64,
    pub seals_delta: i64,
    pub seals_per_sec: f64,
    /// Every statement `pg_stat_statements` saw across the window, as one JSON
    /// line, when `--statements` asked for it.
    pub statements_json: Option<String>,
}

impl IdleCostResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"idle-cost\",\"application_threads\":{},\
             \"maintenance_interval_ms\":{},\"poll_interval_ms\":{},\"duration_secs\":{:.3},\
             \"xact_commit_delta\":{},\"xact_commit_per_sec\":{:.2},\"wal_bytes_delta\":{},\
             \"wal_bytes_per_sec\":{:.1},\"seals_delta\":{},\"seals_per_sec\":{:.3}}}",
            self.application_threads,
            self.maintenance_interval_ms,
            self.poll_interval_ms,
            self.duration_secs,
            self.xact_commit_delta,
            self.xact_commit_per_sec,
            self.wal_bytes_delta,
            self.wal_bytes_per_sec,
            self.seals_delta,
            self.seals_per_sec,
        )
    }
}

/// Starts a real streaming client against a fresh cluster with a captured but
/// never-written-to source table — so there is genuinely zero source traffic —
/// waits `warmup` for start-of-day work (capture reconcile, first
/// maintenance tick, drainer registration) to finish so it doesn't pollute the
/// steady-state reading, then samples across a `duration`-long idle window.
///
/// The sampling queries themselves commit, so they contribute to
/// `xact_commit`: three reads at the start and three at the end, i.e. a fixed
/// handful of transactions across the whole window rather than a rate. At this
/// module's window lengths that is well under a transaction/sec of the
/// hundreds being measured, and it is not subtracted out — the reported number
/// is what the database saw, with the instrument's own tiny cost included.
pub async fn run(
    warmup: Duration,
    duration: Duration,
    tuning: &EngineTuning,
    statements: bool,
) -> IdleCostResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let source = create_chain_source_table(&raw, "idle").await;
    // A definition only goes live with a staging worker and a drain thread, so
    // a half-install (`--no-staging-worker`, `--application-threads 0`) is
    // brought up by a full client that the measured one then replaces.
    let setup = EngineTuning {
        staging_worker: true,
        application_threads: tuning.application_threads.max(1),
        ..tuning.clone()
    };
    let mut client =
        trellis::Client::start(db.dsn(), setup.client_options()).expect("client start");
    let chain = install_chain_hops(&db.pool, &source, 1).await;
    wait_for_chain_live(&raw, &chain, SETUP_TIMEOUT).await;
    if !tuning.staging_worker || tuning.application_threads == 0 {
        client.shutdown().await.expect("setup client shutdown");
        client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    }

    tokio::time::sleep(warmup).await;

    if statements {
        reset_statements(&raw).await;
    }
    let commit_before = xact_commit(&raw).await;
    let lsn_before = wal_lsn(&raw).await;
    let seg_seq_before = active_seg_seq(&raw).await;
    let start = Instant::now();

    tokio::time::sleep(duration).await;

    let elapsed = start.elapsed().as_secs_f64();
    let commit_delta = xact_commit(&raw).await - commit_before;
    let wal_delta = wal_bytes_since(&raw, &lsn_before).await;
    let seals_delta = active_seg_seq(&raw).await - seg_seq_before;
    let statements_json = if statements {
        Some(read_statements(&raw).await)
    } else {
        None
    };

    client.shutdown().await.expect("client shutdown");

    IdleCostResult {
        application_threads: tuning.application_threads,
        maintenance_interval_ms: tuning.maintenance_interval.as_millis() as u64,
        poll_interval_ms: tuning.poll_interval.as_millis() as u64,
        duration_secs: elapsed,
        xact_commit_delta: commit_delta,
        xact_commit_per_sec: commit_delta as f64 / elapsed,
        wal_bytes_delta: wal_delta,
        wal_bytes_per_sec: wal_delta as f64 / elapsed,
        seals_delta,
        seals_per_sec: seals_delta as f64 / elapsed,
        statements_json,
    }
}

/// Creates `pg_stat_statements` (if needed) and zeroes it. Panics with the
/// settings to pass if the cluster did not preload the library.
async fn reset_statements(raw: &RawClient) {
    let preload: String = raw
        .query_one("show shared_preload_libraries", &[])
        .await
        .expect("show shared_preload_libraries")
        .get(0);
    assert!(
        preload.contains("pg_stat_statements"),
        "--statements needs a cluster that preloads pg_stat_statements; run with \
         TRELLIS_TESTKIT_PG_OPTIONS='shared_preload_libraries=pg_stat_statements \
         pg_stat_statements.track=all track_activity_query_size=4096'"
    );
    raw.batch_execute("create extension if not exists pg_stat_statements")
        .await
        .expect("create extension pg_stat_statements");
    raw.query_one("select pg_stat_statements_reset()::text", &[])
        .await
        .expect("reset pg_stat_statements");
}

/// Every recorded statement as a single `idle-cost-statements` JSON line,
/// busiest first. The reads `reset_statements` and this function themselves
/// issue show up in it (a handful of calls); the analysis drops them by text.
async fn read_statements(raw: &RawClient) -> String {
    raw.query_one(
        "select jsonb_build_object('scenario', 'idle-cost-statements', 'rows', \
                coalesce(jsonb_agg(to_jsonb(t) order by t.calls desc), '[]'::jsonb))::text \
         from (select dbid::bigint as dbid, queryid::text as queryid, toplevel, calls, rows, \
                      wal_bytes::bigint as wal_bytes, wal_records, \
                      round(total_exec_time::numeric, 3)::float8 as total_exec_ms, query \
               from pg_stat_statements) t",
        &[],
    )
    .await
    .expect("read pg_stat_statements")
    .get(0)
}
