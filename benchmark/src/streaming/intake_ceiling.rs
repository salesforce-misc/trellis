//! Intake ceiling — issue #266's E3 (tests H2): a staging-worker client with
//! **zero transforms and zero application threads**, so nothing ever drains
//! the ring and every row reported came only from CDC intake's
//! single-threaded `pgoutput` decode plus the ring append.
//!
//! Reported as the hard ceiling every other throughput number in this suite
//! sits under, per H2: "whether that path alone clears 100k rows/s — let alone
//! 400k — is unknown and bounds every other number in this issue."
//!
//! There is no oracle check here, deliberately: with no transforms installed
//! there is no target table to check, and the measured quantity *is* the ring
//! row count. The equivalent integrity check is the offered-vs-appended
//! comparison this already reports (`append_backlog`).

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::ClientOptions;
use trellis::config::DEFAULT_SCHEMA;

use crate::scenario::connect_raw;
use crate::streaming::load::run_max_rate_load;

const SOURCE_TABLE: &str = "intake_src";

/// Total rows across every physical ring table (`seg_0..seg_<RING_SIZE-1>`),
/// discovered via `pg_tables` rather than hardcoding `RING_SIZE` — a private
/// staging constant this crate has no access to by ADR-0012 design. Counts
/// every ring table whatever its state, sealed or active.
async fn total_ring_rows(raw: &RawClient) -> i64 {
    let tables: Vec<String> = raw
        .query(
            "select tablename from pg_tables where schemaname = $1 and tablename ~ '^seg_[0-9]+$'",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("list ring tables")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(
        !tables.is_empty(),
        "expected at least one seg_N ring table after migration"
    );

    let mut total = 0i64;
    for table in tables {
        let count: i64 = raw
            .query_one(
                &format!("select count(*) from {DEFAULT_SCHEMA}.{table}"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("count ring table {table}: {e}"))
            .get(0);
        total += count;
    }
    total
}

#[derive(Debug)]
pub struct IntakeCeilingResult {
    pub rows_per_commit: usize,
    pub offered_duration_secs: f64,
    pub rows_offered: u64,
    pub offered_achieved_rows_per_sec: f64,
    pub ring_rows_appended: i64,
    pub append_achieved_rows_per_sec: f64,
    /// Rows offered but not yet in the ring when the grace period expired.
    /// Positive means the reported append rate is a **floor**, not a ceiling:
    /// intake never caught up, so all that's known is that it sustained at
    /// least this much.
    pub append_backlog: i64,
    /// True when the offered and append rates came out essentially equal —
    /// meaning the *generator*, not intake, was the limiter, and this number
    /// is again a floor rather than the engine's ceiling.
    pub generator_bound: bool,
}

impl IntakeCeilingResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"intake-ceiling\",\"rows_per_commit\":{},\
             \"offered_duration_secs\":{},\"rows_offered\":{},\
             \"offered_achieved_rows_per_sec\":{:.1},\"ring_rows_appended\":{},\
             \"append_achieved_rows_per_sec\":{:.1},\"append_backlog\":{},\
             \"generator_bound\":{}}}",
            self.rows_per_commit,
            self.offered_duration_secs,
            self.rows_offered,
            self.offered_achieved_rows_per_sec,
            self.ring_rows_appended,
            self.append_achieved_rows_per_sec,
            self.append_backlog,
            self.generator_bound,
        )
    }
}

/// Runs the probe: pushes `rows_per_commit`-sized batches back to back with no
/// pacing for `offered_duration`, then polls up to `catch_up_grace` for the
/// ring's row count to catch up with what was offered (decode lags commit, so
/// this isn't instantaneous) before reporting the append-side rate.
pub async fn run(
    rows_per_commit: usize,
    offered_duration: Duration,
    catch_up_grace: Duration,
) -> IntakeCeilingResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(&format!(
        "create table public.{SOURCE_TABLE} (id bigint primary key, val numeric)"
    ))
    .await
    .expect("create intake-ceiling source table");

    // Not built from `EngineTuning`: this scenario's defining property is
    // `application_threads: 0` with no transforms at all, which is not a
    // tuning of the streaming pipeline but the deliberate absence of it.
    let client = trellis::Client::start(
        db.dsn(),
        ClientOptions {
            staging_worker: true,
            application_threads: 0,
            source_tables: vec![format!("public.{SOURCE_TABLE}")],
            ..Default::default()
        },
    )
    .expect("client start");

    let baseline_ring_rows = total_ring_rows(&raw).await;
    let load = run_max_rate_load(&raw, SOURCE_TABLE, 1, rows_per_commit, offered_duration).await;

    let deadline = Instant::now() + catch_up_grace;
    let ring_rows_now = loop {
        let now = total_ring_rows(&raw).await;
        if (now - baseline_ring_rows) as u64 >= load.rows_issued || Instant::now() >= deadline {
            break now;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    client.shutdown().await.expect("client shutdown");

    let appended = ring_rows_now - baseline_ring_rows;
    let offered_rate = load.achieved_rows_per_sec();
    let append_rate = appended as f64 / load.elapsed.as_secs_f64();
    IntakeCeilingResult {
        rows_per_commit,
        offered_duration_secs: offered_duration.as_secs_f64(),
        rows_offered: load.rows_issued,
        offered_achieved_rows_per_sec: offered_rate,
        ring_rows_appended: appended,
        append_achieved_rows_per_sec: append_rate,
        append_backlog: load.rows_issued as i64 - appended,
        // Within 2%: intake kept up with everything offered, so the offered
        // rate is the binding constraint, not the append path.
        generator_bound: (append_rate - offered_rate).abs() <= offered_rate * 0.02,
    }
}
