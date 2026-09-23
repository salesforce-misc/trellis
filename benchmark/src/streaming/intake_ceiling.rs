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
//!
//! **Mind the volume.** At [`Pace::Max`] the multi-connection generator
//! offers millions of rows/sec at large commit shapes (issue #276), and
//! nothing drains the ring, so every offered row is stored twice (source and
//! ring) plus its WAL, which the replication slot retains while intake lags.
//! testkit clusters live under `$TMPDIR` — a RAM-backed tmpfs on the dev box —
//! so the default window is short, and `--rate` paces the generator just above
//! the ceiling being measured rather than flat out.
//!
//! **Pace it for the real number, too.** The generator and intake share one
//! Postgres, so a flat-out generator's own writes slow intake down: at 1
//! row/commit, ~12.8k rows/sec appended under a 428k/sec flat-out offer vs
//! ~18k under `--rate 50000` (issue #276). A [`Pace::Max`] run finds roughly
//! where the ceiling is; re-run with `--rate` a little above it to measure it.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::ClientOptions;
use trellis::config::DEFAULT_SCHEMA;

use crate::scenario::connect_raw;
use crate::streaming::idle_cost::{wal_bytes_since, wal_lsn};
use crate::streaming::load::{
    GENERATOR_UNDERSHOOT_TOLERANCE, Pace, ParallelLoad, generator_bound, run_parallel_load,
};

const SOURCE_TABLE: &str = "intake_src";

/// Total rows across every physical ring table (`seg_0..seg_<RING_SIZE-1>`),
/// discovered via `pg_tables` rather than hardcoding `RING_SIZE` — a private
/// staging constant this crate has no access to by ADR-0012 design. Counts
/// every ring table whatever its state, sealed or active, in **one**
/// statement, so the total is a single snapshot: the ring as of the moment the
/// statement started, which is what lets [`run`] sample it at the instant the
/// offer window closes.
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

    let sum = tables
        .iter()
        .map(|table| format!("(select count(*) from {DEFAULT_SCHEMA}.{table})"))
        .collect::<Vec<_>>()
        .join(" + ");
    raw.query_one(&format!("select ({sum})::bigint"), &[])
        .await
        .expect("count ring rows")
        .get(0)
}

#[derive(Debug)]
pub struct IntakeCeilingResult {
    /// Writer connections the generator used ([`run_parallel_load`]).
    pub connections: usize,
    /// The paced target, or `None` for a max-rate run.
    pub target_rows_per_sec: Option<f64>,
    pub rows_per_commit: usize,
    pub offered_duration_secs: f64,
    pub rows_offered: u64,
    pub offered_achieved_rows_per_sec: f64,
    /// Rows in the ring at the instant the offer window closed — what intake
    /// had actually decoded and appended *while* load was arriving.
    pub ring_rows_appended_in_window: i64,
    /// `ring_rows_appended_in_window` over the window: intake's append rate
    /// under load. When intake was the limiter (it fell behind during the
    /// window) this **is** the intake ceiling; when it kept up, it equals the
    /// offered rate and `generator_bound` says so.
    ///
    /// Deliberately not `ring_rows_appended` over the window: rows appended
    /// during the grace period, after the generator stopped, would credit a
    /// slower intake with the generator's rate whenever it merely drained its
    /// backlog in time.
    pub append_achieved_rows_per_sec: f64,
    /// Rows in the ring once the grace period ended (or everything arrived).
    pub ring_rows_appended: i64,
    /// Rows offered but not yet in the ring when the grace period expired.
    /// Positive means intake never even drained what was offered — the
    /// integrity side of this probe, since nothing else checks the ring.
    pub append_backlog: i64,
    /// Intake appended (within tolerance) everything offered while it was
    /// being offered. When false, `append_achieved_rows_per_sec` is intake's
    /// ceiling.
    pub intake_kept_pace: bool,
    /// Issue #276's self-check ([`generator_bound`]): intake appended
    /// (within tolerance) everything offered while it was being offered, so
    /// the *generator* was the limiter and `append_achieved_rows_per_sec` is a
    /// floor on intake's ceiling, not the ceiling. Raise `--connections`.
    pub generator_bound: bool,
    /// Issue #274: `pg_current_wal_lsn()` delta over the whole run, divided
    /// by `rows_offered` — a per-source-row WAL cost, comparable across a
    /// `group_commit` override and the stock (grouped) default at the same
    /// `rows_per_commit`. Fewer, larger ring transactions should cut this at
    /// the 1-row/commit shape, where the un-grouped path pays a full
    /// transaction commit's WAL overhead (clog/commit record) per source row.
    pub wal_bytes_per_source_row: f64,
}

impl IntakeCeilingResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"intake-ceiling\",\"connections\":{},\"target_rows_per_sec\":{},\
             \"rows_per_commit\":{},\
             \"offered_duration_secs\":{:.3},\"rows_offered\":{},\
             \"offered_achieved_rows_per_sec\":{:.1},\"ring_rows_appended_in_window\":{},\
             \"append_achieved_rows_per_sec\":{:.1},\"ring_rows_appended\":{},\
             \"append_backlog\":{},\"intake_kept_pace\":{},\"generator_bound\":{},\"wal_bytes_per_source_row\":{:.2}}}",
            self.connections,
            match self.target_rows_per_sec {
                Some(rate) => rate.to_string(),
                None => "null".to_string(),
            },
            self.rows_per_commit,
            self.offered_duration_secs,
            self.rows_offered,
            self.offered_achieved_rows_per_sec,
            self.ring_rows_appended_in_window,
            self.append_achieved_rows_per_sec,
            self.ring_rows_appended,
            self.append_backlog,
            self.intake_kept_pace,
            self.generator_bound,
            self.wal_bytes_per_source_row,
        )
    }
}

/// Whether intake kept pace with the offered load *during* the window:
/// appended at least `1 - GENERATOR_UNDERSHOOT_TOLERANCE` of it by the time
/// the window closed. The slack absorbs intake's normal in-flight lag (a
/// group-commit batch plus decode) at the window's edge.
fn intake_kept_pace(rows_offered: u64, appended_in_window: i64) -> bool {
    appended_in_window as f64 >= rows_offered as f64 * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE)
}

/// Runs the probe: offers `load` (its `groups` is ignored — this source
/// table has none), samples the ring the moment the window closes (the
/// append rate under load), then polls up to `catch_up_grace` for the ring to
/// hold everything offered.
pub async fn run(
    load: ParallelLoad,
    catch_up_grace: Duration,
    group_commit: Option<trellis::GroupCommitConfig>,
) -> IntakeCeilingResult {
    let cfg = ParallelLoad {
        groups: None,
        ..load
    };
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
    // `group_commit` (issue #274) is threaded through directly for the same
    // reason — the caller picks `Some(..)`/`None` explicitly rather than
    // inheriting whatever `ClientOptions::default()` happens to ship, so a
    // stock-vs-grouped A/B at the same `rows_per_commit` is a single flag.
    let client = trellis::Client::start(
        db.dsn(),
        ClientOptions {
            staging_worker: true,
            application_threads: 0,
            source_tables: vec![format!("public.{SOURCE_TABLE}")],
            group_commit,
            ..Default::default()
        },
    )
    .expect("client start");

    let baseline_ring_rows = total_ring_rows(&raw).await;
    let wal_lsn_before = wal_lsn(&raw).await;
    let load = run_parallel_load(db.dsn(), SOURCE_TABLE, 1, &cfg).await;
    let appended_in_window = total_ring_rows(&raw).await - baseline_ring_rows;

    let deadline = Instant::now() + catch_up_grace;
    let ring_rows_now = loop {
        let now = total_ring_rows(&raw).await;
        if (now - baseline_ring_rows) as u64 >= load.rows_issued || Instant::now() >= deadline {
            break now;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let wal_bytes = wal_bytes_since(&raw, &wal_lsn_before).await;

    client.shutdown().await.expect("client shutdown");

    let appended = ring_rows_now - baseline_ring_rows;
    let offered_rate = load.achieved_rows_per_sec();
    let kept_pace = intake_kept_pace(load.rows_issued, appended_in_window);
    let target_rows_per_sec = match cfg.pace {
        Pace::Max => None,
        Pace::RowsPerSec(rate) => Some(rate),
    };
    IntakeCeilingResult {
        connections: cfg.connections,
        target_rows_per_sec,
        rows_per_commit: cfg.rows_per_commit,
        offered_duration_secs: load.elapsed.as_secs_f64(),
        rows_offered: load.rows_issued,
        offered_achieved_rows_per_sec: offered_rate,
        ring_rows_appended_in_window: appended_in_window,
        append_achieved_rows_per_sec: appended_in_window as f64 / load.elapsed.as_secs_f64(),
        ring_rows_appended: appended,
        append_backlog: load.rows_issued as i64 - appended,
        intake_kept_pace: kept_pace,
        generator_bound: generator_bound(target_rows_per_sec, offered_rate, kept_pace),
        wal_bytes_per_source_row: wal_bytes as f64 / load.rows_issued as f64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intake_that_fell_behind_during_the_window_is_the_measurement() {
        // #266's E3 failure mode, the other way round: 20M offered, only 12M
        // in the ring when the window closed. Even if the grace period later
        // drains the rest, intake was the limiter.
        assert!(!intake_kept_pace(20_000_000, 12_000_000));
        assert!(!generator_bound(
            None,
            1_000_000.0,
            intake_kept_pace(20_000_000, 12_000_000)
        ));
    }

    #[test]
    fn intake_that_kept_pace_makes_the_run_generator_bound() {
        // Within the in-flight slack at the window's edge.
        assert!(intake_kept_pace(10_000_000, 9_950_000));
        assert!(generator_bound(
            None,
            500_000.0,
            intake_kept_pace(10_000_000, 9_950_000)
        ));
    }
}
