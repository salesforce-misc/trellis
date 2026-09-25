//! The load generator's own reach (issue #276): [`run_parallel_load`] at
//! [`Pace::Max`] against a plain source table with **no engine running** —
//! no client, no replication slot, nothing decoding the WAL.
//!
//! This validates the instrument rather than measuring Trellis. A throughput
//! scenario can only report an engine ceiling below the rate the generator
//! can offer, so this is the number to check before trusting one: if an
//! engine scenario's achieved rate sits near what this reports for the same
//! `--connections`/`--rows-per-commit`, raise `--connections` (or treat the
//! result as generator-bound) rather than reading it as the engine's limit.
//!
//! Volume scales with reach: 8 connections at 1,000 rows/commit write ~45M
//! rows in the default 10s, into a cluster under `$TMPDIR` (RAM-backed tmpfs
//! on the dev box). Keep `--duration-secs` short at large commit shapes.

use std::time::Duration;

use testkit::TestCluster;

use crate::scenario::connect_raw;
use crate::streaming::load::{Pace, ParallelLoad, run_parallel_load};

const SOURCE_TABLE: &str = "reach_src";

#[derive(Debug)]
pub struct GeneratorReachResult {
    pub connections: usize,
    pub rows_per_commit: usize,
    pub offered_duration_secs: f64,
    /// How long the generator waited, after the offer window closed, for the
    /// commits it issued inside it ([`LoadSummary::commit_tail`]). Not part of
    /// `offered_duration_secs` or the achieved rate (issue #541).
    pub commit_tail_secs: f64,
    pub commits_issued: u64,
    pub rows_issued: u64,
    pub achieved_commits_per_sec: f64,
    pub achieved_rows_per_sec: f64,
}

impl GeneratorReachResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"generator-reach\",\"connections\":{},\"rows_per_commit\":{},\
             \"offered_duration_secs\":{:.3},\"commit_tail_secs\":{:.3},\"commits_issued\":{},\"rows_issued\":{},\
             \"achieved_commits_per_sec\":{:.1},\"achieved_rows_per_sec\":{:.1}}}",
            self.connections,
            self.rows_per_commit,
            self.offered_duration_secs,
            self.commit_tail_secs,
            self.commits_issued,
            self.rows_issued,
            self.achieved_commits_per_sec,
            self.achieved_rows_per_sec,
        )
    }
}

/// Runs the generator flat out for `duration` and reports what it achieved.
/// The source table has the same `(id bigint primary key, val numeric)`
/// shape the chain and intake-ceiling scenarios write to, so the per-row
/// insert cost matches theirs.
pub async fn run(
    connections: usize,
    rows_per_commit: usize,
    duration: Duration,
) -> GeneratorReachResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(&format!(
        "create table public.{SOURCE_TABLE} (id bigint primary key, val numeric)"
    ))
    .await
    .expect("create generator-reach source table");

    let load = run_parallel_load(
        db.dsn(),
        SOURCE_TABLE,
        1,
        &ParallelLoad {
            connections,
            rows_per_commit,
            duration,
            groups: None,
            pace: Pace::Max,
        },
    )
    .await;

    let landed: i64 = raw
        .query_one(&format!("select count(*) from public.{SOURCE_TABLE}"), &[])
        .await
        .expect("count generator-reach rows")
        .get(0);
    assert_eq!(
        landed as u64, load.rows_issued,
        "every row the generator reported issuing must have landed"
    );

    let secs = load.window.as_secs_f64();
    GeneratorReachResult {
        connections,
        rows_per_commit,
        offered_duration_secs: secs,
        commit_tail_secs: load.commit_tail().as_secs_f64(),
        commits_issued: load.commits_issued,
        rows_issued: load.rows_issued,
        achieved_commits_per_sec: load.commits_issued as f64 / secs,
        achieved_rows_per_sec: load.achieved_rows_per_sec(),
    }
}
