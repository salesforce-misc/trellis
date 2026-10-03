//! The serialization-failure tax (#623 D8a): how often capture makes a
//! `SERIALIZABLE` application write fail with `40001` that would have
//! committed without it.
//!
//! D8a's capture re-reads each captured row from the source table by primary
//! key, inside the application's write. Under `SERIALIZABLE` that index probe
//! takes SIREAD predicate locks on the key's btree leaf page, so concurrent
//! writers touching neighbouring keys can form the rw-antidependency chains
//! SSI cancels. This scenario counts them.
//!
//! Every cell starts a fresh cluster (on `$TMPDIR`, so `bench --disk` moves it
//! onto the disk), creates `public.ssi_src (id bigint primary key default
//! nextval(…), val numeric)`, registers one transform on it (the same catalog
//! for every variant), loads [`PRELOAD_ROWS`] rows, then sets up the variant
//! ([`write_tax::set_up_variant`]: `none` is the no-capture baseline,
//! `trigger` installs the real capture triggers) and runs `w` writers for a
//! fixed window. Nothing drains the ring.
//!
//! Workloads ([`Workload`]), one transaction each, `BEGIN ISOLATION LEVEL …;
//! <statement>; COMMIT` with one round trip per step:
//!
//! - `serial1`: one `INSERT … DEFAULT` row, the id from the sequence, so
//!   every writer inserts on the index's rightmost leaf page;
//! - `serial-batch`: one `INSERT … SELECT generate_series` statement of
//!   `--batch` sequence-id rows;
//! - `random1`: one row with a uniformly random 62-bit id;
//! - `update1`: `UPDATE … SET val = val + 1` of one uniformly random
//!   preloaded id.
//!
//! A failed transaction is rolled back and counted by SQLSTATE, and the writer
//! moves on to a new one (no retry), so `failure_rate` is failed / attempted.
//! `committed_txns_per_sec` and `rows_per_sec` count committed work only.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rand::Rng;
use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use tokio_postgres::IsolationLevel;
use tokio_postgres::error::SqlState;

use crate::scenario::connect_raw;
use crate::streaming::chain::install_chain_hops;
use crate::streaming::disk_tier::{self, json_escape};
use crate::streaming::write_tax::{self, Variant};

pub const SOURCE_TABLE: &str = "ssi_src";

/// Rows loaded before the window, so `update1` has rows to update and the
/// index has more than one leaf page.
pub const PRELOAD_ROWS: i64 = 100_000;

pub const DEFAULT_WRITERS: &[usize] = &[1, 4, 8, 16];
pub const DEFAULT_BATCH: usize = 10;
pub const DEFAULT_WINDOW: Duration = Duration::from_secs(8);
pub const DEFAULT_VARIANTS: &[Variant] = &[Variant::None, Variant::Trigger];

/// The pause between cells.
const CELL_SETTLE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    Serial1,
    SerialBatch,
    Random1,
    Update1,
}

pub const DEFAULT_WORKLOADS: &[Workload] = &[
    Workload::Serial1,
    Workload::SerialBatch,
    Workload::Random1,
    Workload::Update1,
];

impl Workload {
    pub fn parse(raw: &str) -> Workload {
        match raw.trim() {
            "serial1" => Workload::Serial1,
            "serial-batch" => Workload::SerialBatch,
            "random1" => Workload::Random1,
            "update1" => Workload::Update1,
            other => panic!(
                "unknown ssi-tax workload {other:?}: want serial1, serial-batch, random1 or update1"
            ),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Workload::Serial1 => "serial1",
            Workload::SerialBatch => "serial-batch",
            Workload::Random1 => "random1",
            Workload::Update1 => "update1",
        }
    }

    fn rows_per_txn(self, batch: usize) -> u64 {
        match self {
            Workload::SerialBatch => batch as u64,
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    Serializable,
    ReadCommitted,
}

impl Isolation {
    pub fn parse(raw: &str) -> Isolation {
        match raw.trim() {
            "serializable" => Isolation::Serializable,
            "read-committed" => Isolation::ReadCommitted,
            other => panic!("unknown isolation {other:?}: want serializable or read-committed"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Isolation::Serializable => "serializable",
            Isolation::ReadCommitted => "read-committed",
        }
    }

    fn level(self) -> IsolationLevel {
        match self {
            Isolation::Serializable => IsolationLevel::Serializable,
            Isolation::ReadCommitted => IsolationLevel::ReadCommitted,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Cell {
    pub rep: usize,
    pub workload: Workload,
    pub writers: usize,
    pub variant: Variant,
    pub isolation: Isolation,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub batch: usize,
    pub window: Duration,
}

/// Every cell, repetition by repetition; within a (workload, writers) pair
/// the variants alternate order with the repetition, so drift doesn't always
/// land on the same one.
pub fn schedule(
    workloads: &[Workload],
    writers: &[usize],
    variants: &[Variant],
    isolations: &[Isolation],
    reps: usize,
) -> Vec<Cell> {
    let mut cells = Vec::new();
    for rep in 1..=reps {
        for &isolation in isolations {
            for &workload in workloads {
                for &w in writers {
                    let mut order = variants.to_vec();
                    if rep % 2 == 0 {
                        order.reverse();
                    }
                    for variant in order {
                        cells.push(Cell {
                            rep,
                            workload,
                            writers: w,
                            variant,
                            isolation,
                        });
                    }
                }
            }
        }
    }
    cells
}

#[derive(Debug, Default, Clone)]
struct Tally {
    attempted: u64,
    committed: u64,
    serialization_failures: u64,
    /// Every other failure, by SQLSTATE.
    other: BTreeMap<String, u64>,
}

impl Tally {
    fn merge(&mut self, o: &Tally) {
        self.attempted += o.attempted;
        self.committed += o.committed;
        self.serialization_failures += o.serialization_failures;
        for (k, v) in &o.other {
            *self.other.entry(k.clone()).or_default() += v;
        }
    }
}

#[derive(Debug, Clone)]
pub struct CellResult {
    pub cell: Cell,
    pub batch: usize,
    pub secs: f64,
    pub attempted: u64,
    pub committed: u64,
    pub serialization_failures: u64,
    pub other_failures: BTreeMap<String, u64>,
    pub failure_rate: f64,
    pub committed_txns_per_sec: f64,
    pub rows_per_sec: f64,
    pub rows_written: u64,
    pub storage_fs: String,
}

impl CellResult {
    pub fn to_json(&self) -> String {
        let other: Vec<String> = self
            .other_failures
            .iter()
            .map(|(k, v)| format!("\"{}\":{v}", json_escape(k)))
            .collect();
        format!(
            "{{\"scenario\":\"ssi-tax\",\"rep\":{},\"workload\":\"{}\",\"batch\":{},\
             \"writers\":{},\"variant\":\"{}\",\"isolation\":\"{}\",\"secs\":{:.3},\
             \"attempted\":{},\"committed\":{},\"serialization_failures\":{},\
             \"other_failures\":{{{}}},\"failure_rate\":{:.5},\
             \"committed_txns_per_sec\":{:.1},\"rows_per_sec\":{:.1},\"rows_written\":{},\
             \"storage_fs_type\":\"{}\"}}",
            self.cell.rep,
            self.cell.workload.name(),
            self.batch,
            self.cell.writers,
            self.cell.variant.name(),
            self.cell.isolation.name(),
            self.secs,
            self.attempted,
            self.committed,
            self.serialization_failures,
            other.join(","),
            self.failure_rate,
            self.committed_txns_per_sec,
            self.rows_per_sec,
            self.rows_written,
            json_escape(&self.storage_fs),
        )
    }

    pub fn human(&self) -> String {
        format!(
            "{} {} w={} {} {}: {}/{} failed 40001 ({:.3}%), other {:?}, {:.0} txn/s, {:.0} rows/s",
            self.cell.isolation.name(),
            self.cell.workload.name(),
            self.cell.writers,
            self.cell.variant.name(),
            self.storage_fs,
            self.serialization_failures,
            self.attempted,
            self.failure_rate * 100.0,
            self.other_failures,
            self.committed_txns_per_sec,
            self.rows_per_sec,
        )
    }
}

/// A tiny xorshift, so each writer draws ids without sharing a generator.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn statement_sql(workload: Workload, batch: usize) -> String {
    match workload {
        Workload::Serial1 => format!("insert into public.{SOURCE_TABLE} (val) values (1)"),
        Workload::SerialBatch => format!(
            "insert into public.{SOURCE_TABLE} (val) select g from generate_series(1, {batch}) g"
        ),
        Workload::Random1 => {
            format!("insert into public.{SOURCE_TABLE} (id, val) values ($1::bigint, 1)")
        }
        Workload::Update1 => {
            format!("update public.{SOURCE_TABLE} set val = val + 1 where id = $1::bigint")
        }
    }
}

async fn run_writer(
    mut raw: RawClient,
    workload: Workload,
    isolation: Isolation,
    batch: usize,
    seed: u64,
    deadline: Instant,
) -> Tally {
    let statement = raw
        .prepare(&statement_sql(workload, batch))
        .await
        .expect("prepare the writer's statement");
    let mut rng = XorShift(seed | 1);
    let mut tally = Tally::default();
    while Instant::now() < deadline {
        tally.attempted += 1;
        let id: i64 = match workload {
            // Above the preloaded and sequence range, so they never collide.
            Workload::Random1 => (1i64 << 40) + (rng.next() >> 2) as i64 % (1i64 << 61),
            Workload::Update1 => 1 + (rng.next() % PRELOAD_ROWS as u64) as i64,
            _ => 0,
        };
        let outcome = async {
            let txn = raw
                .build_transaction()
                .isolation_level(isolation.level())
                .start()
                .await?;
            match workload {
                Workload::Random1 | Workload::Update1 => {
                    txn.execute(&statement, &[&id]).await?;
                }
                _ => {
                    txn.execute(&statement, &[]).await?;
                }
            }
            txn.commit().await
        }
        .await;
        match outcome {
            Ok(()) => tally.committed += 1,
            Err(e) => match e.code() {
                Some(code) if *code == SqlState::T_R_SERIALIZATION_FAILURE => {
                    tally.serialization_failures += 1;
                }
                Some(code) => {
                    *tally.other.entry(code.code().to_string()).or_default() += 1;
                }
                None => panic!("writer connection failed: {e}"),
            },
        }
    }
    tally
}

pub async fn run_cell(cell: Cell, opts: &Options) -> CellResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(&format!(
        "create sequence public.{SOURCE_TABLE}_id_seq; \
         create table public.{SOURCE_TABLE} (\
           id bigint primary key default nextval('public.{SOURCE_TABLE}_id_seq'), \
           val numeric)"
    ))
    .await
    .expect("create the source table");
    install_chain_hops(&db.pool, SOURCE_TABLE, 1).await;
    raw.batch_execute(&format!(
        "insert into public.{SOURCE_TABLE} (val) select g from generate_series(1, {PRELOAD_ROWS}) g"
    ))
    .await
    .expect("preload the source table");
    raw.batch_execute(&format!("vacuum analyze public.{SOURCE_TABLE}"))
        .await
        .expect("vacuum the source table");
    write_tax::set_up_variant(cell.variant, SOURCE_TABLE, &mut raw).await;
    raw.batch_execute("checkpoint").await.expect("checkpoint");

    let mut writers = Vec::with_capacity(cell.writers);
    for _ in 0..cell.writers {
        writers.push(connect_raw(db.dsn()).await);
    }
    let rows_before: i64 = raw
        .query_one(&format!("select count(*) from public.{SOURCE_TABLE}"), &[])
        .await
        .expect("count rows")
        .get(0);

    let mut seeder = rand::rng();
    let start = Instant::now();
    let deadline = start + opts.window;
    let tasks: Vec<_> = writers
        .into_iter()
        .map(|w| {
            let seed: u64 = seeder.random();
            tokio::spawn(run_writer(
                w,
                cell.workload,
                cell.isolation,
                opts.batch,
                seed,
                deadline,
            ))
        })
        .collect();
    let mut tally = Tally::default();
    for task in tasks {
        tally.merge(&task.await.expect("writer task panicked"));
    }
    let secs = start.elapsed().as_secs_f64();

    let rows_after: i64 = raw
        .query_one(&format!("select count(*) from public.{SOURCE_TABLE}"), &[])
        .await
        .expect("count rows")
        .get(0);
    let rows_per_txn = cell.workload.rows_per_txn(opts.batch);
    let rows_written = tally.committed * rows_per_txn;
    if !matches!(cell.workload, Workload::Update1) {
        assert_eq!(
            (rows_after - rows_before) as u64,
            rows_written,
            "every committed insert is in the table"
        );
    }
    drop(raw);
    drop(db);
    drop(cluster);

    CellResult {
        cell,
        batch: opts.batch,
        secs,
        attempted: tally.attempted,
        committed: tally.committed,
        serialization_failures: tally.serialization_failures,
        other_failures: tally.other,
        failure_rate: if tally.attempted == 0 {
            0.0
        } else {
            tally.serialization_failures as f64 / tally.attempted as f64
        },
        committed_txns_per_sec: tally.committed as f64 / secs,
        rows_per_sec: rows_written as f64 / secs,
        rows_written,
        storage_fs: disk_tier::storage().fs_type,
    }
}

pub async fn run_matrix(cells: Vec<Cell>, opts: &Options) -> Vec<CellResult> {
    let total = cells.len();
    let mut results = Vec::with_capacity(total);
    for (i, cell) in cells.into_iter().enumerate() {
        eprintln!(
            "ssi-tax cell {}/{total}: rep {} {} {} w={} {} ...",
            i + 1,
            cell.rep,
            cell.isolation.name(),
            cell.workload.name(),
            cell.writers,
            cell.variant.name(),
        );
        let result = run_cell(cell, opts).await;
        tokio::time::sleep(CELL_SETTLE).await;
        println!("{}", result.to_json());
        eprintln!("  {}", result.human());
        results.push(result);
    }
    results
}

/// One line per (isolation, workload, writers, variant): the summed failure
/// rate over the repetitions and the median committed txn/s.
pub fn summary(results: &[CellResult]) -> String {
    let mut groups: BTreeMap<(String, String, usize, String), Vec<&CellResult>> = BTreeMap::new();
    for r in results {
        groups
            .entry((
                r.cell.isolation.name().to_string(),
                r.cell.workload.name().to_string(),
                r.cell.writers,
                r.cell.variant.name().to_string(),
            ))
            .or_default()
            .push(r);
    }
    let mut out = String::from(
        "isolation       workload      w  variant   40001/attempted        rate     txn/s(med)\n",
    );
    for ((iso, workload, w, variant), rs) in groups {
        let failed: u64 = rs.iter().map(|r| r.serialization_failures).sum();
        let attempted: u64 = rs.iter().map(|r| r.attempted).sum();
        let mut tps: Vec<f64> = rs.iter().map(|r| r.committed_txns_per_sec).collect();
        let tps = write_tax::median(&mut tps).unwrap_or(0.0);
        out.push_str(&format!(
            "{iso:<15} {workload:<13} {w:>2}  {variant:<8} {failed:>8}/{attempted:<10} {:>8.3}% {tps:>10.0}\n",
            if attempted == 0 {
                0.0
            } else {
                failed as f64 * 100.0 / attempted as f64
            }
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_alternate_order_by_repetition() {
        let cells = schedule(
            &[Workload::Serial1],
            &[4],
            DEFAULT_VARIANTS,
            &[Isolation::Serializable],
            2,
        );
        let names: Vec<&str> = cells.iter().map(|c| c.variant.name()).collect();
        assert_eq!(names, ["none", "trigger", "trigger", "none"]);
    }

    #[test]
    fn workloads_and_isolations_parse_by_name() {
        for w in DEFAULT_WORKLOADS {
            assert_eq!(Workload::parse(w.name()), *w);
        }
        assert_eq!(Isolation::parse("read-committed"), Isolation::ReadCommitted);
    }
}
