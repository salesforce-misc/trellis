//! The write-path tax (#565's E1, rebuilt for #622 C4): what capturing a
//! table costs the application writing to it.
//!
//! Every cell starts a fresh cluster (on `$TMPDIR`, so `bench --disk` moves
//! it onto the disk), creates `public.wt_src (id bigint primary key, val
//! numeric)`, registers one transform on it so every variant has the same
//! catalog, then sets up one [`Variant`] and runs one [`Shape`] of inserts
//! against it. Nothing drains the ring in any variant.
//!
//! Variants:
//!
//! - `none`: no capture. The control.
//! - `btree` and `regex-index`: no capture, one extra index on `val`: a plain
//!   btree, or ADR-0002 A7's comparator, an expression index on
//!   `regexp_count(val::text, '[13579]')`. They put the capture numbers next to
//!   costs application engineers already accept.
//! - `slot`: today's path. A `trellis::Client` with the staging worker and no
//!   application threads, so the walsender and intake stage every row and
//!   nothing drains. Only while intake exists (C8 deletes it).
//! - `trigger`: the capture triggers, installed through the real installer
//!   ([`trellis::dev::capture::install`]), with the capture spec computed
//!   from the catalog the way C5's reconcile will.
//! - `trigger+exception` and `trigger+column-check`: open question Q2's two
//!   candidate guards against a renamed or dropped read column, as
//!   benchmark-only rewrites of the installed functions
//!   ([`exception_variant`], [`column_check_variant`]).
//!   `trigger+column-check-guard` runs the same check per statement inside
//!   the empty-statement guard's query ([`column_check_guard_variant`]), and
//!   `trigger+column-check-txn` runs it once per transaction
//!   ([`column_check_per_txn_variant`]). None is product code; C6 builds
//!   whichever Q2 picks.
//!
//! Shapes ([`Shape::parse`]):
//!
//! - `<r>x<w>`: `w` writers, each committing `r` rows per transaction in one
//!   `INSERT … SELECT FROM generate_series` statement (E1's pgbench script).
//! - `orm<s>x<w>`: `w` writers, each committing `s` single-row `INSERT`s per
//!   transaction, one round trip each: what an ORM saving objects one at a
//!   time sends. At more than 64 captured statements per transaction the
//!   `trigger+exception` variant overflows the backend's subtransaction
//!   cache (#622 plan finding 8), which this shape exists to show.
//! - `copy`: one `COPY … FROM STDIN` of `--copy-rows` rows, one transaction.
//! - A `+hold` suffix holds a transaction with an xid open on another
//!   connection for the whole window: the long-running transaction under
//!   which a suboverflowed snapshot has to consult `pg_subtrans` for every
//!   tuple newer than the holder.
//!
//! What a cell reports, besides rows/s: per-transaction commit latency p50 and
//! p99; Postgres CPU per row (every postgres process, reaped backends
//! included, less the harness's own sampling and probe backends) and, for
//! `slot`, the Trellis engine's in-process CPU; WAL bytes per row (for
//! `slot`, including the ring WAL intake writes while catching up); the top
//! wait events of active client backends, sampled every 10 ms; the storage
//! and the settings that decide durability.
//!
//! **The subtransaction effect** is measured three ways, on every cell:
//! the sampler records whether any backend's subtransaction cache had
//! overflowed (`pg_stat_get_backend_subxact`) and the largest cache it saw;
//! the `pg_stat_slru` counters for `pg_subtrans` over the window; and, on
//! ORM shapes by default (`--snapshot-probe`), a second connection that
//! takes a fresh snapshot in a tight loop and reads the 1,000 newest rows of
//! the first writer, reporting how many snapshots per second it managed and
//! their latency. A suboverflowed snapshot makes every visibility check of a
//! tuple newer than the snapshot's `xmin` look the xid up in `pg_subtrans`,
//! for every backend in the cluster, not only the writer's.
//!
//! The run is a fixed number of rows per shape, cut off at `--max-secs`, so a
//! slow variant on disk doesn't run for minutes. Rates are over the window
//! actually run.
//!
//! **Order.** [`schedule`] runs every shape once per repetition, and within a
//! shape the control first, then the other variants in an order that
//! rotates with the repetition, then the control again, so drift across a
//! session shows as the two controls disagreeing. Each cell prints one JSON
//! line as it finishes, so `bench`'s contention tag lands on exactly the cell
//! it concerns and a contended cell can be dropped and re-run alone
//! (`--variants`, `--shapes`, `--reps 1`).

use std::collections::BTreeMap;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::SinkExt;
use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::ClientOptions;
use trellis::config::DEFAULT_SCHEMA;
use trellis::dev::capture::{CaptureEvent, CaptureSpec};

use crate::scenario::connect_raw;
use crate::streaming::chain::{install_chain_hops, wait_for_markers_discharged};
use crate::streaming::disk_tier::{self, LatencyHistogram, json_escape, json_ms};
use crate::streaming::idle_cost::{wal_bytes_since, wal_lsn};

pub const SOURCE_TABLE: &str = "wt_src";

/// Where writer `w`'s ids start. Each writer inserts into its own band, so
/// the snapshot probe can read one writer's newest rows with one index range.
const WRITER_BAND: i64 = 1_000_000_000_000;

/// How long a `slot` cell's client may take to publish the table and
/// discharge its capture marker before the window opens.
const SLOT_SETUP_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the sampler reads `pg_stat_activity`, as E1 did.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(10);

/// The pause between cells.
const CELL_SETTLE: Duration = Duration::from_secs(2);

/// The rows one snapshot probe reads.
const PROBE_ROWS: i64 = 1_000;

/// Buffer size for a `COPY` cell's pre-generated data.
const COPY_CHUNK_BYTES: usize = 1 << 20;

// --- Variants and shapes ---------------------------------------------------

/// What, if anything, captures the source table in a cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Variant {
    None,
    Btree,
    RegexIndex,
    Slot,
    Trigger,
    TriggerException,
    TriggerColumnCheck,
    TriggerColumnCheckGuard,
    TriggerColumnCheckPerTxn,
}

impl Variant {
    pub const ALL: [Variant; 9] = [
        Variant::None,
        Variant::Btree,
        Variant::RegexIndex,
        Variant::Slot,
        Variant::Trigger,
        Variant::TriggerException,
        Variant::TriggerColumnCheck,
        Variant::TriggerColumnCheckGuard,
        Variant::TriggerColumnCheckPerTxn,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Variant::None => "none",
            Variant::Btree => "btree",
            Variant::RegexIndex => "regex-index",
            Variant::Slot => "slot",
            Variant::Trigger => "trigger",
            Variant::TriggerException => "trigger+exception",
            Variant::TriggerColumnCheck => "trigger+column-check",
            Variant::TriggerColumnCheckGuard => "trigger+column-check-guard",
            Variant::TriggerColumnCheckPerTxn => "trigger+column-check-txn",
        }
    }

    pub fn parse(raw: &str) -> Variant {
        let raw = raw.trim();
        Variant::ALL
            .into_iter()
            .find(|v| v.name() == raw)
            .unwrap_or_else(|| {
                let names: Vec<&str> = Variant::ALL.iter().map(|v| v.name()).collect();
                panic!("unknown variant {raw:?}; expected one of {names:?}")
            })
    }

    /// Writes ring rows inside the writer's transaction.
    fn is_trigger(self) -> bool {
        matches!(
            self,
            Variant::Trigger
                | Variant::TriggerException
                | Variant::TriggerColumnCheck
                | Variant::TriggerColumnCheckGuard
                | Variant::TriggerColumnCheckPerTxn
        )
    }

    /// Captures at all, so the ring must end up holding every row.
    fn captures(self) -> bool {
        self.is_trigger() || self == Variant::Slot
    }
}

/// How a shape's writers write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// One multi-row `INSERT … SELECT` per transaction.
    Batch { rows_per_commit: usize },
    /// `statements` single-row `INSERT`s per transaction.
    Orm { statements: usize },
    /// One `COPY` of the cell's `--copy-rows`.
    Copy,
}

/// One workload: how each writer writes, how many writers, and whether a
/// long-running transaction holds the cluster's `xmin` back meanwhile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    pub kind: Kind,
    pub writers: usize,
    pub hold_xmin: bool,
}

impl Shape {
    /// Parses `<r>x<w>`, `orm<s>x<w>` or `copy`, each optionally suffixed
    /// `+hold` (see the module doc).
    pub fn parse(raw: &str) -> Shape {
        let raw = raw.trim();
        let (body, hold_xmin) = match raw.strip_suffix("+hold") {
            Some(body) => (body, true),
            None => (raw, false),
        };
        if body == "copy" {
            return Shape {
                kind: Kind::Copy,
                writers: 1,
                hold_xmin,
            };
        }
        let (orm, body) = match body.strip_prefix("orm") {
            Some(rest) => (true, rest),
            None => (false, body),
        };
        let (per_txn, writers) = body.split_once('x').unwrap_or_else(|| {
            panic!("shape {raw:?} must be <rows>x<writers>, orm<statements>x<writers> or copy")
        });
        let number = |s: &str, what: &str| -> usize {
            let n: usize = s
                .parse()
                .unwrap_or_else(|e| panic!("shape {raw:?}: {what} {s:?}: {e}"));
            assert!(n >= 1, "shape {raw:?}: {what} must be at least 1");
            n
        };
        let per_txn = number(per_txn, "rows or statements per transaction");
        let writers = number(writers, "writers");
        Shape {
            kind: if orm {
                Kind::Orm {
                    statements: per_txn,
                }
            } else {
                Kind::Batch {
                    rows_per_commit: per_txn,
                }
            },
            writers,
            hold_xmin,
        }
    }

    pub fn batch(rows_per_commit: usize, writers: usize) -> Shape {
        Shape {
            kind: Kind::Batch { rows_per_commit },
            writers,
            hold_xmin: false,
        }
    }

    pub fn label(&self) -> String {
        let body = match self.kind {
            Kind::Batch { rows_per_commit } => format!("{rows_per_commit}x{}", self.writers),
            Kind::Orm { statements } => format!("orm{statements}x{}", self.writers),
            Kind::Copy => "copy".to_string(),
        };
        if self.hold_xmin {
            format!("{body}+hold")
        } else {
            body
        }
    }

    /// Rows one transaction writes.
    pub fn rows_per_txn(&self, copy_rows: u64) -> u64 {
        match self.kind {
            Kind::Batch { rows_per_commit } => rows_per_commit as u64,
            Kind::Orm { statements } => statements as u64,
            Kind::Copy => copy_rows,
        }
    }

    /// Statements one transaction sends, which is what the capture trigger
    /// fires once per.
    pub fn statements_per_txn(&self) -> u64 {
        match self.kind {
            Kind::Orm { statements } => statements as u64,
            Kind::Batch { .. } | Kind::Copy => 1,
        }
    }

    /// E1's row counts: 400k at one row per statement, 4M in batches. More
    /// writers get more rows (up to 4x), so a 16-writer cell isn't over in a
    /// second. `--max-secs` cuts off whichever variant is too slow for it.
    pub fn default_rows(&self, copy_rows: u64) -> u64 {
        let scale = self.writers.min(4) as u64;
        match self.kind {
            Kind::Batch { rows_per_commit: 1 } | Kind::Orm { .. } => 400_000 * scale,
            Kind::Batch { rows_per_commit } if rows_per_commit < 1000 => 2_000_000,
            Kind::Batch { .. } => 4_000_000,
            Kind::Copy => copy_rows,
        }
    }
}

/// E1's shapes plus the ORM shape (#622 plan, C4), and the ORM shape again at
/// 16 writers and under a held `xmin`, for the subtransaction effect.
pub const DEFAULT_SHAPES: &[&str] = &[
    "1x1",
    "1x16",
    "100x1",
    "1000x1",
    "1000x16",
    "orm100x1",
    "orm100x16",
    "orm100x16+hold",
    "copy",
];

/// When the snapshot probe runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeMode {
    Off,
    /// ORM shapes only (the default).
    Orm,
    All,
}

impl ProbeMode {
    pub fn parse(raw: &str) -> ProbeMode {
        match raw.trim() {
            "off" => ProbeMode::Off,
            "orm" => ProbeMode::Orm,
            "all" => ProbeMode::All,
            other => panic!("--snapshot-probe {other:?} must be off, orm or all"),
        }
    }

    fn runs_for(self, shape: &Shape) -> bool {
        match self {
            ProbeMode::Off => false,
            ProbeMode::Orm => matches!(shape.kind, Kind::Orm { .. }),
            ProbeMode::All => true,
        }
    }
}

/// Knobs every cell of a matrix shares.
#[derive(Debug, Clone, Copy)]
pub struct CellOptions {
    /// The longest a cell's writers run.
    pub max_window: Duration,
    /// Rows a `copy` cell loads.
    pub copy_rows: u64,
    /// Overrides every shape's [`Shape::default_rows`].
    pub rows: Option<u64>,
    /// How long a `slot` cell waits, after the writers stop, for intake to
    /// stage the catch-up sentinel, which it stages after every row the
    /// writers committed.
    pub slot_catch_up: Duration,
    pub probe: ProbeMode,
}

impl Default for CellOptions {
    fn default() -> Self {
        CellOptions {
            max_window: Duration::from_secs(30),
            copy_rows: 1_000_000,
            rows: None,
            slot_catch_up: Duration::from_secs(180),
            probe: ProbeMode::Orm,
        }
    }
}

// --- Schedule ---------------------------------------------------------------

/// Where a cell sits in its shape's run of variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// The control, run first; the other variants' share is against it.
    Head,
    Middle,
    /// The control again, run last, to show drift.
    Tail,
}

impl Position {
    fn name(self) -> &'static str {
        match self {
            Position::Head => "head",
            Position::Middle => "middle",
            Position::Tail => "tail",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cell {
    pub rep: usize,
    pub shape: Shape,
    pub variant: Variant,
    pub position: Position,
}

/// Every cell of the matrix, in run order (see the module doc's "Order").
/// Repetitions are the outer loop, so a session cut short still has whole
/// repetitions of every shape. `none`, when asked for, brackets each shape's
/// run; the others rotate by one place per repetition.
pub fn schedule(variants: &[Variant], shapes: &[Shape], reps: usize) -> Vec<Cell> {
    let control = variants.contains(&Variant::None);
    let others: Vec<Variant> = variants
        .iter()
        .copied()
        .filter(|v| *v != Variant::None)
        .collect();
    let mut cells = Vec::new();
    for rep in 0..reps {
        for &shape in shapes {
            let cell = |variant, position| Cell {
                rep,
                shape,
                variant,
                position,
            };
            if control {
                cells.push(cell(Variant::None, Position::Head));
            }
            for i in 0..others.len() {
                cells.push(cell(others[(i + rep) % others.len()], Position::Middle));
            }
            if control {
                cells.push(cell(Variant::None, Position::Tail));
            }
        }
    }
    cells
}

// --- Q2's benchmark-only variants -------------------------------------------

/// The events a write-tax cell fires. Truncate's function is left as
/// installed: nothing truncates.
const REWRITTEN_EVENTS: [CaptureEvent; 3] = [
    CaptureEvent::Insert,
    CaptureEvent::Update,
    CaptureEvent::Delete,
];

fn replace_once(ddl: &str, from: &str, to: &str) -> String {
    assert_eq!(
        ddl.matches(from).count(),
        1,
        "the capture function no longer has exactly one {from:?}; update the write-tax rewrite"
    );
    ddl.replacen(from, to, 1)
}

/// Q2(a): the capture wrapped in a `BEGIN … EXCEPTION` block, so a failed
/// capture never fails the application's statement. Entering the block
/// starts a subtransaction, and the ring insert inside it gives that
/// subtransaction its own xid: one subxid per captured statement.
///
/// The empty-statement check stays outside the block, as a real
/// implementation would keep it.
pub fn exception_variant(ddl: &str) -> String {
    let ddl = replace_once(ddl, "    -- #597:", "    begin\n    -- #597:");
    replace_once(
        &ddl,
        "    end case;\n    return null;\nend;\n",
        "    end case;\n    exception when others then\n        \
         raise warning 'trellis capture skipped: %', sqlerrm;\n    end;\n    return null;\nend;\n",
    )
}

/// The `pg_attribute` probe Q2(c) runs: how many of `columns` `tg_relid`
/// still has.
fn column_probe(columns: &[String]) -> String {
    let names: Vec<String> = columns
        .iter()
        .map(|c| format!("'{}'", c.replace('\'', "''")))
        .collect();
    format!(
        "(select count(*) from pg_catalog.pg_attribute a\n          \
         where a.attrelid = tg_relid and a.attnum > 0 and not a.attisdropped\n            \
         and a.attname = any (array[{}]::name[]))",
        names.join(", ")
    )
}

/// The mirror-read statement's span in `ddl`: `slot := <case>;`, and the
/// case expression.
fn mirror_read(ddl: &str) -> (usize, usize, String) {
    let start = ddl
        .find("    slot := case when")
        .expect("the capture function assigns slot from the mirror read");
    let end = start
        + ddl[start..]
            .find(" end;\n")
            .expect("the mirror read ends its case")
        + " end;\n".len();
    let expr = ddl[start + "    slot := ".len()..end - ";\n".len()].to_string();
    (start, end, expr)
}

fn declare_present(ddl: &str) -> String {
    replace_once(
        ddl,
        "    ts timestamptz;\nbegin\n",
        "    ts timestamptz;\n    present bigint;\nbegin\n",
    )
}

/// Q2(c): one `pg_attribute` probe for the capture columns, folded into the
/// statement that reads the mirror (so #597's single expression still
/// assigns the xid and reads the slot). On a miss the function returns
/// without capturing; C6 would stage a `schema_changed` marker there. The
/// benchmark never takes that branch, so it measures only the probe.
///
/// Folding the probe in turns the mirror read from a PL/pgSQL simple
/// expression, which skips the executor, into a query.
pub fn column_check_variant(ddl: &str, columns: &[String]) -> String {
    let (start, end, expr) = mirror_read(ddl);
    let replacement = format!(
        "    select {expr},\n        {}\n      \
         into slot, present;\n    \
         if present <> {} then\n        \
         return null;\n    end if;\n",
        column_probe(columns),
        columns.len(),
    );
    declare_present(&format!("{}{replacement}{}", &ddl[..start], &ddl[end..]))
}

/// The empty-statement guard's span in `ddl`: `if not exists (select 1
/// from <transition table>) then return null; end if;`, and the table.
fn empty_guard(ddl: &str) -> (usize, usize, String) {
    const OPEN: &str = "    if not exists (select 1 from ";
    const CLOSE: &str = "    end if;\n";
    assert_eq!(
        ddl.matches(OPEN).count(),
        1,
        "the capture function no longer has exactly one {OPEN:?}; update the write-tax rewrite"
    );
    let start = ddl.find(OPEN).expect("counted above");
    let table_end = start
        + ddl[start..]
            .find(") then\n        return null;\n")
            .expect("the empty guard returns null");
    let table = ddl[start + OPEN.len()..table_end].to_string();
    let end = table_end + ddl[table_end..].find(CLOSE).expect("the guard ends") + CLOSE.len();
    (start, end, table)
}

/// Q2(c), with the `pg_attribute` probe riding in the empty-statement
/// guard's query instead of the mirror read's. The guard is already a query
/// (its `EXISTS` subquery keeps it off PL/pgSQL's simple-expression path),
/// so the probe adds a subplan to a query the function runs anyway, and the
/// mirror read stays a simple expression. An empty statement skips the
/// probe; a miss returns without capturing, as in [`column_check_variant`].
pub fn column_check_guard_variant(ddl: &str, columns: &[String]) -> String {
    let (start, end, table) = empty_guard(ddl);
    let guard = format!(
        "    present := case when exists (select 1 from {table})\n        \
         then {} else -1 end;\n    \
         if present < 0 then\n        return null;\n    end if;\n    \
         if present <> {} then\n        return null;\n    end if;\n",
        column_probe(columns),
        columns.len(),
    );
    declare_present(&format!("{}{guard}{}", &ddl[..start], &ddl[end..]))
}

/// Q2(c), checked once per transaction: the same probe, run only when a
/// transaction-local setting keyed by the table doesn't say this transaction
/// already checked it. The mirror read stays a simple expression.
///
/// Sound only because `RENAME`/`DROP COLUMN` take `ACCESS EXCLUSIVE`, which
/// waits for every transaction already holding the table's `ROW EXCLUSIVE`.
/// Once a transaction has written the table, only that transaction can change
/// its columns before it commits. A transaction that writes, renames a read
/// column and writes again would reach the stale images and fail, where
/// [`column_check_variant`] wouldn't.
pub fn column_check_per_txn_variant(ddl: &str, columns: &[String]) -> String {
    let (_, end, _) = mirror_read(ddl);
    let check = format!(
        "    if pg_catalog.current_setting('trellis.capture_checked_' || tg_relid, true)\n        \
         is distinct from 'on' then\n        \
         select {} into present;\n        \
         if present <> {} then\n            \
         return null;\n        end if;\n        \
         perform pg_catalog.set_config('trellis.capture_checked_' || tg_relid, 'on', true);\n    \
         end if;\n",
        column_probe(columns),
        columns.len(),
    );
    declare_present(&format!("{}{check}{}", &ddl[..end], &ddl[end..]))
}

// --- Measurement helpers ----------------------------------------------------

/// `/proc/*/stat`'s CPU times are in `USER_HZ` ticks, which Linux fixes at
/// 100 on every architecture the benchmark runs on (reading `sysconf` would
/// need a libc dependency for a constant).
const CLOCK_TICKS: f64 = 100.0;

/// `utime + stime` of `/proc/<path>/stat`, plus `cutime + cstime` when
/// `children` is set, in seconds. `None` for a process that has gone.
fn proc_cpu(path: &str, children: bool) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{path}/stat")).ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    // After the comm: state is fields[0], utime [11], stime [12], cutime [13], cstime [14].
    let field = |i: usize| fields.get(i).and_then(|f| f.parse::<f64>().ok());
    let mut ticks = field(11)? + field(12)?;
    if children {
        ticks += field(13)? + field(14)?;
    }
    Some(ticks / CLOCK_TICKS)
}

/// Every postgres process's CPU, as E1 counted it: the postmaster with its
/// reaped children, plus each live child.
fn pg_cpu(postmaster: u32) -> f64 {
    let mut total = proc_cpu(&postmaster.to_string(), true).unwrap_or(0.0);
    let children =
        std::fs::read_to_string(format!("/proc/{postmaster}/task/{postmaster}/children"))
            .unwrap_or_default();
    for child in children.split_whitespace() {
        // A backend that exits after the postmaster's read and before its
        // own is in neither. The reads sit at the window's edges, where no
        // cell backend connects or leaves, so that loses a stray autovacuum
        // worker's CPU at most.
        total += proc_cpu(child, false).unwrap_or(0.0);
    }
    total
}

/// The CPU of this process's Trellis engine threads: the client's own thread
/// and its runtime's workers, which keep tokio's default names
/// (`tokio-rt-worker`, `tokio-runtime-worker` in older releases). The
/// benchmark's own runtime names its threads `bench-writer`
/// ([`crate::streaming::cli`]), so they are not counted.
fn engine_cpu() -> f64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0.0;
    };
    tasks
        .filter_map(Result::ok)
        .filter_map(|task| {
            let tid = task.file_name().into_string().ok()?;
            let comm = std::fs::read_to_string(format!("/proc/self/task/{tid}/comm")).ok()?;
            let comm = comm.trim();
            (comm.starts_with("trellis-") || comm.starts_with("tokio-"))
                .then(|| proc_cpu(&format!("self/task/{tid}"), false))
                .flatten()
        })
        .sum()
}

/// Every physical ring table (`seg_0..`), read from `pg_tables` rather than
/// hardcoding the ring size, a private staging constant.
async fn ring_tables(raw: &RawClient) -> Vec<String> {
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
    assert!(!tables.is_empty(), "expected seg_N ring tables");
    tables
}

/// The ring's row count across every `seg_N`, in one statement.
pub async fn total_ring_rows(raw: &RawClient) -> i64 {
    let tables = ring_tables(raw).await;
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

async fn wal_insert_lsn(raw: &RawClient) -> String {
    raw.query_one("select pg_current_wal_insert_lsn()::text", &[])
        .await
        .expect("read pg_current_wal_insert_lsn")
        .get(0)
}

/// A statement whose one boolean says whether any ring row's `origin_lsn`
/// is past `$1` (text), reading each `seg_N`'s `origin_lsn` index.
async fn newest_ring_origin_sql(raw: &RawClient) -> String {
    let tables = ring_tables(raw).await;
    let newest: Vec<String> = tables
        .iter()
        .map(|t| format!("(select max(origin_lsn) from {DEFAULT_SCHEMA}.{t})"))
        .collect();
    format!(
        "select coalesce(greatest({}) > $1::text::pg_lsn, false)",
        newest.join(", ")
    )
}

/// `pg_stat_slru`'s `pg_subtrans` counters: hits, reads, zeroed pages. The
/// SLRU's name changed in PostgreSQL 17, so both spellings are read.
async fn subtrans_slru(raw: &RawClient) -> (i64, i64, i64) {
    raw.batch_execute("select pg_stat_force_next_flush(); select pg_stat_clear_snapshot()")
        .await
        .expect("flush statistics");
    let row = raw
        .query_one(
            "select coalesce(sum(blks_hit), 0)::bigint, coalesce(sum(blks_read), 0)::bigint, \
                    coalesce(sum(blks_zeroed), 0)::bigint \
             from pg_stat_slru where lower(name) in ('subtrans', 'subtransaction')",
            &[],
        )
        .await
        .expect("read pg_stat_slru");
    (row.get(0), row.get(1), row.get(2))
}

/// What the server runs with, for the JSON line.
#[derive(Debug, Clone, Default)]
struct Settings {
    server_version: String,
    wal_level: String,
    fsync: String,
    synchronous_commit: String,
    full_page_writes: String,
    shared_buffers: String,
}

async fn settings(raw: &RawClient) -> Settings {
    let row = raw
        .query_one(
            "select current_setting('server_version'), current_setting('wal_level'), \
                    current_setting('fsync'), current_setting('synchronous_commit'), \
                    current_setting('full_page_writes'), current_setting('shared_buffers')",
            &[],
        )
        .await
        .expect("read settings");
    Settings {
        server_version: row.get(0),
        wal_level: row.get(1),
        fsync: row.get(2),
        synchronous_commit: row.get(3),
        full_page_writes: row.get(4),
        shared_buffers: row.get(5),
    }
}

async fn backend_pid(raw: &RawClient) -> i32 {
    raw.query_one("select pg_backend_pid()", &[])
        .await
        .expect("read pg_backend_pid")
        .get(0)
}

/// What the sampler saw over the window.
#[derive(Debug, Default)]
struct Samples {
    /// `wait_event_type:wait_event` (or `CPU:-`) -> active client backends
    /// seen in it, summed over samples.
    waits: BTreeMap<String, u64>,
    samples: u64,
    /// Samples in which some backend's subtransaction cache had overflowed.
    overflowed: u64,
    max_subxact_count: i32,
}

impl Samples {
    /// The six most common waits, each as a share of all backend samples.
    fn top_waits(&self) -> Vec<(String, f64)> {
        let total: u64 = self.waits.values().sum();
        let mut waits: Vec<(String, u64)> =
            self.waits.iter().map(|(k, v)| (k.clone(), *v)).collect();
        waits.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        waits
            .into_iter()
            .take(6)
            .map(|(k, v)| (k, v as f64 / total.max(1) as f64))
            .collect()
    }

    fn overflow_share(&self) -> Option<f64> {
        (self.samples > 0).then(|| self.overflowed as f64 / self.samples as f64)
    }
}

/// Samples until `stop`, then hands the connection back: the caller reads
/// its backend's CPU before letting it go.
async fn sample_until(
    raw: RawClient,
    exclude: Vec<i32>,
    stop: Arc<AtomicBool>,
) -> (Samples, RawClient) {
    let has_subxact: bool = raw
        .query_one(
            "select to_regprocedure('pg_stat_get_backend_subxact(integer)') is not null",
            &[],
        )
        .await
        .expect("look up pg_stat_get_backend_subxact")
        .get(0);
    let waits_sql = raw
        .prepare(
            "select coalesce(wait_event_type, 'CPU') || ':' || coalesce(wait_event, '-'), \
                    count(*)::bigint \
             from pg_stat_activity \
             where backend_type = 'client backend' and state = 'active' \
               and pid <> pg_backend_pid() and pid <> all($1) \
             group by 1",
        )
        .await
        .expect("prepare the wait sampler");
    let subxact_sql = if has_subxact {
        Some(
            raw.prepare(
                "select coalesce(max(s.subxact_count), 0)::int, \
                        coalesce(bool_or(s.subxact_overflowed), false) \
                 from pg_stat_get_backend_idset() b(id), \
                      lateral pg_stat_get_backend_subxact(b.id) s",
            )
            .await
            .expect("prepare the subxact sampler"),
        )
    } else {
        None
    };
    let mut out = Samples::default();
    while !stop.load(Ordering::Relaxed) {
        for row in raw
            .query(&waits_sql, &[&exclude])
            .await
            .expect("sample waits")
        {
            *out.waits.entry(row.get(0)).or_default() += row.get::<_, i64>(1) as u64;
        }
        if let Some(sql) = &subxact_sql {
            let row = raw.query_one(sql, &[]).await.expect("sample subxacts");
            let count: i32 = row.get(0);
            out.max_subxact_count = out.max_subxact_count.max(count);
            if row.get::<_, bool>(1) {
                out.overflowed += 1;
            }
        }
        out.samples += 1;
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
    (out, raw)
}

/// The snapshot probe's record.
#[derive(Debug, Default)]
struct Probe {
    queries: u64,
    latency: LatencyHistogram,
    elapsed: Duration,
}

/// Takes a snapshot and reads writer 0's [`PROBE_ROWS`] newest rows, back to
/// back, until `stop`.
///
/// The read is pinned to a backward scan of the primary key. The statement
/// is prepared while the table is still empty, and the plan cached then (a
/// sequential scan and a sort) would read the whole table on every probe,
/// so the probe's cost would grow with the table and swamp the visibility
/// checks it exists to time.
async fn probe_until(raw: RawClient, stop: Arc<AtomicBool>) -> (Probe, RawClient) {
    raw.batch_execute(
        "set enable_seqscan = off; set enable_bitmapscan = off; set enable_sort = off",
    )
    .await
    .expect("pin the probe to an index scan");
    let sql = raw
        .prepare(&format!(
            "select count(*) from (select 1 from public.{SOURCE_TABLE} \
             where id < $1 order by id desc limit {PROBE_ROWS}) t"
        ))
        .await
        .expect("prepare the snapshot probe");
    let bound = 2 * WRITER_BAND;
    let mut out = Probe::default();
    let start = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let t = Instant::now();
        raw.query_one(&sql, &[&bound])
            .await
            .expect("snapshot probe");
        out.latency.record(t.elapsed());
        out.queries += 1;
    }
    out.elapsed = start.elapsed();
    (out, raw)
}

// --- Writers ----------------------------------------------------------------

/// What the writers did.
#[derive(Debug, Default)]
struct Writes {
    txns: u64,
    rows: u64,
    latency: LatencyHistogram,
    elapsed: Duration,
}

/// How many transactions a cell runs: `rows` rounded up to whole
/// transactions.
pub fn target_txns(rows: u64, rows_per_txn: u64) -> u64 {
    rows.div_ceil(rows_per_txn.max(1)).max(1)
}

/// One writer's connection and its prepared insert.
type Writer = (usize, RawClient, tokio_postgres::Statement);

/// Connects and prepares every writer of `shape`, before the window opens,
/// so connection setup is never measured.
async fn connect_writers(dsn: &str, shape: Shape) -> Vec<Writer> {
    let mut writers = Vec::with_capacity(shape.writers);
    for w in 0..shape.writers {
        let raw = connect_raw(dsn).await;
        let sql = match shape.kind {
            Kind::Batch { .. } => format!(
                "insert into public.{SOURCE_TABLE} (id, val) \
                 select g, g from generate_series($1::bigint, $2::bigint) g"
            ),
            Kind::Orm { .. } => format!(
                "insert into public.{SOURCE_TABLE} (id, val) values ($1::bigint, $1::bigint)"
            ),
            Kind::Copy => unreachable!("a copy cell has no writers"),
        };
        let statement = raw
            .prepare(&sql)
            .await
            .expect("prepare the writer's insert");
        writers.push((w, raw, statement));
    }
    writers
}

/// Runs `txns` transactions of `shape` across `writers`, stopping early at
/// `max_window`.
async fn run_writers(
    writers: Vec<Writer>,
    shape: Shape,
    txns: u64,
    max_window: Duration,
) -> Writes {
    let rows_per_txn = shape.rows_per_txn(0);
    let claimed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let deadline = start + max_window;
    let tasks: Vec<_> = writers
        .into_iter()
        .map(|(w, mut raw, statement)| {
            let claimed = Arc::clone(&claimed);
            tokio::spawn(async move {
                let mut next_id = (w as i64 + 1) * WRITER_BAND;
                let mut done = 0u64;
                let mut latency = LatencyHistogram::default();
                while Instant::now() < deadline && claimed.fetch_add(1, Ordering::Relaxed) < txns {
                    let t = Instant::now();
                    match shape.kind {
                        Kind::Batch { .. } => {
                            let hi = next_id + rows_per_txn as i64 - 1;
                            raw.execute(&statement, &[&next_id, &hi])
                                .await
                                .expect("batch insert");
                        }
                        Kind::Orm { statements } => {
                            let txn = raw.transaction().await.expect("begin");
                            for i in 0..statements as i64 {
                                txn.execute(&statement, &[&(next_id + i)])
                                    .await
                                    .expect("single-row insert");
                            }
                            txn.commit().await.expect("commit");
                        }
                        Kind::Copy => unreachable!(),
                    }
                    latency.record(t.elapsed());
                    next_id += rows_per_txn as i64;
                    done += 1;
                }
                (done, latency)
            })
        })
        .collect();

    let mut out = Writes::default();
    for task in tasks {
        let (done, latency) = task.await.expect("writer task panicked");
        out.txns += done;
        out.latency.merge(&latency);
    }
    out.elapsed = start.elapsed();
    out.rows = out.txns * rows_per_txn;
    out
}

/// The `COPY` cell's data, generated before the clock starts so the client
/// never limits the load: `id, val` lines in writer 0's band.
fn copy_chunks(rows: u64) -> Vec<Bytes> {
    use std::io::Write as _;
    let mut chunks = Vec::new();
    let mut buf = Vec::with_capacity(COPY_CHUNK_BYTES + 64);
    for i in 0..rows as i64 {
        let id = WRITER_BAND + i;
        writeln!(buf, "{id}\t{id}").expect("format a copy row");
        if buf.len() >= COPY_CHUNK_BYTES {
            chunks.push(Bytes::from(std::mem::replace(
                &mut buf,
                Vec::with_capacity(COPY_CHUNK_BYTES + 64),
            )));
        }
    }
    if !buf.is_empty() {
        chunks.push(Bytes::from(buf));
    }
    chunks
}

async fn run_copy(raw: &RawClient, chunks: Vec<Bytes>, rows: u64) -> Writes {
    let start = Instant::now();
    let sink = raw
        .copy_in::<_, Bytes>(&format!("copy public.{SOURCE_TABLE} (id, val) from stdin"))
        .await
        .expect("start copy");
    let mut sink = pin!(sink);
    for chunk in chunks {
        sink.send(chunk).await.expect("copy data");
    }
    let copied = sink.as_mut().finish().await.expect("finish copy");
    assert_eq!(copied, rows, "copy loaded every row");
    let elapsed = start.elapsed();
    let mut latency = LatencyHistogram::default();
    latency.record(elapsed);
    Writes {
        txns: 1,
        rows,
        latency,
        elapsed,
    }
}

// --- One cell -----------------------------------------------------------------

/// One cell's measurements.
#[derive(Debug)]
pub struct CellResult {
    pub scenario: &'static str,
    pub cell: Cell,
    settings_json: String,
    pub rows: u64,
    pub txns: u64,
    pub secs: f64,
    pub rows_per_sec: f64,
    /// `rows_per_sec` over the same repetition's head control, when there is one.
    pub share_of_control: Option<f64>,
    pub pg_cpu_us_per_row: f64,
    pub engine_cpu_us_per_row: f64,
    /// `pg_cpu_us_per_row + engine_cpu_us_per_row` less the head control's.
    pub extra_cpu_us_per_row: Option<f64>,
    pub commit_p50_ms: Option<f64>,
    pub commit_p99_ms: Option<f64>,
    pub wal_bytes_per_row: f64,
    disk: disk_tier::DiskTier,
    top_waits: Vec<(String, f64)>,
    pub subxact_overflow_share: Option<f64>,
    pub max_subxact_count: i32,
    subtrans: (i64, i64, i64),
    pub probe_qps: Option<f64>,
    probe_p50_ms: Option<f64>,
    probe_p99_ms: Option<f64>,
    /// Rows/s reaching the ring: the writers' rate for a trigger (the ring
    /// row commits with the source row), intake's staged rate over the
    /// window plus its catch-up for `slot`, `None` without capture.
    pub capture_rows_per_sec: Option<f64>,
    slot_catch_up_secs: Option<f64>,
    slot_caught_up: Option<bool>,
    pub ring_rows: i64,
    /// The ring holds exactly the rows written (no capture: none).
    pub ring_ok: bool,
}

fn opt_f(v: Option<f64>, decimals: usize) -> String {
    v.map(|x| format!("{x:.decimals$}"))
        .unwrap_or_else(|| "null".into())
}

fn opt_b(v: Option<bool>) -> String {
    v.map(|b| b.to_string()).unwrap_or_else(|| "null".into())
}

impl CellResult {
    pub fn to_json(&self) -> String {
        let waits: Vec<String> = self
            .top_waits
            .iter()
            .map(|(k, v)| format!("\"{}\":{v:.3}", json_escape(k)))
            .collect();
        let shape = self.cell.shape;
        format!(
            "{{\"scenario\":\"{}\",\"variant\":\"{}\",\"shape\":\"{}\",\"writers\":{},\
             \"rows_per_txn\":{},\"statements_per_txn\":{},\"hold_xmin\":{},\"rep\":{},\
             \"position\":\"{}\",{},\"rows\":{},\"txns\":{},\"secs\":{:.3},\
             \"rows_per_sec\":{:.1},\"share_of_control\":{},\"pg_cpu_us_per_row\":{:.3},\
             \"engine_cpu_us_per_row\":{:.3},\"extra_cpu_us_per_row\":{},\
             \"commit_p50_ms\":{},\"commit_p99_ms\":{},\"wal_bytes_per_row\":{:.1},{},\
             \"top_waits\":{{{}}},\"subxact_overflow_share\":{},\"max_subxact_count\":{},\
             \"subtrans_slru_hits\":{},\"subtrans_slru_reads\":{},\"subtrans_slru_zeroed\":{},\
             \"probe_qps\":{},\"probe_p50_ms\":{},\"probe_p99_ms\":{},\
             \"capture_rows_per_sec\":{},\"slot_catch_up_secs\":{},\"slot_caught_up\":{},\
             \"ring_rows\":{},\"ring_ok\":{}}}",
            self.scenario,
            self.cell.variant.name(),
            shape.label(),
            shape.writers,
            self.rows.checked_div(self.txns).unwrap_or(0),
            shape.statements_per_txn(),
            shape.hold_xmin,
            self.cell.rep,
            self.cell.position.name(),
            self.settings_json,
            self.rows,
            self.txns,
            self.secs,
            self.rows_per_sec,
            opt_f(self.share_of_control, 4),
            self.pg_cpu_us_per_row,
            self.engine_cpu_us_per_row,
            opt_f(self.extra_cpu_us_per_row, 3),
            json_ms(self.commit_p50_ms),
            json_ms(self.commit_p99_ms),
            self.wal_bytes_per_row,
            self.disk.json_fields(),
            waits.join(","),
            opt_f(self.subxact_overflow_share, 4),
            self.max_subxact_count,
            self.subtrans.0,
            self.subtrans.1,
            self.subtrans.2,
            opt_f(self.probe_qps, 1),
            json_ms(self.probe_p50_ms),
            json_ms(self.probe_p99_ms),
            opt_f(self.capture_rows_per_sec, 1),
            opt_f(self.slot_catch_up_secs, 2),
            opt_b(self.slot_caught_up),
            self.ring_rows,
            self.ring_ok,
        )
    }

    /// One line for stderr.
    pub fn human(&self) -> String {
        let share = self
            .share_of_control
            .map(|s| format!(" ({:.0}% of control)", s * 100.0))
            .unwrap_or_default();
        let extra = self
            .extra_cpu_us_per_row
            .map(|e| format!(", {e:+.2} us/row CPU"))
            .unwrap_or_default();
        let probe = self
            .probe_qps
            .map(|q| format!(", probe {q:.0} snapshots/s"))
            .unwrap_or_default();
        let overflow = self
            .subxact_overflow_share
            .filter(|s| *s > 0.0)
            .map(|s| format!(", SUBXACT OVERFLOW in {:.0}% of samples", s * 100.0))
            .unwrap_or_default();
        format!(
            "[rep {}] {} {}: {:.0} rows/s{share}{extra}, p50/p99 {}/{} ms, {:.0} B/row WAL{probe}{overflow}{}",
            self.cell.rep,
            self.cell.shape.label(),
            self.cell.variant.name(),
            self.rows_per_sec,
            json_ms(self.commit_p50_ms),
            json_ms(self.commit_p99_ms),
            self.wal_bytes_per_row,
            if self.ring_ok { "" } else { ", RING MISMATCH" },
        )
    }
}

/// The rows/s `slot` staged over `span` (the writers' window plus the
/// catch-up), from the ring's count once the catch-up ended. The sentinel is
/// staged last, so it is in the count only when intake caught up; a cell that
/// hit the cap reports what intake staged, not what the writers offered.
pub fn slot_staged_rows_per_sec(ring_rows: i64, caught_up: bool, span: f64) -> f64 {
    let staged = if caught_up { ring_rows - 1 } else { ring_rows };
    staged.max(0) as f64 / span
}

/// The head control's numbers a cell is compared with.
#[derive(Debug, Clone, Copy)]
pub struct Control {
    pub rows_per_sec: f64,
    pub cpu_us_per_row: f64,
}

/// A cell's share of the control's rate, and its extra CPU per row.
pub fn against_control(
    control: Option<Control>,
    rows_per_sec: f64,
    cpu_us_per_row: f64,
) -> (Option<f64>, Option<f64>) {
    match control {
        Some(c) if c.rows_per_sec > 0.0 => (
            Some(rows_per_sec / c.rows_per_sec),
            Some(cpu_us_per_row - c.cpu_us_per_row),
        ),
        _ => (None, None),
    }
}

/// Sets `variant` up on a fresh database: an index, the capture triggers, or
/// a running client. Returns the client for `slot`.
async fn set_up_variant(
    variant: Variant,
    db: &testkit::TestDatabase,
    raw: &mut RawClient,
) -> Option<trellis::Client> {
    match variant {
        Variant::None => None,
        Variant::Btree => {
            raw.batch_execute(&format!(
                "create index wt_val_idx on public.{SOURCE_TABLE} (val)"
            ))
            .await
            .expect("create the btree index");
            None
        }
        Variant::RegexIndex => {
            raw.batch_execute(&format!(
                "create index wt_val_regex_idx on public.{SOURCE_TABLE} \
                 (regexp_count(val::text, '[13579]'))"
            ))
            .await
            .expect("create the expression index");
            None
        }
        Variant::Slot => {
            let client = trellis::Client::start(
                db.dsn(),
                ClientOptions {
                    staging_worker: true,
                    application_threads: 0,
                    ..Default::default()
                },
            )
            .expect("client start");
            let deadline = Instant::now() + SLOT_SETUP_TIMEOUT;
            wait_for_markers_discharged(raw, deadline).await;
            loop {
                let active: i64 = raw
                    .query_one(
                        "select count(*) from pg_replication_slots where active",
                        &[],
                    )
                    .await
                    .expect("read pg_replication_slots")
                    .get(0);
                if active > 0 {
                    break;
                }
                assert!(Instant::now() < deadline, "the slot never became active");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // As E1: let intake settle past its startup work.
            tokio::time::sleep(Duration::from_secs(1)).await;
            Some(client)
        }
        Variant::Trigger
        | Variant::TriggerException
        | Variant::TriggerColumnCheck
        | Variant::TriggerColumnCheckGuard
        | Variant::TriggerColumnCheckPerTxn => {
            let table = format!("public.{SOURCE_TABLE}");
            let catalog = trellis::dev::capture::load_catalog(&*raw, DEFAULT_SCHEMA)
                .await
                .expect("load the capture catalog");
            let spec: CaptureSpec = trellis::dev::capture::capture_spec(&*raw, &catalog, &table)
                .await
                .expect("compute the capture spec");
            assert!(
                trellis::dev::capture::install(raw, DEFAULT_SCHEMA, &spec, None)
                    .await
                    .expect("install the capture triggers"),
                "a fresh table's install changes something"
            );
            for event in REWRITTEN_EVENTS {
                let ddl = trellis::dev::capture::function_ddl(DEFAULT_SCHEMA, &spec, event)
                    .expect("generate the capture function");
                let replaced = match variant {
                    Variant::TriggerException => exception_variant(&ddl),
                    Variant::TriggerColumnCheck => column_check_variant(&ddl, spec.columns()),
                    Variant::TriggerColumnCheckGuard => {
                        column_check_guard_variant(&ddl, spec.columns())
                    }
                    Variant::TriggerColumnCheckPerTxn => {
                        column_check_per_txn_variant(&ddl, spec.columns())
                    }
                    _ => continue,
                };
                raw.batch_execute(&replaced).await.unwrap_or_else(|e| {
                    panic!("replace the {} function: {e}\n{replaced}", event.as_str())
                });
            }
            None
        }
    }
}

/// Runs one cell on a fresh cluster.
pub async fn run_cell(
    scenario: &'static str,
    cell: Cell,
    opts: &CellOptions,
    control: Option<Control>,
) -> CellResult {
    let shape = cell.shape;
    let variant = cell.variant;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(&format!(
        "create table public.{SOURCE_TABLE} (id bigint primary key, val numeric)"
    ))
    .await
    .expect("create the source table");
    // The same catalog for every variant: one transform reading `val`.
    install_chain_hops(&db.pool, SOURCE_TABLE, 1).await;
    let client = set_up_variant(variant, &db, &mut raw).await;

    let rows_target = opts
        .rows
        .unwrap_or_else(|| shape.default_rows(opts.copy_rows));
    let copy = matches!(shape.kind, Kind::Copy).then(|| copy_chunks(rows_target));

    let holder = if shape.hold_xmin {
        let holder = connect_raw(db.dsn()).await;
        holder
            .batch_execute("begin; select pg_current_xact_id()")
            .await
            .expect("open the xmin holder");
        Some(holder)
    } else {
        None
    };

    let settings = settings(&raw).await;
    let storage = disk_tier::storage();
    let settings_json = format!(
        "\"server_version\":\"{}\",\"wal_level\":\"{}\",\"fsync\":\"{}\",\
         \"synchronous_commit\":\"{}\",\"full_page_writes\":\"{}\",\"shared_buffers\":\"{}\",\
         \"storage_dir\":\"{}\",\"storage_fs_type\":\"{}\"",
        json_escape(&settings.server_version),
        json_escape(&settings.wal_level),
        json_escape(&settings.fsync),
        json_escape(&settings.synchronous_commit),
        json_escape(&settings.full_page_writes),
        json_escape(&settings.shared_buffers),
        json_escape(&storage.dir),
        json_escape(&storage.fs_type),
    );

    raw.batch_execute("checkpoint").await.expect("checkpoint");

    // The harness's own backends, whose CPU is taken out of the cell's.
    let sampler = connect_raw(db.dsn()).await;
    let sampler_pid = backend_pid(&sampler).await;
    let probe = if opts.probe.runs_for(&shape) {
        let probe = connect_raw(db.dsn()).await;
        let pid = backend_pid(&probe).await;
        Some((probe, pid))
    } else {
        None
    };
    let mut harness_pids = vec![sampler_pid];
    harness_pids.extend(probe.as_ref().map(|(_, pid)| *pid));
    let harness_cpu = || -> f64 {
        harness_pids
            .iter()
            .filter_map(|pid| proc_cpu(&pid.to_string(), false))
            .sum()
    };

    let writers = match shape.kind {
        Kind::Copy => Vec::new(),
        _ => connect_writers(db.dsn(), shape).await,
    };

    let postmaster = cluster.server_pid();
    let subtrans_before = subtrans_slru(&raw).await;
    let disk_before = disk_tier::sample(&raw).await;
    let lsn_before = wal_lsn(&raw).await;
    let harness_before = harness_cpu();
    let pg_before = pg_cpu(postmaster);
    let engine_before = engine_cpu();

    let stop = Arc::new(AtomicBool::new(false));
    let sampling = tokio::spawn(sample_until(
        sampler,
        harness_pids.clone(),
        Arc::clone(&stop),
    ));
    let probing = probe.map(|(probe, _)| tokio::spawn(probe_until(probe, Arc::clone(&stop))));

    let writes = match copy {
        Some(chunks) => run_copy(&raw, chunks, rows_target).await,
        None => {
            let txns = target_txns(rows_target, shape.rows_per_txn(0));
            run_writers(writers, shape, txns, opts.max_window).await
        }
    };

    stop.store(true, Ordering::Relaxed);
    // The harness's connections stay open until their backends' CPU is
    // read below: a backend that exits hands its CPU to the postmaster's
    // `cutime`, where it can no longer be told apart from the cell's.
    let (samples, _sampler) = sampling.await.expect("sampler panicked");
    let (probe, _probe) = match probing {
        Some(task) => {
            let (probe, raw) = task.await.expect("probe panicked");
            (Some(probe), Some(raw))
        }
        None => (None, None),
    };
    if let Some(holder) = holder {
        holder
            .batch_execute("rollback")
            .await
            .expect("close the xmin holder");
    }
    let disk = disk_tier::since(&raw, &disk_before).await;

    // `slot`: wait for intake to stage everything the writers committed, so
    // its CPU and the ring's WAL are counted. A sentinel row committed after
    // the writers finish is staged after all of theirs (intake stages in
    // commit order), and nothing else commits after `sentinel_from`, so the
    // ring's newest `origin_lsn` (the commit position, read through the
    // `origin_lsn` indexes) passing it means the ring holds every row.
    let (slot_catch_up_secs, slot_caught_up) = if client.is_some() {
        let t = Instant::now();
        let sentinel_from = wal_insert_lsn(&raw).await;
        raw.batch_execute(&format!(
            "insert into public.{SOURCE_TABLE} (id, val) values (0, 0)"
        ))
        .await
        .expect("insert the catch-up sentinel");
        let newest = newest_ring_origin_sql(&raw).await;
        let deadline = t + opts.slot_catch_up;
        let caught_up = loop {
            let staged: bool = raw
                .query_one(&newest, &[&sentinel_from])
                .await
                .expect("read the ring's newest origin_lsn")
                .get(0);
            if staged {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        (Some(t.elapsed().as_secs_f64()), Some(caught_up))
    } else {
        (None, None)
    };

    let pg_after = pg_cpu(postmaster);
    let engine_after = engine_cpu();
    let harness_after = harness_cpu();
    let wal_bytes = wal_bytes_since(&raw, &lsn_before).await;
    let subtrans_after = subtrans_slru(&raw).await;
    if let Some(client) = client {
        client.shutdown().await.expect("client shutdown");
    }
    let ring_rows = total_ring_rows(&raw).await;

    let rows = writes.rows.max(1) as f64;
    let secs = writes.elapsed.as_secs_f64();
    let rows_per_sec = writes.rows as f64 / secs;
    let pg_cpu_us_per_row =
        ((pg_after - pg_before) - (harness_after - harness_before)).max(0.0) / rows * 1e6;
    let engine_cpu_us_per_row = (engine_after - engine_before) / rows * 1e6;
    let (share_of_control, extra_cpu_us_per_row) =
        if variant == Variant::None && cell.position == Position::Head {
            (None, None)
        } else {
            against_control(
                control,
                rows_per_sec,
                pg_cpu_us_per_row + engine_cpu_us_per_row,
            )
        };
    let capture_rows_per_sec = if variant.is_trigger() {
        Some(rows_per_sec)
    } else if variant == Variant::Slot {
        slot_catch_up_secs
            .map(|c| slot_staged_rows_per_sec(ring_rows, slot_caught_up == Some(true), secs + c))
    } else {
        None
    };
    let expected_ring = match variant {
        // The catch-up sentinel is staged too.
        Variant::Slot => writes.rows as i64 + 1,
        v if v.captures() => writes.rows as i64,
        _ => 0,
    };

    CellResult {
        scenario,
        cell,
        settings_json,
        rows: writes.rows,
        txns: writes.txns,
        secs,
        rows_per_sec,
        share_of_control,
        pg_cpu_us_per_row,
        engine_cpu_us_per_row,
        extra_cpu_us_per_row,
        commit_p50_ms: writes.latency.quantile_ms(0.5),
        commit_p99_ms: writes.latency.quantile_ms(0.99),
        wal_bytes_per_row: wal_bytes as f64 / rows,
        disk,
        top_waits: samples.top_waits(),
        subxact_overflow_share: samples.overflow_share(),
        max_subxact_count: samples.max_subxact_count,
        subtrans: (
            subtrans_after.0 - subtrans_before.0,
            subtrans_after.1 - subtrans_before.1,
            subtrans_after.2 - subtrans_before.2,
        ),
        probe_qps: probe
            .as_ref()
            .map(|p| p.queries as f64 / p.elapsed.as_secs_f64().max(1e-9)),
        probe_p50_ms: probe.as_ref().and_then(|p| p.latency.quantile_ms(0.5)),
        probe_p99_ms: probe.as_ref().and_then(|p| p.latency.quantile_ms(0.99)),
        capture_rows_per_sec,
        slot_catch_up_secs,
        slot_caught_up,
        ring_rows,
        ring_ok: ring_rows == expected_ring,
    }
}

/// Runs every cell of `schedule(variants, shapes, reps)`, printing each
/// result's JSON line to stdout and a readable line to stderr as it
/// finishes, and returns them all. Each non-control cell is compared with
/// its repetition's head control for the same shape.
pub async fn run_matrix(
    scenario: &'static str,
    variants: &[Variant],
    shapes: &[Shape],
    reps: usize,
    opts: &CellOptions,
) -> Vec<CellResult> {
    let cells = schedule(variants, shapes, reps);
    let total = cells.len();
    let mut control: Option<Control> = None;
    let mut results = Vec::with_capacity(total);
    for (i, cell) in cells.into_iter().enumerate() {
        if cell.position == Position::Head {
            control = None;
        }
        eprintln!(
            "{scenario} cell {}/{total}: rep {} {} {} ...",
            i + 1,
            cell.rep,
            cell.shape.label(),
            cell.variant.name()
        );
        let result = run_cell(scenario, cell, opts, control).await;
        // The previous cell's cluster is gone; let the box settle (its
        // freed memory, a checkpoint's writeback on disk) before the next.
        tokio::time::sleep(CELL_SETTLE).await;
        if cell.position == Position::Head {
            control = Some(Control {
                rows_per_sec: result.rows_per_sec,
                cpu_us_per_row: result.pg_cpu_us_per_row + result.engine_cpu_us_per_row,
            });
        }
        println!("{}", result.to_json());
        eprintln!("  {}", result.human());
        results.push(result);
    }
    results
}

/// The median of `values`, `None` when empty.
pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}

/// A per-shape, per-variant table of medians across repetitions, for
/// stderr at the end of a session. The recorded numbers come from the JSON
/// lines (with contended cells dropped), not from this.
pub fn summary(results: &[CellResult]) -> String {
    let mut keys: Vec<(String, Variant)> = results
        .iter()
        .map(|r| (r.cell.shape.label(), r.cell.variant))
        .collect();
    keys.sort();
    keys.dedup();
    let mut out = String::from(
        "shape            variant                rows/s   share  +CPU us/row  p50 ms   p99 ms  WAL B/row  probe/s\n",
    );
    for (shape, variant) in keys {
        let of = |f: &dyn Fn(&CellResult) -> Option<f64>| -> Option<f64> {
            let mut v: Vec<f64> = results
                .iter()
                .filter(|r| r.cell.shape.label() == shape && r.cell.variant == variant)
                .filter_map(f)
                .collect();
            median(&mut v)
        };
        let cell = |v: Option<f64>, decimals: usize| {
            v.map(|x| format!("{x:.decimals$}"))
                .unwrap_or_else(|| "-".into())
        };
        out.push_str(&format!(
            "{shape:<16} {:<20} {:>10} {:>7} {:>12} {:>7} {:>8} {:>10} {:>8}\n",
            variant.name(),
            cell(of(&|r| Some(r.rows_per_sec)), 0),
            cell(of(&|r| r.share_of_control), 2),
            cell(of(&|r| r.extra_cpu_us_per_row), 2),
            cell(of(&|r| r.commit_p50_ms), 3),
            cell(of(&|r| r.commit_p99_ms), 3),
            cell(of(&|r| Some(r.wal_bytes_per_row)), 0),
            cell(of(&|r| r.probe_qps), 0),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::dev::capture::function_ddl;

    fn spec() -> CaptureSpec {
        CaptureSpec::new(
            "public.wt_src",
            vec!["id".to_string()],
            vec!["id".to_string(), "val".to_string()],
            Vec::new(),
        )
        .expect("valid spec")
    }

    fn ddl(event: CaptureEvent) -> String {
        function_ddl("trellis", &spec(), event).expect("ddl")
    }

    #[test]
    fn shapes_parse_and_label_round_trip() {
        for raw in DEFAULT_SHAPES {
            assert_eq!(Shape::parse(raw).label(), *raw);
        }
        assert_eq!(
            Shape::parse("orm100x16+hold"),
            Shape {
                kind: Kind::Orm { statements: 100 },
                writers: 16,
                hold_xmin: true
            }
        );
        assert_eq!(Shape::parse("1000x16"), Shape::batch(1000, 16));
        assert_eq!(Shape::parse("copy").kind, Kind::Copy);
    }

    #[test]
    #[should_panic(expected = "must be <rows>x<writers>")]
    fn a_shape_without_writers_is_rejected() {
        Shape::parse("1000");
    }

    #[test]
    #[should_panic(expected = "must be at least 1")]
    fn a_shape_with_no_writers_is_rejected() {
        Shape::parse("1x0");
    }

    #[test]
    fn variants_parse_by_name() {
        for v in Variant::ALL {
            assert_eq!(Variant::parse(v.name()), v);
        }
    }

    #[test]
    fn rows_per_transaction_and_statements_follow_the_shape() {
        assert_eq!(Shape::parse("1000x1").rows_per_txn(5), 1000);
        assert_eq!(Shape::parse("1000x1").statements_per_txn(), 1);
        assert_eq!(Shape::parse("orm100x1").rows_per_txn(5), 100);
        assert_eq!(Shape::parse("orm100x1").statements_per_txn(), 100);
        assert_eq!(Shape::parse("copy").rows_per_txn(5), 5);
        assert_eq!(target_txns(400_000, 1), 400_000);
        assert_eq!(
            target_txns(1_000, 300),
            4,
            "rounded up to whole transactions"
        );
        assert_eq!(target_txns(0, 100), 1);
    }

    #[test]
    fn default_rows_follow_e1_and_scale_with_writers() {
        assert_eq!(Shape::parse("1x1").default_rows(0), 400_000);
        assert_eq!(Shape::parse("1x16").default_rows(0), 1_600_000);
        assert_eq!(Shape::parse("100x1").default_rows(0), 2_000_000);
        assert_eq!(Shape::parse("1000x16").default_rows(0), 4_000_000);
        assert_eq!(Shape::parse("orm100x1").default_rows(0), 400_000);
        assert_eq!(Shape::parse("copy").default_rows(7), 7);
    }

    #[test]
    fn the_schedule_brackets_each_shape_with_the_control_and_rotates_the_rest() {
        let variants = [
            Variant::None,
            Variant::Slot,
            Variant::Trigger,
            Variant::Btree,
        ];
        let shapes = [Shape::batch(1, 1), Shape::batch(1000, 16)];
        let cells = schedule(&variants, &shapes, 2);
        assert_eq!(cells.len(), 2 * 2 * 5);
        let names = |rep: usize, shape: Shape| -> Vec<&str> {
            cells
                .iter()
                .filter(|c| c.rep == rep && c.shape == shape)
                .map(|c| c.variant.name())
                .collect()
        };
        assert_eq!(
            names(0, shapes[0]),
            ["none", "slot", "trigger", "btree", "none"]
        );
        assert_eq!(
            names(1, shapes[0]),
            ["none", "trigger", "btree", "slot", "none"]
        );
        // Repetitions are the outer loop.
        assert!(cells[..10].iter().all(|c| c.rep == 0));
        assert_eq!(cells[0].position, Position::Head);
        assert_eq!(cells[4].position, Position::Tail);
        assert_eq!(cells[5].shape, shapes[1]);
    }

    #[test]
    fn a_schedule_without_the_control_has_no_brackets() {
        let cells = schedule(&[Variant::Trigger], &[Shape::batch(1, 1)], 3);
        assert_eq!(cells.len(), 3);
        assert!(cells.iter().all(|c| c.position == Position::Middle));
    }

    #[test]
    fn share_and_extra_cpu_are_against_the_control() {
        let control = Some(Control {
            rows_per_sec: 70_000.0,
            cpu_us_per_row: 12.0,
        });
        let (share, extra) = against_control(control, 35_000.0, 28.5);
        assert_eq!(share, Some(0.5));
        assert_eq!(extra, Some(16.5));
        assert_eq!(against_control(None, 1.0, 1.0), (None, None));
    }

    #[test]
    fn slot_reports_what_it_staged_not_what_was_offered() {
        // Caught up: every row plus the sentinel.
        assert_eq!(slot_staged_rows_per_sec(1_001, true, 10.0), 100.0);
        // Hit the cap with none of a big COPY staged (#565 E1's 10M-row case).
        assert_eq!(slot_staged_rows_per_sec(0, false, 180.0), 0.0);
        assert_eq!(slot_staged_rows_per_sec(500, false, 5.0), 100.0);
    }

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&mut []), None);
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&mut [4.0, 1.0, 2.0, 3.0]), Some(2.5));
    }

    #[test]
    fn top_waits_are_shares_of_every_sampled_backend() {
        let mut s = Samples::default();
        s.waits.insert("CPU:-".into(), 6);
        s.waits.insert("LWLock:WALWrite".into(), 3);
        s.waits.insert("IO:WalSync".into(), 1);
        let top = s.top_waits();
        assert_eq!(top[0], ("CPU:-".to_string(), 0.6));
        assert_eq!(top[1], ("LWLock:WALWrite".to_string(), 0.3));
        assert_eq!(s.overflow_share(), None);
        s.samples = 4;
        s.overflowed = 1;
        assert_eq!(s.overflow_share(), Some(0.25));
    }

    #[test]
    fn the_exception_variant_wraps_the_capture_after_the_empty_check() {
        let before = ddl(CaptureEvent::Insert);
        let after = exception_variant(&before);
        let check = after.find("if not exists").expect("empty check");
        let block = after.find("    begin\n    -- #597:").expect("inner block");
        let handler = after.find("exception when others then").expect("handler");
        assert!(check < block && block < handler);
        assert!(after.contains("    end;\n    return null;\nend;\n"));
        // Everything else is untouched.
        assert!(after.contains("security definer"));
        assert_eq!(
            after.matches("insert into").count(),
            before.matches("insert into").count()
        );
    }

    #[test]
    fn the_column_check_variant_probes_pg_attribute_in_the_mirror_statement() {
        let before = ddl(CaptureEvent::Update);
        let after = column_check_variant(&before, spec().columns());
        assert!(!after.contains("slot := case"), "{after}");
        let select = after
            .find("    select case when pg_current_xact_id()")
            .expect("select");
        let probe = after.find("pg_catalog.pg_attribute a").expect("probe");
        let into = after.find("into slot, present;").expect("into");
        assert!(select < probe && probe < into, "{after}");
        assert!(after.contains("array['id', 'val']::name[]"), "{after}");
        assert!(after.contains("if present <> 2 then"), "{after}");
        assert!(after.contains("    present bigint;\nbegin\n"), "{after}");
        assert!(
            !after.contains("exception when"),
            "no subtransaction: {after}"
        );
    }

    #[test]
    fn the_guard_check_probes_in_the_empty_statement_query() {
        for (event, table) in [
            (CaptureEvent::Insert, "trellis_new"),
            (CaptureEvent::Update, "trellis_new"),
            (CaptureEvent::Delete, "trellis_old"),
        ] {
            let before = ddl(event);
            let after = column_check_guard_variant(&before, spec().columns());
            assert!(!after.contains("if not exists"), "{after}");
            let guard = after
                .find(&format!(
                    "    present := case when exists (select 1 from {table})"
                ))
                .expect("guard");
            let probe = after.find("pg_catalog.pg_attribute a").expect("probe");
            let empty = after.find("if present < 0 then").expect("empty return");
            let miss = after.find("if present <> 2 then").expect("miss return");
            let read = after
                .find("    slot := case when")
                .expect("the mirror read stays a simple expression");
            assert!(
                guard < probe && probe < empty && empty < miss && miss < read,
                "{after}"
            );
            assert_eq!(after.matches("pg_catalog.pg_attribute").count(), 1);
            assert!(after.contains("    present bigint;\nbegin\n"), "{after}");
            assert!(!after.contains("exception when"), "{after}");
        }
    }

    #[test]
    fn the_per_transaction_check_keeps_the_mirror_read_a_simple_expression() {
        let before = ddl(CaptureEvent::Insert);
        let after = column_check_per_txn_variant(&before, spec().columns());
        let read = after
            .find("    slot := case when")
            .expect("mirror read kept");
        let check = after
            .find("current_setting('trellis.capture_checked_' || tg_relid, true)")
            .expect("per-transaction guard");
        let probe = after.find("pg_catalog.pg_attribute a").expect("probe");
        let mark = after
            .find("set_config('trellis.capture_checked_' || tg_relid, 'on', true)")
            .expect("marks the transaction checked");
        let capture = after.find("    case slot\n").expect("capture");
        assert!(
            read < check && check < probe && probe < mark && mark < capture,
            "{after}"
        );
        assert!(after.contains("if present <> 2 then"), "{after}");
        assert!(
            !after.contains("exception when"),
            "no subtransaction: {after}"
        );
    }

    #[test]
    fn every_rewritten_event_has_the_text_the_rewrites_expect() {
        for event in REWRITTEN_EVENTS {
            exception_variant(&ddl(event));
            column_check_variant(&ddl(event), spec().columns());
            column_check_guard_variant(&ddl(event), spec().columns());
            column_check_per_txn_variant(&ddl(event), spec().columns());
        }
    }

    #[test]
    #[should_panic(expected = "update the write-tax rewrite")]
    fn a_rewrite_that_no_longer_matches_fails_loudly() {
        exception_variant("create function f() returns trigger as $$ begin end $$");
    }
}
