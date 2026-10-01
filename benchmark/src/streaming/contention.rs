//! Where the engine's backends spend their time while an aggregate is under
//! load — issue #277's lock-wait attribution.
//!
//! [`sample`] polls `pg_stat_activity` on its own connection every
//! [`SAMPLE_INTERVAL`] for the length of a probe's offer window and sorts
//! every non-idle backend in the database into one [`WaitClass`]. Summed
//! over the window, a class's sample count is proportional to the backend
//! time spent in it, so [`ContentionSummary`]'s means read directly as "on
//! average, this many engine backends were doing X" — e.g. 6.1 of 8 drain
//! workers blocked on another transaction's row lock is contention, whatever
//! the throughput number says.
//!
//! Two splits matter for #277's hypothesis:
//!
//! * **Generator vs engine.** The load generator's own backends share the
//!   database, and one waiting on a lock would say nothing about Trellis. A
//!   backend running the generator's `INSERT INTO public.<source>` is
//!   counted apart ([`Role::Generator`]); every other client backend in the
//!   database — drain workers and the maintenance tick — is the engine. The
//!   capture triggers run inside the generator's own backends, so their ring
//!   writes count as the generator's.
//! * **Row locks vs everything else.** Two drain workers applying
//!   overlapping aggregate groups serialize on the target rows' tuple locks,
//!   which Postgres reports as a `Lock` wait on `transactionid` (waiting for
//!   the holder's transaction to end) or `tuple` (queued behind another
//!   waiter for the same row). Those two are [`WaitClass::RowLock`]; and a
//!   row-lock wait whose statement is the aggregate apply's ordered pre-lock
//!   (`... for update of t`, `staging::apply_aggregate`) is additionally
//!   counted as [`ContentionSummary::prelock_wait_mean`] — the one wait that
//!   is unambiguously "another drain worker holds my target groups".
//!
//! Sampling is cheap (one catalog query per interval, on a connection that is
//! never a generator's or the engine's) and stands in for lock-wait
//! *time*, which Postgres doesn't account for without `log_lock_waits` or an
//! extension this harness's ephemeral cluster doesn't have.
//!
//! **Lock waits and holds by statement class (#623 D1).** Each engine
//! statement is put in a [`StatementClass`] by its text: the claim, the
//! aggregate pre-lock, a ledger lock (any statement naming a `__ledger`
//! table; none exist before #623 D2), a group upsert (any other write naming
//! the scenario's target), or other. The same samples then give two
//! per-episode distributions, both from Postgres's own timestamps so they
//! don't depend on how the engine is built:
//!
//! * **Lock wait** ([`ContentionSummary::lock_wait_classes`], and
//!   [`ContentionSummary::page_lock_waits`] over the three page classes):
//!   one episode is one engine statement (`pid`, `query_start`) seen blocked
//!   on a heavyweight lock (`wait_event_type = 'Lock'`) at least once. Its
//!   wait is the sum, over each distinct lock it was seen waiting for, of
//!   the last sample it was still waiting minus `pg_locks.waitstart` (exact:
//!   when Postgres started the wait).
//! * **Page lock hold** ([`ContentionSummary::page_lock_holds`]): one episode
//!   is one engine transaction (`pid`, `xact_start`) seen running (or idle
//!   after) a pre-lock, ledger-lock or group-upsert statement. Its hold is
//!   the last sample the transaction was still open minus the earliest
//!   `query_start` of such a statement seen in it: from the first observed
//!   statement that locks target rows to the transaction's last observed
//!   instant, the span its row locks were held. **It includes that first
//!   statement's own lock wait**: a sorted pre-lock queued behind another
//!   page already holds the rows it locked before the one it waits on, so
//!   under contention a hold is part wait. Read it next to the wait columns,
//!   not as pure hold time. A statement that writes the target outside a
//!   drain page (build-under-load's backfill chunks) counts as a page too.
//!
//! Both ends that come from sampling are up to one [`SAMPLE_INTERVAL`] early,
//! so an episode reads short by up to 50 ms, and an episode that begins and
//! ends between two samples is never seen at all. The distributions are
//! therefore of the episodes that spanned a sample (length-biased toward
//! long ones), which is the tail the p99 and max are for; a p50 well under
//! the interval means little.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

use crate::streaming::disk_tier::{LatencyHistogram, json_ms};

/// How often [`sample`] reads `pg_stat_activity`. Fine enough that a
/// sub-second offer window still gets tens of samples, coarse enough that
/// the sampler's own query is noise next to the load.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

/// The aggregate apply's ordered pre-lock (`staging::apply_aggregate`'s
/// `apply_aggregate_target`) ends in this. So does a 1-1 transform's
/// composite-key pre-lock (`staging::apply`), which `fold_in`'s
/// aggregate-only pipeline never installs; a scenario that mixes the two
/// would have to tell them apart.
const PRELOCK_MARKER: &str = "for update of t";

/// The claim's statement (`staging::claim`'s `CLAIM_SQL`) takes the batch's
/// buckets with this.
const CLAIM_MARKER: &str = "insert into seg_claims";

/// The claim's first statement (`staging::claim`'s `LOCK_SEGMENT_SQL`, #690),
/// where a claim now waits for the segment's other claims and completions.
const CLAIM_LOCK_MARKER: &str = "select 1 from segments where seg_seq = $1 for no key update";

/// Every ledger table's name ends in this (#623 D2).
const LEDGER_MARKER: &str = "__ledger";

/// What an engine statement is, for lock-wait attribution (see the module
/// doc). Order is [`ContentionSummary::lock_wait_classes`]' order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementClass {
    /// The claim (`insert into seg_claims ...`).
    Claim,
    /// The aggregate's ordered target-row pre-lock ([`PRELOCK_MARKER`]).
    PreLock,
    /// Any statement naming a `%__ledger` table.
    LedgerLock,
    /// Any other `insert`/`update`/`delete` naming the target table: the
    /// group upserts, the bulk update/insert rounds, the empty-group delete.
    GroupUpsert,
    Other,
}

impl StatementClass {
    pub const ALL: [StatementClass; 5] = [
        StatementClass::Claim,
        StatementClass::PreLock,
        StatementClass::LedgerLock,
        StatementClass::GroupUpsert,
        StatementClass::Other,
    ];

    pub fn label(self) -> &'static str {
        match self {
            StatementClass::Claim => "claim",
            StatementClass::PreLock => "prelock",
            StatementClass::LedgerLock => "ledger_lock",
            StatementClass::GroupUpsert => "group_upsert",
            StatementClass::Other => "other",
        }
    }

    /// A statement that takes locks on target (or ledger) rows inside a
    /// drain page: what a page lock hold is measured from.
    pub fn locks_page_rows(self) -> bool {
        matches!(
            self,
            StatementClass::PreLock | StatementClass::LedgerLock | StatementClass::GroupUpsert
        )
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Classifies an engine statement. `target` is the scenario's target table's
/// bare name (`agg_totals`); an empty `target` never matches.
pub fn statement_class(query: &str, target: &str) -> StatementClass {
    let q = query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let target = target.to_ascii_lowercase();
    if q.contains(LEDGER_MARKER) {
        StatementClass::LedgerLock
    } else if q.contains(PRELOCK_MARKER) {
        StatementClass::PreLock
    } else if q.contains(CLAIM_MARKER) || q.contains(CLAIM_LOCK_MARKER) {
        StatementClass::Claim
    } else if !target.is_empty()
        && q.contains(&target)
        && (q.contains("insert into ") || q.contains("update ") || q.contains("delete from "))
    {
        StatementClass::GroupUpsert
    } else {
        StatementClass::Other
    }
}

/// A Re-derive build's statement classes (#625 F2), told apart by the
/// `application_name` its transactions set (`trellis::dev::staging`'s
/// `CHUNK_APPLICATION_NAME`/`MERGE_APPLICATION_NAME`): a chunk's entry lock
/// shares its text with a drain page's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuildClass {
    /// A chunk's entry lock: the placeholder insert and the sorted
    /// `for update`.
    ChunkLock,
    /// The rest of a chunk: its key read, its read-and-write statement (the
    /// entries and the delta insert) and its commit.
    ChunkWrite,
    /// A merger pass: the claim, sum and group upsert, the all-zero delete
    /// and the seam.
    MergeUpsert,
}

impl BuildClass {
    pub const ALL: [BuildClass; 3] = [
        BuildClass::ChunkLock,
        BuildClass::ChunkWrite,
        BuildClass::MergeUpsert,
    ];

    pub fn label(self) -> &'static str {
        match self {
            BuildClass::ChunkLock => "chunk_lock",
            BuildClass::ChunkWrite => "chunk_write",
            BuildClass::MergeUpsert => "merge_upsert",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// The build class of a backend running `query` under `application_name`,
/// or `None` for anything that isn't a Re-derive build's.
pub fn build_class(application_name: &str, query: &str) -> Option<BuildClass> {
    if application_name == trellis::dev::staging::MERGE_APPLICATION_NAME {
        return Some(BuildClass::MergeUpsert);
    }
    if application_name != trellis::dev::staging::CHUNK_APPLICATION_NAME {
        return None;
    }
    let q = query.to_ascii_lowercase();
    Some(
        if q.contains(LEDGER_MARKER)
            && (q.contains("for update") || q.contains("on conflict do nothing"))
            && !q.contains("pg_current_snapshot")
        {
            BuildClass::ChunkLock
        } else {
            BuildClass::ChunkWrite
        },
    )
}

/// One sampled wait, `type:event` (`Lock:transactionid`, `LWLock:WALWrite`,
/// `IO:DataFileRead`), or `running` / `idle_in_txn` when not waiting.
fn wait_label(state: &str, wait_event_type: Option<&str>, wait_event: Option<&str>) -> String {
    if state.starts_with("idle in transaction") {
        return "idle_in_txn".to_string();
    }
    match (wait_event_type, wait_event) {
        (Some(t), Some(e)) => format!("{t}:{e}"),
        (Some(t), None) => t.to_string(),
        (None, _) => "running".to_string(),
    }
}

/// Whose backend a sampled row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Generator,
    Engine,
}

/// What a sampled, non-idle backend was doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaitClass {
    /// Waiting on another transaction's row lock (`Lock`:`transactionid`
    /// or `Lock`:`tuple`).
    RowLock,
    /// Waiting on any other heavyweight lock (relation, advisory, ...).
    OtherLock,
    /// Waiting on a lightweight lock (buffer mapping, WAL insert, ...).
    LwLock,
    /// Waiting on I/O (including WAL flush at commit).
    Io,
    /// Inside a transaction but between statements — the client (Trellis)
    /// is working, or on its way back, while the transaction holds its
    /// locks.
    IdleInTransaction,
    /// Executing, not waiting on anything Postgres reports.
    Running,
    /// Any other wait (`Client`, `IPC`, `Timeout`, ...).
    Other,
}

/// Classifies one `pg_stat_activity` row. `state` is never `idle` (the
/// sampler filters those out); `wait_event_type`/`wait_event` are `None`
/// when the backend isn't waiting.
pub fn classify(state: &str, wait_event_type: Option<&str>, wait_event: Option<&str>) -> WaitClass {
    if state.starts_with("idle in transaction") {
        return WaitClass::IdleInTransaction;
    }
    match (wait_event_type, wait_event) {
        (None, _) => WaitClass::Running,
        (Some("Lock"), Some("transactionid" | "tuple")) => WaitClass::RowLock,
        (Some("Lock"), _) => WaitClass::OtherLock,
        (Some("LWLock"), _) => WaitClass::LwLock,
        (Some("IO"), _) => WaitClass::Io,
        (Some(_), _) => WaitClass::Other,
    }
}

/// Which side a backend running `query` is on. `generator_prefixes` are the
/// starts of the generators' own statements against the scenario's source
/// table, each through the space after the table name (see
/// [`generator_prefixes`]) so a table whose name merely starts with
/// `<source>` isn't mistaken for it.
pub fn role(query: &str, generator_prefixes: &[String]) -> Role {
    let q = query.trim_start().to_ascii_lowercase();
    if generator_prefixes.iter().any(|p| q.starts_with(p.as_str())) {
        Role::Generator
    } else {
        Role::Engine
    }
}

/// A distribution of episode durations (see the module doc).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DurationStats {
    pub count: u64,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub max_ms: Option<f64>,
}

impl DurationStats {
    /// `"<prefix>_ms_p50":..,"<prefix>_ms_p99":..,"<prefix>_ms_max":..,
    /// "<prefix>s":<count>`.
    pub fn json_fields(&self, prefix: &str) -> String {
        format!(
            "\"{prefix}_ms_p50\":{},\"{prefix}_ms_p99\":{},\"{prefix}_ms_max\":{},\"{prefix}s\":{}",
            json_ms(self.p50_ms),
            json_ms(self.p99_ms),
            json_ms(self.max_ms),
            self.count,
        )
    }
}

#[derive(Debug, Clone, Default)]
struct DurationRecorder {
    hist: LatencyHistogram,
    count: u64,
    max_secs: f64,
}

impl DurationRecorder {
    fn record(&mut self, secs: f64) {
        let secs = secs.max(0.0);
        self.hist.record(Duration::from_secs_f64(secs));
        self.count += 1;
        self.max_secs = self.max_secs.max(secs);
    }

    /// The histogram's quantiles are bucket upper bounds, so they are
    /// clamped to the exact max: a p99 above the max it summarizes reads as
    /// a bug in the numbers.
    fn stats(&self) -> DurationStats {
        let max_ms = (self.count > 0).then_some(self.max_secs * 1000.0);
        let quantile = |q| {
            self.hist
                .quantile_ms(q)
                .map(|ms| max_ms.map_or(ms, |max| ms.min(max)))
        };
        DurationStats {
            count: self.count,
            p50_ms: quantile(0.5),
            p99_ms: quantile(0.99),
            max_ms,
        }
    }
}

/// One statement class's heavyweight-lock waits.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ClassWaits {
    /// Engine backends of this class waiting on a heavyweight lock, averaged
    /// over the samples.
    pub wait_mean: f64,
    /// Its wait episodes (see the module doc).
    pub waits: DurationStats,
}

/// Per-class totals over a window of samples. Every `*_mean` is a count of
/// backends summed over all samples, divided by the sample count: the
/// average number of backends in that class at any instant.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContentionSummary {
    pub samples: u64,
    /// Engine backends not idle — the denominator the shares below use.
    pub engine_busy_mean: f64,
    pub engine_row_lock_mean: f64,
    /// The subset of `engine_row_lock_mean` blocked in the aggregate apply's
    /// ordered target-row pre-lock.
    pub prelock_wait_mean: f64,
    pub engine_other_lock_mean: f64,
    pub engine_lwlock_mean: f64,
    pub engine_io_mean: f64,
    pub engine_idle_in_txn_mean: f64,
    pub engine_running_mean: f64,
    pub engine_other_mean: f64,
    /// Generator backends waiting on any heavyweight lock — should be ~0;
    /// if not, the generator is contending with itself or the engine and the
    /// offered rate is suspect.
    pub generator_lock_mean: f64,
    /// Where busy engine backend time went, by statement: the
    /// [`TOP_STATEMENTS`] most-sampled `(class, statement prefix)` pairs, each
    /// with its mean backend count — so a row-lock wait, or a long-running
    /// statement, is attributed to the statement doing it.
    pub top_statements: Vec<(WaitClass, String, f64)>,
    /// Heavyweight-lock waits per [`StatementClass`], in
    /// [`StatementClass::ALL`] order.
    pub lock_wait_classes: [ClassWaits; 5],
    /// Wait episodes of the three page classes together (pre-lock, ledger
    /// lock, group upsert).
    pub page_lock_waits: DurationStats,
    /// Page transactions' row-lock hold (see the module doc).
    pub page_lock_holds: DurationStats,
    /// Per [`BuildClass::ALL`]: each sampled wait ([`wait_label`]) of that
    /// class's backends, as a mean backend count. A Re-derive build's
    /// statements are counted here and not in the page classes above.
    pub build_waits: [Vec<(String, f64)>; 3],
}

/// How many `(class, statement)` pairs [`ContentionSummary::top_statements`]
/// keeps.
pub const TOP_STATEMENTS: usize = 6;

/// How much of a statement's (whitespace-collapsed) text identifies it in
/// [`ContentionSummary::top_statements`].
const STATEMENT_PREFIX_CHARS: usize = 72;

/// `query` with runs of whitespace collapsed and cut to
/// [`STATEMENT_PREFIX_CHARS`] — enough to tell the engine's statements apart,
/// short enough to group every instance of one together.
pub fn statement_prefix(query: &str) -> String {
    query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(STATEMENT_PREFIX_CHARS)
        .collect()
}

impl WaitClass {
    pub fn label(self) -> &'static str {
        match self {
            WaitClass::RowLock => "row_lock",
            WaitClass::OtherLock => "other_lock",
            WaitClass::LwLock => "lwlock",
            WaitClass::Io => "io",
            WaitClass::IdleInTransaction => "idle_in_txn",
            WaitClass::Running => "running",
            WaitClass::Other => "other",
        }
    }
}

impl ContentionSummary {
    /// The share of busy engine backend time spent waiting on row locks —
    /// the headline attribution. `0.0` when the engine was never busy.
    pub fn row_lock_share(&self) -> f64 {
        if self.engine_busy_mean > 0.0 {
            self.engine_row_lock_mean / self.engine_busy_mean
        } else {
            0.0
        }
    }

    pub fn to_json(&self) -> String {
        format!(
            "{{\"samples\":{},\"engine_busy_mean\":{:.3},\"engine_row_lock_mean\":{:.3},\
             \"prelock_wait_mean\":{:.3},\"row_lock_share\":{:.4},\
             \"engine_other_lock_mean\":{:.3},\"engine_lwlock_mean\":{:.3},\
             \"engine_io_mean\":{:.3},\"engine_idle_in_txn_mean\":{:.3},\
             \"engine_running_mean\":{:.3},\"engine_other_mean\":{:.3},\
             \"generator_lock_mean\":{:.3},\"top_statements\":[{}],\
             \"lock_wait_classes\":{{{}}},\"build_waits\":{{{}}}}}",
            self.samples,
            self.engine_busy_mean,
            self.engine_row_lock_mean,
            self.prelock_wait_mean,
            self.row_lock_share(),
            self.engine_other_lock_mean,
            self.engine_lwlock_mean,
            self.engine_io_mean,
            self.engine_idle_in_txn_mean,
            self.engine_running_mean,
            self.engine_other_mean,
            self.generator_lock_mean,
            self.top_statements
                .iter()
                .map(|(class, statement, mean)| format!(
                    "{{\"class\":\"{}\",\"statement\":\"{}\",\"mean\":{mean:.3}}}",
                    class.label(),
                    statement.replace('\\', "\\\\").replace('"', "\\\""),
                ))
                .collect::<Vec<_>>()
                .join(","),
            StatementClass::ALL
                .iter()
                .zip(&self.lock_wait_classes)
                .map(|(class, w)| format!(
                    "\"{}\":{{\"wait_mean\":{:.3},{}}}",
                    class.label(),
                    w.wait_mean,
                    w.waits.json_fields("wait"),
                ))
                .collect::<Vec<_>>()
                .join(","),
            BuildClass::ALL
                .iter()
                .zip(&self.build_waits)
                .map(|(class, waits)| format!(
                    "\"{}\":{{{}}}",
                    class.label(),
                    waits
                        .iter()
                        .map(|(wait, mean)| format!("\"{wait}\":{mean:.3}"))
                        .collect::<Vec<_>>()
                        .join(",")
                ))
                .collect::<Vec<_>>()
                .join(","),
        )
    }

    /// The headline lock columns for a scenario's top level:
    /// `page_lock_hold_ms_{p50,p99,max}` and `page_lock_holds`,
    /// `page_lock_wait_ms_{p50,p99,max}` and `page_lock_waits` (see the
    /// module doc for what one episode is).
    pub fn lock_json_fields(&self) -> String {
        format!(
            "{},{}",
            self.page_lock_holds.json_fields("page_lock_hold"),
            self.page_lock_waits.json_fields("page_lock_wait"),
        )
    }
}

/// One sampled, non-idle backend. Times are the server's, in epoch seconds.
#[derive(Debug, Clone, Default)]
pub struct ActivityRow {
    pub pid: i32,
    pub state: String,
    pub wait_event_type: Option<String>,
    pub wait_event: Option<String>,
    pub query: String,
    /// The backend's `application_name` ([`build_class`]).
    pub application_name: String,
    /// When the sample was taken (`clock_timestamp()`).
    pub at: f64,
    pub xact_start: Option<f64>,
    pub query_start: Option<f64>,
    /// The earliest `pg_locks.waitstart` among the backend's ungranted locks.
    pub lock_wait_start: Option<f64>,
}

/// A statement's lock-wait episode in progress.
struct OpenWait {
    query_start: f64,
    class: StatementClass,
    /// Waits already over, in seconds.
    closed: f64,
    /// The lock wait in progress: `(waitstart, last sample still waiting)`.
    current: Option<(f64, f64)>,
    waited: bool,
}

impl OpenWait {
    fn close_current(&mut self) {
        if let Some((start, last)) = self.current.take() {
            self.closed += last - start;
        }
    }
}

/// A transaction's page lock hold in progress.
struct OpenXact {
    xact_start: f64,
    /// The earliest `query_start` of a [`StatementClass::locks_page_rows`]
    /// statement seen in it.
    locks_from: Option<f64>,
    last_seen: f64,
}

/// Folds snapshots into a [`ContentionSummary`] one at a time, so a long
/// window's memory stays flat.
pub struct Accumulator {
    generator_prefixes: Vec<String>,
    target: String,
    summary: ContentionSummary,
    by_statement: HashMap<(WaitClass, String), f64>,
    waits: HashMap<i32, OpenWait>,
    xacts: HashMap<i32, OpenXact>,
    class_waits: [DurationRecorder; 5],
    page_waits: DurationRecorder,
    page_holds: DurationRecorder,
    build_waits: [HashMap<String, f64>; 3],
}

impl Accumulator {
    /// `generator_prefixes` as for [`role`]; `target` as for
    /// [`statement_class`].
    pub fn new(generator_prefixes: Vec<String>, target: &str) -> Self {
        Accumulator {
            generator_prefixes,
            target: target.to_string(),
            summary: ContentionSummary::default(),
            by_statement: HashMap::new(),
            waits: HashMap::new(),
            xacts: HashMap::new(),
            class_waits: Default::default(),
            page_waits: DurationRecorder::default(),
            page_holds: DurationRecorder::default(),
            build_waits: Default::default(),
        }
    }

    fn finish_wait(&mut self, wait: OpenWait) {
        let mut wait = wait;
        wait.close_current();
        if wait.waited {
            self.class_waits[wait.class.index()].record(wait.closed);
            if wait.class.locks_page_rows() {
                self.page_waits.record(wait.closed);
            }
        }
    }

    fn finish_xact(&mut self, xact: OpenXact) {
        if let Some(from) = xact.locks_from {
            self.page_holds.record(xact.last_seen - from);
        }
    }

    /// Adds one snapshot: every non-idle client backend at one instant.
    pub fn add(&mut self, snapshot: &[ActivityRow]) {
        self.summary.samples += 1;
        let mut seen = Vec::with_capacity(snapshot.len());
        for row in snapshot {
            let class = classify(
                &row.state,
                row.wait_event_type.as_deref(),
                row.wait_event.as_deref(),
            );
            if role(&row.query, &self.generator_prefixes) == Role::Generator {
                if matches!(class, WaitClass::RowLock | WaitClass::OtherLock) {
                    self.summary.generator_lock_mean += 1.0;
                }
                continue;
            }
            seen.push(row.pid);
            let s = &mut self.summary;
            s.engine_busy_mean += 1.0;
            *self
                .by_statement
                .entry((class, statement_prefix(&row.query)))
                .or_default() += 1.0;
            let slot = match class {
                WaitClass::RowLock => {
                    if row.query.to_ascii_lowercase().contains(PRELOCK_MARKER) {
                        s.prelock_wait_mean += 1.0;
                    }
                    &mut s.engine_row_lock_mean
                }
                WaitClass::OtherLock => &mut s.engine_other_lock_mean,
                WaitClass::LwLock => &mut s.engine_lwlock_mean,
                WaitClass::Io => &mut s.engine_io_mean,
                WaitClass::IdleInTransaction => &mut s.engine_idle_in_txn_mean,
                WaitClass::Running => &mut s.engine_running_mean,
                WaitClass::Other => &mut s.engine_other_mean,
            };
            *slot += 1.0;

            // A Re-derive build's statements are reported on their own, and
            // kept out of the page classes: its entry lock reads like a
            // page's.
            let build = build_class(&row.application_name, &row.query);
            if let Some(build) = build {
                *self.build_waits[build.index()]
                    .entry(wait_label(
                        &row.state,
                        row.wait_event_type.as_deref(),
                        row.wait_event.as_deref(),
                    ))
                    .or_default() += 1.0;
            }
            let statement = match build {
                Some(_) => StatementClass::Other,
                None => statement_class(&row.query, &self.target),
            };
            let lock_waiting = row.wait_event_type.as_deref() == Some("Lock");
            if lock_waiting {
                s.lock_wait_classes[statement.index()].wait_mean += 1.0;
            }
            self.track_xact(row, statement);
            self.track_wait(row, statement, lock_waiting);
        }
        // A backend missing from this sample went idle: whatever it had open
        // ended before now.
        let gone_waits: Vec<i32> = self
            .waits
            .keys()
            .copied()
            .filter(|pid| !seen.contains(pid))
            .collect();
        for pid in gone_waits {
            let wait = self.waits.remove(&pid).expect("listed above");
            self.finish_wait(wait);
        }
        let gone_xacts: Vec<i32> = self
            .xacts
            .keys()
            .copied()
            .filter(|pid| !seen.contains(pid))
            .collect();
        for pid in gone_xacts {
            let xact = self.xacts.remove(&pid).expect("listed above");
            self.finish_xact(xact);
        }
    }

    fn track_xact(&mut self, row: &ActivityRow, statement: StatementClass) {
        let Some(xact_start) = row.xact_start else {
            if let Some(old) = self.xacts.remove(&row.pid) {
                self.finish_xact(old);
            }
            return;
        };
        if self
            .xacts
            .get(&row.pid)
            .is_some_and(|x| x.xact_start != xact_start)
        {
            let old = self.xacts.remove(&row.pid).expect("checked above");
            self.finish_xact(old);
        }
        let xact = self.xacts.entry(row.pid).or_insert(OpenXact {
            xact_start,
            locks_from: None,
            last_seen: row.at,
        });
        xact.last_seen = row.at;
        if statement.locks_page_rows() {
            let from = row.query_start.unwrap_or(row.at);
            xact.locks_from = Some(xact.locks_from.map_or(from, |f| f.min(from)));
        }
    }

    fn track_wait(&mut self, row: &ActivityRow, statement: StatementClass, lock_waiting: bool) {
        let Some(query_start) = row.query_start else {
            return;
        };
        if self
            .waits
            .get(&row.pid)
            .is_some_and(|w| w.query_start != query_start)
        {
            let old = self.waits.remove(&row.pid).expect("checked above");
            self.finish_wait(old);
        }
        let wait = self.waits.entry(row.pid).or_insert(OpenWait {
            query_start,
            class: statement,
            closed: 0.0,
            current: None,
            waited: false,
        });
        if lock_waiting {
            // `waitstart` can read null for an instant after a wait begins.
            let start = row.lock_wait_start.unwrap_or(row.at);
            match wait.current {
                Some((s, _)) if s == start => wait.current = Some((s, row.at)),
                _ => {
                    wait.close_current();
                    wait.current = Some((start, row.at));
                }
            }
            wait.waited = true;
        } else {
            wait.close_current();
        }
    }

    pub fn finish(mut self) -> ContentionSummary {
        for (_, wait) in std::mem::take(&mut self.waits) {
            self.finish_wait(wait);
        }
        for (_, xact) in std::mem::take(&mut self.xacts) {
            self.finish_xact(xact);
        }
        let mut summary = self.summary;
        for (class, recorder) in summary.lock_wait_classes.iter_mut().zip(&self.class_waits) {
            class.waits = recorder.stats();
        }
        summary.page_lock_waits = self.page_waits.stats();
        summary.page_lock_holds = self.page_holds.stats();
        if summary.samples == 0 {
            return summary;
        }
        let n = summary.samples as f64;
        for class in &mut summary.lock_wait_classes {
            class.wait_mean /= n;
        }
        for (out, waits) in summary.build_waits.iter_mut().zip(&self.build_waits) {
            let mut waits: Vec<(String, f64)> = waits
                .iter()
                .map(|(wait, count)| (wait.clone(), count / n))
                .collect();
            waits.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            *out = waits;
        }
        for mean in [
            &mut summary.engine_busy_mean,
            &mut summary.engine_row_lock_mean,
            &mut summary.prelock_wait_mean,
            &mut summary.engine_other_lock_mean,
            &mut summary.engine_lwlock_mean,
            &mut summary.engine_io_mean,
            &mut summary.engine_idle_in_txn_mean,
            &mut summary.engine_running_mean,
            &mut summary.engine_other_mean,
            &mut summary.generator_lock_mean,
        ] {
            *mean /= n;
        }
        let mut top: Vec<_> = self
            .by_statement
            .into_iter()
            .map(|((class, statement), count)| (class, statement, count / n))
            .collect();
        // Most-sampled first; ties broken by text so the order is deterministic.
        top.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.1.cmp(&b.1)));
        top.truncate(TOP_STATEMENTS);
        summary.top_statements = top;
        summary
    }
}

/// Folds sampled snapshots (each a list of non-idle backends) into a
/// [`ContentionSummary`] in one go.
#[cfg(test)]
pub fn summarize(
    snapshots: &[Vec<ActivityRow>],
    generator_prefixes: Vec<String>,
    target: &str,
) -> ContentionSummary {
    let mut acc = Accumulator::new(generator_prefixes, target);
    for snapshot in snapshots {
        acc.add(snapshot);
    }
    acc.finish()
}

/// One sample: every non-idle client backend in the database but the
/// sampler's own, with the earliest ungranted lock's `waitstart`.
const SAMPLE_SQL: &str = "\
    with w as ( \
        select pid, min(waitstart) as waitstart from pg_locks \
        where not granted group by pid \
    ) \
    select a.pid, a.state, a.wait_event_type, a.wait_event, a.query, \
           extract(epoch from clock_timestamp())::float8, \
           extract(epoch from a.xact_start)::float8, \
           extract(epoch from a.query_start)::float8, \
           extract(epoch from w.waitstart)::float8, a.application_name \
    from pg_stat_activity a left join w on w.pid = a.pid \
    where a.datname = current_database() and a.pid <> pg_backend_pid() \
      and a.backend_type = 'client backend' and a.state <> 'idle'";

/// Samples every non-idle client backend in `raw`'s database (other than
/// `raw`'s own) every [`SAMPLE_INTERVAL`] from `from` until `until`, and
/// summarizes them, telling the generators' statements against
/// `public.<source_table>` apart and classifying statements against
/// `target` (the target table's bare name).
pub async fn sample(
    raw: &RawClient,
    source_table: &str,
    target: &str,
    from: Instant,
    until: Instant,
) -> ContentionSummary {
    sample_while(raw, source_table, target, from, || Instant::now() < until).await
}

/// [`sample`], until `keep_going` returns false.
pub async fn sample_while(
    raw: &RawClient,
    source_table: &str,
    target: &str,
    from: Instant,
    keep_going: impl Fn() -> bool,
) -> ContentionSummary {
    tokio::time::sleep_until(from.into()).await;
    let mut acc = Accumulator::new(generator_prefixes(source_table), target);
    while keep_going() {
        let rows = raw
            .query(SAMPLE_SQL, &[])
            .await
            .expect("sample pg_stat_activity");
        let snapshot: Vec<ActivityRow> = rows
            .iter()
            .map(|r| ActivityRow {
                pid: r.get(0),
                state: r.get(1),
                wait_event_type: r.get(2),
                wait_event: r.get(3),
                query: r.get(4),
                at: r.get(5),
                xact_start: r.get(6),
                query_start: r.get(7),
                lock_wait_start: r.get(8),
                application_name: r.get::<_, Option<String>>(9).unwrap_or_default(),
            })
            .collect();
        acc.add(&snapshot);
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
    acc.finish()
}

/// The prefixes every generator statement against `public.<source_table>`
/// starts with, each through the space after the table name: `load`'s
/// `insert into`, and `build_under_load`'s writers' `update`/`delete from`.
pub fn generator_prefixes(source_table: &str) -> Vec<String> {
    ["insert into", "update", "delete from"]
        .iter()
        .map(|verb| format!("{verb} public.{source_table} "))
        .collect()
}

/// `pg_stat_database`'s deadlock and rollback counters for `raw`'s database.
/// A rollback on the drain path is an apply attempt thrown away (a fence
/// miss, a deadlock, a released claim), so their deltas across a window are
/// the "retry churn" half of the contention question.
pub async fn deadlocks_and_rollbacks(raw: &RawClient) -> (i64, i64) {
    let row = raw
        .query_one(
            "select deadlocks, xact_rollback from pg_stat_database \
             where datname = current_database()",
            &[],
        )
        .await
        .expect("read pg_stat_database deadlocks/xact_rollback");
    (row.get(0), row.get(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::load;

    fn row(pid: i32, state: &str, wait: Option<(&str, &str)>, query: &str) -> ActivityRow {
        ActivityRow {
            pid,
            state: state.to_string(),
            wait_event_type: wait.map(|(t, _)| t.to_string()),
            wait_event: wait.map(|(_, e)| e.to_string()),
            query: query.to_string(),
            ..Default::default()
        }
    }

    /// A row sampled at `at`, in transaction `xact` running a statement
    /// started at `query`, and (if `waiting_since` is set) blocked on a
    /// transaction lock since then.
    fn timed(
        pid: i32,
        sql: &str,
        at: f64,
        xact: f64,
        query: f64,
        waiting_since: Option<f64>,
    ) -> ActivityRow {
        ActivityRow {
            at,
            xact_start: Some(xact),
            query_start: Some(query),
            lock_wait_start: waiting_since,
            ..row(
                pid,
                "active",
                waiting_since.map(|_| ("Lock", "transactionid")),
                sql,
            )
        }
    }

    const PRELOCK: &str = "select 1 from \"public\".\"agg_totals\" t join unnest($1) k \
                           on t.grp = k.c0 order by t.grp for update of t";

    #[test]
    fn a_build_chunk_is_told_from_a_page_by_its_application_name() {
        let lock = "select 1 from \"public\".\"agg_totals__ledger\" where \"__from_key\" = \
                    any($1::text[]) order by \"__from_key\" for update";
        let chunk = trellis::dev::staging::CHUNK_APPLICATION_NAME;
        let merge = trellis::dev::staging::MERGE_APPLICATION_NAME;
        assert_eq!(build_class(chunk, lock), Some(BuildClass::ChunkLock));
        assert_eq!(
            build_class(
                chunk,
                "with snap as (select pg_current_snapshot() ...) select 1"
            ),
            Some(BuildClass::ChunkWrite)
        );
        assert_eq!(
            build_class(merge, "with claimed as ..."),
            Some(BuildClass::MergeUpsert)
        );
        assert_eq!(build_class("", lock), None, "a page's lock is a page's");

        let mut acc = Accumulator::new(Vec::new(), "agg_totals");
        acc.add(&[ActivityRow {
            application_name: chunk.to_string(),
            ..row(1, "active", Some(("Lock", "transactionid")), lock)
        }]);
        let summary = acc.finish();
        assert_eq!(
            summary.build_waits[BuildClass::ChunkLock.index()],
            vec![("Lock:transactionid".to_string(), 1.0)]
        );
        assert_eq!(
            summary.lock_wait_classes[StatementClass::LedgerLock.index()].wait_mean,
            0.0,
            "a chunk's lock wait isn't a page's"
        );
    }

    #[test]
    fn row_lock_waits_are_transactionid_and_tuple_only() {
        assert_eq!(
            classify("active", Some("Lock"), Some("transactionid")),
            WaitClass::RowLock
        );
        assert_eq!(
            classify("active", Some("Lock"), Some("tuple")),
            WaitClass::RowLock
        );
        assert_eq!(
            classify("active", Some("Lock"), Some("relation")),
            WaitClass::OtherLock
        );
        assert_eq!(
            classify("active", Some("LWLock"), Some("WALInsert")),
            WaitClass::LwLock
        );
        assert_eq!(
            classify("active", Some("IO"), Some("WalSync")),
            WaitClass::Io
        );
        assert_eq!(classify("active", None, None), WaitClass::Running);
        assert_eq!(
            classify("active", Some("Client"), Some("ClientRead")),
            WaitClass::Other
        );
        // Between statements the backend reports the *client* wait; what
        // matters is that the transaction (and its locks) is still open.
        assert_eq!(
            classify("idle in transaction", Some("Client"), Some("ClientRead")),
            WaitClass::IdleInTransaction
        );
    }

    #[test]
    fn the_generators_statements_are_not_engine_time() {
        let prefixes = generator_prefixes("agg_src");
        // The statements `load` actually sends, not a hand-copied string:
        // if the generator's SQL drifts, attribution breaks here first.
        for groups in [Some(400), None] {
            assert_eq!(
                role(&load::parallel_insert_sql("agg_src", groups), &prefixes),
                Role::Generator
            );
        }
        for writer in [
            "  INSERT INTO public.agg_src (id) values (1)",
            "update public.agg_src set amt = amt + $2 where id = $1",
            "delete from public.agg_src where id = $1",
        ] {
            assert_eq!(role(writer, &prefixes), Role::Generator, "{writer}");
        }
        assert_eq!(
            role("insert into public.agg_totals (grp) values (1)", &prefixes),
            Role::Engine
        );
        // Only the source table itself, not one whose name extends it.
        assert_eq!(
            role(
                "insert into public.agg_src_archive (id) values (1)",
                &prefixes
            ),
            Role::Engine
        );
    }

    #[test]
    fn statements_classify_by_their_text() {
        let class = |q| statement_class(q, "agg_totals");
        assert_eq!(class(PRELOCK), StatementClass::PreLock);
        assert_eq!(
            class(
                "with taken as ( select bucket from seg_claims where seg_seq = $1 ), \
                 mine as ( insert into seg_claims (seg_seq, bucket, claimed_by) select 1 ) \
                 select mine.bucket from mine"
            ),
            StatementClass::Claim
        );
        assert_eq!(
            class("select 1 from segments where seg_seq = $1 for no key update"),
            StatementClass::Claim
        );
        assert_eq!(
            class(
                "insert into \"public\".\"agg_totals\" (\"grp\", \"val\") values ($1, $2) \
                 on conflict (\"grp\") do update set \"val\" = excluded.\"val\" returning 1"
            ),
            StatementClass::GroupUpsert
        );
        assert_eq!(
            class("update \"public\".\"agg_totals\" set \"val\" = 1 from unnest($1) as k(c0)"),
            StatementClass::GroupUpsert
        );
        assert_eq!(
            class("select 1 from trellis.agg_totals__ledger l where l.k = $1 for update"),
            StatementClass::LedgerLock
        );
        // Reading the target is not a write to it.
        assert_eq!(
            class("select grp from \"public\".\"agg_totals\""),
            StatementClass::Other
        );
        assert_eq!(
            class("update segments set state = 'drained'"),
            StatementClass::Other
        );
        assert_eq!(
            statement_class("update agg_totals set val = 1", ""),
            StatementClass::Other
        );
    }

    #[test]
    fn summary_means_are_per_sample_and_split_the_prelock_out() {
        let snapshots = vec![
            vec![
                row(1, "active", Some(("Lock", "transactionid")), PRELOCK),
                row(2, "active", Some(("Lock", "tuple")), PRELOCK),
                row(3, "active", None, "insert into agg_totals ..."),
                row(
                    4,
                    "active",
                    Some(("Lock", "transactionid")),
                    "update drainers set ...",
                ),
                row(
                    5,
                    "active",
                    Some(("Lock", "transactionid")),
                    "insert into public.agg_src (id, grp, val) select 1",
                ),
            ],
            vec![row(1, "idle in transaction", None, PRELOCK)],
        ];
        let s = summarize(&snapshots, generator_prefixes("agg_src"), "agg_totals");
        assert_eq!(s.samples, 2);
        assert_eq!(s.engine_busy_mean, 2.5);
        assert_eq!(s.engine_row_lock_mean, 1.5);
        assert_eq!(s.prelock_wait_mean, 1.0);
        assert_eq!(s.engine_running_mean, 0.5);
        assert_eq!(s.engine_idle_in_txn_mean, 0.5);
        assert_eq!(s.generator_lock_mean, 0.5);
        assert_eq!(s.row_lock_share(), 0.6);
        let mean = |c: StatementClass| s.lock_wait_classes[c.index()].wait_mean;
        assert_eq!(mean(StatementClass::PreLock), 1.0);
        assert_eq!(mean(StatementClass::Other), 0.5);
        assert_eq!(mean(StatementClass::GroupUpsert), 0.0);
        // The pre-lock was sampled waiting twice in the first snapshot (two
        // backends) and idle-in-transaction once in the second.
        assert_eq!(s.top_statements[0].0, WaitClass::RowLock);
        assert!(
            s.top_statements[0]
                .1
                .starts_with("select 1 from \"public\".\"agg_totals\" t")
        );
        assert_eq!(s.top_statements[0].2, 1.0);
        let json = s.to_json();
        assert!(json.contains("\"class\":\"row_lock\""));
        assert!(
            json.contains("\"lock_wait_classes\":{\"claim\":{\"wait_mean\":0.000,"),
            "{json}"
        );
    }

    /// One page transaction (pid 1, from t=100.0): its pre-lock starts at
    /// 100.2 and waits from 100.25 for two samples; after it, an upsert waits
    /// on a second lock from 100.4; the transaction is last seen at 100.6.
    /// A claim (pid 2) waits from 100.1 to at least 100.2.
    #[test]
    fn lock_waits_and_holds_are_per_episode() {
        let upsert =
            "insert into \"public\".\"agg_totals\" (grp) values ($1) on conflict do nothing";
        let claim = "with mine as (insert into seg_claims (seg_seq) select 1) select 1";
        let snapshots = vec![
            vec![
                timed(1, "fold ...", 100.1, 100.0, 100.05, None),
                timed(2, claim, 100.15, 100.1, 100.1, Some(100.1)),
            ],
            vec![
                timed(1, PRELOCK, 100.3, 100.0, 100.2, Some(100.25)),
                timed(2, claim, 100.2, 100.1, 100.1, Some(100.1)),
            ],
            vec![timed(1, PRELOCK, 100.35, 100.0, 100.2, Some(100.25))],
            vec![timed(1, upsert, 100.45, 100.0, 100.4, Some(100.4))],
            vec![ActivityRow {
                state: "idle in transaction".to_string(),
                ..timed(1, upsert, 100.6, 100.0, 100.4, None)
            }],
            // pid 1 committed and went idle.
            vec![],
        ];
        let s = summarize(&snapshots, generator_prefixes("agg_src"), "agg_totals");
        let waits = |c: StatementClass| s.lock_wait_classes[c.index()].waits;
        let close = |v: Option<f64>, ms: f64| (v.expect("observed") - ms).abs() < 0.02 * ms;

        assert_eq!(waits(StatementClass::PreLock).count, 1);
        assert!(close(waits(StatementClass::PreLock).max_ms, 100.0));
        assert_eq!(waits(StatementClass::GroupUpsert).count, 1);
        assert!(close(waits(StatementClass::GroupUpsert).max_ms, 50.0));
        assert_eq!(waits(StatementClass::Claim).count, 1);
        assert!(close(waits(StatementClass::Claim).max_ms, 100.0));
        // The fold statement never waited: no episode.
        assert_eq!(waits(StatementClass::Other).count, 0);

        assert_eq!(
            s.page_lock_waits.count, 2,
            "pre-lock + upsert, not the claim"
        );
        assert!(close(s.page_lock_waits.max_ms, 100.0));
        // One page transaction, holding target rows from the pre-lock's
        // start (100.2) to its last sighting (100.6).
        assert_eq!(s.page_lock_holds.count, 1);
        assert!(close(s.page_lock_holds.max_ms, 400.0));
        let fields = s.lock_json_fields();
        assert!(fields.contains("\"page_lock_holds\":1"), "{fields}");
        assert!(fields.contains("\"page_lock_wait_ms_p99\":"), "{fields}");
    }

    #[test]
    fn quantiles_never_exceed_the_max() {
        let mut r = DurationRecorder::default();
        for secs in [0.000_875, 0.010_922] {
            r.record(secs);
        }
        let s = r.stats();
        let max = s.max_ms.unwrap();
        assert!((max - 10.922).abs() < 1e-9, "{max}");
        assert!(s.p99_ms.unwrap() <= max, "{s:?}");
        assert!(s.p50_ms.unwrap() <= max, "{s:?}");
    }

    #[test]
    fn a_statement_that_waits_twice_sums_its_waits() {
        let snapshots = vec![
            vec![timed(1, PRELOCK, 10.1, 10.0, 10.0, Some(10.05))],
            vec![timed(1, PRELOCK, 10.2, 10.0, 10.0, None)],
            vec![timed(1, PRELOCK, 10.3, 10.0, 10.0, Some(10.25))],
            vec![],
        ];
        let s = summarize(&snapshots, generator_prefixes("agg_src"), "agg_totals");
        let w = s.lock_wait_classes[StatementClass::PreLock.index()].waits;
        assert_eq!(w.count, 1);
        let ms = w.max_ms.unwrap();
        assert!((ms - 100.0).abs() < 1.0, "50 ms + 50 ms, got {ms}");
    }

    #[test]
    fn statement_prefixes_collapse_whitespace_and_truncate() {
        assert_eq!(statement_prefix("select  1\n  from\tx"), "select 1 from x");
        assert_eq!(
            statement_prefix(&"a".repeat(200)).len(),
            STATEMENT_PREFIX_CHARS
        );
    }

    #[test]
    fn an_empty_window_summarizes_to_zeroes() {
        let s = summarize(&[], generator_prefixes("agg_src"), "agg_totals");
        assert_eq!(s, ContentionSummary::default());
        assert_eq!(s.row_lock_share(), 0.0);
        assert!(
            s.lock_json_fields()
                .contains("\"page_lock_hold_ms_p50\":null")
        );
    }
}
