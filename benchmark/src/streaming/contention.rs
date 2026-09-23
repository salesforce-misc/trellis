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
//!   database — drain workers, intake's staging writes, the maintenance tick
//!   — is the engine. Intake's replication stream is a `walsender`, not a
//!   client backend, so the server-side WAL decoding it drives isn't sampled.
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

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

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

/// Which side a backend running `query` is on. `generator_insert_prefix` is
/// the start of the generator's own statement against the scenario's source
/// table, through the space after the table name (`insert into
/// public.<source> `, see [`generator_insert_prefix`]) so a table whose name
/// merely starts with `<source>` isn't mistaken for it.
pub fn role(query: &str, generator_insert_prefix: &str) -> Role {
    if query
        .trim_start()
        .to_ascii_lowercase()
        .starts_with(generator_insert_prefix)
    {
        Role::Generator
    } else {
        Role::Engine
    }
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
             \"generator_lock_mean\":{:.3},\"top_statements\":[{}]}}",
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
        )
    }
}

/// One sampled backend: `(state, wait_event_type, wait_event, query)`.
pub type ActivityRow = (String, Option<String>, Option<String>, String);

/// Folds sampled snapshots (each a list of non-idle backends) into a
/// [`ContentionSummary`].
pub fn summarize(
    snapshots: &[Vec<ActivityRow>],
    generator_insert_prefix: &str,
) -> ContentionSummary {
    let mut summary = ContentionSummary {
        samples: snapshots.len() as u64,
        ..Default::default()
    };
    if snapshots.is_empty() {
        return summary;
    }
    let mut by_statement: HashMap<(WaitClass, String), f64> = HashMap::new();
    for snapshot in snapshots {
        for (state, wait_type, wait_event, query) in snapshot {
            let class = classify(state, wait_type.as_deref(), wait_event.as_deref());
            match role(query, generator_insert_prefix) {
                Role::Generator => {
                    if matches!(class, WaitClass::RowLock | WaitClass::OtherLock) {
                        summary.generator_lock_mean += 1.0;
                    }
                }
                Role::Engine => {
                    summary.engine_busy_mean += 1.0;
                    *by_statement
                        .entry((class, statement_prefix(query)))
                        .or_default() += 1.0;
                    let slot = match class {
                        WaitClass::RowLock => {
                            if query.to_ascii_lowercase().contains(PRELOCK_MARKER) {
                                summary.prelock_wait_mean += 1.0;
                            }
                            &mut summary.engine_row_lock_mean
                        }
                        WaitClass::OtherLock => &mut summary.engine_other_lock_mean,
                        WaitClass::LwLock => &mut summary.engine_lwlock_mean,
                        WaitClass::Io => &mut summary.engine_io_mean,
                        WaitClass::IdleInTransaction => &mut summary.engine_idle_in_txn_mean,
                        WaitClass::Running => &mut summary.engine_running_mean,
                        WaitClass::Other => &mut summary.engine_other_mean,
                    };
                    *slot += 1.0;
                }
            }
        }
    }
    let n = snapshots.len() as f64;
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
    let mut top: Vec<_> = by_statement
        .into_iter()
        .map(|((class, statement), count)| (class, statement, count / n))
        .collect();
    // Most-sampled first; ties broken by text so the order is deterministic.
    top.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.1.cmp(&b.1)));
    top.truncate(TOP_STATEMENTS);
    summary.top_statements = top;
    summary
}

/// Samples every non-idle client backend in `raw`'s database (other than
/// `raw`'s own) every [`SAMPLE_INTERVAL`] from `from` until `until`, and
/// summarizes them against the generator's `insert into public.<source_table>`.
pub async fn sample(
    raw: &RawClient,
    source_table: &str,
    from: Instant,
    until: Instant,
) -> ContentionSummary {
    tokio::time::sleep_until(from.into()).await;
    let mut snapshots = Vec::new();
    while Instant::now() < until {
        let rows = raw
            .query(
                "select state, wait_event_type, wait_event, query from pg_stat_activity \
                 where datname = current_database() and pid <> pg_backend_pid() \
                   and backend_type = 'client backend' and state <> 'idle'",
                &[],
            )
            .await
            .expect("sample pg_stat_activity");
        snapshots.push(
            rows.iter()
                .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                .collect(),
        );
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
    summarize(&snapshots, &generator_insert_prefix(source_table))
}

/// The prefix every `load` generator statement against
/// `public.<source_table>` starts with: `insert into public.<source_table>`
/// and the space before its column list.
pub fn generator_insert_prefix(source_table: &str) -> String {
    format!("insert into public.{source_table} ")
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

    fn row(state: &str, wait: Option<(&str, &str)>, query: &str) -> ActivityRow {
        (
            state.to_string(),
            wait.map(|(t, _)| t.to_string()),
            wait.map(|(_, e)| e.to_string()),
            query.to_string(),
        )
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
    fn the_generators_inserts_are_not_engine_time() {
        let prefix = generator_insert_prefix("agg_src");
        // The statements `load` actually sends, not a hand-copied string:
        // if the generator's SQL drifts, attribution breaks here first.
        for groups in [Some(400), None] {
            assert_eq!(
                role(&load::parallel_insert_sql("agg_src", groups), &prefix),
                Role::Generator
            );
        }
        assert_eq!(
            role("  INSERT INTO public.agg_src (id) values (1)", &prefix),
            Role::Generator
        );
        assert_eq!(
            role("insert into public.agg_totals (grp) values (1)", &prefix),
            Role::Engine
        );
        // Only the source table itself, not one whose name extends it.
        assert_eq!(
            role(
                "insert into public.agg_src_archive (id) values (1)",
                &prefix
            ),
            Role::Engine
        );
    }

    #[test]
    fn summary_means_are_per_sample_and_split_the_prelock_out() {
        let prelock = "select 1 from \"public\".\"agg_totals\" t join unnest($1) k on t.grp = k.c0 order by t.grp for update of t";
        let snapshots = vec![
            vec![
                row("active", Some(("Lock", "transactionid")), prelock),
                row("active", Some(("Lock", "tuple")), prelock),
                row("active", None, "insert into agg_totals ..."),
                row(
                    "active",
                    Some(("Lock", "transactionid")),
                    "update drainers set ...",
                ),
                row(
                    "active",
                    Some(("Lock", "transactionid")),
                    "insert into public.agg_src (id, grp, val) select 1",
                ),
            ],
            vec![row("idle in transaction", None, prelock)],
        ];
        let s = summarize(&snapshots, &generator_insert_prefix("agg_src"));
        assert_eq!(s.samples, 2);
        assert_eq!(s.engine_busy_mean, 2.5);
        assert_eq!(s.engine_row_lock_mean, 1.5);
        assert_eq!(s.prelock_wait_mean, 1.0);
        assert_eq!(s.engine_running_mean, 0.5);
        assert_eq!(s.engine_idle_in_txn_mean, 0.5);
        assert_eq!(s.generator_lock_mean, 0.5);
        assert_eq!(s.row_lock_share(), 0.6);
        // The pre-lock was sampled waiting twice in the first snapshot (two
        // backends) and idle-in-transaction once in the second.
        assert_eq!(s.top_statements[0].0, WaitClass::RowLock);
        assert!(
            s.top_statements[0]
                .1
                .starts_with("select 1 from \"public\".\"agg_totals\" t")
        );
        assert_eq!(s.top_statements[0].2, 1.0);
        assert!(s.to_json().contains("\"class\":\"row_lock\""));
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
        let s = summarize(&[], &generator_insert_prefix("agg_src"));
        assert_eq!(s, ContentionSummary::default());
        assert_eq!(s.row_lock_share(), 0.0);
    }
}
