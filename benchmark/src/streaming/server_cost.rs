//! What a measured window cost the Postgres side, beyond storage
//! ([`disk_tier`](super::disk_tier)): server CPU, error lines, and the size of
//! any ledger tables (#623 D1). Everything here is read from the cluster or
//! its processes, not from the engine, so the columns mean the same thing
//! before and after the engine's apply path is rewritten.
//!
//! - **Server CPU** ([`cluster_cpu_ticks`]): user + system CPU of the whole
//!   cluster's process tree, read from `/proc`: the postmaster, every live
//!   child (backends, autovacuum, checkpointer, ...), and, through
//!   the postmaster's `cutime`/`cstime`, every child it reaped in the window.
//!   That includes the load generator's own backends. They cost the same
//!   per row before and after a change to the drain, so a before/after delta
//!   across one is the drain's. Not across a change to capture: trigger
//!   capture (#622 C5 on) runs inside the generator's backends, and D8's
//!   NEW-only images change what it costs there. Resolution is one clock
//!   tick (10 ms) per process.
//! - **`deadlock detected` lines** in the cluster's `postgres.log` written
//!   during the window ([`PgLogCursor`]). `pg_stat_database.deadlocks`
//!   (the scenarios' `deadlocks` key) counts the same events; this is the
//!   log-line count ADR-0002's acceptance criteria are stated in.
//! - **Engine lock-timeout warnings** ([`scrape::lock_timeout_warnings`]): the
//!   drain's `drain page waited out its lock_timeout` line, counted in-process.
//! - **Ledger bytes** ([`ledger_bytes`]): `pg_total_relation_size` summed over
//!   every table named `%__ledger`, read at the window's end. `0` until the
//!   engine creates ledger tables (#623 D2).
//! - **Ledger updates** ([`ledger_updates`]): `pg_stat_user_tables`'
//!   `n_tup_upd` and `n_tup_hot_upd` summed over the same tables, read at the
//!   window's end (#775). They count from the ledger's creation, not from the
//!   window's start, and miss what a backend has not yet flushed to the
//!   cumulative statistics (up to a second or so), so only their ratio is
//!   meaningful: the share of ledger updates that were HOT.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use super::scrape;

/// Linux reports `/proc/<pid>/stat` times in `USER_HZ` ticks, which is 100 on
/// every architecture Linux exposes to user space.
const USER_HZ: f64 = 100.0;

/// The line Postgres logs for every deadlock it breaks.
pub const DEADLOCK_DETECTED: &str = "deadlock detected";

/// `(ppid, utime + stime + cutime + cstime)` from one `/proc/<pid>/stat`.
/// The command name (field 2) may contain spaces and parentheses, so fields
/// are counted from the last `)`.
fn parse_stat(stat: &str) -> Option<(u32, u64)> {
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After the `)`: state, ppid, ... utime is field 14 overall, so index 11.
    let ppid = fields.get(1)?.parse().ok()?;
    let ticks = fields
        .get(11..15)?
        .iter()
        .map(|f| f.parse::<u64>().ok())
        .sum::<Option<u64>>()?;
    Some((ppid, ticks))
}

/// CPU ticks the cluster rooted at `postmaster` has used so far: its own,
/// its reaped children's, and every live child's (see the module doc). A
/// child reaped between the two reads below is missed, which is at most one
/// backend's CPU per window.
pub fn cluster_cpu_ticks(postmaster: u32) -> u64 {
    let read = |pid: &str| std::fs::read_to_string(format!("/proc/{pid}/stat")).ok();
    let own = read(&postmaster.to_string())
        .and_then(|s| parse_stat(&s))
        .map_or(0, |(_, ticks)| ticks);
    let children: u64 = std::fs::read_dir("/proc")
        .map(|dir| {
            dir.filter_map(Result::ok)
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|name| name.bytes().all(|b| b.is_ascii_digit()))
                .filter_map(|pid| read(&pid).and_then(|s| parse_stat(&s)))
                .filter(|(ppid, _)| *ppid == postmaster)
                .map(|(_, ticks)| ticks)
                .sum()
        })
        .unwrap_or(0);
    own + children
}

/// A byte offset into the cluster's `postgres.log`, to count the lines
/// written after it.
pub struct PgLogCursor {
    path: PathBuf,
    offset: u64,
}

impl PgLogCursor {
    pub fn at_end(path: &Path) -> Self {
        let offset = std::fs::metadata(path).map_or(0, |m| m.len());
        Self {
            path: path.to_path_buf(),
            offset,
        }
    }

    /// Lines containing `needle` written since this cursor was taken.
    pub fn count_since(&self, needle: &str) -> u64 {
        let mut bytes = Vec::new();
        if let Ok(mut file) = std::fs::File::open(&self.path)
            && file.seek(SeekFrom::Start(self.offset)).is_ok()
        {
            let _ = file.read_to_end(&mut bytes);
        }
        String::from_utf8_lossy(&bytes)
            .lines()
            .filter(|l| l.contains(needle))
            .count() as u64
    }
}

/// `pg_total_relation_size` over every table whose name ends in `__ledger`,
/// in any schema.
pub async fn ledger_bytes(raw: &RawClient) -> i64 {
    raw.query_one(
        "select coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint from pg_class c \
         where c.relkind in ('r', 'p') and c.relname like '%\\_\\_ledger'",
        &[],
    )
    .await
    .expect("sum ledger table sizes")
    .get(0)
}

/// `n_tup_upd` and `n_tup_hot_upd` summed over the tables [`ledger_bytes`]
/// sizes.
pub async fn ledger_updates(raw: &RawClient) -> (i64, i64) {
    let row = raw
        .query_one(
            "select coalesce(sum(n_tup_upd), 0)::bigint, coalesce(sum(n_tup_hot_upd), 0)::bigint \
             from pg_stat_user_tables where relname like '%\\_\\_ledger'",
            &[],
        )
        .await
        .expect("sum ledger update counts");
    (row.get(0), row.get(1))
}

/// The start of a [`ServerCost`] window.
pub struct ServerCostStart {
    at: Instant,
    postmaster: u32,
    cpu_ticks: u64,
    log: PgLogCursor,
    lock_timeouts: u64,
}

/// Opens a window on `cluster`.
pub fn start(cluster: &TestCluster) -> ServerCostStart {
    let postmaster = cluster.server_pid();
    ServerCostStart {
        at: Instant::now(),
        postmaster,
        cpu_ticks: cluster_cpu_ticks(postmaster),
        log: PgLogCursor::at_end(&cluster.root().join("postgres.log")),
        lock_timeouts: scrape::lock_timeout_warnings(),
    }
}

/// A window's server-side costs; see the module doc for each.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ServerCost {
    pub window_secs: f64,
    pub pg_cpu_secs: f64,
    pub deadlock_detected_lines: u64,
    pub lock_timeout_warnings: u64,
    pub ledger_bytes: i64,
    pub ledger_tup_upd: i64,
    pub ledger_tup_hot_upd: i64,
}

impl ServerCostStart {
    /// Closes the window now. `raw` reads the ledger sizes.
    pub async fn finish(self, raw: &RawClient) -> ServerCost {
        let ticks = cluster_cpu_ticks(self.postmaster).saturating_sub(self.cpu_ticks);
        let (ledger_tup_upd, ledger_tup_hot_upd) = ledger_updates(raw).await;
        ServerCost {
            window_secs: self.at.elapsed().as_secs_f64(),
            pg_cpu_secs: ticks as f64 / USER_HZ,
            deadlock_detected_lines: self.log.count_since(DEADLOCK_DETECTED),
            lock_timeout_warnings: scrape::lock_timeout_warnings()
                .saturating_sub(self.lock_timeouts),
            ledger_bytes: ledger_bytes(raw).await,
            ledger_tup_upd,
            ledger_tup_hot_upd,
        }
    }
}

impl ServerCost {
    /// The JSON fields, comma-separated with no surrounding comma.
    /// `folded_rows` is the source rows (or changes) the engine folded in the
    /// window, the denominator for CPU; `source_rows` the source table's
    /// size, the denominator for ledger bytes.
    ///
    /// - `pg_cpu_secs`: cluster CPU seconds over the window;
    /// - `pg_cpu_cores`: the same over the window's wall time;
    /// - `pg_cpu_us_per_folded_row`: CPU microseconds per folded row;
    /// - `deadlock_detected_log_lines`, `lock_timeout_warnings`: counts;
    /// - `ledger_bytes`, `ledger_bytes_per_source_row`;
    /// - `ledger_tup_upd`, `ledger_tup_hot_upd` and `ledger_hot_update_ratio`,
    ///   their quotient (0 with no updates).
    pub fn json_fields(&self, folded_rows: u64, source_rows: u64) -> String {
        let per = |n: f64, d: u64| if d == 0 { 0.0 } else { n / d as f64 };
        format!(
            "\"pg_cpu_secs\":{:.3},\"pg_cpu_cores\":{:.3},\"pg_cpu_us_per_folded_row\":{:.3},\
             \"deadlock_detected_log_lines\":{},\"lock_timeout_warnings\":{},\
             \"ledger_bytes\":{},\"ledger_bytes_per_source_row\":{:.3},\
             \"ledger_tup_upd\":{},\"ledger_tup_hot_upd\":{},\"ledger_hot_update_ratio\":{:.3}",
            self.pg_cpu_secs,
            if self.window_secs > 0.0 {
                self.pg_cpu_secs / self.window_secs
            } else {
                0.0
            },
            per(self.pg_cpu_secs * 1e6, folded_rows),
            self.deadlock_detected_lines,
            self.lock_timeout_warnings,
            self.ledger_bytes,
            per(self.ledger_bytes as f64, source_rows),
            self.ledger_tup_upd,
            self.ledger_tup_hot_upd,
            per(
                self.ledger_tup_hot_upd as f64,
                self.ledger_tup_upd.max(0) as u64
            ),
        )
    }

    pub fn human(&self, folded_rows: u64) -> String {
        format!(
            "postgres CPU {:.1}s ({:.1} us/folded row), {} deadlock lines, {} lock-timeout \
             warnings, ledger {} bytes",
            self.pg_cpu_secs,
            if folded_rows == 0 {
                0.0
            } else {
                self.pg_cpu_secs * 1e6 / folded_rows as f64
            },
            self.deadlock_detected_lines,
            self.lock_timeout_warnings,
            self.ledger_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_fields_count_from_the_last_paren() {
        // A backend's command name carries spaces and parentheses.
        let stat = "4242 (postgres: mike db [local] idle (x)) S 4200 4200 4200 0 -1 \
                    4194304 100 0 0 0 150 25 7 3 20 0 1 0 12345 0 0";
        assert_eq!(parse_stat(stat), Some((4200, 150 + 25 + 7 + 3)));
        assert_eq!(parse_stat("garbage"), None);
    }

    #[test]
    fn this_process_reads_its_own_cpu() {
        let me = std::process::id();
        let stat = std::fs::read_to_string(format!("/proc/{me}/stat")).unwrap();
        assert!(parse_stat(&stat).is_some());
    }

    #[test]
    fn log_cursor_counts_only_lines_after_it() {
        let dir = std::env::temp_dir().join(format!("server-cost-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("postgres.log");
        std::fs::write(&path, "ERROR:  deadlock detected\n").unwrap();
        let cursor = PgLogCursor::at_end(&path);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(
            &mut f,
            b"LOG:  checkpoint\nERROR:  deadlock detected\nDETAIL: ...\nERROR:  deadlock detected\n",
        )
        .unwrap();
        assert_eq!(cursor.count_since(DEADLOCK_DETECTED), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn json_fields_divide_by_their_own_denominators() {
        let cost = ServerCost {
            window_secs: 2.0,
            pg_cpu_secs: 3.0,
            deadlock_detected_lines: 1,
            lock_timeout_warnings: 2,
            ledger_bytes: 8192,
            ledger_tup_upd: 400,
            ledger_tup_hot_upd: 100,
        };
        let json = cost.json_fields(1_000_000, 4096);
        assert!(json.contains("\"pg_cpu_cores\":1.500"), "{json}");
        assert!(
            json.contains("\"pg_cpu_us_per_folded_row\":3.000"),
            "{json}"
        );
        assert!(
            json.contains("\"ledger_bytes_per_source_row\":2.000"),
            "{json}"
        );
        assert!(json.contains("\"ledger_hot_update_ratio\":0.250"), "{json}");
        assert!(
            ServerCost::default()
                .json_fields(0, 0)
                .contains("\"pg_cpu_us_per_folded_row\":0.000")
        );
    }
}
