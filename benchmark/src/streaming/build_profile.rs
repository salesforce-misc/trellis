//! The Re-derive build's profile columns for `build-under-load` (#625 F2,
//! the plan's §4 "The profile").
//!
//! Most come from the engine's own metrics (`trellis::metrics`), diffed
//! across the window like every other scrape here ([`super::scrape`]):
//!
//! - worker-seconds per build statement class
//!   (`trellis_build_statement_seconds{class}`'s `_sum`): the chunk's key
//!   read, entry lock, read-and-write (its one statement, which also inserts
//!   the deltas) and commit, and the merger's upsert, finish and commit, plus
//!   the plan job. Per 10k built rows, they are the profile's cost split;
//! - chunk transactions (`trellis_build_chunk_seconds`): how many, and p50
//!   and p99 read off the histogram's buckets (about 25% apart, so each is
//!   the upper bound of the bucket it falls in), and the max
//!   (`trellis_build_chunk_seconds_max`, the process's, which is this run's:
//!   the bench runs one scenario per process);
//! - rows built, delta rows appended and merged, chunks that gave up on
//!   their entry lock, and seals refused for a full ring.
//!
//! The rest are sampled on their own connection every [`PEAK_POLL`] from the
//! definition to the end of the window: the `pg_wal` directory's size
//! (`pg_ls_waldir()`), and the target's `__deltas` table, its rows and its
//! size.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio_postgres::Client as RawClient;
use trellis::metrics::BuildStatement;

use crate::streaming::disk_tier::json_ms;

/// How often [`sample_peaks`] reads the WAL directory and the delta table.
pub const PEAK_POLL: Duration = Duration::from_millis(250);

/// Every series of one scrape, by its full name and labels.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot(HashMap<String, f64>);

impl MetricsSnapshot {
    /// Parses one Prometheus text exposition.
    pub fn parse(rendered: &str) -> Self {
        Self(
            rendered
                .lines()
                .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
                .filter_map(|line| {
                    let (series, value) = line.rsplit_once(' ')?;
                    Some((series.to_string(), value.parse().ok()?))
                })
                .collect(),
        )
    }

    /// One scrape of this process's registry.
    pub fn take() -> Self {
        Self::parse(&super::scrape::scrape())
    }

    /// `series` (`name{labels}` exactly as rendered, or a bare name), or 0.
    pub fn get(&self, series: &str) -> f64 {
        self.0.get(series).copied().unwrap_or(0.0)
    }

    /// `series`' growth since `before`.
    fn since(&self, before: &MetricsSnapshot, series: &str) -> f64 {
        (self.get(series) - before.get(series)).max(0.0)
    }

    /// The `(le, cumulative count)` buckets of histogram `name` with no
    /// other label, sorted by `le`.
    fn buckets(&self, name: &str) -> Vec<(f64, f64)> {
        let prefix = format!("{name}_bucket{{le=\"");
        let mut buckets: Vec<(f64, f64)> = self
            .0
            .iter()
            .filter_map(|(series, value)| {
                let le = series.strip_prefix(&prefix)?.strip_suffix("\"}")?;
                let le = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().ok()?
                };
                Some((le, *value))
            })
            .collect();
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        buckets
    }
}

/// The smallest bucket bound under which at least `q` of the observations
/// fall, in milliseconds, from cumulative `buckets` diffed over the window
/// (`None` when nothing was observed, or it fell past the last finite bound).
fn bucket_quantile_ms(buckets: &[(f64, f64)], count: f64, q: f64) -> Option<f64> {
    if count <= 0.0 {
        return None;
    }
    buckets
        .iter()
        .find(|(_, cumulative)| *cumulative >= q * count)
        .map(|(le, _)| le * 1000.0)
        .filter(|ms| ms.is_finite())
}

/// What [`sample_peaks`] saw.
#[derive(Debug, Clone, Copy, Default)]
pub struct Peaks {
    pub wal_dir_bytes: i64,
    pub deltas_rows: i64,
    pub deltas_bytes: i64,
}

/// Samples the `pg_wal` directory and `public.<target>__deltas` every
/// [`PEAK_POLL`] until `stop`, returning the peaks. The delta table counts
/// as empty before the definition creates it.
pub async fn sample_peaks(raw: RawClient, target: String, stop: Arc<AtomicBool>) -> Peaks {
    let deltas = format!("public.{target}__deltas");
    let mut peaks = Peaks::default();
    while !stop.load(Ordering::Relaxed) {
        let wal: i64 = raw
            .query_one(
                "select coalesce(sum(size), 0)::bigint from pg_ls_waldir()",
                &[],
            )
            .await
            .expect("read the WAL directory's size")
            .get(0);
        peaks.wal_dir_bytes = peaks.wal_dir_bytes.max(wal);
        let exists: bool = raw
            .query_one("select to_regclass($1) is not null", &[&deltas])
            .await
            .expect("look up the delta table")
            .get(0);
        if exists {
            // A drop between the two reads is the run's own business; a
            // failed read here only skips a sample.
            if let Ok(row) = raw
                .query_one(
                    &format!("select count(*), pg_total_relation_size('{deltas}') from {deltas}"),
                    &[],
                )
                .await
            {
                peaks.deltas_rows = peaks.deltas_rows.max(row.get(0));
                peaks.deltas_bytes = peaks.deltas_bytes.max(row.get(1));
            }
        }
        tokio::time::sleep(PEAK_POLL).await;
    }
    peaks
}

/// The profile's engine-side columns over one window.
#[derive(Debug, Clone, Default)]
pub struct BuildProfile {
    /// Per [`BuildStatement::ALL`]: worker-seconds, and how many statements.
    pub statements: Vec<(&'static str, f64, u64)>,
    pub rows_built: u64,
    pub delta_rows_appended: u64,
    pub delta_rows_merged: u64,
    pub chunk_lock_timeouts: u64,
    pub seal_refusals: u64,
    pub chunks: u64,
    pub chunk_p50_ms: Option<f64>,
    pub chunk_p99_ms: Option<f64>,
    pub chunk_max_ms: Option<f64>,
    pub peaks: Peaks,
}

impl BuildProfile {
    /// The window between two scrapes, with the sampled peaks.
    pub fn between(before: &MetricsSnapshot, after: &MetricsSnapshot, peaks: Peaks) -> Self {
        let statements = BuildStatement::ALL
            .iter()
            .map(|class| {
                let label = class.label();
                let series = |suffix: &str| {
                    format!("trellis_build_statement_seconds_{suffix}{{class=\"{label}\"}}")
                };
                (
                    label,
                    after.since(before, &series("sum")),
                    after.since(before, &series("count")) as u64,
                )
            })
            .collect();
        let chunks = after.since(before, "trellis_build_chunk_seconds_count");
        let before_buckets: HashMap<u64, f64> = before
            .buckets("trellis_build_chunk_seconds")
            .into_iter()
            .map(|(le, n)| (le.to_bits(), n))
            .collect();
        let buckets: Vec<(f64, f64)> = after
            .buckets("trellis_build_chunk_seconds")
            .into_iter()
            .map(|(le, n)| {
                (
                    le,
                    n - before_buckets.get(&le.to_bits()).copied().unwrap_or(0.0),
                )
            })
            .collect();
        let max = after.get("trellis_build_chunk_seconds_max");
        let max_ms = (chunks > 0.0).then_some(max * 1000.0);
        // A quantile is its bucket's upper bound, so clamp it to the exact
        // max: a p99 above the max it summarizes reads as a bug.
        let quantile = |q| {
            bucket_quantile_ms(&buckets, chunks, q).map(|ms| max_ms.map_or(ms, |max| ms.min(max)))
        };
        Self {
            statements,
            rows_built: after.since(before, "trellis_build_rows_total") as u64,
            delta_rows_appended: after
                .since(before, "trellis_build_delta_rows_total{step=\"appended\"}")
                as u64,
            delta_rows_merged: after
                .since(before, "trellis_build_delta_rows_total{step=\"merged\"}")
                as u64,
            chunk_lock_timeouts: after.since(before, "trellis_build_chunk_lock_timeouts_total")
                as u64,
            seal_refusals: after.since(before, "trellis_seal_refused_total") as u64,
            chunks: chunks as u64,
            chunk_p50_ms: quantile(0.5),
            chunk_p99_ms: quantile(0.99),
            chunk_max_ms: max_ms,
            peaks,
        }
    }

    /// Worker-seconds per 10k built rows in `class`, or `None` with no rows.
    fn per_10k(&self, secs: f64) -> Option<f64> {
        (self.rows_built > 0).then(|| secs * 10_000.0 / self.rows_built as f64)
    }

    /// The JSON fields, without braces.
    pub fn json_fields(&self) -> String {
        let total: f64 = self.statements.iter().map(|(_, secs, _)| secs).sum();
        let classes = self
            .statements
            .iter()
            .map(|(label, secs, count)| {
                format!(
                    "\"{label}\":{{\"worker_secs\":{secs:.3},\"statements\":{count},\
                     \"worker_secs_per_10k_rows\":{}}}",
                    json_ms(self.per_10k(*secs))
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "\"build_rows\":{},\"build_worker_secs\":{total:.3},\
             \"build_worker_secs_per_10k_rows\":{},\"build_statements\":{{{classes}}},\
             \"build_delta_rows_appended\":{},\"build_delta_rows_merged\":{},\
             \"build_chunk_lock_timeouts\":{},\"seal_refusals\":{},\"build_chunks\":{},\
             \"build_chunk_ms_p50\":{},\"build_chunk_ms_p99\":{},\"build_chunk_ms_max\":{},\
             \"pg_wal_peak_bytes\":{},\"deltas_peak_rows\":{},\"deltas_peak_bytes\":{}",
            self.rows_built,
            json_ms(self.per_10k(total)),
            self.delta_rows_appended,
            self.delta_rows_merged,
            self.chunk_lock_timeouts,
            self.seal_refusals,
            self.chunks,
            json_ms(self.chunk_p50_ms),
            json_ms(self.chunk_p99_ms),
            json_ms(self.chunk_max_ms),
            self.peaks.wal_dir_bytes,
            self.peaks.deltas_rows,
            self.peaks.deltas_bytes,
        )
    }

    /// One human-readable line.
    pub fn human(&self) -> String {
        let total: f64 = self.statements.iter().map(|(_, secs, _)| secs).sum();
        let top = self
            .statements
            .iter()
            .filter(|(_, secs, _)| *secs > 0.0)
            .map(|(label, secs, _)| format!("{label} {secs:.1}s"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "build: {} rows, {:.1} worker-s ({} per 10k rows: {top}); {} chunks p50/p99/max \
             {}/{}/{} ms, {} lock timeouts; deltas {} appended / {} merged, peak {} rows; \
             seal refusals {}; pg_wal peak {:.0} MB",
            self.rows_built,
            total,
            json_ms(self.per_10k(total)),
            self.chunks,
            json_ms(self.chunk_p50_ms),
            json_ms(self.chunk_p99_ms),
            json_ms(self.chunk_max_ms),
            self.chunk_lock_timeouts,
            self.delta_rows_appended,
            self.delta_rows_merged,
            self.peaks.deltas_rows,
            self.seal_refusals,
            self.peaks.wal_dir_bytes as f64 / 1e6,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEFORE: &str = "\
# TYPE trellis_build_chunk_seconds histogram
trellis_build_chunk_seconds_bucket{le=\"0.01\"} 1
trellis_build_chunk_seconds_bucket{le=\"0.1\"} 1
trellis_build_chunk_seconds_bucket{le=\"1\"} 1
trellis_build_chunk_seconds_bucket{le=\"+Inf\"} 1
trellis_build_chunk_seconds_count 1
trellis_build_statement_seconds_sum{class=\"chunk_write\"} 0.5
trellis_build_statement_seconds_count{class=\"chunk_write\"} 1
trellis_build_rows_total 100
";

    const AFTER: &str = "\
trellis_build_chunk_seconds_bucket{le=\"0.01\"} 1
trellis_build_chunk_seconds_bucket{le=\"0.1\"} 91
trellis_build_chunk_seconds_bucket{le=\"1\"} 100
trellis_build_chunk_seconds_bucket{le=\"+Inf\"} 101
trellis_build_chunk_seconds_count 101
trellis_build_chunk_seconds_max 2.5
trellis_build_statement_seconds_sum{class=\"chunk_write\"} 10.5
trellis_build_statement_seconds_count{class=\"chunk_write\"} 101
trellis_build_rows_total 20100
trellis_build_delta_rows_total{step=\"appended\"} 300
trellis_seal_refused_total 2
";

    #[test]
    fn the_window_is_the_difference_of_two_scrapes() {
        let p = BuildProfile::between(
            &MetricsSnapshot::parse(BEFORE),
            &MetricsSnapshot::parse(AFTER),
            Peaks::default(),
        );
        assert_eq!(p.rows_built, 20_000);
        assert_eq!(p.chunks, 100);
        assert_eq!(p.delta_rows_appended, 300);
        assert_eq!(p.seal_refusals, 2);
        let write = p
            .statements
            .iter()
            .find(|(label, _, _)| *label == "chunk_write")
            .expect("chunk_write");
        assert_eq!((write.1, write.2), (10.0, 100));
        assert_eq!(p.per_10k(write.1), Some(5.0));
        // 90 of the window's 100 chunks took at most 100 ms, 99 at most 1 s.
        assert_eq!(p.chunk_p50_ms, Some(100.0));
        assert_eq!(p.chunk_p99_ms, Some(1000.0));
        assert_eq!(p.chunk_max_ms, Some(2500.0));
        assert!(p.json_fields().contains("\"build_chunk_ms_p99\":1000.000"));
    }
}
