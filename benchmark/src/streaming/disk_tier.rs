//! Disk-tier columns (from issue #558 experiment 5, #617): what a measured window cost
//! the cluster's storage, so a number taken with the cluster on a real disk
//! (`bench --disk`) carries its own I/O context rather than just a rate.
//!
//! [`sample`] reads, from the cluster, at the start and end of a window:
//!
//! - `pg_current_wal_lsn()`, whose delta is the window's WAL bytes (source
//!   writes and engine writes together, as [`super::idle_cost`] measures it);
//! - `pg_stat_wal.wal_sync` (PG 14-17), the number of WAL fsyncs. PG 18 moved
//!   it to `pg_stat_io` (`object = 'wal'`, `fsyncs`), which is read instead
//!   when the column is gone; neither present reads as `null`, never `0`;
//! - `pg_stat_checkpointer.buffers_written`/`num_timed`/`num_requested`
//!   (PG 17+), or `pg_stat_bgwriter.buffers_checkpoint`/`checkpoints_timed`/
//!   `checkpoints_req` before it.
//!
//! All of these are cluster-wide cumulative statistics that backends flush
//! at most once a second, so a delta is accurate to about a second's worth of
//! activity at either end; that is why this only reports whole-window rates.
//!
//! [`storage`] names where the cluster lives ([`std::env::temp_dir`], which
//! testkit creates it under) and that directory's filesystem type, so a
//! JSON line says by itself whether it was measured on tmpfs or on a disk.
//!
//! [`LatencyHistogram`] is the writers' per-commit latency record the same
//! issue asks for next to these columns.

use std::path::Path;
use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

/// One reading of the cluster's cumulative WAL/checkpoint counters.
#[derive(Debug, Clone)]
pub struct DiskSample {
    at: Instant,
    lsn: String,
    wal_syncs: Option<i64>,
    checkpoint_buffers: Option<i64>,
    checkpoints_timed: Option<i64>,
    checkpoints_req: Option<i64>,
}

/// A window's deltas between two [`DiskSample`]s.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiskTier {
    pub window_secs: f64,
    pub wal_bytes: i64,
    pub wal_mb_per_sec: f64,
    /// `None` when this Postgres exposes no WAL fsync counter.
    pub fsyncs_per_sec: Option<f64>,
    pub checkpoint_buffers_written: Option<i64>,
    pub checkpoints_timed: Option<i64>,
    pub checkpoints_req: Option<i64>,
}

async fn has_column(raw: &RawClient, view: &str, column: &str) -> bool {
    raw.query_one(
        "select exists (select 1 from pg_attribute \
         where attrelid = to_regclass($1) and attname = $2 and not attisdropped)",
        &[&view, &column],
    )
    .await
    .expect("look up a statistics view's column")
    .get(0)
}

async fn opt_i64(raw: &RawClient, sql: &str) -> Option<i64> {
    raw.query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("read {sql:?}: {e}"))
        .get::<_, Option<i64>>(0)
}

pub async fn sample(raw: &RawClient) -> DiskSample {
    // This backend's own pending counters, at least, are current.
    raw.batch_execute("select pg_stat_force_next_flush()")
        .await
        .expect("flush this backend's statistics");
    let at = Instant::now();
    let lsn = super::idle_cost::wal_lsn(raw).await;
    let wal_syncs = if has_column(raw, "pg_catalog.pg_stat_wal", "wal_sync").await {
        opt_i64(raw, "select wal_sync from pg_stat_wal").await
    } else if has_column(raw, "pg_catalog.pg_stat_io", "fsyncs").await {
        opt_i64(
            raw,
            "select sum(fsyncs)::bigint from pg_stat_io where object = 'wal'",
        )
        .await
    } else {
        None
    };
    let (checkpoint_buffers, checkpoints_timed, checkpoints_req) =
        if has_column(raw, "pg_catalog.pg_stat_checkpointer", "buffers_written").await {
            let row = raw
                .query_one(
                    "select buffers_written, num_timed, num_requested from pg_stat_checkpointer",
                    &[],
                )
                .await
                .expect("read pg_stat_checkpointer");
            (Some(row.get(0)), Some(row.get(1)), Some(row.get(2)))
        } else if has_column(raw, "pg_catalog.pg_stat_bgwriter", "buffers_checkpoint").await {
            let row = raw
                .query_one(
                    "select buffers_checkpoint, checkpoints_timed, checkpoints_req \
                     from pg_stat_bgwriter",
                    &[],
                )
                .await
                .expect("read pg_stat_bgwriter");
            (Some(row.get(0)), Some(row.get(1)), Some(row.get(2)))
        } else {
            (None, None, None)
        };
    DiskSample {
        at,
        lsn,
        wal_syncs,
        checkpoint_buffers,
        checkpoints_timed,
        checkpoints_req,
    }
}

/// The deltas from `start` to a fresh [`sample`] taken now.
pub async fn since(raw: &RawClient, start: &DiskSample) -> DiskTier {
    let end = sample(raw).await;
    let wal_bytes = super::idle_cost::wal_bytes_since(raw, &start.lsn).await;
    DiskTier::between(start, &end, wal_bytes)
}

fn delta(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    Some(b? - a?)
}

impl DiskTier {
    fn between(start: &DiskSample, end: &DiskSample, wal_bytes: i64) -> Self {
        let window_secs = end.at.duration_since(start.at).as_secs_f64();
        let per_sec = |n: f64| {
            if window_secs > 0.0 {
                n / window_secs
            } else {
                0.0
            }
        };
        DiskTier {
            window_secs,
            wal_bytes,
            wal_mb_per_sec: per_sec(wal_bytes as f64 / 1_000_000.0),
            fsyncs_per_sec: delta(start.wal_syncs, end.wal_syncs).map(|n| per_sec(n as f64)),
            checkpoint_buffers_written: delta(start.checkpoint_buffers, end.checkpoint_buffers),
            checkpoints_timed: delta(start.checkpoints_timed, end.checkpoints_timed),
            checkpoints_req: delta(start.checkpoints_req, end.checkpoints_req),
        }
    }

    /// The disk-tier JSON fields, comma-separated with no surrounding comma,
    /// **excluding** `wal_bytes` (the scenarios that already report it keep
    /// their own key). Includes [`storage`]'s two fields.
    pub fn json_fields(&self) -> String {
        let storage = storage();
        format!(
            "\"disk_window_secs\":{:.3},\"wal_mb_per_sec\":{:.3},\"fsyncs_per_sec\":{},\
             \"checkpoint_buffers_written\":{},\"checkpoints_timed\":{},\"checkpoints_req\":{},\
             \"storage\":\"{}\",\"storage_fs\":\"{}\"",
            self.window_secs,
            self.wal_mb_per_sec,
            self.fsyncs_per_sec
                .map(|v| format!("{v:.1}"))
                .unwrap_or_else(|| "null".into()),
            json_opt(self.checkpoint_buffers_written),
            json_opt(self.checkpoints_timed),
            json_opt(self.checkpoints_req),
            json_escape(&storage.dir),
            json_escape(&storage.fs_type),
        )
    }

    /// A short human-readable form for a scenario's stderr summary.
    pub fn human(&self) -> String {
        format!(
            "wal {:.1} MB/s, {} fsyncs/s, {} checkpoint buffers ({} timed + {} requested checkpoints)",
            self.wal_mb_per_sec,
            self.fsyncs_per_sec
                .map(|v| format!("{v:.0}"))
                .unwrap_or_else(|| "?".into()),
            json_opt(self.checkpoint_buffers_written),
            json_opt(self.checkpoints_timed),
            json_opt(self.checkpoints_req),
        )
    }
}

pub fn json_opt(v: Option<i64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "null".into())
}

pub fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Where the cluster's data directory lives, and that filesystem's type.
pub struct Storage {
    pub dir: String,
    pub fs_type: String,
}

pub fn storage() -> Storage {
    let dir = std::env::temp_dir();
    let dir = dir.canonicalize().unwrap_or(dir);
    let fs_type = std::fs::read_to_string("/proc/self/mounts")
        .ok()
        .and_then(|mounts| fs_type_of(&mounts, &dir))
        .unwrap_or_else(|| "unknown".into());
    Storage {
        dir: dir.display().to_string(),
        fs_type,
    }
}

/// The filesystem type of the longest mount point in `mounts` (the
/// `/proc/self/mounts` format) that contains `path`.
fn fs_type_of(mounts: &str, path: &Path) -> Option<String> {
    mounts
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let _device = fields.next()?;
            let mount_point = fields.next()?.replace("\\040", " ");
            let fs_type = fields.next()?;
            path.starts_with(&mount_point)
                .then(|| (mount_point.len(), fs_type.to_string()))
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, fs_type)| fs_type)
}

/// A log-linear latency histogram in microseconds: exact below 128us, then 64
/// sub-buckets per power of two (under 1.6% relative error), in a fixed
/// ~30KB whatever the sample count, so a writer can record every commit of a
/// long run with flat memory.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    buckets: Vec<u64>,
    count: u64,
}

const SUB_BUCKETS: u64 = 64;
const LINEAR: u64 = 2 * SUB_BUCKETS;
const BUCKETS: usize = (LINEAR + (64 - 7) * SUB_BUCKETS) as usize;

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram {
            buckets: vec![0; BUCKETS],
            count: 0,
        }
    }
}

fn bucket_of(micros: u64) -> usize {
    if micros < LINEAR {
        return micros as usize;
    }
    let msb = 63 - u64::from(micros.leading_zeros()); // >= 7
    let shift = msb - 6;
    (LINEAR + (msb - 7) * SUB_BUCKETS + ((micros >> shift) - SUB_BUCKETS)) as usize
}

/// The midpoint of `bucket`'s range, in microseconds.
fn bucket_value(bucket: usize) -> f64 {
    let bucket = bucket as u64;
    if bucket < LINEAR {
        return bucket as f64;
    }
    let msb = (bucket - LINEAR) / SUB_BUCKETS + 7;
    let mantissa = (bucket - LINEAR) % SUB_BUCKETS + SUB_BUCKETS;
    let shift = msb - 6;
    let lo = (mantissa << shift) as f64;
    lo + ((1u64 << shift) as f64 - 1.0) / 2.0
}

impl LatencyHistogram {
    pub fn record(&mut self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.buckets[bucket_of(micros)] += 1;
        self.count += 1;
    }

    pub fn merge(&mut self, other: &LatencyHistogram) {
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += b;
        }
        self.count += other.count;
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// The `q` quantile (0..=1) in milliseconds, `None` when empty.
    pub fn quantile_ms(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((q * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0;
        for (bucket, &n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return Some(bucket_value(bucket) / 1000.0);
            }
        }
        unreachable!("rank {rank} is within count {}", self.count)
    }
}

/// A quantile as JSON: three decimals, or `null`.
pub fn json_ms(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.3}"))
        .unwrap_or_else(|| "null".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_within_resolution() {
        let mut last = 0;
        for micros in (0..200_000u64).chain([1 << 30, u64::MAX / 3, u64::MAX]) {
            let b = bucket_of(micros);
            assert!(b >= last, "bucket of {micros} went backwards");
            assert!(b < BUCKETS, "bucket of {micros} out of range");
            last = b;
            let v = bucket_value(b);
            let err = (v - micros as f64).abs() / (micros.max(1) as f64);
            assert!(err <= 1.0 / 64.0, "{micros}us reads back as {v} ({err})");
        }
    }

    #[test]
    fn quantiles_read_the_recorded_distribution() {
        let mut h = LatencyHistogram::default();
        assert_eq!(h.quantile_ms(0.5), None);
        for ms in 1..=100u64 {
            h.record(Duration::from_millis(ms));
        }
        let p50 = h.quantile_ms(0.5).unwrap();
        let p99 = h.quantile_ms(0.99).unwrap();
        assert!((p50 - 50.0).abs() / 50.0 < 0.02, "p50 {p50}");
        assert!((p99 - 99.0).abs() / 99.0 < 0.02, "p99 {p99}");

        let mut other = LatencyHistogram::default();
        for _ in 0..900 {
            other.record(Duration::from_micros(10));
        }
        h.merge(&other);
        assert_eq!(h.count(), 1000);
        assert_eq!(h.quantile_ms(0.5), Some(0.010));
    }

    #[test]
    fn fs_type_is_the_longest_containing_mount() {
        let mounts = "/dev/nvme0n1p3 / btrfs rw 0 0\n\
                      tmpfs /tmp tmpfs rw 0 0\n\
                      /dev/nvme0n1p3 /home btrfs rw 0 0\n\
                      /dev/sdb1 /mnt/my\\040disk xfs rw 0 0\n";
        assert_eq!(
            fs_type_of(mounts, Path::new("/tmp/x")).as_deref(),
            Some("tmpfs")
        );
        assert_eq!(
            fs_type_of(mounts, Path::new("/var/tmp")).as_deref(),
            Some("btrfs")
        );
        assert_eq!(
            fs_type_of(mounts, Path::new("/mnt/my disk/pg")).as_deref(),
            Some("xfs")
        );
    }
}
