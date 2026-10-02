//! `build-under-load`: an aggregate's build over a large source while a
//! write load runs throughout. Built for issue #558 experiment 5 (#617) and
//! ported to main as the harness every #556 milestone measures itself with
//! (#620 part A3, #629).
//!
//! `agg_src(id bigint primary key, grp integer, amt bigint)` is loaded with
//! `--rows` rows by streamed `COPY ... FROM STDIN` (text format, one
//! transaction per million-row batch, `--loaders` connections), `grp`
//! uniform over `--groups`. `--writers` paced writers then start a mixed
//! single-row load at `--write-rate` statements/sec in total (70% `amt`
//! updates, 15% group moves, 10% inserts above the loaded ids, 5% deletes)
//! `--pre-define-secs` **before** `GROUP BY grp SELECT SUM(amt) AS total,
//! COUNT(*) AS n` is defined (with `--min-max`, also `MIN(amt) AS lo,
//! MAX(amt) AS hi`: recomputed fields, #625 F5), and keep it up through the build and for
//! `--duration-secs` after the definition reads `live`. Once they stop, the
//! target must equal a from-scratch `GROUP BY` over the source (the SQL
//! oracle) within `--grace-secs`. That comparison is a full scan, so it only
//! runs once the definition is `live` and the engine reports nothing pending
//! through the writers' last commit (`converged_through` on a token read
//! after they stopped; `engine_converged_secs` records when), and after a
//! mismatch waits `max(--oracle-poll-min-secs, 2 x its last duration)` before
//! the next (`oracle_checks`/`oracle_check_secs` record how many ran and how
//! long the last took).
//!
//! What it measures:
//!
//! - the build's wall time and progress (sampled from `backfill_chunks` every
//!   [`MONITOR_POLL`]; today's aggregate build is one direct-build job, so
//!   `chunks` reads 1), and define -> `live`, which includes the go-live
//!   catch-up re-read;
//! - define -> converged and the tail from the writers stopping to
//!   convergence;
//! - the writers' commit latency during the build and overall, and whether
//!   they kept `--write-rate`;
//! - the window's [`disk_tier`](super::disk_tier) columns (WAL MB/s and
//!   total, fsyncs/s, checkpoint buffers), deadlocks and rollbacks, and its
//!   [`server_cost`](super::server_cost) columns (Postgres CPU, `deadlock
//!   detected` lines, lock-timeout warnings, ledger bytes); per-row columns
//!   divide by the rows folded in the window, the `--rows` the build read
//!   plus the writer statements that changed a row;
//! - lock waits and page lock holds by statement class
//!   ([`contention`](super::contention)), sampled over the same window. The
//!   build's own writes to the target count as group upserts;
//! - the oldest client `xmin` held from define to the writers' stop, and the
//!   oldest open transaction;
//! - peak process RSS ([`process_memory`](super::process_memory)): overall,
//!   and per phase (`load`: COPY + index; `build`: client start, the
//!   `--pre-define-secs` of writes, then define -> `live`; `converge`:
//!   `live` -> converged or the grace deadline), plus `VmHWM` and the enclosing
//!   cgroup's `memory.peak`. #617 found today's drain holds a bucket's whole
//!   share of the go-live re-read's segment in memory (about 650 B per
//!   staged row, killed at 13.8 GB for 20M rows); the `converge` peak is the
//!   number #620's batch cap has to bound.
//!
//! - the Re-derive build's profile (#625 F2)
//!   ([`build_profile`]): worker-seconds per build statement class, chunk
//!   transaction p50/p99/max, delta rows appended and merged and the delta
//!   table's peak, chunks that gave up on their entry lock, seal refusals,
//!   and the `pg_wal` directory's peak; and the contention sampler's
//!   `build_waits`, the build's backends' waits by class. `--writers 0`
//!   makes it a build-only run.
//!
//! The run's stderr carries a progress line every `--progress-secs` with the
//! current and peak RSS. Run it under a memory cap on a shared box:
//! `systemd-run --user --scope -p MemoryMax=16G -p MemorySwapMax=0 bench ...`.

use std::io::Write as _;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::SinkExt;
use rand::{Rng, SeedableRng};
use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;
use crate::streaming::build_profile::{self, BuildProfile, MetricsSnapshot};
use crate::streaming::chain::numeric_columns;
use crate::streaming::contention::{self, ContentionSummary};
use crate::streaming::disk_tier::{self, LatencyHistogram, json_ms};
use crate::streaming::load::GENERATOR_UNDERSHOOT_TOLERANCE;
use crate::streaming::process_memory::{self, RssSampler, RssSummary};
use crate::streaming::server_cost::{self, ServerCost};
use crate::streaming::tuning::EngineTuning;

const SOURCE: &str = "agg_src";
/// The definition's target table (its bare name).
const TARGET: &str = "agg_totals";
const LOAD_BATCH_ROWS: u64 = 1_000_000;
const COPY_BUFFER_BYTES: usize = 1 << 20;
const MONITOR_POLL: Duration = Duration::from_millis(100);
const CONVERGE_POLL: Duration = Duration::from_millis(500);
const XMIN_POLL: Duration = Duration::from_millis(250);

/// The scenario's knobs; see the module doc and `cli.rs` for defaults.
#[derive(Debug, Clone, Copy)]
pub struct BuildUnderLoad {
    pub rows: u64,
    pub groups: i32,
    pub loaders: usize,
    pub writers: usize,
    pub write_rate: f64,
    /// How long the writers run before the definition is created.
    pub pre_define: Duration,
    /// How long the writers keep running once the definition is `live`.
    pub post_live: Duration,
    /// Deadline for define -> `live`.
    pub build_timeout: Duration,
    /// Deadline for convergence once the writers stop.
    pub grace: Duration,
    /// After a mismatched oracle comparison, the next waits at least this
    /// long, or twice the comparison's own duration if that is longer.
    pub oracle_poll_min: Duration,
    /// How often the run prints its RSS progress line (`None`: never).
    pub progress: Option<Duration>,
    /// `--min-max`: the definition also has `MIN(amt)` and `MAX(amt)`,
    /// recomputed fields (#625 F5), and the oracle compares them too.
    pub min_max: bool,
}

impl BuildUnderLoad {
    /// The definition's text over `target` and `source`.
    fn definition(&self) -> String {
        let extremes = if self.min_max {
            ", MIN(amt) AS lo, MAX(amt) AS hi"
        } else {
            ""
        };
        format!(
            "TRANSFORM {TARGET} FROM public.{SOURCE} GROUP BY grp \
             SELECT grp AS grp, SUM(amt) AS total, COUNT(*) AS n{extremes}"
        )
    }

    /// The oracle's `GROUP BY` over the source, as `o`'s columns.
    fn oracle(&self) -> String {
        let extremes = if self.min_max {
            ", min(amt) as lo, max(amt) as hi"
        } else {
            ""
        };
        format!(
            "select grp, sum(amt) as total, count(*) as n{extremes} \
             from public.{SOURCE} group by grp"
        )
    }

    /// The predicate over target `t` and oracle `o` that a group differs.
    fn differs(&self) -> &'static str {
        if self.min_max {
            "t.grp is null or o.grp is null \
             or t.total::numeric is distinct from o.total::numeric \
             or t.n::bigint is distinct from o.n \
             or t.lo::numeric is distinct from o.lo::numeric \
             or t.hi::numeric is distinct from o.hi::numeric"
        } else {
            "t.grp is null or o.grp is null \
             or t.total::numeric is distinct from o.total::numeric \
             or t.n::bigint is distinct from o.n"
        }
    }
}

/// Writer statement kinds, in [`WriterTally::issued`] order.
const KINDS: [&str; 4] = ["update_amt", "update_grp", "insert", "delete"];

#[derive(Debug, Clone, Default)]
pub struct WriterTally {
    /// Statements that succeeded, per [`KINDS`].
    pub issued: [u64; 4],
    /// Of those, how many touched no row (an id deleted earlier).
    pub noop: u64,
    pub errors: u64,
    /// Every successful statement's commit latency.
    pub latency_all: LatencyHistogram,
    /// Those that started while the definition was building (define -> live).
    pub latency_build: LatencyHistogram,
}

impl WriterTally {
    fn merge(&mut self, other: &WriterTally) {
        for (a, b) in self.issued.iter_mut().zip(other.issued) {
            *a += b;
        }
        self.noop += other.noop;
        self.errors += other.errors;
        self.latency_all.merge(&other.latency_all);
        self.latency_build.merge(&other.latency_build);
    }

    pub fn total(&self) -> u64 {
        self.issued.iter().sum()
    }
}

#[derive(Debug)]
pub struct BuildUnderLoadResult {
    pub cfg: BuildUnderLoad,
    pub application_threads: usize,
    /// The engine's `build_chunk_rows` (#625 F2).
    pub build_chunk_rows: i64,
    /// The Re-derive build's profile columns ([`build_profile`]), over the
    /// same window as `disk`.
    pub build: BuildProfile,
    pub load_secs: f64,
    pub load_rows_per_sec: f64,
    /// `alter table ... add primary key` after the load.
    pub index_secs: f64,
    /// define -> the first chunk claimed / the first chunk done.
    pub first_claim_secs: Option<f64>,
    pub first_chunk_secs: Option<f64>,
    /// `backfill_chunks` rows the build had (the most seen at once).
    pub chunks: i64,
    /// define -> the definition left `waiting_to_backfill`/`backfilling`.
    pub build_secs: f64,
    /// `chunks` over first claim -> build done; `None` without both.
    pub chunks_per_sec: Option<f64>,
    pub define_to_live_secs: f64,
    /// define -> the target equalled the oracle (`None`: not within grace).
    pub converged_secs: Option<f64>,
    /// writers stopped -> the target equalled the oracle.
    pub tail_secs: Option<f64>,
    /// writers stopped -> the engine first reported nothing pending through
    /// their last commit (`None`: not within grace). A run whose oracle fails
    /// with this set converged to a wrong value; with it `None`, it was stuck.
    pub engine_converged_secs: Option<f64>,
    pub oracle_ok: bool,
    pub oracle_mismatched_groups: i64,
    /// Full-source oracle comparisons run inside the measured window.
    pub oracle_checks: u32,
    /// How long the last of those took (`None` if none ran).
    pub oracle_check_secs: Option<f64>,
    /// Writer start -> writer stop.
    pub writer_secs: f64,
    pub achieved_write_rate: f64,
    pub kept_target_rate: bool,
    pub writes: WriterTally,
    pub deadlocks: i64,
    pub xact_rollbacks: i64,
    pub disk: disk_tier::DiskTier,
    /// Same window as `disk`; see the module doc.
    pub server: ServerCost,
    /// Same window as `disk`, from writer start to convergence.
    pub contention: ContentionSummary,
    pub memory: RssSummary,
    /// The oldest backend `xmin` seen from define to the writers' stop, as an
    /// age in transaction ids, and the longest time one `xmin` value stayed
    /// the oldest (a held horizon: what vacuum could not clean past).
    pub peak_xmin_age_xids: i64,
    pub peak_xmin_hold_secs: f64,
    /// The query text (first 100 chars) of that longest hold's backend.
    pub peak_xmin_holder: String,
    /// The oldest open client transaction holding an xid (the thing that
    /// holds everybody's `xmin` back), its age at its peak and its query.
    pub peak_xact_secs: f64,
    pub peak_xact_query: String,
    /// The source's and the target's total size (heap, indexes, toast)
    /// after convergence.
    pub source_bytes: i64,
    pub target_bytes: i64,
}

/// Samples the oldest client-backend `xmin` on its own connection until
/// `stop`; see [`BuildUnderLoadResult::peak_xmin_age_xids`]. Autovacuum and
/// other background workers are left out: a lazy vacuum's `xmin` holds
/// nobody's horizon, and it is not the build's.
async fn sample_xmin(raw: RawClient, stop: Arc<AtomicBool>) -> XminSample {
    let mut peak_xact = 0f64;
    let mut peak_xact_query = String::new();
    let mut peak_age = 0i64;
    let mut peak_hold = 0f64;
    let mut peak_holder = String::new();
    let mut held: Option<(String, Instant)> = None;
    while !stop.load(Ordering::Relaxed) {
        let row = raw
            .query_opt(
                "select backend_xmin::text, age(backend_xmin)::bigint, \
                        left(regexp_replace(query, '\\s+', ' ', 'g'), 100) \
                 from pg_stat_activity \
                 where backend_xmin is not null and pid <> pg_backend_pid() \
                   and backend_type = 'client backend' \
                 order by age(backend_xmin) desc limit 1",
                &[],
            )
            .await
            .expect("sample backend xmin");
        // The oldest open transaction with an xid: what holds everybody's
        // xmin back.
        if let Some(r) = raw
            .query_opt(
                "select extract(epoch from now() - xact_start)::float8, \
                        left(regexp_replace(query, '\\s+', ' ', 'g'), 100) \
                 from pg_stat_activity \
                 where backend_xid is not null and pid <> pg_backend_pid() \
                   and backend_type = 'client backend' \
                 order by xact_start limit 1",
                &[],
            )
            .await
            .expect("sample the oldest transaction")
        {
            let secs: f64 = r.get(0);
            if secs > peak_xact {
                peak_xact = secs;
                peak_xact_query = r.get(1);
            }
        }
        let now = Instant::now();
        held = match (held, row) {
            (held, Some(row)) => {
                let oldest: String = row.get(0);
                peak_age = peak_age.max(row.get(1));
                match held {
                    Some((x, since)) if x == oldest => {
                        let hold = now.duration_since(since).as_secs_f64();
                        if hold > peak_hold {
                            peak_hold = hold;
                            peak_holder = row.get(2);
                        }
                        Some((x, since))
                    }
                    _ => Some((oldest, now)),
                }
            }
            (_, None) => None,
        };
        tokio::time::sleep(XMIN_POLL).await;
    }
    XminSample {
        peak_age,
        peak_hold,
        peak_holder,
        peak_xact,
        peak_xact_query,
    }
}

struct XminSample {
    peak_age: i64,
    peak_hold: f64,
    peak_holder: String,
    peak_xact: f64,
    peak_xact_query: String,
}

/// Total on-disk size of the source and the target.
async fn relation_sizes(raw: &RawClient, terminal: &str) -> (i64, i64) {
    let row = raw
        .query_one(
            &format!(
                "select pg_total_relation_size('public.{SOURCE}'), \
                 pg_total_relation_size('public.{terminal}')"
            ),
            &[],
        )
        .await
        .expect("read relation sizes");
    (row.get(0), row.get(1))
}

fn opt_f(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.3}"))
        .unwrap_or_else(|| "null".into())
}

impl BuildUnderLoadResult {
    pub fn to_json(&self, scenario: &str) -> String {
        let writes_issued = KINDS
            .iter()
            .zip(self.writes.issued)
            .map(|(k, n)| format!("\"{k}\":{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let w = &self.writes;
        format!(
            "{{\"scenario\":\"{}\",\"rows\":{},\"groups\":{},\"min_max\":{},\"writers\":{},\"write_rate\":{},\
             \"application_threads\":{},\"build_chunk_rows\":{},\
             \"load_secs\":{:.3},\"load_rows_per_sec\":{:.0},\
             \"index_secs\":{:.3},\"build_secs\":{:.3},\"chunks\":{},\"first_claim_secs\":{},\
             \"first_chunk_secs\":{},\"chunks_per_sec\":{},\"define_to_live_secs\":{:.3},\
             \"post_live_secs\":{:.3},\"converged_secs\":{},\"tail_secs\":{},\
             \"engine_converged_secs\":{},\"oracle_ok\":{},\
             \"oracle_mismatched_groups\":{},\"oracle_checks\":{},\"oracle_check_secs\":{},\
             \"writer_secs\":{:.3},\"achieved_write_rate\":{:.1},\
             \"kept_target_rate\":{},\"writes_issued\":{{{}}},\"writes_noop\":{},\
             \"writer_errors\":{},\"writer_lat_p50_ms\":{},\"writer_lat_p99_ms\":{},\
             \"writer_lat_build_p50_ms\":{},\"writer_lat_build_p99_ms\":{},\
             \"deadlocks\":{},\"xact_rollbacks\":{},\"wal_bytes\":{},\
             \"folded_rows\":{},\"wal_bytes_per_row\":{:.1},{},{},{},{},{},\
             \"peak_xmin_age_xids\":{},\"peak_xmin_hold_secs\":{:.3},\
             \"peak_xmin_holder\":\"{}\",\"peak_xact_secs\":{:.3},\"peak_xact_query\":\"{}\",\
             \"source_bytes\":{},\"target_bytes\":{},\"contention\":{}}}",
            scenario,
            self.cfg.rows,
            self.cfg.groups,
            self.cfg.min_max,
            self.cfg.writers,
            self.cfg.write_rate,
            self.application_threads,
            self.build_chunk_rows,
            self.load_secs,
            self.load_rows_per_sec,
            self.index_secs,
            self.build_secs,
            self.chunks,
            opt_f(self.first_claim_secs),
            opt_f(self.first_chunk_secs),
            opt_f(self.chunks_per_sec),
            self.define_to_live_secs,
            self.cfg.post_live.as_secs_f64(),
            opt_f(self.converged_secs),
            opt_f(self.tail_secs),
            opt_f(self.engine_converged_secs),
            self.oracle_ok,
            self.oracle_mismatched_groups,
            self.oracle_checks,
            opt_f(self.oracle_check_secs),
            self.writer_secs,
            self.achieved_write_rate,
            self.kept_target_rate,
            writes_issued,
            w.noop,
            w.errors,
            json_ms(w.latency_all.quantile_ms(0.5)),
            json_ms(w.latency_all.quantile_ms(0.99)),
            json_ms(w.latency_build.quantile_ms(0.5)),
            json_ms(w.latency_build.quantile_ms(0.99)),
            self.deadlocks,
            self.xact_rollbacks,
            self.disk.wal_bytes,
            self.folded_rows(),
            self.disk.wal_bytes as f64 / self.folded_rows().max(1) as f64,
            self.disk.json_fields(),
            self.server.json_fields(self.folded_rows(), self.cfg.rows),
            self.contention.lock_json_fields(),
            self.memory.json_fields(),
            self.build.json_fields(),
            self.peak_xmin_age_xids,
            self.peak_xmin_hold_secs,
            disk_tier::json_escape(&self.peak_xmin_holder),
            self.peak_xact_secs,
            disk_tier::json_escape(&self.peak_xact_query),
            self.source_bytes,
            self.target_bytes,
            self.contention.to_json(),
        )
    }

    /// Source rows the engine folded in the measured window: the `--rows`
    /// the build read, plus every writer statement that changed a row.
    pub fn folded_rows(&self) -> u64 {
        self.cfg.rows + self.writes.total().saturating_sub(self.writes.noop)
    }

    pub fn human(&self) -> String {
        let w = &self.writes;
        format!(
            "build-under-load: {} rows / {} groups, loaded at {:.0} rows/s; build {:.1}s over {} \
             chunks (first done {}), live after {:.1}s; converged {} (tail {}, engine settled {}, {} oracle checks of {}), oracle_ok={} \
             ({} mismatched); writers {:.0}/{} stmt/s (kept={}), commit p50/p99 {}/{} ms \
             overall, {}/{} ms during build; {}; {}; page lock hold p99 {} ms, wait p99 {} ms; {}; {}",
            self.cfg.rows,
            self.cfg.groups,
            self.load_rows_per_sec,
            self.build_secs,
            self.chunks,
            self.first_chunk_secs
                .map(|s| format!("{s:.1}s"))
                .unwrap_or_else(|| "never".into()),
            self.define_to_live_secs,
            self.converged_secs
                .map(|s| format!("{s:.1}s"))
                .unwrap_or_else(|| "never".into()),
            self.tail_secs
                .map(|s| format!("{s:.1}s"))
                .unwrap_or_else(|| "-".into()),
            self.engine_converged_secs
                .map(|s| format!("+{s:.1}s"))
                .unwrap_or_else(|| "never".into()),
            self.oracle_checks,
            self.oracle_check_secs
                .map(|s| format!("{s:.2}s"))
                .unwrap_or_else(|| "-".into()),
            self.oracle_ok,
            self.oracle_mismatched_groups,
            self.achieved_write_rate,
            self.cfg.write_rate,
            self.kept_target_rate,
            json_ms(w.latency_all.quantile_ms(0.5)),
            json_ms(w.latency_all.quantile_ms(0.99)),
            json_ms(w.latency_build.quantile_ms(0.5)),
            json_ms(w.latency_build.quantile_ms(0.99)),
            self.disk.human(),
            self.server.human(self.folded_rows()),
            json_ms(self.contention.page_lock_holds.p99_ms),
            json_ms(self.contention.page_lock_waits.p99_ms),
            self.memory.human(),
            self.build.human(),
        )
    }
}

/// Streams `rows` rows into the (index-less) source over `loaders`
/// connections, one `COPY` per [`LOAD_BATCH_ROWS`] batch. Each batch's rows
/// come from its own seeded RNG and a fixed-size buffer, so memory stays flat
/// whatever `rows` is.
async fn copy_load(dsn: &str, rows: u64, groups: i32, loaders: usize) {
    let batches = rows.div_ceil(LOAD_BATCH_ROWS);
    let next = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(loaders);
    for _ in 0..loaders.max(1) {
        let dsn = dsn.to_string();
        let next = next.clone();
        tasks.push(tokio::spawn(async move {
            let client = connect_raw(&dsn).await;
            loop {
                let batch = next.fetch_add(1, Ordering::Relaxed);
                if batch >= batches {
                    break;
                }
                let lo = batch * LOAD_BATCH_ROWS;
                let hi = (lo + LOAD_BATCH_ROWS).min(rows);
                let sink = client
                    .copy_in::<_, Bytes>(&format!("copy public.{SOURCE} (id, grp, amt) from stdin"))
                    .await
                    .expect("start copy");
                let mut sink = pin!(sink);
                let mut rng = rand::rngs::StdRng::seed_from_u64(0x5585_0000 ^ batch);
                let mut buf = Vec::with_capacity(COPY_BUFFER_BYTES + 64);
                for id in lo..hi {
                    let grp: i32 = rng.random_range(0..groups);
                    let amt: i64 = rng.random_range(0..1000);
                    writeln!(buf, "{id}\t{grp}\t{amt}").expect("format copy row");
                    if buf.len() >= COPY_BUFFER_BYTES {
                        let chunk =
                            std::mem::replace(&mut buf, Vec::with_capacity(COPY_BUFFER_BYTES + 64));
                        sink.send(Bytes::from(chunk)).await.expect("copy data");
                    }
                }
                if !buf.is_empty() {
                    sink.send(Bytes::from(buf)).await.expect("copy data");
                }
                sink.as_mut().finish().await.expect("finish copy");
            }
        }));
    }
    for t in tasks {
        t.await.expect("copy loader");
    }
}

const PHASE_PRE: u8 = 0;
const PHASE_BUILD: u8 = 1;
const PHASE_LIVE: u8 = 2;

struct WriterShared {
    start: Instant,
    rate: f64,
    /// Statement `k` is due at `k / rate` seconds after `start`.
    next_op: AtomicU64,
    /// The next id an insert takes; ids below it may exist.
    next_id: AtomicI64,
    phase: AtomicU8,
    stop: AtomicBool,
}

/// One writer: takes the next due statement slot, sleeps until it is due
/// (not at all when behind, so a writer that can't keep up shows up as a
/// lower achieved rate), then issues one autocommit statement of a kind drawn
/// from the 70/15/10/5 mix.
async fn writer(dsn: String, shared: Arc<WriterShared>, groups: i32, seed: u64) -> WriterTally {
    let client = connect_raw(&dsn).await;
    let upd_amt = client
        .prepare(&format!(
            "update public.{SOURCE} set amt = amt + $2 where id = $1"
        ))
        .await
        .expect("prepare amt update");
    let upd_grp = client
        .prepare(&format!(
            "update public.{SOURCE} set grp = $2 where id = $1"
        ))
        .await
        .expect("prepare group move");
    let insert = client
        .prepare(&format!(
            "insert into public.{SOURCE} (id, grp, amt) values ($1, $2, $3)"
        ))
        .await
        .expect("prepare insert");
    let delete = client
        .prepare(&format!("delete from public.{SOURCE} where id = $1"))
        .await
        .expect("prepare delete");
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut tally = WriterTally::default();
    loop {
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        let k = shared.next_op.fetch_add(1, Ordering::Relaxed);
        let due = shared.start + Duration::from_secs_f64(k as f64 / shared.rate);
        tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        let roll: u32 = rng.random_range(0..100);
        let kind = match roll {
            0..70 => 0,
            70..85 => 1,
            85..95 => 2,
            _ => 3,
        };
        let bound = shared.next_id.load(Ordering::Relaxed);
        let phase = shared.phase.load(Ordering::Relaxed);
        let (t0, result) = match kind {
            0 => {
                let id: i64 = rng.random_range(0..bound);
                let delta: i64 = rng.random_range(1..=10);
                (
                    Instant::now(),
                    client.execute(&upd_amt, &[&id, &delta]).await,
                )
            }
            1 => {
                let id: i64 = rng.random_range(0..bound);
                let grp: i32 = rng.random_range(0..groups);
                (Instant::now(), client.execute(&upd_grp, &[&id, &grp]).await)
            }
            2 => {
                let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
                let grp: i32 = rng.random_range(0..groups);
                let amt: i64 = rng.random_range(0..1000);
                (
                    Instant::now(),
                    client.execute(&insert, &[&id, &grp, &amt]).await,
                )
            }
            _ => {
                let id: i64 = rng.random_range(0..bound);
                (Instant::now(), client.execute(&delete, &[&id]).await)
            }
        };
        let latency = t0.elapsed();
        match result {
            Ok(n) => {
                tally.issued[kind] += 1;
                if n == 0 {
                    tally.noop += 1;
                }
                tally.latency_all.record(latency);
                if phase == PHASE_BUILD {
                    tally.latency_build.record(latency);
                }
            }
            Err(e) => {
                tally.errors += 1;
                if tally.errors <= 3 {
                    eprintln!("build-under-load: writer {} failed: {e}", KINDS[kind]);
                }
            }
        }
    }
    tally
}

/// Where the build stands, sampled every [`MONITOR_POLL`].
#[derive(Debug, Default)]
struct BuildProgress {
    first_claim: Option<f64>,
    first_done: Option<f64>,
    chunks: i64,
    built: Option<f64>,
    live: Option<f64>,
}

/// Polls the definition's status and `backfill_chunks` rows from `define_at`
/// until it reads `live`, recording when each milestone was first seen.
async fn monitor_build(
    raw: &RawClient,
    terminal: &str,
    define_at: Instant,
    deadline: Instant,
) -> BuildProgress {
    let target = format!("public.{terminal}");
    let mut p = BuildProgress::default();
    loop {
        let row = raw
            .query_opt(
                "select d.status, count(c.id), count(c.id) filter (where c.done), \
                     count(c.id) filter (where c.done or c.claimed_by is not null) \
                 from transform_definitions d \
                 left join backfill_chunks c on c.definition_id = d.id \
                 where d.target_table = $1 group by d.status",
                &[&target],
            )
            .await
            .expect("read build progress");
        let now = define_at.elapsed().as_secs_f64();
        if let Some(row) = row {
            let status: String = row.get(0);
            let (total, done, touched): (i64, i64, i64) = (row.get(1), row.get(2), row.get(3));
            p.chunks = p.chunks.max(total);
            if touched > 0 {
                p.first_claim.get_or_insert(now);
            }
            if done > 0 {
                p.first_done.get_or_insert(now);
            }
            match status.as_str() {
                "waiting_to_backfill" | "backfilling" => {}
                "catching_up" => {
                    p.built.get_or_insert(now);
                }
                "live" => {
                    p.built.get_or_insert(now);
                    p.live = Some(now);
                    return p;
                }
                other => panic!("{target} went {other:?} during its build"),
            }
        }
        assert!(
            Instant::now() < deadline,
            "{target} never reached 'live' within the build timeout ({p:?})"
        );
        tokio::time::sleep(MONITOR_POLL).await;
    }
}

/// How long to wait after a mismatched oracle comparison that took `took`:
/// `max(min, 2 * took)`, so a slow full-source scan never runs back to back.
fn oracle_backoff(min: Duration, took: Duration) -> Duration {
    min.max(took * 2)
}

async fn is_live(raw: &RawClient, terminal: &str) -> bool {
    raw.query_opt(
        "select 1 from transform_definitions where target_table = $1 and status = 'live'",
        &[&format!("public.{terminal}")],
    )
    .await
    .expect("read definition status")
    .is_some()
}

/// On a failed run: up to 20 mismatched groups, target vs oracle (stderr),
/// so the log carries the shape.
async fn dump_mismatches(raw: &RawClient, cfg: &BuildUnderLoad, terminal: &str) {
    let rows = raw
        .query(
            &format!(
                "with o as ({oracle}) \
                 select coalesce(t.grp::text, o.grp::text), t.total::text, o.total::text, \
                        t.n::text, o.n::text, {extremes} \
                 from o full outer join public.{terminal} t on t.grp::numeric = o.grp::numeric \
                 where {differs} \
                 order by 1 limit 20",
                oracle = cfg.oracle(),
                differs = cfg.differs(),
                // `--min-max` runs' extremes, so a group that differs only in
                // its `MIN`/`MAX` shows how.
                extremes = if cfg.min_max {
                    "format('lo=%s hi=%s', t.lo, t.hi), format('lo=%s hi=%s', o.lo, o.hi)"
                } else {
                    "'', ''"
                },
            ),
            &[],
        )
        .await
        .expect("list mismatched groups");
    for r in rows {
        eprintln!(
            "build-under-load: MISMATCH grp={} target total={:?} n={:?} {} oracle total={:?} n={:?} {}",
            r.get::<_, String>(0),
            r.get::<_, Option<String>>(1),
            r.get::<_, Option<String>>(3),
            r.get::<_, String>(5),
            r.get::<_, Option<String>>(2),
            r.get::<_, Option<String>>(4),
            r.get::<_, String>(6),
        );
    }
}

/// Whether the engine reports nothing pending through `token` (a WAL position
/// taken after every writer's last commit): no ring slot holds an unapplied row at or below it (a phase-gap straggler in
/// a drained segment's slot included) and nothing is parked in
/// `poison_held`. This is the engine's own one-statement, one-snapshot
/// predicate (`converged_through`), used only to decide when the full-source
/// oracle is worth running, never as the verdict. A hand-rolled ring probe
/// can't answer it: it would miss a straggler and a parked row, and its
/// per-slot queries race a seal.
async fn engine_converged(raw: &RawClient, token: trellis::PgLsn) -> bool {
    trellis::dev::staging::converged_through(raw, token)
        .await
        .expect("check the engine's convergence through the writers' last commit")
}

/// What the engine still holds when the target disagrees with the oracle, so
/// a failed run's log says whether it was stuck (a parked row, a segment
/// never drained) or converged to a wrong value.
async fn dump_pending(raw: &RawClient, token: trellis::PgLsn) {
    let segments = raw
        .query(
            "select state::text, count(*) from trellis.segments group by 1 order by 1",
            &[],
        )
        .await
        .expect("read segment states")
        .iter()
        .map(|r| format!("{}={}", r.get::<_, String>(0), r.get::<_, i64>(1)))
        .collect::<Vec<_>>()
        .join(" ");
    let parked: i64 = raw
        .query_one("select count(*) from trellis.poison_held", &[])
        .await
        .expect("count parked rows")
        .get(0);
    eprintln!(
        "build-under-load: engine converged through the writers' last commit: {}; \
         segments: {segments}; poison_held rows: {parked}",
        engine_converged(raw, token).await
    );
}

/// Groups where the target disagrees with a from-scratch `GROUP BY` over the
/// source, computed entirely on the server.
async fn mismatched_groups(raw: &RawClient, cfg: &BuildUnderLoad, terminal: &str) -> i64 {
    raw.query_one(
        &format!(
            "select count(*) from ({oracle}) o \
             full outer join public.{terminal} t on t.grp::numeric = o.grp::numeric \
             where {differs}",
            oracle = cfg.oracle(),
            differs = cfg.differs(),
        ),
        &[],
    )
    .await
    .expect("compare aggregate target against a from-scratch group by")
    .get(0)
}

pub async fn run(cfg: BuildUnderLoad, tuning: &EngineTuning) -> BuildUnderLoadResult {
    assert!(cfg.rows >= 1 && cfg.groups >= 1 && (cfg.writers == 0 || cfg.write_rate > 0.0));
    let memory = RssSampler::start(
        process_memory::DEFAULT_INTERVAL,
        cfg.progress.map(|every| (every, "build-under-load")),
    );

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let sampler = connect_raw(db.dsn()).await;

    // Loaded index-less, then keyed: the usual bulk-load order, and it keeps
    // the load's rows/s about the COPY rather than about btree inserts.
    raw.batch_execute(&format!(
        "create table public.{SOURCE} (id bigint not null, grp integer not null, \
             amt bigint not null)"
    ))
    .await
    .expect("create source table");
    let load_start = Instant::now();
    copy_load(db.dsn(), cfg.rows, cfg.groups, cfg.loaders).await;
    let load_secs = load_start.elapsed().as_secs_f64();
    let load_rows_per_sec = cfg.rows as f64 / load_secs.max(f64::EPSILON);
    eprintln!(
        "build-under-load: loaded {} rows in {load_secs:.1}s ({load_rows_per_sec:.0} rows/s)",
        cfg.rows
    );
    let index_start = Instant::now();
    raw.batch_execute(&format!(
        "alter table public.{SOURCE} add primary key (id); \
         analyze public.{SOURCE};"
    ))
    .await
    .expect("key and analyze the source");
    let index_secs = index_start.elapsed().as_secs_f64();
    // The bulk load's dirty pages are written now, so the window's
    // checkpoint columns are about the build and the writers.
    raw.batch_execute("checkpoint")
        .await
        .expect("checkpoint after the load");

    memory.end_phase("load");

    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");

    let disk_start = disk_tier::sample(&sampler).await;
    let metrics_start = MetricsSnapshot::take();
    let peaks_stop = Arc::new(AtomicBool::new(false));
    let peaks_task = tokio::spawn(build_profile::sample_peaks(
        connect_raw(db.dsn()).await,
        TARGET.to_string(),
        peaks_stop.clone(),
    ));
    let server_start = server_cost::start(&cluster);
    let (deadlocks_before, rollbacks_before) = contention::deadlocks_and_rollbacks(&sampler).await;
    let contention_stop = Arc::new(AtomicBool::new(false));
    let contention_task = {
        let (dsn, stop) = (db.dsn().to_string(), contention_stop.clone());
        tokio::spawn(async move {
            let raw = connect_raw(&dsn).await;
            contention::sample_while(&raw, SOURCE, TARGET, Instant::now(), || {
                !stop.load(Ordering::Relaxed)
            })
            .await
        })
    };
    let shared = Arc::new(WriterShared {
        start: Instant::now(),
        rate: cfg.write_rate,
        next_op: AtomicU64::new(0),
        next_id: AtomicI64::new(cfg.rows as i64),
        phase: AtomicU8::new(PHASE_PRE),
        stop: AtomicBool::new(false),
    });
    let writers: Vec<_> = (0..cfg.writers)
        .map(|w| {
            tokio::spawn(writer(
                db.dsn().to_string(),
                shared.clone(),
                cfg.groups,
                0x558_0005 + w as u64,
            ))
        })
        .collect();
    tokio::time::sleep(cfg.pre_define).await;

    let columns = numeric_columns(&["id", "grp", "amt"]);
    let source_text = cfg.definition();
    let xmin_stop = Arc::new(AtomicBool::new(false));
    let xmin_task = tokio::spawn(sample_xmin(connect_raw(db.dsn()).await, xmin_stop.clone()));
    let define_at = Instant::now();
    shared.phase.store(PHASE_BUILD, Ordering::Relaxed);
    let def = trellis::dev::defs::install_definition(&db.pool, &source_text, &columns, "public")
        .await
        .expect("install aggregate definition");
    let terminal = def.def.target.clone();
    let progress = monitor_build(&raw, &terminal, define_at, define_at + cfg.build_timeout).await;
    shared.phase.store(PHASE_LIVE, Ordering::Relaxed);
    memory.end_phase("build");
    let define_to_live_secs = progress.live.expect("monitor returns once live");
    let build_secs = progress.built.unwrap_or(define_to_live_secs);
    eprintln!(
        "build-under-load: built in {build_secs:.1}s ({} chunks), live after \
         {define_to_live_secs:.1}s; writers continue {:.0}s",
        progress.chunks,
        cfg.post_live.as_secs_f64()
    );

    tokio::time::sleep(cfg.post_live).await;
    shared.stop.store(true, Ordering::Relaxed);
    let mut writes = WriterTally::default();
    for w in writers {
        writes.merge(&w.await.expect("writer task"));
    }
    let stopped_at = Instant::now();
    // Up to here, not through convergence: the oracle's full scan would be
    // the oldest snapshot of all.
    xmin_stop.store(true, Ordering::Relaxed);
    let xmin = xmin_task.await.expect("xmin sampler");
    let writer_secs = stopped_at.duration_since(shared.start).as_secs_f64();
    let achieved_write_rate = writes.total() as f64 / writer_secs;
    // `--writers 0`: a build-only run, which keeps its (zero) rate.
    let kept_target_rate = cfg.writers == 0
        || achieved_write_rate >= cfg.write_rate * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE);

    // Every writer has returned, so each of its statements has committed:
    // a position read now bounds all of them from above.
    let token = trellis::dev::staging::watermark_token(&raw)
        .await
        .expect("read the writers' stop position");

    // Converged: the definition is live, the engine reports nothing pending
    // through `token`, and the target equals the oracle. The first two are
    // cheap and polled every CONVERGE_POLL; the oracle is a full-source
    // GROUP BY (a full scan at 100M rows, inside the disk window), so it runs
    // only once both hold, and after a mismatch not again for oracle_backoff.
    let deadline = stopped_at + cfg.grace;
    let mut converged_at = None;
    let mut engine_converged_at = None;
    let mut mismatched = None;
    let mut oracle_checks = 0u32;
    let mut oracle_check_secs = None;
    let mut next_oracle = Instant::now();
    loop {
        let observed = Instant::now();
        let settled = match engine_converged_at {
            Some(_) => true,
            None => {
                let settled = engine_converged(&raw, token).await;
                if settled {
                    engine_converged_at = Some(observed);
                }
                settled
            }
        };
        if observed >= next_oracle && settled && is_live(&raw, &terminal).await {
            let m = mismatched_groups(&raw, &cfg, &terminal).await;
            let took = observed.elapsed();
            oracle_checks += 1;
            oracle_check_secs = Some(took.as_secs_f64());
            mismatched = Some(m);
            if m == 0 {
                converged_at = Some(observed);
                break;
            }
            next_oracle = Instant::now() + oracle_backoff(cfg.oracle_poll_min, took);
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }
    contention_stop.store(true, Ordering::Relaxed);
    peaks_stop.store(true, Ordering::Relaxed);
    let disk = disk_tier::since(&sampler, &disk_start).await;
    let build = BuildProfile::between(
        &metrics_start,
        &MetricsSnapshot::take(),
        peaks_task.await.expect("peak sampler"),
    );
    let server = server_start.finish(&sampler).await;
    let contention = contention_task.await.expect("contention sampler");
    memory.end_phase("converge");
    let (source_bytes, target_bytes) = relation_sizes(&raw, &terminal).await;
    let (deadlocks_after, rollbacks_after) = contention::deadlocks_and_rollbacks(&sampler).await;
    // Unconverged: a fresh count for the verdict, outside the disk window
    // and not counted in oracle_checks.
    let mismatched = match mismatched {
        Some(m) if converged_at.is_some() => m,
        _ => mismatched_groups(&raw, &cfg, &terminal).await,
    };
    if mismatched != 0 {
        dump_mismatches(&raw, &cfg, &terminal).await;
        dump_pending(&raw, token).await;
    }

    client.shutdown().await.expect("client shutdown");
    let memory = memory.finish();

    BuildUnderLoadResult {
        cfg,
        application_threads: tuning.application_threads,
        build_chunk_rows: tuning.build_chunk_rows,
        build,
        load_secs,
        load_rows_per_sec,
        index_secs,
        first_claim_secs: progress.first_claim,
        first_chunk_secs: progress.first_done,
        chunks: progress.chunks,
        build_secs,
        chunks_per_sec: progress
            .first_claim
            .filter(|&c| build_secs > c && progress.chunks > 0)
            .map(|c| progress.chunks as f64 / (build_secs - c)),
        define_to_live_secs,
        converged_secs: converged_at.map(|t| t.duration_since(define_at).as_secs_f64()),
        tail_secs: converged_at.map(|t| t.duration_since(stopped_at).as_secs_f64()),
        engine_converged_secs: engine_converged_at
            .map(|t| t.duration_since(stopped_at).as_secs_f64()),
        oracle_ok: mismatched == 0,
        oracle_mismatched_groups: mismatched,
        oracle_checks,
        oracle_check_secs,
        writer_secs,
        achieved_write_rate,
        kept_target_rate,
        writes,
        deadlocks: deadlocks_after - deadlocks_before,
        xact_rollbacks: rollbacks_after - rollbacks_before,
        disk,
        server,
        contention,
        memory,
        peak_xmin_age_xids: xmin.peak_age,
        peak_xmin_hold_secs: xmin.peak_hold,
        peak_xmin_holder: xmin.peak_holder,
        peak_xact_secs: xmin.peak_xact,
        peak_xact_query: xmin.peak_xact_query,
        source_bytes,
        target_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_backoff_is_the_floor_or_twice_the_last_check() {
        let min = Duration::from_secs(5);
        assert_eq!(oracle_backoff(min, Duration::from_millis(40)), min);
        assert_eq!(
            oracle_backoff(min, Duration::from_secs(8)),
            Duration::from_secs(16)
        );
    }

    #[test]
    fn tallies_merge_counts_and_latencies() {
        let mut a = WriterTally {
            issued: [7, 1, 1, 1],
            ..Default::default()
        };
        a.latency_all.record(Duration::from_millis(2));
        let mut b = WriterTally {
            issued: [70, 15, 10, 5],
            noop: 2,
            ..Default::default()
        };
        b.latency_build.record(Duration::from_millis(4));
        a.merge(&b);
        assert_eq!(a.issued, [77, 16, 11, 6]);
        assert_eq!(a.total(), 110);
        assert_eq!(a.noop, 2);
        assert_eq!(a.latency_all.count(), 1);
        assert_eq!(a.latency_build.count(), 1);
    }
}
