//! `join`'s build-from-seed mode: seed the tables server-side,
//! then time what it costs to *build* each target over them, rather than
//! measuring only the pages that follow.
//!
//! # Seeding (`--load direct`)
//!
//! [`seed_direct`] creates `a`, `b` (and B0's `a0`) without their primary keys
//! and secondary indexes, and fills them with set-based `INSERT ... SELECT`
//! statements over `generate_series` id ranges on `--loaders` connections:
//! nothing is captured and nothing enters the ring, so the load costs the
//! server's insert rate and not the engine's drain rate (a 20M load
//! through capture did not converge in 1,800 s). Then the keys and indexes are
//! built (one connection per index), the tables are `VACUUM (ANALYZE)`d and
//! the cluster checkpointed: J2's recipe, so a build starts from settled
//! heaps and a flushed WAL. Every value is a hash of the row's id, so this
//! loads exactly the rows the capture path (`--load capture`) loads, and the
//! two paths' targets can be compared by [`BuildMeasure`]'s digests.
//!
//! # What a build reports ([`BuildMeasure`])
//!
//! One line per target (`"kind":"build"`), from the `install` of its
//! definition (or the request that rebuilds it) until it reads `live`:
//!
//! - `define_to_live_s`: includes up to one reconcile tick of padding (the
//!   plan job starts on a reconcile pass), which is why `join` runs at a
//!   250 ms reconcile interval unless `--reconcile-interval-ms` says
//!   otherwise;
//! - `longest_xact_s` and `xacts_s`: the longest transactions any other
//!   backend held open during the build, sampled every [`SAMPLE`]. The
//!   build's own transaction is the longest one (a partitioned side's
//!   per-leaf steps show up as several);
//! - `xmin_hold_s` / `xmin_age_max`: how long a snapshot-holding backend was
//!   open, and the oldest `age(backend_xmin)` seen, in transactions;
//! - `wal_bytes`, `pg_wal_peak_bytes`, `temp_bytes`, `state_bytes`.
//!
//! # Rebuilds (`--rebuild`)
//!
//! After the fresh builds and their oracle check, each captured source table
//! is rebuilt in place with `request_backfill` (`a` and `b` for a `JOIN`
//! variant, `a0` for B0), then each target is `PAUSE`d and `RESUME`d. Each is
//! timed like a fresh build, and the oracle runs again afterwards. A
//! partitioned side (B3) adds a `--partition` layout flag and a per-leaf row
//! per step; the report's `build` and `target` fields already name what was
//! rebuilt.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use testkit::TestDatabase;
use tokio_postgres::Client as RawClient;

use super::{JoinBench, PAD, Shape, Variant, VariantDb, fact_rows};
use crate::scenario::connect_raw;
use crate::streaming::chain::wait_for_live;
use crate::streaming::idle_cost::{wal_bytes_since, wal_lsn};

/// How the tables are filled before the definitions go in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    /// Server-side `INSERT ... SELECT`, before the definitions: nothing is
    /// captured, and each target is built over the loaded rows.
    Direct,
    /// Definitions over empty tables, then `--load-chunk`-row transactions
    /// through capture, the ring and the drain.
    Capture,
}

impl LoadMode {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "direct" => LoadMode::Direct,
            "capture" => LoadMode::Capture,
            other => panic!("unknown load mode {other:?} (direct, capture)"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            LoadMode::Direct => "direct",
            LoadMode::Capture => "capture",
        }
    }
}

/// How often a build is sampled.
const SAMPLE: Duration = Duration::from_millis(100);
/// Postgres flushes a backend's statistics about once a second; wait this
/// long after a build before reading `temp_bytes`.
const STATS_FLUSH: Duration = Duration::from_millis(1_500);

/// Fact ids `1..=facts` as `(lo, hi)` ranges of at most `chunk` ids.
pub(super) fn chunk_ranges(facts: i64, chunk: i64) -> Vec<(i64, i64)> {
    let chunk = chunk.max(1);
    let mut out = Vec::new();
    let mut lo = 1;
    while lo <= facts {
        let hi = (lo + chunk - 1).min(facts);
        out.push((lo, hi));
        lo = hi + 1;
    }
    out
}

/// What [`seed_direct`] did.
#[derive(Debug, Clone)]
pub(super) struct SeedStats {
    pub load_s: f64,
    pub index_s: f64,
    pub vacuum_s: f64,
    pub checkpoint_s: f64,
    /// `(table, total bytes with indexes)`.
    pub sizes: Vec<(&'static str, i64)>,
}

impl SeedStats {
    pub fn total_s(&self) -> f64 {
        self.load_s + self.index_s + self.vacuum_s + self.checkpoint_s
    }

    pub fn sizes_json(&self) -> String {
        let parts: Vec<String> = self
            .sizes
            .iter()
            .map(|(t, b)| format!("\"{t}\":{b}"))
            .collect();
        format!("{{{}}}", parts.join(","))
    }
}

/// The indexes (and keys) built after the load, each on a connection of its
/// own. A key is built as a unique index and then attached: `ADD PRIMARY KEY`
/// would take an `ACCESS EXCLUSIVE` lock for its whole build and serialize the
/// table's other index builds behind it.
fn index_statements(variant: Variant) -> Vec<&'static str> {
    let mut out = vec![
        "create unique index a_pkey on public.a (id); \
         alter table public.a add constraint a_pkey primary key using index a_pkey",
        "create index on public.a (k)",
    ];
    if variant == Variant::B0 {
        out.extend([
            "create unique index a0_pkey on public.a0 (id); \
             alter table public.a0 add constraint a0_pkey primary key using index a0_pkey",
            "create index on public.a0 (k)",
            "create index on public.a0 (gb)",
            "create index on public.a0 (day, gb)",
        ]);
    }
    out
}

/// Creates and fills `variant`'s tables server-side; see the module doc.
pub(super) async fn seed_direct(
    db: &TestDatabase,
    raw: &RawClient,
    variant: Variant,
    cfg: &JoinBench,
    shape: &Shape,
) -> SeedStats {
    let started = Instant::now();
    raw.batch_execute(
        "create table public.b (id bigint, k int not null, gb int, y bigint); \
         create table public.a (id bigint not null, day int not null, k int not null, \
             x bigint, pad text)",
    )
    .await
    .expect("create the tables");
    if variant == Variant::B0 {
        raw.batch_execute(
            "create table public.a0 (id bigint not null, day int not null, k int not null, \
                 x bigint, pad text, gb int, y bigint)",
        )
        .await
        .expect("create a0");
    }
    // `b` first, with its keys: B0's `a0` rows read `gb` and `y` from it.
    raw.batch_execute(&format!(
        "insert into public.b (id, k, gb, y) \
         select k, k, mod(hashint8extended(k, 4) & 9223372036854775807, 500)::int, \
                case when mod(k, 100) = 0 then null \
                     else mod(hashint8extended(k, 5) & 9223372036854775807, 1000) end \
         from generate_series(0::bigint, {}::bigint) k; \
         alter table public.b add primary key (id); \
         alter table public.b add unique (k)",
        shape.keys - 1
    ))
    .await
    .expect("load b");
    raw.batch_execute("analyze public.b")
        .await
        .expect("analyze b");

    let ranges = chunk_ranges(cfg.facts, cfg.load_chunk);
    let loaders = cfg.loaders.max(1);
    let mut set = tokio::task::JoinSet::new();
    for i in 0..loaders {
        let mine: Vec<(i64, i64)> = ranges.iter().copied().skip(i).step_by(loaders).collect();
        if mine.is_empty() {
            continue;
        }
        let conn = connect_raw(db.dsn()).await;
        let all_keys = shape.all_keys;
        set.spawn(async move {
            for (lo, hi) in mine {
                let rows = fact_rows(lo, hi, all_keys);
                let mut sql = format!(
                    "insert into public.a (id, day, k, x, pad) \
                     select id, day, k, x, '{PAD}' from ({rows}) s"
                );
                if variant == Variant::B0 {
                    sql.push_str(&format!(
                        "; insert into public.a0 (id, day, k, x, pad, gb, y) \
                         select f.id, f.day, f.k, f.x, '{PAD}', b.gb, b.y \
                         from ({rows}) f left join public.b on b.k = f.k"
                    ));
                }
                conn.batch_execute(&sql).await.expect("load facts");
            }
        });
    }
    while let Some(done) = set.join_next().await {
        done.expect("a loader panicked");
    }
    let load_s = started.elapsed().as_secs_f64();

    let t = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for sql in index_statements(variant) {
        let conn = connect_raw(db.dsn()).await;
        set.spawn(async move {
            conn.batch_execute(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        });
    }
    while let Some(done) = set.join_next().await {
        done.expect("an index build panicked");
    }
    let index_s = t.elapsed().as_secs_f64();

    let t = Instant::now();
    raw.batch_execute("vacuum (analyze) public.a, public.b")
        .await
        .expect("vacuum analyze");
    if variant == Variant::B0 {
        raw.batch_execute("vacuum (analyze) public.a0")
            .await
            .expect("vacuum analyze a0");
    }
    let vacuum_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    raw.batch_execute("checkpoint").await.expect("checkpoint");
    let checkpoint_s = t.elapsed().as_secs_f64();

    let mut sizes = Vec::new();
    for table in ["a", "b", "a0"] {
        if table == "a0" && variant != Variant::B0 {
            continue;
        }
        let bytes: i64 = raw
            .query_one(
                &format!("select pg_total_relation_size('public.{table}')::bigint"),
                &[],
            )
            .await
            .expect("size")
            .get(0);
        sizes.push((table, bytes));
    }
    SeedStats {
        load_s,
        index_s,
        vacuum_s,
        checkpoint_s,
        sizes,
    }
}

/// What the sampler saw.
#[derive(Debug, Default)]
struct Sampled {
    /// `(pid, xact_start)` to the longest age seen.
    xacts: HashMap<(i32, String), f64>,
    xmin_hold_s: f64,
    xmin_age_max: i64,
    wal_dir_peak: i64,
}

/// Samples the server while a build runs, from a connection of its own.
pub(super) struct BuildProbe {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Sampled>,
    wal0: String,
    temp0: i64,
    started: Instant,
}

async fn temp_bytes(raw: &RawClient) -> i64 {
    raw.query_one(
        "select coalesce(temp_bytes, 0)::bigint from pg_stat_database \
         where datname = current_database()",
        &[],
    )
    .await
    .expect("read temp_bytes")
    .get(0)
}

impl BuildProbe {
    pub async fn start(db: &TestDatabase, raw: &RawClient) -> Self {
        let sampler = connect_raw(db.dsn()).await;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let task = tokio::spawn(async move {
            let mut seen = Sampled::default();
            while !flag.load(Ordering::Relaxed) {
                let rows = sampler
                    .query(
                        "select pid, xact_start::text, \
                                extract(epoch from now() - xact_start)::float8, \
                                backend_xmin is not null, coalesce(age(backend_xmin), 0)::bigint \
                         from pg_stat_activity \
                         where datname = current_database() and pid <> pg_backend_pid() \
                           and backend_type = 'client backend' and xact_start is not null \
                           and state <> 'idle'",
                        &[],
                    )
                    .await
                    .expect("sample pg_stat_activity");
                for row in rows {
                    let age: f64 = row.get(2);
                    let slot = seen.xacts.entry((row.get(0), row.get(1))).or_insert(0.0);
                    *slot = slot.max(age);
                    if row.get::<_, bool>(3) {
                        seen.xmin_hold_s = seen.xmin_hold_s.max(age);
                        seen.xmin_age_max = seen.xmin_age_max.max(row.get(4));
                    }
                }
                let wal: i64 = sampler
                    .query_one(
                        "select coalesce(sum(size), 0)::bigint from pg_ls_waldir()",
                        &[],
                    )
                    .await
                    .expect("list pg_wal")
                    .get(0);
                seen.wal_dir_peak = seen.wal_dir_peak.max(wal);
                tokio::time::sleep(SAMPLE).await;
            }
            seen
        });
        Self {
            stop,
            task,
            wal0: wal_lsn(raw).await,
            temp0: temp_bytes(raw).await,
            started: Instant::now(),
        }
    }

    /// Stops sampling; `what` and `target` name the build.
    pub async fn finish(self, raw: &RawClient, what: &str, target: &str) -> BuildMeasure {
        let define_to_live_s = self.started.elapsed().as_secs_f64();
        let wal_bytes = wal_bytes_since(raw, &self.wal0).await;
        self.stop.store(true, Ordering::Relaxed);
        let seen = self.task.await.expect("the sampler panicked");
        tokio::time::sleep(STATS_FLUSH).await;
        let temp = temp_bytes(raw).await - self.temp0;
        // Transactions shorter than this are the engine's ordinary work.
        let mut xacts: Vec<f64> = seen.xacts.into_values().filter(|s| *s >= 0.25).collect();
        xacts.sort_by(|a, b| b.total_cmp(a));
        BuildMeasure {
            what: what.to_string(),
            target: target.to_string(),
            define_to_live_s,
            xacts_s: xacts,
            xmin_hold_s: seen.xmin_hold_s,
            xmin_age_max: seen.xmin_age_max,
            wal_bytes,
            pg_wal_peak_bytes: seen.wal_dir_peak,
            temp_bytes: temp,
            state_bytes: 0,
        }
    }
}

/// One timed build.
#[derive(Debug, Clone)]
pub(super) struct BuildMeasure {
    /// `fresh`, `rebuild <table>` or `pause_resume`.
    what: String,
    target: String,
    pub define_to_live_s: f64,
    /// Transactions of 250 ms or more, longest first.
    xacts_s: Vec<f64>,
    xmin_hold_s: f64,
    xmin_age_max: i64,
    wal_bytes: i64,
    pg_wal_peak_bytes: i64,
    temp_bytes: i64,
    pub state_bytes: i64,
}

impl BuildMeasure {
    pub fn to_json(&self, dataset: &str, variant: &str, load: LoadMode, common: &str) -> String {
        let xacts: Vec<String> = self
            .xacts_s
            .iter()
            .take(8)
            .map(|s| format!("{s:.2}"))
            .collect();
        format!(
            "{{\"scenario\":\"join\",\"dataset\":\"{dataset}\",\"variant\":\"{variant}\",\
             \"kind\":\"build\",\"build\":\"{}\",\"target\":\"{}\",\"load\":\"{}\",\
             \"define_to_live_s\":{:.3},\"longest_xact_s\":{:.2},\"xacts_s\":[{}],\
             \"xmin_hold_s\":{:.2},\"xmin_age_max\":{},\"wal_bytes\":{},\
             \"pg_wal_peak_bytes\":{},\"temp_bytes\":{},\"state_bytes\":{},{common}}}",
            self.what,
            self.target,
            load.name(),
            self.define_to_live_s,
            self.xacts_s.first().copied().unwrap_or(0.0),
            xacts.join(","),
            self.xmin_hold_s,
            self.xmin_age_max,
            self.wal_bytes,
            self.pg_wal_peak_bytes,
            self.temp_bytes,
            self.state_bytes,
        )
    }
}

impl VariantDb {
    /// The tables a rebuild in place can be requested of: those the
    /// variant's definitions read.
    fn rebuild_sources(&self) -> &'static [&'static str] {
        match self.variant {
            Variant::S1 | Variant::S2 => &["a", "b"],
            Variant::B0 => &["a0"],
        }
    }

    /// Each target's md5 over its rows: equal across `--load` modes.
    pub async fn digests(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for target in &self.targets {
            let digest: Option<String> = self
                .raw
                .query_one(
                    &format!(
                        "select md5(string_agg(t::text, E'\\n' order by t::text)) \
                         from public.{target} t"
                    ),
                    &[],
                )
                .await
                .expect("digest a target")
                .get(0);
            out.push((target.clone(), digest.unwrap_or_default()));
        }
        out
    }

    /// Rebuilds in place: every source table by `request_backfill`, then
    /// every target by `PAUSE` and `RESUME`. A request that took no rebuild
    /// is pushed to `failures`: its time would be garbage.
    pub async fn rebuild_all(
        &self,
        cfg: &JoinBench,
        failures: &mut Vec<String>,
    ) -> Vec<BuildMeasure> {
        let trellis = trellis::Trellis::connect(
            trellis::Config::from_dsn(self.db.dsn().to_string()).expect("dsn"),
            trellis::TrellisOptions::default(),
        )
        .await
        .expect("connect a Trellis");
        let all = self.targets.join(",");
        let mut out = Vec::new();
        for table in self.rebuild_sources() {
            self.settle(cfg, &format!("rebuild {table}")).await;
            let before = self.definition_versions().await;
            let probe = BuildProbe::start(&self.db, &self.raw).await;
            trellis
                .request_backfill(&format!("public.{table}"))
                .await
                .unwrap_or_else(|e| panic!("request a rebuild of {table}: {e:?}"));
            if !self.wait_rebuilt(&before, cfg.build_timeout).await {
                failures.push(format!(
                    "{}: the request to rebuild {table} took no rebuild",
                    self.variant.name()
                ));
            }
            let mut m = probe
                .finish(&self.raw, &format!("rebuild {table}"), &all)
                .await;
            m.state_bytes = self.state_bytes().await;
            out.push(m);
        }
        for target in &self.targets {
            self.settle(cfg, &format!("pause_resume {target}")).await;
            trellis
                .apply(&format!("PAUSE TRANSFORM {target}"))
                .await
                .unwrap_or_else(|e| panic!("pause {target}: {e:?}"));
            let probe = BuildProbe::start(&self.db, &self.raw).await;
            trellis
                .apply(&format!("RESUME TRANSFORM {target}"))
                .await
                .unwrap_or_else(|e| panic!("resume {target}: {e:?}"));
            wait_for_live(&self.raw, target, Instant::now() + cfg.build_timeout).await;
            let mut m = probe.finish(&self.raw, "pause_resume", target).await;
            m.state_bytes = self.state_bytes().await;
            out.push(m);
        }
        out
    }

    /// Waits until no build is running and the ring has drained, so work
    /// left over from the previous step isn't charged to the next timed step.
    /// Prints how long that took as a `settle` line.
    async fn settle(&self, cfg: &JoinBench, before: &str) {
        let started = Instant::now();
        let deadline = started + cfg.build_timeout;
        loop {
            let pending: bool = self
                .raw
                .query_one(
                    "select exists (select 1 from transform_definitions \
                                    where status in ('waiting_to_backfill', 'backfilling'))",
                    &[],
                )
                .await
                .expect("read the running builds")
                .get(0);
            if !pending {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "a build stayed unfinished before {before}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        super::timed_converge(&self.raw, deadline - Instant::now(), before).await;
        println!(
            "{{\"scenario\":\"join\",\"variant\":\"{}\",\"kind\":\"settle\",\
             \"before\":\"{before}\",\"settle_s\":{:.3}}}",
            self.variant.name(),
            started.elapsed().as_secs_f64()
        );
    }

    /// Each definition's row version (`xmin`): a request that takes a
    /// rebuild rewrites the row (`status` to `backfilling`, its build
    /// segment), and so does the build going `live` again.
    async fn definition_versions(&self) -> Vec<(i64, String)> {
        self.raw
            .query(
                "select id, xmin::text from transform_definitions order by id",
                &[],
            )
            .await
            .expect("read definition versions")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    /// After a rebuild was requested: whether it took one (some definition's
    /// row changed since `before`, read before the request), then waits for
    /// every target to be `live` again. Comparing row versions rather than
    /// polling for a status other than `live` can't miss a rebuild that
    /// finished before the first poll.
    async fn wait_rebuilt(&self, before: &[(i64, String)], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let took = self.definition_versions().await != before;
        for target in &self.targets {
            wait_for_live(&self.raw, target, deadline).await;
        }
        took
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_ranges_cover_every_id_once() {
        for (facts, chunk) in [(10, 3), (9, 3), (1, 100), (0, 5), (100_000, 7_919)] {
            let ranges = chunk_ranges(facts, chunk);
            let mut next = 1;
            for (lo, hi) in &ranges {
                assert_eq!(*lo, next, "ranges are contiguous");
                assert!(hi >= lo && hi - lo < chunk);
                next = hi + 1;
            }
            assert_eq!(next, facts + 1, "facts={facts} chunk={chunk}");
        }
        assert_eq!(
            chunk_ranges(5, 0),
            chunk_ranges(5, 1),
            "a zero chunk is one"
        );
    }

    #[test]
    fn load_modes_parse_and_name_round_trip() {
        for mode in [LoadMode::Direct, LoadMode::Capture] {
            assert_eq!(LoadMode::parse(mode.name()), mode);
        }
        assert_eq!(LoadMode::parse(" Direct "), LoadMode::Direct);
    }

    #[test]
    #[should_panic(expected = "unknown load mode")]
    fn an_unknown_load_mode_is_refused() {
        LoadMode::parse("bulk");
    }

    #[test]
    fn b0_indexes_a0_and_the_others_do_not() {
        assert!(
            index_statements(Variant::S1)
                .iter()
                .all(|s| !s.contains("a0"))
        );
        assert_eq!(index_statements(Variant::B0).len(), 6);
    }
}
