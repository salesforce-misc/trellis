//! The scheduled Re-derive build (#625 F2/F3, epic #556; ADR-0002 "A build
//! is Re-derive over chunks, and applies from its first chunk"): the only
//! build of a plain invertible aggregate since F3.
//!
//! Every test steps the engine by hand: the staging worker's reconcile pass
//! (`trellis::client::reconcile_pass`), a drain worker's build step
//! (`trellis::staging::build::work_once`), and the ring's seal and drain. No
//! background worker runs and nothing waits for convergence (#297); a loop
//! here runs a bounded number of deterministic steps.

use std::collections::BTreeSet;
use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue::{self, ChunkWork};
use trellis::staging::build::{self, BuildPlan, Step, WorkerOptions};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments};
use trellis::{Trellis, TrellisOptions};

const WAKE: &str = "rederive_wake";

/// `SUM` and `COUNT(*)` by `g`: a shape the Re-derive build takes.
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

/// A plain 1-1 target: the Re-derive build takes it since #625 F8a.
const ONE: &str = "TRANSFORM one FROM public.src SELECT g AS g, v + v AS dbl";

const ONE_ACTUAL: &str = "select (id, g, dbl)::text from public.one order by id";
const ONE_EXPECTED: &str = "select (id, g, v + v)::text from public.src order by id";

const AGG_ACTUAL: &str = "select (g, total, n)::text from public.agg order by g";
const AGG_EXPECTED: &str =
    "select (g, sum(v), count(*))::text from public.src group by g order by g";

/// Ten rows per chunk, so a few hundred rows make a few dozen chunks.
const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 10,
    drain_batch_cap: 100_000,
    heartbeat_interval: Duration::from_secs(1),
    reclaim_ttl: Duration::from_secs(30),
};

/// The most steps [`Fixture::run`] takes before declaring the build stuck.
const MAX_STEPS: usize = 2_000;

struct Fixture {
    db: TestDatabase,
    raw: Client,
    _cluster: TestCluster,
}

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

impl Fixture {
    /// `public.src (id, g, v)` with `rows` rows (`g` = `id % 7`, `v` =
    /// `id`), and `definitions` registered over it (`waiting_to_backfill`).
    /// Nothing is captured yet.
    async fn new(rows: i64, definitions: &[&str]) -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect(db.dsn()).await;
        raw.batch_execute(&format!(
            "create table public.src (id bigint primary key, g integer, v bigint); \
             insert into public.src select i, i % 7, i from generate_series(1, {rows}) i"
        ))
        .await
        .expect("seed the source");
        // The source's own types: a resume re-types every column Trellis
        // created whose type differs from the one define gives it from
        // the live schema.
        let columns = [
            (
                "id".to_string(),
                ValueType::Integer(trellis::integer::IntWidth::Int8),
            ),
            (
                "g".to_string(),
                ValueType::Integer(trellis::integer::IntWidth::Int4),
            ),
            (
                "v".to_string(),
                ValueType::Integer(trellis::integer::IntWidth::Int8),
            ),
        ]
        .into_iter()
        .collect();
        for definition in definitions {
            trellis::defs::install_definition(&db.pool, definition, &columns, "public")
                .await
                .expect("register the definition");
        }
        Self {
            db,
            raw,
            _cluster: cluster,
        }
    }

    /// One staging-worker reconcile pass: installs the source's capture,
    /// starts every qualifying definition, and discharges the markers.
    async fn pass(&mut self) {
        trellis::client::reconcile_pass(
            &mut self.raw,
            &self.db.pool,
            DEFAULT_SCHEMA,
            WAKE,
            Duration::from_secs(5),
        )
        .await
        .expect("reconcile pass");
    }

    /// One drain worker's build step.
    async fn step(&self, options: &WorkerOptions) -> Step {
        build::work_once(
            &self.db.pool,
            "worker",
            options,
            &mut build::MergeFailures::default(),
        )
        .await
        .expect("build step")
    }

    /// Claims and runs up to `max` plan jobs and chunks by hand, with no
    /// merge between them. Returns how many it ran.
    async fn run_chunks_by_hand(&self, max: usize) -> usize {
        let pool = &self.db.pool;
        for ran in 0..max {
            let claimed = {
                let client = pool.get().await.expect("pool");
                chunk_queue::claim_chunks_of(
                    &**client,
                    "hand",
                    1,
                    &[chunk_queue::KIND_PLAN, chunk_queue::KIND_REDERIVE],
                )
                .await
                .expect("claim")
            };
            let Some(chunk) = claimed.into_iter().next() else {
                return ran;
            };
            build::run_claimed(pool, &chunk, "hand", &OPTIONS).await;
        }
        max
    }

    /// The one definition's id.
    async fn definition_id(&self) -> i64 {
        self.raw
            .query_one("select id from transform_definitions", &[])
            .await
            .expect("definition id")
            .get(0)
    }

    /// Seals and drains the ring until nothing is pending.
    async fn drain(&mut self) {
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next claimable segment")
            {
                apply::drain_once(&self.db.pool, seg, "drainer", 1, WAKE, &watermark)
                    .await
                    .expect("drain_once");
            }
            retire_drained_segments(&mut self.raw)
                .await
                .expect("retire drained segments");
            if !has_pending(&self.raw).await.expect("has_pending") {
                return;
            }
        }
        panic!("the ring did not quiesce within 16 seal/drain rounds");
    }

    /// Runs build steps, draining the ring before each, until one does no
    /// work. Returns every step and every status `target` read after one.
    async fn run(&mut self, target: &str) -> (Vec<Step>, BTreeSet<String>) {
        let mut steps = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..MAX_STEPS {
            self.drain().await;
            let step = self.step(&OPTIONS).await;
            steps.push(step);
            if let Some(status) = self.status(target).await {
                seen.insert(status);
            }
            if !step.progressed() {
                return (steps, seen);
            }
        }
        panic!("the build did not finish within {MAX_STEPS} steps");
    }

    /// `target`'s stored status, or `None` once it is dropped.
    async fn status(&self, target: &str) -> Option<String> {
        self.raw
            .query_opt(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("read the status")
            .map(|row| row.get(0))
    }

    /// `target`'s `build` column.
    async fn build(&self, target: &str) -> Option<String> {
        self.raw
            .query_one(
                "select build from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("read the build")
            .get(0)
    }

    async fn rows(&self, sql: &str) -> Vec<String> {
        self.raw
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    }

    async fn count(&self, sql: &str) -> i64 {
        self.raw
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get(0)
    }

    /// Merges every one of `plan`'s delta rows by hand, a committed pass per
    /// merge partition (#717), until a pass merges nothing.
    async fn merge_all(&self, plan: &BuildPlan) {
        loop {
            let mut client = self.db.pool.get().await.expect("pool");
            let txn = client.transaction().await.expect("begin");
            let outcome = build::merge_deltas(&txn, plan, 10_000)
                .await
                .expect("merge");
            txn.commit().await.expect("commit the merge");
            assert!(!outcome.skipped, "no other merger runs: {outcome:?}");
            if outcome.claimed == 0 {
                return;
            }
        }
    }

    /// The merge partitions `agg`'s delta rows are in (#717).
    async fn partitions(&self) -> i64 {
        self.count("select count(distinct __part) from public.agg__deltas")
            .await
    }

    async fn assert_one_oracle(&self) {
        assert_eq!(
            self.rows(ONE_ACTUAL).await,
            self.rows(ONE_EXPECTED).await,
            "the 1-1 target equals its source"
        );
    }

    async fn assert_agg_oracle(&self) {
        assert_eq!(
            self.rows(AGG_ACTUAL).await,
            self.rows(AGG_EXPECTED).await,
            "the target equals a from-scratch GROUP BY over the source"
        );
    }

    /// A facade connection for `PAUSE`/`DROP`.
    async fn trellis(&self) -> Trellis {
        Trellis::connect(
            Config::from_dsn(self.db.dsn().to_string()).expect("valid dsn"),
            TrellisOptions::default(),
        )
        .await
        .expect("connect a define-only Trellis")
    }
}

/// The build's lifecycle is `waiting_to_backfill -> backfilling -> live`,
/// with no `catching_up` and no marker, and the target ends equal to the
/// oracle.
#[tokio::test]
async fn a_rederive_build_goes_from_waiting_to_live_with_no_catch_up() {
    let mut f = Fixture::new(200, &[AGG]).await;
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("waiting_to_backfill")
    );

    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'plan'")
            .await,
        1,
        "the start enqueued the plan job"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'sweep'")
            .await,
        0,
        "a fresh build's ledger is empty at its start, so it gets no sweep"
    );
    assert_eq!(
        f.count("select count(*) from pending_backfill").await,
        0,
        "no marker is parked for a re-derive build (the install's join marker discharged)"
    );

    let (steps, seen) = f.run("agg").await;
    assert_eq!(steps.first(), Some(&Step::Planned));
    assert!(steps.contains(&Step::Chunk) && steps.contains(&Step::Merged));
    assert_eq!(
        seen,
        BTreeSet::from(["backfilling".to_string(), "live".to_string()]),
        "the build never passes through catching_up"
    );
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(
        f.build("agg").await,
        None,
        "a finished build clears `build`"
    );
    assert_eq!(
        steps.iter().filter(|step| **step == Step::Chunk).count(),
        20,
        "200 rows in chunks of 10"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks").await,
        0,
        "the flip deleted the build's rows (#966)"
    );
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);
    f.assert_agg_oracle().await;
}

/// A started definition applies at once (B1): a write drained before any
/// chunk runs reaches the target, and every kind of write made while the
/// build runs ends up counted exactly once.
#[tokio::test]
async fn changes_drained_while_backfilling_are_applied() {
    let mut f = Fixture::new(200, &[AGG]).await;
    f.pass().await;

    // A new group, before any chunk: only a page can put it there (a
    // Re-derive, its batch being the start's own segment, #733).
    f.raw
        .batch_execute("insert into public.src values (1000, 99, 5)")
        .await
        .expect("insert");
    f.drain().await;
    assert_eq!(
        f.rows("select (g, total, n)::text from public.agg where g = 99")
            .await,
        vec!["(99,5,1)".to_string()],
        "the change was applied while backfilling"
    );

    // Writes interleaved with the build: updates, group moves, deletes and
    // inserts, on keys whose chunks have and haven't run.
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);
    for round in 0..6 {
        let base = round * 30;
        f.raw
            .batch_execute(&format!(
                "update public.src set v = v + 100 where id in ({}, {}); \
                 update public.src set g = (g + 3) % 7 where id = {}; \
                 delete from public.src where id = {}; \
                 insert into public.src values ({}, {}, 7)",
                base + 1,
                base + 25,
                base + 12,
                base + 18,
                2000 + round,
                round % 7,
            ))
            .await
            .expect("write");
        f.drain().await;
        for _ in 0..3 {
            f.step(&OPTIONS).await;
        }
    }
    let (_, seen) = f.run("agg").await;
    assert!(!seen.contains("catching_up"));
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// `SUM`, `MAX` (a recomputed field that folds), `AVG` and `COUNT(*)` by `g`.
const AGG_WIDE: &str = "TRANSFORM agg FROM public.src GROUP BY g \
     SELECT SUM(v) AS total, MAX(v) AS biggest, AVG(v) AS mean, COUNT(*) AS n";

/// The read-your-writes contract over a build (#728; ADR-0002, "What `live`
/// promises"): a workload runs while the target builds, and the moment the
/// definition reports `live`, with no step past the flip, a watermark token
/// taken after a later write is already met and the target equals a
/// from-scratch `GROUP BY`.
///
/// The workload covers what the facade-level oracle tests in
/// `defs_exact_integers.rs` and `defs_floats.rs` raced against the build
/// before #728: group moves, the `NULL` group (entered by Apply before any
/// chunk and by chunks after), a group emptied mid-build across chunked and
/// unchunked keys, a group created and emptied within the build, and groups
/// losing their `MAX`. Each round's write is read by the next chunks before
/// it drains, and by later chunks after it drains.
///
/// Group 50 loses its `MAX` through the merger alone: Apply counts keys 7
/// (v 10000) and 8 into it before the plan, and the first chunk then reads
/// key 7 lowered to 1 before that change drains, so only its delta row says
/// a value left the group. The merge must recompute the group rather than
/// fold, and no later page writes it to cover for a fold.
#[tokio::test]
async fn a_target_reporting_live_meets_a_token_and_the_oracle_after_a_build_under_writes() {
    let mut f = Fixture::new(200, &[AGG_WIDE]).await;
    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));

    // Before any chunk, by Apply alone: two keys into the `NULL` group, a
    // new group 99, and keys 7 and 8 into group 50 with 7 its `MAX`.
    f.raw
        .batch_execute(
            "update public.src set g = null where id in (2, 150); \
             insert into public.src values (5000, 99, 5); \
             update public.src set g = 50, v = 10000 where id = 7; \
             update public.src set g = 50 where id = 8",
        )
        .await
        .expect("write before the plan");
    f.drain().await;
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);

    // Group 50's `MAX` leaves through the first chunk (keys 1 to 10), which
    // reads the change before it drains.
    f.raw
        .batch_execute("update public.src set v = 1 where id = 7")
        .await
        .expect("lower group 50's MAX");
    assert_eq!(f.step(&OPTIONS).await, Step::Chunk);
    assert_eq!(
        f.count("select count(*) from public.agg__deltas where g = 50")
            .await,
        1,
        "the chunk's delta row for group 50"
    );
    f.drain().await;

    let mut steps = Vec::new();
    for round in 0..6 {
        let base = round * 30;
        // The moves and the `+ 1`s reach a key in every chunk, so the next
        // chunk to run reads one of each before it drains. None touches
        // group 50.
        let mut sql = format!(
            "update public.src set g = null where id = {null}; \
             update public.src set g = (g + 1) % 6 where id % 10 = {round} and g <> 50; \
             update public.src set v = v + 1 where id % 10 = {plus} and g is distinct from 50; \
             update public.src set v = v + 1000 where id = {raised}; \
             delete from public.src where id = {deleted};",
            null = base + 3,
            plus = round + 5,
            raised = base + 20,
            deleted = base + 26,
        );
        sql.push_str(match round {
            // Group 6 emptied, over keys chunked and not; nothing moves
            // into it again (`% 6`).
            2 => "delete from public.src where g = 6;",
            // Out of the `NULL` group again.
            3 => "update public.src set g = 4 where id = 2;",
            // The group created before the plan, emptied.
            4 => "delete from public.src where g = 99;",
            // Group 1 loses its `MAX`.
            5 => {
                "delete from public.src where id = \
                 (select id from public.src where g = 1 order by v desc, id limit 1);"
            }
            _ => "",
        });
        f.raw.batch_execute(&sql).await.expect("write");
        // Chunks (and merges) read the write before it drains, then more
        // after.
        for _ in 0..2 {
            steps.push(f.step(&OPTIONS).await);
        }
        f.drain().await;
        steps.push(f.step(&OPTIONS).await);
    }

    // Step only until the flip, and not one step past it.
    let mut live = false;
    for _ in 0..MAX_STEPS {
        f.drain().await;
        let step = f.step(&OPTIONS).await;
        steps.push(step);
        if f.status("agg").await.as_deref() == Some("live") {
            live = true;
            break;
        }
        assert!(
            step.progressed(),
            "the build stalled before live: {steps:?}"
        );
    }
    assert!(
        live,
        "the build did not reach live within {MAX_STEPS} steps"
    );
    assert!(
        steps.contains(&Step::Chunk) && steps.contains(&Step::Merged),
        "the build went through chunks and the merger: {steps:?}"
    );

    // Once `live`, a write's token covers it (ADR-0002): drained, a token
    // taken after it is already met, with no wait.
    f.raw
        .batch_execute(
            "update public.src set g = null where id = 199; \
             delete from public.src where id = 33",
        )
        .await
        .expect("write after live");
    f.drain().await;
    let trellis = f.trellis().await;
    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::ZERO)
        .await
        .expect("a drained ring meets the token at once");
    trellis.shutdown().await.expect("shutdown");

    assert_eq!(
        f.count("select count(*) from public.agg__deltas").await,
        0,
        "live leaves no delta behind"
    );
    let differences = f
        .count(
            "with expected as ( \
                 select g, sum(v) as total, max(v) as biggest, avg(v) as mean, count(*) as n \
                 from public.src group by g \
             ), \
             actual as (select g, total, biggest, mean, n from public.agg) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)",
        )
        .await;
    assert_eq!(differences, 0, "the live target equals the oracle");
    // The cases the workload is for are really in the outcome.
    assert_eq!(
        f.count("select count(*) from public.agg where g is null")
            .await,
        1,
        "the NULL group is there"
    );
    assert_eq!(
        f.count("select count(*) from public.agg where g in (6, 99)")
            .await,
        0,
        "the emptied groups are gone"
    );
    assert_eq!(
        f.count("select biggest::bigint from public.agg where g = 50")
            .await,
        8,
        "group 50's MAX is key 8's, after key 7 left it"
    );
}

/// The flip waits for the last merge (B7): with every chunk done but deltas
/// still pending, the definition stays `backfilling`, and the merge that
/// empties the delta table moves it to `live`.
#[tokio::test]
async fn the_flip_happens_only_after_the_last_merge() {
    let mut f = Fixture::new(60, &[AGG]).await;
    f.pass().await;
    let pool = &f.db.pool;

    // Run the plan and every chunk by hand, with no merge between them.
    f.run_chunks_by_hand(usize::MAX).await;
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done")
            .await,
        0,
        "every chunk is done"
    );
    let pending = f.count("select count(*) from public.agg__deltas").await;
    assert!(pending > 0, "the chunks left deltas for the merger");
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    let id = f.definition_id().await;
    assert!(
        !build::try_complete(pool, id).await.expect("try_complete"),
        "not live while deltas are pending"
    );
    assert_eq!(
        f.rows(AGG_ACTUAL).await,
        Vec::<String>::new(),
        "no group yet"
    );

    assert!(f.partitions().await > 1, "the deltas span partitions");
    // A step merges one partition (#717): every merge but the last leaves
    // deltas in another partition, so the definition stays `backfilling`.
    while f.partitions().await > 1 {
        assert_eq!(f.step(&OPTIONS).await, Step::Merged);
        assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    }
    assert_eq!(f.step(&OPTIONS).await, Step::Merged);
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("live"),
        "the merge that emptied the delta table flipped it"
    );
    f.assert_agg_oracle().await;
}

/// The strict flip still waits for every merge in flight (#625 F2b, B7,
/// #717): a merge that has claimed and deleted delta rows but not committed
/// leaves the table non-empty to everyone else, so neither `try_complete`
/// nor a worker's step flips the build until the last of them commits. Two
/// merges are left in flight, on the last two partitions with rows: the
/// worker skips the merge (others hold every partition with rows), finds
/// nothing else to do and reports idle without waiting on them, and the
/// flip comes only once both committed.
#[tokio::test]
async fn the_flip_waits_for_every_merge_in_flight() {
    let mut f = Fixture::new(60, &[AGG]).await;
    f.pass().await;
    let pool = &f.db.pool;
    f.run_chunks_by_hand(usize::MAX).await;
    let id = f.definition_id().await;
    let plan = BuildPlan::load(pool, "agg")
        .await
        .expect("load the plan")
        .expect("a buildable target");
    while f.partitions().await > 2 {
        let mut client = pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        build::merge_deltas(&txn, &plan, i64::MAX)
            .await
            .expect("merge a partition");
        txn.commit().await.expect("commit the merge");
    }
    assert_eq!(f.partitions().await, 2);

    let mut client_a = pool.get().await.expect("pool");
    let merge_a = client_a.transaction().await.expect("begin merge A");
    let a = build::merge_deltas(&merge_a, &plan, i64::MAX)
        .await
        .expect("merge A");
    let mut client_b = pool.get().await.expect("pool");
    let merge_b = client_b.transaction().await.expect("begin merge B");
    let b = build::merge_deltas(&merge_b, &plan, i64::MAX)
        .await
        .expect("merge B");
    assert!(a.claimed > 0 && b.claimed > 0, "{a:?} {b:?}");
    assert_ne!(
        a.partition, b.partition,
        "B takes the partition A doesn't hold"
    );

    assert!(
        !build::try_complete(pool, id).await.expect("try_complete"),
        "not live while both merges are uncommitted"
    );
    assert_eq!(
        f.step(&OPTIONS).await,
        Step::Idle,
        "a worker skips the busy partitions, and has nothing else to do"
    );
    merge_a.commit().await.expect("commit merge A");
    assert!(
        !build::try_complete(pool, id).await.expect("try_complete"),
        "not live while B is uncommitted"
    );
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));

    merge_b.commit().await.expect("commit merge B");
    assert!(
        build::try_complete(pool, id).await.expect("try_complete"),
        "the flip once both merges committed"
    );
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// A worker that finds every partition with rows being merged takes a chunk
/// instead of waiting for a merge or reporting idle (#625 F2b, #717): one
/// merger per partition, and the rest of the workers build.
#[tokio::test]
async fn a_worker_takes_a_chunk_while_others_merge() {
    let mut f = Fixture::new(60, &[AGG]).await;
    f.pass().await;
    let pool = &f.db.pool;
    // The plan job and two chunks, so every group has two delta rows, and
    // chunks are left.
    assert_eq!(f.run_chunks_by_hand(3).await, 3);
    let plan = BuildPlan::load(pool, "agg")
        .await
        .expect("load the plan")
        .expect("a buildable target");
    // A merge in flight on every partition, of one row each: its group's
    // other row is left for a second merger of the partition, which would
    // wait on this merge's group row if it took it.
    let partitions = usize::try_from(f.partitions().await).expect("a count");
    let mut clients = Vec::new();
    for _ in 0..=partitions {
        clients.push(pool.get().await.expect("pool"));
    }
    let mut merges = Vec::new();
    for client in &mut clients {
        merges.push(client.transaction().await.expect("begin a merge"));
    }
    for (i, merge) in merges.iter().enumerate() {
        let outcome = build::merge_deltas(merge, &plan, 1).await.expect("merge");
        if i < partitions {
            assert_eq!(
                (outcome.claimed, outcome.skipped),
                (1, false),
                "{outcome:?}"
            );
        } else {
            assert!(
                outcome.skipped,
                "every partition is being merged: {outcome:?}"
            );
        }
    }

    let step = tokio::time::timeout(Duration::from_secs(30), f.step(&OPTIONS))
        .await
        .expect("the step doesn't wait on the merges in flight");
    assert_eq!(step, Step::Chunk);
    for merge in merges {
        merge.commit().await.expect("commit a merge");
    }
    drop(clients);

    let (steps, _) = f.run("agg").await;
    assert!(steps.contains(&Step::Merged), "{steps:?}");
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// A source with no rows plans no chunk, and the plan job itself moves the
/// definition to `live`.
#[tokio::test]
async fn a_zero_row_source_goes_live_from_the_plan_job() {
    let mut f = Fixture::new(0, &[AGG]).await;
    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive'")
            .await,
        0
    );
    assert_eq!(f.step(&OPTIONS).await, Step::Idle);
}

/// A chunk whose worker dies mid-write (its transaction rolls back after the
/// entries and deltas were written) is reclaimed and run again, and the
/// build still equals the oracle.
#[tokio::test]
async fn a_chunk_killed_mid_write_is_reclaimed_and_rerun() {
    let mut f = Fixture::new(100, &[AGG]).await;
    f.pass().await;
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);
    let pool = &f.db.pool;

    let chunk = {
        let client = pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "doomed", 1, &[chunk_queue::KIND_REDERIVE])
            .await
            .expect("claim")
            .into_iter()
            .next()
            .expect("a chunk to claim")
    };
    let ChunkWork::Rederive { lo, hi } = &chunk.work else {
        panic!("claimed {:?}", chunk.work);
    };
    let plan = BuildPlan::load(pool, "agg")
        .await
        .expect("load")
        .expect("a re-derive shape");
    {
        let mut client = pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        let outcome = build::run_chunk(&txn, &plan, lo.as_deref(), hi)
            .await
            .expect("the chunk writes");
        assert!(outcome.keys > 0 && outcome.delta_rows > 0);
        // The worker dies here: its transaction never commits.
        txn.rollback().await.expect("roll back");
    }
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);

    let reclaimed = chunk_queue::reclaim_stale_chunks(&mut f.raw, Duration::ZERO)
        .await
        .expect("reclaim");
    assert_eq!(reclaimed, 1, "the dead worker's claim is freed");
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    // The flip deleted the build's rows (#966), so the chunk's range is
    // known to have run by the target alone: the oracle below covers it.
    assert_eq!(
        f.count("select count(*) from backfill_chunks").await,
        0,
        "another worker ran the reclaimed chunk, and the flip deleted it"
    );
    f.assert_agg_oracle().await;
}

/// A chunk whose entry lock a drain page holds gives up after its short lock
/// timeout and is retried later without a charge (B6); the build then
/// finishes.
#[tokio::test]
async fn a_chunk_behind_a_held_entry_gives_up_and_is_retried_uncharged() {
    let mut f = Fixture::new(40, &[AGG]).await;
    f.pass().await;
    // An entry for key 5, which the first chunk (ids 1..=10) covers.
    f.raw
        .batch_execute("update public.src set v = v + 1 where id = 5")
        .await
        .expect("write key 5");
    f.drain().await;
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);

    let holder = connect(f.db.dsn()).await;
    holder
        .batch_execute("begin; select 1 from public.agg__ledger where __from_key = '5' for update")
        .await
        .expect("hold key 5's entry");
    assert_eq!(f.step(&OPTIONS).await, Step::Chunk);
    let (attempts, charged, error, done): (i32, i32, Option<String>, bool) = {
        let row = f
            .raw
            .query_one(
                "select attempts, charged, last_error, done from backfill_chunks \
                 where kind = 'rederive' order by id limit 1",
                &[],
            )
            .await
            .expect("read the first chunk");
        (row.get(0), row.get(1), row.get(2), row.get(3))
    };
    assert!(!done, "the chunk gave up");
    assert_eq!((attempts, charged), (1, 0), "a lock timeout isn't charged");
    assert!(
        error.as_deref().is_some_and(|e| e.contains("lock")),
        "the failure is recorded: {error:?}"
    );

    holder.batch_execute("rollback").await.expect("release");
    f.raw
        .batch_execute("update backfill_chunks set next_attempt_at = now()")
        .await
        .expect("make the retry due");
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// A chunk is claimed only while the ring's sealed, undrained backlog is
/// under twice the drain batch cap (B6); the plan job runs regardless.
#[tokio::test]
async fn chunks_wait_while_the_ring_has_a_backlog() {
    let mut f = Fixture::new(40, &[AGG]).await;
    f.pass().await;
    f.drain().await;
    let tight = WorkerOptions {
        drain_batch_cap: 1,
        ..OPTIONS
    };
    f.raw
        .batch_execute("update public.src set v = v + 1 where id <= 5")
        .await
        .expect("write");
    trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
        .await
        .expect("seal");
    assert_eq!(f.step(&tight).await, Step::Planned, "planning isn't held");
    assert_eq!(f.step(&tight).await, Step::Backpressure);
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive' and done")
            .await,
        0
    );
    f.drain().await;
    assert_eq!(
        f.step(&tight).await,
        Step::Chunk,
        "the drained ring lets it go"
    );
    f.run("agg").await;
    f.assert_agg_oracle().await;
}

/// A source `TRUNCATE` while the build runs: its deltas and entries go with
/// the ledger, and the rest of the build and Apply rebuild the target from
/// the rows inserted after it.
#[tokio::test]
async fn a_truncate_during_the_build_is_absorbed() {
    let mut f = Fixture::new(100, &[AGG]).await;
    f.pass().await;
    for _ in 0..4 {
        f.drain().await;
        f.step(&OPTIONS).await;
    }
    assert!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive' and done")
            .await
            > 0,
        "some chunks ran before the truncate"
    );
    f.raw
        .batch_execute(
            "truncate public.src; \
             insert into public.src select i, i % 3, 2 * i from generate_series(5, 60) i",
        )
        .await
        .expect("truncate and refill");
    f.drain().await;
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// A drop during the build: pause, then drop. The chunk rows, the delta
/// table, the ledger and the target all go, and the workers find nothing to
/// do.
#[tokio::test]
async fn a_drop_during_the_build_takes_its_work_with_it() {
    let mut f = Fixture::new(100, &[AGG]).await;
    f.pass().await;
    for _ in 0..3 {
        f.drain().await;
        f.step(&OPTIONS).await;
    }
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM agg")
        .await
        .expect("pause the building transform");
    assert_eq!(
        f.step(&OPTIONS).await,
        Step::Idle,
        "a paused build gets no merge and no chunk"
    );
    trellis
        .apply("DROP TRANSFORM agg")
        .await
        .expect("drop the paused transform");
    assert_eq!(f.status("agg").await, None);
    assert_eq!(f.count("select count(*) from backfill_chunks").await, 0);
    for table in ["agg", "agg__ledger", "agg__deltas"] {
        let exists: bool = f
            .raw
            .query_one(
                "select to_regclass($1) is not null",
                &[&format!("public.{table}")],
            )
            .await
            .expect("look the table up")
            .get(0);
        assert!(!exists, "{table} is dropped");
    }
    f.raw
        .batch_execute("update public.src set v = v + 1 where id = 3")
        .await
        .expect("write after the drop");
    f.drain().await;
    assert_eq!(f.step(&OPTIONS).await, Step::Idle);
}

/// Two definitions on one source: the one a Re-derive build serves takes
/// it, the other (relationship-fed, which the Re-derive build doesn't take
/// until #625 F9) the old direct build with its go-live catch-up, and both
/// end equal to their oracles.
#[tokio::test]
async fn two_definitions_on_one_source_take_one_build_path_each() {
    const MAXES: &str = "TRANSFORM agg_max FROM public.src GROUP BY g \
         SELECT MAX(v) AS top, MAX(grp.w) AS w";
    let mut f = Fixture::new(150, &[AGG]).await;
    f.raw
        .batch_execute(
            "create table public.grps (id integer primary key, w integer); \
             insert into public.grps select i, i * 10 from generate_series(0, 6) i",
        )
        .await
        .expect("seed the relationship's to-side");
    trellis::defs::create_relationship(&f.db.pool, "RELATIONSHIP grp FROM src.g TO grps.id")
        .await
        .expect("create the to-one relationship");
    let columns = [
        (
            "id".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
        (
            "g".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int4),
        ),
        (
            "v".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(&f.db.pool, MAXES, &columns, "public")
        .await
        .expect("register the relationship-fed definition");
    // The first pass installs `grps`'s capture, whose gated marker keeps a
    // definition reading it waiting (`capture::reconcile`'s rule 3) until
    // that pass's own discharge; the second dispatches it.
    f.pass().await;
    f.pass().await;
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    assert_eq!(f.build("agg_max").await, None);
    assert_eq!(
        f.status("agg_max").await.as_deref(),
        Some("backfilling"),
        "the discharge dispatched the old build's job"
    );
    let direct = f
        .count(
            "select count(*) from backfill_chunks c join transform_definitions d \
             on d.id = c.definition_id \
             where d.target_table = 'public.agg_max' and c.kind = 'direct'",
        )
        .await;
    assert_eq!(direct, 1);

    // The old build's job, as a drain worker runs it.
    let pool = f.db.pool.clone();
    let claimed = {
        let client = pool.get().await.expect("pool");
        chunk_queue::claim_chunks(&**client, "old", 1)
            .await
            .expect("claim the direct build")
    };
    let job = claimed.into_iter().next().expect("the direct build job");
    assert_eq!(job.work, ChunkWork::DirectBuild);
    chunk_queue::run_claimed_chunk(
        &pool,
        &job,
        "old",
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .await
    .expect("run the direct build");
    chunk_queue::finish_chunk(&pool, &job, "old")
        .await
        .expect("finish the direct build");
    assert_eq!(f.status("agg_max").await.as_deref(), Some("catching_up"));

    // Interleave the re-derive build with the old build's catch-up.
    f.raw
        .batch_execute("update public.src set v = v * 3 where id % 10 = 0")
        .await
        .expect("write");
    for _ in 0..5 {
        f.drain().await;
        f.step(&OPTIONS).await;
    }
    for _ in 0..3 {
        f.pass().await;
        f.drain().await;
    }
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(f.status("agg_max").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
    assert_eq!(
        f.rows("select (g, top, w)::text from public.agg_max order by g")
            .await,
        f.rows(
            "select (s.g, max(s.v), max(p.w))::text from public.src s \
             left join public.grps p on p.id = s.g group by s.g order by s.g"
        )
        .await,
        "the old build's target equals its oracle"
    );
}

/// #986: a build's start that fails for good (here, the definition's ledger
/// table was dropped, so the start's read of it errors) pauses that
/// definition with the error as its `capture_failure`, instead of failing
/// the reconcile pass on every pass after. The definition registered after
/// it, whose start comes later in the same pass, still starts and goes live.
/// Resuming the paused definition refuses and names the repair.
#[tokio::test]
async fn a_start_that_fails_pauses_its_definition_and_the_pass_starts_the_others() {
    let mut f = Fixture::new(150, &[ONE, AGG]).await;
    f.raw
        .batch_execute("drop table public.one__ledger")
        .await
        .expect("drop the ledger of the first definition");

    // The first definition's start fails; the pass goes on to the second's.
    f.pass().await;
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));

    let trellis = f.trellis().await;
    let failure = trellis
        .status("one")
        .await
        .expect("status")
        .expect("one is registered")
        .capture_failure
        .expect("the pause carries the error");
    assert_eq!(failure.kind, trellis::CaptureFailureKind::Halt);
    assert!(
        failure.error.contains("one__ledger") && failure.error.contains("couldn't start"),
        "{}",
        failure.error
    );

    // The next pass doesn't meet it again, and the healthy build finishes.
    f.pass().await;
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));

    // The resume refuses (the ledger is gone) and says what to do.
    let refusal = trellis
        .apply("RESUME TRANSFORM one")
        .await
        .expect_err("a resume of a definition without its ledger is refused")
        .to_string();
    assert!(
        refusal.contains("DROP TRANSFORM") && refusal.contains("one__ledger"),
        "{refusal}"
    );
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));
}

/// #986: the rest of the pass runs after a start fails, not only the other
/// starts: the registration marker of a definition the Re-derive build
/// doesn't take (it reads a relationship) is parked and discharged, so that
/// definition's build is dispatched in the same pass.
#[tokio::test]
async fn a_start_that_fails_doesn_t_stop_the_pass_discharging_other_markers() {
    const MAXES: &str = "TRANSFORM agg_max FROM public.src GROUP BY g \
         SELECT MAX(v) AS top, MAX(grp.w) AS w";
    let mut f = Fixture::new(150, &[ONE]).await;
    f.raw
        .batch_execute(
            "create table public.grps (id integer primary key, w integer); \
             insert into public.grps select i, i * 10 from generate_series(0, 6) i",
        )
        .await
        .expect("seed the relationship's to-side");
    trellis::defs::create_relationship(&f.db.pool, "RELATIONSHIP grp FROM src.g TO grps.id")
        .await
        .expect("create the to-one relationship");
    let columns = [
        (
            "id".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
        (
            "g".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int4),
        ),
        (
            "v".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(&f.db.pool, MAXES, &columns, "public")
        .await
        .expect("register the relationship-fed definition");
    f.raw
        .batch_execute("drop table public.one__ledger")
        .await
        .expect("drop the ledger of the first definition");

    // The first pass installs `grps`'s capture, which keeps `agg_max`
    // waiting until its own discharge; the second dispatches it.
    f.pass().await;
    f.pass().await;
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));
    assert_eq!(
        f.status("agg_max").await.as_deref(),
        Some("backfilling"),
        "the discharge dispatched the old build's job"
    );
}

/// #986: a start that fails transiently (here its read of the ledger waits
/// out `lock_timeout` behind another session's lock, `55P03`) is retried on
/// the next pass, not paused, and the pass still starts the other
/// definitions' builds.
#[tokio::test]
async fn a_start_that_fails_transiently_is_retried_next_pass_and_not_paused() {
    let mut f = Fixture::new(150, &[ONE, AGG]).await;
    let holder = connect(f.db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.one__ledger in access exclusive mode")
        .await
        .expect("hold the first definition's ledger");
    f.raw
        .batch_execute("set lock_timeout = '200ms'")
        .await
        .expect("make the pass's lock waits short");

    // The holder's open transaction is one the discharge's fence wait waits
    // out, so this pass gives it only a moment.
    trellis::client::reconcile_pass(
        &mut f.raw,
        &f.db.pool,
        DEFAULT_SCHEMA,
        WAKE,
        Duration::from_millis(100),
    )
    .await
    .expect("reconcile pass");
    assert_eq!(
        f.status("one").await.as_deref(),
        Some("waiting_to_backfill")
    );
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(
        f.count("select count(*) from capture_failures").await,
        0,
        "a transient failure records nothing"
    );

    holder
        .batch_execute("rollback")
        .await
        .expect("release the ledger");
    f.raw
        .batch_execute("reset lock_timeout")
        .await
        .expect("reset lock_timeout");
    f.pass().await;
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("one").await.as_deref(), Some("rederive"));
}

/// Known-gaps entry 8: a live definition whose ledger is dropped fails each
/// drain page that applies a change for it, until the keys it touches are
/// held and the definition is quarantined. The other definition on the
/// source stays live, and the quarantined one's resume refuses with the
/// repair.
#[tokio::test]
async fn a_live_definition_whose_ledger_is_dropped_is_quarantined_alone() {
    let mut f = Fixture::new(50, &[ONE, AGG]).await;
    f.pass().await;
    f.run("one").await;
    f.run("agg").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.raw
        .batch_execute(
            "drop table public.one__ledger; \
             update public.src set v = v + 1 where id <= 6",
        )
        .await
        .expect("drop the ledger and change some rows");

    // Each page fails until its keys' retries run out: a bounded number of
    // explicit drains, not a timed wait.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
            .await
            .expect("seal");
        while let Some(seg) = apply::next_claimable_segment(&f.raw)
            .await
            .expect("next claimable segment")
        {
            if apply::drain_once(&f.db.pool, seg, "drainer", 1, WAKE, &watermark)
                .await
                .is_err()
            {
                break;
            }
        }
        if f.status("one").await.as_deref() == Some("quarantined") {
            break;
        }
    }
    assert_eq!(f.status("one").await.as_deref(), Some("quarantined"));
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;

    let refusal = f
        .trellis()
        .await
        .apply("RESUME TRANSFORM one")
        .await
        .expect_err("a resume of a definition without its ledger is refused")
        .to_string();
    assert!(
        refusal.contains("DROP TRANSFORM") && refusal.contains("one__ledger"),
        "{refusal}"
    );
}

/// A worker that dies after a chunk's or a merge's commit, before its own
/// flip check, leaves a finished build `backfilling`; the next worker step
/// that finds nothing to claim makes the flip (`work_once`'s idle check).
#[tokio::test]
async fn a_flip_missed_by_a_dead_worker_is_made_by_the_next_idle_step() {
    let mut f = Fixture::new(30, &[AGG]).await;
    f.pass().await;
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);
    let pool = &f.db.pool;
    let plan = BuildPlan::load(pool, "agg")
        .await
        .expect("load")
        .expect("a re-derive shape");

    // Every chunk and the merge, each committed by a worker that dies before
    // it checks for the flip.
    loop {
        let claimed = {
            let client = pool.get().await.expect("pool");
            chunk_queue::claim_chunks_of(&**client, "dying", 1, &[chunk_queue::KIND_REDERIVE])
                .await
                .expect("claim")
        };
        let Some(chunk) = claimed.into_iter().next() else {
            break;
        };
        let ChunkWork::Rederive { lo, hi } = &chunk.work else {
            panic!("claimed {:?}", chunk.work);
        };
        let mut client = pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        build::run_chunk(&txn, &plan, lo.as_deref(), hi)
            .await
            .expect("the chunk writes");
        txn.execute(
            "update backfill_chunks set done = true, claimed_by = null where id = $1",
            &[&chunk.id],
        )
        .await
        .expect("mark the chunk done");
        txn.commit().await.expect("commit the chunk");
    }
    f.merge_all(&plan).await;
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("backfilling"),
        "nobody checked for the flip"
    );

    assert_eq!(f.step(&OPTIONS).await, Step::Completed);
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(f.build("agg").await, None);
    f.assert_agg_oracle().await;
}

/// The start waits out its source's capture gate (#625 Q2(a)): while a
/// change the old, narrower capture function staged is still pending, a
/// definition that needs the widened column stays `waiting_to_backfill`,
/// and the pass after that change drains starts it.
#[tokio::test]
async fn the_start_waits_for_the_widens_capture_gate() {
    const COUNTS: &str = "TRANSFORM cnt FROM public.src GROUP BY g SELECT COUNT(*) AS n";
    let mut f = Fixture::new(50, &[COUNTS]).await;
    f.pass().await;
    f.run("cnt").await;
    assert_eq!(f.status("cnt").await.as_deref(), Some("live"));

    // Staged by the capture function `cnt` needs, which doesn't image `v`,
    // and left in the ring. It moves `g` too: an update of only `v`, which
    // nothing reads yet, stages nothing (#623 D8a).
    f.raw
        .batch_execute("update public.src set v = v + 1000, g = g + 1 where id = 3")
        .await
        .expect("write before the widen");
    let columns = [
        (
            "id".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
        (
            "g".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int4),
        ),
        (
            "v".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(&f.db.pool, AGG, &columns, "public")
        .await
        .expect("register a definition that reads v");

    f.pass().await;
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("waiting_to_backfill"),
        "the widen's gate holds the start while the old-body row is pending"
    );
    assert_eq!(f.build("agg").await, None);
    assert_eq!(
        f.count(
            "select count(*) from backfill_chunks c join transform_definitions d \
             on d.id = c.definition_id where d.target_table = 'public.agg'"
        )
        .await,
        0,
        "no plan job yet"
    );

    f.drain().await;
    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// A definition paused part-way through its Re-derive build, with keys
/// deleted, updated and inserted while it was frozen, is rebuilt by a
/// Re-derive build over the entries it kept (#625 F3): the resume parks no
/// marker and keeps the group deltas still owed (B4), the next pass starts
/// a rebuild with a sweep, the sweep waits for every chunk, and it retires
/// the entries of the keys deleted meanwhile, which no chunk reaches.
#[tokio::test]
async fn a_resumed_build_rebuilds_over_its_entries_and_sweeps_the_deleted_keys() {
    let mut f = Fixture::new(100, &[AGG]).await;
    f.pass().await;
    // The plan job and three chunks (ids 1..=30), and no merge.
    assert_eq!(f.run_chunks_by_hand(4).await, 4);
    let owed = f.count("select count(*) from public.agg__deltas").await;
    assert!(owed > 0, "the chunks left deltas to merge");
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM agg")
        .await
        .expect("pause the building transform");
    // Ids 1..=15 have live entries, which no chunk of the rebuild reaches;
    // 95 has none yet.
    f.raw
        .batch_execute(
            "delete from public.src where id <= 15 or id = 95; \
             update public.src set v = v + 1000, g = g + 1 where id in (20, 50); \
             insert into public.src values (200, 3, 7)",
        )
        .await
        .expect("write while paused");
    f.drain().await;

    trellis.apply("RESUME TRANSFORM agg").await.expect("resume");
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("waiting_to_backfill")
    );
    assert_eq!(f.build("agg").await, None, "a resume clears `build`");
    assert_eq!(
        f.count("select count(*) from pending_backfill").await,
        0,
        "a resume of a re-derive shape parks no marker"
    );
    assert_eq!(
        f.count("select count(*) from public.agg__deltas").await,
        owed,
        "a resume keeps the deltas still owed to the groups (B4)"
    );

    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    assert_eq!(
        f.count(
            "select count(*) from backfill_chunks \
             where kind = 'sweep' and not done and start_xid is not null"
        )
        .await,
        1,
        "a rebuild over a non-empty ledger enqueues one sweep"
    );
    let early = {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "early", 1, &[chunk_queue::KIND_SWEEP])
            .await
            .expect("claim")
    };
    assert!(
        early.is_empty(),
        "the sweep isn't claimed before its plan job and chunks are done"
    );

    let (_, seen) = f.run("agg").await;
    assert_eq!(
        seen,
        BTreeSet::from(["backfilling".to_string(), "live".to_string()])
    );
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done")
            .await,
        0,
        "the sweep ran to its end"
    );
    assert_eq!(
        f.count(
            "select count(*) from public.agg__ledger \
             where __member and not __tombstone and __from_key::bigint <= 15"
        )
        .await,
        0,
        "the sweep retired the deleted keys' entries"
    );
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);
    f.assert_agg_oracle().await;
}

/// A quarantined definition resumes the same way (#625 F3): through the
/// start, with a sweep, and no marker.
#[tokio::test]
async fn a_quarantined_build_resumes_through_the_start() {
    let mut f = Fixture::new(60, &[AGG]).await;
    f.pass().await;
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    // The fuse's own write, as `quarantine_if_crossed` makes it.
    f.raw
        .batch_execute(
            "update transform_definitions set status = 'quarantined' \
             where target_table = 'public.agg'",
        )
        .await
        .expect("quarantine");
    f.raw
        .batch_execute("delete from public.src where id between 10 and 19")
        .await
        .expect("delete while quarantined");
    f.drain().await;

    f.trellis()
        .await
        .apply("RESUME TRANSFORM agg")
        .await
        .expect("resume");
    assert_eq!(f.count("select count(*) from pending_backfill").await, 0);
    f.pass().await;
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'sweep'")
            .await,
        1
    );
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// The sweep's batch (#625 F3), by hand: it reads a bounded window of
/// entries per call, re-derives the live ones whose basis is older than its
/// start, tombstones the keys whose rows are gone, and a second pass over
/// entries it already re-derived finds nothing to do.
#[tokio::test]
async fn a_sweep_batch_rederives_stale_live_entries_in_bounded_windows() {
    let mut f = Fixture::new(50, &[AGG]).await;
    f.pass().await;
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    let pool = f.db.pool.clone();
    let plan = BuildPlan::load(&pool, "agg")
        .await
        .expect("load")
        .expect("a re-derive shape");

    // Captured and left in the ring: the sweep reads the source as it is.
    f.raw
        .batch_execute("delete from public.src where id % 5 = 0")
        .await
        .expect("delete");
    let start_xid: String = f
        .raw
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("a start xid")
        .get(0);

    let sweep = |start_xid: String| {
        let pool = pool.clone();
        let plan = plan.clone();
        async move {
            let mut outcomes = Vec::new();
            let mut cursor: Option<String> = None;
            loop {
                let mut client = pool.get().await.expect("pool");
                let txn = client
                    .build_transaction()
                    .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
                    .start()
                    .await
                    .expect("begin");
                let outcome = build::sweep_batch(&txn, &plan, &start_xid, cursor.as_deref(), 7)
                    .await
                    .expect("sweep batch");
                txn.commit().await.expect("commit");
                cursor = outcome.next.clone();
                let finished = outcome.finished;
                outcomes.push(outcome);
                if finished {
                    return outcomes;
                }
            }
        }
    };
    let first = sweep(start_xid.clone()).await;
    assert!(first.iter().all(|o| o.scanned <= 7), "bounded windows");
    assert_eq!(first.iter().map(|o| o.scanned).sum::<i64>(), 50);
    assert_eq!(
        first.iter().map(|o| o.rederived).sum::<usize>(),
        50,
        "every live entry predates the start"
    );
    assert_eq!(
        f.count("select count(*) from public.agg__ledger where __tombstone")
            .await,
        10,
        "the deleted keys are tombstones"
    );

    let second = sweep(start_xid).await;
    assert_eq!(
        second.iter().map(|o| o.rederived).sum::<usize>(),
        0,
        "a re-derived entry's basis is past the start, and a tombstone isn't live"
    );

    f.merge_all(&plan).await;
    // The deletes' captured changes are visible in the sweep's bases, so
    // they drain to nothing.
    f.drain().await;
    f.assert_agg_oracle().await;
}

/// The sweep picks an entry Apply alone wrote (#625 F3, review): a key
/// inserted after the build is counted by its insert's change, with no
/// `basis`, and no chunk of a rebuild reaches it once its row is deleted
/// while the definition is frozen. The sweep's `basis is null` arm retires
/// it.
#[tokio::test]
async fn a_rebuild_sweeps_a_deleted_key_only_apply_had_counted() {
    let mut f = Fixture::new(30, &[AGG]).await;
    f.pass().await;
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    // Nothing has sealed since the start, so the start's segment is still
    // active, and a page re-derives its batch's keys (#733). A write sealed
    // into that batch moves the inserts below to the next, which Apply
    // applies. It changes `v`: an update that changes nothing imaged stages
    // nothing (#623 D8a's skip-no-op).
    f.raw
        .batch_execute("update public.src set v = v + 1 where id = 1")
        .await
        .expect("a write in the start's segment");
    f.drain().await;
    f.raw
        .batch_execute("insert into public.src values (100, 3, 1000), (101, 4, 2000)")
        .await
        .expect("insert after the build");
    f.drain().await;
    assert_eq!(
        f.count(
            "select count(*) from public.agg__ledger \
             where __from_key in ('100', '101') and __basis is null \
               and __member and not __tombstone"
        )
        .await,
        2,
        "Apply alone counted the new keys"
    );

    let trellis = f.trellis().await;
    trellis.apply("PAUSE TRANSFORM agg").await.expect("pause");
    f.raw
        .batch_execute("delete from public.src where id = 100")
        .await
        .expect("delete while paused");
    f.drain().await;
    trellis.apply("RESUME TRANSFORM agg").await.expect("resume");
    f.pass().await;
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

/// The sweep reads the source by a composite key's typed parts (#625 F3,
/// review): text parts holding the key encoding's separator, its NULL
/// sentinel, a backslash and a quote, beside a `timestamptz` part. A live
/// key the sweep re-derives is found and kept; a key deleted while frozen
/// is retired.
#[tokio::test]
async fn a_rebuild_sweeps_a_composite_typed_key() {
    let mut f = Fixture::new(0, &[]).await;
    f.raw
        .batch_execute(
            "create table public.csrc (a text, t timestamptz, g integer, v bigint, \
                 primary key (a, t)); \
             insert into public.csrc \
             select k, timestamptz '2024-01-01 00:00:00+00' + i * interval '1 hour 0.5 seconds', \
                    i % 3, i \
             from generate_series(1, 40) i, \
                  lateral (select (array['x', 'y' || chr(31) || 'z', 'q' || chr(1), \
                                         'b\\s', 'o''q', ''])[1 + i % 6] || i as k) s",
        )
        .await
        .expect("seed the composite source");
    let columns = [
        ("a".to_string(), ValueType::Text),
        (
            "g".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int4),
        ),
        (
            "v".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(
        &f.db.pool,
        "TRANSFORM cagg FROM public.csrc GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("register");
    let actual = "select (g, total, n)::text from public.cagg order by g";
    let expected = "select (g, sum(v), count(*))::text from public.csrc group by g order by g";
    f.pass().await;
    f.run("cagg").await;
    assert_eq!(f.status("cagg").await.as_deref(), Some("live"));
    assert_eq!(f.rows(actual).await, f.rows(expected).await);

    let trellis = f.trellis().await;
    trellis.apply("PAUSE TRANSFORM cagg").await.expect("pause");
    f.raw
        .batch_execute("delete from public.csrc where v % 4 = 0")
        .await
        .expect("delete while paused");
    f.drain().await;
    trellis
        .apply("RESUME TRANSFORM cagg")
        .await
        .expect("resume");
    f.pass().await;
    assert_eq!(f.build("cagg").await.as_deref(), Some("rederive"));
    f.run("cagg").await;
    assert_eq!(f.status("cagg").await.as_deref(), Some("live"));
    assert_eq!(
        f.count("select count(*) from public.cagg__ledger where __member and not __tombstone")
            .await,
        30,
        "every live row's entry is live, and only those"
    );
    assert_eq!(f.rows(actual).await, f.rows(expected).await);

    // The rebuild's chunks re-derived every live key, so its sweep picked
    // only the deleted ones. A sweep from a later start picks the live ones
    // too, and must find each one's row by its typed parts.
    let pool = f.db.pool.clone();
    let plan = BuildPlan::load(&pool, "cagg")
        .await
        .expect("load")
        .expect("a re-derive shape");
    let start_xid: String = f
        .raw
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("a start xid")
        .get(0);
    let mut cursor: Option<String> = None;
    let mut rederived = 0;
    loop {
        let mut client = pool.get().await.expect("pool");
        let txn = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await
            .expect("begin");
        let outcome = build::sweep_batch(&txn, &plan, &start_xid, cursor.as_deref(), 7)
            .await
            .expect("sweep batch");
        txn.commit().await.expect("commit");
        rederived += outcome.rederived;
        cursor = outcome.next.clone();
        if outcome.finished {
            break;
        }
    }
    assert_eq!(rederived, 30, "every live entry predates the later start");
    assert_eq!(
        f.count("select count(*) from public.cagg__ledger where __member and not __tombstone")
            .await,
        30,
        "the sweep found every live key's row"
    );
    f.merge_all(&plan).await;
    assert_eq!(f.rows(actual).await, f.rows(expected).await);
}

/// The backfill discharge never dispatches a definition the Re-derive build
/// takes (#625 F3, `plan_waiting_builds`' skip), even a discharge with no
/// `ready` list over a marker on its source: it is left
/// `waiting_to_backfill` for the staging worker's start.
#[tokio::test]
async fn the_discharge_leaves_a_rederive_shape_to_the_start() {
    let mut f = Fixture::new(20, &[AGG]).await;
    f.raw
        .batch_execute("insert into pending_backfill (table_name) values ('public.src')")
        .await
        .expect("park a marker on the source");
    for _ in 0..100 {
        trellis::intake::markers::run_pending_backfills(
            &mut f.raw,
            WAKE,
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("discharge");
        if f.count("select count(*) from pending_backfill").await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        f.count("select count(*) from pending_backfill").await,
        0,
        "the marker discharged"
    );
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("waiting_to_backfill")
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks").await,
        0,
        "no old build was dispatched"
    );

    f.pass().await;
    assert_eq!(f.build("agg").await.as_deref(), Some("rederive"));
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    f.assert_agg_oracle().await;
}

// ------------------------------------------------------- 1-1 targets (F8a)

/// A plain 1-1 target's build (#625 F8a) is `waiting_to_backfill ->
/// backfilling -> live`, with no `catching_up`, no marker, and no group
/// deltas: its chunks write the target rows, so no step merges, and it has
/// no delta table.
#[tokio::test]
async fn a_one_to_one_build_goes_from_waiting_to_live_with_no_catch_up() {
    let mut f = Fixture::new(200, &[ONE]).await;
    assert_eq!(
        f.status("one").await.as_deref(),
        Some("waiting_to_backfill")
    );
    f.pass().await;
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("one").await.as_deref(), Some("rederive"));
    assert_eq!(f.count("select count(*) from pending_backfill").await, 0);

    let (steps, seen) = f.run("one").await;
    assert_eq!(steps.first(), Some(&Step::Planned));
    assert!(steps.contains(&Step::Chunk));
    assert!(!steps.contains(&Step::Merged), "a 1-1 build never merges");
    assert_eq!(
        seen,
        BTreeSet::from(["backfilling".to_string(), "live".to_string()]),
        "the build never passes through catching_up"
    );
    assert_eq!(f.build("one").await, None);
    assert_eq!(
        steps.iter().filter(|step| **step == Step::Chunk).count(),
        20,
        "200 rows in chunks of 10, and no old build ran"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks").await,
        0,
        "the flip deleted the build's rows (#966)"
    );
    assert_eq!(
        f.count("select count(*) from pg_class where relname = 'one__deltas'")
            .await,
        0,
        "a 1-1 target has no delta table"
    );
    assert_eq!(
        f.count("select count(*) from public.one__ledger where __basis is null")
            .await,
        0,
        "every entry was re-derived by a chunk"
    );
    f.assert_one_oracle().await;
}

/// A started 1-1 definition applies at once (B1), and every kind of write
/// made while its build runs, on keys whose chunks have and haven't run,
/// ends up in the target exactly as the source has it.
#[tokio::test]
async fn changes_drained_while_a_one_to_one_builds_are_applied() {
    let mut f = Fixture::new(200, &[ONE]).await;
    f.pass().await;
    f.raw
        .batch_execute("insert into public.src values (1000, 99, 5)")
        .await
        .expect("insert");
    f.drain().await;
    assert_eq!(
        f.rows("select (id, g, dbl)::text from public.one").await,
        vec!["(1000,99,10)".to_string()],
        "the change was applied while backfilling, before any chunk"
    );

    assert_eq!(f.step(&OPTIONS).await, Step::Planned);
    for round in 0..6 {
        let base = round * 30;
        f.raw
            .batch_execute(&format!(
                "update public.src set v = v + 100 where id in ({}, {}); \
                 update public.src set g = (g + 3) % 7 where id = {}; \
                 delete from public.src where id = {}; \
                 insert into public.src values ({}, {}, 7)",
                base + 1,
                base + 25,
                base + 12,
                base + 18,
                2000 + round,
                round % 7,
            ))
            .await
            .expect("write");
        f.drain().await;
        for _ in 0..3 {
            f.step(&OPTIONS).await;
        }
    }
    let (_, seen) = f.run("one").await;
    assert!(!seen.contains("catching_up"));
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// A 1-1 definition paused part-way through its build, with keys deleted,
/// updated and inserted while frozen, is rebuilt over the entries it kept
/// (#625 F3, F8a): no marker, a sweep, and the sweep deletes the target
/// rows of the keys deleted meanwhile, which no chunk reaches.
#[tokio::test]
async fn a_resumed_one_to_one_build_sweeps_the_keys_deleted_while_paused() {
    let mut f = Fixture::new(100, &[ONE]).await;
    f.pass().await;
    // The plan job and three chunks (ids 1..=30).
    assert_eq!(f.run_chunks_by_hand(4).await, 4);
    assert_eq!(f.count("select count(*) from public.one").await, 30);
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    f.raw
        .batch_execute(
            "delete from public.src where id <= 15 or id = 95; \
             update public.src set v = v + 1000, g = g + 1 where id in (20, 50); \
             insert into public.src values (200, 3, 7)",
        )
        .await
        .expect("write while paused");
    f.drain().await;
    assert_eq!(
        f.count("select count(*) from public.one where id <= 15")
            .await,
        15,
        "the frozen target still has the deleted keys' rows"
    );

    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
    assert_eq!(
        f.status("one").await.as_deref(),
        Some("waiting_to_backfill")
    );
    assert_eq!(f.count("select count(*) from pending_backfill").await, 0);
    f.pass().await;
    assert_eq!(f.build("one").await.as_deref(), Some("rederive"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'sweep' and not done")
            .await,
        1,
        "a rebuild over a non-empty ledger enqueues one sweep"
    );

    let (_, seen) = f.run("one").await;
    assert_eq!(
        seen,
        BTreeSet::from(["backfilling".to_string(), "live".to_string()])
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done")
            .await,
        0
    );
    assert_eq!(
        f.count(
            "select count(*) from public.one__ledger \
             where not __tombstone and __from_key::bigint <= 15"
        )
        .await,
        0,
        "the sweep retired the deleted keys' entries"
    );
    f.assert_one_oracle().await;
}

/// The sweep is what removes a key deleted while paused: with the rebuild's
/// sweep job dropped, the chunks alone leave its target row behind. (So the
/// test above isn't passing for a reason other than the sweep.)
#[tokio::test]
async fn without_its_sweep_a_one_to_one_rebuild_keeps_a_deleted_key() {
    let mut f = Fixture::new(30, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    let trellis = f.trellis().await;
    trellis.apply("PAUSE TRANSFORM one").await.expect("pause");
    f.raw
        .batch_execute("delete from public.src where id = 5")
        .await
        .expect("delete while paused");
    f.drain().await;
    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
    f.pass().await;
    f.raw
        .batch_execute("delete from backfill_chunks where kind = 'sweep'")
        .await
        .expect("drop the sweep");
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.count("select count(*) from public.one where id = 5")
            .await,
        1,
        "only the sweep reaches a key with no source row"
    );
}

/// A 1-1 build leaves a quarantined key out (#625 F-A5): the build goes
/// `live` without its row, and its entry is never taken.
#[tokio::test]
async fn a_one_to_one_build_leaves_a_quarantined_key_out() {
    let mut f = Fixture::new(50, &[ONE]).await;
    f.raw
        .batch_execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, 'public.src', '17', 'test' from transform_definitions \
             where source_table = 'public.src'",
        )
        .await
        .expect("quarantine key 17");
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.rows(ONE_ACTUAL).await,
        f.rows("select (id, g, v + v)::text from public.src where id <> 17 order by id")
            .await
    );
    assert_eq!(
        f.count("select count(*) from public.one__ledger where __from_key = '17'")
            .await,
        0
    );
}

/// A plain 1-1 whose source loses its primary key once captured (#625
/// F8a): the start reads no key, so it isn't what fails; the plan job is
/// what needs the key. That job fails, unnarrowable, and is charged and
/// retried while status reports the error (#616); its last charge pauses
/// the definition with the error on status. The build never goes `live`
/// and nothing is written to the target.
#[tokio::test]
async fn a_one_to_one_whose_source_lost_its_key_fails_through_its_plan_job() {
    // `chunk_queue::MAX_CHARGED_ATTEMPTS`, which is crate-private.
    const MAX_CHARGED_ATTEMPTS: u32 = 5;
    let mut f = Fixture::new(50, &[ONE]).await;
    // Capture needs the key, so it goes after the pass that installs the
    // capture and starts the build, and before the plan job runs.
    f.pass().await;
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("one").await.as_deref(), Some("rederive"));
    f.raw
        .batch_execute("alter table public.src drop constraint src_pkey")
        .await
        .expect("drop the source's primary key");

    let trellis = f.trellis().await;
    for charge in 1..MAX_CHARGED_ATTEMPTS {
        f.raw
            .batch_execute("update backfill_chunks set next_attempt_at = now()")
            .await
            .expect("make the plan job due");
        assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job is claimed");
        let status = trellis
            .status("one")
            .await
            .expect("status")
            .expect("the transform is registered");
        assert_eq!(status.status, trellis::TransformStatus::Backfilling);
        let failure = status
            .backfill_failure
            .expect("status reports the failing plan job");
        assert_eq!(failure.attempts, charge);
        assert!(
            failure.last_error.contains("no primary key"),
            "the error names the cause, got {:?}",
            failure.last_error
        );
        assert!(
            failure.next_attempt_at > std::time::SystemTime::now(),
            "the retry is backed off"
        );
    }
    f.raw
        .batch_execute("update backfill_chunks set next_attempt_at = now()")
        .await
        .expect("make the plan job due");
    assert_eq!(f.run_chunks_by_hand(1).await, 1);

    let status = trellis
        .status("one")
        .await
        .expect("status")
        .expect("the transform is registered");
    assert_eq!(status.status, trellis::TransformStatus::Paused);
    let failure = status
        .backfill_failure
        .expect("the pause carries the error");
    assert_eq!(failure.attempts, MAX_CHARGED_ATTEMPTS);
    assert!(failure.last_error.contains("no primary key"));

    // Nothing more runs: no chunk was planned, a paused definition's plan
    // job isn't claimed, and the target is empty.
    f.raw
        .batch_execute("update backfill_chunks set next_attempt_at = now()")
        .await
        .expect("make every job due");
    assert_eq!(f.run_chunks_by_hand(4).await, 0);
    let (steps, _) = f.run("one").await;
    assert!(steps.iter().all(|step| !step.progressed()), "{steps:?}");
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive'")
            .await,
        0
    );
    assert_eq!(f.count("select count(*) from public.one").await, 0);
}

/// A second `SUM` and `COUNT(*)` by `g` over the same source, registered
/// once the first is live.
const AGG2: &str =
    "TRANSFORM agg2 FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

/// #733: a change committed before a build starts, in a batch drained only
/// after the start, while a later change to its key was drained before the
/// start. The later change never reaches the new definition (no page that
/// ran before the start applies it), and the key is gone before any chunk
/// reads its range, so no chunk re-derives it either. Applied as it stands,
/// the older change counted the key back in for good: a group present
/// that the oracle doesn't have, or a count one too high. A page re-derives
/// every record of a batch the start may have preceded instead.
///
/// The batches are sealed and drained by hand, newest first, which is what
/// a page held at its entry lock does to a loaded engine (the steady-load
/// tier's stall).
#[tokio::test]
async fn a_batch_older_than_the_start_drained_after_it_does_not_revive_a_deleted_key() {
    let mut f = Fixture::new(20, &[AGG]).await;
    f.pass().await;
    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    let watermark = StagedWatermark::saturated();
    let seal = async |f: &mut Fixture, sql: &str| -> i64 {
        f.raw.batch_execute(sql).await.expect("write");
        trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
            .await
            .expect("seal")
            .expect("the write made the active segment non-empty")
            .sealed_seg_seq
    };
    // Key 100 is inserted into a new group in one batch and deleted in the
    // next.
    let older = seal(&mut f, "insert into public.src values (100, 50, 7)").await;
    let newer = seal(&mut f, "delete from public.src where id = 100").await;
    // The newer batch drains before `agg2` exists, let alone applies.
    apply::drain_once(&f.db.pool, newer, "drainer", 1, WAKE, &watermark)
        .await
        .expect("drain the newer batch");

    let columns = [
        (
            "id".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
        (
            "g".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int4),
        ),
        (
            "v".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(&f.db.pool, AGG2, &columns, "public")
        .await
        .expect("register agg2");
    f.pass().await;
    assert_eq!(f.status("agg2").await.as_deref(), Some("backfilling"));

    // The older batch drains after the start: its insert reaches `agg2`.
    apply::drain_once(&f.db.pool, older, "drainer", 1, WAKE, &watermark)
        .await
        .expect("drain the older batch");
    f.run("agg2").await;
    assert_eq!(f.status("agg2").await.as_deref(), Some("live"));
    assert_eq!(
        f.rows("select (g, total, n)::text from public.agg2 order by g")
            .await,
        f.rows(AGG_EXPECTED).await,
        "agg2 equals a from-scratch GROUP BY over the source"
    );
    f.assert_agg_oracle().await;
}

/// A second plain 1-1 target over the same source, registered once the
/// first is live.
const ONE2: &str = "TRANSFORM one2 FROM public.src SELECT g AS g, v + v AS dbl";

/// #733 on a 1-1 target (F8a): the same out-of-order drain across the
/// start as `a_batch_older_than_the_start_drained_after_it_does_not_revive_a_deleted_key`.
/// The key's delete drained before `one2` started, so it never reached
/// `one2`, and its insert, in an older batch, drained after the start. No
/// chunk reads a key the source no longer has, and a fresh build has no
/// sweep, so an Apply of that insert left a target row the source doesn't
/// back for good.
#[tokio::test]
async fn a_batch_older_than_a_one_to_one_start_drained_after_it_does_not_revive_a_deleted_key() {
    let mut f = Fixture::new(20, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    let watermark = StagedWatermark::saturated();
    let seal = async |f: &mut Fixture, sql: &str| -> i64 {
        f.raw.batch_execute(sql).await.expect("write");
        trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
            .await
            .expect("seal")
            .expect("the write made the active segment non-empty")
            .sealed_seg_seq
    };
    let older = seal(&mut f, "insert into public.src values (100, 5, 7)").await;
    let newer = seal(&mut f, "delete from public.src where id = 100").await;
    apply::drain_once(&f.db.pool, newer, "drainer", 1, WAKE, &watermark)
        .await
        .expect("drain the newer batch");

    let columns = [
        (
            "id".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
        (
            "g".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int4),
        ),
        (
            "v".to_string(),
            ValueType::Integer(trellis::integer::IntWidth::Int8),
        ),
    ]
    .into_iter()
    .collect();
    trellis::defs::install_definition(&f.db.pool, ONE2, &columns, "public")
        .await
        .expect("register one2");
    f.pass().await;
    assert_eq!(f.status("one2").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("one2").await.as_deref(), Some("rederive"));

    apply::drain_once(&f.db.pool, older, "drainer", 1, WAKE, &watermark)
        .await
        .expect("drain the older batch");
    f.run("one2").await;
    assert_eq!(f.status("one2").await.as_deref(), Some("live"));
    assert_eq!(
        f.rows("select (id, g, dbl)::text from public.one2 order by id")
            .await,
        f.rows(ONE_EXPECTED).await,
        "one2 equals its source"
    );
    f.assert_one_oracle().await;
}

/// Seals the active segment after `sql`, returning the batch it sealed.
async fn write_and_seal(f: &mut Fixture, sql: &str) -> i64 {
    f.raw.batch_execute(sql).await.expect("write");
    trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
        .await
        .expect("seal")
        .expect("the write made the active segment non-empty")
        .sealed_seg_seq
}

/// #742: a page's Re-derive reads the source live, so its snapshot can see
/// changes in batches newer than the page's own. Its tombstone was stamped
/// with the page's segment all the same, so the tombstone GC collected it
/// while a batch its read saw was still pending, and that batch's stale
/// change then applied to a fresh entry and brought the deleted key back.
///
/// Key 5 is updated in three batches and deleted in the last. The oldest
/// batch is at the build's start segment, so its page re-derives the key
/// (#733) after the delete: a tombstone. The newest batch's delete is
/// refused (the read saw it), the GC runs while the middle batch is still
/// pending, and then the middle batch's update drains. The steady-load
/// tier's stall reorders drains this way (seed 1 case 7).
async fn rederive_tombstone_outlives_a_batch_its_read_saw(
    definition: &str,
    target: &str,
) -> Fixture {
    let mut f = Fixture::new(20, &[definition]).await;
    f.pass().await;
    assert_eq!(f.status(target).await.as_deref(), Some("backfilling"));
    let watermark = StagedWatermark::saturated();
    let at_start = write_and_seal(&mut f, "update public.src set v = 101 where id = 5").await;
    let middle = write_and_seal(&mut f, "update public.src set v = 102 where id = 5").await;
    let newest = write_and_seal(&mut f, "delete from public.src where id = 5").await;
    for seg in [at_start, newest] {
        apply::drain_once(&f.db.pool, seg, "drainer", 1, WAKE, &watermark)
            .await
            .expect("drain");
    }
    trellis::staging::collect_tombstones(&mut f.raw)
        .await
        .expect("collect tombstones");
    apply::drain_once(&f.db.pool, middle, "drainer", 1, WAKE, &watermark)
        .await
        .expect("drain the middle batch");
    f.run(target).await;
    assert_eq!(f.status(target).await.as_deref(), Some("live"));
    f
}

#[tokio::test]
async fn a_one_to_one_rederive_tombstone_outlives_a_batch_its_read_saw() {
    let f = rederive_tombstone_outlives_a_batch_its_read_saw(ONE, "one").await;
    assert_eq!(
        f.rows(ONE_ACTUAL).await,
        f.rows(ONE_EXPECTED).await,
        "one equals its source"
    );
    f.assert_one_oracle().await;
}

#[tokio::test]
async fn an_aggregate_rederive_tombstone_outlives_a_batch_its_read_saw() {
    let f = rederive_tombstone_outlives_a_batch_its_read_saw(AGG, "agg").await;
    assert_eq!(
        f.rows(AGG_ACTUAL).await,
        f.rows(AGG_EXPECTED).await,
        "agg equals a from-scratch GROUP BY over the source"
    );
    f.assert_agg_oracle().await;
}

/// The `backfill_chunks` rows of the one definition.
async fn build_rows(f: &Fixture) -> i64 {
    f.count("select count(*) from backfill_chunks").await
}

/// A finished build leaves none of its rows behind (#966): the flip deletes
/// the definition's done rows with the build, so the table doesn't keep a
/// full set per rebuild, and the completion check (`build_done`) that reads
/// the definition's rows after every chunk sees only the current build's.
/// A rebuild over a non-empty ledger is a plan job and a sweep, whatever
/// rebuilds came before it.
async fn rebuilds_leave_no_rows(definition: &str, target: &str) {
    let mut f = Fixture::new(200, &[definition]).await;
    f.pass().await;
    f.run(target).await;
    assert_eq!(f.status(target).await.as_deref(), Some("live"));
    assert_eq!(
        build_rows(&f).await,
        0,
        "the first build's rows went with it"
    );

    let trellis = f.trellis().await;
    for rebuild in 1..=3 {
        trellis
            .request_backfill("src")
            .await
            .expect("request a rebuild");
        assert_eq!(f.status(target).await.as_deref(), Some("backfilling"));
        assert_eq!(
            build_rows(&f).await,
            2,
            "rebuild {rebuild} starts from its plan job and its sweep alone"
        );
        f.run(target).await;
        assert_eq!(f.status(target).await.as_deref(), Some("live"));
        assert_eq!(
            build_rows(&f).await,
            0,
            "rebuild {rebuild} leaves no rows behind"
        );
    }
    match target {
        "agg" => f.assert_agg_oracle().await,
        _ => f.assert_one_oracle().await,
    }
}

#[tokio::test]
async fn an_aggregate_s_done_build_rows_do_not_pile_up_over_rebuilds() {
    rebuilds_leave_no_rows(AGG, "agg").await;
}

#[tokio::test]
async fn a_one_to_one_s_done_build_rows_do_not_pile_up_over_rebuilds() {
    rebuilds_leave_no_rows(ONE, "one").await;
}

/// Done rows stay while their build runs (#966): only the flip deletes
/// them, so a build in progress is judged by all its rows. With the plan
/// job and every chunk done but deltas owed, `try_complete` leaves the rows
/// and the definition as they are, and the merge that flips the definition
/// takes the rows with it.
#[tokio::test]
async fn done_rows_stay_until_the_flip() {
    let mut f = Fixture::new(60, &[AGG]).await;
    f.pass().await;
    f.run_chunks_by_hand(usize::MAX).await;
    let rows = build_rows(&f).await;
    assert_eq!(rows, 7, "the plan job and six chunks");
    assert_eq!(
        f.count("select count(*) from backfill_chunks where done")
            .await,
        rows
    );
    let id = f.definition_id().await;
    assert!(!build::try_complete(&f.db.pool, id).await.expect("try"));
    assert_eq!(build_rows(&f).await, rows, "an unfinished build keeps them");

    while f.step(&OPTIONS).await.progressed() {}
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(build_rows(&f).await, 0, "the flip took them");
    f.assert_agg_oracle().await;
}

/// A build a resume superseded leaves done rows behind (the plan job and
/// chunks it finished), which the resumed build's flip deletes with its own
/// (#966). Completion still waits for every row of the resumed build.
#[tokio::test]
async fn a_resumed_build_s_flip_takes_the_superseded_builds_done_rows() {
    let mut f = Fixture::new(100, &[AGG]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(4).await, 4);
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM agg")
        .await
        .expect("pause the building transform");
    trellis.apply("RESUME TRANSFORM agg").await.expect("resume");
    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where done")
            .await,
        4,
        "the superseded build's plan job and three chunks are still there"
    );
    let id = f.definition_id().await;
    assert!(!build::try_complete(&f.db.pool, id).await.expect("try"));
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));

    f.run("agg").await;
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(build_rows(&f).await, 0);
    f.assert_agg_oracle().await;
}
