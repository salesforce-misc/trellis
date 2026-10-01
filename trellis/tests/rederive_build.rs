//! The scheduled Re-derive build (#625 F2, epic #556; ADR-0002 "A build is
//! Re-derive over chunks, and applies from its first chunk"), with
//! `ClientOptions::rederive_build` on.
//!
//! Every test steps the engine by hand: the staging worker's reconcile pass
//! (`trellis::client::reconcile_pass_with`), a drain worker's build step
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
        let columns = [
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Numeric),
            ("v".to_string(), ValueType::Numeric),
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

    /// One staging-worker reconcile pass with the knob on: installs the
    /// source's capture, starts every qualifying definition, and discharges
    /// the markers.
    async fn pass(&mut self) {
        trellis::client::reconcile_pass_with(
            &mut self.raw,
            &self.db.pool,
            DEFAULT_SCHEMA,
            WAKE,
            Duration::from_secs(5),
            true,
        )
        .await
        .expect("reconcile pass");
    }

    /// One drain worker's build step.
    async fn step(&self, options: &WorkerOptions) -> Step {
        build::work_once(&self.db.pool, "worker", options)
            .await
            .expect("build step")
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
        f.count("select count(*) from backfill_chunks where kind = 'rederive'")
            .await,
        20,
        "200 rows in chunks of 10"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done")
            .await,
        0
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

    // A new group, before any chunk: only Apply can put it there.
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

/// The flip waits for the last merge (B7): with every chunk done but deltas
/// still pending, the definition stays `backfilling`, and the merge that
/// empties the delta table moves it to `live`.
#[tokio::test]
async fn the_flip_happens_only_after_the_last_merge() {
    let mut f = Fixture::new(60, &[AGG]).await;
    f.pass().await;
    let pool = &f.db.pool;

    // Run the plan and every chunk by hand, with no merge between them.
    loop {
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
            break;
        };
        build::run_claimed(pool, &chunk, "hand", &OPTIONS).await;
    }
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done")
            .await,
        0,
        "every chunk is done"
    );
    let pending = f.count("select count(*) from public.agg__deltas").await;
    assert!(pending > 0, "the chunks left deltas for the merger");
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    let id: i64 = f
        .raw
        .query_one("select id from transform_definitions", &[])
        .await
        .expect("definition id")
        .get(0);
    assert!(
        !build::try_complete(pool, id).await.expect("try_complete"),
        "not live while deltas are pending"
    );
    assert_eq!(
        f.rows(AGG_ACTUAL).await,
        Vec::<String>::new(),
        "no group yet"
    );

    assert_eq!(f.step(&OPTIONS).await, Step::Merged);
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("live"),
        "the merge that emptied the delta table flipped it"
    );
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
    let done: bool = f
        .raw
        .query_one(
            "select done from backfill_chunks where id = $1",
            &[&chunk.id],
        )
        .await
        .expect("read the chunk")
        .get(0);
    assert!(done, "another worker ran the reclaimed chunk");
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
/// it, the other (a recomputed `MAX`) the old direct build with its go-live
/// catch-up, and both end equal to their oracles.
#[tokio::test]
async fn two_definitions_on_one_source_take_one_build_path_each() {
    const MAXES: &str = "TRANSFORM agg_max FROM public.src GROUP BY g SELECT MAX(v) AS top";
    let mut f = Fixture::new(150, &[AGG, MAXES]).await;
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
        f.rows("select (g, top)::text from public.agg_max order by g")
            .await,
        f.rows("select (g, max(v))::text from public.src group by g order by g")
            .await,
        "the old build's target equals its oracle"
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
    {
        let mut client = pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        build::merge_deltas(&txn, &plan, 10_000)
            .await
            .expect("merge");
        txn.commit().await.expect("commit the merge");
    }
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
    // and left in the ring.
    f.raw
        .batch_execute("update public.src set v = v + 1000 where id = 3")
        .await
        .expect("write before the widen");
    let columns = [
        ("id".to_string(), ValueType::Numeric),
        ("g".to_string(), ValueType::Numeric),
        ("v".to_string(), ValueType::Numeric),
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

/// A definition paused part-way through its Re-derive build and resumed has
/// ledger entries, some for keys deleted while it was paused, which only
/// #625 F3's sweep would retire. So it goes to the old build, which empties
/// the ledger and the delta table, and `build` is cleared at the resume.
#[tokio::test]
async fn a_build_resumed_with_entries_takes_the_old_build() {
    let mut f = Fixture::new(100, &[AGG]).await;
    f.pass().await;
    for _ in 0..4 {
        f.drain().await;
        f.step(&OPTIONS).await;
    }
    assert!(
        f.count("select count(*) from public.agg__ledger").await > 0,
        "some chunks ran"
    );
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM agg")
        .await
        .expect("pause the building transform");
    // Deleted while paused: the entries of the keys the first chunks re-derived
    // stay live.
    f.raw
        .batch_execute("delete from public.src where id <= 15")
        .await
        .expect("delete while paused");
    f.drain().await;

    trellis.apply("RESUME TRANSFORM agg").await.expect("resume");
    assert_eq!(
        f.status("agg").await.as_deref(),
        Some("waiting_to_backfill")
    );
    assert_eq!(f.build("agg").await, None, "a resume clears `build`");

    f.pass().await;
    assert_eq!(f.status("agg").await.as_deref(), Some("backfilling"));
    assert_eq!(f.build("agg").await, None, "the old build took it");
    let pool = f.db.pool.clone();
    let job = {
        let client = pool.get().await.expect("pool");
        chunk_queue::claim_chunks(&**client, "old", 1)
            .await
            .expect("claim the direct build")
            .into_iter()
            .next()
            .expect("the direct build job")
    };
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
    assert_eq!(f.count("select count(*) from public.agg__deltas").await, 0);
    for _ in 0..3 {
        f.pass().await;
        f.drain().await;
    }
    assert_eq!(f.status("agg").await.as_deref(), Some("live"));
    assert_eq!(
        f.step(&OPTIONS).await,
        Step::Idle,
        "nothing of the re-derive build is left"
    );
    f.assert_agg_oracle().await;
}
