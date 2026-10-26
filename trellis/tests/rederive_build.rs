//! The scheduled Re-derive build (#625 F2/F3, epic #556; ADR-0002 "A build
//! is Re-derive over chunks, and applies from its first chunk"): the build of
//! a plain 1-1 target. (An aggregate is built by the group basis's one
//! absolute write, `group_basis_build.rs`.)
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
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::staging::{apply, has_pending, retire_drained_segments};
use trellis::{Trellis, TrellisOptions};

const WAKE: &str = "rederive_wake";

/// `SUM` and `COUNT(*)` by `g`: a shape the Re-derive build takes.
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";

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
        ];
        Self::custom(
            &format!(
                "create table public.src (id bigint primary key, g integer, v bigint); \
                 insert into public.src select i, i % 7, i from generate_series(1, {rows}) i"
            ),
            columns,
            definitions,
        )
        .await
    }

    /// A source `setup` creates (named `public.src`, whose columns are
    /// `columns`), and `definitions` registered over it.
    async fn custom(
        setup: &str,
        columns: impl IntoIterator<Item = (String, ValueType)>,
        definitions: &[&str],
    ) -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect(db.dsn()).await;
        raw.batch_execute(setup).await.expect("seed the source");
        // The source's own types: a resume re-types every column Trellis
        // created whose type differs from the one define gives it from
        // the live schema.
        let columns = columns.into_iter().collect();
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

    /// One staging-worker reconcile pass: installs the source's capture and
    /// starts every ready definition.
    async fn pass(&mut self) {
        trellis::client::reconcile_pass(&mut self.raw, &self.db.pool, DEFAULT_SCHEMA)
            .await
            .expect("reconcile pass");
    }

    /// One drain worker's build step.
    async fn step(&self, options: &WorkerOptions) -> Step {
        build::work_once(&self.db.pool, "worker", options)
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
        for _ in 0..16 {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next claimable segment")
            {
                apply::drain_once(&self.db.pool, seg, "drainer", 1, WAKE)
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

/// A chunk whose entry lock a drain page holds gives up after its short lock
/// timeout and is retried later without a charge (B6); the build then
/// finishes.
#[tokio::test]
async fn a_chunk_behind_a_held_entry_gives_up_and_is_retried_uncharged() {
    let mut f = Fixture::new(40, &[ONE]).await;
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
        .batch_execute("begin; select 1 from public.one__ledger where __from_key = '5' for update")
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
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// A chunk is claimed only while the ring's sealed, undrained backlog is
/// under twice the drain batch cap (B6); the plan job runs regardless.
#[tokio::test]
async fn chunks_wait_while_the_ring_has_a_backlog() {
    let mut f = Fixture::new(40, &[ONE]).await;
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
    f.run("one").await;
    f.assert_one_oracle().await;
}

/// A source `TRUNCATE` while the build runs: its entries go with the
/// ledger, and the rest of the build and Apply rebuild the target from
/// the rows inserted after it.
#[tokio::test]
async fn a_truncate_during_the_build_is_absorbed() {
    let mut f = Fixture::new(100, &[ONE]).await;
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
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// A drop during the build: pause, then drop. The chunk rows, the delta
/// table, the ledger and the target all go, and the workers find nothing to
/// do.
#[tokio::test]
async fn a_drop_during_the_build_takes_its_work_with_it() {
    let mut f = Fixture::new(100, &[ONE]).await;
    f.pass().await;
    for _ in 0..3 {
        f.drain().await;
        f.step(&OPTIONS).await;
    }
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    assert_eq!(
        f.step(&OPTIONS).await,
        Step::Idle,
        "a paused build gets no merge and no chunk"
    );
    trellis
        .apply("DROP TRANSFORM one")
        .await
        .expect("drop the paused transform");
    assert_eq!(f.status("one").await, None);
    assert_eq!(f.count("select count(*) from backfill_chunks").await, 0);
    for table in ["one", "one__ledger"] {
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

    // The start's read of the ledger waits on the holder's lock, so this
    // pass gives it only a moment.
    trellis::client::reconcile_pass(&mut f.raw, &f.db.pool, DEFAULT_SCHEMA)
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

/// A ledger dropped by hand (`docs/known-correctness-gaps.md`, rows written into
/// what Trellis owns): a live definition whose ledger is dropped fails each
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
    for _ in 0..16 {
        trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
            .await
            .expect("seal");
        while let Some(seg) = apply::next_claimable_segment(&f.raw)
            .await
            .expect("next claimable segment")
        {
            if apply::drain_once(&f.db.pool, seg, "drainer", 1, WAKE)
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

/// The start waits out its source's capture gate (#625 Q2(a)): while a
/// change the old, narrower capture function staged is still pending, a
/// definition that needs the widened column stays `waiting_to_backfill`,
/// and the pass after that change drains starts it.
#[tokio::test]
async fn the_start_waits_for_the_widens_capture_gate() {
    const COUNTS: &str =
        "TRANSFORM cnt FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(*) AS n";
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

/// A quarantined definition resumes the same way (#625 F3): through the
/// start, with a sweep.
#[tokio::test]
async fn a_quarantined_build_resumes_through_the_start() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    // The fuse's own write, as `quarantine_if_crossed` makes it.
    f.raw
        .batch_execute(
            "update transform_definitions set status = 'quarantined' \
             where target_table = 'public.one'",
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
        .apply("RESUME TRANSFORM one")
        .await
        .expect("resume");
    f.pass().await;
    assert_eq!(f.build("one").await.as_deref(), Some("rederive"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'sweep'")
            .await,
        1
    );
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

// ------------------------------------------------------- 1-1 targets (F8a)

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
/// (#625 F3, F8a): a sweep, and the sweep deletes the target
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
const AGG2: &str = "TRANSFORM agg2 FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";

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
    apply::drain_once(&f.db.pool, newer, "drainer", 1, WAKE)
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
    apply::drain_once(&f.db.pool, older, "drainer", 1, WAKE)
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
    apply::drain_once(&f.db.pool, newer, "drainer", 1, WAKE)
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

    apply::drain_once(&f.db.pool, older, "drainer", 1, WAKE)
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
    let at_start = write_and_seal(&mut f, "update public.src set v = 101 where id = 5").await;
    let middle = write_and_seal(&mut f, "update public.src set v = 102 where id = 5").await;
    let newest = write_and_seal(&mut f, "delete from public.src where id = 5").await;
    for seg in [at_start, newest] {
        apply::drain_once(&f.db.pool, seg, "drainer", 1, WAKE)
            .await
            .expect("drain");
    }
    trellis::staging::collect_tombstones(&mut f.raw)
        .await
        .expect("collect tombstones");
    apply::drain_once(&f.db.pool, middle, "drainer", 1, WAKE)
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
async fn an_one_to_one_s_done_build_rows_do_not_pile_up_over_rebuilds() {
    rebuilds_leave_no_rows(ONE, "one").await;
}

#[tokio::test]
async fn a_one_to_one_s_done_build_rows_do_not_pile_up_over_rebuilds() {
    rebuilds_leave_no_rows(ONE, "one").await;
}

/// A build a resume superseded leaves done rows behind (the plan job and
/// chunks it finished), which the resumed build's flip deletes with its own
/// (#966). Completion still waits for every row of the resumed build.
#[tokio::test]
async fn a_resumed_build_s_flip_takes_the_superseded_builds_done_rows() {
    let mut f = Fixture::new(100, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(4).await, 4);
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
    f.pass().await;
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where done")
            .await,
        4,
        "the superseded build's plan job and three chunks are still there"
    );
    let id = f.definition_id().await;
    assert!(!build::try_complete(&f.db.pool, id).await.expect("try"));
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));

    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(build_rows(&f).await, 0);
    f.assert_one_oracle().await;
}

/// The flip leaves an undone row behind (#966): a chunk a worker held
/// across a pause and a resume (`chunk_queue::STALE`). The resumed build
/// goes live without it, and the worker that still holds it writes nothing
/// when it runs it after the flip; giving it up discards the row.
#[tokio::test]
async fn a_chunk_held_across_a_resume_outlives_the_flip_and_is_discarded() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    let held = {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "held", 1, &[chunk_queue::KIND_REDERIVE])
            .await
            .expect("claim a chunk")
    };
    assert_eq!(held.len(), 1);
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done and claimed_by = 'held'")
            .await,
        1,
        "the flip leaves the held chunk to its worker"
    );
    assert_eq!(build_rows(&f).await, 1, "and deletes every other row");

    build::run_claimed(&f.db.pool, &held[0], "held", &OPTIONS).await;
    assert_eq!(build_rows(&f).await, 0, "giving it up discards it");
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// The advisory lock the frozen plan job waits on.
const PLAN_PAUSE_LOCK: i64 = 0x7a1a;

/// Waits until the backends blocked on a lock number `queued` and, when
/// `behind` is set, until one of them is blocked behind that backend. Not a
/// wait for convergence: it returns once Postgres reports the queue the test
/// built on purpose, as the other lock-order tests do with `pg_blocking_pids`.
async fn wait_for_queue(raw: &Client, behind: Option<i32>, queued: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let waiting: i64 = raw
            .query_one(
                "select count(*) from pg_stat_activity \
                 where pid <> pg_backend_pid() \
                   and ($1::int is null or $1 = any(pg_blocking_pids(pid))) \
                   and cardinality(pg_blocking_pids(pid)) > 0",
                &[&behind],
            )
            .await
            .expect("count the queue")
            .get(0);
        if waiting >= queued {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the queue never formed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Claims the Re-derive plan job as `held` and pauses the building transform,
/// the state a pause leaves a job a worker has claimed in.
async fn paused_with_a_claimed_plan_job(f: &mut Fixture) -> (chunk_queue::ClaimedChunk, Trellis) {
    f.pass().await;
    let plan = {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "held", 1, &[chunk_queue::KIND_PLAN])
            .await
            .expect("claim the plan job")
    };
    assert_eq!(plan.len(), 1, "the plan job");
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    (plan.into_iter().next().expect("one plan job"), trellis)
}

/// Runs `action` while a plan job's batch is frozen after it holds its claim
/// and before it inserts its chunks (`PausePoint::AfterPlanHold`), and lets
/// the batch go once `action` has queued behind it. The batch's foreign-key
/// check on the definition row must not wait on `action`, which holds that
/// row and waits on the plan row: the batch takes the definition row first.
/// Returns what `action` returned.
async fn action_against_a_frozen_plan_batch(
    f: &mut Fixture,
    action: &'static str,
) -> Result<(), String> {
    use trellis::staging::interleave::{PausePoint, PauseScope, with_scope};

    let (plan, trellis) = paused_with_a_claimed_plan_job(f).await;
    let gate = connect(f.db.dsn()).await;
    gate.execute("select pg_advisory_lock($1)", &[&PLAN_PAUSE_LOCK])
        .await
        .expect("take the pause lock");
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::AfterPlanHold, "public.one", PLAN_PAUSE_LOCK);
    let pool = f.db.pool.clone();
    let batch = tokio::spawn(with_scope(scope, async move {
        build::run_claimed(&pool, &plan, "held", &OPTIONS).await;
    }));
    let frozen = reached.await.expect("the plan batch reached its pause");

    let acting = tokio::spawn(async move { trellis.apply(action).await });
    wait_for_queue(&f.raw, Some(frozen.backend_pid), 1).await;
    gate.execute("select pg_advisory_unlock($1)", &[&PLAN_PAUSE_LOCK])
        .await
        .expect("release the pause lock");
    batch.await.expect("the plan batch task");
    acting
        .await
        .expect("the action task")
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// A `RESUME TRANSFORM` that arrives while a plan job's batch holds its claim
/// completes: the batch took the definition row before its claim, so the
/// resume waits behind it for the definition row and the batch's insert never
/// waits on the resume (found by a generative sweep as `40P01` on
/// `backfill_chunks`). The resumed build then runs to live.
#[tokio::test]
async fn a_resume_arriving_mid_plan_batch_does_not_deadlock_with_it() {
    let mut f = Fixture::new(60, &[ONE]).await;
    action_against_a_frozen_plan_batch(&mut f, "RESUME TRANSFORM one")
        .await
        .expect("the resume is not a deadlock victim");
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// The same for `DROP TRANSFORM`, whose cascade delete of the chunks waits on
/// the plan row the batch holds.
#[tokio::test]
async fn a_drop_arriving_mid_plan_batch_does_not_deadlock_with_it() {
    let mut f = Fixture::new(60, &[ONE]).await;
    action_against_a_frozen_plan_batch(&mut f, "DROP TRANSFORM one")
        .await
        .expect("the drop is not a deadlock victim");
    assert_eq!(f.status("one").await, None, "the definition is gone");
    assert_eq!(
        f.count("select count(*) from backfill_chunks").await,
        0,
        "and its chunks with it"
    );
}

/// Starts a plan job's batch while `action` holds the definition row and
/// waits on a fenced writer's lock on the plan row, then lets the writer
/// commit. The batch must wait for `action` at its first statement, the
/// definition row, holding nothing: a batch that took the plan row first
/// would wait in its insert instead, behind `action`. Returns the plan job's
/// id and what `action` returned, once the batch has ended.
async fn plan_batch_behind(f: &mut Fixture, action: &'static str) -> (i64, Result<(), String>) {
    let (plan, trellis) = paused_with_a_claimed_plan_job(f).await;

    // A fenced writer on the plan row (`ClaimFence::hold`'s lock), which is
    // what `action` waits out after it takes the definition row.
    let holder = connect(f.db.dsn()).await;
    let holder_pid: i32 = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("holder pid")
        .get(0);
    holder.batch_execute("begin").await.expect("begin");
    holder
        .execute(
            "select 1 from backfill_chunks \
             where id = $1 and claimed_by = 'held' and not done for key share",
            &[&plan.id],
        )
        .await
        .expect("hold the plan row");

    let acting = tokio::spawn(async move { trellis.apply(action).await });
    wait_for_queue(&f.raw, Some(holder_pid), 1).await;
    let pool = f.db.pool.clone();
    let claimed = plan.clone();
    let batch = tokio::spawn(async move {
        build::run_claimed(&pool, &claimed, "held", &OPTIONS).await;
    });
    // `action` waits on the holder, and the batch on `action`, at the
    // batch's definition-row statement.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let waiting: i64 = f
            .raw
            .query_one(
                "select count(*) from pg_stat_activity \
                 where datname = current_database() and wait_event_type = 'Lock' \
                   and query = 'select 1 from transform_definitions where id = $1 for key share'",
                &[],
            )
            .await
            .expect("count the batches waiting on the definition row")
            .get(0);
        if waiting > 0 {
            break;
        }
        assert!(
            !batch.is_finished(),
            "the plan batch ended without waiting on the definition row"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the plan batch never waited on the definition row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    holder.batch_execute("commit").await.expect("commit");

    let acted = acting
        .await
        .expect("the action task")
        .map(|_| ())
        .map_err(|e| format!("{e:?}"));
    batch.await.expect("the plan batch task");
    (plan.id, acted)
}

/// A plan job whose batch starts while a resume holds the definition row
/// waits for it holding nothing, and once the resume commits its claim is
/// stale: it stops as superseded and writes nothing.
#[tokio::test]
async fn a_plan_batch_behind_a_resume_stops_as_superseded() {
    let mut f = Fixture::new(60, &[ONE]).await;
    let (plan_id, resumed) = plan_batch_behind(&mut f, "RESUME TRANSFORM one").await;
    resumed.expect("the resume is not a deadlock victim");

    assert_eq!(
        f.count(&format!(
            "select count(*) from backfill_chunks where id = {plan_id}"
        ))
        .await,
        0,
        "the superseded plan job is discarded"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive'")
            .await,
        0,
        "and it planned no chunk"
    );
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// The same behind a drop: once the drop commits, the batch's definition-row
/// statement finds no row, its claim is gone with the plan row, and it ends
/// quietly having written nothing.
#[tokio::test]
async fn a_plan_batch_behind_a_drop_writes_nothing() {
    let mut f = Fixture::new(60, &[ONE]).await;
    let (_, dropped) = plan_batch_behind(&mut f, "DROP TRANSFORM one").await;
    dropped.expect("the drop is not a deadlock victim");
    assert_eq!(f.status("one").await, None, "the definition is gone");
    assert_eq!(
        f.count("select count(*) from backfill_chunks").await,
        0,
        "and the batch planned no chunk"
    );
}

/// #1201: a rebuild of a 1-1 target deletes rows someone added or re-keyed
/// by hand, which the source no longer backs, not only the rows it
/// re-derives from the source.
#[tokio::test]
async fn a_rebuild_deletes_target_rows_added_or_rekeyed_by_hand() {
    let mut f = Fixture::new(150, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));

    let trellis = f.trellis().await;
    trellis.apply("PAUSE TRANSFORM one").await.expect("pause");
    f.raw
        .batch_execute(
            "insert into public.one (id, g, dbl) values (9999, 1, 1); \
             update public.one set id = 8888 where id = 7",
        )
        .await
        .expect("change the target by hand");
    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.rows(ONE_ACTUAL).await,
        f.rows(ONE_EXPECTED).await,
        "the rebuild repaired the hand-made rows"
    );
}

/// #1201: a resume of a definition whose target table is gone refuses and
/// names the repair, as for a missing ledger, instead of succeeding and
/// leaving a build that fails.
#[tokio::test]
async fn a_resume_refuses_when_the_target_table_is_gone() {
    let mut f = Fixture::new(150, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    let trellis = f.trellis().await;
    trellis.apply("PAUSE TRANSFORM one").await.expect("pause");
    f.raw
        .batch_execute("drop table public.one")
        .await
        .expect("drop the target");
    let refusal = trellis
        .apply("RESUME TRANSFORM one")
        .await
        .expect_err("a resume without its target is refused")
        .to_string();
    assert!(
        refusal.contains("DROP TRANSFORM") && refusal.contains("public.one"),
        "{refusal}"
    );
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));
}

/// #1201: the sweep of a rebuild walks more hand-added rows than one
/// batch holds, and never deletes a target row the source backs: a key whose
/// source row committed after the plan read the source, with its captured
/// change still in the ring when the sweep runs, is re-derived from the
/// source, not deleted as unbacked.
#[tokio::test]
async fn a_rebuild_sweeps_many_hand_added_rows_and_keeps_a_row_the_source_backs() {
    let mut f = Fixture::new(150, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));

    let trellis = f.trellis().await;
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    f.run_chunks_by_hand(1_000).await;
    // After the plan read the source: a source row whose change waits in the
    // ring, a wrong target row for it, and three batches' worth of rows
    // nothing backs.
    f.raw
        .batch_execute(
            "insert into public.src values (5000, 1, 7); \
             insert into public.one (id, g, dbl) values (5000, 0, 0); \
             insert into public.one (id, g, dbl) \
                 select i, 0, 0 from generate_series(6001, 6025) i",
        )
        .await
        .expect("change the source and the target");
    assert!(
        has_pending(&f.raw).await.expect("has_pending"),
        "the source row's change is still in the ring"
    );
    for _ in 0..MAX_STEPS {
        if !f.step(&OPTIONS).await.progressed() {
            break;
        }
    }
    assert_eq!(
        f.rows("select (id, g, dbl)::text from public.one where id >= 5000 order by id")
            .await,
        ["(5000,1,14)"],
        "the sweep re-derived the backed row and deleted every unbacked one"
    );
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// A plain 1-1 target's build (#625 F8a) is `waiting_to_backfill ->
/// backfilling -> live`, with no `catching_up`: its chunks
/// write the target rows.
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

    let (steps, seen) = f.run("one").await;
    assert_eq!(steps.first(), Some(&Step::Planned));
    assert!(steps.contains(&Step::Chunk));
    assert_eq!(
        seen,
        BTreeSet::from(["backfilling".to_string(), "live".to_string()]),
        "the build never passes through catching_up"
    );
    assert_eq!(f.build("one").await, None);
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
    assert_eq!(
        f.count("select count(*) from public.one__ledger where __basis is null")
            .await,
        0,
        "every entry was re-derived by a chunk"
    );
    f.assert_one_oracle().await;
}

/// A worker whose claim the stale-claim sweep took (#434) writes nothing when
/// it runs the chunk after all: the claim fence stops its first write, and
/// the chunk stays undone for the next claimant.
#[tokio::test]
async fn a_worker_whose_claim_was_reclaimed_writes_nothing() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    let held = {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "held", 1, &[chunk_queue::KIND_REDERIVE])
            .await
            .expect("claim a chunk")
    };
    assert_eq!(held.len(), 1);
    {
        let mut client = f.db.pool.get().await.expect("pool");
        client
            .execute(
                "update backfill_chunks set claimed_at = now() - interval '1 hour' where id = $1",
                &[&held[0].id],
            )
            .await
            .expect("age the claim");
        let reclaimed = chunk_queue::reclaim_stale_chunks(&mut **client, OPTIONS.reclaim_ttl)
            .await
            .expect("reclaim");
        assert_eq!(reclaimed, 1, "the sweep takes the aged claim");
    }
    let before = f.count("select count(*) from public.one").await;
    let entries_before = f
        .count("select count(*) from public.one__ledger where __basis is not null")
        .await;

    build::run_claimed(&f.db.pool, &held[0], "held", &OPTIONS).await;

    assert_eq!(
        f.count("select count(*) from public.one").await,
        before,
        "the reclaimed worker wrote no target row"
    );
    assert_eq!(
        f.count("select count(*) from public.one__ledger where __basis is not null")
            .await,
        entries_before,
        "and no ledger entry"
    );
    assert_eq!(
        f.count(&format!(
            "select count(*) from backfill_chunks where id = {} and not done and claimed_by is null",
            held[0].id
        ))
        .await,
        1,
        "the chunk is left for the next claimant"
    );
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// A pause gates new claims, not a chunk a worker already holds. The last
/// chunks finishing after the pause must not flip the paused definition
/// `live`: the completion reads only a `backfilling` one.
#[tokio::test]
async fn chunks_finishing_after_a_pause_leave_the_definition_paused() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    let held = {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "held", 100, &[chunk_queue::KIND_REDERIVE])
            .await
            .expect("claim every chunk")
    };
    assert!(!held.is_empty(), "the plan job enqueued chunks");
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    for chunk in &held {
        build::run_claimed(&f.db.pool, chunk, "held", &OPTIONS).await;
    }
    let id = f.definition_id().await;
    assert!(
        !build::try_complete(&f.db.pool, id)
            .await
            .expect("try complete"),
        "a paused definition doesn't complete"
    );
    assert_eq!(f.status("one").await.as_deref(), Some("paused"));
}

// ----------------------------------------------------- the chunk queue's driver
//
// The Re-derive build's plan, rederive and sweep jobs run through the chunk
// queue (`defs::chunk_queue`): the claim, its fence, the failure ladder and the
// reclaim. Each test claims by hand and runs with `build::run_claimed`, and
// moves a backoff or a claim's age with SQL instead of waiting for it (#297).

/// Claims the lowest claimable chunk of `kind` as `who`.
async fn claim(f: &Fixture, who: &str, kind: &str) -> chunk_queue::ClaimedChunk {
    let client = f.db.pool.get().await.expect("pool");
    let mut claimed = chunk_queue::claim_chunks_of(&**client, who, 1, &[kind])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1, "a claimable {kind} chunk for {who}");
    claimed.remove(0)
}

/// Ages every claim past any reclaim TTL, as a stalled worker's is.
async fn age_claims(f: &Fixture) {
    f.raw
        .execute(
            "update backfill_chunks set claimed_at = now() - interval '1 hour' \
             where claimed_by is not null",
            &[],
        )
        .await
        .expect("age the claims");
}

/// Ends every failed chunk's backoff, so it can be claimed again at once.
async fn end_backoffs(f: &Fixture) {
    f.raw
        .execute("update backfill_chunks set next_attempt_at = null", &[])
        .await
        .expect("end the backoffs");
}

async fn pause_and_resume(f: &Fixture) {
    let trellis = f.trellis().await;
    trellis
        .apply("PAUSE TRANSFORM one")
        .await
        .expect("pause the building transform");
    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
}

/// A build with its plan job run, a chunk (`stale`) held by `held` across a
/// pause and a resume, and the resumed build's chunk (`current`) held by
/// `current`.
async fn held_across_a_resume() -> (
    Fixture,
    chunk_queue::ClaimedChunk,
    chunk_queue::ClaimedChunk,
) {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    let stale = claim(&f, "held", chunk_queue::KIND_REDERIVE).await;
    pause_and_resume(&f).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the resumed plan job");
    let current = claim(&f, "current", chunk_queue::KIND_REDERIVE).await;
    (f, stale, current)
}

async fn chunk_exists(f: &Fixture, id: i64) -> bool {
    f.count(&format!(
        "select count(*) from backfill_chunks where id = {id}"
    ))
    .await
        == 1
}

async fn chunk_is_free(f: &Fixture, id: i64) -> bool {
    f.count(&format!(
        "select count(*) from backfill_chunks \
         where id = {id} and not done and claimed_by is null and claimed_at is null"
    ))
    .await
        == 1
}

/// The stale-claim sweep deletes a chunk held across a resume, as running it
/// again would redo what the resumed build covers, and frees a current one
/// for the next claimant. The build then finishes without the discarded one.
#[tokio::test]
async fn the_reclaim_sweep_discards_a_chunk_held_across_a_resume_and_frees_a_current_one() {
    let (mut f, stale, current) = held_across_a_resume().await;
    age_claims(&f).await;
    {
        let mut client = f.db.pool.get().await.expect("pool");
        let swept = chunk_queue::reclaim_stale_chunks(&mut **client, OPTIONS.reclaim_ttl)
            .await
            .expect("reclaim");
        assert_eq!(swept, 2, "both aged claims");
    }
    assert!(!chunk_exists(&f, stale.id).await, "the stale chunk is gone");
    assert!(
        chunk_is_free(&f, current.id).await,
        "the current chunk is claimable again"
    );
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(build_rows(&f).await, 0);
    f.assert_one_oracle().await;
}

/// `release_chunk` does the same for one claim: it deletes a chunk held across
/// a resume, frees a current one, and leaves a claim another worker holds.
#[tokio::test]
async fn releasing_a_chunk_held_across_a_resume_discards_it_and_frees_a_current_one() {
    let (mut f, stale, current) = held_across_a_resume().await;
    {
        let mut client = f.db.pool.get().await.expect("pool");
        assert_eq!(
            chunk_queue::release_chunk(&mut **client, current.id, "someone else")
                .await
                .expect("release"),
            0,
            "a claim another worker holds is left alone"
        );
        assert_eq!(
            chunk_queue::release_chunk(&mut **client, stale.id, "held")
                .await
                .expect("release"),
            1
        );
        assert_eq!(
            chunk_queue::release_chunk(&mut **client, current.id, "current")
                .await
                .expect("release"),
            1
        );
    }
    assert!(!chunk_exists(&f, stale.id).await, "the stale chunk is gone");
    assert!(chunk_is_free(&f, current.id).await);
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(build_rows(&f).await, 0);
    f.assert_one_oracle().await;
}

/// The advisory lock the frozen chunk waits on.
const CHUNK_PAUSE_LOCK: i64 = 0x7a1b;

/// Runs the claimed Re-derive chunk as `who` and freezes it after its write,
/// inside its fenced transaction (`PausePoint::AfterRederiveRead`). Returns the
/// connection that holds the pause lock and the chunk's task; releasing the
/// lock lets it finish.
async fn freeze_chunk(
    f: &Fixture,
    chunk: chunk_queue::ClaimedChunk,
    who: &'static str,
    options: WorkerOptions,
) -> (Client, tokio::task::JoinHandle<()>) {
    use trellis::staging::interleave::{PausePoint, PauseScope, with_scope};

    let gate = connect(f.db.dsn()).await;
    gate.execute("select pg_advisory_lock($1)", &[&CHUNK_PAUSE_LOCK])
        .await
        .expect("take the pause lock");
    let scope = PauseScope::new();
    let reached = scope.arm(
        PausePoint::AfterRederiveRead,
        "public.one",
        CHUNK_PAUSE_LOCK,
    );
    let pool = f.db.pool.clone();
    let task = tokio::spawn(with_scope(scope, async move {
        build::run_claimed(&pool, &chunk, who, &options).await;
    }));
    reached.await.expect("the chunk reached its pause");
    (gate, task)
}

async fn release_chunk_freeze(gate: Client, task: tokio::task::JoinHandle<()>) {
    gate.execute("select pg_advisory_unlock($1)", &[&CHUNK_PAUSE_LOCK])
        .await
        .expect("release the pause lock");
    task.await.expect("the chunk task");
}

/// A chunk mid-write holds its claim row `for key share`, so the sweep passes
/// it over however old its claim looks, and the chunk finishes whole.
#[tokio::test]
async fn the_reclaim_sweep_skips_a_chunk_that_is_mid_write() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    let chunk = claim(&f, "held", chunk_queue::KIND_REDERIVE).await;
    let id = chunk.id;
    let (gate, task) = freeze_chunk(&f, chunk, "held", OPTIONS).await;
    age_claims(&f).await;
    {
        let mut client = f.db.pool.get().await.expect("pool");
        assert_eq!(
            chunk_queue::reclaim_stale_chunks(&mut **client, OPTIONS.reclaim_ttl)
                .await
                .expect("reclaim"),
            0,
            "the sweep skips the fenced chunk"
        );
    }
    assert_eq!(
        f.count(&format!(
            "select count(*) from backfill_chunks where id = {id} and claimed_by = 'held'"
        ))
        .await,
        1
    );
    release_chunk_freeze(gate, task).await;
    assert_eq!(
        f.count(&format!(
            "select count(*) from backfill_chunks where id = {id} and done"
        ))
        .await,
        1,
        "the chunk committed"
    );
    f.run("one").await;
    f.assert_one_oracle().await;
}

/// A chunk whose write takes longer than the reclaim TTL keeps its claim fresh
/// (`ChunkHeartbeat`): the claim's age, forced past the TTL while the chunk is
/// mid-write, is refreshed by the heartbeat. The wait is for one heartbeat
/// tick, bounded; nothing else here waits on the clock.
#[tokio::test]
async fn a_slow_chunk_write_keeps_its_claim_fresh() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    let chunk = claim(&f, "held", chunk_queue::KIND_REDERIVE).await;
    let id = chunk.id;
    let options = WorkerOptions {
        heartbeat_interval: Duration::from_millis(20),
        ..OPTIONS
    };
    let (gate, task) = freeze_chunk(&f, chunk, "held", options).await;
    age_claims(&f).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let fresh = f
            .count(&format!(
                "select count(*) from backfill_chunks \
                 where id = {id} and claimed_at > now() - interval '30 seconds'"
            ))
            .await;
        if fresh == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the heartbeat never refreshed the claim"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    release_chunk_freeze(gate, task).await;
    f.run("one").await;
    f.assert_one_oracle().await;
}

/// A worker claims only the chunk it runs: after one step the other chunks are
/// still unclaimed for the rest of the fleet.
#[tokio::test]
async fn a_worker_claims_only_the_chunk_it_runs() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.step(&OPTIONS).await, Step::Planned);
    let queued = f
        .count("select count(*) from backfill_chunks where kind = 'rederive'")
        .await;
    assert!(queued > 2, "the plan job enqueued several chunks: {queued}");
    assert_eq!(f.step(&OPTIONS).await, Step::Chunk);
    assert_eq!(
        f.count("select count(*) from backfill_chunks where claimed_by is not null")
            .await,
        0,
        "the step left no chunk claimed"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive' and done")
            .await,
        1,
        "and ran exactly one"
    );
    f.run("one").await;
    f.assert_one_oracle().await;
}

/// A resume deletes the chunks of the build it supersedes that are neither
/// done nor held; the done ones stay until the new build's flip takes them.
#[tokio::test]
async fn a_resume_deletes_the_undone_unclaimed_chunks() {
    let mut f = Fixture::new(100, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(3).await, 3, "the plan job, two chunks");
    let held = claim(&f, "held", chunk_queue::KIND_REDERIVE).await;
    assert!(
        f.count("select count(*) from backfill_chunks where not done and claimed_by is null")
            .await
            > 3,
        "several chunks wait"
    );
    pause_and_resume(&f).await;
    assert_eq!(
        f.count("select count(*) from backfill_chunks where not done and claimed_by is null")
            .await,
        0,
        "no undone, unclaimed chunk survives the resume"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where done")
            .await,
        3
    );
    assert!(chunk_exists(&f, held.id).await, "the held one stays");
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// `x + x` over a source whose row `(7, 3)` overflows: a data failure the
/// chunk narrows to that row's key.
const OVERFLOWING: &str = "TRANSFORM one FROM public.src SELECT x + x AS dbl";

/// A composite-key source of 300 rows `(i / 10, i % 10)` where `(7, 3)` holds
/// `x = 2147483647`, with `OVERFLOWING` registered over it.
async fn composite_overflow_fixture() -> Fixture {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    Fixture::custom(
        "create table public.src (a integer, b integer, x integer, primary key (a, b)); \
         insert into public.src select i / 10, i % 10, i from generate_series(1, 300) i; \
         update public.src set x = 2147483647 where a = 7 and b = 3",
        [
            ("a".to_string(), int4),
            ("b".to_string(), int4),
            ("x".to_string(), int4),
        ],
        &[OVERFLOWING],
    )
    .await
}

/// Runs claimed chunks by hand until the build has quarantined a key or has
/// nothing left to claim. Returns how many ran.
async fn run_until_quarantined(f: &Fixture) -> usize {
    for ran in 0..MAX_STEPS {
        if f.count("select count(*) from poison").await > 0 {
            return ran;
        }
        if f.run_chunks_by_hand(1).await == 0 {
            return ran;
        }
    }
    panic!("no key was quarantined within {MAX_STEPS} chunks");
}

/// A `rederive` chunk's data failure over a composite primary key splits until
/// it fails on one key, which is quarantined under the ring's key text for a
/// composite key (its parts joined by the unit separator), and the build
/// finishes without it.
#[tokio::test]
async fn a_composite_key_data_failure_narrows_to_its_key() {
    let mut f = composite_overflow_fixture().await;
    f.pass().await;
    run_until_quarantined(&f).await;
    assert!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive'")
            .await
            > 30,
        "the failing chunk split on the way"
    );
    f.run_chunks_by_hand(MAX_STEPS).await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.rows("select replace(key, chr(31), '|') from poison")
            .await,
        ["7|3"]
    );
    assert_eq!(
        f.count("select count(*) from public.one").await,
        299,
        "the target lacks only the quarantined key"
    );
    assert_eq!(
        f.count("select count(*) from public.one where a = 7 and b = 3")
            .await,
        0
    );
    assert_eq!(
        f.count("select count(*) from public.one where dbl = 2 * (a * 10 + b)")
            .await,
        299
    );
}

/// The key a build quarantines keeps the SQLSTATE of the failure that
/// narrowed to it (#824): an overflow is `22003`.
#[tokio::test]
async fn a_build_quarantined_key_keeps_its_sqlstate() {
    let mut f = composite_overflow_fixture().await;
    f.pass().await;
    f.run_chunks_by_hand(MAX_STEPS).await;
    assert_eq!(
        f.rows("select coalesce(sqlstate, 'none') from poison")
            .await,
        ["22003"],
        "the key keeps the SQLSTATE of the overflow"
    );
}

/// A key the build quarantined and an operator released while the build is
/// still `backfilling` reaches the target, though the chunk that quarantined
/// it has long since committed without it.
#[tokio::test]
async fn a_key_quarantined_by_the_build_and_released_mid_build_reaches_the_target() {
    let mut f = Fixture::custom(
        "create table public.src (id bigint primary key, g integer, v bigint); \
         insert into public.src select i, i % 7, i from generate_series(1, 300) i; \
         update public.src set v = 9223372036854775807 where id = 150",
        [
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
        ],
        &[ONE],
    )
    .await;
    f.pass().await;
    run_until_quarantined(&f).await;
    assert_eq!(f.rows("select key from poison").await, ["150"]);
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));
    assert!(
        f.count("select count(*) from backfill_chunks where not done")
            .await
            >= 1,
        "the chunk that quarantined the key has yet to run again"
    );

    f.raw
        .execute("update public.src set v = 150 where id = 150", &[])
        .await
        .expect("fix the row");
    // One round of the drain parks the fix behind the held key.
    trellis::staging::seal_if_active_nonempty(&mut f.raw, WAKE)
        .await
        .expect("seal");
    while let Some(seg) = apply::next_claimable_segment(&f.raw)
        .await
        .expect("next claimable segment")
    {
        apply::drain_once(&f.db.pool, seg, "drainer", 1, WAKE)
            .await
            .expect("drain_once");
    }
    f.trellis()
        .await
        .release_key("one", "public.src", "150")
        .await
        .expect("release the key");
    f.drain().await;
    assert_eq!(f.status("one").await.as_deref(), Some("backfilling"));
    assert_eq!(
        f.rows("select dbl::text from public.one where id = 150")
            .await,
        ["300"],
        "the released key reached the target before the build finished"
    );
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;
}

/// Runs the lowest claimable chunk of `kind` to its failure until the build
/// pauses its definition, clearing the backoff between attempts. Asserts that
/// every failure before the last is charged and retried, and that the
/// last one pauses the definition and leaves the chunk, with its error, as the
/// record. Returns the chunk's id.
async fn fail_until_paused(f: &Fixture, kind: &str) -> i64 {
    let mut id = None;
    for attempt in 1..=5 {
        end_backoffs(f).await;
        let chunk = claim(f, "worker", kind).await;
        assert_eq!(*id.get_or_insert(chunk.id), chunk.id, "the same chunk");
        build::run_claimed(&f.db.pool, &chunk, "worker", &OPTIONS).await;
        let row = f
            .raw
            .query_one(
                "select attempts, charged, claimed_by is null, last_error \
                 from backfill_chunks where id = $1",
                &[&chunk.id],
            )
            .await
            .expect("the chunk stays");
        assert_eq!(row.get::<_, i32>(0), attempt, "attempts");
        assert_eq!(row.get::<_, i32>(1), attempt, "every failure is charged");
        assert!(row.get::<_, bool>(2), "the claim is given up");
        assert!(row.get::<_, Option<String>>(3).is_some(), "the error");
        let want = if attempt < 5 { "backfilling" } else { "paused" };
        assert_eq!(f.status("one").await.as_deref(), Some(want), "{attempt}");
    }
    id.expect("a chunk")
}

/// A `rederive` chunk that fails in a way no key explains is charged each
/// time, and the fifth charge pauses its definition with the error on the
/// chunk's row. A resume replaces the chunk and the build runs to `live`.
#[tokio::test]
async fn an_unnarrowable_rederive_failure_pauses_and_a_resume_rebuilds() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    f.raw
        .batch_execute(
            "create function public.refuse() returns trigger language plpgsql as \
             $$ begin raise exception 'refused by the test'; end $$; \
             create trigger refuse before insert on public.one \
             for each row execute function public.refuse()",
        )
        .await
        .expect("make the target refuse its rows");
    let id = fail_until_paused(&f, chunk_queue::KIND_REDERIVE).await;
    let failure = f
        .trellis()
        .await
        .status("one")
        .await
        .expect("status")
        .expect("the definition")
        .backfill_failure
        .expect("the failure is on the status");
    assert!(
        failure.last_error.contains("refused by the test"),
        "{failure:?}"
    );
    assert_eq!(failure.attempts, 5);
    assert!(chunk_exists(&f, id).await, "the chunk stays as the record");

    f.raw
        .batch_execute("drop trigger refuse on public.one")
        .await
        .expect("fix the cause");
    pause_and_resume_only(&f).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.trellis()
            .await
            .status("one")
            .await
            .unwrap()
            .unwrap()
            .backfill_failure,
        None
    );
    assert!(!chunk_exists(&f, id).await);
    f.assert_one_oracle().await;
}

/// Resumes a transform the build paused.
async fn pause_and_resume_only(f: &Fixture) {
    f.trellis()
        .await
        .apply("RESUME TRANSFORM one")
        .await
        .expect("resume");
}

/// Gives `Fixture`'s database a login role without superuser, with every
/// privilege but `select` on the source, and runs the fixture's workers as it:
/// what a revoked source looks like to the build.
async fn run_workers_without_the_source_privilege(f: &mut Fixture) {
    f.raw
        .batch_execute(&format!(
            "create role builder login; \
             grant usage, create on schema {DEFAULT_SCHEMA} to builder; \
             grant all on all tables in schema {DEFAULT_SCHEMA} to builder; \
             grant all on all sequences in schema {DEFAULT_SCHEMA} to builder; \
             grant execute on all functions in schema {DEFAULT_SCHEMA} to builder; \
             grant all on all tables in schema public to builder; \
             revoke select on public.src from builder"
        ))
        .await
        .expect("create the unprivileged role");
    let dsn = f.db.dsn().replace("user=postgres", "user=builder");
    let config = Config::from_dsn(dsn).expect("valid dsn");
    f.db.pool = trellis::Pool::new(&config).expect("pool");
}

/// A `42501` on a `rederive` chunk's read is not narrowed to a key: it is
/// charged like any failure no row explains, up to the fifth, which pauses the
/// definition with the permission error on the chunk's row.
#[tokio::test]
async fn a_refused_rederive_chunk_is_charged_and_pauses_the_definition() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    assert_eq!(f.run_chunks_by_hand(1).await, 1, "the plan job");
    run_workers_without_the_source_privilege(&mut f).await;
    let id = fail_until_paused(&f, chunk_queue::KIND_REDERIVE).await;
    assert_eq!(
        f.count(&format!(
            "select count(*) from backfill_chunks \
             where id = {id} and last_error like '%permission denied%'"
        ))
        .await,
        1
    );
    assert_eq!(
        f.count("select count(*) from poison").await,
        0,
        "no key was quarantined"
    );
    assert_eq!(
        f.count("select count(*) from backfill_chunks where kind = 'rederive'")
            .await,
        f.count("select count(*) from backfill_chunks where kind = 'rederive' and not done")
            .await,
        "and the chunk was never split"
    );
}

/// The same for the plan job's read of the source.
#[tokio::test]
async fn a_refused_plan_read_is_charged_and_pauses_the_definition() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    run_workers_without_the_source_privilege(&mut f).await;
    let id = fail_until_paused(&f, chunk_queue::KIND_PLAN).await;
    assert_eq!(
        f.count(&format!(
            "select count(*) from backfill_chunks \
             where id = {id} and last_error like '%permission denied%'"
        ))
        .await,
        1
    );
}

/// A column paused while its target is rebuilt keeps the value it had: the
/// `rederive` chunks of the rebuild write every other column from the source
/// and leave the paused one out.
#[tokio::test]
async fn a_rerun_rederive_chunk_leaves_a_paused_column_alone() {
    let mut f = Fixture::new(60, &[ONE]).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    f.assert_one_oracle().await;

    f.raw
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('one', 'dbl', 'paused by the test', true)",
            &[],
        )
        .await
        .expect("pause one.dbl");
    f.raw
        .execute("update public.src set g = g + 100, v = v + 1000", &[])
        .await
        .expect("change every source row");
    pause_and_resume(&f).await;
    f.pass().await;
    f.run("one").await;
    assert_eq!(f.status("one").await.as_deref(), Some("live"));
    assert_eq!(
        f.count(
            "select count(*) from public.one o join public.src s using (id) \
             where o.g = s.g"
        )
        .await,
        60,
        "the rebuild wrote the other column"
    );
    assert_eq!(
        f.count("select count(*) from public.one where dbl = 2 * id")
            .await,
        60,
        "and left the paused one with the value it had"
    );
}
