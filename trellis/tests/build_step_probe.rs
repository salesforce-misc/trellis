//! The drain workers' idle build probe (issue #273): `build_step` asks one
//! question, [`build::work_probe`], instead of claiming an old-build
//! chunk and then listing the Re-derive builds on every idle wake. A `false`
//! must never hide a step the old probes would have found.
//!
//! Every test steps the engine by hand and nothing waits for convergence
//! (#297).

use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue};
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments};

const WAKE: &str = "build_step_probe_wake";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total";

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 10,
    drain_batch_cap: 100_000,
    heartbeat_interval: Duration::from_secs(1),
    reclaim_ttl: Duration::from_secs(30),
};

struct Fixture {
    db: TestDatabase,
    raw: Client,
    _cluster: TestCluster,
}

impl Fixture {
    async fn new() -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; \
             create table public.src (id integer primary key, g integer, v bigint); \
             insert into public.src select i, i % 7, i from generate_series(1, 50) i"
        ))
        .await
        .expect("seed");
        let columns = [
            (
                "id".to_string(),
                ValueType::Integer(trellis::IntWidth::Int4),
            ),
            ("g".to_string(), ValueType::Integer(trellis::IntWidth::Int4)),
            ("v".to_string(), ValueType::Integer(trellis::IntWidth::Int8)),
        ]
        .into_iter()
        .collect();
        trellis::defs::install_definition(&db.pool, AGG, &columns, "public")
            .await
            .expect("register");
        Self {
            db,
            raw,
            _cluster: cluster,
        }
    }

    async fn may_exist(&self) -> bool {
        build::work_probe(&self.db.pool)
            .await
            .expect("probe")
            .may_exist
    }

    /// What the two probes the step used to make find, by running them.
    async fn old_probes_find_work(&self) -> bool {
        let client = self.db.pool.get().await.expect("pool");
        let old = chunk_queue::claim_chunks(&**client, "old-probe", 1)
            .await
            .expect("claim old kinds");
        if !old.is_empty() {
            // Put it back, as if never claimed.
            self.raw
                .execute(
                    "update backfill_chunks set claimed_by = null, claimed_at = null \
                     where claimed_by = 'old-probe'",
                    &[],
                )
                .await
                .expect("release");
            return true;
        }
        let count: i64 = self
            .raw
            .query_one(
                "select count(*) from transform_definitions \
                 where status in ('backfilling', 'catching_up')",
                &[],
            )
            .await
            .expect("count")
            .get(0);
        count > 0
    }

    async fn drain(&mut self) {
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next")
            {
                apply::drain_once(&self.db.pool, seg, "drainer", 1, WAKE, &watermark)
                    .await
                    .expect("drain_once");
            }
            retire_drained_segments(&mut self.raw)
                .await
                .expect("retire");
            if !has_pending(&self.raw).await.expect("pending") {
                return;
            }
        }
        panic!("ring did not quiesce");
    }
}

/// Through a whole build, at every step: a probe that says no is never
/// followed by a step that works, and every step that works was announced.
/// Once the build is live the probe says no.
#[tokio::test]
async fn the_probe_never_hides_a_build_step() {
    let mut f = Fixture::new().await;
    assert!(!f.may_exist().await, "nothing is building before a pass");

    trellis::client::reconcile_pass(
        &mut f.raw,
        &f.db.pool,
        DEFAULT_SCHEMA,
        WAKE,
        Duration::from_secs(5),
    )
    .await
    .expect("reconcile pass");

    let mut merges = build::MergeFailures::default();
    let mut worked = 0;
    for _ in 0..500 {
        f.drain().await;
        let may = f.may_exist().await;
        assert!(
            may || !f.old_probes_find_work().await,
            "the probe said no while a former probe finds work"
        );
        let step = build::work_once(&f.db.pool, "worker", &OPTIONS, &mut merges)
            .await
            .expect("step");
        if step.progressed() {
            worked += 1;
            assert!(may, "a step worked after the probe said there was none");
        } else if !may {
            break;
        }
    }
    assert!(worked > 0, "the build ran steps");
    assert!(!f.may_exist().await, "a finished build leaves nothing");
    let status: String = f
        .raw
        .query_one("select status from transform_definitions", &[])
        .await
        .expect("status")
        .get(0);
    assert_eq!(status, "live");
}

/// Chunks the old claim would not hand out do not wake the build step: one
/// held by a worker, one backing off after a failure, one done.
#[tokio::test]
async fn the_probe_ignores_claimed_backing_off_and_done_chunks() {
    let mut f = Fixture::new().await;
    trellis::client::reconcile_pass(
        &mut f.raw,
        &f.db.pool,
        DEFAULT_SCHEMA,
        WAKE,
        Duration::from_secs(5),
    )
    .await
    .expect("reconcile pass");
    // Freeze the definition's build so only the chunk conditions matter.
    f.raw
        .batch_execute("update transform_definitions set status = 'paused'")
        .await
        .expect("pause");
    assert!(f.may_exist().await, "an unclaimed, undone chunk");

    f.raw
        .batch_execute("update backfill_chunks set claimed_by = 'peer', claimed_at = now()")
        .await
        .expect("claim");
    assert!(!f.may_exist().await, "claimed");
    f.raw
        .batch_execute(
            "update backfill_chunks set claimed_by = null, claimed_at = null, \
             next_attempt_at = now() + interval '1 hour'",
        )
        .await
        .expect("back off");
    assert!(!f.may_exist().await, "backing off");
    f.raw
        .batch_execute("update backfill_chunks set next_attempt_at = null, done = true")
        .await
        .expect("done");
    assert!(!f.may_exist().await, "done");
}

/// An old build's chunk (a 1-1 `range` or a `direct` build job) is the one
/// kind of work `building` does not list: its definition is `backfilling`
/// with no Re-derive build. The probe's chunk clause alone must find it, or
/// an idle worker would never claim it.
#[tokio::test]
async fn the_probe_finds_an_old_build_chunk_nothing_else_announces() {
    let f = Fixture::new().await;
    f.raw
        .batch_execute(
            "update transform_definitions set status = 'backfilling', build = null; \
             insert into backfill_chunks (definition_id, kind, lo, hi, fuse_rearmed_at) \
             select id, 'direct', null, null, fuse_rearmed_at from transform_definitions",
        )
        .await
        .expect("enqueue a direct build job");
    assert!(f.may_exist().await, "an old build's job is work");
    let client = f.db.pool.get().await.expect("pool");
    let claimed = chunk_queue::claim_chunks(&**client, "worker", 1)
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1, "the old claim hands it out");
    assert!(!f.may_exist().await, "held by a worker, nothing is left");
}
