//! The drain workers' idle build probe (issue #273): `build_step` asks one
//! question, [`build::work_probe`], instead of claiming an old-build
//! chunk and then listing the Re-derive builds on every idle wake. A `false`
//! must never hide a step the old probes would have found.
//!
//! Every test steps the engine by hand and nothing waits for convergence.

use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue};
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::{apply, has_pending, retire_drained_segments};

const WAKE: &str = "build_step_probe_wake";
const AGG: &str =
    "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total";

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
                ValueType::Integer(trellis::integer::IntWidth::Int4),
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

    async fn drain(&mut self) {
        for _ in 0..16 {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next")
            {
                apply::drain_once(&self.db.pool, seg, "drainer", 1, WAKE)
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

    trellis::client::reconcile_pass(&mut f.raw, &f.db.pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    let mut worked = 0;
    for _ in 0..500 {
        f.drain().await;
        let may = f.may_exist().await;
        let step = build::work_once(&f.db.pool, "worker", &OPTIONS)
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
    trellis::client::reconcile_pass(&mut f.raw, &f.db.pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    // A plain backfill (no Re-derive build) so only the chunk conditions matter.
    f.raw
        .batch_execute("update transform_definitions set status = 'backfilling', build = null")
        .await
        .expect("backfilling");
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

/// The probe gates a chunk on its definition's status with the claim's own
/// allowlist: a frozen definition's undone, unclaimed, due chunk
/// hands out nothing, so it must not keep an idle worker awake; every
/// dispatchable status does. The claim is asked too, so the probe cannot
/// drift from it.
#[tokio::test]
async fn the_probe_follows_the_claims_definition_status_gate() {
    let mut f = Fixture::new().await;
    trellis::client::reconcile_pass(&mut f.raw, &f.db.pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    for status in trellis::defs::TransformStatus::ALL {
        f.raw
            .batch_execute(&format!(
                "update transform_definitions set build = null, status = '{}'",
                status.as_str()
            ))
            .await
            .expect("set status");
        // The claim's gate: a frozen definition is handed no new work.
        let claimable = !status.is_frozen();
        let probe = build::work_probe(&f.db.pool).await.expect("probe");
        assert_eq!(probe.may_exist, claimable, "probe at {}", status.as_str());
        let claimed = chunk_queue::claim_chunks_of(&f.raw, "w", 100, &["plan"])
            .await
            .expect("claim");
        assert_eq!(
            !claimed.is_empty(),
            claimable,
            "claim at {}",
            status.as_str()
        );
        f.raw
            .batch_execute("update backfill_chunks set claimed_by = null, claimed_at = null")
            .await
            .expect("release");
    }
}

/// A frozen definition's chunk backing off is not a clock to wake for either:
/// the resume announces itself.
#[tokio::test]
async fn a_paused_definitions_backing_off_chunk_schedules_no_wake() {
    let mut f = Fixture::new().await;
    trellis::client::reconcile_pass(&mut f.raw, &f.db.pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    f.raw
        .batch_execute(
            "update backfill_chunks set next_attempt_at = now() + interval '1 hour'; \
             update transform_definitions set status = 'paused'",
        )
        .await
        .expect("back off and pause");
    let probe = build::work_probe(&f.db.pool).await.expect("probe");
    assert_eq!((probe.may_exist, probe.next_due), (false, None));
    f.raw
        .batch_execute("update transform_definitions set status = 'backfilling', build = null")
        .await
        .expect("resume");
    let probe = build::work_probe(&f.db.pool).await.expect("probe");
    assert!(!probe.may_exist);
    assert!(
        probe.next_due.is_some(),
        "the resumed chunk's backoff is a clock again"
    );
}
