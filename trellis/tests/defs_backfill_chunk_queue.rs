//! Integration tests for the durable backfill-chunk work queue
//! (`trellis::defs::chunk_queue`, docs/decisions/0007's "Backgrounding and
//! resumability" amendment): the claim/reclaim-stale idiom a drain worker
//! uses to finish a plain (non-relationship) 1-1 direct-build definition's
//! backfill in the background, and the CDC-race closure that makes excluding
//! a non-`live` definition from the apply path ([`transforms_for_source`])
//! safe rather than lossy.
//!
//! The staging harness (connect, stage a CDC row, seal/drain to quiescence)
//! mirrors `defs_install_definition.rs`/`apply_relationships.rs`; see those
//! files for the ring/seal mechanics.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::{TransformStatus, ValueType, chunk_queue, install_definition};
use trellis::intake::publication;
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

async fn stage_cdc(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark — this helper
    // has no live `Intake` running (these tests stage CDC rows by hand),
    // and none of this file's tests exercise guard (a) specifically, so a
    // real watermark would only ever make guard (a) reject spuriously.
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "chunk_queue_test",
            1,
            "trellis_chunk_queue_test",
            watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

fn numeric(names: &[&str]) -> std::collections::HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// A chunk that gets claimed and then abandoned (the claiming worker "dies":
/// no [`chunk_queue::finish_chunk`], no heartbeat refresh at all) must be
/// freed by [`chunk_queue::reclaim_stale_chunks`] once its claim goes stale,
/// exactly like a stale `seg_claims` row is today — and a second worker that
/// then claims and finishes it must both build the target correctly and
/// flip the definition to `Live`.
#[tokio::test]
async fn a_chunk_abandoned_by_its_claimant_is_reclaimed_and_completed_by_another_worker() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 10), (2, 20), (3, 30)",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition enumerates chunk work and returns");
    assert_eq!(
        def.status,
        TransformStatus::Backfilling,
        "a plain 1-1 definition sits at backfilling until its chunks are claimed and finished"
    );

    // The dead worker: claims the (only, for this small table) chunk and
    // does nothing else with it — no execution, no finish, no heartbeat.
    let claimed = chunk_queue::claim_chunks(&client, "dead-worker", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");
    let chunk = &claimed[0];

    // Confirm it's really unavailable to a second claimant while the first
    // claim is still fresh.
    let none_yet = chunk_queue::claim_chunks(&client, "fresh-worker", 10)
        .await
        .expect("claim_chunks while still fresh");
    assert!(
        none_yet.is_empty(),
        "a freshly-claimed chunk must not be claimable again before it goes stale"
    );

    let ttl = Duration::from_millis(300);
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;

    let reclaimed = chunk_queue::reclaim_stale_chunks(&client, ttl)
        .await
        .expect("reclaim_stale_chunks");
    assert_eq!(reclaimed, 1, "the dead worker's stale claim must be freed");

    // A fresh worker now claims exactly that chunk, executes it, and
    // finishes it.
    let re_claimed = chunk_queue::claim_chunks(&client, "fresh-worker", 10)
        .await
        .expect("re-claim after reclaim_stale_chunks");
    assert_eq!(re_claimed.len(), 1);
    assert_eq!(
        re_claimed[0].id, chunk.id,
        "the fresh worker must win exactly the reclaimed chunk"
    );

    chunk_queue::run_claimed_chunk(
        &db.pool,
        &re_claimed[0],
        "fresh-worker",
        Duration::from_secs(5),
    )
    .await
    .expect("run_claimed_chunk");
    chunk_queue::finish_chunk(&db.pool, &re_claimed[0], "fresh-worker")
        .await
        .expect("finish_chunk");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.id is null or t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the reclaiming worker's re-execution must have built the target correctly"
    );

    let status: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.t'"
            ),
            &[],
        )
        .await
        .expect("read back status")
        .get(0);
    assert_eq!(
        status, "live",
        "finishing the one remaining chunk must flip the definition to live"
    );
}

/// Issue #370: a chunk of a definition whose `TRANSFORM` clause names its own
/// target schema (`custom.t`) must be written into that schema, whatever the
/// running worker's configured default target schema is (`public` here, the
/// common case: a fleet that never changed `Config::target_schema` but
/// redirected just this one definition). The chunk executor used to render
/// its `INSERT` against the worker's configured schema instead, failing with
/// `relation "public.t" does not exist`.
#[tokio::test]
async fn a_chunk_of_an_explicitly_schema_qualified_target_is_written_into_that_schema() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 10), (2, 20), (3, 30)",
        )
        .await
        .expect("seed source and create the custom schema");

    let def = install_definition(
        &db.pool,
        "TRANSFORM custom.t FROM s SELECT a + a AS x",
        &numeric(&["a"]),
        "public",
    )
    .await
    .expect("install_definition enumerates chunk work and returns");
    assert_eq!(def.status, TransformStatus::Backfilling);

    let claimed = chunk_queue::claim_chunks(&client, "worker", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");
    chunk_queue::run_claimed_chunk(&db.pool, &claimed[0], "worker", Duration::from_secs(5))
        .await
        .expect("run_claimed_chunk must write into the definition's own declared schema");
    chunk_queue::finish_chunk(&db.pool, &claimed[0], "worker")
        .await
        .expect("finish_chunk");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join custom.t on custom.t.id = s.id \
             where custom.t.id is null or custom.t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .expect("compare custom.t against the source")
        .get(0);
    assert_eq!(mismatches, 0, "every source row must land in custom.t");

    let status: String = client
        .query_one(
            "select status from transform_definitions where id = $1",
            &[&def.id],
        )
        .await
        .expect("read back status")
        .get(0);
    assert_eq!(status, "live");
}

/// Issue #297: the composite-key counterpart of
/// `a_chunk_abandoned_by_its_claimant_is_reclaimed_and_completed_by_another_worker`
/// above — same claim/reclaim/re-execute dance, but over a genuinely
/// composite (`a`, `b`) primary key, so the reclaimed chunk's `lo`/`hi`
/// bounds round-trip through `ddl::join_pk_key`/`ddl::split_pk_key`'s
/// composite text encoding (`backfill_chunks.lo`/`.hi` are each a single
/// `text` column) rather than degenerating to a single scalar value.
///
/// A prior version of this coverage (`client_e2e.rs`'s
/// `a_composite_key_backfill_chunks_a_boundary_inside_a_group_exactly_once`,
/// from #121) proved this same "a reclaimed composite-key chunk actually
/// gets backfilled correctly" property by seeding 99999 rows and polling a
/// real, running `TrellisClient` for up to 20s — which flaked under CI
/// runner load (#297) despite the property itself needing neither a live
/// client nor any wall-clock wait: `claim_chunks`/`reclaim_stale_chunks`/
/// `run_claimed_chunk`/`finish_chunk` are the exact functions a running
/// client's maintenance loop and app workers call, and driving them directly
/// here proves the identical reclaim-then-build behavior, deterministically,
/// in well under a second. (`client_e2e.rs` keeps a much smaller live-client
/// version of this scenario, to prove the *live client's own background
/// loops* are wired up to call these functions automatically — a distinct,
/// narrower concern this test doesn't cover.)
#[tokio::test]
async fn a_composite_key_chunk_abandoned_by_its_claimant_is_reclaimed_and_completed_by_another_worker()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table widgets (a bigint, b bigint, primary key (a, b)); \
             insert into widgets (a, b) values (1, 1), (1, 2), (2, 1), (2, 2), (3, 1)",
        )
        .await
        .expect("seed widgets with a composite primary key");

    let cols = numeric(&["a", "b"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM widgets_calc FROM widgets SELECT a + b AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition enumerates chunk work and returns");
    assert_eq!(
        def.status,
        TransformStatus::Backfilling,
        "a plain 1-1 definition sits at backfilling until its chunks are claimed and finished"
    );

    // The dead worker: claims the (only, for this small table) chunk and
    // does nothing else with it — no execution, no finish, no heartbeat.
    let claimed = chunk_queue::claim_chunks(&client, "dead-worker", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");
    let chunk = &claimed[0];

    let ttl = Duration::from_millis(300);
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;

    let reclaimed = chunk_queue::reclaim_stale_chunks(&client, ttl)
        .await
        .expect("reclaim_stale_chunks");
    assert_eq!(reclaimed, 1, "the dead worker's stale claim must be freed");

    let re_claimed = chunk_queue::claim_chunks(&client, "fresh-worker", 10)
        .await
        .expect("re-claim after reclaim_stale_chunks");
    assert_eq!(re_claimed.len(), 1);
    assert_eq!(
        re_claimed[0].id, chunk.id,
        "the fresh worker must win exactly the reclaimed chunk"
    );

    chunk_queue::run_claimed_chunk(
        &db.pool,
        &re_claimed[0],
        "fresh-worker",
        Duration::from_secs(5),
    )
    .await
    .expect("run_claimed_chunk");
    chunk_queue::finish_chunk(&db.pool, &re_claimed[0], "fresh-worker")
        .await
        .expect("finish_chunk");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from widgets \
             left join widgets_calc on widgets_calc.a = widgets.a and widgets_calc.b = widgets.b \
             where widgets_calc.a is null \
                or widgets_calc.total is distinct from (widgets.a + widgets.b)::numeric",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the reclaiming worker's re-execution must have built the composite-keyed target correctly"
    );

    let status: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.widgets_calc'"
            ),
            &[],
        )
        .await
        .expect("read back status")
        .get(0);
    assert_eq!(
        status, "live",
        "finishing the one remaining chunk must flip the definition to live"
    );
}

/// public-api-design review gap #1: a chunk's entire write must not have to
/// complete inside `reclaim_ttl` to avoid being falsely reclaimed —
/// `run_claimed_chunk` must heartbeat the claim out-of-band for the whole
/// duration of the write, exactly as `staging::liveness::HeartbeatDaemon`
/// does for segment claims.
///
/// Simulated via a statement-level `before insert` trigger on the target
/// table that sleeps for longer than the (deliberately short) test
/// `reclaim_ttl` used here: the chunk's `insert ... select ... on conflict`
/// is one statement, so the trigger fires exactly once regardless of how
/// many rows it touches, making the whole write take ~10x the reclaim TTL.
/// While that write is in flight, a concurrent loop sweeps for stale claims
/// far more often than the TTL requires — mirroring the cadence
/// `maintenance_loop`/`sweep_stale_chunks_if_due` run in production — and
/// must never actually reclaim anything, because the heartbeat spawned
/// inside `run_claimed_chunk` keeps refreshing `claimed_at` well within the
/// TTL the whole time.
#[tokio::test]
async fn a_chunk_write_slower_than_the_reclaim_ttl_is_not_falsely_reclaimed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 10), (2, 20), (3, 30)",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition enumerates chunk work and returns");

    // A statement-level trigger that sleeps once per `insert` statement,
    // simulating a chunk write slow enough (~600ms) to outlast several
    // multiples of the short `reclaim_ttl` this test uses below (150ms).
    client
        .batch_execute(
            "create function _slow_backfill_write() returns trigger as $$ \
             begin perform pg_sleep(0.6); return null; end; \
             $$ language plpgsql; \
             create trigger _slow_backfill_write_trigger \
             before insert on t for each statement \
             execute function _slow_backfill_write()",
        )
        .await
        .expect("install a slow-write trigger on the target table");

    let claimed = chunk_queue::claim_chunks(&client, "slow-worker", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");
    let chunk = claimed[0].clone();

    let ttl = Duration::from_millis(150);
    // Far below `ttl`, the same margin `HeartbeatDaemon`'s own interval
    // keeps under `reclaim_ttl` in production.
    let heartbeat_interval = Duration::from_millis(20);

    let pool = db.pool.clone();
    let chunk_for_task = chunk.clone();
    let run_task = tokio::spawn(async move {
        chunk_queue::run_claimed_chunk(&pool, &chunk_for_task, "slow-worker", heartbeat_interval)
            .await
    });

    // While the chunk write is (slowly) in flight, repeatedly sweep for
    // stale claims at a cadence much faster than the write's own runtime.
    // Without Fix 1's heartbeat, this would reclaim the still-in-flight
    // claim well before the write finishes.
    let mut total_reclaimed = 0u64;
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        total_reclaimed += chunk_queue::reclaim_stale_chunks(&client, ttl)
            .await
            .expect("reclaim_stale_chunks");
    }

    run_task
        .await
        .expect("run_claimed_chunk task panicked")
        .expect("run_claimed_chunk");

    assert_eq!(
        total_reclaimed, 0,
        "the heartbeat must keep the claim fresh for the whole chunk write, even though the \
         write itself takes several multiples of the reclaim TTL"
    );

    chunk_queue::finish_chunk(&db.pool, &chunk, "slow-worker")
        .await
        .expect("finish_chunk by the original (never-reclaimed) claimant");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.id is null or t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the never-reclaimed worker must have built the target correctly"
    );

    let status: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.t'"
            ),
            &[],
        )
        .await
        .expect("read back status")
        .get(0);
    assert_eq!(status, "live");
}

/// A chunk already marked done cannot be double-completed by its original
/// claimant after the claim was reclaimed and finished by someone else —
/// `finish_chunk`'s `claimed_by = $2 and not done` scoping must leave a
/// stale claimant's late finish as a no-op rather than erroring or
/// re-triggering the definition-completion path a second time.
#[tokio::test]
async fn a_stale_claimants_late_finish_after_reclaim_is_a_no_op() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 10)",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition");

    let claimed = chunk_queue::claim_chunks(&client, "dead-worker", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1);

    let ttl = Duration::from_millis(300);
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;
    chunk_queue::reclaim_stale_chunks(&client, ttl)
        .await
        .expect("reclaim_stale_chunks");

    let re_claimed = chunk_queue::claim_chunks(&client, "fresh-worker", 10)
        .await
        .expect("re-claim");
    assert_eq!(re_claimed.len(), 1);
    chunk_queue::run_claimed_chunk(
        &db.pool,
        &re_claimed[0],
        "fresh-worker",
        Duration::from_secs(5),
    )
    .await
    .expect("run_claimed_chunk");
    chunk_queue::finish_chunk(&db.pool, &re_claimed[0], "fresh-worker")
        .await
        .expect("finish_chunk by the fresh worker");

    // The original (dead) worker's late finish must not error and must not
    // disturb the already-`live` definition.
    chunk_queue::finish_chunk(&db.pool, &claimed[0], "dead-worker")
        .await
        .expect("a stale claimant's late finish must be a harmless no-op");

    let status: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.t'"
            ),
            &[],
        )
        .await
        .expect("read back status")
        .get(0);
    assert_eq!(status, "live");
}

/// The CDC race docs/decisions/0007's amendment closes: a delta arriving on
/// a source table shared by a `live` definition and a still-`backfilling`
/// one must apply normally to the `live` definition while being excluded
/// from the `backfilling` one — not corrupting/pre-populating it with a
/// premature partial fold — and once the `backfilling` definition's chunk
/// work finishes (flipping it `live`), the parked catch-up
/// (`pending_backfill`, reused from the ring-fallback path — see
/// `defs::catalog::complete_direct_backfill`) must fold in whatever changed
/// on the source table while it sat excluded.
#[tokio::test]
async fn a_delta_is_excluded_from_a_backfilling_definition_and_discharged_once_it_goes_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             alter table s replica identity full; \
             insert into s (id, a) values (1, 10)",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);

    // Definition A: install and fully drain its chunk work now, so it's
    // `live` before the CDC delta below arrives.
    install_definition(
        &db.pool,
        "TRANSFORM a_calc FROM s SELECT a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition A");
    let claimed_a = chunk_queue::claim_chunks(&client, "worker-a", 10)
        .await
        .expect("claim A's chunk");
    assert_eq!(claimed_a.len(), 1);
    chunk_queue::run_claimed_chunk(&db.pool, &claimed_a[0], "worker-a", Duration::from_secs(5))
        .await
        .expect("run A's chunk");
    chunk_queue::finish_chunk(&db.pool, &claimed_a[0], "worker-a")
        .await
        .expect("finish A's chunk");
    let status_a: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.a_calc'"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(status_a, "live", "A must be live before the delta arrives");

    // Definition B: install (its target table now exists, one chunk
    // enumerated) but do NOT drain its chunk — claim and *execute* it (so
    // its one row is already built from `a`'s pre-delta value) without
    // finishing it, so B sits at `backfilling` for the rest of this test
    // until explicitly finished below. This is deliberately the harder case:
    // even though this row's own chunk has already committed, the
    // *definition* is still non-`live`, and the exclusion is definition-
    // scoped (docs/decisions/0007's amendment: parked per-definition, not
    // per-chunk/per-row).
    install_definition(
        &db.pool,
        "TRANSFORM b_calc FROM s SELECT a AS y",
        &cols,
        "public",
    )
    .await
    .expect("install_definition B");
    let claimed_b = chunk_queue::claim_chunks(&client, "worker-b", 10)
        .await
        .expect("claim B's chunk");
    assert_eq!(claimed_b.len(), 1);
    chunk_queue::run_claimed_chunk(&db.pool, &claimed_b[0], "worker-b", Duration::from_secs(5))
        .await
        .expect("run B's chunk");
    // Deliberately not finished yet.

    let pre_delta_y: i64 = client
        .query_one("select y::bigint from b_calc where id = 1", &[])
        .await
        .expect("read b_calc before the delta")
        .get(0);
    assert_eq!(pre_delta_y, 10, "B's chunk built from a's pre-delta value");

    // A live CDC update on `s`: a=10 -> a=99.
    client
        .execute("update s set a = 99 where id = 1", &[])
        .await
        .expect("update s.a");
    stage_cdc(
        &client,
        &format!("{DEFAULT_SCHEMA}.s"),
        "1",
        "update",
        Some(r#"{"id":1,"a":10}"#),
        Some(r#"{"id":1,"a":99}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    // A (live): the delta applied normally.
    let a_x: i64 = client
        .query_one("select x::bigint from a_calc where id = 1", &[])
        .await
        .expect("read a_calc after the delta")
        .get(0);
    assert_eq!(a_x, 99, "the live definition A must see the delta normally");

    // B (still backfilling): must NOT have been touched by the delta —
    // `dependents_of`/`transforms_for_source`'s status filter excludes it.
    let b_y_before_discharge: i64 = client
        .query_one("select y::bigint from b_calc where id = 1", &[])
        .await
        .expect("read b_calc after the delta, before B goes live")
        .get(0);
    assert_eq!(
        b_y_before_discharge, 10,
        "B must not observe the delta while still non-live — it was excluded, not folded in"
    );

    // Finish B's chunk: flips it to live and parks the pending_backfill
    // catch-up marker for `s` (see `complete_direct_backfill`).
    chunk_queue::finish_chunk(&db.pool, &claimed_b[0], "worker-b")
        .await
        .expect("finish B's chunk");
    let status_b: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.b_calc'"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(status_b, "live");

    // Immediately after going live, B still hasn't been caught up — the
    // discharge is a separate, deliberate step (`run_pending_backfills`),
    // not folded into `finish_chunk` itself.
    let b_y_still_stale: i64 = client
        .query_one("select y::bigint from b_calc where id = 1", &[])
        .await
        .expect("read b_calc immediately after going live")
        .get(0);
    assert_eq!(
        b_y_still_stale, 10,
        "the catch-up is parked, not applied synchronously by finish_chunk"
    );

    // Discharge the parked marker (the same `run_pending_backfills` event
    // the ring-fallback path already relies on) and drain the resulting
    // enumeration through the ring.
    publication::run_pending_backfills(
        &mut client,
        "trellis_chunk_queue_test",
        &trellis::staging::StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
    drain_to_quiescence(&db.pool, &mut client).await;

    let b_y_after_discharge: i64 = client
        .query_one("select y::bigint from b_calc where id = 1", &[])
        .await
        .expect("read b_calc after discharge")
        .get(0);
    assert_eq!(
        b_y_after_discharge, 99,
        "discharging the parked marker must fold in the delta B missed while backfilling"
    );

    // A must be unaffected by the redundant re-enumeration (idempotent
    // overwrite recomputes the same, already-correct value).
    let a_x_after_discharge: i64 = client
        .query_one("select x::bigint from a_calc where id = 1", &[])
        .await
        .expect("read a_calc after discharge")
        .get(0);
    assert_eq!(a_x_after_discharge, 99);
}

/// Discharges every parked `pending_backfill` marker and drains the
/// resulting enumeration through the ring. `run_pending_backfills` only
/// discharges a marker once its `xmin` fence has settled, and that fence is
/// cluster-wide (another test in this binary can briefly hold a transaction
/// open against the same cluster), so this retries the discharge — briefly,
/// and bounded — until the table is empty rather than assuming a single pass
/// settles it.
async fn discharge_pending_backfills(pool: &trellis::Pool, client: &mut Client) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        publication::run_pending_backfills(
            client,
            "trellis_chunk_queue_test",
            &trellis::staging::StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("run_pending_backfills");
        let remaining: i64 = client
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count pending_backfill")
            .get(0);
        if remaining == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "pending_backfill markers never settled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drain_to_quiescence(pool, client).await;
}

/// Issue #305: `ALTER TRANSFORM ... ADD` must close its new column's
/// backfill with the same parked catch-up a first `define`'s own chunked
/// build closes with (`defs::catalog::complete_direct_backfill`), not just
/// unpause the column and hope nothing slipped past.
///
/// The race: while the single-pass backfill runs, the new column sits in
/// `column_status`, so live CDC apply writes every *other* column of a
/// changed row but leaves the new one alone. A source row changed after the
/// backfill already read it — its delta applied while the column was still
/// paused — ends up with every other column current and the new column
/// permanently stale, since nothing re-derives it once the column unpauses.
///
/// The interleaving itself can't be forced from outside `alter_transform`,
/// so this reproduces the state it leaves behind directly (source row
/// updated, target's existing column updated to match, new column still
/// holding the backfill's pre-change value) and then asserts the catch-up
/// the edit parked repairs it.
#[tokio::test]
async fn alter_transform_add_parks_a_catch_up_that_repairs_a_row_changed_mid_backfill() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             alter table s replica identity full; \
             insert into s (id, a) values (1, 10), (2, 20)",
        )
        .await
        .expect("seed source");

    install_definition(
        &db.pool,
        "TRANSFORM a_calc FROM s SELECT a AS x",
        &numeric(&["a"]),
        "public",
    )
    .await
    .expect("install_definition");
    let claimed = chunk_queue::claim_chunks(&client, "worker", 10)
        .await
        .expect("claim the chunk");
    assert_eq!(claimed.len(), 1);
    chunk_queue::run_claimed_chunk(&db.pool, &claimed[0], "worker", Duration::from_secs(5))
        .await
        .expect("run the chunk");
    chunk_queue::finish_chunk(&db.pool, &claimed[0], "worker")
        .await
        .expect("finish the chunk");
    // Settle the define's own catch-up first, so the only marker left to
    // discharge below is one the ALTER itself parked.
    discharge_pending_backfills(&db.pool, &mut client).await;

    let statement = trellis::defs::parse_statement("ALTER TRANSFORM a_calc ADD a + a AS double_a")
        .expect("parse the ALTER");
    let trellis::defs::Statement::AlterTransform(alter) = statement else {
        panic!("expected an ALTER TRANSFORM statement, got {statement:?}");
    };
    trellis::defs::alter_transform(&db.pool, &alter)
        .await
        .expect("alter_transform");

    let target = format!("{DEFAULT_TARGET_SCHEMA}.a_calc");
    let double_a: i64 = client
        .query_one(
            &format!("select double_a::bigint from {target} where id = 1"),
            &[],
        )
        .await
        .expect("read the backfilled column")
        .get(0);
    assert_eq!(double_a, 20, "the single-pass backfill populated the row");

    // The state the race leaves behind: `s.a` moved 10 -> 50 after the
    // backfill read row 1, and its delta was applied while `double_a` was
    // still paused — `x` followed it, `double_a` didn't.
    client
        .execute("update s set a = 50 where id = 1", &[])
        .await
        .expect("update the source row");
    client
        .execute(&format!("update {target} set x = 50 where id = 1"), &[])
        .await
        .expect("apply the delta's unpaused column");

    let parked: i64 = client
        .query_one(
            "select count(*) from pending_backfill where table_name = $1",
            &[&format!("{DEFAULT_SCHEMA}.s")],
        )
        .await
        .expect("read pending_backfill")
        .get(0);
    assert_eq!(
        parked, 1,
        "ALTER TRANSFORM must park a catch-up marker for its source table when it unpauses \
         the new column, exactly as complete_direct_backfill does for a first define"
    );

    discharge_pending_backfills(&db.pool, &mut client).await;

    let rows = client
        .query(
            &format!("select id, x::bigint, double_a::bigint from {target} order by id"),
            &[],
        )
        .await
        .expect("read the target after the catch-up");
    let rows: Vec<(i64, i64, i64)> = rows
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, 50, 100), (2, 20, 40)],
        "the catch-up must re-derive the new column for the row that changed mid-backfill"
    );
}
