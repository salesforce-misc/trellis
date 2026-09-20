//! Integration tests for issue #55 (epic #49): the transform lifecycle
//! status (`waiting_to_backfill` -> `backfilling` -> `live`, plus
//! `quarantined`) actually transitions, rather than sitting wherever
//! creation first put it — see docs/observability.md's "Transform status
//! lifecycle" and docs/decisions/0009-observability-decisions.md's decision
//! 4.
//!
//! Two scenarios, each exercising a real fence/marker rather than a
//! shortcut:
//!
//! - A fresh transform whose source table's `pending_backfill` marker
//!   (`intake::publication`) is pinned by a concurrent straggler
//!   transaction sits in `waiting_to_backfill` until that transaction
//!   commits and the marker's `xmin` fence settles, then reaches `live` —
//!   mirroring `intake_robustness.rs`'s own
//!   `table_add_backfills_existing_rows_once_the_fence_settles_and_retries_safely`
//!   straggler pattern for the fence mechanics, plus asserting the status
//!   column this issue wires up.
//! - `quarantine::resume_transform` drops an (manually) `quarantined`
//!   definition back to `waiting_to_backfill` and re-parks a catch-up
//!   marker, which the same `run_pending_backfills` discharge carries
//!   through to `live` again, repopulating the target from current source
//!   state.
//!
//! Both backfill mechanisms (the chunked plain-1-1 path and the
//! direct/set-based path) reaching `live` in the ordinary (no unsettled
//! marker) case is already covered extensively by `defs_install_definition.rs`
//! and `defs_backfill_chunk_queue.rs`; this file only adds the piece those
//! didn't cover: the deferred-to-`waiting_to_backfill` path this issue adds,
//! and whole-transform quarantine resume.
//!
//! A third scenario (issue #105) drives the whole-transform fuse's *trip*
//! half for real, rather than simulating it: five distinct keys on one
//! source table each fail every real apply attempt until
//! `staging::quarantine`'s whole-transform fuse quarantines the definition,
//! then the same resume/re-backfill lifecycle as the scenario above carries
//! it back to `live`.
//!
//! A fourth scenario (issue #160) carries that one further: once resumed and
//! live again, the transform must have a *fresh* whole-transform fuse budget
//! — one new eviction must not immediately re-quarantine it just because the
//! pre-resume `poison` rows are (deliberately) still there, while a full
//! fresh threshold's worth of new evictions must.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use testkit::crash::OpenTransaction;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::{
    TransformStatus, ValueType, chunk_queue, create_definition, install_definition,
};
use trellis::intake::publication;
use trellis::staging::apply;
use trellis::staging::quarantine;
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

fn numeric(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Bare `name` qualified under [`DEFAULT_SCHEMA`] — matches
/// `tests/quarantine.rs`'s own `qualify_fixture_table`: a CDC-staged
/// `src_table` (and everything keyed off it downstream, including
/// `poison`/`transforms_for_source`) must be fully qualified.
fn qualify_fixture_table(name: &str) -> String {
    format!("{DEFAULT_SCHEMA}.{name}")
}

/// The ring table backing the currently-active segment — read from
/// `segment_pointer.ring_slot` rather than assuming `"seg_0"` the way
/// `tests/quarantine.rs`'s fixtures can (those start from a completely
/// fresh database with nothing yet sealed). This file's own fixtures run
/// real backfill/catch-up activity first, which seals and advances the ring
/// pointer before a test ever stages its own CDC rows, so the active slot
/// cannot be assumed to still be `0`.
async fn active_ring_table(client: &Client) -> String {
    let slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    format!("seg_{slot}")
}

/// Stages one image-bearing (CDC-shaped) change directly into ring table
/// `table` — the same "reach past the mechanism, insert directly"
/// convention `tests/quarantine.rs`'s own `insert_cdc_row` uses.
async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
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
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the
/// ring — the same harness `defs_install_definition.rs` uses.
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
            "status_lifecycle_test",
            1,
            "trellis_status_test",
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

/// Seals and drains repeatedly, like [`drain_to_quiescence`], but exits once
/// `target` reaches [`TransformStatus::Live`] rather than waiting for
/// `has_pending` to clear entirely. Needed whenever a scenario leaves a
/// permanently-parked `poison_held` row behind (an evicted key with no
/// releasable fix, by construction) — [`has_pending`] counts *any* parked
/// contribution as pending forever (issue #16: "it hasn't drained, it's
/// excluded from the active batch until release replays it"), so
/// `drain_to_quiescence` would spin for all 16 rounds and panic even though
/// the one thing this test actually cares about — the target reaching live
/// again — already happened.
async fn drain_until_live(pool: &trellis::Pool, client: &mut Client, target: &str) {
    // Issue #132: see `drain_to_quiescence`'s own comment — no live `Intake`
    // is running here either, so a throwaway, always-caught-up watermark is
    // correct.
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "status_lifecycle_test",
            1,
            "trellis_status_test",
            watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if status_of(client, target).await == TransformStatus::Live {
            return;
        }
    }
    panic!("{target} did not reach live within 16 seal/drain rounds");
}

/// Claims and executes every pending direct-build backfill chunk until none
/// remain — the harness stand-in for a running drain worker, matching
/// `defs_install_definition.rs`'s `drain_backfill_chunks`.
async fn drain_backfill_chunks(pool: &trellis::Pool, target_schema: &str) {
    const CLAIMED_BY: &str = "status_lifecycle_test_backfill_worker";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                target_schema,
                CLAIMED_BY,
                Duration::from_secs(5),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn status_of(client: &Client, target_table: &str) -> TransformStatus {
    // `transform_definitions.target_table` is persisted fully-qualified
    // (issue #73) — every definition this test file installs lands in
    // `DEFAULT_TARGET_SCHEMA` ("public"), so qualify the bare name callers
    // pass rather than matching against the unqualified column value.
    let qualified = format!("{DEFAULT_TARGET_SCHEMA}.{target_table}");
    let text: String = client
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&qualified],
        )
        .await
        .expect("query status")
        .get(0);
    TransformStatus::from_persisted(&text).unwrap_or_else(|| panic!("unrecognized status {text}"))
}

/// One seal/drain/retire round, with no exit condition of its own — the
/// "flush whatever is still staged" step [`drain_to_quiescence`] can't be
/// used for once a scenario has parked a permanently-unreleasable
/// `poison_held` row (see [`drain_until_live`]'s own comment on why
/// `has_pending` never clears then).
async fn drain_one_round(pool: &trellis::Pool, client: &mut Client) {
    let watermark = trellis::staging::StagedWatermark::saturated();
    let seg = seal_active_segment(client).await;
    while apply::drain_once(
        pool,
        seg,
        "status_lifecycle_test",
        1,
        "trellis_status_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .is_some()
    {}
    retire_drained_segments(client)
        .await
        .expect("retire drained segments");
}

/// Drives real evictions for `keys` on `src_table`: stages one malformed-key
/// CDC insert per key (see
/// `quarantine_trips_for_real_on_five_poisoned_keys_then_resumes_to_live`'s
/// own long comment for why a bad *key* rather than a bad *value* is what
/// charges only the row-level fuse), seals, then drains until the segment
/// finally drains — which, by construction, is once every one of `keys` has
/// crossed `DEFAULT_DEATH_THRESHOLD` real failures and been evicted to
/// `poison`. Asserts exactly that before returning.
async fn evict_keys_for_real(
    pool: &trellis::Pool,
    raw: &mut Client,
    src_table: &str,
    keys: &[&str],
) {
    let ring_table = active_ring_table(raw).await;
    for key in keys {
        insert_cdc_row(
            raw,
            &ring_table,
            src_table,
            key,
            "insert",
            None,
            Some(r#"{"a":"1"}"#),
        )
        .await;
    }
    let seg_seq = seal_active_segment(raw).await;

    // Each external `drain_once` call charges every still-failing key's
    // death counter once (`isolate_and_evict`'s probe loop) and, unless that
    // charge crosses the threshold for at least one key, surfaces the
    // failure immediately rather than looping internally — see
    // `staging::apply::classify_and_retry`'s `Isolate` arm. So reaching
    // `DEFAULT_DEATH_THRESHOLD` (5) takes five external failures, the fifth
    // of which evicts every one of `keys` together (they all fail every
    // attempt alike) and lets the retry drain succeed within that same call.
    // Issue #132: see `drain_to_quiescence`'s own comment — no live `Intake`
    // is running here either, so a throwaway, always-caught-up watermark is
    // correct.
    let watermark = trellis::staging::StagedWatermark::saturated();
    let mut real_failures = 0;
    loop {
        match apply::drain_once(
            pool,
            seg_seq,
            "status_lifecycle_test",
            1,
            "trellis_status_test",
            &watermark,
        )
        .await
        {
            Ok(Some(_)) => break,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => {
                real_failures += 1;
                assert!(
                    real_failures <= 20,
                    "did not cross the eviction threshold within a reasonable number of real \
                     attempts"
                );
            }
        }
    }
    assert!(
        real_failures >= 1,
        "the eviction must be driven by real, observed failures, not conjured"
    );

    for key in keys {
        let poisoned = raw
            .query_opt(
                "select 1 from poison where src_table = $1 and key = $2",
                &[&src_table, key],
            )
            .await
            .expect("query poison")
            .is_some();
        assert!(poisoned, "key {key} must have actually been evicted");
    }

    retire_drained_segments(raw)
        .await
        .expect("retire drained segments");
}

/// A fresh transform whose source table's publication-join fence is pinned
/// by a concurrent straggler transaction sits in `waiting_to_backfill` for
/// as long as that straggler is open — never silently `live` or
/// `backfilling` while its rows are genuinely unpopulated — then reaches
/// `backfilling`/`live` once the straggler commits and
/// `run_pending_backfills` discharges the marker, with the target correctly
/// populated from every pre-existing source row.
#[tokio::test]
async fn a_fresh_transform_waits_on_the_xmin_fence_then_reaches_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s (id bigint primary key, a numeric); \
         insert into s (id, a) select g, g from generate_series(1, 25) g; \
         create publication test_pub;",
    )
    .await
    .expect("seed source table and publication");

    // A straggler holds an xid open before the table joins the publication,
    // so the marker `reconcile_publication` leaves must name it as
    // in-flight — same pattern as
    // `intake_robustness.rs`'s `table_add_backfills_existing_rows_once_the_fence_settles_and_retries_safely`.
    let straggler = OpenTransaction::begin(db.dsn()).await;
    straggler.execute("select txid_current()").await;

    publication::reconcile_publication(&mut raw, "test_pub", &[format!("{DEFAULT_SCHEMA}.s")])
        .await
        .expect("reconcile adds s and leaves an unsettled pending_backfill marker");

    // The definition is created *while the marker is still unsettled* — the
    // scenario issue #55 closes: without the fix, this would persist
    // `backfilling` (the chunked path's usual speculative status) and
    // silently start enumerating/building right away, racing the straggler.
    let cols = numeric(&["a"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition defers instead of racing the fence");

    assert_eq!(
        def.status,
        TransformStatus::WaitingToBackfill,
        "a definition created while its source table's fence is unsettled must defer, not \
         speculatively persist backfilling/live"
    );
    assert_eq!(
        status_of(&raw, "t").await,
        TransformStatus::WaitingToBackfill,
        "the persisted row must agree with the returned Definition"
    );

    // No chunk work was enqueued and no direct build ran — the target
    // exists (DDL always runs) but is empty.
    let target_rows: i64 = raw
        .query_one("select count(*) from t", &[])
        .await
        .expect("count t")
        .get(0);
    assert_eq!(
        target_rows, 0,
        "a deferred definition's target must not be populated until the fence settles"
    );
    let chunk_count: i64 = raw
        .query_one(
            "select count(*) from backfill_chunks where definition_id = $1",
            &[&def.id],
        )
        .await
        .expect("count backfill_chunks")
        .get(0);
    assert_eq!(
        chunk_count, 0,
        "a deferred definition must not enqueue chunk work it can't safely run yet"
    );

    // A second run_pending_backfills pass while the straggler is still open
    // must leave the definition exactly where it was — no flicker, no
    // partial progress.
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills (unsettled)");
    assert_eq!(
        status_of(&raw, "t").await,
        TransformStatus::WaitingToBackfill,
        "an unsettled fence must not advance the deferred definition"
    );

    // The straggler settles.
    straggler.commit().await;

    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills (settled)");

    // `run_pending_backfills` only *stages* the enumeration into the ring;
    // draining it is what actually populates the target and is where the
    // deferred definition's own promotion to `backfilling` (transient,
    // inside that same discharge) resolves to `live`.
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        status_of(&raw, "t").await,
        TransformStatus::Live,
        "once the fence settles and the deferred backfill drains, the definition must be live"
    );

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.id is null or t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .expect("compare s and t")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the deferred backfill must populate every pre-existing source row correctly"
    );
}

/// Whole-transform quarantine resume (ADR-0003's coarser fuse tier):
/// `quarantine::resume_transform` requires `quarantined`, drops the
/// definition to `waiting_to_backfill`, and re-parks a catch-up marker that
/// `run_pending_backfills` carries back through to `live`, with the target
/// correctly re-populated. The quarantine itself is simulated here with a
/// direct status write, deliberately isolating the *resume* half of the
/// contract from the trip mechanism — see
/// `quarantine_trips_for_real_on_five_poisoned_keys_then_resumes_to_live`
/// below for the real trip (issue #105) driving this exact same lifecycle
/// end to end.
#[tokio::test]
async fn quarantine_resume_drops_to_waiting_to_backfill_and_re_backfills_to_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s2 (id bigint primary key, a numeric); \
         insert into s2 (id, a) select g, g from generate_series(1, 25) g;",
    )
    .await
    .expect("seed source table");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t2 FROM s2 SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition");
    // The plain 1-1 fast path backgrounds its build as chunk work; drive it
    // to completion (this also parks its own post-build catch-up marker —
    // `complete_direct_backfill` — which the next `run_pending_backfills`
    // call below discharges so it can't be mistaken for the marker
    // `resume_transform` parks later).
    drain_backfill_chunks(&db.pool, "public").await;
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("discharge the post-build catch-up marker");
    assert_eq!(status_of(&raw, "t2").await, TransformStatus::Live);

    // Break it: mutate a source row so the target visibly diverges, then
    // simulate the fuse tripping (no automatic trip exists yet — see the
    // module doc comment above).
    raw.execute("update s2 set a = 999 where id = 1", &[])
        .await
        .expect("mutate a source row so the divergence is checkable");
    raw.execute(
        &format!(
            "update transform_definitions set status = 'quarantined' \
             where target_table = '{DEFAULT_TARGET_SCHEMA}.t2'"
        ),
        &[],
    )
    .await
    .expect("simulate the fuse tripping");

    quarantine::resume_transform(&db.pool, "t2")
        .await
        .expect("resume_transform");

    assert_eq!(
        status_of(&raw, "t2").await,
        TransformStatus::WaitingToBackfill,
        "resume must drop straight to waiting_to_backfill, never directly to backfilling/live"
    );

    // No concurrent transaction pins the fence this time, so a single
    // discharge pass both settles and processes the re-parked marker.
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills discharges the resume's own marker");
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        status_of(&raw, "t2").await,
        TransformStatus::Live,
        "the re-backfill must reach live again, exactly like a fresh transform's own initial \
         backfill"
    );

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from s2 left join t2 on t2.id = s2.id \
             where t2.id is null or t2.x is distinct from s2.a + s2.a",
            &[],
        )
        .await
        .expect("compare s2 and t2")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the re-backfill must re-derive every row from current source state, including the row \
         mutated while quarantined"
    );
}

/// Whole-transform quarantine, tripped for real (issue #105). Before this
/// fix, nothing in the codebase ever wrote `Quarantined` to
/// `transform_definitions.status` — the sibling test above had to simulate
/// the trip with a raw UPDATE for exactly that reason (see its own doc
/// comment). This test drives the real mechanism instead:
/// `staging::quarantine::DEFAULT_TRANSFORM_DEATH_THRESHOLD` (5, same value
/// as the row- and column-level fuses) distinct keys on one source table
/// each fail every real apply attempt; `isolate_and_evict` evicts all five
/// together (every attempt fails all five alike, so each of their per-key
/// `key_deaths` counters crosses its own threshold in the same call), and
/// once `poison` holds five distinct keys for `s4`,
/// `trip_transform_fuse_if_crossed` quarantines `t4` in that same
/// transaction. From there the lifecycle is identical to the sibling test:
/// `resume_transform` drops it to `waiting_to_backfill`, and the re-backfill
/// correctly re-derives every real row from current source state (the five
/// poisoned keys are synthetic, non-numeric key strings with no
/// corresponding `s4` row at all, so they never appear in `t4` either way —
/// see the staging comment below for why a bad *key* rather than a bad
/// *value* is what this test needs).
#[tokio::test]
async fn quarantine_trips_for_real_on_five_poisoned_keys_then_resumes_to_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s4 (id bigint primary key, a numeric); \
         insert into s4 (id, a) select g, g from generate_series(1, 25) g;",
    )
    .await
    .expect("seed source table");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t4 FROM s4 SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool, "public").await;
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("discharge the post-build catch-up marker");
    // The post-build catch-up marker only *stages* its recompute triggers
    // into the still-active (unsealed) ring segment — draining it to
    // quiescence here (rather than leaving those triggers pending) keeps
    // the CDC rows this test stages next isolated in a fresh segment of
    // their own, rather than folded together with 25 unrelated recompute
    // triggers for this same source table.
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status_of(&raw, "t4").await, TransformStatus::Live);

    // Stage a real CDC change for five distinct *keys* that are themselves
    // malformed (not numeric, so casting to `s4.id`'s `bigint` at write time
    // fails) — deliberately **not** a bad calculated-field value: a bad
    // `new_image` (like `tests/quarantine.rs`'s own
    // `repeated_real_failures_cross_the_eviction_threshold_and_the_batch_still_drains`)
    // would fail *evaluation*, which `attribute_column_failure` attributes
    // to the `(t4, x)` column pair — and since all five keys fail on their
    // first attempt, that fuse (also threshold 5, but counting *distinct
    // poisoned rows* rather than *repeated attempts*) would trip in a single
    // external call and freeze the column before the row-level/
    // whole-transform fuse ever got a chance to accumulate five real
    // attempts. A bad key is a `key-shape` failure exactly like ADR-0003's
    // own example ("a key-shape/DDL failure that dooms every column's write
    // for that row alike") — [`quarantine::attribute_column_failure`] only
    // ever fires for [`trellis::defs::eval::EvalError`], so this charges
    // only the row-level fuse, letting the whole-transform fuse trip for
    // real. Staged into the *currently* active ring table, not a hardcoded
    // `"seg_0"` — the backfill/catch-up activity above has already sealed
    // and advanced the ring pointer past its initial slot.
    let s4 = qualify_fixture_table("s4");
    evict_keys_for_real(
        &db.pool,
        &mut raw,
        &s4,
        &[
            "bad-key-1",
            "bad-key-2",
            "bad-key-3",
            "bad-key-4",
            "bad-key-5",
        ],
    )
    .await;

    assert_eq!(
        status_of(&raw, "t4").await,
        TransformStatus::Quarantined,
        "five distinct evicted keys on t4's source must trip the whole-transform fuse for real"
    );

    // Mutate a formerly-poisoned source row too, so the re-backfill's
    // correctness check below actually exercises a real divergence.
    raw.execute("update s4 set a = 999 where id = 1", &[])
        .await
        .expect("mutate a source row so the divergence is checkable");

    quarantine::resume_transform(&db.pool, "t4")
        .await
        .expect("resume_transform");
    assert_eq!(
        status_of(&raw, "t4").await,
        TransformStatus::WaitingToBackfill,
        "resume must drop straight to waiting_to_backfill, never directly to backfilling/live"
    );

    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills discharges the resume's own marker");
    // Not `drain_to_quiescence`: the five bad-key evictions above parked
    // permanently-unreleasable `poison_held` rows (their key can never cast
    // to `s4.id`'s `bigint`, so releasing them would just fail identically),
    // and `has_pending` counts a parked row as pending forever — waiting for
    // it to clear would spin all 16 rounds and panic even once `t4` is
    // correctly live again.
    drain_until_live(&db.pool, &mut raw, "t4").await;

    assert_eq!(
        status_of(&raw, "t4").await,
        TransformStatus::Live,
        "the re-backfill must reach live again after a real trip, exactly like the simulated-trip \
         sibling test"
    );

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from s4 left join t4 on t4.id = s4.id \
             where t4.id is null or t4.x is distinct from s4.a + s4.a",
            &[],
        )
        .await
        .expect("compare s4 and t4")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the re-backfill must re-derive every real row from current source state, including the \
         mutated one"
    );
}

/// Regression coverage for issue #160: a resumed transform gets a **fresh**
/// whole-transform fuse budget, rather than re-tripping on the very next
/// eviction forever.
///
/// Before the fix, `resume_transform` moved the status out of `quarantined`
/// but left the `poison` rows that tripped the fuse in place, and
/// `trip_transform_fuse_if_crossed` counted *every* `poison` row for the
/// source table — so the count was already at 5 (the threshold) before any
/// new eviction even landed, and eviction number six re-quarantined the
/// transform immediately. The decided semantics (`V29__transform_fuse_rearm.sql`,
/// issue #160's option (b)) are that a resume *re-arms* the fuse by stamping
/// `transform_definitions.fuse_rearmed_at`, so only evictions after that
/// instant count: history and parked work are preserved, the budget is not.
///
/// This test pins both halves of that: exactly one new eviction after a
/// resume must **not** re-trip, and a full further threshold's worth of new
/// evictions must.
#[tokio::test]
async fn a_resumed_transform_gets_a_fresh_fuse_budget_rather_than_re_tripping_at_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s6 (id bigint primary key, a numeric); \
         insert into s6 (id, a) select g, g from generate_series(1, 5) g;",
    )
    .await
    .expect("seed source table");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t6 FROM s6 SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition");
    drain_backfill_chunks(&db.pool, "public").await;
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("discharge the post-build catch-up marker");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status_of(&raw, "t6").await, TransformStatus::Live);

    let s6 = qualify_fixture_table("s6");

    // Phase 1: trip the fuse for real, exactly like
    // `quarantine_trips_for_real_on_five_poisoned_keys_then_resumes_to_live`.
    evict_keys_for_real(
        &db.pool,
        &mut raw,
        &s6,
        &["bad-1", "bad-2", "bad-3", "bad-4", "bad-5"],
    )
    .await;
    assert_eq!(
        status_of(&raw, "t6").await,
        TransformStatus::Quarantined,
        "five distinct evicted keys must trip the whole-transform fuse"
    );

    // Resume, and carry the re-backfill through to live so the fuse is even
    // eligible to trip again (`transforms_for_source` only ever returns
    // `live` definitions).
    quarantine::resume_transform(&db.pool, "t6")
        .await
        .expect("resume_transform");
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills discharges the resume's own marker");
    drain_until_live(&db.pool, &mut raw, "t6").await;
    // Flush whatever the re-backfill's own catch-up left staged, so the
    // eviction rounds below start from a segment holding only their own
    // bad-key rows.
    drain_one_round(&db.pool, &mut raw).await;

    let rearmed: Option<std::time::SystemTime> = raw
        .query_one(
            &format!(
                "select fuse_rearmed_at from transform_definitions \
                 where target_table = '{DEFAULT_TARGET_SCHEMA}.t6'"
            ),
            &[],
        )
        .await
        .expect("read fuse_rearmed_at")
        .get(0);
    assert!(
        rearmed.is_some(),
        "resume_transform must stamp the fuse's re-arm point"
    );

    // Phase 2: exactly one new eviction. The five pre-resume `poison` rows
    // are deliberately still there (they are the fold's global exclusion
    // marker and own parked `poison_held` work) — they just must not count
    // toward the re-armed budget any more.
    evict_keys_for_real(&db.pool, &mut raw, &s6, &["bad-6"]).await;

    let poisoned_total: i64 = raw
        .query_one("select count(*) from poison where src_table = $1", &[&s6])
        .await
        .expect("count poison")
        .get(0);
    assert_eq!(
        poisoned_total, 6,
        "the resume must preserve the pre-resume poison rows (audit trail + parked work), not \
         delete them"
    );
    assert_eq!(
        status_of(&raw, "t6").await,
        TransformStatus::Live,
        "one single new eviction after a resume must not re-trip the whole-transform fuse — the \
         resumed transform gets a fresh full threshold's budget (issue #160)"
    );

    // Phase 3: four more new evictions — five fresh ones in total — must
    // re-trip it, so the re-arm rearms the fuse rather than disabling it.
    evict_keys_for_real(
        &db.pool,
        &mut raw,
        &s6,
        &["bad-7", "bad-8", "bad-9", "bad-10"],
    )
    .await;
    assert_eq!(
        status_of(&raw, "t6").await,
        TransformStatus::Quarantined,
        "a full fresh threshold's worth of post-resume evictions must re-trip the fuse"
    );
}

/// `resume_transform` refuses a target that isn't currently frozen by either
/// of ADR-0014's two pause triggers — caller error, not a silent no-op,
/// mirroring `resume_column`'s `ColumnNotPaused` discipline.
#[tokio::test]
async fn resume_transform_refuses_a_target_that_is_not_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table s3 (id bigint primary key, a numeric); \
         insert into s3 (id, a) values (1, 1);",
    )
    .await
    .expect("seed source table");

    let cols = numeric(&["a"]);
    create_definition(&db.pool, "TRANSFORM t3 FROM s3 SELECT a + a AS x", &cols)
        .await
        .expect("create_definition (ring path, live immediately)");

    let err = quarantine::resume_transform(&db.pool, "t3")
        .await
        .expect_err("t3 is live, so it is frozen by neither trigger");
    match err {
        apply::ApplyError::TransformNotPaused { transform } => {
            assert_eq!(transform, "t3");
        }
        other => panic!("expected TransformNotPaused, got {other:?}"),
    }
}

/// `resume_transform` reports `TransformNotFound` for a target with no
/// `transform_definitions` row at all, rather than a generic DB error.
#[tokio::test]
async fn resume_transform_reports_not_found_for_an_unregistered_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = quarantine::resume_transform(&db.pool, "no_such_transform")
        .await
        .expect_err("no such transform is registered");
    match err {
        apply::ApplyError::TransformNotFound { transform } => {
            assert_eq!(transform, "no_such_transform");
        }
        other => panic!("expected TransformNotFound, got {other:?}"),
    }
}

/// Regression coverage (this issue's items 2/3): with no unsettled marker at
/// creation time, both backfill mechanisms still reach `live` directly, the
/// same as before this issue's deferral logic was added — the deferral
/// check must be a no-op on the ordinary path.
#[tokio::test]
async fn both_backfill_mechanisms_still_reach_live_with_no_unsettled_marker() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table plain_s (id bigint primary key, a numeric); \
         insert into plain_s (id, a) select g, g from generate_series(1, 10) g; \
         create table agg_s (id bigint primary key, grp bigint, a numeric); \
         alter table agg_s replica identity full; \
         insert into agg_s (id, grp, a) select g, g % 3, g from generate_series(1, 10) g;",
    )
    .await
    .expect("seed source tables");

    // Chunked path: a plain (non-relationship) 1-1 definition.
    let cols = numeric(&["a"]);
    let plain = install_definition(
        &db.pool,
        "TRANSFORM plain_t FROM plain_s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition (chunked path)");
    assert_eq!(plain.status, TransformStatus::Backfilling);
    drain_backfill_chunks(&db.pool, "public").await;
    assert_eq!(status_of(&raw, "plain_t").await, TransformStatus::Live);

    // Direct/set-based path: an aggregate definition.
    let agg_cols = numeric(&["grp", "a"]);
    let agg = install_definition(
        &db.pool,
        "TRANSFORM agg_t FROM agg_s GROUP BY grp SELECT grp AS grp, SUM(a) AS total",
        &agg_cols,
        "public",
    )
    .await
    .expect("install_definition (direct/aggregate path)");
    assert_eq!(
        agg.status,
        TransformStatus::Live,
        "the direct/set-based path still builds synchronously and ends up live in-call"
    );
    assert_eq!(status_of(&raw, "agg_t").await, TransformStatus::Live);
}
