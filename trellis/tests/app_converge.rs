//! Integration tests for the `Trellis`/`BlockingTrellis` facade's
//! read-your-writes convergence methods (issue #192, T3 of epic #189):
//! [`Trellis::watermark_token`]/[`Trellis::await_converged`] and their
//! `BlockingTrellis` counterparts.
//!
//! Before this issue, the only way an embedder could block until a write was
//! reflected in its target(s) was to reach past the facade into
//! `trellis::staging::converge` directly (`watermark_token`/`await_converged`)
//! — see that module's doc comment ("the caller-side await poll") and
//! `generative/src/backend/manual.rs::quiesce`, the one real production-shaped
//! caller. These tests exercise the same two-call pattern
//! (`manual.rs::quiesce`'s `let token = watermark_token(...); await_converged(
//! &raw, token, TIMEOUT).await`) through the public `Trellis`/`BlockingTrellis`
//! facade instead.
//!
//! None of them runs a live `Client` (issue #301). They manipulate the ring
//! directly, the style `trellis/tests/converge.rs` established, so every
//! ordering they assert on is controlled by the test rather than raced
//! against real intake/apply timing under a wall-clock budget.
//! `await_converged_waits_for_a_sealed_write_until_it_is_applied` stages a
//! real write's CDC row by hand and drains it through the engine's own apply
//! path, so the target value it reads back is one the engine computed.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{
    StagedWatermark, StagingError, apply, has_pending, retire_drained_segments, seal,
};
use trellis::{BlockingTrellis, Config, ErrorCode, Trellis, TrellisError, TrellisOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `trellis/tests/converge.rs`/`client_e2e.rs`'s own helper of the same name.
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

async fn seed_progress(client: &Client, slot: &str, confirmed_lsn: PgLsn) {
    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
            &[&slot, &confirmed_lsn],
        )
        .await
        .expect("seed replication_progress");
}

/// Inserts a bare recompute row directly into `seg_0` (the fresh, always-
/// active slot on an untouched ring) with an explicit `origin_lsn` —
/// `trellis/tests/converge.rs`'s own `insert_with_origin`, reproduced here
/// rather than shared across a test-binary boundary.
async fn insert_pending_row(client: &Client, key: &str, origin_lsn: PgLsn) {
    client
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen, origin_lsn) \
             values ('widgets', $1, 'recompute', 0, $2)",
            &[&key, &origin_lsn],
        )
        .await
        .expect("insert pending row with origin_lsn");
}

/// Seals the active segment and drains it through the engine's own apply
/// path, repeating until nothing is pending: the hand-driven stand-in for a
/// running `Client`'s drain workers (`self_check.rs`'s helper of the same
/// name).
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        let sealed = seal_active_segment(client).await;
        drain_segment(pool, sealed).await;
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// Drains every bucket of the sealed segment `seg_seq` through the engine's
/// own apply path.
async fn drain_segment(pool: &trellis::Pool, seg_seq: i64) {
    // No live `Intake` stages anything here, so there is no real staged
    // watermark to hold apply back. A saturated one never does.
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        pool,
        seg_seq,
        "app_converge_test",
        1,
        "trellis_app_converge_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .is_some()
    {}
}

/// Seals the active segment (both phases), returning the sealed `seg_seq`.
async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// `Trellis::watermark_token`/`Trellis::await_converged` against a real
/// write that is staged and sealed but not yet applied: the facade must
/// keep waiting while it is pending, return `Ok` once it has been applied,
/// and by then the value must actually be in the target table (not just a
/// flipped predicate).
///
/// No live `Client` (issue #301). This used to run a staging-only pipeline
/// and start a drain-capable one after a 400ms delay, asserting
/// `await_converged` took at least that long. That shape rests on real CDC
/// intake advancing `replication_progress` past the token, which on a quiet
/// stream waits on intake's 10s-paced keepalive persist (see
/// `Trellis::await_converged`'s doc comment), all under a fixed 30s budget.
/// Here the CDC insert is staged by hand, the same text-valued image intake
/// stages (NULL `origin_lsn`, as intake leaves it, so it gates every token),
/// and intake's confirmed position is seeded at the token. The ordering is
/// then controlled rather than timed: nothing drains the row until the test
/// itself does, so a return during the window before that is a false
/// `converged`, however slow the box is.
///
/// The facade over a live pipeline stays covered where a live pipeline is
/// the subject: `app.rs`'s
/// `the_background_client_runs_in_the_configured_schema_not_the_process_default`,
/// and `defs_floats.rs`/`defs_exact_integers.rs`'s live round-trips.
#[tokio::test]
async fn await_converged_waits_for_a_sealed_write_until_it_is_applied() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    // No background work: nothing but this test ever seals or drains.
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define over an empty source");
    // Stand in for the staging worker's discharge (ADR-0016), which takes a
    // definition over an empty source straight to `live`.
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");

    raw.execute("insert into widgets (id, price) values (1, 9)", &[])
        .await
        .expect("insert source row");
    let active: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    raw.execute(
        &format!(
            "insert into seg_{active} (src_table, key, op, lsn, new_image, hop_gen) \
             values ($1, '1', 'insert', $2, $3::text::jsonb, 0)"
        ),
        &[
            &format!("{DEFAULT_SCHEMA}.widgets"),
            &testkit::wal_insert_lsn(&raw).await,
            &r#"{"id":"1","price":"9"}"#,
        ],
    )
    .await
    .expect("stage the insert's CDC row");

    // Per `Trellis::watermark_token`'s contract: taken after the write's own
    // commit has returned. Intake has confirmed through it; only the ring
    // stands between the token and convergence.
    let token = trellis.watermark_token().await.expect("watermark_token");
    seed_progress(&raw, "slot1", token).await;
    // Sealed but not drained, as a staging-only pipeline would leave it.
    let sealed = seal_active_segment(&mut raw).await;

    {
        let converged = trellis.await_converged(token, Duration::from_secs(30));
        tokio::pin!(converged);
        // Long enough for several polls (5ms backoff, doubling). Every one
        // must see the sealed row pending. Nothing can drain it during this
        // window, so a slow box only means fewer polls, never a false failure.
        let early = tokio::time::timeout(Duration::from_millis(300), &mut converged).await;
        assert!(
            early.is_err(),
            "await_converged returned {early:?} while the write was still sealed and undrained; \
             nothing could have applied it yet"
        );

        // The same in-flight call, resumed after the drain: it has to notice
        // on a later poll, not just on a fresh call.
        drain_segment(&db.pool, sealed).await;
        drain_to_quiescence(&db.pool, &mut raw).await;
        converged
            .await
            .expect("await_converged should succeed once the write has been applied");
    }

    let price: Option<i32> = raw
        .query_opt("select price from widget_prices where id = 1", &[])
        .await
        .expect("read target table")
        .map(|row| row.get(0));
    assert_eq!(
        price,
        Some(9),
        "await_converged returned Ok, so the write must actually be reflected in the target, \
         not merely have flipped a predicate"
    );

    trellis.shutdown().await.expect("shutdown");
}

/// [`Trellis::await_converged`] must propagate a real timeout as the named
/// [`StagingError::ConvergenceTimeout`] (wrapped in [`TrellisError::Staging`]),
/// not swallow or misclassify it — mirrors
/// `trellis/tests/converge.rs::await_converged_times_out_with_a_named_error`,
/// but through the facade and its own error type. Uses direct ring
/// manipulation (no live CDC/drain workers at all) so the "never converges"
/// half of this test is deterministic rather than a race against real
/// intake/apply timing.
#[tokio::test]
async fn await_converged_times_out_with_a_named_staging_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let token = trellis.watermark_token().await.expect("watermark_token");

    let raw = connect_raw(db.dsn()).await;
    // Condition 1 (replication progress) satisfied exactly at `token`;
    // conditions 2/3 never will be — a pending row in the active slot,
    // deliberately never sealed or drained.
    seed_progress(&raw, "slot1", token).await;
    insert_pending_row(&raw, "k1", PgLsn::from(0)).await;

    let timeout = Duration::from_millis(80);
    let started = Instant::now();
    let result = trellis.await_converged(token, timeout).await;
    let elapsed = started.elapsed();

    match result {
        Err(
            err @ TrellisError::Staging(StagingError::ConvergenceTimeout {
                token: got_token,
                waited,
            }),
        ) => {
            assert_eq!(got_token, token);
            assert!(waited >= timeout);
            // Issue #586: an expired deadline is its own retryable code, not
            // `internal` (which a host reads as "a Trellis bug").
            assert_eq!(err.code(), ErrorCode::Timeout);
        }
        other => panic!("expected TrellisError::Staging(ConvergenceTimeout), got {other:?}"),
    }
    assert!(
        elapsed >= timeout,
        "await_converged returned after {elapsed:?}, before its own {timeout:?} timeout budget \
         was exhausted"
    );

    trellis.shutdown().await.expect("shutdown");
}

/// `BlockingTrellis::watermark_token`/`BlockingTrellis::await_converged` must
/// work from a plain, non-async `#[test]` with no surrounding `tokio`
/// runtime on the calling thread — `trellis/tests/blocking_trellis.rs`'s own
/// documented convention. Direct ring manipulation, same as the async
/// timeout test above, keeps this deterministic without needing a live CDC
/// pipeline.
#[test]
fn blocking_trellis_watermark_token_and_await_converged_round_trip() {
    let cluster = TestCluster::start();

    // One scratch runtime for every bit of async scaffolding this test
    // needs, kept alive (never dropped mid-test) so the raw connection's
    // background IO task — spawned onto it once, inside `connect_raw` —
    // keeps running across every later `block_on` call that reuses `raw`.
    // Each `block_on` call below is its own, non-overlapping entry into the
    // runtime, so it never coincides with a `BlockingTrellis` call on this
    // same (otherwise runtime-free) thread — the exact condition
    // `BlockingTrellis::submit`'s `CalledFromAsyncContext` guard checks for.
    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_isolated_database());
    let raw = setup_runtime.block_on(connect_raw(db.dsn()));

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");

    let token = trellis.watermark_token().expect("watermark_token (sync)");
    setup_runtime.block_on(seed_progress(&raw, "slot1", token));

    // Nothing is pending anywhere on a fresh ring with a satisfied progress
    // row — this must converge immediately, well inside a generous timeout.
    trellis
        .await_converged(token, Duration::from_secs(5))
        .expect("await_converged (sync) should converge immediately on an empty ring");

    // Now stage a pending row that will never drain, and confirm the
    // timeout path also round-trips correctly through the sync wrapper.
    setup_runtime.block_on(insert_pending_row(&raw, "k1", PgLsn::from(0)));

    let timeout = Duration::from_millis(80);
    let result = trellis.await_converged(token, timeout);
    match result {
        Err(
            err @ TrellisError::Staging(StagingError::ConvergenceTimeout {
                token: got_token, ..
            }),
        ) => {
            assert_eq!(got_token, token);
            assert_eq!(err.code(), ErrorCode::Timeout);
        }
        other => panic!("expected TrellisError::Staging(ConvergenceTimeout), got {other:?}"),
    }

    trellis.shutdown().expect("shutdown (sync)");
    drop(setup_runtime);
}
