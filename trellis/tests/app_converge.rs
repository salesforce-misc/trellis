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
//! `await_converged_observes_a_real_write_only_after_a_deliberately_delayed_drain_worker_applies_it`
//! is the real end-to-end proof: a genuine CDC write through the full
//! `Trellis` pipeline, raced against a *deliberately delayed* drain worker so
//! a too-early `await_converged` return is a visible assertion failure, not a
//! coincidence of timing — then the target table is read back directly to
//! confirm the write actually landed (not just that the predicate flipped
//! true). `await_converged_times_out_with_a_named_staging_error` and the
//! `BlockingTrellis` test below use the same direct-ring-manipulation style
//! `trellis/tests/converge.rs` already established, to cheaply exercise the
//! timeout path and the synchronous wrapper without needing a full live
//! pipeline for those.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::StagingError;
use trellis::{BlockingTrellis, Config, Trellis, TrellisError, TrellisOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `trellis/tests/converge.rs`/`client_e2e.rs`'s own helper of the same name.
async fn connect_raw(dsn: &str) -> Client {
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

/// The real end-to-end proof: `Trellis::watermark_token`/
/// `Trellis::await_converged` against the full live pipeline (real CDC
/// intake, real ring maintenance, real apply), not a hand-staged ring.
///
/// Starts a staging-only connection (CDC intake + ring maintenance, zero
/// drain workers) so a written row is genuinely staged and sealed but *never
/// applied* until a second, drain-capable connection is started — and that
/// second connection is only started after a deliberate delay, from a
/// background task. `await_converged` is called concurrently with that
/// delay: if it returned before the delay elapsed, nothing could possibly
/// have applied the write yet, so `elapsed >= delay` below is a genuine
/// correctness assertion, not a coincidence of timing — a predicate that
/// reported `converged` too early (or a facade that raced ahead of it) would
/// fail this test, not just run fast.
#[tokio::test]
async fn await_converged_observes_a_real_write_only_after_a_deliberately_delayed_drain_worker_applies_it()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    db.pool
        .get()
        .await
        .expect("connection")
        .batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    // Register the transform through the same facade under test, with no
    // background work attached yet (module doc's "Lifecycle" two-connection
    // pattern: define first, run the live pipeline separately).
    let definer_config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let definer = Trellis::connect(definer_config, TrellisOptions::default())
        .await
        .expect("connect definer");
    definer
        .define("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    // Staging only: CDC intake + ring maintenance (seal on its normal
    // cadence), but zero drain workers — nothing here will ever apply or
    // mark-drained on its own (mirrors `client_e2e.rs`'s documented
    // "staging_worker and application_threads are genuinely independent
    // knobs" contract).
    let running_config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let running = Trellis::connect(
        running_config,
        TrellisOptions {
            staging: true,
            drain_threads: 0,
            ..Default::default()
        },
    )
    .await
    .expect("connect running (staging only)");

    let raw = connect_raw(db.dsn()).await;
    raw.execute("insert into widgets (id, price) values (1, 9)", &[])
        .await
        .expect("insert source row");

    // Per `Trellis::watermark_token`'s contract: taken after the write's own
    // commit (the `execute` above) has already returned.
    let token = running.watermark_token().await.expect("watermark_token");

    let dsn = db.dsn().to_string();
    let delay = Duration::from_millis(400);
    let drain_started = tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let config = Config::from_dsn(dsn).expect("valid dsn");
        Trellis::connect(
            config,
            TrellisOptions {
                staging: false,
                drain_threads: 2,
                ..Default::default()
            },
        )
        .await
        .expect("connect delayed drain-only trellis")
    });

    let started = Instant::now();
    let converge_result = running
        .await_converged(token, Duration::from_secs(30))
        .await;
    let elapsed = started.elapsed();

    converge_result
        .expect("await_converged should succeed once the delayed drain worker applies the write");
    assert!(
        elapsed >= delay,
        "await_converged returned after {elapsed:?}, before the deliberately delayed drain \
         worker even started (at {delay:?}) — nothing could have applied the write yet, so this \
         must not have reported convergence"
    );

    let drain = drain_started.await.expect("delayed-drain task");

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

    drain.shutdown().await.expect("shutdown drain trellis");
    running.shutdown().await.expect("shutdown running trellis");
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
        Err(TrellisError::Staging(StagingError::ConvergenceTimeout {
            token: got_token,
            waited,
        })) => {
            assert_eq!(got_token, token);
            assert!(waited >= timeout);
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
        Err(TrellisError::Staging(StagingError::ConvergenceTimeout {
            token: got_token, ..
        })) => {
            assert_eq!(got_token, token);
        }
        other => panic!("expected TrellisError::Staging(ConvergenceTimeout), got {other:?}"),
    }

    trellis.shutdown().expect("shutdown (sync)");
    drop(setup_runtime);
}
