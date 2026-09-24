//! Integration tests for the worker registry (issue #144; ADR-0010 decision
//! 3): `staging::worker_registry`'s register/deregister/liveness primitives,
//! `Client::start`'s wiring of them, and the `Trellis::has_live_drain_workers`
//! health check they back (plus its staging-side counterpart,
//! `Trellis::has_live_staging_worker`, issue #428) — all run against a real, ephemeral Postgres
//! instance via the shared harness (`testkit::TestCluster`).
//!
//! `staleness_is_read_time_and_needs_no_reclaim_pass` is the one test that
//! goes straight at `staging::worker_registry` rather than through `Trellis`:
//! it backdates a row's `last_seen` directly via raw SQL (matching
//! `trellis/tests/liveness.rs`'s own convention for simulating a dead
//! worker) with a short `ttl`, so it can assert staleness within
//! milliseconds instead of waiting out `Trellis::has_live_drain_workers`'s
//! fixed 30s default — see that test's own doc comment for why "before any
//! reclaim pass runs" is the semantic under test, not an incidental detail.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::worker_registry;
use trellis::{Client as TrellisClient, ClientOptions, Config, Trellis, TrellisOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `client_e2e.rs`/`liveness.rs`'s own helper of the same name.
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

async fn worker_registry_row_count(client: &Client) -> i64 {
    client
        .query_one("select count(*) from worker_registry", &[])
        .await
        .expect("count worker_registry")
        .get(0)
}

/// The health check's core contract, exercised through the real public
/// facade (not the lower-level `staging::worker_registry` primitives): a
/// connection with no background work reports no live drain workers until a
/// *separate* connection actually starts one with `drain_threads > 0`, and
/// reports none again the instant that connection shuts down cleanly — this
/// is also this file's "clean shutdown removes the row" coverage, observed
/// through the facade rather than a raw row count.
#[tokio::test]
async fn has_live_drain_workers_tracks_a_real_drain_connection_starting_and_stopping() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // The "health check" connection itself: no background work of its own,
    // exactly the shape `docs/embedding.md`'s Phoenix/Rails sketch expects
    // an application health check to hold.
    let health_config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let health = Trellis::connect(health_config, TrellisOptions::default())
        .await
        .expect("connect health-check handle");

    assert!(
        !health
            .has_live_drain_workers()
            .await
            .expect("has_live_drain_workers before any worker exists"),
        "an empty fleet must report no live drain workers"
    );

    let worker_config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let worker = Trellis::connect(
        worker_config,
        TrellisOptions {
            staging: false,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect drain-worker handle");

    assert!(
        health
            .has_live_drain_workers()
            .await
            .expect("has_live_drain_workers while a worker runs"),
        "a running `drain_threads > 0` connection must be visible immediately, \
         before any maintenance tick — registration happens at `Client::start`"
    );

    worker.shutdown().await.expect("clean shutdown");

    assert!(
        !health
            .has_live_drain_workers()
            .await
            .expect("has_live_drain_workers after clean shutdown"),
        "a clean shutdown must remove the worker's registry row immediately, \
         not leave it to be discovered stale later"
    );

    health
        .shutdown()
        .await
        .expect("shutdown health-check handle");
}

/// Issue #428: a fleet with drain workers but no staging worker passes
/// `has_live_drain_workers` while nothing captures changes or dispatches a
/// new transform's backfill. `has_live_staging_worker` is the check that
/// catches it: `false` with only drain workers running, `true` once a
/// staging connection is up, read from a handle that runs neither.
#[tokio::test]
async fn has_live_staging_worker_needs_a_staging_connection_not_just_drain_workers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    db.pool
        .get()
        .await
        .expect("connection")
        .batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    let health = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect health-check handle");
    health
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");
    assert!(
        !health
            .has_live_staging_worker()
            .await
            .expect("has_live_staging_worker in an empty fleet"),
        "an empty fleet has no staging worker"
    );

    let drain_only = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: false,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect drain-only handle");
    assert!(
        health
            .has_live_drain_workers()
            .await
            .expect("has_live_drain_workers"),
        "the drain-only connection is a live drain worker"
    );
    assert!(
        !health
            .has_live_staging_worker()
            .await
            .expect("has_live_staging_worker with only drain workers"),
        "drain workers alone must not pass as a staging worker (issue #428)"
    );

    // `Trellis::connect` returns only after intake has connected, and intake
    // holds the producer singleton from then on, so no wait is needed.
    let staging = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 0,
            ..Default::default()
        },
    )
    .await
    .expect("connect staging handle");
    assert!(
        health
            .has_live_staging_worker()
            .await
            .expect("has_live_staging_worker with a staging connection"),
        "a running staging connection must be visible as soon as it connects"
    );

    staging.shutdown().await.expect("shutdown staging handle");
    drain_only
        .shutdown()
        .await
        .expect("shutdown drain-only handle");
    health
        .shutdown()
        .await
        .expect("shutdown health-check handle");
}

/// The other half of the "does drain_threads matter" contract: a connection
/// that runs the staging worker (CDC intake + ring maintenance) but zero
/// drain threads must not count as a live drain worker — `seg_claims`-style
/// liveness inference would get this right by accident (nothing to claim
/// without drain workers either), but this asserts the *registry* itself
/// draws the same line, since a staging-only client never calls
/// `staging::register_worker` at all.
#[tokio::test]
async fn has_live_drain_workers_is_false_for_a_staging_only_connection() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    db.pool
        .get()
        .await
        .expect("connection")
        .batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    let definer_config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let definer = Trellis::connect(definer_config, TrellisOptions::default())
        .await
        .expect("connect definer");
    definer
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    let staging_config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let staging_only = Trellis::connect(
        staging_config,
        TrellisOptions {
            staging: true,
            drain_threads: 0,
            ..Default::default()
        },
    )
    .await
    .expect("connect staging-only handle");

    assert!(
        !staging_only
            .has_live_drain_workers()
            .await
            .expect("has_live_drain_workers under a staging-only connection"),
        "staging + ring maintenance with zero drain threads must not register as a \
         live drain worker — this is exactly the misconfiguration issue #144 exists \
         to make detectable"
    );

    staging_only.shutdown().await.expect("clean shutdown");
}

/// Issue #144's explicit instruction: the registry reuses the reclaim TTL's
/// own notion of liveness rather than inventing a second one, and
/// [`worker_registry::has_live_workers`] must observe staleness as a
/// straight read-time comparison — correct the instant `ttl` has elapsed,
/// with **no dependency on [`worker_registry::reclaim_stale_workers`] (or
/// any other reclaim pass) having run first**. That's the right semantic
/// (not "false only after an explicit reclaim"): the fleet this feature most
/// needs to get right — every `drain_threads` at 0 — is also a fleet where
/// `maintenance_loop` never spawns anywhere (see `Client::run`'s own doc
/// comment), so nothing would ever run a reclaim pass to observe. A design
/// that needed one first would silently fail to detect the exact
/// misconfiguration it exists for.
#[tokio::test]
async fn staleness_is_read_time_and_needs_no_reclaim_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let ttl = Duration::from_millis(300);

    worker_registry::register_worker(&client, "dead-worker")
        .await
        .expect("register_worker");
    assert!(
        worker_registry::has_live_workers(&client, ttl)
            .await
            .expect("has_live_workers right after registering"),
        "a freshly registered worker must count as live"
    );

    // Simulate the worker dying without ever heartbeating or deregistering
    // again: backdate `last_seen` directly via raw SQL, matching
    // `trellis/tests/liveness.rs`'s own convention for `seg_claims.claimed_at`.
    client
        .execute(
            "update worker_registry set last_seen = now() - interval '10 seconds' \
             where worker_id = 'dead-worker'",
            &[],
        )
        .await
        .expect("backdate last_seen");

    // No call to `reclaim_stale_workers` anywhere in this test — the row is
    // still physically present, just stale.
    assert!(
        worker_registry_row_count(&client).await > 0,
        "the stale row must still be physically present (no reclaim pass has run)"
    );
    assert!(
        !worker_registry::has_live_workers(&client, ttl)
            .await
            .expect("has_live_workers after the row went stale"),
        "a worker whose last heartbeat is older than the reused reclaim TTL must not \
         count as live, even though nothing has deleted its row yet"
    );
}

/// Table-hygiene coverage for [`worker_registry::reclaim_stale_workers`]
/// itself (wired into `maintenance_loop` for a staging-worker client): it
/// must actually delete a stale row, and leave a live one alone — the same
/// shape `liveness::reclaim_stale`'s own test asserts for `seg_claims`.
#[tokio::test]
async fn reclaim_stale_workers_deletes_only_the_stale_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let ttl = Duration::from_millis(300);
    worker_registry::register_worker(&client, "dead-worker")
        .await
        .expect("register dead-worker");
    worker_registry::register_worker(&client, "live-worker")
        .await
        .expect("register live-worker");

    client
        .execute(
            "update worker_registry set last_seen = now() - interval '10 seconds' \
             where worker_id = 'dead-worker'",
            &[],
        )
        .await
        .expect("backdate dead-worker");

    let reclaimed = worker_registry::reclaim_stale_workers(&client, ttl)
        .await
        .expect("reclaim_stale_workers");
    assert_eq!(reclaimed, 1, "exactly the stale row must be reclaimed");

    let remaining: Vec<String> = client
        .query("select worker_id from worker_registry", &[])
        .await
        .expect("list remaining workers")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        remaining,
        vec!["live-worker".to_string()],
        "the live worker's row must survive the sweep untouched"
    );
}

/// `Client::start`/`shutdown` (the lower-level API `Trellis` itself is built
/// on) directly: registers exactly one row per `Client` — not one per
/// `application_threads` task — and removes it once `shutdown` returns,
/// mirroring `client_e2e.rs`'s own drain-only-client conventions
/// (`staging_worker: false, application_threads: N`).
#[tokio::test]
async fn clean_shutdown_removes_the_one_row_a_multi_thread_client_registered() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    assert_eq!(worker_registry_row_count(&raw).await, 0);

    let options = ClientOptions {
        staging_worker: false,
        application_threads: 3,
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    assert_eq!(
        worker_registry_row_count(&raw).await,
        1,
        "one `Client` with three app-worker tasks must still register exactly one row"
    );

    client.shutdown().await.expect("clean shutdown");

    assert_eq!(
        worker_registry_row_count(&raw).await,
        0,
        "a clean shutdown must remove the row"
    );
}

/// A staging-only `Client` (`application_threads: 0`) must never register at
/// all — the lower-level mirror of `has_live_drain_workers_is_false_for_a_staging_only_connection`.
#[tokio::test]
async fn a_staging_only_client_never_registers_a_worker_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute("create table orders (id integer primary key, a numeric, b numeric)")
        .await
        .expect("create source table");

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 0,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    assert_eq!(
        worker_registry_row_count(&raw).await,
        0,
        "staging + ring maintenance alone must never write a worker-registry row"
    );

    client.shutdown().await.expect("clean shutdown");
    assert_eq!(worker_registry_row_count(&raw).await, 0);
}
