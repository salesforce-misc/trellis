//! Integration tests for [`trellis::BlockingTrellis`] (`docs/decisions/0008-public-api-design.md`,
//! decision 1): the synchronous wrapper around [`trellis::Trellis`] built for
//! issue #87's future FFI embedding, where the calling convention can't
//! assume a `tokio` runtime already exists on the calling thread.
//!
//! Every test function here is a plain, non-`async` `#[test]` — deliberately
//! *not* `#[tokio::test]` — to prove `BlockingTrellis`'s public surface
//! really doesn't leak the async requirement onto its caller. The one piece
//! of scaffolding that's unavoidably async is `testkit::TestCluster`'s own
//! database provisioning (and, in `define_returns_before_backfill_completes`,
//! seeding the source table): each test spins up a scratch `tokio::Runtime`
//! purely to drive that setup, then drops it *before* constructing or
//! calling `BlockingTrellis` at all — the same "sync test, scratch runtime
//! for async engine setup" split `generative/tests/noise.rs` already uses.
//! No `.await` appears anywhere in the `BlockingTrellis`-driving portion of
//! any test below.

use testkit::TestCluster;
use trellis::config::DEFAULT_TARGET_SCHEMA;
use trellis::{BlockingTrellis, Config, TransformStatus, TrellisError, TrellisOptions};

/// (a) + (c): a full lifecycle — connect, migrate, define, definitions(), a
/// status check, and shutdown — driven entirely through `BlockingTrellis`
/// from a plain, non-async `#[test]` function, with no surrounding
/// `#[tokio::main]`/`#[tokio::test]` and no `.await` in this function at
/// all. If `BlockingTrellis` ever required its caller to already be inside a
/// `tokio` runtime, every call below would panic ("there is no reactor
/// running") rather than this test merely failing an assertion.
#[test]
fn full_lifecycle_is_synchronous_start_to_finish() {
    let cluster = TestCluster::start();

    // Scaffolding only: provision an empty database and seed a source table
    // with a scratch runtime, then drop it. `BlockingTrellis` below never
    // touches this runtime.
    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_empty_database());
    setup_runtime.block_on(async {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("create table widgets (id bigint primary key, price numeric)")
            .await
            .expect("create source table");
    });
    drop(setup_runtime);

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");

    trellis.migrate().expect("migrate (sync)");

    let def = trellis
        .apply("TRANSFORM widget_totals FROM widgets SELECT price + price AS total")
        .expect("define (sync)")
        .into_transform()
        .expect("a TRANSFORM statement registers a transform");
    assert_eq!(def.def.target, "widget_totals");

    let defs = trellis.definitions().expect("definitions (sync)");
    assert_eq!(defs.len(), 1);
    // Issue #73: `DefinitionSummary.target_table` reports the persisted,
    // fully-qualified identity now, matching `source_table`'s own
    // already-qualified precedent from issue #72.
    assert_eq!(
        defs[0].target_table,
        format!("{DEFAULT_TARGET_SCHEMA}.widget_totals")
    );

    let status = trellis.status("widget_totals").expect("status (sync)");
    assert!(
        status.is_some(),
        "a just-registered definition must report some status"
    );

    trellis.shutdown().expect("shutdown (sync)");
}

/// (b): `BlockingTrellis::apply`ing a `TRANSFORM` statement must return before
/// a plain (non-relationship) 1-1 transform's backfill actually runs — per
/// `docs/decisions/0008-public-api-design.md`'s
/// decision 1 and ADR-0016, registering a definition only records it; a
/// running staging worker dispatches its build and some running drain
/// (`application_threads`) worker elsewhere in the fleet executes it.
///
/// This `BlockingTrellis` connects with the default options — no staging, no
/// drain threads — and no other client of any kind runs against this
/// isolated test database. That makes the check below deterministic rather
/// than a timing-dependent race: with nothing anywhere dispatching the
/// build, `status()` immediately after `apply()` returns can only read back
/// `WaitingToBackfill`. Were registration still synchronous, the target
/// would already be built in-call and `status()` would read back `Live`
/// instead, with no worker involved at all — so this test does discriminate
/// the behavior it's meant to prove.
#[test]
fn define_returns_before_backfill_completes() {
    let cluster = TestCluster::start();

    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_empty_database());
    setup_runtime.block_on(async {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("create table widgets (id bigint primary key, price numeric)")
            .await
            .expect("create source table");
        // Enough rows that, even if something were watching the chunk
        // queue, a direct build wouldn't be instantaneous — belt and
        // braces on top of the "nothing is watching at all" determinism
        // this test actually relies on (see the doc comment above).
        for start in (0..2000).step_by(200) {
            let mut stmt = String::from(
                "insert into widgets (id, price) select g, g::numeric from generate_series(",
            );
            stmt.push_str(&format!("{start}, {}) g", start + 199));
            client.batch_execute(&stmt).await.expect("seed rows");
        }
    });
    drop(setup_runtime);

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");
    trellis.migrate().expect("migrate (sync)");

    trellis
        .apply("TRANSFORM widget_totals FROM widgets SELECT price + price AS total")
        .expect("define (sync)");

    let status = trellis
        .status("widget_totals")
        .expect("status (sync)")
        .expect("definition must be registered");
    assert_eq!(
        status,
        TransformStatus::WaitingToBackfill,
        "apply() must return before any staging worker (of which there are none here) could \
         possibly have dispatched the build (ADR-0016)"
    );

    trellis.shutdown().expect("shutdown (sync)");
}

/// A `BlockingTrellis` method called from a thread that already has a
/// `tokio` runtime entered must return
/// [`TrellisError::CalledFromAsyncContext`] rather than panicking —
/// `oneshot::Receiver::blocking_recv()` panics if called from inside an
/// async execution context, so `submit()` must guard against that case
/// itself. `BlockingTrellis::connect` is unaffected (it signals readiness
/// over a plain `std::sync::mpsc` channel, not `blocking_recv()`) — this
/// test connects normally first, then makes the *next* call from inside a
/// runtime.
#[test]
fn calling_from_inside_a_tokio_runtime_errors_instead_of_panicking() {
    let cluster = TestCluster::start();

    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_empty_database());
    drop(setup_runtime);

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");

    let caller_runtime = tokio::runtime::Runtime::new().expect("build caller runtime");
    let result = caller_runtime.block_on(async { trellis.migrate() });
    assert!(
        matches!(result, Err(TrellisError::CalledFromAsyncContext)),
        "expected CalledFromAsyncContext, got {result:?}"
    );

    drop(caller_runtime);
    trellis.shutdown().expect("shutdown (sync)");
}
