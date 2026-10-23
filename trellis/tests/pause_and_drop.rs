//! Integration tests for issue #142 / ADR-0014 — the other half of a
//! definition's lifecycle: **pause**, **resume**, and **drop**.
//!
//! Every pause, resume and drop here goes through the public
//! [`trellis::Trellis`] facade (ADR-0012: the sibling crates and these suites
//! reach the engine the way a user does, not through internals), and is
//! verified against Postgres directly — reading `transform_definitions`,
//! `information_schema`, the quarantine tables and the installed capture
//! with a raw connection rather than re-asking the engine what it thinks it
//! did.
//!
//! No test runs a live pipeline (issue #301). Where a scenario needs work a
//! running `Client` would do — running a 1-1 target's backfill chunks,
//! draining CDC changes, discharging a resume's backfill marker — the test
//! does that step itself through the same engine function the `Client`
//! calls, then asserts. Nothing waits for a pipeline to converge under a
//! wall-clock budget.
//!
//! The scenarios, one per ADR-0014 decision:
//!
//! - pause is idempotent, and reaching a paused definition it didn't create
//!   is a success (`pausing_twice_is_a_no_op_success`)
//! - a pause landing while a worker holds the last backfill chunk survives
//!   that chunk finishing — issue #331
//!   (`a_chunk_finishing_after_the_pause_does_not_unpause_the_definition`)
//! - resume rebuilds by a fresh backfill rather than catching up over
//!   buffered changes, and a paused definition never pins the ring for its
//!   siblings (`resume_rebuilds_by_backfill_and_a_paused_definition_never_pins_the_ring`),
//!   and the paused definition's unclaimed chunks don't re-run on top of it —
//!   issue #332 (`resuming_discards_the_paused_definitions_unclaimed_chunks`)
//! - a drop always takes the target table and its data with it
//!   (`dropping_takes_the_target_table_and_its_data_with_it`)
//! - a live dependent refuses the drop and is named
//!   (`dropping_is_refused_and_names_the_live_dependents`), and so does a
//!   dependent in any other status short of gone — issue #231
//!   (`dropping_is_refused_by_a_dependent_in_any_status_not_only_live`)
//! - dropping an absent definition is a success
//!   (`dropping_an_unregistered_definition_is_a_no_op_success`)
//! - the target's own `column_*` quarantine bookkeeping goes with it; the
//!   shared, source-keyed poison band does not
//!   (`dropping_takes_target_owned_quarantine_rows_and_leaves_the_poison_band`)
//! - a drop leaves the source's capture to the staging worker's reconcile
//!   (`dropping_leaves_capture_to_the_staging_worker`)
//! - the target's `schema_nodes` row is actually reaped, not merely
//!   unreachable — issue #232 (`dropping_reaps_the_targets_schema_nodes_row`)

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
// Internals reaches (ADR-0012's `internals` feature, on for this crate's own
// test targets), for two reasons. The durable chunk queue's dispatch gate has
// no facade spelling, and asserting it through a hand-written copy of its own
// `where` clause is what issue #231 called out. And the ring, the chunk queue
// and the backfill discharge are driven by hand in place of a running
// pipeline (issue #301); see the module doc.
use trellis::defs::chunk_queue;
use trellis::intake::markers;
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
use trellis::{CatalogError, Config, TransformStatus, Trellis, TrellisError, TrellisOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`) with `search_path`
/// pinned — the same helper every other integration test in this directory
/// uses to check Postgres's own view of what the engine did.
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

/// The kinds of a Re-derive build's work rows (#625), which
/// `chunk_queue::claim_chunks` (the old builds' kinds only) never claims.
const REDERIVE_KINDS: [&str; 3] = [
    chunk_queue::KIND_PLAN,
    chunk_queue::KIND_REDERIVE,
    chunk_queue::KIND_SWEEP,
];

/// A define-only facade connection: no staging worker, no drain threads.
async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// `transform_definitions.status` for a bare target, straight from Postgres
/// — deliberately not [`Trellis::status`], so a test asserting on a status
/// transition isn't reading it back through the same code path that wrote it.
async fn persisted_status(raw: &Client, target: &str) -> Option<String> {
    raw.query_opt(
        "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read status")
    .map(|row| row.get(0))
}

async fn table_exists(raw: &Client, schema: &str, table: &str) -> bool {
    raw.query_one(
        "select exists(select 1 from information_schema.tables \
         where table_schema = $1 and table_name = $2)",
        &[&schema, &table],
    )
    .await
    .expect("introspect information_schema")
    .get(0)
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count query").get(0)
}

/// Claims, runs, and finishes every pending backfill chunk until none remain:
/// the hand-driven stand-in for a drain worker (`alter_transform.rs`'s helper
/// of the same name). A chunked 1-1 target reaches `live` this way with no
/// running pipeline and nothing to wait for.
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0002 (#418): registration only records a definition; the backfill
    // discharge dispatches its chunks.
    trellis::intake::markers::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    const CLAIMED_BY: &str = "pause_and_drop_test_backfill_worker";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            // A plain aggregate or plain 1-1 is the Re-derive build's (#625 F3, F5, F8a).
            trellis::staging::build::settle_builds(pool).await;
            // The staging worker's next pass: the builds' go-live catch-ups
            // take them `live` (issue #476).
            trellis::intake::markers::discharge_registrations(pool)
                .await
                .expect("discharge the go-live catch-ups");
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

/// Stages the CDC row capture would stage for `insert into orders (id, g, a)
/// values (id, g, a)`, into the active ring segment.
async fn stage_orders_insert(raw: &Client, id: i64, g: i64, a: i64) {
    let active: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    let image = format!(r#"{{"id":"{id}","g":"{g}","a":"{a}"}}"#);
    raw.execute(
        &format!(
            "insert into seg_{active} (src_table, key, op, lsn, new_image, hop_gen) \
             values ($1, $2, 'insert', $3, $4::text::jsonb, 0)"
        ),
        &[
            &format!("{DEFAULT_SCHEMA}.orders"),
            &id.to_string(),
            &testkit::wal_insert_lsn(raw).await,
            &image,
        ],
    )
    .await
    .unwrap_or_else(|e| panic!("stage cdc for orders row {id}: {e}"));
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending anywhere in the ring: the hand-driven stand-in for a running
/// `Client`'s maintenance loop and drain workers.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // No live capture stages anything here, so there is no real staged
    // watermark to hold apply back. A saturated one never does.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "pause_and_drop_test",
            1,
            "trellis_pause_and_drop_test",
            &watermark,
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
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

/// Seeds a source table. Aggregates are this file's default
/// transform shape. Registration only records one as `waiting_to_backfill`
/// (issue #419); a test that needs it `live` settles it with
/// [`markers::settle_registrations`], which dispatches and runs its one
/// direct-build job in place of the staging worker and a drain thread.
async fn seed_source(raw: &Client, table: &str, rows: i64) {
    raw.batch_execute(&format!(
        "create table {table} (id bigint primary key, g bigint, a numeric(20,0)); \
         insert into {table} (id, g, a) select s, s % 2, s from generate_series(1, {rows}) s;"
    ))
    .await
    .expect("seed source table");
}

// ---------------------------------------------------------------------
// Pause
// ---------------------------------------------------------------------

/// ADR-0014, "Pause and drop are idempotent": pause runs on Trellis's own
/// connections rather than inside a caller's migration transaction, so a
/// migration that is replayed — or interleaved with a rollback — must be safe
/// to re-run. "Did the pause land?" therefore resolves to success either way.
///
/// Also pins the gate itself: a paused definition is `paused` in the *same*
/// `transform_definitions.status` column the poison fuse writes
/// `quarantined` into, which is what makes this reuse the one freeze the
/// claim-time fold already honors instead of adding a second one.
#[tokio::test]
async fn pausing_twice_is_a_no_op_success() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 6).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("an aggregate definition registers")
        .into_transform()
        .expect("a TRANSFORM statement registers a transform");
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .expect("registered")
            .status,
        TransformStatus::WaitingToBackfill,
        "registration only records an aggregate; its build is dispatched by the backfill discharge (#419)"
    );
    markers::settle_registrations(&db.pool).await;
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("live"),
        "precondition: the direct-build job took the aggregate live"
    );

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause a live transform");
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("paused"),
        "a pause writes the same status column the fold's `status = 'live'` gate reads"
    );

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pausing an already-paused transform is a no-op success, not a conflict");
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("paused"),
        "the second pause left the row exactly as the first one did"
    );
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .map(|s| s.status),
        Some(TransformStatus::Paused),
        "the facade reports the paused state back through its own read path too"
    );
}

/// The one asymmetry between pause and drop, stated as a test: you cannot
/// freeze something that was never defined, and unlike a drop there is no
/// replayed-migration reading under which "it isn't there" is the outcome the
/// caller wanted.
#[tokio::test]
async fn pausing_an_unregistered_transform_reports_not_found() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let trellis = define_only(db.dsn()).await;

    let err = trellis
        .apply("PAUSE TRANSFORM no_such_transform")
        .await
        .expect_err("nothing by that name is registered");
    match err {
        TrellisError::Catalog(CatalogError::TransformNotFound { transform }) => {
            assert_eq!(transform, "no_such_transform");
        }
        other => panic!("expected CatalogError::TransformNotFound, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------

/// ADR-0014's two load-bearing claims about a paused definition, proved
/// together because each is the reason for the other:
///
/// 1. **A paused definition never pins the staging ring.** Holding ring
///    segments open for a paused target would wedge the ring for every
///    sibling reading the same source. So while `order_rollup` is paused, the
///    changes flowing through `orders` are still drained — and `order_echo`,
///    a sibling definition over the same source, still converges on them.
///
/// 2. **Resume therefore rebuilds by backfill, not by catch-up.** Because its
///    share of the change stream was drained for the sibling and is *not*
///    recoverable by replay, there are no buffered changes to apply on the way
///    back. Resume re-parks a `pending_backfill` marker — the exact mechanism
///    a fresh definition's own initial backfill uses — and the target comes
///    back reconciled against the *current* source, including rows that were
///    inserted while it was paused and whose change records are long gone.
///
/// The second point is what makes this test's final assertion meaningful: if
/// resume were a catch-up over buffered changes, the rows written during the
/// pause would be missing, because nothing held them.
#[tokio::test]
async fn resume_rebuilds_by_backfill_and_a_paused_definition_never_pins_the_ring() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;

    // No live pipeline (issue #301): every change below is staged by hand and
    // drained through the engine's own apply path, and the resume's backfill
    // marker is discharged and its build job run by hand, so each claim is checked the moment its
    // precondition holds instead of polled for under a wall-clock budget.
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define the transform under test");
    trellis
        .apply("TRANSFORM order_echo FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define a sibling over the same source");
    // Registration only records the two aggregates (#419); their direct-build
    // jobs take both live before one is paused.
    markers::settle_registrations(&db.pool).await;

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause one of the two siblings");

    // Where the ring stood before any of the pause-window changes reached it
    // — the baseline the retention assertion below is measured against, so
    // "nothing is held" can't pass by nothing ever having been staged.
    let active_seq_before = count(&raw, "select active_seq from segment_pointer").await;

    // Changes that arrive while `order_rollup` is paused, each mirrored by the
    // CDC row capture would stage for it. Its share of these is drained for
    // `order_echo` and is not recoverable by replay.
    raw.batch_execute(
        "insert into orders (id, g, a) select s, s % 2, s from generate_series(5, 12) s",
    )
    .await
    .expect("write to the source while one reader is paused");
    for id in 5..=12i64 {
        stage_orders_insert(&raw, id, id % 2, id).await;
    }

    // Claim 1: the sibling converges anyway, and nothing is left holding the
    // slots its rows arrived in. `drain_to_quiescence` only returns once the
    // ring has nothing pending, so a paused definition that pinned its share
    // of the ring fails it outright. The retention bookkeeping is then
    // asserted directly: a segment sits in `sealed`/`draining` until every
    // bucket of it has been applied-and-marked-drained
    // (`segments.drained_mask`, `V10__drained_mask.sql`), and `seg_claims`
    // holds a row for every bucket a worker still owns. If a paused
    // definition's share were held open for its eventual resume — the thing
    // ADR-0014's "Resume reconciles with source, not by catch-up" section rules
    // out — those segments could never finish draining, and the ring would
    // fill and wedge for `order_echo` too.
    drain_to_quiescence(&db.pool, &mut raw).await;
    let expected_total: i64 = (1..=12).sum();
    assert_eq!(
        count(
            &raw,
            &format!(
                "select coalesce(sum(total), 0)::bigint from {DEFAULT_TARGET_SCHEMA}.order_echo"
            ),
        )
        .await,
        expected_total,
        "a paused definition must not keep its sibling from converging"
    );
    assert!(
        count(&raw, "select active_seq from segment_pointer").await > active_seq_before,
        "the pause-window changes must actually have cycled through the ring"
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from segments where state in ('sealed', 'draining')"
        )
        .await,
        0,
        "a paused definition must not hold its share of the ring open"
    );
    assert_eq!(
        count(&raw, "select count(*) from seg_claims").await,
        0,
        "no bucket may still be claimed once the ring has drained"
    );

    // ...while the paused target held its stale, pre-pause value throughout.
    assert_eq!(
        count(
            &raw,
            &format!(
                "select coalesce(sum(total), 0)::bigint from {DEFAULT_TARGET_SCHEMA}.order_rollup"
            )
        )
        .await,
        (1..=4).sum::<i64>(),
        "the paused target must hold its pre-pause value, not keep folding"
    );

    trellis
        .apply("RESUME TRANSFORM order_rollup")
        .await
        .expect("resume the paused definition");

    // Claim 2, mechanism: resume hands the target to the ordinary
    // fresh-backfill path rather than replaying anything.
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("waiting_to_backfill"),
        "resume drops the definition back into the backfill lifecycle it was defined through"
    );

    // Claim 2, mechanism continued: a plain `SUM` is the Re-derive build's
    // (#625 F3). Its resume parks no marker; the staging worker's next pass
    // starts the same build a freshly registered one gets: on the group basis
    // that is one plan job, with no ledger to sweep (#1100).
    assert_eq!(
        count(&raw, "select count(*) from pending_backfill").await,
        0,
        "the resume of a re-derive shape parks no marker"
    );
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("start the rebuild");
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("backfilling"),
        "the staging worker's pass starts the resumed aggregate's rebuild"
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from backfill_chunks c \
             join transform_definitions d on d.id = c.definition_id \
             where split_part(d.target_table, '.', 2) = 'order_rollup' \
               and c.kind in ('plan', 'sweep') and not c.done"
        )
        .await,
        1,
        "the rebuild is a plan job, not a replay of buffered changes"
    );

    // Claim 2, outcome: the target comes back reconciled against the *current*
    // source — including the eight rows written while it was paused, whose
    // change records were drained for the sibling and never held for it.
    drain_backfill_chunks(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        count(
            &raw,
            &format!(
                "select coalesce(sum(total), 0)::bigint from {DEFAULT_TARGET_SCHEMA}.order_rollup"
            ),
        )
        .await,
        expected_total,
        "a resumed target must be rebuilt from current source data, not from buffered changes"
    );
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("live"),
        "the rebuild must finish the backfill lifecycle it re-entered"
    );

    trellis.shutdown().await.expect("shut down");
}

// ---------------------------------------------------------------------
// Drop
// ---------------------------------------------------------------------

/// ADR-0014, "Drop always removes the associated data": dropping a definition
/// drops its target table. There is no option to retire the definition while
/// keeping the data — keeping derived rows after removing the definition that
/// explains them has no use worth naming, and the paused state already serves
/// the caller who wants the data to stick around unmaintained.
///
/// Because the target table is Trellis-owned, dropping it is Trellis's to do.
/// The source table is the user's and stays exactly where it was.
#[tokio::test]
async fn dropping_takes_the_target_table_and_its_data_with_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 6).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM order_rollup FROM orders GROUP BY g \
            WITHOUT SOURCE INDEX SELECT sum(a) AS total",
        )
        .await
        .expect("define");
    markers::settle_registrations(&db.pool).await;
    assert!(
        table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_rollup").await,
        "precondition: the target table exists"
    );
    assert!(
        count(
            &raw,
            &format!("select count(*) from {DEFAULT_TARGET_SCHEMA}.order_rollup")
        )
        .await
            > 0,
        "precondition: the target was built"
    );

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("drop a paused definition");
    assert_eq!(
        persisted_status(&raw, "order_rollup").await,
        None,
        "the definition row is gone"
    );
    assert!(
        !table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_rollup").await,
        "the drop takes the Trellis-owned target table, and its rows, with it"
    );
    assert!(
        table_exists(&raw, DEFAULT_SCHEMA, "orders").await,
        "the source table is the user's and is never touched by a drop"
    );
    assert_eq!(
        count(&raw, "select count(*) from orders").await,
        6,
        "...nor are its rows"
    );
}

/// Issue #418: re-registering a transform after dropping it builds it again
/// from scratch through the backfill discharge, like any registration: the
/// drop leaves no marker, chunk or status behind that the new definition
/// could trip over, and the new target holds every source row.
#[tokio::test]
async fn a_transform_registered_again_after_its_drop_is_built_afresh() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 40).await;
    let text = "TRANSFORM order_doubles FROM orders SELECT a + a AS x";

    let trellis = define_only(db.dsn()).await;
    trellis.apply(text).await.expect("define");
    drain_backfill_chunks(&db.pool).await;
    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("live")
    );
    trellis
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM order_doubles")
        .await
        .expect("drop");
    raw.batch_execute("delete from orders where id > 30")
        .await
        .expect("change the source while nothing reads it");

    trellis.apply(text).await.expect("define again");
    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("waiting_to_backfill")
    );
    drain_backfill_chunks(&db.pool).await;
    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("live")
    );
    assert_eq!(
        count(
            &raw,
            &format!(
                "select count(*) from orders o \
                 full join {DEFAULT_TARGET_SCHEMA}.order_doubles t on t.id = o.id \
                 where t.x is distinct from o.a + o.a"
            ),
        )
        .await,
        0,
        "the new target is built from the source as it is now"
    );
    // #623 D2: the drop took the old ledger, so registering again could
    // create a new one. The Re-derive build of a plain 1-1 (#625 F8a) puts
    // every key it writes on the 1-1 slim ledger (#623 D6), so the new
    // ledger holds the 30 surviving keys and none of the 40 the dropped
    // target's build entered.
    assert_eq!(
        count(
            &raw,
            &format!("select count(*) from {DEFAULT_TARGET_SCHEMA}.order_doubles__ledger")
        )
        .await,
        30,
        "the new target has a ledger of its own"
    );
}

/// Issue #232: the node reap inside `drop_transform` decides whether the
/// target's `schema_nodes` row can go with a `not exists (... from
/// transform_definitions ...)` guard. That guard used to run *before* the
/// `transform_definitions` row itself was deleted, so it always found the
/// about-to-vanish row still there and never reaped the node — every drop
/// left an orphaned `schema_nodes` row behind. Confirmed benign at the time
/// (unreachable by `all_source_tables`'s walk, unlike #231's surviving
/// *edge*), but still worth actually reaping rather than leaving litter.
#[tokio::test]
async fn dropping_reaps_the_targets_schema_nodes_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 3).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define");
    assert_eq!(
        count(
            &raw,
            &format!(
                "select count(*) from schema_nodes \
                 where table_name = '{DEFAULT_TARGET_SCHEMA}.order_rollup'"
            )
        )
        .await,
        1,
        "precondition: the target has a schema_nodes row"
    );

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("drop a paused definition");

    assert_eq!(
        count(
            &raw,
            &format!(
                "select count(*) from schema_nodes \
                 where table_name = '{DEFAULT_TARGET_SCHEMA}.order_rollup'"
            )
        )
        .await,
        0,
        "the target's schema_nodes row is actually reaped, not left orphaned (issue #232)"
    );
}

/// ADR-0014, "Drops go in reverse dependency order — no cascade": Trellis
/// does not cascade the removal, and does not leave a dependent silently
/// deriving from a table that is about to disappear. It refuses, and names
/// the blockers so the order to retire them in is explicit rather than
/// something the operator has to reconstruct.
///
/// Retiring the dependent first then unblocks the drop — the reverse
/// dependency order the ADR asks for, demonstrated rather than asserted.
#[tokio::test]
async fn dropping_is_refused_and_names_the_live_dependents() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 6).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define the upstream");
    // A transform chains only off a live target; the upstream's direct-build
    // job takes it there (#419).
    markers::settle_registrations(&db.pool).await;
    trellis
        .apply("TRANSFORM grand_total FROM order_rollup GROUP BY g WITHOUT SOURCE INDEX SELECT count(*) AS t")
        .await
        .expect("define a transform chained off the first one's target");
    markers::settle_registrations(&db.pool).await;
    assert_eq!(
        persisted_status(&raw, "grand_total").await.as_deref(),
        Some("live"),
        "precondition: the dependent is live, the status this test's refusal is named for"
    );

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");

    let err = trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect_err("a live definition still derives from this target");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
            ..
        }) => {
            assert_eq!(subject, "order_rollup");
            assert_eq!(
                dependents,
                vec!["grand_total".to_string()],
                "the refusal names the blocker, so the order to retire in is explicit"
            );
        }
        other => panic!("expected CatalogError::DependentsBlockDrop, got {other:?}"),
    }
    assert!(
        table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_rollup").await,
        "a refused drop writes nothing at all — the check runs before any mutation"
    );

    // Reverse dependency order: retire the leaf, and the upstream drop is
    // no longer blocked.
    trellis
        .apply("PAUSE TRANSFORM grand_total")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM grand_total")
        .await
        .expect("drop the dependent first");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("with nothing left deriving from it, the upstream drops cleanly");

    assert_eq!(
        count(&raw, "select count(*) from transform_definitions").await,
        0,
        "both definitions are gone"
    );
}

/// Issue #231: the refusal's question is "is anything still registered that
/// could need this target?", not "is anything *live* on it right now". Every
/// status other than gone answers yes, and for two different reasons:
///
/// - a `waiting_to_backfill`/`backfilling` dependent is building *from* this
///   target at this moment, so dropping it fails that build mid-flight or
///   leaves the dependent holding a partial result;
/// - a `paused`/`quarantined` dependent is worse, not better: ADR-0014's
///   resume rebuilds by a *fresh backfill from source*, so a dependent frozen
///   over a source that has been dropped can never be resumed at all. Letting
///   the drop through would trade a recoverable refusal for an unrecoverable
///   definition.
///
/// The dependent's status is set directly here rather than provoked through a
/// real backfill or poisoning, because what's under test is which statuses
/// block a drop, not how a definition comes to be in one.
#[tokio::test]
async fn dropping_is_refused_by_a_dependent_in_any_status_not_only_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 6).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define the upstream");
    // A transform chains only off a live target; the upstream's direct-build
    // job takes it there (#419).
    markers::settle_registrations(&db.pool).await;
    trellis
        .apply("TRANSFORM grand_total FROM order_rollup GROUP BY g WITHOUT SOURCE INDEX SELECT count(*) AS t")
        .await
        .expect("define a transform chained off the first one's target");
    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause the target under test");

    for status in [
        "waiting_to_backfill",
        "backfilling",
        "quarantined",
        "paused",
    ] {
        raw.execute(
            "update transform_definitions set status = $1 \
             where split_part(target_table, '.', 2) = 'grand_total'",
            &[&status],
        )
        .await
        .expect("put the dependent in the status under test");

        let err = trellis
            .apply("DROP TRANSFORM order_rollup")
            .await
            .expect_err(&format!("a '{status}' dependent still needs this target"));
        match err {
            TrellisError::Catalog(CatalogError::DependentsBlockDrop {
                subject,
                dependents,
                ..
            }) => {
                assert_eq!(subject, "order_rollup");
                assert_eq!(
                    dependents,
                    vec!["grand_total".to_string()],
                    "a '{status}' dependent is named just as a live one is"
                );
            }
            other => {
                panic!("expected DependentsBlockDrop for a '{status}' dependent, got {other:?}")
            }
        }
        assert!(
            table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_rollup").await,
            "the refused drop wrote nothing, even from inside its own transaction"
        );
    }

    // Fully retired — the one state that does not block — and the upstream
    // drops. Reverse dependency order, exactly as before.
    trellis
        .apply("DROP TRANSFORM grand_total")
        .await
        .expect("the dependent is frozen, so it can be dropped");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("with the dependent gone rather than merely not-live, the drop proceeds");
    assert_eq!(
        count(&raw, "select count(*) from transform_definitions").await,
        0,
        "both definitions are gone"
    );
}

/// The other half of ADR-0014's idempotency clause. A migration rollback can
/// reasonably run a drop twice, or run one against a definition an earlier
/// rollback already reaped; "is it already gone?" resolves to success.
#[tokio::test]
async fn dropping_an_unregistered_definition_is_a_no_op_success() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 3).await;

    let trellis = define_only(db.dsn()).await;

    trellis
        .apply("DROP TRANSFORM never_defined")
        .await
        .expect("dropping something that was never defined is a success");

    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define");
    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("first drop");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("a replayed drop is a no-op success, not a NotFound");

    assert!(
        !table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_rollup").await,
        "the replayed drop did not resurrect or re-drop anything"
    );
}

/// There is no direct live-to-gone edge in the lifecycle. Quiescing through
/// the pause is a precondition, so the removal never has to reason about a
/// fold still dispatching to the target.
#[tokio::test]
async fn dropping_a_live_definition_is_refused_until_it_is_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 3).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define");
    markers::settle_registrations(&db.pool).await;

    let err = trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect_err("a live definition cannot be dropped out from under the fold");
    match err {
        TrellisError::Catalog(CatalogError::TransformNotPaused { transform, status }) => {
            assert_eq!(transform, "order_rollup");
            assert_eq!(status, TransformStatus::Live);
        }
        other => panic!("expected CatalogError::TransformNotPaused, got {other:?}"),
    }
    assert!(
        table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_rollup").await,
        "the refused drop wrote nothing"
    );
}

/// ADR-0014, "Quarantine state follows its owner" and "In-flight work is
/// quiesced by the pause, never by deleting shared state" — the two halves of
/// one assertion:
///
/// - the target's own per-column quarantine bookkeeping (`column_status`,
///   `column_deaths`, `column_failures`, `column_pause_cascades`) is keyed to
///   the target and goes with it; its forensic value is about rows that are
///   about to stop existing.
/// - the whole-key poison band (`poison`, `poison_held`, `key_deaths`) and the
///   fuse gate are per transform (#799): the dropped definition's rows go
///   with it, by `on delete cascade`, and a sibling reading the same source
///   keeps its own, exactly as the drop found them. A drain worker may be
///   mid-batch over that source right now, for the sibling that is still
///   live.
///
/// The rows are seeded directly rather than provoked through a real poisoning,
/// because what's under test is which rows a drop removes, not how they came
/// to exist.
#[tokio::test]
async fn dropping_takes_target_owned_quarantine_rows_and_leaves_a_siblings_poison() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 6).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define the definition under test");
    trellis
        .apply("TRANSFORM order_echo FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("a sibling over the same source, which co-owns the poison band");

    let qualified_source = format!("{DEFAULT_SCHEMA}.orders");
    raw.execute(
        "insert into column_status (transform_table, column_name, last_error) \
         values ('order_rollup', 'total', 'boom'), ('order_echo', 'total', 'boom')",
        &[],
    )
    .await
    .expect("seed column_status");
    raw.execute(
        "insert into column_deaths (transform_table, column_name, deaths) \
         values ('order_rollup', 'total', 3)",
        &[],
    )
    .await
    .expect("seed column_deaths");
    raw.execute(
        "insert into column_failures (transform_table, column_name, src_table, key, error) \
         values ('order_rollup', 'total', $1, '1', 'boom')",
        &[&qualified_source],
    )
    .await
    .expect("seed column_failures");
    raw.execute(
        "insert into column_pause_cascades \
             (downstream_transform, downstream_column, upstream_transform, upstream_column) \
         values ('order_echo', 'total', 'order_rollup', 'total')",
        &[],
    )
    .await
    .expect("seed column_pause_cascades");

    // The same key held for both definitions.
    let both = "select id from transform_definitions \
                where split_part(target_table, '.', 2) in ('order_rollup', 'order_echo')";
    raw.execute(
        &format!(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, $1, '1', 'boom' from ({both}) d"
        ),
        &[&qualified_source],
    )
    .await
    .expect("seed the poison band");
    raw.execute(
        &format!(
            "insert into key_deaths (transform_id, src_table, key, deaths, last_error) \
             select id, $1, '1', 2, 'boom' from ({both}) d"
        ),
        &[&qualified_source],
    )
    .await
    .expect("seed key_deaths");
    raw.execute(
        &format!("insert into transform_fuse_gate (transform_id) select id from ({both}) d"),
        &[],
    )
    .await
    .expect("seed the fuse gates");

    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("drop");

    // Target-owned: gone, and only this target's.
    assert_eq!(
        count(
            &raw,
            "select count(*) from column_status where transform_table = 'order_rollup'"
        )
        .await,
        0,
        "the dropped target's own column_status rows go with it"
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from column_status where transform_table = 'order_echo'"
        )
        .await,
        1,
        "...and the surviving sibling's do not"
    );
    assert_eq!(
        count(&raw, "select count(*) from column_deaths").await,
        0,
        "column_deaths follows its owner"
    );
    assert_eq!(
        count(&raw, "select count(*) from column_failures").await,
        0,
        "column_failures follows its owner"
    );
    assert_eq!(
        count(&raw, "select count(*) from column_pause_cascades").await,
        0,
        "a cascade edge naming the dropped target on either end goes with it"
    );

    // Per transform (#799): the dropped definition's rows go with it, and
    // the sibling's stay.
    let echo = "(select id from transform_definitions \
                 where split_part(target_table, '.', 2) = 'order_echo')";
    for table in ["poison", "key_deaths", "transform_fuse_gate"] {
        assert_eq!(
            count(&raw, &format!("select count(*) from {table}")).await,
            1,
            "{table}: the dropped definition's row goes with it, the sibling's stays"
        );
        assert_eq!(
            count(
                &raw,
                &format!("select count(*) from {table} where transform_id = {echo}")
            )
            .await,
            1,
            "{table}: the row left is the sibling's"
        );
    }
}

/// Per-definition backfill work is the one thing keyed to the definition, and
/// it rides the drop out on `backfill_chunks`' own `on delete cascade`. A
/// paused definition also stops being handed new chunks — the pause is what
/// quiesces the durable backfill queue, exactly as it quiesces the fold.
///
/// #625 F8a: a plain 1-1 over a captured table is the Re-derive build's, so
/// the queue holds its plan job, claimed through `claim_chunks_of` with the
/// Re-derive build's kinds; the dispatch gate under test is the same one.
#[tokio::test]
async fn pausing_stops_chunk_dispatch_and_dropping_cascades_the_chunks_away() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 40).await;

    let trellis = define_only(db.dsn()).await;
    // A plain 1-1 transform's discharge starts its build with a durable
    // plan job before it is built, which is exactly the in-flight backfill
    // state under test here.
    trellis
        .apply("TRANSFORM order_doubles FROM orders SELECT a + a AS x")
        .await
        .expect("define a 1-1 transform");
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("the discharge starts the re-derive build");
    assert!(
        count(&raw, "select count(*) from backfill_chunks").await > 0,
        "precondition: the chunk queue was enumerated"
    );

    // The real dispatch path, not a hand-written mirror of its predicate: a
    // test that re-spells `claim_chunks`' own `where` clause passes no matter
    // what that clause becomes. Claim for real first, so "zero after the
    // pause" is a difference this call can actually see — then release, so the
    // pause is the only reason the next call comes back empty.
    let before = chunk_queue::claim_chunks_of(&raw, "issue-231-worker", 100, &REDERIVE_KINDS)
        .await
        .expect("claim chunks from a backfilling definition");
    assert!(
        !before.is_empty(),
        "precondition: the real dispatch path hands out chunks while the definition is not frozen"
    );
    for chunk in &before {
        chunk_queue::release_chunk(&mut raw, chunk.id, "issue-231-worker")
            .await
            .expect("release the claim taken to prove dispatch was open");
    }

    trellis
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("a backfilling definition pauses too — that is what stopping a runaway build is");
    let after = chunk_queue::claim_chunks_of(&raw, "issue-231-worker", 100, &REDERIVE_KINDS)
        .await
        .expect("claiming against a paused definition is a success that yields nothing");
    assert!(
        after.is_empty(),
        "no chunk of a paused definition is claimable: {after:?}"
    );
    assert!(
        count(
            &raw,
            "select count(*) from backfill_chunks where not done and claimed_by is null"
        )
        .await
            > 0,
        "...and the chunks are still there, unclaimed — the pause withheld them, \
         it did not consume them"
    );

    trellis
        .apply("DROP TRANSFORM order_doubles")
        .await
        .expect("drop");
    assert_eq!(
        count(&raw, "select count(*) from backfill_chunks").await,
        0,
        "per-definition chunk rows cascade off the definition"
    );
}

/// Issue #331: the pause gates *new* chunk claims, but a chunk a worker already
/// holds is left to finish (see `claim_chunks`' own comment). Finishing the last
/// of them used to run the `backfilling` -> `live` completion unconditionally,
/// so a pause landing while a worker held the final chunk was silently undone
/// the moment that worker finished: the definition went `live` and its fold
/// resumed as if nobody had paused it.
///
/// The completion must leave a definition that is no longer `backfilling`
/// exactly where it is, while still retiring the chunk — a paused definition's
/// finished chunks are done, not re-queued, and resume rebuilds it by a fresh
/// backfill of its own regardless.
///
/// `orders` is made to read as another definition's target
/// ([`markers::feed_from_a_test_definition`]; no staging worker runs here):
/// #625 F8a gives a plain 1-1 over a captured table to the Re-derive build,
/// and the chunked build whose `finish_chunk` completion this pins survives
/// for a seam-fed source.
#[tokio::test]
async fn a_chunk_finishing_after_the_pause_does_not_unpause_the_definition() {
    const WORKER: &str = "issue-331-worker";
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 40).await;
    markers::feed_from_a_test_definition(&raw, &format!("{DEFAULT_SCHEMA}.orders"))
        .await
        .expect("make orders another definition's target");

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_doubles FROM orders SELECT a + a AS x")
        .await
        .expect("define a chunked 1-1 transform");
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("backfilling"),
        "precondition: a chunked 1-1 definition is backfilling behind its queue"
    );

    // The worker takes every chunk *before* the pause, so the pause's dispatch
    // gate has nothing left to withhold: this is exactly a worker already
    // holding the definition's last chunk when the operator pauses.
    let held = chunk_queue::claim_chunks(&raw, WORKER, 100)
        .await
        .expect("claim every chunk");
    assert!(!held.is_empty(), "precondition: there were chunks to hold");
    assert_eq!(
        count(
            &raw,
            "select count(*) from backfill_chunks where not done and claimed_by is null"
        )
        .await,
        0,
        "precondition: the worker holds every chunk"
    );

    trellis
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause the backfilling definition");

    for chunk in &held {
        chunk_queue::run_claimed_chunk(
            &db.pool,
            chunk,
            WORKER,
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .await
        .expect("an in-flight chunk runs to completion");
        chunk_queue::finish_chunk(&db.pool, chunk, WORKER)
            .await
            .expect("an in-flight chunk finishes");
    }

    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("paused"),
        "finishing the last in-flight chunk must not undo the pause"
    );
    assert_eq!(
        count(&raw, "select count(*) from backfill_chunks where not done").await,
        0,
        "the finished chunks are retired all the same, not left to be re-run"
    );

    // And the pause is still an ordinary one: resume hands the definition back
    // to the backfill lifecycle rather than finding it already `live`.
    trellis
        .apply("RESUME TRANSFORM order_doubles")
        .await
        .expect("resume the still-paused definition");
    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("waiting_to_backfill"),
        "resume rebuilds by backfill"
    );
}

/// Issue #332: resume rebuilds the target with a fresh backfill, so a paused
/// definition's leftover unclaimed chunks are redundant work and must not
/// become claimable again once it is unfrozen. A chunk a worker still holds
/// is left alone: its completion is what parks the catch-up marker that
/// repairs the target should its write land after the rebuild (#331).
///
/// Both sources are made to read as another definition's target
/// ([`markers::feed_from_a_test_definition`]; no staging worker runs here):
/// #625 F8a gives a plain 1-1 over a captured table to the Re-derive build,
/// and the chunked build whose range chunks this pins survives for a
/// seam-fed source.
#[tokio::test]
async fn resuming_discards_the_paused_definitions_unclaimed_chunks() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    // One row past a single chunk's 50,000-row bound, so the queue holds one
    // chunk to keep in flight and one to leave unclaimed.
    seed_source(&raw, "orders", 50_001).await;
    // A sibling definition's queue, which resuming `order_doubles` must not
    // touch.
    seed_source(&raw, "items", 10).await;
    for source in ["orders", "items"] {
        markers::feed_from_a_test_definition(&raw, &format!("{DEFAULT_SCHEMA}.{source}"))
            .await
            .expect("make the source another definition's target");
    }

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_doubles FROM orders SELECT a + a AS x")
        .await
        .expect("define a chunked 1-1 transform");
    // Dispatched one at a time, so `order_doubles`' chunks get the lower ids.
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    trellis
        .apply("TRANSFORM item_doubles FROM items SELECT a + a AS x")
        .await
        .expect("define a sibling chunked 1-1 transform");
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        count(&raw, "select count(*) from backfill_chunks").await,
        3,
        "precondition: the queues were enumerated as two chunks and one"
    );

    // `claim_chunks` hands out the lowest id first: `order_doubles`' first chunk.
    let in_flight = chunk_queue::claim_chunks(&raw, "issue-332-worker", 1)
        .await
        .expect("claim one chunk");
    assert_eq!(in_flight.len(), 1, "precondition: one chunk is in flight");
    let in_flight = &in_flight[0];
    let paused_id = in_flight.definition_id;

    trellis
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause the backfilling definition");
    trellis
        .apply("RESUME TRANSFORM order_doubles")
        .await
        .expect("resume it");
    assert_eq!(
        persisted_status(&raw, "order_doubles").await.as_deref(),
        Some("waiting_to_backfill"),
        "precondition: resume re-parked the definition for a fresh backfill"
    );

    let reclaimed = chunk_queue::claim_chunks(&raw, "issue-332-worker", 100)
        .await
        .expect("claim after resume");
    assert!(
        reclaimed.iter().all(|c| c.definition_id != paused_id),
        "no pre-pause chunk is claimable after resume, since the fresh backfill \
         rebuilds the target anyway: {reclaimed:?}"
    );
    assert_eq!(
        reclaimed.len(),
        1,
        "the sibling definition's queue is left intact: {reclaimed:?}"
    );
    let unclaimed: i64 = raw
        .query_one(
            "select count(*) from backfill_chunks \
             where definition_id = $1 and not done and claimed_by is null",
            &[&paused_id],
        )
        .await
        .expect("count the paused definition's unclaimed chunks")
        .get(0);
    assert_eq!(
        unclaimed, 0,
        "the unclaimed chunk was discarded, not merely withheld"
    );
    let still_held: i64 = raw
        .query_one(
            "select count(*) from backfill_chunks \
             where id = $1 and claimed_by = 'issue-332-worker' and not done",
            &[&in_flight.id],
        )
        .await
        .expect("read the in-flight chunk")
        .get(0);
    assert_eq!(
        still_held, 1,
        "the chunk a worker still holds is left for that worker to finish"
    );
}

// ---------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------

/// ADR-0014's capture set shrinks by reconciliation, which ADR-0002's
/// staging worker runs from the catalog alone (issue #427) and #622 C5 restates it for trigger capture: a drop only
/// removes catalog rows and never touches the source's capture triggers, so
/// the process applying it needs no privilege on the source. The staging
/// worker's next reconcile pass uninstalls them from the catalog;
/// `client::reconcile_tests` covers that pass directly.
#[tokio::test]
async fn dropping_leaves_capture_to_the_staging_worker() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define");
    let desired = trellis::defs::tables_to_capture(&db.pool)
        .await
        .expect("the tables to capture");
    trellis::capture::reconcile::reconcile(
        &mut raw,
        DEFAULT_SCHEMA,
        &desired,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
    )
    .await
    .expect("capture the source");
    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect("drop the source's only reader");

    let installed = trellis::capture::reconcile::installed_tables(&raw, DEFAULT_SCHEMA)
        .await
        .expect("read the installed capture");
    assert!(
        installed.contains(&format!("{DEFAULT_SCHEMA}.orders")),
        "the drop leaves the capture alone; the staging worker's reconcile removes it: \
         {installed:?}"
    );
}
