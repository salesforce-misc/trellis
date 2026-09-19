//! Integration tests for issue #142 / ADR-0014 — the other half of a
//! definition's lifecycle: **pause**, **resume**, and **drop**.
//!
//! Everything here drives the public [`trellis::Trellis`] facade (ADR-0012:
//! the sibling crates and these suites reach the engine the way a user does,
//! not through internals), and verifies against Postgres directly — reading
//! `transform_definitions`, `information_schema`, the quarantine tables and
//! `pg_publication_tables` with a raw connection rather than re-asking the
//! engine what it thinks it did.
//!
//! The scenarios, one per ADR-0014 decision:
//!
//! - pause is idempotent, and reaching a paused definition it didn't create
//!   is a success (`pausing_twice_is_a_no_op_success`)
//! - resume rebuilds by a fresh backfill rather than catching up over
//!   buffered changes, and a paused definition never pins the ring for its
//!   siblings (`resume_rebuilds_by_backfill_and_a_paused_definition_never_pins_the_ring`)
//! - a drop always takes the target table and its data with it
//!   (`dropping_takes_the_target_table_and_its_data_with_it`)
//! - a live dependent refuses the drop and is named
//!   (`dropping_is_refused_and_names_the_live_dependents`)
//! - dropping an absent definition is a success
//!   (`dropping_an_unregistered_definition_is_a_no_op_success`)
//! - the target's own `column_*` quarantine bookkeeping goes with it; the
//!   shared, source-keyed poison band does not
//!   (`dropping_takes_target_owned_quarantine_rows_and_leaves_the_poison_band`)
//! - the publication shrinks by reconciliation, and only once nothing
//!   derives from a source any longer
//!   (`dropping_shrinks_the_publication_to_what_still_derives`), and that
//!   same reconcile still *grows* it correctly
//!   (`dropping_reconciles_a_publication_that_still_has_to_grow`)
//! - "chains off the target" includes reading it through a relationship, not
//!   only `FROM <target>`
//!   (`dropping_is_refused_by_a_live_dependent_that_reads_through_a_relationship`)

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
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
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// Polls `predicate` until it holds or `timeout` elapses, then panics with
/// `message`. Mirrors `client_e2e.rs`'s helper of the same name: no wait in
/// this file is a bare `sleep`, and none is unbounded.
async fn poll_until<F>(timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {timeout:?}: {message}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

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

/// Seeds a source table carrying `REPLICA IDENTITY FULL`, which both the
/// aggregate shape (`assert_replica_identity_supports_aggregate`) and the
/// reverse-recompute machinery require. Aggregates are this file's default
/// transform shape for a reason: they build synchronously, so a test that
/// needs a definition to actually reach `live` doesn't have to stand up drain
/// workers just to get there.
async fn seed_source(raw: &Client, table: &str, rows: i64) {
    raw.batch_execute(&format!(
        "create table {table} (id bigint primary key, g bigint, a numeric); \
         alter table {table} replica identity full; \
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
    let def = trellis
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("an aggregate definition builds synchronously");
    assert_eq!(
        def.status,
        TransformStatus::Live,
        "an aggregate transform is live the moment it is defined"
    );

    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pause a live transform");
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("paused"),
        "a pause writes the same status column the fold's `status = 'live'` gate reads"
    );

    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pausing an already-paused transform is a no-op success, not a conflict");
    assert_eq!(
        persisted_status(&raw, "order_rollup").await.as_deref(),
        Some("paused"),
        "the second pause left the row exactly as the first one did"
    );
    assert_eq!(
        trellis.status("order_rollup").await.expect("status"),
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
        .pause_transform("no_such_transform")
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
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;

    let definer = define_only(db.dsn()).await;
    definer
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define the transform under test");
    definer
        .define("TRANSFORM order_echo FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define a sibling over the same source");
    definer.shutdown().await.expect("shut the definer down");

    // The live pipeline: CDC intake, ring maintenance, and drain workers.
    let running = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("start the live pipeline");

    running
        .pause_transform("order_rollup")
        .await
        .expect("pause one of the two siblings");

    // Changes that arrive while `order_rollup` is paused. Its share of these
    // is drained for `order_echo` and is not recoverable by replay.
    raw.batch_execute(
        "insert into orders (id, g, a) select s, s % 2, s from generate_series(5, 12) s",
    )
    .await
    .expect("write to the source while one reader is paused");

    // Claim 1: the sibling converges anyway. If the paused definition pinned
    // the ring, these changes would never drain and this would time out.
    let expected_total: i64 = (1..=12).sum();
    poll_until(
        Duration::from_secs(60),
        "a paused definition must not wedge the ring for its siblings",
        async || {
            count(
                &raw,
                &format!(
                    "select coalesce(sum(total), 0)::bigint from {DEFAULT_TARGET_SCHEMA}.order_echo"
                ),
            )
            .await
                == expected_total
        },
    )
    .await;

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

    running
        .resume_transform("order_rollup")
        .await
        .expect("resume the paused definition");

    // Claim 2, mechanism: resume hands the target to the ordinary
    // fresh-backfill path rather than replaying anything.
    assert!(
        matches!(
            persisted_status(&raw, "order_rollup").await.as_deref(),
            Some("waiting_to_backfill") | Some("backfilling") | Some("live")
        ),
        "resume drops the definition back into the backfill lifecycle it was defined through"
    );

    // Claim 2, outcome: the target comes back reconciled against the *current*
    // source — including the eight rows written while it was paused, whose
    // change records were drained for the sibling and never held for it.
    poll_until(
        Duration::from_secs(60),
        "a resumed target must be rebuilt from current source data, not from buffered changes",
        async || {
            count(
                &raw,
                &format!(
                    "select coalesce(sum(total), 0)::bigint from \
                     {DEFAULT_TARGET_SCHEMA}.order_rollup"
                ),
            )
            .await
                == expected_total
        },
    )
    .await;

    running.shutdown().await.expect("shut the pipeline down");
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
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define");
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
        .pause_transform("order_rollup")
        .await
        .expect("pause");
    trellis
        .drop_transform("order_rollup")
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
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define the upstream");
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_rollup replica identity full"
    ))
    .await
    .expect("a chained aggregate's source needs a full replica identity");
    trellis
        .define("TRANSFORM grand_total FROM order_rollup GROUP BY g SELECT sum(total) AS t")
        .await
        .expect("define a transform chained off the first one's target");

    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pause");

    let err = trellis
        .drop_transform("order_rollup")
        .await
        .expect_err("a live definition still derives from this target");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
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
    trellis.pause_transform("grand_total").await.expect("pause");
    trellis
        .drop_transform("grand_total")
        .await
        .expect("drop the dependent first");
    trellis
        .drop_transform("order_rollup")
        .await
        .expect("with nothing left deriving from it, the upstream drops cleanly");

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
        .drop_transform("never_defined")
        .await
        .expect("dropping something that was never defined is a success");

    trellis
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define");
    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pause");
    trellis
        .drop_transform("order_rollup")
        .await
        .expect("first drop");
    trellis
        .drop_transform("order_rollup")
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
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define");

    let err = trellis
        .drop_transform("order_rollup")
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
///   fuse gate are keyed to the **source** table and co-owned by every
///   definition reading it, so a drop leaves them exactly as it found them. A
///   drain worker may be mid-batch over that source right now, for a sibling
///   that is still live.
///
/// The rows are seeded directly rather than provoked through a real poisoning,
/// because what's under test is which rows a drop removes, not how they came
/// to exist.
#[tokio::test]
async fn dropping_takes_target_owned_quarantine_rows_and_leaves_the_poison_band() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 6).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define the definition under test");
    trellis
        .define("TRANSFORM order_echo FROM orders GROUP BY g SELECT sum(a) AS total")
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

    raw.execute(
        "insert into poison (src_table, key, last_error) values ($1, '1', 'boom')",
        &[&qualified_source],
    )
    .await
    .expect("seed the source-keyed poison band");
    raw.execute(
        "insert into key_deaths (src_table, key, deaths, last_error) values ($1, '1', 2, 'boom')",
        &[&qualified_source],
    )
    .await
    .expect("seed key_deaths");
    raw.execute(
        "insert into transform_fuse_gate (src_table) values ($1)",
        &[&qualified_source],
    )
    .await
    .expect("seed the per-source fuse gate");

    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pause");
    trellis.drop_transform("order_rollup").await.expect("drop");

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

    // Source-keyed and shared: untouched.
    assert_eq!(
        count(&raw, "select count(*) from poison").await,
        1,
        "the whole-key poison band is keyed to the source and shared with siblings"
    );
    assert_eq!(
        count(&raw, "select count(*) from key_deaths").await,
        1,
        "key_deaths is the independent per-key fuse tier, also source-keyed"
    );
    assert_eq!(
        count(&raw, "select count(*) from transform_fuse_gate").await,
        1,
        "the per-source fuse serialization row survives its reader"
    );
}

/// Per-definition backfill work is the one thing keyed to the definition, and
/// it rides the drop out on `backfill_chunks`' own `on delete cascade`. A
/// paused definition also stops being handed new chunks — the pause is what
/// quiesces the durable backfill queue, exactly as it quiesces the fold.
#[tokio::test]
async fn pausing_stops_chunk_dispatch_and_dropping_cascades_the_chunks_away() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 40).await;
    raw.batch_execute("create publication trellis_pub")
        .await
        .expect("create publication");

    let trellis = define_only(db.dsn()).await;
    // A plain 1-1 transform enumerates a durable chunk queue and returns
    // before it is built, which is exactly the in-flight backfill state under
    // test here.
    trellis
        .define("TRANSFORM order_doubles FROM orders SELECT a + a AS x")
        .await
        .expect("define a chunked 1-1 transform");
    assert!(
        count(&raw, "select count(*) from backfill_chunks").await > 0,
        "precondition: the chunk queue was enumerated"
    );

    trellis
        .pause_transform("order_doubles")
        .await
        .expect("a backfilling definition pauses too — that is what stopping a runaway build is");
    assert_eq!(
        count(
            &raw,
            "select count(*) from backfill_chunks c join transform_definitions d \
             on d.id = c.definition_id \
             where d.status <> 'paused' and not c.done and c.claimed_by is null"
        )
        .await,
        0,
        "no chunk of a paused definition is claimable"
    );

    trellis.drop_transform("order_doubles").await.expect("drop");
    assert_eq!(
        count(&raw, "select count(*) from backfill_chunks").await,
        0,
        "per-definition chunk rows cascade off the definition"
    );
}

// ---------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------

/// ADR-0014, "The publication shrinks by reconciliation": a drop does not
/// hand-edit the publication. It reconciles against the definitions that
/// remain — which removes a source table from replication only once nothing
/// derives from it any longer, and leaves it in place while a sibling still
/// does.
#[tokio::test]
async fn dropping_shrinks_the_publication_to_what_still_derives() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;
    seed_source(&raw, "shipments", 4).await;
    raw.batch_execute("create publication trellis_pub for table trellis.orders, trellis.shipments")
        .await
        .expect("create a publication already covering both sources");

    let trellis = define_only(db.dsn()).await;
    trellis
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define");
    trellis
        .define("TRANSFORM order_echo FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define a sibling over the same source");
    trellis
        .define("TRANSFORM shipment_rollup FROM shipments GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define over the other source");

    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pause");
    trellis
        .drop_transform("order_rollup")
        .await
        .expect("drop one of two readers of `orders`");

    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_publication_tables \
             where pubname = 'trellis_pub' and tablename = 'orders'"
        )
        .await,
        1,
        "`orders` stays published: a sibling definition still derives from it"
    );

    trellis.pause_transform("order_echo").await.expect("pause");
    trellis
        .drop_transform("order_echo")
        .await
        .expect("drop its last reader");

    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_publication_tables \
             where pubname = 'trellis_pub' and tablename = 'orders'"
        )
        .await,
        0,
        "`orders` leaves replication once nothing derives from it any longer"
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_publication_tables \
             where pubname = 'trellis_pub' and tablename = 'shipments'"
        )
        .await,
        1,
        "...and the untouched source stays exactly where it was"
    );
}

/// Reviewer regression (ADR-0014, "The publication shrinks by
/// reconciliation"): the inline reconcile runs on a standalone
/// `tokio_postgres` connection, which — unlike every pooled one — does not
/// get `pool::session_bootstrap`'s `search_path`. A reconcile that has to
/// *add* a table parks a `pending_backfill` marker, and against a non-`public`
/// Trellis schema that failed `42P01` with the definition already gone.
///
/// A drop whose desired set is *larger* than the published one is ordinary:
/// a define-only connection registered a second source after the pipeline
/// last reconciled.
#[tokio::test]
async fn dropping_reconciles_a_publication_that_still_has_to_grow() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 4).await;
    seed_source(&raw, "shipments", 4).await;
    // Deliberately published for `orders` only: `shipments` joins on the
    // next reconcile, which is the drop's.
    raw.batch_execute("create publication trellis_pub for table trellis.orders")
        .await
        .expect("create a publication covering only one of the two sources");

    let trellis = define_only(db.dsn()).await;
    trellis
        .define("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define");
    trellis
        .define("TRANSFORM shipment_rollup FROM shipments GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define over the as-yet-unpublished source");

    trellis
        .pause_transform("order_rollup")
        .await
        .expect("pause");
    trellis
        .drop_transform("order_rollup")
        .await
        .expect("the drop's own reconcile must not fail over an unpinned search_path");

    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_publication_tables \
             where pubname = 'trellis_pub' and tablename = 'shipments'"
        )
        .await,
        1,
        "reconciliation grows the publication as well as shrinking it"
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_publication_tables \
             where pubname = 'trellis_pub' and tablename = 'orders'"
        )
        .await,
        0,
        "...and `orders` left it with its last reader"
    );
}

// ---------------------------------------------------------------------
// Relationships
// ---------------------------------------------------------------------

/// Reviewer regression (ADR-0014, "Drops go in reverse dependency order — no
/// cascade"): "a live definition chains off the target" is not only the
/// `FROM <target>` spelling. A relationship whose **to**-side is the target
/// makes every live transform reading `<rel>.<column>` a reader of its rows,
/// and that path persists no `source` edge for `dependents_of` to find.
///
/// Dropping anyway used to succeed, leaving the reader deriving from a
/// vanished table — and because the surviving `relationship` edge keeps the
/// target's `schema_nodes` row alive, `all_source_tables` kept naming the
/// dropped table, so every later `reconcile_publication` (including the
/// running client's own periodic one) failed `42P01`.
#[tokio::test]
async fn dropping_is_refused_by_a_live_dependent_that_reads_through_a_relationship() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_source(&raw, "orders", 9).await;
    raw.batch_execute(
        "create table reports (id bigint primary key, oid bigint); \
         alter table reports replica identity full; \
         insert into reports (id, oid) values (1, 1), (2, 2), (3, 3);",
    )
    .await
    .expect("seed the relationship's from-side");

    let definer = define_only(db.dsn()).await;
    definer
        .define("TRANSFORM order_doubles FROM orders SELECT a + a AS total")
        .await
        .expect("define the upstream target");
    definer.shutdown().await.expect("shut the definer down");

    // A 1-1 target builds through the durable chunk queue, so it needs drain
    // workers to reach `live` — and it has to be live for the relationship's
    // to-side type check to see its columns.
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("start the live pipeline");
    poll_until(
        Duration::from_secs(60),
        "the chunked 1-1 target must finish its backfill",
        async || persisted_status(&raw, "order_doubles").await.as_deref() == Some("live"),
    )
    .await;
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_doubles replica identity full"
    ))
    .await
    .expect("replica identity");

    trellis
        .define_relationship("RELATIONSHIP rollup FROM reports.oid TO order_doubles.id")
        .await
        .expect("a relationship whose to-side is a Trellis-owned target table");
    trellis
        .define("TRANSFORM report_view FROM reports SELECT rollup.total AS t")
        .await
        .expect("a live transform that reads the target through it");

    trellis
        .pause_transform("order_doubles")
        .await
        .expect("pause");
    let err = trellis
        .drop_transform("order_doubles")
        .await
        .expect_err("a live transform still reads this target through a relationship");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
        }) => {
            assert_eq!(subject, "order_doubles");
            assert_eq!(dependents, vec!["report_view".to_string()]);
        }
        other => panic!("expected CatalogError::DependentsBlockDrop, got {other:?}"),
    }
    assert!(
        table_exists(&raw, DEFAULT_TARGET_SCHEMA, "order_doubles").await,
        "the refused drop wrote nothing"
    );

    trellis.shutdown().await.expect("shut the pipeline down");
}

/// A relationship is a definition too, and the same refuse-don't-cascade rule
/// applies: a live transform whose text still reads it blocks the drop and is
/// named. Once nothing reads it, it drops — along with the Trellis-owned
/// parent projection table its declaration created.
#[tokio::test]
async fn dropping_a_relationship_is_refused_while_a_live_transform_reads_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table authors (id bigint primary key, name text); \
         create table posts (id bigint primary key, author bigint); \
         alter table authors replica identity full; \
         alter table posts replica identity full; \
         insert into authors (id, name) values (1, 'a'), (2, 'b'); \
         insert into posts (id, author) values (1, 1), (2, 1), (3, 2);",
    )
    .await
    .expect("seed a from/to pair");

    let trellis = define_only(db.dsn()).await;
    trellis
        .define_relationship("RELATIONSHIP posts FROM authors.id TO posts.author")
        .await
        .expect("declare the relationship");
    trellis
        .define("TRANSFORM author_stats FROM authors SELECT count(posts.id) AS post_count")
        .await
        .expect("define a transform that reads it");

    let err = trellis
        .drop_relationship("authors", "posts")
        .await
        .expect_err("a live transform still reads this relationship");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
        }) => {
            assert_eq!(subject, "authors.posts");
            assert_eq!(dependents, vec!["author_stats".to_string()]);
        }
        other => panic!("expected CatalogError::DependentsBlockDrop, got {other:?}"),
    }

    trellis
        .pause_transform("author_stats")
        .await
        .expect("pause");
    trellis
        .drop_transform("author_stats")
        .await
        .expect("retire the reader first");
    trellis
        .drop_relationship("authors", "posts")
        .await
        .expect("with nothing live reading it, the relationship drops");

    assert_eq!(
        count(&raw, "select count(*) from relationship_definitions").await,
        0,
        "the relationship row is gone"
    );
    assert_eq!(
        count(&raw, "select count(*) from relationship_projections").await,
        0,
        "and its projection bookkeeping cascaded with it"
    );
    assert!(
        table_exists(&raw, DEFAULT_SCHEMA, "authors").await
            && table_exists(&raw, DEFAULT_SCHEMA, "posts").await,
        "both endpoint tables are the user's and are untouched"
    );

    trellis
        .drop_relationship("authors", "posts")
        .await
        .expect("a replayed relationship drop is a no-op success");
}
