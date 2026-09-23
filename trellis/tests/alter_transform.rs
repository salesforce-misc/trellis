//! Integration tests for `ALTER TRANSFORM` (ADR-0015, issues #241/#242) and
//! its retrofit of `DROP TRANSFORM`'s dependency refusal to column
//! granularity.
//!
//! Everything here drives the public [`trellis::Trellis`] facade (ADR-0012),
//! the same convention `pause_and_drop.rs` follows, and verifies against
//! Postgres directly rather than re-asking the engine what it thinks it did.
//!
//! Covered:
//! - `ADD`/`DROP`/`ALTER` individually, and combined in one statement
//!   (`add_single_column_backfills_existing_rows`,
//!   `drop_single_column_removes_the_physical_column`,
//!   `alter_single_column_recomputes_existing_rows`,
//!   `combined_add_drop_alter_in_one_statement`)
//! - single-pass backfill of multiple added columns under a concurrent
//!   writer (`single_pass_backfill_stays_consistent_under_concurrent_writes`,
//!   which runs no live pipeline and drives the initial build's chunk queue
//!   by hand instead; see its doc comment for why)
//! - the column-granularity pause state: the new column reports paused while
//!   backfilling, and the rest of the target stays live and queryable
//!   (`the_new_column_pauses_while_backfilling_and_the_rest_of_the_target_stays_live`,
//!   which holds the backfill at a fixed point mid-way and drives live apply
//!   by hand; see its doc comment)
//! - that pause is only ever undone for a field the `ALTER` itself paused,
//!   including when another pause lands mid-backfill, and `DROP <field>`
//!   clears the dropped column's quarantine bookkeeping (issue #309:
//!   `an_alter_leaves_a_pause_it_did_not_create_in_place`,
//!   `a_pause_landing_mid_alter_backfill_survives_the_alters_unpause`,
//!   `dropping_a_paused_field_clears_its_quarantine_state`)
//! - idempotency in both directions for all three edit kinds
//!   (`edits_are_idempotent_in_both_directions`), and the two genuine
//!   conflicts that are *not* idempotent no-ops
//!   (`add_an_existing_field_with_a_different_formula_is_rejected`,
//!   `altering_a_field_that_does_not_exist_is_rejected`)
//! - cycle detection reuse (`add_and_alter_that_would_cycle_is_rejected`)
//! - the column-granularity `DROP <field>` refusal, and the same
//!   infrastructure's naming precision retrofitted onto `DROP TRANSFORM`
//!   (`dropping_a_field_still_read_by_a_dependent_is_refused`,
//!   `dropping_a_transform_names_the_specific_column_a_dependent_reads`)
//! - this release's deliberate scope-downs: aggregate targets, a
//!   not-yet-live target, and a genuine column-type change
//!   (`altering_an_aggregate_transform_is_unsupported`,
//!   `altering_a_target_that_is_not_live_is_rejected`,
//!   `altering_a_field_to_a_different_result_type_is_refused`,
//!   `a_type_changing_alter_refuses_the_whole_statement`)

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::chunk_queue;
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
use trellis::{
    Applied, CatalogError, Config, TransformStatus, Trellis, TrellisError, TrellisOptions,
};

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
/// `message` — the same no-bare-sleep discipline every other integration
/// suite in this directory follows.
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
        tokio::time::sleep(Duration::from_millis(50)).await;
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

/// The live pipeline: CDC intake, ring maintenance, and drain workers — what
/// a plain (non-aggregate) 1-1 transform needs to actually reach `live`, and
/// what keeps applying change events while a test's own `ALTER TRANSFORM`
/// call runs concurrently.
async fn running(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("start the live pipeline")
}

async fn persisted_status(raw: &Client, target: &str) -> Option<String> {
    raw.query_opt(
        "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read status")
    .map(|row| row.get(0))
}

/// `transform_definitions.definition_version` for `target`, read straight
/// from Postgres — the audit-visible counter `alter_transform` bumps once
/// per *real* edit and deliberately leaves untouched for an idempotent
/// no-op clause. Used to verify a no-op claim isn't just "the reported delta
/// was empty" but "nothing was actually written."
async fn persisted_definition_version(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select definition_version from transform_definitions \
         where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read definition_version")
    .get(0)
}

async fn wait_for_live(raw: &Client, target: &str) {
    poll_until(
        Duration::from_secs(60),
        "the chunked 1-1 target must finish its initial backfill",
        async || persisted_status(raw, target).await.as_deref() == Some("live"),
    )
    .await;
}

/// `id bigint primary key, a numeric, b numeric` — `id` is `1..rows`, `a` is
/// `id` itself, `b` is `id * 2`, so every downstream assertion has a simple,
/// independently-recomputable expectation.
async fn seed_orders(raw: &Client, rows: i64) {
    raw.batch_execute(&format!(
        "create table orders (id bigint primary key, a numeric, b numeric); \
         alter table orders replica identity full; \
         insert into orders (id, a, b) \
             select s, s::numeric, (s * 2)::numeric from generate_series(1, {rows}) s;"
    ))
    .await
    .expect("seed orders");
}

async fn column_names(raw: &Client, schema: &str, table: &str) -> HashSet<String> {
    raw.query(
        "select column_name from information_schema.columns \
         where table_schema = $1 and table_name = $2",
        &[&schema, &table],
    )
    .await
    .expect("introspect information_schema")
    .into_iter()
    .map(|row| row.get(0))
    .collect()
}

/// The concrete Postgres type (`format_type`) `schema.table.column`
/// currently has — used by the type-changing-`ALTER` refusal tests to
/// confirm a refused `ALTER` never actually touches the physical column,
/// not just that the call returned an error.
async fn column_pg_type(raw: &Client, schema: &str, table: &str, column: &str) -> String {
    raw.query_one(
        "select pg_catalog.format_type(a.atttypid, a.atttypmod) \
         from pg_attribute a \
         where a.attrelid = pg_catalog.to_regclass($1) \
           and a.attname = $2 \
           and a.attnum > 0 \
           and not a.attisdropped",
        &[&format!("{schema}.{table}"), &column],
    )
    .await
    .expect("introspect the column's physical type")
    .get(0)
}

fn into_altered(applied: Applied) -> (Vec<String>, Vec<String>, Vec<String>) {
    match applied {
        Applied::Altered {
            added,
            dropped,
            altered,
            ..
        } => (added, dropped, altered),
        other => panic!("expected Applied::Altered, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// ADD / DROP / ALTER, individually and combined
// ---------------------------------------------------------------------

#[tokio::test]
async fn add_single_column_backfills_existing_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 25).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
        .await
        .expect("add a column");
    let (added, dropped, altered) = into_altered(applied);
    assert_eq!(added, vec!["double_a".to_string()]);
    assert!(dropped.is_empty());
    assert!(altered.is_empty());

    let rows = raw
        .query(
            &format!(
                "select a::text, double_a::text from {DEFAULT_TARGET_SCHEMA}.order_calc order by id"
            ),
            &[],
        )
        .await
        .expect("read target");
    assert_eq!(rows.len(), 25, "every existing row is present");
    for row in rows {
        let a: String = row.get(0);
        let double_a: String = row.get(1);
        assert_eq!(
            double_a.parse::<f64>().unwrap(),
            a.parse::<f64>().unwrap() * 2.0,
            "the single-pass backfill must have populated every existing row"
        );
    }

    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn drop_single_column_removes_the_physical_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let applied = trellis
        .apply("ALTER TRANSFORM order_calc DROP b")
        .await
        .expect("drop a column");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty());
    assert_eq!(dropped, vec!["b".to_string()]);
    assert!(altered.is_empty());

    // The data the dropped column explained is actually removed, not merely
    // hidden — the column itself is gone.
    let columns = column_names(&raw, DEFAULT_TARGET_SCHEMA, "order_calc").await;
    assert!(
        !columns.contains("b"),
        "a dropped field's physical column must be removed: {columns:?}"
    );
    assert!(columns.contains("a"), "the untouched field must survive");

    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn alter_single_column_recomputes_existing_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 25).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b, a + b AS total")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    // `total`'s new formula (`a + b + b`) infers the exact same `ValueType`
    // (numeric) its column already has — issue #241's second review round:
    // this is the case a type-changing-`ALTER` refusal must *not* catch, so
    // this test also pins the column's physical type stays `numeric`
    // throughout, not just that the call succeeds.
    assert_eq!(
        column_pg_type(&raw, DEFAULT_TARGET_SCHEMA, "order_calc", "total").await,
        "numeric"
    );

    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ALTER total AS a + b + b")
        .await
        .expect("a same-result-type ALTER must still succeed");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty());
    assert!(dropped.is_empty());
    assert_eq!(altered, vec!["total".to_string()]);

    assert_eq!(
        column_pg_type(&raw, DEFAULT_TARGET_SCHEMA, "order_calc", "total").await,
        "numeric",
        "a same-type ALTER never touches the physical column type"
    );

    let rows = raw
        .query(
            &format!(
                "select a::text, b::text, total::text from {DEFAULT_TARGET_SCHEMA}.order_calc \
                 order by id"
            ),
            &[],
        )
        .await
        .expect("read target");
    for row in rows {
        let a: f64 = row.get::<_, String>(0).parse().unwrap();
        let b: f64 = row.get::<_, String>(1).parse().unwrap();
        let total: f64 = row.get::<_, String>(2).parse().unwrap();
        assert_eq!(total, a + b + b, "every existing row must be recomputed");
    }

    trellis.shutdown().await.expect("shutdown");
}

/// Issue #241/#242's second review round: `ALTER <field> AS <expr>` refuses
/// outright when the new formula's inferred result type genuinely differs
/// from the field's current physical column type, rather than issuing the
/// `ALTER COLUMN ... TYPE ... USING NULL` that would otherwise silently
/// rewrite the *entire* target table under an `ACCESS EXCLUSIVE` lock —
/// exactly what ADR-0015 (`docs/decisions/0015-transform-redefinition.md`)
/// promises an edit never does. `total` here is declared `numeric` (`a +
/// b`); `a > b` infers `boolean`, a genuine type change.
#[tokio::test]
async fn altering_a_field_to_a_different_result_type_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b, a + b AS total")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let before_type = column_pg_type(&raw, DEFAULT_TARGET_SCHEMA, "order_calc", "total").await;
    assert_eq!(before_type, "numeric");
    let before_version = persisted_definition_version(&raw, "order_calc").await;

    let err = trellis
        .apply("ALTER TRANSFORM order_calc ALTER total AS a > b")
        .await
        .expect_err("a genuine type change (numeric -> boolean) must be refused");
    match err {
        TrellisError::Catalog(CatalogError::UnsupportedAlter(detail)) => {
            assert!(
                detail.contains("total")
                    && detail.contains("numeric")
                    && detail.contains("boolean"),
                "the refusal must name the field and both types: {detail}"
            );
            assert!(
                detail.to_uppercase().contains("DROP") && detail.to_uppercase().contains("ADD"),
                "the refusal must point at DROP+ADD as the supported path: {detail}"
            );
        }
        other => panic!("expected CatalogError::UnsupportedAlter, got {other:?}"),
    }

    // The refusal is a pre-flight validation, not a runtime failure midway
    // through work: no DDL and no backfill ever ran, so the column's
    // physical type and the definition's persisted version are exactly as
    // they were before this call.
    assert_eq!(
        column_pg_type(&raw, DEFAULT_TARGET_SCHEMA, "order_calc", "total").await,
        before_type,
        "a refused ALTER must never touch the physical column type — no table lock, \
         no rewrite, was ever attempted"
    );
    assert_eq!(
        persisted_definition_version(&raw, "order_calc").await,
        before_version,
        "a refused ALTER must not bump the definition version — nothing was written"
    );

    // The existing data is untouched too — not just the schema.
    let rows = raw
        .query(
            &format!(
                "select a::text, b::text, total::text from {DEFAULT_TARGET_SCHEMA}.order_calc \
                 order by id"
            ),
            &[],
        )
        .await
        .expect("read target");
    assert_eq!(rows.len(), 5);
    for row in rows {
        let a: f64 = row.get::<_, String>(0).parse().unwrap();
        let b: f64 = row.get::<_, String>(1).parse().unwrap();
        let total: f64 = row.get::<_, String>(2).parse().unwrap();
        assert_eq!(
            total,
            a + b,
            "the refused ALTER must not have recomputed anything"
        );
    }

    trellis.shutdown().await.expect("shutdown");
}

/// The type-change refusal blocks the *whole* statement, not just its own
/// clause: a combined `ADD ..., ALTER ...` where the `ALTER` half is a
/// genuine type change must leave the `ADD` half unapplied too — matching
/// every other all-or-nothing refusal `ALTER TRANSFORM` already has (a
/// cycle, a blocked `DROP`).
#[tokio::test]
async fn a_type_changing_alter_refuses_the_whole_statement() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b, a + b AS total")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let before_version = persisted_definition_version(&raw, "order_calc").await;

    let err = trellis
        .apply(
            "ALTER TRANSFORM order_calc \
             ADD a + a AS double_a, \
             ALTER total AS a > b",
        )
        .await
        .expect_err("the ALTER half's type change must refuse the whole statement");
    match err {
        TrellisError::Catalog(CatalogError::UnsupportedAlter(_)) => {}
        other => panic!("expected CatalogError::UnsupportedAlter, got {other:?}"),
    }

    let columns = column_names(&raw, DEFAULT_TARGET_SCHEMA, "order_calc").await;
    assert!(
        !columns.contains("double_a"),
        "the ADD half must not have been applied either: {columns:?}"
    );
    assert_eq!(
        column_pg_type(&raw, DEFAULT_TARGET_SCHEMA, "order_calc", "total").await,
        "numeric",
        "the ALTER half must never have touched the physical column type"
    );
    assert_eq!(
        persisted_definition_version(&raw, "order_calc").await,
        before_version,
        "a refused combined statement must not bump the definition version"
    );

    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn combined_add_drop_alter_in_one_statement() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 25).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b, a + b AS total")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let applied = trellis
        .apply(
            "ALTER TRANSFORM order_calc \
             ADD a + b AS sum_ab, \
             DROP b, \
             ALTER total AS a + a",
        )
        .await
        .expect("combined edit");
    let (added, dropped, altered) = into_altered(applied);
    assert_eq!(added, vec!["sum_ab".to_string()]);
    assert_eq!(dropped, vec!["b".to_string()]);
    assert_eq!(altered, vec!["total".to_string()]);

    let columns = column_names(&raw, DEFAULT_TARGET_SCHEMA, "order_calc").await;
    assert!(!columns.contains("b"));
    assert!(columns.contains("sum_ab"));
    assert!(columns.contains("total"));

    let rows = raw
        .query(
            &format!(
                "select a::text, sum_ab::text, total::text from {DEFAULT_TARGET_SCHEMA}.order_calc \
                 order by id"
            ),
            &[],
        )
        .await
        .expect("read target");
    for row in rows {
        let a: f64 = row.get::<_, String>(0).parse().unwrap();
        let sum_ab: f64 = row.get::<_, String>(1).parse().unwrap();
        let total: f64 = row.get::<_, String>(2).parse().unwrap();
        // `sum_ab = a + b` reads `b` as a *source* column (`orders.b`), which
        // dropping the target's own `b` passthrough field never touches.
        assert_eq!(sum_ab, a + (a * 2.0), "sum_ab still reads source column b");
        assert_eq!(total, a + a);
    }

    trellis.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------
// Single-pass backfill and the version fence
// ---------------------------------------------------------------------

/// Claims, runs, and finishes every pending direct-build backfill chunk
/// until none remain, flipping a freshly-defined 1-1 target to `live` with
/// no running `Client`. This is the hand-driven stand-in for a drain worker
/// that `defs_install_definition.rs` uses.
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    const CLAIMED_BY: &str = "alter_transform_test_backfill_worker";
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
            chunk_queue::run_claimed_chunk(pool, chunk, CLAIMED_BY, Duration::from_secs(5))
                .await
                .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

/// **Single pass, not one job per column.** Adds two columns in one
/// statement while a concurrent writer keeps rewriting `a`. `col_x = a` and
/// `col_y = a + 1` only agree (`col_y == col_x + 1`) if both came from the
/// same read of a row. A backfill that ran one pass per column would read
/// `a` twice, and every row the writer changed between the two passes would
/// break the invariant.
///
/// The check reads the backfill's own output the moment `ALTER TRANSFORM`
/// returns, with no live pipeline running. That's deliberate. With a
/// pipeline, the catch-up the edit parks (issue #305) re-derives every row
/// from a single source image once it drains, so a converged final state
/// satisfies the invariant whether or not the backfill was single-pass. The
/// earlier version of this test asserted on exactly that converged state
/// and passed with a per-column backfill substituted in (issue #299). It
/// also spent most of its time waiting for a live pipeline to drain several
/// hundred thousand enumerated rows under a wall-clock budget. With no
/// pipeline there is nothing to wait for.
///
/// What the old version's final state also touched is covered where it can
/// be checked directly. The version-fence bump:
/// `an_alter_racing_a_drain_for_the_version_fence_does_not_deadlock` below,
/// and `apply.rs`'s
/// `a_definition_change_on_a_touched_source_trips_the_version_fence`. Rows
/// changed while the new column was paused getting repaired:
/// `defs_backfill_chunk_queue.rs`'s
/// `alter_transform_add_parks_a_catch_up_that_repairs_a_row_changed_mid_backfill`.
/// Live apply writing every column except a paused one:
/// `column_quarantine.rs`'s
/// `paused_column_freezes_instead_of_going_null_or_being_overwritten`.
#[tokio::test]
async fn single_pass_backfill_stays_consistent_under_concurrent_writes() {
    // Large enough that each backfill pass takes long enough for the writer
    // to land many updates inside it.
    const ROWS: i64 = 20_000;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, ROWS).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    drain_backfill_chunks(&db.pool).await;
    assert_eq!(
        persisted_status(&raw, "order_calc").await.as_deref(),
        Some("live"),
        "ALTER TRANSFORM requires a live target"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let updates = Arc::new(AtomicU64::new(0));
    let writer = {
        let dsn = db.dsn().to_string();
        let stop = Arc::clone(&stop);
        let updates = Arc::clone(&updates);
        tokio::spawn(async move {
            let client = connect_raw(&dsn).await;
            let mut id = 1i64;
            while !stop.load(Ordering::Relaxed) {
                // Rewrite `a` in place (`id` stays the key), so every value
                // `a` ever holds is `id` plus a multiple of 1,000,000.
                client
                    .execute("update orders set a = a + 1000000 where id = $1", &[&id])
                    .await
                    .expect("concurrent update");
                updates.fetch_add(1, Ordering::Relaxed);
                id = (id % ROWS) + 1;
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        })
    };

    // Let the writer get going before the edit starts.
    while updates.load(Ordering::Relaxed) == 0 {
        tokio::task::yield_now().await;
    }
    let before = updates.load(Ordering::Relaxed);
    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ADD a AS col_x, ADD a + 1 AS col_y")
        .await
        .expect("add two columns under write pressure");
    let during = updates.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer task");

    let (added, _, _) = into_altered(applied);
    assert_eq!(added, vec!["col_x".to_string(), "col_y".to_string()]);
    assert!(
        during > 0,
        "the writer must have updated rows while the backfill ran, or this test proves nothing"
    );

    let rows = raw
        .query(
            &format!("select id, col_x::text, col_y::text from {DEFAULT_TARGET_SCHEMA}.order_calc"),
            &[],
        )
        .await
        .expect("read the backfilled target");
    assert_eq!(rows.len() as i64, ROWS, "the backfill must cover every row");
    for row in rows {
        let id: i64 = row.get(0);
        let col_x: i64 = row
            .get::<_, Option<String>>(1)
            .unwrap_or_else(|| panic!("row {id}: col_x must be backfilled"))
            .parse()
            .unwrap();
        let col_y: i64 = row
            .get::<_, Option<String>>(2)
            .unwrap_or_else(|| panic!("row {id}: col_y must be backfilled"))
            .parse()
            .unwrap();
        assert_eq!(
            col_x % 1_000_000,
            id,
            "row {id}: col_x must be a value `a` actually held"
        );
        assert_eq!(
            col_y,
            col_x + 1,
            "row {id}: col_y must equal col_x + 1. This only fails if the two columns were \
             populated from two different reads of the row, i.e. a backfill per column \
             rather than a single pass"
        );
    }

    trellis.shutdown().await.expect("shutdown");
}

/// Stages the CDC row intake would stage for `update orders set a = new_a
/// where id = id` (`orders` has `replica identity full`, so both images are
/// complete), into the active ring segment.
async fn stage_orders_update(raw: &Client, id: i64, old_a: i64, new_a: i64) {
    let active: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    let b = id * 2;
    let old_image = format!(r#"{{"id":"{id}","a":"{old_a}","b":"{b}"}}"#);
    let new_image = format!(r#"{{"id":"{id}","a":"{new_a}","b":"{b}"}}"#);
    raw.execute(
        &format!(
            "insert into seg_{active} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
             values ($1, $2, 'update', $3, $4::text::jsonb, $5::text::jsonb, 0)"
        ),
        &[
            &format!("{DEFAULT_SCHEMA}.orders"),
            &id.to_string(),
            &PgLsn::from(1u64),
            &old_image,
            &new_image,
        ],
    )
    .await
    .unwrap_or_else(|e| panic!("stage cdc for orders row {id}: {e}"));
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending anywhere in the ring: the hand-driven stand-in for a running
/// `Client`'s maintenance loop and drain workers (the same helper
/// `pause_and_drop.rs` uses).
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // No live `Intake` stages anything here, so there is no real staged
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
            "alter_transform_test",
            1,
            "trellis_alter_transform_test",
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

/// The column-granularity form of the pause state, checked at a fixed point
/// in the middle of the backfill rather than raced against it: while
/// `double_a` is backfilling, `quarantine_status` reports it paused, live
/// apply leaves it alone, and the rest of the target (its already-live `a`
/// column) stays readable and keeps taking live changes. That is the ADR's
/// "the rest of the target stays live."
///
/// The backfill writes one PK-range chunk per autocommit statement
/// (`BACKFILL_CHUNK_ROWS`, 50,000 rows). The test holds it inside its second
/// chunk with a row trigger on the target that waits on an advisory lock the
/// test holds, so at the checkpoint the first chunk is committed and the
/// second is not. Nothing here waits on a live pipeline: the initial build
/// runs through the chunk queue by hand, and the live change is staged and
/// drained through `apply::drain_once` by hand.
///
/// The earlier version of this test (issue #359) ran a live pipeline over
/// 150,000 rows and polled `quarantine_status` hoping to catch the paused
/// state inside the backfill's wall-clock window. It never checked the rest
/// of the target at all.
#[tokio::test]
async fn the_new_column_pauses_while_backfilling_and_the_rest_of_the_target_stays_live() {
    // One row past the first 50,000-row chunk, so the backfill has a second
    // chunk to be held inside.
    const ROWS: i64 = 50_001;
    const GATE_ID: i64 = ROWS;
    const GATE_KEY: i64 = 359;
    // Row 1 sits in the already-committed first chunk.
    const LIVE_ID: i64 = 1;
    const LIVE_NEW_A: i64 = 1000;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, ROWS).await;

    let trellis = Arc::new(define_only(db.dsn()).await);
    trellis
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    drain_backfill_chunks(&db.pool).await;
    assert_eq!(
        persisted_status(&raw, "order_calc").await.as_deref(),
        Some("live"),
        "ALTER TRANSFORM requires a live target"
    );

    // The gate: any write to the target's GATE_ID row waits for GATE_KEY,
    // which `gate` holds until the checkpoint is done.
    raw.batch_execute(&format!(
        "create function public.alter_transform_test_gate() returns trigger \
         language plpgsql as $$ begin \
             if new.id = {GATE_ID} then perform pg_advisory_xact_lock_shared({GATE_KEY}); end if; \
             return new; \
         end $$; \
         create trigger alter_transform_test_gate \
             before insert or update on {DEFAULT_TARGET_SCHEMA}.order_calc \
             for each row execute function public.alter_transform_test_gate();"
    ))
    .await
    .expect("install the backfill gate");
    let gate = connect_raw(db.dsn()).await;
    gate.execute("select pg_advisory_lock($1)", &[&GATE_KEY])
        .await
        .expect("close the gate");

    let alter = {
        let trellis = Arc::clone(&trellis);
        tokio::spawn(async move {
            trellis
                .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
                .await
        })
    };

    // Not a race: once the backfill reaches the gate it stays there until
    // the test opens it. The timeout only turns a backfill that never gets
    // there into a failure instead of a hang.
    poll_until(
        Duration::from_secs(60),
        "the backfill must reach the gated row in its second chunk",
        async || {
            raw.query_one(
                "select count(*) from pg_locks \
                 where locktype = 'advisory' and not granted and objid = $1::bigint::oid \
                   and database = (select oid from pg_database where datname = current_database())",
                &[&GATE_KEY],
            )
            .await
            .expect("read pg_locks")
            .get::<_, i64>(0)
                > 0
        },
    )
    .await;
    assert!(
        !alter.is_finished(),
        "the ALTER must still be running while its backfill is held"
    );

    // Checkpoint: the first chunk is committed, the second isn't.
    //
    // 1. The new column reports paused.
    let entry = trellis
        .quarantine_status("order_calc.double_a")
        .await
        .expect("status");
    assert_eq!(
        entry.state,
        trellis::QuarantineState::Paused,
        "the new column must report paused while its backfill is in flight"
    );

    // 2. The target stays readable. A lock timeout turns a backfill that
    //    locked readers out into a failure instead of a hang.
    raw.batch_execute("set lock_timeout = '10s'")
        .await
        .expect("set lock_timeout");
    let row = raw
        .query_one(
            &format!(
                "select count(*), \
                        count(*) filter (where double_a is not null), \
                        count(*) filter (where double_a is distinct from a + a and double_a is not null), \
                        count(*) filter (where a is distinct from id) \
                 from {DEFAULT_TARGET_SCHEMA}.order_calc"
            ),
            &[],
        )
        .await
        .expect("the target must stay readable while the new column backfills");
    let (total, populated, wrong, stale_a): (i64, i64, i64, i64) =
        (row.get(0), row.get(1), row.get(2), row.get(3));
    assert_eq!(total, ROWS, "every row stays in the target");
    assert_eq!(stale_a, 0, "the already-live column keeps its values");
    assert!(
        populated > 0 && populated < ROWS,
        "the checkpoint must fall mid-backfill, with the first chunk committed and the \
         second not (populated {populated} of {ROWS}); if BACKFILL_CHUNK_ROWS changed, \
         resize ROWS"
    );
    assert_eq!(wrong, 0, "the committed chunk wrote double_a = a + a");

    // 3. Live apply keeps writing the already-live column and leaves the
    //    paused one alone. Row LIVE_ID's double_a was backfilled from a = 1,
    //    so it must stay 2: neither recomputed to 2000 nor nulled.
    raw.execute(
        "update orders set a = $1::bigint where id = $2",
        &[&LIVE_NEW_A, &LIVE_ID],
    )
    .await
    .expect("update the source row");
    stage_orders_update(&raw, LIVE_ID, LIVE_ID, LIVE_NEW_A).await;
    let mut ring = connect_raw(db.dsn()).await;
    tokio::time::timeout(
        Duration::from_secs(60),
        drain_to_quiescence(&db.pool, &mut ring),
    )
    .await
    .expect("live apply must not wait on the in-flight backfill");
    let row = raw
        .query_one(
            &format!(
                "select a::text, double_a::text from {DEFAULT_TARGET_SCHEMA}.order_calc \
                 where id = $1"
            ),
            &[&LIVE_ID],
        )
        .await
        .expect("read the live row");
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some(LIVE_NEW_A.to_string().as_str()),
        "live apply must keep writing the already-live column while the new one backfills"
    );
    assert_eq!(
        row.get::<_, Option<String>>(1).as_deref(),
        Some("2"),
        "live apply must leave the paused column at its backfilled value"
    );

    // Open the gate. The edit finishes and the column goes live.
    gate.execute("select pg_advisory_unlock($1)", &[&GATE_KEY])
        .await
        .expect("open the gate");
    let applied = alter
        .await
        .expect("join the ALTER")
        .expect("the ALTER completes once the gate opens");
    let (added, _, _) = into_altered(applied);
    assert_eq!(added, vec!["double_a".to_string()]);

    let entry = trellis
        .quarantine_status("order_calc.double_a")
        .await
        .expect("status");
    assert_eq!(entry.state, trellis::QuarantineState::Live);

    // Every row is backfilled. Row LIVE_ID is left out of the value check:
    // its chunk read a = 1 before the live change, and repairing that is the
    // catch-up's job (`defs_backfill_chunk_queue.rs`'s
    // `alter_transform_add_parks_a_catch_up_that_repairs_a_row_changed_mid_backfill`).
    let row = raw
        .query_one(
            &format!(
                "select count(*) filter (where double_a is null), \
                        count(*) filter (where id <> $1 and double_a is distinct from a + a) \
                 from {DEFAULT_TARGET_SCHEMA}.order_calc"
            ),
            &[&LIVE_ID],
        )
        .await
        .expect("read the backfilled target");
    let (missing, wrong): (i64, i64) = (row.get(0), row.get(1));
    assert_eq!(missing, 0, "the backfill must cover every row");
    assert_eq!(wrong, 0, "every backfilled double_a must equal a + a");

    Arc::into_inner(trellis)
        .expect("the ALTER task released its handle")
        .shutdown()
        .await
        .expect("shutdown");
}

/// `(local_fuse, cascade edges into it)` for `transform.column`'s
/// `column_status` row, or `None` when the column isn't paused at all.
async fn column_pause_state(raw: &Client, transform: &str, column: &str) -> Option<(bool, i64)> {
    raw.query_opt(
        "select s.local_fuse, \
                (select count(*) from column_pause_cascades c \
                 where c.downstream_transform = s.transform_table \
                   and c.downstream_column = s.column_name) \
         from column_status s \
         where s.transform_table = $1 and s.column_name = $2",
        &[&transform, &column],
    )
    .await
    .expect("read column_status")
    .map(|row| (row.get(0), row.get(1)))
}

/// Issue #309: `ALTER TRANSFORM`'s own column pause (held while it backfills
/// a changed field) must only ever be undone for a field whose pause *it*
/// created. A field an operator paused, or one paused by cascade from an
/// upstream column, stays paused through an `ALTER` of that same field —
/// its recovery belongs to `RESUME`, not to an unrelated edit.
#[tokio::test]
async fn an_alter_leaves_a_pause_it_did_not_create_in_place() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 6).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b, a + b AS total")
        .await
        .expect("define the upstream 1-1 target");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_calc replica identity full"
    ))
    .await
    .expect("a chained target's source needs a full replica identity");
    trellis
        .apply("TRANSFORM order_next FROM order_calc SELECT total AS t2, a AS a2")
        .await
        .expect("define the chained 1-1 target");
    wait_for_live(&raw, "order_next").await;

    // An operator pause on `order_calc.total`, which cascades onto
    // `order_next.t2` (the only field reading it).
    trellis
        .apply("PAUSE TRANSFORM order_calc.total")
        .await
        .expect("pause the column");
    assert_eq!(
        column_pause_state(&raw, "order_calc", "total").await,
        Some((true, 0))
    );
    assert_eq!(
        column_pause_state(&raw, "order_next", "t2").await,
        Some((false, 1))
    );

    // ALTER the operator-paused field, together with an ADD whose pause is
    // this call's own.
    trellis
        .apply("ALTER TRANSFORM order_calc ALTER total AS a + b + b, ADD a + a AS double_a")
        .await
        .expect("alter a paused field");
    assert_eq!(
        column_pause_state(&raw, "order_calc", "total").await,
        Some((true, 0)),
        "the operator's pause must survive an ALTER of the same field"
    );
    assert_eq!(
        column_pause_state(&raw, "order_calc", "double_a").await,
        None,
        "the ALTER's own pause on the field it added must be cleared"
    );

    // ALTER the cascade-paused field downstream.
    trellis
        .apply("ALTER TRANSFORM order_next ALTER t2 AS total + 1")
        .await
        .expect("alter a cascade-paused field");
    assert_eq!(
        column_pause_state(&raw, "order_next", "t2").await,
        Some((false, 1)),
        "a pause cascaded from a still-paused upstream column must survive an ALTER"
    );

    trellis.shutdown().await.expect("shutdown");
}

/// `ALTER TRANSFORM ... DROP <field>` takes the dropped column's quarantine
/// bookkeeping with it, the same "quarantine state follows its owner" rule
/// `DROP TRANSFORM` applies to a whole target. Otherwise a stale pause would
/// outlive its column and silently land on a later `ADD` of the same name,
/// now that an `ALTER` no longer clears pauses it didn't create (#309).
#[tokio::test]
async fn dropping_a_paused_field_clears_its_quarantine_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 6).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    trellis
        .apply("PAUSE TRANSFORM order_calc.b")
        .await
        .expect("pause the column");
    raw.batch_execute(
        "insert into column_deaths (transform_table, column_name, deaths) \
             values ('order_calc', 'b', 1); \
         insert into column_failures (transform_table, column_name, src_table, key, error) \
             values ('order_calc', 'b', 'orders', '1', 'boom');",
    )
    .await
    .expect("seed column-fuse bookkeeping for b");

    trellis
        .apply("ALTER TRANSFORM order_calc DROP b")
        .await
        .expect("drop the paused field");
    for table in ["column_status", "column_deaths", "column_failures"] {
        let n: i64 = raw
            .query_one(
                &format!(
                    "select count(*) from {table} \
                     where transform_table = 'order_calc' and column_name = 'b'"
                ),
                &[],
            )
            .await
            .expect("count bookkeeping rows")
            .get(0);
        assert_eq!(n, 0, "{table} must not outlive the dropped column");
    }

    // Re-adding the same name starts clean and live.
    trellis
        .apply("ALTER TRANSFORM order_calc ADD b AS b")
        .await
        .expect("re-add the field");
    assert_eq!(column_pause_state(&raw, "order_calc", "b").await, None);

    trellis.shutdown().await.expect("shutdown");
}

/// Issue #309, the mid-backfill half: a pause that lands on a field *while*
/// `ALTER TRANSFORM`'s own backfill of it is still running takes that pause
/// over, so the `ALTER`'s closing unpause must leave it alone. An operator
/// pause upgrades the `ALTER`'s row to `local_fuse`; an upstream pause
/// cascading onto it leaves the row as it was but records an edge into it.
///
/// Deterministic rather than timing-based: the test holds an `ACCESS
/// EXCLUSIVE` lock on the edited definition's source table, which blocks the
/// backfill's first read of it (its own pause is committed before that
/// read), so the `ALTER` cannot reach its unpause until the lock is released.
#[tokio::test]
async fn a_pause_landing_mid_alter_backfill_survives_the_alters_unpause() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 6).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b, a + b AS total")
        .await
        .expect("define the upstream 1-1 target");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_calc replica identity full"
    ))
    .await
    .expect("a chained target's source needs a full replica identity");
    trellis
        .apply("TRANSFORM order_next FROM order_calc SELECT total AS t2, a AS a2")
        .await
        .expect("define the chained 1-1 target");
    wait_for_live(&raw, "order_next").await;

    // Freeze the backfill: `order_calc` is `order_next`'s source.
    let mut locker = connect_raw(db.dsn()).await;
    let lock = locker.transaction().await.expect("begin the lock holder");
    lock.batch_execute(&format!(
        "lock table {DEFAULT_TARGET_SCHEMA}.order_calc in access exclusive mode"
    ))
    .await
    .expect("lock the ALTER's source table");

    let alter = {
        let dsn = db.dsn().to_string();
        tokio::spawn(async move {
            let trellis = define_only(&dsn).await;
            let applied = trellis
                .apply(
                    "ALTER TRANSFORM order_next \
                     ADD total + total AS t3, ADD a2 + 1 AS a3, ADD a2 + 2 AS a4",
                )
                .await
                .expect("add three columns");
            trellis.shutdown().await.expect("shutdown");
            applied
        })
    };

    // The ALTER's own pauses are committed before its backfill touches the
    // (locked) source, so seeing all three means it is parked mid-backfill.
    poll_until(
        Duration::from_secs(30),
        "the ALTER must commit its own pauses before backfilling",
        async || {
            let n: i64 = raw
                .query_one(
                    "select count(*) from column_status \
                     where transform_table = 'order_next' \
                       and column_name in ('t3', 'a3', 'a4')",
                    &[],
                )
                .await
                .expect("count the ALTER's pauses")
                .get(0);
            n == 3
        },
    )
    .await;
    assert!(!alter.is_finished(), "the backfill must still be blocked");

    let operator = define_only(db.dsn()).await;
    // An operator pause on one field the ALTER is backfilling...
    operator
        .apply("PAUSE TRANSFORM order_next.a3")
        .await
        .expect("pause a3 mid-backfill");
    // ...and an upstream pause that cascades onto another (`t3` reads
    // `total`).
    operator
        .apply("PAUSE TRANSFORM order_calc.total")
        .await
        .expect("pause the upstream column mid-backfill");
    assert!(!alter.is_finished(), "the backfill must still be blocked");

    lock.rollback().await.expect("release the source lock");
    alter.await.expect("alter task");

    assert_eq!(
        column_pause_state(&raw, "order_next", "a3").await,
        Some((true, 0)),
        "an operator pause taken mid-backfill must survive the ALTER's unpause"
    );
    assert_eq!(
        column_pause_state(&raw, "order_next", "t3").await,
        Some((false, 1)),
        "a cascade onto the field mid-backfill must survive the ALTER's unpause"
    );
    assert_eq!(
        column_pause_state(&raw, "order_next", "a4").await,
        None,
        "a field nothing else paused is still released by the ALTER"
    );

    operator.shutdown().await.expect("shutdown");
    trellis.shutdown().await.expect("shutdown");
}

/// Lock order against a concurrent drain. Apply's version fence takes the
/// `source_table_versions` row `for share` and then row-locks the target;
/// `alter_transform` must take that same row *before* its DDL locks the
/// target, or the two deadlock. This test stands in for the drain with a raw
/// transaction that takes the same two locks in the same order. It holds the
/// version row until the ALTER is blocked behind it, then locks target rows.
#[tokio::test]
async fn an_alter_racing_a_drain_for_the_version_fence_does_not_deadlock() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = Arc::new(running(db.dsn()).await);
    wait_for_live(&raw, "order_calc").await;

    let mut drain = connect_raw(db.dsn()).await;
    let txn = drain.transaction().await.expect("begin");
    txn.query_one(
        "select version from source_table_versions \
         where split_part(source_table, '.', 2) = 'orders' for share",
        &[],
    )
    .await
    .expect("take the version fence row for share");

    let alter = {
        let trellis = Arc::clone(&trellis);
        tokio::spawn(async move {
            trellis
                .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
                .await
        })
    };

    poll_until(
        Duration::from_secs(30),
        "the ALTER must block on the version fence row",
        async || {
            raw.query_one(
                "select count(*) from pg_stat_activity \
                 where wait_event_type = 'Lock' \
                   and query like 'insert into source_table_versions%'",
                &[],
            )
            .await
            .expect("read pg_stat_activity")
            .get::<_, i64>(0)
                > 0
        },
    )
    .await;

    txn.query(
        &format!("select id from {DEFAULT_TARGET_SCHEMA}.order_calc order by id for update"),
        &[],
    )
    .await
    .expect("a drain holding the version fence must still be able to lock target rows");
    txn.commit().await.expect("commit the drain");

    let applied = alter
        .await
        .expect("join the ALTER")
        .expect("the ALTER completes once the drain commits");
    let (added, _, _) = into_altered(applied);
    assert_eq!(added, vec!["double_a".to_string()]);

    Arc::into_inner(trellis)
        .expect("the ALTER task released its handle")
        .shutdown()
        .await
        .expect("shutdown");
}

// ---------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------

#[tokio::test]
async fn edits_are_idempotent_in_both_directions() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    // ADD, then re-ADD the same field with the same formula.
    trellis
        .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
        .await
        .expect("add");
    let version_before = persisted_definition_version(&raw, "order_calc").await;
    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
        .await
        .expect("re-adding the same field with the same formula is a no-op success");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty(), "no-op: nothing was actually added again");
    assert!(dropped.is_empty());
    assert!(altered.is_empty());
    assert_eq!(
        persisted_definition_version(&raw, "order_calc").await,
        version_before,
        "a no-op re-ADD must not bump definition_version — nothing was actually written"
    );

    // ALTER to a new formula, then re-ALTER to the same (now-current) one.
    trellis
        .apply("ALTER TRANSFORM order_calc ALTER double_a AS a + a + a")
        .await
        .expect("alter");
    let version_before = persisted_definition_version(&raw, "order_calc").await;
    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ALTER double_a AS a + a + a")
        .await
        .expect("re-altering to the formula it already has is a no-op success");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty());
    assert!(dropped.is_empty());
    assert!(
        altered.is_empty(),
        "no-op: nothing was actually altered again"
    );
    assert_eq!(
        persisted_definition_version(&raw, "order_calc").await,
        version_before,
        "a no-op re-ALTER must not bump definition_version — nothing was actually written"
    );

    // DROP, then re-DROP the same, now-absent field.
    trellis
        .apply("ALTER TRANSFORM order_calc DROP double_a")
        .await
        .expect("drop");
    let version_before = persisted_definition_version(&raw, "order_calc").await;
    let applied = trellis
        .apply("ALTER TRANSFORM order_calc DROP double_a")
        .await
        .expect("dropping an already-absent field is a no-op success");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty());
    assert!(dropped.is_empty(), "no-op: it was already gone");
    assert!(altered.is_empty());
    assert_eq!(
        persisted_definition_version(&raw, "order_calc").await,
        version_before,
        "a no-op re-DROP must not bump definition_version — nothing was actually written"
    );

    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn add_an_existing_field_with_a_different_formula_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let err = trellis
        .apply("ALTER TRANSFORM order_calc ADD a + a AS a")
        .await
        .expect_err(
            "ADD naming an existing field with a different formula is ambiguous with ALTER",
        );
    match err {
        TrellisError::Catalog(CatalogError::AlterFieldAlreadyExists { transform, field }) => {
            assert_eq!(transform, "order_calc");
            assert_eq!(field, "a");
        }
        other => panic!("expected CatalogError::AlterFieldAlreadyExists, got {other:?}"),
    }

    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn altering_a_field_that_does_not_exist_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let err = trellis
        .apply("ALTER TRANSFORM order_calc ALTER nope AS a + a")
        .await
        .expect_err("there is nothing named 'nope' to alter");
    match err {
        TrellisError::Catalog(CatalogError::AlterFieldNotFound {
            transform, field, ..
        }) => {
            assert_eq!(transform, "order_calc");
            assert_eq!(field, "nope");
        }
        other => panic!("expected CatalogError::AlterFieldNotFound, got {other:?}"),
    }

    trellis.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------
// Cycle detection reuse
// ---------------------------------------------------------------------

/// `validate`'s column-cycle detector — the same one a first `define`
/// runs — is reused unchanged for an edit's merged field list: `b` and `c`
/// only become mutually dependent once this one `ALTER TRANSFORM` statement
/// lands, so the cycle exists nowhere before it and must be caught here.
#[tokio::test]
async fn add_and_alter_that_would_cycle_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, a AS m")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let err = trellis
        .apply("ALTER TRANSFORM order_calc ADD m + 1 AS c, ALTER m AS c + 1")
        .await
        .expect_err("m and c would depend on each other after this edit");
    match err {
        TrellisError::Catalog(CatalogError::Validate(_)) => {}
        other => panic!("expected CatalogError::Validate(.. Cycle ..), got {other:?}"),
    }

    // A rejected edit writes nothing at all.
    let columns = column_names(&raw, DEFAULT_TARGET_SCHEMA, "order_calc").await;
    assert!(!columns.contains("c"));

    trellis.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------
// Column-granularity drop refusal (issue #241), and its retrofit onto
// `DROP TRANSFORM` (issue #242)
// ---------------------------------------------------------------------

#[tokio::test]
async fn dropping_a_field_still_read_by_a_dependent_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b")
        .await
        .expect("define the upstream target");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_calc replica identity full"
    ))
    .await
    .expect("replica identity");

    trellis
        .apply("TRANSFORM order_calc_reader FROM order_calc SELECT a AS a_copy")
        .await
        .expect("a chained transform reading only order_calc.a, not order_calc.b");
    wait_for_live(&raw, "order_calc_reader").await;

    // Dropping `b` — which nothing reads — succeeds.
    trellis
        .apply("ALTER TRANSFORM order_calc DROP b")
        .await
        .expect("no dependent reads column b specifically");

    // Dropping `a` — which `order_calc_reader` reads — is refused, naming
    // the specific column-level relationship.
    let err = trellis
        .apply("ALTER TRANSFORM order_calc DROP a")
        .await
        .expect_err("order_calc_reader reads exactly this column");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
            column_detail,
        }) => {
            assert_eq!(subject, "order_calc.a");
            assert_eq!(dependents, vec!["order_calc_reader".to_string()]);
            assert_eq!(column_detail.len(), 1);
            assert!(column_detail[0].contains("order_calc_reader"));
            assert!(column_detail[0].contains("order_calc.a"));
        }
        other => panic!("expected CatalogError::DependentsBlockDrop, got {other:?}"),
    }

    // A refused drop writes nothing: the column is still there.
    let columns = column_names(&raw, DEFAULT_TARGET_SCHEMA, "order_calc").await;
    assert!(columns.contains("a"));

    trellis.shutdown().await.expect("shutdown");
}

/// Issue #242's retrofit: `drop_transform`'s existing table-level refusal
/// still refuses on *any* reader (unchanged — a reader of any column still
/// blocks a whole-table drop), but now names the *specific* column that
/// reader depends on, using the same column-granularity infrastructure
/// `ALTER TRANSFORM ... DROP <field>` needs for its own, load-bearing check.
#[tokio::test]
async fn dropping_a_transform_names_the_specific_column_a_dependent_reads() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b")
        .await
        .expect("define the upstream target");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_calc replica identity full"
    ))
    .await
    .expect("replica identity");

    trellis
        .apply("TRANSFORM order_calc_reader FROM order_calc SELECT a AS a_copy")
        .await
        .expect("a chained transform reading only order_calc.a");
    wait_for_live(&raw, "order_calc_reader").await;

    trellis
        .apply("PAUSE TRANSFORM order_calc")
        .await
        .expect("pause before a drop attempt");
    let err = trellis
        .apply("DROP TRANSFORM order_calc")
        .await
        .expect_err("order_calc_reader still derives from this target");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
            column_detail,
        }) => {
            assert_eq!(subject, "order_calc");
            assert_eq!(dependents, vec!["order_calc_reader".to_string()]);
            assert_eq!(
                column_detail,
                vec!["order_calc_reader.a_copy reads order_calc.a".to_string()],
                "the refusal names the exact column a_copy reads, not just \
                 \"something reads this table\""
            );
        }
        other => panic!("expected CatalogError::DependentsBlockDrop, got {other:?}"),
    }

    trellis.shutdown().await.expect("shutdown");
}

/// Issue #242's whole point: `column_dependents_any_keyspace` must see a
/// dependency that only exists through a downstream **aggregate's own
/// `GROUP BY` key** — not a `SELECT` field — since a `GROUP BY` key is not
/// itself a [`trellis`]-internal `FieldDef`
/// (`ast::KeySpace::Aggregate`'s own doc comment) and so is invisible to the
/// plain field-list loop `column_dependents_via` also runs. Every other
/// drop-refusal test in this file (and in `pause_and_drop.rs`) exercises a
/// dependent that reads the column through an ordinary `SELECT` field, which
/// would still pass even if the `KeySpace::Aggregate { group_by }` branch in
/// `column_dependents_via` (`catalog.rs`) were deleted outright — this test
/// exists specifically to fail if that branch regresses.
#[tokio::test]
async fn dropping_a_field_read_by_a_downstream_aggregates_group_by_key_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 6).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a, b AS b")
        .await
        .expect("define the upstream 1-1 target");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;
    raw.batch_execute(&format!(
        "alter table {DEFAULT_TARGET_SCHEMA}.order_calc replica identity full"
    ))
    .await
    .expect("a chained aggregate's source needs a full replica identity");

    // `order_group` groups directly on `order_calc.a` — not a passthrough
    // `SELECT` field of its own — so the only edge from `order_group` back
    // to `order_calc.a` is its `GROUP BY` key.
    trellis
        .apply("TRANSFORM order_group FROM order_calc GROUP BY a SELECT count(*) AS n")
        .await
        .expect("an aggregate grouping directly on the upstream's column");

    // Dropping `b` — which nothing reads, as a field or a GROUP BY key —
    // still succeeds.
    trellis
        .apply("ALTER TRANSFORM order_calc DROP b")
        .await
        .expect("no dependent reads column b, as a field or a GROUP BY key");

    // Dropping `a` — which `order_group` GROUPs BY — is refused, naming
    // `order_group` and the exact column its own `GROUP BY` key reads.
    let err = trellis
        .apply("ALTER TRANSFORM order_calc DROP a")
        .await
        .expect_err("order_group's GROUP BY key reads exactly this column");
    match err {
        TrellisError::Catalog(CatalogError::DependentsBlockDrop {
            subject,
            dependents,
            column_detail,
        }) => {
            assert_eq!(subject, "order_calc.a");
            assert_eq!(dependents, vec!["order_group".to_string()]);
            assert_eq!(
                column_detail,
                vec!["order_group.a reads order_calc.a".to_string()],
                "the refusal must be traced to order_group's own GROUP BY key, \
                 not just \"something reads this table\""
            );
        }
        other => panic!("expected CatalogError::DependentsBlockDrop, got {other:?}"),
    }

    // A refused drop writes nothing: the column is still there.
    let columns = column_names(&raw, DEFAULT_TARGET_SCHEMA, "order_calc").await;
    assert!(columns.contains("a"));

    trellis.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------
// Scope-downs, named
// ---------------------------------------------------------------------

#[tokio::test]
async fn altering_an_aggregate_transform_is_unsupported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table orders (id bigint primary key, g bigint, a numeric); \
         alter table orders replica identity full; \
         insert into orders (id, g, a) select s, s % 2, s from generate_series(1, 6) s;",
    )
    .await
    .expect("seed");

    // Aggregates build synchronously, so no running pipeline is needed here.
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("an aggregate definition builds synchronously");

    let err = trellis
        .apply("ALTER TRANSFORM order_rollup ADD g AS g2")
        .await
        .expect_err("ALTER TRANSFORM only supports 1-1 transforms in this release");
    match err {
        TrellisError::Catalog(CatalogError::UnsupportedAlter(_)) => {}
        other => panic!("expected CatalogError::UnsupportedAlter, got {other:?}"),
    }
}

#[tokio::test]
async fn altering_a_target_that_is_not_live_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 5).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    trellis
        .apply("PAUSE TRANSFORM order_calc")
        .await
        .expect("pause");

    let err = trellis
        .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
        .await
        .expect_err("a paused definition is not live");
    match err {
        TrellisError::Catalog(CatalogError::TransformNotLive { transform, status }) => {
            assert_eq!(transform, "order_calc");
            assert_eq!(status, TransformStatus::Paused);
        }
        other => panic!("expected CatalogError::TransformNotLive, got {other:?}"),
    }

    trellis.shutdown().await.expect("shutdown");
}
