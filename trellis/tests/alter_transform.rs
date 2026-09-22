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
//! - single-pass backfill of multiple added columns, and a real concurrent
//!   race the version fence must resolve correctly rather than merely not
//!   crash on (`single_pass_backfill_stays_consistent_under_concurrent_writes`)
//! - the column-granularity pause state: the new column reports paused while
//!   backfilling, and the rest of the target stays live and queryable
//!   (`the_new_column_pauses_while_backfilling_and_the_rest_of_the_target_stays_live`)
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
//! - this release's deliberate scope-downs: aggregate targets and a
//!   not-yet-live target (`altering_an_aggregate_transform_is_unsupported`,
//!   `altering_a_target_that_is_not_live_is_rejected`)

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
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

    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ALTER total AS a + b + b")
        .await
        .expect("alter a column's formula");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty());
    assert!(dropped.is_empty());
    assert_eq!(altered, vec!["total".to_string()]);

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

/// Adds *two* columns in one statement, over a source large enough
/// (150k rows, three 50k chunks) that the backfill takes real wall time,
/// while a concurrent writer continuously updates existing rows' `a` through
/// the *live, running* CDC pipeline for the whole duration.
///
/// Two things must both hold in the final, converged state:
///
/// 1. **Single pass, not one job per column.** `col_x = a` and `col_y = a +
///    1` are only ever consistent with each other (`col_y == col_x + 1`) if
///    both were computed from the exact same read of a row — a backfill that
///    ran one job per column could observe two *different* values of a
///    concurrently-updated row's `a` between the two jobs and violate this
///    invariant. This test's own `writer` task guarantees the concurrent
///    pressure needed to make that violation possible if it were happening.
/// 2. **The version fence.** `a`'s own already-live passthrough column must
///    keep reflecting the writer's latest updates throughout (the alter must
///    not block ordinary live apply), and the two new columns must end up
///    fully populated and correct for *every* row, including ones the writer
///    touched while the columns were still paused — proving the fence
///    handed any in-flight batch that raced the edit's commit back to a
///    fresh reload rather than a half-populated write.
#[tokio::test]
async fn single_pass_backfill_stays_consistent_under_concurrent_writes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed_orders(&raw, 150_000).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let dsn = db.dsn().to_string();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
                .await
                .expect("set search_path");
            let mut id = 1i64;
            while !stop.load(Ordering::Relaxed) {
                // Rewrite `a` in place (`id` stays the key) so every row is
                // still exactly reproducible from `orders` at any instant.
                client
                    .execute("update orders set a = a + 1000000 where id = $1", &[&id])
                    .await
                    .expect("concurrent update");
                id = (id % 150_000) + 1;
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        })
    };

    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ADD a AS col_x, ADD a + 1 AS col_y")
        .await
        .expect("add two columns while the pipeline is live and under write pressure");
    let (added, _, _) = into_altered(applied);
    assert_eq!(added, vec!["col_x".to_string(), "col_y".to_string()]);

    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer task");

    // Drain whatever the writer's last few updates staged before asserting a
    // final, settled state.
    let token = trellis.watermark_token().await.expect("watermark");
    trellis
        .await_converged(token, Duration::from_secs(30))
        .await
        .expect("convergence");

    let rows = raw
        .query(
            &format!(
                "select o.a::text, t.a::text, t.col_x::text, t.col_y::text \
                 from orders o join {DEFAULT_TARGET_SCHEMA}.order_calc t on o.id = t.id"
            ),
            &[],
        )
        .await
        .expect("read source+target together");
    assert_eq!(rows.len(), 150_000, "no row was lost");
    for row in rows {
        let source_a: f64 = row.get::<_, String>(0).parse().unwrap();
        let target_a: f64 = row.get::<_, String>(1).parse().unwrap();
        let col_x: f64 = row.get::<_, String>(2).parse().unwrap();
        let col_y: f64 = row.get::<_, String>(3).parse().unwrap();
        assert_eq!(
            target_a, source_a,
            "the already-live column must keep reflecting the writer's updates \
             — the edit must not have blocked ordinary live apply"
        );
        assert_eq!(
            col_x, source_a,
            "the new column must reflect the converged source, for every row"
        );
        assert_eq!(
            col_y,
            col_x + 1.0,
            "col_y must always equal col_x + 1 — this can only fail if the two \
             columns were populated from two different reads of the row, i.e. a \
             backfill-per-column implementation rather than a single pass"
        );
    }

    trellis.shutdown().await.expect("shutdown");
}

/// The column-granularity form of the pause state: while `double_a` is
/// backfilling, `quarantine_status` reports it paused, and the rest of the
/// target (its already-live `a` column) stays queryable and correct
/// throughout — the ADR's "the rest of the target stays live."
#[tokio::test]
async fn the_new_column_pauses_while_backfilling_and_the_rest_of_the_target_stays_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    // Large enough that the backfill's own wall time gives the polling loop
    // below a real window to observe the paused state in.
    seed_orders(&raw, 150_000).await;

    let definer = define_only(db.dsn()).await;
    definer
        .apply("TRANSFORM order_calc FROM orders SELECT a AS a")
        .await
        .expect("define");
    definer.shutdown().await.expect("shut the definer down");

    let trellis = running(db.dsn()).await;
    wait_for_live(&raw, "order_calc").await;

    let alter = {
        let dsn = db.dsn().to_string();
        tokio::spawn(async move {
            let trellis = Trellis::connect(
                Config::from_dsn(dsn).expect("valid dsn"),
                TrellisOptions::default(),
            )
            .await
            .expect("connect");
            trellis
                .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
                .await
                .expect("add a column")
        })
    };

    let observed_paused = Arc::new(AtomicBool::new(false));
    {
        let observed_paused = Arc::clone(&observed_paused);
        let dsn = db.dsn().to_string();
        tokio::spawn(async move {
            let trellis = Trellis::connect(
                Config::from_dsn(dsn).expect("valid dsn"),
                TrellisOptions::default(),
            )
            .await
            .expect("connect");
            for _ in 0..600 {
                if let Ok(entry) = trellis.quarantine_status("order_calc.double_a").await
                    && entry.state == trellis::QuarantineState::Paused
                {
                    observed_paused.store(true, Ordering::Relaxed);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
    }

    let applied = alter.await.expect("alter task");
    let (added, _, _) = into_altered(applied);
    assert_eq!(added, vec!["double_a".to_string()]);

    assert!(
        observed_paused.load(Ordering::Relaxed),
        "the new column must report paused while its single-pass backfill is in flight"
    );

    // Once the edit returns, the column is live again.
    let entry = trellis
        .quarantine_status("order_calc.double_a")
        .await
        .expect("status");
    assert_eq!(entry.state, trellis::QuarantineState::Live);

    trellis.shutdown().await.expect("shutdown");
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
    let applied = trellis
        .apply("ALTER TRANSFORM order_calc ADD a + a AS double_a")
        .await
        .expect("re-adding the same field with the same formula is a no-op success");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty(), "no-op: nothing was actually added again");
    assert!(dropped.is_empty());
    assert!(altered.is_empty());

    // ALTER to a new formula, then re-ALTER to the same (now-current) one.
    trellis
        .apply("ALTER TRANSFORM order_calc ALTER double_a AS a + a + a")
        .await
        .expect("alter");
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

    // DROP, then re-DROP the same, now-absent field.
    trellis
        .apply("ALTER TRANSFORM order_calc DROP double_a")
        .await
        .expect("drop");
    let applied = trellis
        .apply("ALTER TRANSFORM order_calc DROP double_a")
        .await
        .expect("dropping an already-absent field is a no-op success");
    let (added, dropped, altered) = into_altered(applied);
    assert!(added.is_empty());
    assert!(dropped.is_empty(), "no-op: it was already gone");
    assert!(altered.is_empty());

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
