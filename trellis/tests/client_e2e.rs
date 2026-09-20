//! End-to-end tests for the [`trellis::Client`] runtime (issue #11's runtime
//! increment): drive the *whole* pipeline — publication/slot setup, CDC
//! intake, ring maintenance, and application workers — through the public
//! `Client` API against a real, ephemeral Postgres instance
//! (`testkit::TestCluster`), rather than staging changes into the ring by
//! hand the way `apply.rs`/`intake_core.rs` do.
//!
//! Two things this file exists to prove:
//!
//! - The full pipeline, started with one `Client::start` call, converges a
//!   real source table's inserts/updates/deletes into its target table, and
//!   that target is byte-exact against an independent oracle
//!   (`defs::oracle::recompute`) at every step.
//! - `staging_worker` and `application_threads` are genuinely independent
//!   knobs: a staging-only client (no app workers) stages and seals but
//!   drains nothing, leaving the target empty.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::Pool;
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    TransformStatus, chunk_queue, create_definition, create_target_table, install_definition,
    recompute, source_primary_key,
};
use trellis::{Client as TrellisClient, ClientOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`) and pins
/// `search_path`, matching `apply.rs`/`intake_core.rs`'s helper of the same
/// name.
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

/// Polls `predicate` on `interval` until it returns `true`, or panics with
/// `message` once `timeout` elapses. Every wait in this file goes through
/// here rather than a bare `sleep` — real logical-replication intake can
/// take a few seconds to first-stage a change, but no wait here is
/// unbounded.
async fn poll_until<F>(timeout: Duration, interval: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("poll_until timed out after {timeout:?}: {message}");
        }
        tokio::time::sleep(interval).await;
    }
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// The transform this file exercises throughout: a 1-1 sum of two source
/// columns, mirroring `apply.rs`'s `order_totals_def` convention.
fn totals_def() -> TransformDef {
    TransformDef {
        target: "totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

/// Creates `orders` plus `totals`'s definition and target table. `orders`
/// keeps Postgres's default replica identity (its primary key) rather than
/// `REPLICA IDENTITY FULL`: this scalar sum never needs an old image (a
/// delete only needs the key, which the primary-key-only default identity
/// already sends) — setting `FULL` would mark *every* column as a key
/// column in pgoutput's `Relation` message, and `intake::extract_key`
/// joins every `is_key` column, so the row's "key" would become every
/// column's value concatenated rather than just its primary key. Returns
/// the primary key column intake's caller needs for both the target DDL
/// and the oracle recompute.
async fn setup_source_and_target(pool: &Pool, raw: &Client) -> trellis::defs::PrimaryKeyColumn {
    raw.batch_execute("create table orders (id integer primary key, a numeric, b numeric)")
        .await
        .expect("create source table");

    let source_columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("a".to_string(), ValueType::Numeric),
        ("b".to_string(), ValueType::Numeric),
    ]);
    create_definition(
        pool,
        "TRANSFORM totals FROM orders SELECT a + b AS total",
        &source_columns,
    )
    .await
    .expect("create definition");

    let pk = trellis::defs::require_single_column_pk(
        source_primary_key(pool, "orders")
            .await
            .expect("introspect source primary key"),
        "orders",
    )
    .expect("single-column pk");
    create_target_table(
        pool,
        &totals_def(),
        "public",
        &pk,
        &source_columns,
        &totals_def().source,
    )
    .await
    .expect("create target table");
    pk
}

/// The target table's current contents, keyed by id (as text) to its
/// `total` (as text) — order-independent so it can be compared directly
/// against [`oracle_snapshot`].
async fn target_snapshot(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select id::text, total::text from totals", &[])
        .await
        .expect("read target table")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// An independent, from-scratch recompute of `totals` from `orders`'s
/// current contents (see `defs::oracle::recompute`'s own doc comment) — the
/// authority every convergence check in this file compares the live target
/// against.
async fn oracle_snapshot(
    pool: &Pool,
    def: &TransformDef,
    pk_name: &str,
    source_columns: &HashMap<String, ValueType>,
) -> HashMap<String, Option<String>> {
    recompute(pool, def, pk_name, source_columns)
        .await
        .expect("oracle recompute")
        .into_iter()
        .map(|(id, fields)| {
            let total = fields
                .get("total")
                .cloned()
                .flatten()
                .map(|n| n.to_string());
            (id, total)
        })
        .collect()
}

#[tokio::test]
async fn the_full_pipeline_converges_inserts_updates_and_deletes_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    setup_source_and_target(&db.pool, &raw).await;
    let def = totals_def();

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 2,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    raw.batch_execute(
        "insert into orders (id, a, b) values \
         (1, 10.00, 1.50), (2, 20.00, 2.00), (3, 5.00, 0.50)",
    )
    .await
    .expect("insert source rows");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged with the oracle after the inserts",
        async || {
            target_snapshot(&raw).await
                == oracle_snapshot(&db.pool, &def, "id", &numeric_columns(&["a", "b"])).await
        },
    )
    .await;
    assert_eq!(
        target_snapshot(&raw).await.len(),
        3,
        "all three inserted rows must have drained"
    );

    raw.execute("update orders set a = 15.00, b = 1.00 where id = 2", &[])
        .await
        .expect("update source row");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged with the oracle after the update",
        async || {
            target_snapshot(&raw).await
                == oracle_snapshot(&db.pool, &def, "id", &numeric_columns(&["a", "b"])).await
        },
    )
    .await;
    let total_after_update: Option<String> = raw
        .query_one("select total::text from totals where id = 2", &[])
        .await
        .expect("read updated row")
        .get(0);
    assert_eq!(total_after_update, Some("16.00".to_string()));

    raw.execute("delete from orders where id = 3", &[])
        .await
        .expect("delete source row");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged with the oracle after the delete",
        async || {
            target_snapshot(&raw).await
                == oracle_snapshot(&db.pool, &def, "id", &numeric_columns(&["a", "b"])).await
        },
    )
    .await;
    let remaining = target_snapshot(&raw).await;
    assert_eq!(remaining.len(), 2, "the deleted row must be gone");
    assert!(
        !remaining.contains_key("3"),
        "id 3's target row must have been deleted"
    );

    client.shutdown().await.expect("clean shutdown");
}

/// Issue #60's end-to-end contract: load rows, derive them, `TRUNCATE` the
/// source, insert new rows, and let the pipeline converge — the target must
/// end up matching a from-scratch oracle computed against `orders`'s
/// current (post-truncate) contents, with no trace of the truncated rows.
/// Also covers the ordering case directly: a row inserted in the very same
/// transaction as the `TRUNCATE` (so it shares one commit `lsn` with it,
/// distinguishable only by intake's append order) must survive, proving the
/// fold's `change_id` tie-break (not just `lsn`) governs the void filter
/// through the real intake path, not just the hand-staged fold tests.
#[tokio::test]
async fn a_truncate_clears_the_target_then_post_truncate_inserts_converge_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    setup_source_and_target(&db.pool, &raw).await;
    let def = totals_def();

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 2,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    raw.batch_execute(
        "insert into orders (id, a, b) values \
         (1, 10.00, 1.50), (2, 20.00, 2.00), (3, 5.00, 0.50)",
    )
    .await
    .expect("insert source rows");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged with the oracle after the inserts",
        async || {
            target_snapshot(&raw).await
                == oracle_snapshot(&db.pool, &def, "id", &numeric_columns(&["a", "b"])).await
        },
    )
    .await;
    assert_eq!(target_snapshot(&raw).await.len(), 3);

    // The truncate itself, plus a same-transaction post-truncate insert
    // (id 4) — both commit under one `lsn`, so surviving this correctly
    // depends on intake's append order (`change_id`), not `lsn` alone. A
    // separate, later transaction adds id 5, exercising the ordinary
    // cross-transaction case too.
    let mut txn_conn = connect_raw(db.dsn()).await;
    let txn = txn_conn.transaction().await.expect("begin truncate txn");
    txn.execute("truncate orders", &[])
        .await
        .expect("truncate source table");
    txn.execute("insert into orders (id, a, b) values (4, 40.00, 4.00)", &[])
        .await
        .expect("insert a same-transaction post-truncate row");
    txn.commit().await.expect("commit truncate txn");

    raw.execute("insert into orders (id, a, b) values (5, 50.00, 5.00)", &[])
        .await
        .expect("insert a later, separate-transaction post-truncate row");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "target never converged with the oracle after the truncate",
        async || {
            target_snapshot(&raw).await
                == oracle_snapshot(&db.pool, &def, "id", &numeric_columns(&["a", "b"])).await
        },
    )
    .await;

    let remaining = target_snapshot(&raw).await;
    assert_eq!(
        remaining.len(),
        2,
        "only the two post-truncate rows must remain: {remaining:?}"
    );
    assert!(
        !remaining.contains_key("1")
            && !remaining.contains_key("2")
            && !remaining.contains_key("3"),
        "every pre-truncate row must be gone: {remaining:?}"
    );
    assert_eq!(remaining.get("4"), Some(&Some("44.00".to_string())));
    assert_eq!(remaining.get("5"), Some(&Some("55.00".to_string())));

    client.shutdown().await.expect("clean shutdown");
}

/// The transform a new-source-table registration exercises: independent of
/// `totals_def`/`orders`, so this file's other tests (which never touch
/// `comments`) can't accidentally satisfy it.
fn comments_calc_def() -> TransformDef {
    TransformDef {
        target: "comments_calc".to_string(),
        source: "comments".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("x".to_string())),
                rhs: Box::new(Expr::Column("y".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

/// Issue #14's regression case: a transform registered — via
/// `defs::create_definition`, mirroring what `defctl add` does under the
/// hood — against a source table that was **not** in
/// `ClientOptions::source_tables` at `Client::start` time must still get
/// published and backfilled while that same client keeps running, with no
/// restart. Before the fix, `reconcile_publication`/`run_pending_backfills`
/// only ever ran once, from `setup_staging`, so `comments` would never join
/// the publication and `comments_calc` would stay empty for as long as this
/// client kept running.
#[tokio::test]
async fn a_transform_registered_against_a_new_source_table_backfills_without_a_client_restart() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    // Only `orders` is known at start time — `comments` doesn't exist yet,
    // let alone have a registered transform.
    setup_source_and_target(&db.pool, &raw).await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 2,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        // A short reconcile cadence so the test doesn't have to wait out a
        // production-sized interval to observe the periodic re-reconcile.
        reconcile_interval: Duration::from_millis(200),
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    // Register the new table and its transform *after* the client is
    // already running — the exact sequence the issue describes (`defctl
    // run` already up, then `defctl add` against a previously-unwatched
    // table) — including pre-existing rows, so this also proves the
    // backfill (not just the go-forward stream) reaches a table added this
    // way.
    raw.batch_execute(
        "create table comments (id integer primary key, x numeric, y numeric); \
         insert into comments (id, x, y) values (1, 3.00, 4.00), (2, 1.00, 1.00)",
    )
    .await
    .expect("create comments and seed pre-existing rows");

    let comments_columns = numeric_columns(&["id", "x", "y"]);
    create_definition(
        &db.pool,
        "TRANSFORM comments_calc FROM comments SELECT x + y AS total",
        &comments_columns,
    )
    .await
    .expect("register comments_calc against the new source table");
    let comments_pk = trellis::defs::require_single_column_pk(
        trellis::defs::source_primary_key(&db.pool, "comments")
            .await
            .expect("introspect comments primary key"),
        "comments",
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &comments_calc_def(),
        "public",
        &comments_pk,
        &comments_columns,
        &comments_calc_def().source,
    )
    .await
    .expect("create comments_calc target table");

    let def = comments_calc_def();
    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "comments_calc never converged with the oracle after registering a transform \
         against a new source table on an already-running client",
        async || {
            let target: HashMap<String, Option<String>> = raw
                .query("select id::text, total::text from comments_calc", &[])
                .await
                .expect("read comments_calc")
                .into_iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect();
            target == oracle_snapshot(&db.pool, &def, "id", &comments_columns).await
        },
    )
    .await;

    let target_count: i64 = raw
        .query_one("select count(*) from comments_calc", &[])
        .await
        .expect("count comments_calc rows")
        .get(0);
    assert_eq!(
        target_count, 2,
        "both pre-existing comments rows must have backfilled"
    );

    client.shutdown().await.expect("clean shutdown");
}

/// The transform issue #75's regression test below exercises: identical to
/// [`comments_calc_def`] except `explicit_source_schema` names `custom`
/// (issue #76's grammar) — needed so [`oracle_snapshot`]'s own
/// `quoted_source_from` reads `custom.comments`, not a bare `comments` that
/// would resolve (wrongly, for this test) via this file's pinned
/// `search_path`.
fn comments_calc_custom_schema_def() -> TransformDef {
    TransformDef {
        explicit_source_schema: Some("custom".to_string()),
        ..comments_calc_def()
    }
}

/// Issue #75, ADR-0007's regression case: `client::reconcile_source_tables`
/// (the same issue #14 periodic re-derivation the test above exercises) must
/// reconcile the publication against each newly-registered source's own
/// *actual* persisted qualified name — not a bare suffix re-guessed against
/// `Config::target_schema` (`"public"` by default). Before the fix,
/// `defs::all_source_tables` returned only `comments`'s bare suffix, and
/// `reconcile_source_tables` re-qualified it as `public.comments` regardless
/// of where the real table lived — silently publishing/backfilling a
/// same-named decoy in `public` instead (or failing loudly if none existed).
///
/// Proven with exactly that shape: a same-named, same-shaped decoy sits in
/// `public` — the schema the old bug guessed — while the real, registered
/// source lives in `custom`, named explicitly via issue #76's `FROM
/// custom.comments` grammar, registered *after* the client is already
/// running (so its own initial `ClientOptions::source_tables` never named
/// it — discovering it is exactly the periodic-reconcile job under test). A
/// row inserted into `custom.comments` after registration must still reach
/// `comments_calc`: that requires the periodic reconcile to have added
/// `custom.comments` (not `public.comments`) to the publication, so CDC
/// actually streams it. If the old bug were still present, `custom.comments`
/// would never join the publication and this insert would simply never
/// propagate, timing the `poll_until` below out.
#[tokio::test]
async fn a_transform_registered_against_an_explicitly_qualified_non_default_schema_source_reconciles_correctly()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    setup_source_and_target(&db.pool, &raw).await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 2,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        reconcile_interval: Duration::from_millis(200),
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    // The decoy: same bare name and shape, sitting in `public` — the schema
    // the old bug's `reconcile_source_tables` would have (wrongly) assumed
    // every newly-discovered source table lived under. Left empty: if the
    // bug is present, this is the table that (wrongly) joins the publication
    // instead of `custom.comments`, so `custom.comments`'s own writes simply
    // never propagate — no row here should ever need to be read.
    raw.batch_execute(
        "create table public.comments (id integer primary key, x numeric, y numeric)",
    )
    .await
    .expect("create decoy public.comments");

    // The real source: explicitly qualified into a schema that's neither
    // `public` nor the Trellis-pinned schema bare resolution would pick.
    raw.batch_execute(
        "create schema custom; \
         create table custom.comments (id integer primary key, x numeric, y numeric); \
         insert into custom.comments (id, x, y) values (1, 3.00, 4.00), (2, 1.00, 1.00)",
    )
    .await
    .expect("create custom.comments and seed pre-existing rows");

    let comments_columns = numeric_columns(&["id", "x", "y"]);
    create_definition(
        &db.pool,
        "TRANSFORM comments_calc FROM custom.comments SELECT x + y AS total",
        &comments_columns,
    )
    .await
    .expect("register comments_calc against the explicitly-qualified source");
    let comments_pk = trellis::defs::require_single_column_pk(
        trellis::defs::source_primary_key(&db.pool, "custom.comments")
            .await
            .expect("introspect custom.comments primary key"),
        "custom.comments",
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &comments_calc_custom_schema_def(),
        "public",
        &comments_pk,
        &comments_columns,
        "custom.comments",
    )
    .await
    .expect("create comments_calc target table");

    let def = comments_calc_custom_schema_def();

    // Proves the direct-enumeration initial backfill (unaffected by this
    // bug, since it enumerates synchronously at registration time rather
    // than through the publication).
    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "comments_calc never converged with the oracle after registering a transform \
         against an explicitly-qualified, non-default-schema source table",
        async || {
            let target: HashMap<String, Option<String>> = raw
                .query("select id::text, total::text from comments_calc", &[])
                .await
                .expect("read comments_calc")
                .into_iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect();
            target == oracle_snapshot(&db.pool, &def, "id", &comments_columns).await
        },
    )
    .await;

    // The part that actually depends on the fix: a write to `custom.comments`
    // *after* registration only reaches `comments_calc` if the periodic
    // reconcile added the correct (`custom.comments`, not `public.comments`)
    // table to the publication.
    raw.batch_execute("insert into custom.comments (id, x, y) values (3, 10.00, 5.00)")
        .await
        .expect("insert a post-registration row into custom.comments");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "a post-registration write to custom.comments never reached comments_calc — \
         the periodic reconcile must have published the wrong table",
        async || {
            let target: HashMap<String, Option<String>> = raw
                .query("select id::text, total::text from comments_calc", &[])
                .await
                .expect("read comments_calc")
                .into_iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect();
            target == oracle_snapshot(&db.pool, &def, "id", &comments_columns).await
        },
    )
    .await;

    let target_count: i64 = raw
        .query_one("select count(*) from comments_calc", &[])
        .await
        .expect("count comments_calc rows")
        .get(0);
    assert_eq!(
        target_count, 3,
        "both pre-existing custom.comments rows plus the post-registration insert must \
         have converged, and nothing from the public.comments decoy"
    );

    let decoy_count: i64 = raw
        .query_one("select count(*) from public.comments", &[])
        .await
        .expect("count public.comments decoy rows")
        .get(0);
    assert_eq!(
        decoy_count, 0,
        "the decoy must never receive any of this test's writes"
    );

    client.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn staging_and_application_threads_are_independent_knobs() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    setup_source_and_target(&db.pool, &raw).await;

    // No app workers: staging (intake + sealing) must still run, but nothing
    // drains the sealed batch into the target.
    let options = ClientOptions {
        staging_worker: true,
        application_threads: 0,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    raw.batch_execute("insert into orders (id, a, b) values (1, 10.00, 1.50), (2, 20.00, 2.00)")
        .await
        .expect("insert source rows");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "the staged insert never sealed into a batch",
        async || {
            let sealed: i64 = raw
                .query_one("select count(*) from segments where state = 'sealed'", &[])
                .await
                .expect("count sealed segments")
                .get(0);
            sealed >= 1
        },
    )
    .await;

    let target_count: i64 = raw
        .query_one("select count(*) from totals", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(
        target_count, 0,
        "with zero application threads nothing should have drained into the target"
    );

    client.shutdown().await.expect("clean shutdown");
}

/// docs/decisions/0007's "Backgrounding and resumability" amendment, proven
/// end-to-end through the real `Client` runtime rather than by manually
/// calling `defs::chunk_queue`'s primitives (see `defs_backfill_chunk_queue.rs`
/// for that lower-level coverage): `install_definition` on a plain
/// (non-relationship) 1-1 transform returns immediately with the definition
/// still `Backfilling`, and it's the running client's own `application_threads`
/// drain workers — with no ring/CDC involved at all here (`staging_worker:
/// false`) — that claim and finish its backfill chunk, flipping it to `Live`
/// and building the target correctly.
#[tokio::test]
async fn a_plain_one_to_one_definition_backfills_via_running_drain_workers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table widgets (id bigint primary key, price numeric); \
         insert into widgets (id, price) select g, g from generate_series(1, 200) g",
    )
    .await
    .expect("seed widgets");

    let options = ClientOptions {
        staging_worker: false,
        application_threads: 2,
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    let widgets_columns = numeric_columns(&["id", "price"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM widgets_calc FROM widgets SELECT price + price AS double_price",
        &widgets_columns,
        "public",
    )
    .await
    .expect("install_definition enumerates chunk work and returns");
    assert_eq!(
        def.status,
        TransformStatus::Backfilling,
        "install_definition must return before any running drain worker finishes the chunk queue"
    );

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        "widgets_calc's backfill chunk was never claimed and finished by a running drain worker",
        async || {
            let status: Option<String> = raw
                .query_opt(
                    // Issue #73: `target_table` is persisted fully-qualified now.
                    &format!(
                        "select status from transform_definitions \
                         where target_table = '{DEFAULT_TARGET_SCHEMA}.widgets_calc'"
                    ),
                    &[],
                )
                .await
                .expect("read status")
                .map(|row| row.get(0));
            status.as_deref() == Some("live")
        },
    )
    .await;

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from widgets \
             left join widgets_calc on widgets_calc.id = widgets.id \
             where widgets_calc.id is null \
                or widgets_calc.double_price is distinct from widgets.price + widgets.price",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "a running drain worker must have built the target correctly"
    );

    client.shutdown().await.expect("clean shutdown");
}

/// public-api-design review gap #2: `chunk_queue::reclaim_stale_chunks` used
/// to run only from `maintenance_loop`, which only spawns `if
/// options.staging_worker`. A drain-only client (`staging_worker: false`,
/// `application_threads > 0` — a normal, documented fleet topology) had no
/// self-healing for a crashed drain worker's stale chunk claim, since nothing
/// else in a fleet running *no* `staging_worker: true` client anywhere would
/// ever sweep it.
///
/// This simulates exactly that crash: a chunk is claimed by a `"dead-worker"`
/// that never executes or finishes it — *before* any `Client` exists at
/// all — then a single `staging_worker: false` client is started (with a
/// short `reclaim_ttl`/`maintenance_interval` so the test doesn't wait out
/// production-sized defaults) and must, entirely on its own, reclaim that
/// stale claim, execute it, and flip the definition to `Live`.
#[tokio::test]
async fn a_drain_only_client_reclaims_a_stale_chunk_claim_with_no_staging_worker_anywhere() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table gadgets (id bigint primary key, price numeric); \
         insert into gadgets (id, price) select g, g from generate_series(1, 50) g",
    )
    .await
    .expect("seed gadgets");

    let gadgets_columns = numeric_columns(&["id", "price"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM gadgets_calc FROM gadgets SELECT price + price AS double_price",
        &gadgets_columns,
        "public",
    )
    .await
    .expect("install_definition enumerates chunk work and returns");
    assert_eq!(def.status, TransformStatus::Backfilling);

    // Simulate a drain worker that claimed this definition's one chunk and
    // then crashed before ever executing or finishing it. No `Client` is
    // running yet at all, so this really is durable state a crashed process
    // left behind, not an artifact of racing a live worker.
    let claimed = chunk_queue::claim_chunks(&raw, "dead-worker", 10)
        .await
        .expect("claim_chunks (simulating a crashed worker)");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");

    // A drain-only client — no staging worker anywhere in this test, and
    // this is the *only* client instance running. Without the app-worker
    // loop's own reclaim sweep, nothing would ever free the stale claim
    // above.
    let options = ClientOptions {
        staging_worker: false,
        application_threads: 2,
        reclaim_ttl: Duration::from_millis(200),
        maintenance_interval: Duration::from_millis(50),
        poll_interval: Duration::from_millis(50),
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        "a drain-only client (staging_worker: false) never reclaimed and finished the stale \
         chunk claim left behind by a simulated crashed worker",
        async || {
            let status: Option<String> = raw
                .query_opt(
                    // Issue #73: `target_table` is persisted fully-qualified now.
                    &format!(
                        "select status from transform_definitions \
                         where target_table = '{DEFAULT_TARGET_SCHEMA}.gadgets_calc'"
                    ),
                    &[],
                )
                .await
                .expect("read status")
                .map(|row| row.get(0));
            status.as_deref() == Some("live")
        },
    )
    .await;

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from gadgets \
             left join gadgets_calc on gadgets_calc.id = gadgets.id \
             where gadgets_calc.id is null \
                or gadgets_calc.double_price is distinct from gadgets.price + gadgets.price",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the reclaiming drain-only client must have built the target correctly"
    );

    client.shutdown().await.expect("clean shutdown");
}
