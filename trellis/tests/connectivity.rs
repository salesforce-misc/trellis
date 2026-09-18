//! Integration tests for connectivity + migrations (issue #20), run against
//! a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`, issue #21).

use testkit::TestCluster;
use trellis::{Config, Error, migrate};

#[tokio::test]
async fn migrate_up_is_idempotent() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid schema");

    migrate(&db.pool, &config).await.expect("first migrate run");

    let client = db.pool.get().await.expect("acquire connection");
    let first_run: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();
    drop(client);

    assert_eq!(
        first_run,
        vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            26, 27, 28, 29, 30
        ],
        // Issue #73 added V22 (drops `column_status`'s now-unenforceable
        // `target_table` foreign key). Its reviewer follow-up added V23
        // (the `transform_definitions_target_suffix_idx` expression unique
        // index — the DB-level backstop for `create_definition_inner`'s
        // bare-target-suffix pre-check). Issue #74 added V24 (clears any
        // bare-keyed `schema_nodes`/`schema_edges` rows now stale under the
        // qualified-identity keying — see that migration's own doc comment
        // for why this is a destructive, assume-empty migration rather than
        // ADR-0007's literal canonicalize-in-place proposal). Issue #54
        // (epic #49) added V25 (`metric_rollup`); later removed (metric
        // history/aggregation is left to an operator's own Prometheus
        // stack — see docs/observability.md's "Retention" section), so V25
        // is not reissued to anything else. Issue #129 (epic #127) added
        // V26 (`relationship_projections`). Issue #133 (epic #127) added
        // V27 (`group_key_array`, widening the ring's `group_key` column
        // from `text` to `text[]`). Issue #134 (epic #127) added V28
        // (`relationship_reverse_deferred`, the new `rel_reverse_deferred`
        // ring op plus `retry_count`/`relationship_id` columns). Issue #160
        // added V29 (`transform_definitions.fuse_rearmed_at`, the point a
        // resume re-arms the whole-transform fuse from, so a resumed
        // transform gets a fresh eviction budget instead of re-tripping on
        // the very next eviction). Issue #159 added V30
        // (`transform_fuse_gate`, the per-source-table row lock that
        // serializes concurrent evictions' whole-transform fuse checks so a
        // threshold crossing reached by two workers at once cannot be
        // undercounted by both of them).
        "expected exactly V1 through V24 and V26 through V30 to be applied"
    );

    // Running again should be a no-op: same ledger, no error.
    migrate(&db.pool, &config)
        .await
        .expect("second migrate run");

    let client = db.pool.get().await.expect("acquire connection");
    let second_run: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(
        first_run, second_run,
        "re-running migrate changed the ledger"
    );
}

#[tokio::test]
async fn pool_acquires_and_releases_a_working_connection() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let client = db.pool.get().await.expect("acquire connection");
    let row = client.query_one("select 1", &[]).await.expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
    drop(client);

    // The connection should be back in the pool, ready to be reused.
    let client = db.pool.get().await.expect("acquire connection again");
    let row = client
        .query_one("select 1", &[])
        .await
        .expect("select 1 again");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

#[tokio::test]
async fn unreachable_dsn_surfaces_a_typed_error_without_panicking() {
    // Syntactically valid, but nothing is listening: a unix socket
    // directory that was never created.
    let config = Config::from_dsn(
        "host=/tmp/trellis-nonexistent-socket-dir port=5432 user=postgres dbname=postgres",
    )
    .expect("valid schema");
    let pool = trellis::Pool::new(&config).expect("pool builds lazily; DSN is well-formed");

    match pool.get().await {
        Err(Error::Pool(_)) => {}
        other => panic!("expected a typed pool error for an unreachable DSN, got {other:?}"),
    }
}

#[tokio::test]
async fn failing_migration_rolls_back_without_partial_ledger_entry() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid schema");

    migrate(&db.pool, &config)
        .await
        .expect("apply real migrations first");

    let mut client = db.pool.get().await.expect("acquire connection");
    let before: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();

    // Inject a deliberately broken migration directly, bypassing the
    // embedded set, so this test doesn't require a permanently-broken file
    // under trellis/migrations/.
    let broken = refinery::Migration::unapplied("V999__broken", "this is not valid sql;")
        .expect("construct ad-hoc migration");
    let runner = refinery::Runner::new(&[broken]);
    let pg_client: &mut tokio_postgres::Client = &mut client;
    let result = runner.run_async(pg_client).await;
    assert!(result.is_err(), "expected the broken migration to fail");

    let after: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("query ledger after failed migration")
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(
        before, after,
        "a failed migration must not leave a partial ledger entry"
    );
}
