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
            1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 43, 44, 45, 46, 47, 48, 49,
            50, 51, 53
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
        // undercounted by both of them). Issue #144 added V31
        // (`worker_registry`, one row per live drain worker — the read
        // behind `Trellis::has_live_drain_workers`). Issue #191 removed V8
        // (`pause_leases`): the fleet-wide claiming-pause lease had no
        // consumer — `Trellis::self_check` (ADR-0013) gets its quiescent
        // read from a watermark-await + snapshot + re-check instead, never
        // pausing claiming — so V8 is not reissued to anything else either.
        // Issue #142 (ADR-0014) added V32 (`paused` joins
        // `transform_definitions.status`'s check constraint — the
        // operator-driven half of the pause state whose other half is the
        // poison fuse's `quarantined`; the same status column, and so the
        // same `status = 'live'` gate the claim-time fold already honors,
        // rather than a second freezing mechanism). Issue #283 added V33
        // (folds pre-existing bare-spelled `poison`/`poison_held`/
        // `key_deaths`/`column_failures`/`transform_fuse_gate` rows into their
        // qualified counterpart, now that all five key on one canonical
        // identity per logical source table instead of on whatever spelling
        // the ring row being diagnosed happened to carry — summing
        // `key_deaths.deaths`, deduplicating the markers). Issue #285 added
        // V34 (`relationship_definitions.from_schema` — the schema a
        // relationship's from-table actually resolved to when it was declared,
        // so a scoped `DROP RELATIONSHIP <schema>.<from_table>.<name>` checks
        // its qualifier against the relationship's own from-table rather than
        // against mere `schema_nodes` existence, which any registered
        // same-named table in any schema satisfied; clears pre-existing rows
        // rather than guessing their schema, per V24's own precedent). Issues
        // #241/#242 (ADR-0015) added V35 (`transform_definitions
        // .definition_version` — the monotonic per-definition edit counter
        // `ALTER TRANSFORM` bumps; `source_table_versions.version` remains
        // the value the version fence itself reads). Issue #310 added V36
        // (`slot_loss_pauses`, which transforms a lost replication slot paused).
        // Issue #315 added V37 (a `recompute` ring row may carry a prior-image
        // hint in `old_image`). Issue #288 added V38 (`relationship_definitions`'
        // uniqueness key widened to the schema-qualified `(from_schema,
        // from_table, name)`). Issues #311/#367 added V39
        // (`pending_backfill.generation`, so a discharge deletes only the
        // marker generation it read).
        // Issue #321 added V40 (`aggregate_extinct_horizon`, the per-target
        // extinct horizon for aggregate recompute basis checks). Issue #379
        // added V41 (`relationship_projections.projection_schema`); later
        // removed when issue #435 moved every projection into the catalog
        // schema, so V41 is not reissued to anything else. Issue
        // #418 added V42 (`backfill_chunks.fuse_rearmed_at`). Issue #407
        // added V43 (`pending_backfill`'s retry state). Issue #419 added V44
        // (`backfill_chunks`' unbounded direct-build job rows). Issue #431
        // added V45 (`pending_backfill.fence_xid`, null until the discharge
        // fences the marker). Issue #372 added V46 (`relationship_definitions
        // .to_schema`, the to-side's resolved schema). Issue #476 added V47
        // (the `catching_up` transform status). Issues #468/#485 added V48
        // (drops V18's `backfill_coverage`). Issue #507 added V49
        // (`pending_backfill.refresh_projections`). Issue #420 added V50
        // (drops `pending_backfill.added_at`, which nothing read). Issue #531
        // added V51 (`relationship_projections.refreshed_lsn`). Issue #595
        // added V53 (`ring_slot_mirror`, the snapshot-independent copy of
        // `segment_pointer.ring_slot` ring writers read); V52 was left free
        // because an experiment branch already uses it.
        "expected exactly V1 through V7, V9 through V24, V26 through V40, V42 through V51, and V53 to be applied"
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

/// Issue #340: a connection killed server-side (`pg_terminate_backend`, the
/// same fault a chaos test injects into intake's producer connection) must
/// classify as `Connectivity`, the same as a transport-level drop. The
/// server reports it as a real `DbError` with SQLSTATE `57P01`
/// (admin_shutdown), which `classify_pg_error` used to fold into `Internal`.
/// The connection future is what deterministically receives that `FATAL`
/// (a query issued afterward sees only "connection closed"), so that's the
/// error this test classifies.
#[tokio::test]
async fn terminated_backend_classifies_as_connectivity() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let (victim, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect victim");
    let connection = tokio::spawn(connection);
    let pid: i32 = victim
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("victim pid")
        .get(0);

    let killer = db.pool.get().await.expect("acquire connection");
    let terminated: bool = killer
        .query_one("select pg_terminate_backend($1)", &[&pid])
        .await
        .expect("terminate victim")
        .get(0);
    assert!(terminated, "pg_terminate_backend must find the victim");

    let err = connection
        .await
        .expect("connection task must not panic")
        .expect_err("a terminated backend must end its connection with an error");
    let db_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected the server's FATAL as a DbError, got {err:?}"));
    assert_eq!(
        *db_err.code(),
        tokio_postgres::error::SqlState::ADMIN_SHUTDOWN,
        "{db_err:?}"
    );
    assert_eq!(
        trellis::error_code::classify_pg_error(&err),
        trellis::ErrorCode::Connectivity
    );
}

/// Issue #182: a pool with no `max_size`/`wait_timeout` configured lets a
/// caller that shows up once every slot is already checked out wait
/// forever — silently, with no error and no log line, ever. Configures a
/// deliberately tiny `max_size` (2, via [`Config::with_pool_max_size`]) and
/// a short `wait_timeout` (via [`Config::with_pool_wait_timeout`]) so
/// exhaustion is trivial to trigger without spinning up dozens of real
/// connections, then proves a third caller gets back a typed, bounded
/// failure — never a hang — once every slot is held.
#[tokio::test]
async fn pool_exhaustion_times_out_with_a_typed_error_instead_of_hanging() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let config = Config::from_dsn(db.dsn().to_string())
        .expect("valid schema")
        .with_pool_max_size(2)
        .expect("2 is a valid pool size")
        .with_pool_wait_timeout(std::time::Duration::from_millis(300));
    let pool = trellis::Pool::new(&config).expect("pool builds lazily; DSN is well-formed");

    // Check out every slot the pool has and hold onto the guards — dropping
    // either would return its connection to the pool and defeat the point
    // of this test.
    let first = pool.get().await.expect("acquire slot 1 of 2");
    let second = pool.get().await.expect("acquire slot 2 of 2");

    // A third caller now has nowhere to go. Wrap the call in a generous
    // outer `tokio::time::timeout` (well beyond the pool's own 300ms
    // `wait_timeout`) so a regression that reintroduces an unbounded wait
    // fails this test loudly and quickly instead of hanging the whole test
    // binary.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), pool.get())
        .await
        .expect("pool.get() must return well within 5s once wait_timeout elapses, not hang");

    match outcome {
        Err(Error::Pool(err)) => {
            let message = err.to_string();
            assert!(
                message.to_lowercase().contains("timeout"),
                "expected a wait-timeout pool error, got: {message}"
            );
        }
        other => panic!(
            "expected a typed, bounded pool-timeout error once the pool was exhausted, got \
             {other:?}"
        ),
    }

    drop(first);
    drop(second);
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
