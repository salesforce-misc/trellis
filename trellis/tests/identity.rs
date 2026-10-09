//! Integration tests for instance identity (`docs/instance-identity.md`):
//! multiple instances sharing a database without interfering, clean
//! re-attach, and refusal of incompatible/foreign schemas. Run against a
//! real, ephemeral Postgres instance via `testkit::TestCluster`.

use testkit::TestCluster;
use trellis::{Config, Error, Pool, migrate};

/// Builds a [`Config`] for `schema` against `dsn` directly, bypassing
/// `TRELLIS_SCHEMA`/env resolution entirely — these tests need several
/// distinct schemas against the very same database in one process, which
/// isn't expressible through the env-var seam without racing other tests.
fn config_for_schema(dsn: &str, schema: &str) -> Config {
    Config::with_schema(dsn, schema).expect("test schema names are valid")
}

/// A [`Config`] for catalog schema `schema` and target schema `target`.
fn config_with_target(dsn: &str, schema: &str, target: &str) -> Config {
    config_for_schema(dsn, schema)
        .with_target_schema(target)
        .expect("test schema names are valid")
}

/// Whether `schema` exists in the database `db` names.
async fn schema_exists(db: &testkit::TestDatabase, schema: &str) -> bool {
    let client = db.pool.get().await.expect("connect");
    client
        .query_one(
            "select exists(select 1 from pg_namespace where nspname = $1)",
            &[&schema],
        )
        .await
        .expect("look up the schema")
        .get(0)
}

/// Attaches `config` through its own pool.
async fn attach(config: &Config) -> Result<(), Error> {
    let pool = Pool::new(config).expect("build pool");
    migrate(&pool, config).await
}

/// The `IncompatibleInstance` message of a refused attach.
fn refusal(result: Result<(), Error>) -> String {
    match result {
        Err(Error::IncompatibleInstance(message)) => message,
        other => panic!("expected an IncompatibleInstance refusal, got {other:?}"),
    }
}

/// The target schema `schema`'s marker records.
async fn recorded_target_schema(db: &testkit::TestDatabase, schema: &str) -> Option<String> {
    let client = db.pool.get().await.expect("connect");
    client
        .query_one(
            &format!("select target_schema from {schema}.trellis_instance"),
            &[],
        )
        .await
        .expect("read the marker")
        .get(0)
}

#[tokio::test]
async fn two_instances_share_a_database_without_interfering() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    let config_a = config_for_schema(db.dsn(), "instance_a");
    let config_b = config_for_schema(db.dsn(), "instance_b");
    let pool_a = Pool::new(&config_a).expect("build pool for instance a");
    let pool_b = Pool::new(&config_b).expect("build pool for instance b");

    migrate(&pool_a, &config_a)
        .await
        .expect("migrate instance a");
    migrate(&pool_b, &config_b)
        .await
        .expect("migrate instance b");

    let client_a = pool_a.get().await.expect("connect to instance a");
    let row_a = client_a
        .query_one(
            "select schema_name, instance_format_version from trellis_instance",
            &[],
        )
        .await
        .expect("read instance a's marker");
    let schema_a: String = row_a.get(0);
    assert_eq!(schema_a, "instance_a");

    let client_b = pool_b.get().await.expect("connect to instance b");
    let row_b = client_b
        .query_one(
            "select schema_name, instance_format_version from trellis_instance",
            &[],
        )
        .await
        .expect("read instance b's marker");
    let schema_b: String = row_b.get(0);
    assert_eq!(schema_b, "instance_b");

    // Each instance's search_path is pinned to its own schema, so an
    // unqualified lookup only ever sees its own marker row, never the
    // other instance's.
    let count_a: i64 = client_a
        .query_one("select count(*) from trellis_instance", &[])
        .await
        .expect("count instance a's markers")
        .get(0);
    assert_eq!(count_a, 1, "instance a must not see instance b's row");
}

#[tokio::test]
async fn reattaching_to_the_same_instance_is_a_clean_noop() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_for_schema(db.dsn(), "instance_reattach");
    let pool = Pool::new(&config).expect("build pool");

    migrate(&pool, &config).await.expect("first attach");
    // A second attach against the same schema/instance must succeed and
    // leave the marker exactly as it was.
    migrate(&pool, &config)
        .await
        .expect("re-attach to same instance");

    let client = pool.get().await.expect("connect");
    let rows = client
        .query(
            "select schema_name, instance_format_version from trellis_instance",
            &[],
        )
        .await
        .expect("read marker");
    assert_eq!(rows.len(), 1, "re-attach must not duplicate the marker row");
    let schema_name: String = rows[0].get(0);
    let format_version: i32 = rows[0].get(1);
    assert_eq!(schema_name, "instance_reattach");
    assert_eq!(format_version, trellis::identity::INSTANCE_FORMAT_VERSION);
}

#[tokio::test]
async fn marker_recording_a_different_schema_name_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_for_schema(db.dsn(), "instance_mismatch");
    let pool = Pool::new(&config).expect("build pool");

    migrate(&pool, &config).await.expect("initial attach");

    // Simulate a marker that was copied/restored under the wrong schema
    // name (e.g. a schema dump renamed on restore).
    let client = pool.get().await.expect("connect");
    client
        .execute(
            "update trellis_instance set schema_name = 'someone_elses_instance'",
            &[],
        )
        .await
        .expect("corrupt the marker's schema_name");
    drop(client);

    let err = migrate(&pool, &config)
        .await
        .expect_err("attaching with a mismatched marker must be refused");
    assert!(
        matches!(err, Error::IncompatibleInstance(_)),
        "expected IncompatibleInstance, got {err:?}"
    );
}

#[tokio::test]
async fn marker_with_a_newer_format_version_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_for_schema(db.dsn(), "instance_future");
    let pool = Pool::new(&config).expect("build pool");

    migrate(&pool, &config).await.expect("initial attach");

    // Simulate a schema last written by a newer Trellis build.
    let client = pool.get().await.expect("connect");
    client
        .execute(
            "update trellis_instance set instance_format_version = $1",
            &[&(trellis::identity::INSTANCE_FORMAT_VERSION + 1)],
        )
        .await
        .expect("simulate a newer instance format version");
    drop(client);

    let err = migrate(&pool, &config)
        .await
        .expect_err("attaching to a newer instance format version must be refused");
    assert!(
        matches!(err, Error::IncompatibleInstance(_)),
        "expected IncompatibleInstance, got {err:?}"
    );
}

#[tokio::test]
async fn preexisting_foreign_schema_without_a_marker_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    // Something else already created this schema and put a table in it,
    // before Trellis ever saw it — no `trellis_instance` marker exists.
    let admin_client = db.pool.get().await.expect("connect for setup");
    admin_client
        .batch_execute(
            "create schema foreign_schema; \
             create table foreign_schema.someone_elses_table (id int)",
        )
        .await
        .expect("set up a foreign schema");
    drop(admin_client);

    let config = config_for_schema(db.dsn(), "foreign_schema");
    let pool = Pool::new(&config).expect("build pool");

    let err = migrate(&pool, &config)
        .await
        .expect_err("attaching to a foreign, unmarked schema must be refused");
    assert!(
        matches!(err, Error::IncompatibleInstance(_)),
        "expected IncompatibleInstance, got {err:?}"
    );
}

#[tokio::test]
async fn preexisting_empty_schema_without_a_marker_is_taken_over_cleanly() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    // The schema already exists (e.g. created by an operator ahead of
    // time) but is empty — nothing foreign to protect, so Trellis may take
    // it over.
    let admin_client = db.pool.get().await.expect("connect for setup");
    admin_client
        .batch_execute("create schema instance_preexisting_empty")
        .await
        .expect("pre-create an empty schema");
    drop(admin_client);

    let config = config_for_schema(db.dsn(), "instance_preexisting_empty");
    let pool = Pool::new(&config).expect("build pool");

    migrate(&pool, &config)
        .await
        .expect("attaching to an empty pre-existing schema must succeed");
}

#[tokio::test]
async fn crash_between_migrations_and_marker_seed_recovers_cleanly() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_for_schema(db.dsn(), "instance_crash_recovery");
    let pool = Pool::new(&config).expect("build pool");

    migrate(&pool, &config).await.expect("initial attach");

    // Simulate a process death between the runner creating
    // `trellis_instance` (V9) and `seed_marker` committing: the ledger and
    // every other Trellis table (V1-V7) are intact, but the marker row
    // itself never made it in. This must NOT read as "foreign" (it has our
    // ledger) and must NOT error on the empty-table lookup (`read_marker`
    // uses `query_opt`, not `query_one`).
    let client = pool.get().await.expect("connect");
    client
        .execute("delete from trellis_instance", &[])
        .await
        .expect("simulate a crash before the marker was seeded");
    let remaining: i64 = client
        .query_one("select count(*) from trellis_instance", &[])
        .await
        .expect("count rows")
        .get(0);
    assert_eq!(remaining, 0, "trellis_instance must be empty, not absent");
    drop(client);

    migrate(&pool, &config)
        .await
        .expect("re-attaching after a crashed first migrate must succeed, not lock us out");

    let client = pool.get().await.expect("connect");
    let rows = client
        .query(
            "select schema_name, instance_format_version from trellis_instance",
            &[],
        )
        .await
        .expect("read the re-seeded marker");
    assert_eq!(rows.len(), 1, "recovery must re-seed exactly one row");
    let schema_name: String = rows[0].get(0);
    assert_eq!(schema_name, "instance_crash_recovery");

    // The ledger and pre-existing tables must be untouched by the recovery
    // path — this is a re-attach, not a re-migration.
    let applied: Vec<i32> = client
        .query(
            "select version from refinery_schema_history order by version",
            &[],
        )
        .await
        .expect("read ledger")
        .iter()
        .map(|row| row.get(0))
        .collect();
    // Issue #73 added V22; its reviewer follow-up added V23. Issue #74
    // added V24. Issue #54 (epic #49) added V25 (`metric_rollup`); later
    // removed (metric history/aggregation is left to an operator's own
    // Prometheus stack — see docs/observability.md's "Retention" section),
    // so V25 is not reissued to anything else. Issue #129 (epic #127)
    // added V26 (`relationship_projections`). Issue #133 (epic #127) added
    // V27 (`group_key_array`). Issue #134 (epic #127) added V28
    // (`relationship_reverse_deferred`). Issue #160 added V29
    // (`transform_definitions.fuse_rearmed_at`, the whole-transform fuse's
    // re-arm point). Issue #159 added V30 (`transform_fuse_gate`, that same
    // fuse's per-source-table serialization point). Issue #144 added V31
    // (`worker_registry`, one row per live drain worker). Issue #191
    // removed V8 (`pause_leases`, the fleet-wide claiming-pause lease that
    // ADR-0013's `Trellis::self_check` never ended up needing), so V8 is
    // not reissued to anything else either. Issue #142 (ADR-0014) added V32
    // (`paused` joins `transform_definitions.status`'s check constraint —
    // the operator-driven half of the pause state whose other half is the
    // poison fuse's `quarantined`). Issue #283 added V33 (folds pre-existing
    // bare-spelled quarantine rows into their qualified counterpart, now that
    // every counter/marker table keys on one canonical identity per logical
    // source table). Issue #285 added V34
    // (`relationship_definitions.from_schema`, the schema a relationship's
    // from-table resolved to at definition time — what a scoped `DROP
    // RELATIONSHIP <schema>.<from_table>.<name>` qualifier is now checked
    // against). Issues #241/#242 (ADR-0015) added V35
    // (`transform_definitions.definition_version`, the monotonic
    // per-definition edit counter `ALTER TRANSFORM` bumps). Issue #310 added
    // V36 (`slot_loss_pauses`, which transforms a lost slot paused).
    // Issue #315 added V37 (a `recompute` ring row may carry a prior-image
    // hint in `old_image`). Issue #288 added V38 (`relationship_definitions`'
    // uniqueness key widened to the schema-qualified `(from_schema,
    // from_table, name)`). Issues #311/#367 added V39
    // (`pending_backfill.generation`, so a discharge deletes only the marker
    // generation it read).
    // Issue #321 added V40 (`aggregate_extinct_horizon`, the per-target
    // extinct horizon for aggregate recompute basis checks). Issue #379 added
    // V41 (`relationship_projections.projection_schema`), later removed by
    // issue #435 (projections live in the catalog schema). Issue #418 added
    // V42 (`backfill_chunks.fuse_rearmed_at`, which tells a chunk planned
    // before a resume apart from one planned for the rebuild). Issue #407
    // added V43 (`pending_backfill`'s retry state). Issue #419 added V44
    // (`backfill_chunks`' unbounded direct-build job rows). Issue #431
    // added V45 (`pending_backfill.fence_xid`, null until the discharge
    // fences the marker). Issue #372 added V46 (`relationship_definitions
    // .to_schema`, the to-side's resolved schema). Issue #476 added V47
    // (the `catching_up` transform status). Issues #468/#485 added V48
    // (drops V18's `backfill_coverage`). Issue #507 added V49
    // (`pending_backfill.refresh_projections`). Issue #420 added V50 (drops
    // `pending_backfill.added_at`, which nothing read). Issue #531 added V51
    // (`relationship_projections.refreshed_lsn`). Issue #595 added V53
    // (`ring_slot_mirror`); V52 is left free for an experiment branch. Issue
    // #620 added V54 (`segments.row_count` and `drain_cursor`, bounded
    // drain paging). Issue #622 added V55 (`pending_backfill
    // .capture_gate_lsn`) and V56 (`column_status.awaiting_capture`). Issue
    // #623 D3 added V57 (`ledger_truncate_floor`), #622 C6 V58
    // (`capture_failures`, the `schema_changed` ring op), and #622 C8 V59
    // (drops `replication_progress` and `slot_loss_pauses`). Issue #687
    // added V60 (`capture_failures.error` and `capture_holdups`). #625 F4
    // added V63 (`backfill_chunks`' failure record, #616), and F2 V64 (the
    // Re-derive build's chunk kinds and `build` column); V61 and V62 are
    // left free for the other lane's #623 work. #623 D5 added V65 (drops
    // V40's `aggregate_extinct_horizon`), #625 F8b V67
    // (`backfill_chunks.fields`, a field build's scope), #733 V68
    // (`transform_definitions.build_seg`, the segment a Re-derive build
    // started in). V66 is unused. #769 added V69
    // (`backfill_chunks.key_collations`), #663 V70
    // (`capture_failures.kind`), #799 V71 (whole-key poison per
    // transform), #760 V72 (`definition_key_types`), #824 V74
    // (`poison.sqlstate`, `retype_releases`), #817 V75 (`drain_holdups`),
    // #828 V76 (`retype_causes`, `capture_failures.caused_by`), #912 V77
    // (`column_status.cascade_pending`), #803 V78 (one `poison_held` row
    // per held key), #944 V79 (a `recompute` row's released join value), and
    // #938 V80 (`transform_definitions.build_marker_generation`), #625 F6
    // V81 (`backfill_chunks.fence_xid`, a seam-fed build's fence), #894
    // V82 (`resume_requests.timeout_cancels`), and #877 V83
    // (`trellis_instance.target_schema`).
    // V73 is unused.
    assert_eq!(
        applied,
        vec![
            1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 43, 44, 45, 46, 47, 48, 49,
            50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 63, 64, 65, 67, 68, 69, 70, 71, 72, 74, 75, 76,
            77, 78, 79, 80, 81, 82, 83,
        ]
    );
}

// Issue #877 (epic #806): an attach refuses a catalog schema that belongs to
// another instance sharing the database, without refusing a second client of
// the same instance.

#[tokio::test]
async fn catalog_schema_public_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_with_target(db.dsn(), "public", "app_targets");

    let message = refusal(attach(&config).await);

    assert!(
        message.contains("\"public\""),
        "names the schema: {message}"
    );
    assert!(
        !schema_exists(&db, "app_targets").await,
        "a refused attach creates nothing"
    );
    let client = db.pool.get().await.expect("connect");
    let marker: bool = client
        .query_one(
            "select to_regclass('public.trellis_instance') is not null",
            &[],
        )
        .await
        .expect("look up public's marker")
        .get(0);
    assert!(!marker, "a refused attach migrates nothing into `public`");
}

#[tokio::test]
async fn catalog_schema_equal_to_its_own_target_schema_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_with_target(db.dsn(), "instance_same", "instance_same");

    let message = refusal(attach(&config).await);

    assert!(
        message.contains("\"instance_same\"") && message.contains("target schema"),
        "names the schema and says it is the target schema: {message}"
    );
    assert!(
        !schema_exists(&db, "instance_same").await,
        "a refused attach creates nothing"
    );
}

#[tokio::test]
async fn target_schema_holding_another_instances_catalog_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    // Instance B's catalog is `instance_b`; instance A is configured to put
    // its targets there.
    attach(&config_with_target(db.dsn(), "instance_b", "app_b"))
        .await
        .expect("attach instance b");

    let message = refusal(attach(&config_with_target(db.dsn(), "instance_a", "instance_b")).await);

    assert!(
        message.contains("\"instance_b\"") && message.contains("\"instance_a\""),
        "names the target schema, which is the other instance's catalog, and this \
         instance's catalog: {message}"
    );
    assert!(
        !schema_exists(&db, "instance_a").await,
        "a refused attach creates nothing"
    );
}

#[tokio::test]
async fn catalog_schema_that_is_another_instances_target_schema_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    attach(&config_with_target(
        db.dsn(),
        "instance_a",
        "instance_a_targets",
    ))
    .await
    .expect("attach instance a");

    let message =
        refusal(attach(&config_with_target(db.dsn(), "instance_a_targets", "app_b")).await);

    assert!(
        message.contains("\"instance_a_targets\"") && message.contains("\"instance_a\""),
        "names the schema and the other instance's catalog schema: {message}"
    );
    assert!(
        !schema_exists(&db, "app_b").await,
        "a refused attach creates nothing"
    );
}

#[tokio::test]
async fn the_same_instance_attaches_twice_in_sequence() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_with_target(db.dsn(), "instance_twice", "app_twice");

    attach(&config).await.expect("first client");
    attach(&config).await.expect("second client");

    assert_eq!(
        recorded_target_schema(&db, "instance_twice")
            .await
            .as_deref(),
        Some("app_twice")
    );
}

#[tokio::test]
async fn two_clients_of_the_same_instance_attach_at_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = config_with_target(db.dsn(), "instance_at_once", "app_at_once");

    // Two fresh clients, each with its own pool, both running the first-ever
    // attach. Whatever order the two interleave in, neither is refused.
    let (first, second) = tokio::join!(attach(&config), attach(&config));

    first.expect("first client");
    second.expect("second client");
    let client = db.pool.get().await.expect("connect");
    let rows: i64 = client
        .query_one(
            "select count(*) from instance_at_once.trellis_instance",
            &[],
        )
        .await
        .expect("count the markers")
        .get(0);
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn two_instances_with_conflicting_schemas_attach_at_once_admit_exactly_one() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    // A's target schema is B's catalog schema. Whichever attaches first is
    // admitted, and the other sees its marker and is refused: A first, and B
    // is refused by rule 4; B first, and A is refused by rule 3. Neither
    // order lets both through.
    let a = config_with_target(db.dsn(), "instance_a", "instance_b");
    let b = config_with_target(db.dsn(), "instance_b", "app_b");

    let (a, b) = tokio::join!(attach(&a), attach(&b));

    assert_eq!(
        [a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(),
        1,
        "exactly one of two conflicting instances attaches: a={a:?}, b={b:?}"
    );
}

#[tokio::test]
async fn an_instances_target_schema_can_change_between_deploys() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    attach(&config_with_target(db.dsn(), "instance_a", "targets_old"))
        .await
        .expect("first deploy");

    attach(&config_with_target(db.dsn(), "instance_a", "targets_new"))
        .await
        .expect("a changed target schema updates the marker, it does not refuse");

    assert_eq!(
        recorded_target_schema(&db, "instance_a").await.as_deref(),
        Some("targets_new")
    );
    // The marker follows the change: the old target schema is free to be
    // another instance's catalog, and the new one is not.
    attach(&config_with_target(db.dsn(), "targets_old", "app_b"))
        .await
        .expect("the old target schema is no longer instance a's");
    let message = refusal(attach(&config_with_target(db.dsn(), "targets_new", "app_c")).await);
    assert!(message.contains("\"targets_new\""), "{message}");
}

#[tokio::test]
async fn an_instance_can_target_the_schema_another_instance_targets() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;

    // The default topology: every instance's targets in `public`. Two
    // instances sharing a target schema is the operator's call
    // (docs/instance-identity.md), not an attach refusal.
    attach(&config_for_schema(db.dsn(), "instance_a"))
        .await
        .expect("instance a");
    attach(&config_for_schema(db.dsn(), "instance_b"))
        .await
        .expect("instance b");
}
