//! Which marker discharges enumerate the table into the ring: every one on a
//! table something reads (issue #417). A direct build's go-live catch-up
//! always does, even for a table that hasn't changed since the build read it
//! (issues #468, #485).

use std::collections::HashMap;

use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, create_relationship, install_definition};
use trellis::intake::markers;

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

/// Installs `tables`' capture (one capture reconcile pass), which parks a
/// join marker for each.
async fn capture(client: &mut Client, tables: &[String]) {
    let outcome = trellis::capture::reconcile::reconcile(
        client,
        DEFAULT_SCHEMA,
        tables,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
    )
    .await
    .expect("capture pass");
    assert!(
        outcome.failed.is_empty() && outcome.waiting.is_empty(),
        "{outcome:?}"
    );
}

/// Recompute rows staged into the fresh install's active segment (`seg_0`) for
/// `src_table` — the enumeration's signature.
async fn recompute_count(client: &Client, src_table: &str) -> i64 {
    client
        .query_one(
            "select count(*) from seg_0 where op = 'recompute' and src_table = $1",
            &[&src_table],
        )
        .await
        .expect("count seg_0 recompute rows")
        .get(0)
}

/// Registers a definition reading `widgets`, so the discharge has a reader to
/// stage for (issue #417). Called while `widgets` is still empty, so the
/// registration's own read stages nothing and every counted `Recompute` is
/// the discharge's.
async fn register_reader(db: &testkit::TestDatabase) {
    trellis::defs::create_definition(
        &db.pool,
        "TRANSFORM widgets_reader FROM widgets SELECT id AS total",
        &HashMap::from([("id".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("register a definition reading widgets");
}

async fn pending_marker_count(client: &Client) -> i64 {
    client
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count pending_backfill")
        .get(0)
}

#[tokio::test]
async fn a_table_with_a_reader_is_enumerated() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table widgets (id bigint primary key)")
        .await
        .expect("create source");
    register_reader(&db).await;
    client
        .batch_execute("insert into widgets (id) values (1), (2), (3)")
        .await
        .expect("seed source");

    let table = format!("{DEFAULT_SCHEMA}.widgets");
    capture(&mut client, std::slice::from_ref(&table)).await;
    markers::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");

    assert_eq!(
        recompute_count(&client, &table).await,
        3,
        "every pre-existing row is enumerated for the reader"
    );
}

/// Issues #468, #485: a direct build's go-live catch-up re-reads every table
/// the build read, even one nothing has written since. (A coverage record
/// used to let it skip such a table, but "unchanged" can't be told apart
/// from a row inserted and deleted again during the build.)
#[tokio::test]
async fn a_direct_builds_catch_up_re_reads_an_unchanged_table() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.authors (id integer primary key, name text); \
             create table public.posts (id integer primary key, author_id integer, words integer); \
             insert into public.authors (id, name) values (1, 'a'), (2, 'b'), (3, 'c'); \
             insert into public.posts (id, author_id, words) values (100, 1, 10), (101, 1, 20), (102, 2, 5);",
        )
        .await
        .expect("seed authors + posts");
    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    install_definition(
        &db.pool,
        "TRANSFORM author_words FROM authors SELECT SUM(posts.words) AS word_sum",
        &HashMap::from([("id".to_string(), ValueType::Numeric)]),
        "public",
    )
    .await
    .expect("install_definition via the direct relationship path");
    markers::settle_builds(&db.pool).await;
    assert_eq!(
        pending_marker_count(&client).await,
        2,
        "one catch-up per table read"
    );

    markers::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");

    assert_eq!(recompute_count(&client, "public.authors").await, 3);
    assert_eq!(recompute_count(&client, "public.posts").await, 3);
    assert_eq!(pending_marker_count(&client).await, 0);
}

/// Issue #417: the discharge skips a table no definition reads, but a table a
/// definition reads only through a relationship still has a reader. Its marker
/// must be enumerated, or a to-side row the definition never saw (#393's gap
/// on a fresh install, say) would never re-derive the rows that depend on it.
#[tokio::test]
async fn a_table_read_only_through_a_relationship_is_still_enumerated() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.authors (id integer primary key, name text); \
             create table public.posts (id integer primary key, author_id integer, words integer); \
             insert into public.authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into public.posts (id, author_id, words) values (100, 1, 10), (101, 2, 5)",
        )
        .await
        .expect("seed authors + posts");
    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    install_definition(
        &db.pool,
        "TRANSFORM author_words FROM authors SELECT SUM(posts.words) AS word_sum",
        &HashMap::from([("id".to_string(), ValueType::Numeric)]),
        "public",
    )
    .await
    .expect("install_definition via the direct relationship path");
    markers::settle_registrations(&db.pool).await;
    // The build's own go-live catch-ups already enumerated both tables.
    let before = recompute_count(&client, "public.posts").await;
    client
        .batch_execute("insert into public.posts (id, author_id, words) values (102, 1, 7)")
        .await
        .expect("write posts after the build");

    capture(&mut client, &["public.posts".to_string()]).await;
    markers::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");

    assert_eq!(
        recompute_count(&client, "public.posts").await - before,
        3,
        "a table read through a relationship has a reader, so it is enumerated"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
}

async fn status_of(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read a definition's status")
        .get(0)
}

async fn discharge(client: &mut Client) {
    markers::run_pending_backfills(
        client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        std::time::Duration::ZERO,
    )
    .await
    .expect("run_pending_backfills");
}

/// Issue #732: a definition the Re-derive build will start reads its source
/// itself, from its own chunks, so a discharge that runs before its start (a
/// capture gate holding the start, say) has no reader to enumerate the table
/// for. Both Re-derive shapes: a plain aggregate (#625 F3) and a plain 1-1
/// target (F8a).
#[tokio::test]
async fn a_table_only_waiting_rederive_builds_read_is_not_enumerated() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table widgets (id bigint primary key, grp text, n bigint); \
             insert into widgets (id, grp, n) values (1, 'a', 1), (2, 'a', 2), (3, 'b', 3)",
        )
        .await
        .expect("seed source");
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("grp".to_string(), ValueType::Text),
        ("n".to_string(), ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM widget_totals FROM widgets GROUP BY grp SELECT grp AS grp, SUM(n) AS total",
        &columns,
        "public",
    )
    .await
    .expect("register a plain aggregate");
    install_definition(
        &db.pool,
        "TRANSFORM widget_copy FROM widgets SELECT n AS n",
        &columns,
        "public",
    )
    .await
    .expect("register a plain 1-1 target");

    let table = format!("{DEFAULT_SCHEMA}.widgets");
    capture(&mut client, std::slice::from_ref(&table)).await;
    assert_eq!(
        pending_marker_count(&client).await,
        1,
        "capture parks a marker"
    );
    discharge(&mut client).await;

    assert_eq!(
        recompute_count(&client, &table).await,
        0,
        "nothing reads the table until the Re-derive builds start, and they read it themselves"
    );
    assert!(
        !trellis::staging::has_pending(&client)
            .await
            .expect("has_pending"),
        "the discharge staged nothing"
    );
    assert_eq!(
        pending_marker_count(&client).await,
        0,
        "the marker is discharged"
    );
    for target in ["widget_totals", "widget_copy"] {
        assert_eq!(
            status_of(&client, target).await,
            "waiting_to_backfill",
            "the discharge leaves {target} to the Re-derive build's start"
        );
    }
}

/// Issue #732: leaving a waiting Re-derive build out of the discharge's
/// readers doesn't leave out the others. A table a `live` definition also
/// reads is still enumerated for it.
#[tokio::test]
async fn a_waiting_rederive_build_beside_a_live_reader_still_enumerates() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table widgets (id bigint primary key, grp text, n bigint)")
        .await
        .expect("create source");
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("grp".to_string(), ValueType::Text),
        ("n".to_string(), ValueType::Numeric),
    ]);
    trellis::defs::create_definition(
        &db.pool,
        "TRANSFORM widget_copy FROM widgets SELECT n AS n",
        &columns,
    )
    .await
    .expect("register a live plain 1-1 target");
    assert_eq!(status_of(&client, "widget_copy").await, "live");

    client
        .batch_execute(
            "insert into widgets (id, grp, n) values (1, 'a', 1), (2, 'a', 2), (3, 'b', 3)",
        )
        .await
        .expect("seed source");
    install_definition(
        &db.pool,
        "TRANSFORM widget_totals FROM widgets GROUP BY grp SELECT grp AS grp, SUM(n) AS total",
        &columns,
        "public",
    )
    .await
    .expect("register a plain aggregate");

    let table = format!("{DEFAULT_SCHEMA}.widgets");
    let before = recompute_count(&client, &table).await;
    capture(&mut client, std::slice::from_ref(&table)).await;
    discharge(&mut client).await;

    assert_eq!(
        recompute_count(&client, &table).await - before,
        3,
        "the live reader still gets every row"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
    assert_eq!(
        status_of(&client, "widget_totals").await,
        "waiting_to_backfill"
    );
}

/// Issue #732: a waiting Re-derive build doesn't read a table its source
/// merely has a relationship to (a definition reading through one isn't
/// Re-derive-built yet), so a marker on that table has no reader either.
#[tokio::test]
async fn a_waiting_rederive_builds_related_table_is_not_enumerated() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.authors (id integer primary key, grp text); \
             create table public.posts (id integer primary key, author_id integer, words integer); \
             insert into public.authors (id, grp) values (1, 'a'), (2, 'b'); \
             insert into public.posts (id, author_id, words) values (100, 1, 10), (101, 2, 5)",
        )
        .await
        .expect("seed authors + posts");
    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    install_definition(
        &db.pool,
        "TRANSFORM authors_per_grp FROM authors GROUP BY grp SELECT grp AS grp, COUNT(id) AS n",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("grp".to_string(), ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("register a plain aggregate on authors");

    capture(&mut client, &["public.posts".to_string()]).await;
    discharge(&mut client).await;

    assert_eq!(recompute_count(&client, "public.posts").await, 0);
    assert_eq!(pending_marker_count(&client).await, 0);
    assert_eq!(
        status_of(&client, "authors_per_grp").await,
        "waiting_to_backfill"
    );
}

/// The marker row's generation for `table`, which every park draws afresh.
async fn marker_generation(client: &Client, table: &str) -> i64 {
    client
        .query_one(
            "select generation from pending_backfill where table_name = $1",
            &[&table],
        )
        .await
        .expect("read the marker's generation")
        .get(0)
}

async fn build_marker_generation(client: &Client, target: &str) -> Option<i64> {
    client
        .query_one(
            "select build_marker_generation from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read a definition's build_marker_generation")
        .get(0)
}

/// Captures `widgets` (three rows already in it), registers `widget_copy`, a
/// plain 1-1 target the Re-derive build takes, starts its build and runs it
/// to `live`, all before the capture's install marker is discharged: the
/// order the maintenance loop's first reconcile pass can meet when the drain
/// workers finish a small build before it discharges the marker (#938).
/// Returns the connection and the definition's id.
async fn build_finished_before_the_install_marker_is_discharged(
    db: &testkit::TestDatabase,
    live_reader_first: bool,
) -> (Client, i64) {
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table widgets (id bigint primary key, grp text, n bigint); \
             insert into widgets (id, grp, n) values (1, 'a', 1), (2, 'a', 2), (3, 'b', 3)",
        )
        .await
        .expect("seed source");
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("grp".to_string(), ValueType::Text),
        ("n".to_string(), ValueType::Numeric),
    ]);
    if live_reader_first {
        trellis::defs::create_definition(
            &db.pool,
            "TRANSFORM widget_old FROM widgets SELECT id AS id2",
            &columns,
        )
        .await
        .expect("register a reader that goes live before the capture");
        assert_eq!(status_of(&client, "widget_old").await, "live");
    }
    let definition = install_definition(
        &db.pool,
        "TRANSFORM widget_copy FROM widgets SELECT n AS n",
        &columns,
        "public",
    )
    .await
    .expect("register a plain 1-1 target");
    let table = format!("{DEFAULT_SCHEMA}.widgets");
    capture(&mut client, std::slice::from_ref(&table)).await;
    let taken =
        trellis::staging::build::start_ready_builds(&mut client, &db.pool, &[definition.id])
            .await
            .expect("start the Re-derive build");
    assert_eq!(taken, vec![definition.id], "the build starts");
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(status_of(&client, "widget_copy").await, "live");
    assert_eq!(
        pending_marker_count(&client).await,
        1,
        "the install marker is still waiting for its discharge"
    );
    (client, definition.id)
}

/// Issue #938 (a): a Re-derive build that started after the capture install
/// parked its marker, and finished before the marker's discharge, read every
/// row itself and has had every later change staged, so the discharge has
/// nothing to enumerate for it.
#[tokio::test]
async fn a_rederive_build_finished_before_its_install_markers_discharge_is_not_enumerated_for() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, _) = build_finished_before_the_install_marker_is_discharged(&db, false).await;
    let table = format!("{DEFAULT_SCHEMA}.widgets");
    assert_eq!(
        build_marker_generation(&client, "widget_copy").await,
        Some(marker_generation(&client, &table).await),
        "the start read the install's marker"
    );

    discharge(&mut client).await;

    assert_eq!(
        recompute_count(&client, &table).await,
        0,
        "no Recompute is staged for the finished build"
    );
    assert!(
        !trellis::staging::has_pending(&client)
            .await
            .expect("has_pending"),
        "the discharge staged nothing"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
    let copied: i64 = client
        .query_one("select count(*) from public.widget_copy", &[])
        .await
        .expect("count the target")
        .get(0);
    assert_eq!(copied, 3, "the build itself read every row");
}

/// Issue #938 (a), the other direction: a park after the build started draws a
/// newer generation than the one its start read, so the build counts as a
/// reader of the new marker again. The skip is for a marker the start has
/// seen, not for every marker on the table.
#[tokio::test]
async fn a_marker_parked_after_a_rederive_builds_start_is_still_enumerated() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, _) = build_finished_before_the_install_marker_is_discharged(&db, false).await;
    let table = format!("{DEFAULT_SCHEMA}.widgets");
    let started_at = build_marker_generation(&client, "widget_copy").await;
    // A park, as `markers::park_marker` makes one.
    client
        .execute(
            "update pending_backfill set generation = default where table_name = $1",
            &[&table],
        )
        .await
        .expect("re-park the marker");
    assert_ne!(
        Some(marker_generation(&client, &table).await),
        started_at,
        "the re-park drew a new generation"
    );

    discharge(&mut client).await;

    assert_eq!(
        recompute_count(&client, &table).await,
        3,
        "every row is enumerated for the live reader"
    );
}

/// Issue #938 (a): skipping the finished build doesn't skip another reader.
/// One that went live before the capture existed (a repair install, say) has
/// no start that read this marker, and still needs every row.
#[tokio::test]
async fn a_reader_the_install_did_not_precede_still_gets_the_enumeration_beside_a_finished_build() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (mut client, _) = build_finished_before_the_install_marker_is_discharged(&db, true).await;
    let table = format!("{DEFAULT_SCHEMA}.widgets");
    // The live reader's own registration staged rows before the capture.
    let before = recompute_count(&client, &table).await;

    discharge(&mut client).await;

    assert_eq!(
        recompute_count(&client, &table).await - before,
        3,
        "the older live reader still gets every row"
    );
}
