//! Which marker discharges enumerate the table into the ring: every one on a
//! table something reads (issue #417). A direct build's go-live catch-up
//! always does, even for a table that hasn't changed since the build read it
//! (issues #468, #485).

use std::collections::HashMap;

use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, create_relationship, install_definition};
use trellis::intake::publication;

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
        .batch_execute(
            "create table widgets (id bigint primary key); \
             create publication test_pub;",
        )
        .await
        .expect("create source");
    register_reader(&db).await;
    client
        .batch_execute("insert into widgets (id) values (1), (2), (3)")
        .await
        .expect("seed source");

    let table = format!("{DEFAULT_SCHEMA}.widgets");
    publication::reconcile_publication(&mut client, "test_pub", std::slice::from_ref(&table))
        .await
        .expect("reconcile adds widgets");
    publication::run_pending_backfills(
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
             alter table public.posts replica identity full; \
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
    publication::settle_builds(&db.pool).await;
    assert_eq!(
        pending_marker_count(&client).await,
        2,
        "one catch-up per table read"
    );

    publication::run_pending_backfills(
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
             alter table public.posts replica identity full; \
             insert into public.authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into public.posts (id, author_id, words) values (100, 1, 10), (101, 2, 5); \
             create publication test_pub;",
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
    publication::settle_registrations(&db.pool).await;
    // The build's own go-live catch-ups already enumerated both tables.
    let before = recompute_count(&client, "public.posts").await;
    client
        .batch_execute("insert into public.posts (id, author_id, words) values (102, 1, 7)")
        .await
        .expect("write posts after the build");

    publication::reconcile_publication(&mut client, "test_pub", &["public.posts".to_string()])
        .await
        .expect("reconcile adds public.posts");
    publication::run_pending_backfills(
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
