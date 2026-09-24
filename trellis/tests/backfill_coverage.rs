//! Issue #79 (bug B): a table whose current contents a *direct* backfill has
//! already folded into a built target must not be re-enumerated row-by-row
//! when it joins the CDC publication. These tests pin the two layers of that
//! fix:
//!
//! - the publication-layer decision itself (`record_backfill_coverage` +
//!   `run_pending_backfills`): a covered, unchanged table skips enumeration; a
//!   table written to *after* the coverage fence is still fully enumerated; a
//!   table with no coverage at all is enumerated as it always was; and
//!
//! - the front-door wiring (`install_definition`): a relationship-enriched 1-1
//!   definition built via the fast path records coverage for its own source
//!   *and* every to-side relationship table it reads, so that to-side table's
//!   later publication join skips its catch-up.

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

/// A covered table that has not changed since its coverage fence skips the
/// catch-up enumeration entirely: the marker is discharged with zero staged
/// rows.
#[tokio::test]
async fn covered_and_unchanged_table_skips_enumeration() {
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
        .expect("seed source with pre-existing rows");

    let table = format!("{DEFAULT_SCHEMA}.widgets");
    publication::record_backfill_coverage(&client, &table)
        .await
        .expect("record coverage as of the current 3 rows");

    publication::reconcile_publication(&mut client, "test_pub", std::slice::from_ref(&table))
        .await
        .expect("reconcile adds widgets and leaves a marker");
    assert_eq!(pending_marker_count(&client).await, 1);

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
        0,
        "a covered, unchanged table must stage nothing"
    );
    assert_eq!(
        pending_marker_count(&client).await,
        0,
        "the marker is still discharged even when enumeration is skipped"
    );
}

/// A write between the coverage fence and the publication join makes the
/// coverage stale, so the table is still fully enumerated — the safety
/// property that distinguishes this fix from a blanket "skip whenever any
/// coverage exists".
#[tokio::test]
async fn a_write_after_the_coverage_fence_forces_full_enumeration() {
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
    publication::record_backfill_coverage(&client, &table)
        .await
        .expect("record coverage as of the current 3 rows");

    // A genuine write lands *after* the coverage fence but *before* the table
    // joins the publication — exactly the gap a direct build can't have seen.
    client
        .execute("insert into widgets (id) values (4)", &[])
        .await
        .expect("write a fourth row after the fence");

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
        4,
        "stale coverage must fall back to enumerating every current row"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
}

/// No coverage record at all (the ring-fallback path never writes one) means
/// the enumeration runs exactly as it always has — the regression guard for
/// the safety-valve default.
#[tokio::test]
async fn a_table_with_no_coverage_is_enumerated_as_before() {
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
        "with no coverage record, every pre-existing row is enumerated"
    );
}

/// An in-place UPDATE after the coverage fence keeps the row *count* unchanged,
/// so the count check alone would wrongly certify the table as covered. The
/// per-row `xmin`-visibility check must independently catch it: the updated
/// tuple carries a fresh, fence-invisible `xmin`, forcing full enumeration.
/// This pins the `xmin` half of `coverage_covers`'s AND as load-bearing —
/// distinct from the insert case (which the count check would also catch).
#[tokio::test]
async fn an_update_after_the_coverage_fence_forces_full_enumeration() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table widgets (id bigint primary key, label text); \
             create publication test_pub;",
        )
        .await
        .expect("create source");
    register_reader(&db).await;
    client
        .batch_execute("insert into widgets (id, label) values (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .expect("seed source");

    let table = format!("{DEFAULT_SCHEMA}.widgets");
    publication::record_backfill_coverage(&client, &table)
        .await
        .expect("record coverage as of the current 3 rows");

    // An in-place update: still 3 rows, but row 2's current value differs from
    // what a direct build folded at the fence. Count is unchanged, so only the
    // xmin-visibility check can catch this.
    client
        .execute("update widgets set label = 'B' where id = 2", &[])
        .await
        .expect("update a row after the fence without changing the count");

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
        "an update the count check can't see must still force enumeration via xmin"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
}

/// A write in the window *between* the pre-build fence capture and the
/// coverage write — i.e. concurrent with the direct build itself — must force
/// enumeration. The build reads each table once, early; a fence captured after
/// the build would be newer than such a write and wrongly certify it as
/// covered, silently dropping it (the table joins the publication only later,
/// so CDC never carries it either). Capturing the fence *before* the build (via
/// [`publication::capture_backfill_coverage_fence`]) leaves the write invisible
/// in the fence, so enumeration runs. This pins issue #79 bug B's fence timing.
#[tokio::test]
async fn a_write_during_the_build_window_forces_full_enumeration() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table widgets (id bigint primary key, label text); \
             create publication test_pub;",
        )
        .await
        .expect("create source");
    register_reader(&db).await;
    client
        .batch_execute("insert into widgets (id, label) values (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .expect("seed source");

    let table = format!("{DEFAULT_SCHEMA}.widgets");

    // Phase 1: capture the fence *before* the build reads the table.
    let fence = publication::capture_backfill_coverage_fence(&client, &table)
        .await
        .expect("capture pre-build fence");

    // A write lands while the build is running — after the fence, before the
    // coverage record is written. An in-place update keeps the count at 3, so
    // only the pre-build fence's xmin visibility can catch it.
    client
        .execute("update widgets set label = 'B' where id = 2", &[])
        .await
        .expect("write during the build window");

    // Phase 2: the build finished; persist coverage with the pre-build fence.
    publication::write_backfill_coverage(&client, &table, &fence)
        .await
        .expect("write coverage with the pre-build fence");

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
        "a build-window write the build never folded must force enumeration"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
}

/// End-to-end: a relationship-enriched 1-1 definition fast-built through
/// `install_definition` records coverage for its own source *and* both to-side
/// relationship tables it reads — and so a to-side table's later publication
/// join skips its catch-up enumeration.
#[tokio::test]
async fn install_definition_records_coverage_and_a_to_side_join_skips() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Source + related tables live explicitly in `public`, so the schema the
    // catalog resolves for them matches the qualified names used below.
    client
        .batch_execute(
            "create table public.authors (id integer primary key, name text); \
             create table public.posts (id integer primary key, author_id integer, words integer); \
             create table public.comments (id integer primary key, author_id integer); \
             alter table public.posts replica identity full; \
             alter table public.comments replica identity full; \
             insert into public.authors (id, name) values (1, 'a'), (2, 'b'), (3, 'c'); \
             insert into public.posts (id, author_id, words) values (100, 1, 10), (101, 1, 20), (102, 2, 5); \
             insert into public.comments (id, author_id) values (200, 1), (201, 2); \
             create publication test_pub;",
        )
        .await
        .expect("seed authors + related tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create comments relationship");

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("id".to_string(), ValueType::Numeric)]);
    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
             SUM(posts.words) AS word_sum, COUNT(comments.id) AS comment_count",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition via the direct relationship path");
    // ADR-0016 (#419): the direct build runs as a background job the
    // discharge dispatches.
    publication::settle_registrations(&db.pool).await;

    // Coverage recorded for the source and both to-side tables it reads.
    let covered: Vec<String> = client
        .query(
            "select table_name from backfill_coverage order by table_name",
            &[],
        )
        .await
        .expect("read backfill_coverage")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        covered,
        vec![
            "public.authors".to_string(),
            "public.comments".to_string(),
            "public.posts".to_string(),
        ],
        "direct build records coverage for its source and every to-side table"
    );

    // `posts` joins the publication with no intervening writes → skip.
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
        recompute_count(&client, "public.posts").await,
        0,
        "a fast-built to-side table's catch-up is skipped"
    );
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
    // A write after the build's coverage fence, so coverage can't skip the
    // enumeration either: only the reader check decides.
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
        recompute_count(&client, "public.posts").await,
        3,
        "a table read through a relationship has a reader, so it is enumerated"
    );
    assert_eq!(pending_marker_count(&client).await, 0);
}
