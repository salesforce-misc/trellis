//! Definition floors (ADR-0024): each write of a definition, and each
//! rebuild of its hidden tables on a resume, stamps the oldest release that
//! can run it, and `status()` reports it. A test stands in for a later
//! release by injecting the binary version (`Config::with_binary_version`)
//! and the release a feature arrived in (`Config::with_feature_since`).
//! Every step is a call; nothing waits for a background pass.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::{Config, Trellis, TrellisOptions};

async fn raw(db: &testkit::TestDatabase) -> Client {
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public",
            trellis::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("search_path");
    client
}

/// A define-only handle running as release `version`, with each
/// `(feature, since)` in `later` registered at that release.
async fn handle(db: &testkit::TestDatabase, version: &str, later: &[(&str, &str)]) -> Trellis {
    let mut config = Config::from_dsn(db.dsn().to_string())
        .expect("config")
        .with_binary_version(version);
    for &(feature, since) in later {
        config = config.with_feature_since(feature, since);
    }
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
}

/// Runs every queued build to completion by hand, the stand-in for a drain
/// worker, so the definitions are `live` for an `ALTER`.
async fn build_everything(trellis: &Trellis) {
    trellis::intake::harness::start_registered_builds(trellis.pool())
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(trellis.pool()).await;
}

/// The stored floor of `target`, and the one its `status()` reports.
async fn floor(raw: &Client, trellis: &Trellis, target: &str) -> String {
    let stored: String = raw
        .query_one(
            "select floor_version from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read the floor")
        .get(0);
    let reported = trellis
        .status(target)
        .await
        .expect("status")
        .expect("defined")
        .floor;
    assert_eq!(reported, stored, "status() reports the stored floor");
    stored
}

/// Define stamps the floor of what it writes; an `ALTER` raises it when it
/// adds the only later construct and lowers it when it removes it; a pause
/// leaves it; a resume's rebuild stamps the layout the running release
/// writes.
#[tokio::test]
async fn define_alter_pause_and_resume_each_leave_the_floor_they_should() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute("create table public.s (id bigint primary key, a numeric, f boolean)")
        .await
        .expect("source");

    // A release that added `CASE` in 0.3.0.
    let case_later = handle(&db, "0.3.0", &[("Expr(Case)", "0.3.0")]).await;
    case_later
        .apply("TRANSFORM t FROM s SELECT a AS a")
        .await
        .expect("define");
    assert_eq!(floor(&raw, &case_later, "t").await, "0.2.0");
    build_everything(&case_later).await;

    case_later
        .apply("ALTER TRANSFORM t ADD CASE WHEN f THEN a END AS b")
        .await
        .expect("add a CASE field");
    assert_eq!(floor(&raw, &case_later, "t").await, "0.3.0");
    build_everything(&case_later).await;

    case_later
        .apply("ALTER TRANSFORM t DROP b")
        .await
        .expect("drop the CASE field");
    assert_eq!(floor(&raw, &case_later, "t").await, "0.2.0");

    // A release whose 1-1 ledger layout is new in 0.3.0: a pause leaves the
    // floor as it was, and the resume's rebuild writes the new layout.
    let layout_later = handle(&db, "0.3.0", &[("Layout(OneToOneLedger)", "0.3.0")]).await;
    layout_later
        .apply("PAUSE TRANSFORM t")
        .await
        .expect("pause");
    assert_eq!(floor(&raw, &layout_later, "t").await, "0.2.0");
    layout_later
        .apply("RESUME TRANSFORM t")
        .await
        .expect("resume");
    assert_eq!(floor(&raw, &layout_later, "t").await, "0.3.0");

    // Reading it under this build changes nothing.
    let this_build = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    assert_eq!(floor(&raw, &this_build, "t").await, "0.3.0");
}

/// A partitioned source counts, and a floor never exceeds the release that
/// stamps it.
#[tokio::test]
async fn a_partitioned_source_counts_and_the_floor_stops_at_the_running_release() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.plain (id bigint primary key, a numeric, f boolean); \
         create table public.lines (id bigint not null, k bigint not null, a numeric, \
             primary key (k, id)) partition by hash (k); \
         create table public.lines_0 partition of public.lines \
             for values with (modulus 2, remainder 0); \
         create table public.lines_1 partition of public.lines \
             for values with (modulus 2, remainder 1)",
    )
    .await
    .expect("sources");

    let later = handle(
        &db,
        "0.3.0",
        &[("PartitionedSource", "0.3.0"), ("Expr(Case)", "0.4.0")],
    )
    .await;
    later
        .apply("TRANSFORM by_k FROM lines GROUP BY k WITHOUT SOURCE INDEX SELECT COUNT(*) AS n")
        .await
        .expect("define over a partitioned source");
    later
        .apply("TRANSFORM by_flag FROM plain GROUP BY f WITHOUT SOURCE INDEX SELECT COUNT(*) AS n")
        .await
        .expect("define over a plain source");
    later
        .apply("TRANSFORM cased FROM plain SELECT CASE WHEN f THEN a END AS b")
        .await
        .expect("define with a construct from after the running release");

    assert_eq!(floor(&raw, &later, "by_k").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "by_flag").await, "0.2.0");
    assert_eq!(floor(&raw, &later, "cased").await, "0.3.0");
}
