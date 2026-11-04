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

/// Everything a drop could remove or bump: the relations in the target and
/// catalog schemas, the catalog rows naming a definition or its tables, and
/// the version fences of the tables it reads.
async fn snapshot(raw: &Client) -> Vec<String> {
    let mut rows = Vec::new();
    for query in [
        "select format('%s %s', source_table, version) from source_table_versions order by 1",
        "select n.nspname || '.' || c.relname from pg_catalog.pg_class c \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where n.nspname in ('public', 'trellis') and c.relkind in ('r', 'p') order by 1",
        "select format('%s %s %s', target_table, status, floor_version) \
         from transform_definitions order by id",
        "select format('%s %s %s', from_node_id, to_node_id, kind) from schema_edges order by 1",
        "select table_name from schema_nodes order by 1",
        "select format('%s %s', transform_table, column_name) from column_status order by 1",
    ] {
        rows.extend(
            raw.query(query, &[])
                .await
                .unwrap_or_else(|err| panic!("{query}: {err}"))
                .into_iter()
                .map(|row| row.get::<_, String>(0)),
        );
    }
    rows
}

/// `DROP TRANSFORM` of a definition whose floor is above the running release
/// refuses, naming the definition, its floor and the running release, and
/// changes nothing: its target, its hidden tables and its catalog rows stay.
/// A definition at or below the running release drops as before, and the
/// refused one drops under a release that meets its floor.
#[tokio::test]
async fn drop_refuses_a_definition_above_the_running_release() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, region text); \
         create table public.items (id integer primary key, order_id integer, qty integer); \
         create index on public.items (order_id)",
    )
    .await
    .expect("sources");

    // A release whose aggregate `JOIN` is new in 0.3.0 defines one, a
    // definition reading its target, and a definition every release can run.
    let newer = handle(&db, "0.3.0", &[("KeySpace(Join)", "0.3.0")]).await;
    newer
        .apply(
            "TRANSFORM by_region FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT SUM(items.qty) AS qty",
        )
        .await
        .expect("define the JOIN");
    newer
        .apply("TRANSFORM regions FROM orders SELECT region AS region")
        .await
        .expect("define the 1-1");
    build_everything(&newer).await;
    newer
        .apply("TRANSFORM region_qty FROM by_region SELECT qty AS qty")
        .await
        .expect("define a reader of the JOIN's target");
    assert_eq!(floor(&raw, &newer, "by_region").await, "0.3.0");
    assert_eq!(floor(&raw, &newer, "regions").await, "0.2.0");
    let join_id: i64 = raw
        .query_one(
            "select id from transform_definitions where target_table = 'public.by_region'",
            &[],
        )
        .await
        .expect("the JOIN's id")
        .get(0);

    // Rolled back to 0.2.0.
    let older = handle(&db, "0.2.0", &[]).await;
    for target in ["by_region", "region_qty", "regions"] {
        older
            .apply(&format!("PAUSE TRANSFORM {target}"))
            .await
            .expect("pause");
    }
    let before = snapshot(&raw).await;
    let hidden = format!("trellis.join_{join_id}_");
    assert!(
        before.iter().any(|row| row.starts_with(&hidden)),
        "the JOIN has hidden tables to keep: {before:?}"
    );
    assert!(before.iter().any(|row| row == "public.by_region"));

    // Refused before its dependents are read: that refusal's column detail
    // parses the definition, which this release may not read.
    let err = older
        .apply("DROP TRANSFORM by_region")
        .await
        .expect_err("a definition above the running release doesn't drop");
    assert_eq!(err.code(), trellis::ErrorCode::VersionFloor, "{err}");
    let message = err.to_string();
    for part in [
        "'by_region'",
        "floor is 0.3.0",
        "release 0.2.0",
        "roll forward",
    ] {
        assert!(message.contains(part), "{part:?} in {message:?}");
    }
    assert_eq!(
        snapshot(&raw).await,
        before,
        "a refused drop changes nothing"
    );

    // A qualified name, and a live definition, refuse the same way.
    newer
        .apply("RESUME TRANSFORM by_region")
        .await
        .expect("resume under the newer release");
    let err = older
        .apply("DROP TRANSFORM public.by_region")
        .await
        .expect_err("a live definition above the running release doesn't drop");
    assert_eq!(err.code(), trellis::ErrorCode::VersionFloor, "{err}");
    newer
        .apply("PAUSE TRANSFORM by_region")
        .await
        .expect("pause again");

    // At or below the running release, a drop is as it always was.
    older
        .apply("DROP TRANSFORM regions")
        .await
        .expect("drop a definition the running release meets");
    assert!(
        !snapshot(&raw)
            .await
            .iter()
            .any(|row| row == "public.regions"),
        "its target went"
    );

    // Rolled forward, the same drop removes everything, once its reader
    // has gone first.
    newer
        .apply("DROP TRANSFORM region_qty")
        .await
        .expect("drop the reader");
    newer
        .apply("DROP TRANSFORM by_region")
        .await
        .expect("drop under a release that meets the floor");
    // A version fence only counts up, so the target's own stays.
    let after = snapshot(&raw).await;
    assert!(
        !after.iter().any(|row| row.starts_with(&hidden)
            || (row.contains("by_region") && !row.starts_with("public.by_region "))),
        "{after:?}"
    );
}
