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

/// Define and `ALTER` count the argument values and type rules the validator
/// admitted the definition by, over its columns' live types, not only its
/// tree.
#[tokio::test]
async fn define_and_alter_count_the_values_and_type_rules_validation_used() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id bigint primary key, ts timestamp, n integer, w bigint)",
    )
    .await
    .expect("source");

    // A release that added the `week` unit and the identity integer cast in
    // 0.3.0.
    let later = handle(
        &db,
        "0.3.0",
        &[
            (r#"Value(DateTruncUnit("week"))"#, "0.3.0"),
            ("TypeRule(Cast(IntegerIdentity))", "0.3.0"),
        ],
    )
    .await;
    later
        .apply("TRANSFORM t FROM s SELECT DATE_TRUNC('week', ts) AS wk")
        .await
        .expect("define");
    assert_eq!(floor(&raw, &later, "t").await, "0.3.0");
    later
        .apply("TRANSFORM u FROM s SELECT DATE_TRUNC('day', ts) AS d, CAST(n AS bigint) AS b")
        .await
        .expect("define");
    assert_eq!(floor(&raw, &later, "u").await, "0.2.0");
    build_everything(&later).await;

    // The same `CAST` over the `bigint` column is the identity case.
    later
        .apply("ALTER TRANSFORM u ADD CAST(w AS bigint) AS c")
        .await
        .expect("add an identity cast");
    assert_eq!(floor(&raw, &later, "u").await, "0.3.0");
}

/// Define counts the `GROUP BY` and `JOIN ... ON` key types the validator
/// admitted, over the live column types: an `inet` key or an enum join key
/// declared later raises the floor, a `text` or `bigint` key doesn't.
#[tokio::test]
async fn define_counts_the_key_types_validation_admitted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create type public.status as enum ('open', 'closed'); \
         create table public.s (id bigint primary key, addr inet, label text, st public.status); \
         create table public.u (id bigint primary key, st public.status)",
    )
    .await
    .expect("sources");

    let later = handle(
        &db,
        "0.3.0",
        &[
            ("TypeRule(GroupByKey(Inet))", "0.3.0"),
            ("JoinKey(Enum)", "0.3.0"),
        ],
    )
    .await;
    for (target, text) in [
        (
            "by_addr",
            "FROM s GROUP BY addr WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        ),
        (
            "by_label",
            "FROM s GROUP BY label WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        ),
        (
            "on_status",
            "FROM s JOIN u ON s.st = u.st GROUP BY s.id WITHOUT SOURCE INDEX \
             SELECT COUNT(*) AS n",
        ),
        (
            "on_id",
            "FROM s JOIN u ON s.id = u.id GROUP BY s.label WITHOUT SOURCE INDEX \
             SELECT COUNT(*) AS n",
        ),
    ] {
        later
            .apply(&format!("TRANSFORM {target} {text}"))
            .await
            .unwrap_or_else(|e| panic!("define {target}: {e}"));
    }

    assert_eq!(floor(&raw, &later, "by_addr").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "by_label").await, "0.2.0");
    assert_eq!(floor(&raw, &later, "on_status").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "on_id").await, "0.2.0");
}

/// The key types the tables a definition reads are admitted by count, over
/// the live catalog: a 1-1 source's primary-key type (an enum included, a
/// composite key by any column, a `JOIN` side's key, an aggregate's source
/// key) and a partitioned source's partition-key type, each declared later.
/// A `text` or `bigint` key doesn't raise the floor.
#[tokio::test]
async fn define_counts_the_primary_and_partition_key_types_admitted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create type public.status as enum ('open', 'closed'); \
         create table public.pk_text (id text primary key, v text); \
         create table public.pk_big (id bigint primary key, v text); \
         create table public.pk_uuid (id uuid primary key, v text); \
         create table public.pk_enum (st public.status primary key, v text); \
         create table public.pk_two (a integer, b uuid, v text, primary key (a, b)); \
         create table public.orders (id bigint primary key, region text); \
         create table public.items (id uuid primary key, order_id bigint, qty integer); \
         create index on public.items (order_id); \
         create table public.events (id bigint not null, day date not null, \
             primary key (day, id)) partition by range (day); \
         create table public.events_a partition of public.events \
             for values from ('2026-01-01') to ('2027-01-01'); \
         create table public.events_b partition of public.events \
             for values from ('2027-01-01') to ('2028-01-01')",
    )
    .await
    .expect("sources");

    let later = handle(
        &db,
        "0.4.0",
        &[
            (r#"JoinKey(Listed("uuid"))"#, "0.3.0"),
            ("JoinKey(Enum)", "0.3.1"),
            (r#"PartitionKeyType("date")"#, "0.3.2"),
        ],
    )
    .await;
    for (target, text) in [
        ("one_text", "FROM pk_text SELECT v AS v"),
        ("one_big", "FROM pk_big SELECT v AS v"),
        ("one_uuid", "FROM pk_uuid SELECT v AS v"),
        ("one_enum", "FROM pk_enum SELECT v AS v"),
        ("one_two", "FROM pk_two SELECT v AS v"),
        (
            "agg_uuid",
            "FROM pk_uuid GROUP BY v WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        ),
        (
            "joined_uuid",
            "FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT SUM(items.qty) AS qty",
        ),
        (
            "by_day",
            "FROM events GROUP BY day WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        ),
    ] {
        later
            .apply(&format!("TRANSFORM {target} {text}"))
            .await
            .unwrap_or_else(|e| panic!("define {target}: {e}"));
    }

    assert_eq!(floor(&raw, &later, "one_text").await, "0.2.0");
    assert_eq!(floor(&raw, &later, "one_big").await, "0.2.0");
    assert_eq!(floor(&raw, &later, "one_uuid").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "one_enum").await, "0.3.1");
    assert_eq!(floor(&raw, &later, "one_two").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "agg_uuid").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "joined_uuid").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "by_day").await, "0.3.2");
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

/// One definition of each key space a release before 0.3.0 runs, and one
/// beside it that uses `CASE`, here new in 0.3.0, so its floor is 0.3.0; and
/// a definition reading one of the latter's targets.
const KEY_SPACES: &[(&str, &str)] = &[
    ("one", "TRANSFORM one FROM orders SELECT amount AS amount"),
    (
        "one_case",
        "TRANSFORM one_case FROM orders SELECT CASE WHEN amount > 0 THEN amount END AS positive",
    ),
    (
        "agg",
        "TRANSFORM agg FROM orders GROUP BY region WITHOUT SOURCE INDEX \
         SELECT SUM(amount) AS total",
    ),
    (
        "agg_case",
        "TRANSFORM agg_case FROM orders GROUP BY region WITHOUT SOURCE INDEX \
         SELECT SUM(CASE WHEN amount > 0 THEN amount END) AS total",
    ),
    (
        "jagg",
        "TRANSFORM jagg FROM orders JOIN items ON orders.id = items.order_id \
         GROUP BY orders.region SELECT SUM(items.qty) AS qty",
    ),
    (
        "jagg_case",
        "TRANSFORM jagg_case FROM orders JOIN items ON orders.id = items.order_id \
         GROUP BY orders.region SELECT SUM(CASE WHEN items.qty > 0 THEN items.qty END) AS qty",
    ),
    (
        "j1",
        "TRANSFORM j1 FROM items JOIN orders ON items.order_id = orders.id \
         SELECT items.qty AS qty, orders.region AS region",
    ),
    (
        "j1_case",
        "TRANSFORM j1_case FROM items JOIN orders ON items.order_id = orders.id \
         SELECT CASE WHEN items.qty > 0 THEN items.qty END AS positive",
    ),
    (
        "part",
        "TRANSFORM part FROM lines GROUP BY k WITHOUT SOURCE INDEX SELECT SUM(qty) AS qty",
    ),
    (
        "part_case",
        "TRANSFORM part_case FROM lines GROUP BY k WITHOUT SOURCE INDEX \
         SELECT SUM(CASE WHEN qty > 0 THEN qty END) AS qty",
    ),
];

/// The targets a release before 0.3.0 can't run.
const HELD: &[&str] = &["one_case", "agg_case", "jagg_case", "j1_case", "part_case"];

/// The targets every release runs, `reader` (which reads `one_case`'s
/// target) aside.
const RUN: &[&str] = &["one", "agg", "jagg", "j1", "part"];

/// A handle running as release `version` with `CASE` registered at 0.3.0,
/// with a staging worker and a drain thread.
async fn running(db: &testkit::TestDatabase, version: &str) -> Trellis {
    let config = Config::from_dsn(db.dsn().to_string())
        .expect("config")
        .with_binary_version(version)
        .with_feature_since("Expr(Case)", "0.3.0");
    Trellis::connect(
        config,
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect a running handle")
}

/// Every row of `target`, as text, in order.
async fn rows(raw: &Client, target: &str) -> Vec<String> {
    raw.query(
        &format!("select t::text from public.{target} t order by 1"),
        &[],
    )
    .await
    .unwrap_or_else(|err| panic!("{target}: {err}"))
    .into_iter()
    .map(|row| row.get(0))
    .collect()
}

/// Waits until each of `targets` reports `status`.
async fn wait_for(trellis: &Trellis, targets: &[&str], status: trellis::TransformStatus) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    for target in targets {
        loop {
            let now = trellis
                .status(target)
                .await
                .expect("status")
                .expect("defined")
                .status;
            if now == status {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{target} never reached {status:?}: {now:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}

/// Waits for every change committed so far to reach the targets.
async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, std::time::Duration::from_secs(60))
        .await
        .expect("converged");
}

/// `self_check` of each of `targets` finds nothing.
async fn self_check_clean(trellis: &Trellis, targets: &[&str]) {
    use trellis::staging::self_check::CheckPageExt;
    for target in targets {
        let report = trellis
            .self_check_to_end(
                target,
                trellis::SelfCheckMode::Strict,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|err| panic!("self_check {target}: {err}"));
        assert_eq!(
            report.outcome,
            trellis::SelfCheckOutcome::Converged,
            "{target}"
        );
    }
}

/// ADR-0024 rules 3-5, for every key space: a release below a definition's
/// floor pauses that definition when it attaches, never parses it, and runs
/// every other definition. Its target keeps its rows and stops updating, a
/// reader of its target reads a paused upstream, and `RESUME`, `ALTER` and
/// `DROP` refuse naming the floor. Rolled forward, it stays paused until
/// `RESUME`, which rebuilds it.
#[tokio::test]
async fn a_definition_above_the_running_release_pauses_and_the_rest_keep_running() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, region text not null, \
             amount integer not null); \
         create table public.items (id integer primary key, order_id integer not null, \
             qty integer not null); \
         create index on public.items (order_id); \
         create table public.lines (id bigint not null, k bigint not null, \
             qty integer not null, primary key (k, id)) partition by hash (k); \
         create table public.lines_0 partition of public.lines \
             for values with (modulus 2, remainder 0); \
         create table public.lines_1 partition of public.lines \
             for values with (modulus 2, remainder 1); \
         insert into public.orders values (1, 'east', 10), (2, 'west', 20); \
         insert into public.items values (1, 1, 3), (2, 2, 4); \
         insert into public.lines values (1, 1, 5), (2, 2, 6)",
    )
    .await
    .expect("sources");

    // Release 0.3.0 defines and builds everything.
    let newer = running(&db, "0.3.0").await;
    for (_, text) in KEY_SPACES {
        newer.apply(text).await.expect(text);
    }
    wait_for(&newer, RUN, trellis::TransformStatus::Live).await;
    wait_for(&newer, HELD, trellis::TransformStatus::Live).await;
    newer
        .apply("TRANSFORM reader FROM one_case SELECT positive AS positive")
        .await
        .expect("define a reader of a 0.3.0 target");
    wait_for(&newer, &["reader"], trellis::TransformStatus::Live).await;
    converge(&newer).await;
    for target in HELD {
        assert_eq!(floor(&raw, &newer, target).await, "0.3.0", "{target}");
    }
    for target in RUN.iter().chain(&["reader"]) {
        assert_eq!(floor(&raw, &newer, target).await, "0.2.0", "{target}");
    }
    newer.shutdown().await.expect("shut 0.3.0 down");

    // Rolled back to 0.2.0: its attach pauses the definitions it can't run.
    let older_attach = handle(&db, "0.2.0", &[("Expr(Case)", "0.3.0")]).await;
    older_attach.migrate().await.expect("0.2.0 attaches");
    let held_rows: Vec<Vec<String>> = {
        let mut held = Vec::new();
        for target in HELD.iter().chain(&["reader"]) {
            held.push(rows(&raw, target).await);
        }
        held
    };
    let older = running(&db, "0.2.0").await;
    wait_for(&older, RUN, trellis::TransformStatus::Live).await;
    for target in HELD {
        let status = older
            .status(target)
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(status.status, trellis::TransformStatus::Paused, "{target}");
        assert_eq!(status.floor, "0.3.0");
        let reason = status.capture_failure.expect("a reason").error;
        for part in [
            format!("public.{target}"),
            "is 0.3.0".to_string(),
            "release 0.2.0".to_string(),
            "roll forward to 0.3.0 or later, then RESUME it or DROP it there".to_string(),
        ] {
            assert!(reason.contains(&part), "{part:?} in {reason:?}");
        }
    }

    // Writes run: every definition 0.2.0 runs converges, and the others'
    // targets keep their rows.
    raw.batch_execute(
        "insert into public.orders values (3, 'east', 30); \
         update public.orders set amount = 11 where id = 1; \
         delete from public.orders where id = 2; \
         insert into public.items values (3, 3, 7); \
         update public.items set qty = 5 where id = 1; \
         insert into public.lines values (3, 1, 8); \
         delete from public.lines where id = 2",
    )
    .await
    .expect("writes");
    converge(&older).await;
    self_check_clean(&older, RUN).await;
    let mut after = Vec::new();
    for target in HELD.iter().chain(&["reader"]) {
        after.push(rows(&raw, target).await);
    }
    assert_eq!(after, held_rows, "the paused targets stop updating");
    assert_eq!(
        older
            .status("reader")
            .await
            .expect("status")
            .expect("defined")
            .status,
        trellis::TransformStatus::CatchingUp,
        "a reader of a paused target reads it as any paused upstream"
    );

    // `RESUME`, `ALTER` and `DROP` refuse, naming the floor.
    for target in HELD {
        for statement in [
            format!("RESUME TRANSFORM {target}"),
            format!("DROP TRANSFORM {target}"),
        ] {
            let err = older
                .apply(&statement)
                .await
                .expect_err("refused below the floor");
            assert_eq!(err.code(), trellis::ErrorCode::VersionFloor, "{err}");
            assert!(err.to_string().contains("0.3.0"), "{statement}: {err}");
        }
    }
    let err = older
        .apply("ALTER TRANSFORM one_case ADD amount AS again")
        .await
        .expect_err("refused below the floor");
    assert_eq!(err.code(), trellis::ErrorCode::VersionFloor, "{err}");
    older.shutdown().await.expect("shut 0.2.0 down");

    // Rolled forward to 0.3.0: still paused, until `RESUME` rebuilds it.
    let forward = running(&db, "0.3.0").await;
    converge(&forward).await;
    for target in HELD {
        let status = forward
            .status(target)
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(status.status, trellis::TransformStatus::Paused, "{target}");
    }
    for target in HELD {
        forward
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|err| panic!("resume {target}: {err}"));
    }
    wait_for(&forward, HELD, trellis::TransformStatus::Live).await;
    wait_for(&forward, &["reader"], trellis::TransformStatus::Live).await;
    converge(&forward).await;
    self_check_clean(&forward, HELD).await;
    self_check_clean(&forward, RUN).await;
    self_check_clean(&forward, &["reader"]).await;
    forward.shutdown().await.expect("shutdown");
}

/// ADR-0024 rule 3, "never parsed": a definition above the running release
/// whose stored text no grammar reads (as a newer release's text may be) is
/// paused at attach, and every path that loads definitions skips it without
/// failing: the capture pass and the drain of the table it reads, the build
/// of the others, `status()` and `definitions()`, the reported status of
/// the others, a define, a `DROP` of the table it reads (its blockers), and a
/// field drop's dependents check, which refuses naming the floor. `RESUME`
/// and `self_check` of it refuse the same way.
#[tokio::test]
async fn no_path_parses_a_definition_above_the_running_release() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, a integer); \
         insert into public.s values (1, 1)",
    )
    .await
    .expect("source");
    let definer = handle(&db, "0.2.0", &[]).await;
    for text in [
        "TRANSFORM t FROM s SELECT a AS a",
        "TRANSFORM t_too FROM s SELECT a AS a",
    ] {
        definer.apply(text).await.expect(text);
    }
    build_everything(&definer).await;
    definer
        .apply("TRANSFORM held FROM t SELECT a AS b")
        .await
        .expect("define held");
    // What a later release wrote: text this one can't read, at its floor.
    raw.execute(
        "update transform_definitions \
         set definition_text = 'TRANSFORM held FROM t SELECT FROBNICATE a AS b', \
             floor_version = '0.3.0' \
         where target_table = 'public.held'",
        &[],
    )
    .await
    .expect("a later release's definition");

    let older = running(&db, "0.2.0").await;
    older.migrate().await.expect("attach");
    wait_for(&older, &["t", "t_too"], trellis::TransformStatus::Live).await;
    raw.execute("insert into public.s values (2, 2)", &[])
        .await
        .expect("write");
    converge(&older).await;
    self_check_clean(&older, &["t", "t_too"]).await;
    assert_eq!(rows(&raw, "t").await, ["(1,1)", "(2,2)"]);

    let held = older
        .status("held")
        .await
        .expect("status")
        .expect("defined");
    assert_eq!(held.status, trellis::TransformStatus::Paused);
    assert_eq!(held.floor, "0.3.0");
    assert!(
        held.capture_failure
            .expect("a reason")
            .error
            .contains("roll forward to 0.3.0"),
    );
    let listed = older.definitions().await.expect("definitions");
    assert_eq!(listed.len(), 3);

    let refused = |what: &str, err: trellis::TrellisError| {
        assert_eq!(
            err.code(),
            trellis::ErrorCode::VersionFloor,
            "{what}: {err}"
        );
        assert!(err.to_string().contains("0.3.0"), "{what}: {err}");
    };
    refused(
        "resume",
        older
            .apply("RESUME TRANSFORM held")
            .await
            .expect_err("resume"),
    );
    refused(
        "self_check",
        older
            .self_check(
                "held",
                trellis::SelfCheckMode::Strict,
                std::time::Duration::from_secs(5),
            )
            .await
            .expect_err("self_check"),
    );
    // A reader of its target is refused as a reader of any paused target.
    let err = older
        .apply("TRANSFORM r FROM held SELECT b AS b")
        .await
        .expect_err("a reader of a paused target");
    assert_eq!(err.code(), trellis::ErrorCode::Conflict, "{err}");
    refused(
        "drop a field it may read",
        older
            .apply("ALTER TRANSFORM t DROP a")
            .await
            .expect_err("a field it may read"),
    );
    // A drop of the table it reads is refused for it as for any reader,
    // its column detail left out.
    older.apply("PAUSE TRANSFORM t").await.expect("pause t");
    let err = older
        .apply("DROP TRANSFORM t")
        .await
        .expect_err("held reads t");
    assert!(
        matches!(
            &err,
            trellis::TrellisError::Catalog(trellis::CatalogError::DependentsBlockDrop {
                dependents,
                ..
            }) if dependents == &["held".to_string()]
        ),
        "{err:?}"
    );
    older.apply("RESUME TRANSFORM t").await.expect("resume t");
    wait_for(&older, &["t"], trellis::TransformStatus::Live).await;
    raw.execute("insert into public.s values (3, 3)", &[])
        .await
        .expect("write");
    converge(&older).await;
    self_check_clean(&older, &["t", "t_too"]).await;
    older.shutdown().await.expect("shutdown");
}

/// The status, floor and pause reason `trellis` reports for `target`.
async fn paused_for(
    trellis: &Trellis,
    target: &str,
) -> (trellis::TransformStatus, String, Option<String>) {
    let status = trellis
        .status(target)
        .await
        .expect("status")
        .expect("defined");
    (
        status.status,
        status.floor,
        status.capture_failure.map(|failure| failure.error),
    )
}

/// ADR-0024 rules 2 and 3 for a handle that never migrates: a read-only
/// call neither pauses a definition above the running release nor fails on
/// it; a state-changing call pauses it before it changes anything, unless a
/// process of another version is live, when it refuses and pauses nothing;
/// and a background process pauses it when it takes over.
#[tokio::test]
async fn a_handle_that_never_migrates_pauses_what_it_cant_run_before_it_changes_anything() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, a integer); \
         insert into public.s values (1, 1)",
    )
    .await
    .expect("source");
    let newer = handle(&db, "0.3.0", &[("Expr(Case)", "0.3.0")]).await;
    for text in [
        "TRANSFORM t FROM s SELECT a AS a",
        "TRANSFORM held FROM s SELECT CASE WHEN a > 0 THEN a END AS b",
    ] {
        newer.apply(text).await.expect(text);
    }
    build_everything(&newer).await;
    assert_eq!(floor(&raw, &newer, "held").await, "0.3.0");
    let live = trellis::TransformStatus::Live;
    let paused = trellis::TransformStatus::Paused;

    // A read-only call of the older release changes nothing.
    let older = handle(&db, "0.2.0", &[("Expr(Case)", "0.3.0")]).await;
    let (status, floor, reason) = paused_for(&older, "held").await;
    assert_ne!(status, paused);
    assert_eq!((floor.as_str(), reason), ("0.3.0", None));
    assert_eq!(older.definitions().await.expect("definitions").len(), 2);

    // While the newer release's process is live, a state-changing call
    // refuses and pauses nothing.
    let newer_running = running(&db, "0.3.0").await;
    let err = older
        .apply("PAUSE TRANSFORM t")
        .await
        .expect_err("another version is live");
    assert_eq!(err.code(), trellis::ErrorCode::VersionHandover, "{err}");
    assert_eq!(paused_for(&older, "held").await.2, None);
    newer_running.shutdown().await.expect("shut 0.3.0 down");

    // Once it is gone, the call pauses the definition first.
    older.apply("PAUSE TRANSFORM t").await.expect("pause t");
    let (status, _, reason) = paused_for(&older, "held").await;
    assert_eq!(status, paused);
    let reason = reason.expect("a reason");
    assert!(
        reason.contains("roll forward to 0.3.0 or later, then RESUME it or DROP it there"),
        "{reason}"
    );

    // Resumed under the newer release, then a background process of the
    // older one that takes over without migrating pauses it again.
    for statement in ["RESUME TRANSFORM held", "RESUME TRANSFORM t"] {
        newer.apply(statement).await.expect(statement);
    }
    build_everything(&newer).await;
    assert_eq!(paused_for(&newer, "held").await.2, None);
    let older_running = running(&db, "0.2.0").await;
    wait_for(&older_running, &["held"], paused).await;
    wait_for(&older_running, &["t"], live).await;
    let reason = paused_for(&older_running, "held")
        .await
        .2
        .expect("a reason");
    assert!(
        reason.contains("this binary runs release 0.2.0"),
        "{reason}"
    );
    older_running.shutdown().await.expect("shutdown");
}

/// A `JOIN` that aliases its tables (#1458) carries the release aliases
/// arrived in as its floor; the same `JOIN` written with table names keeps
/// its floor.
#[tokio::test]
async fn a_join_with_table_aliases_carries_their_release() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.a (id bigint primary key, k bigint, g bigint); \
         create table public.b (id bigint primary key, k bigint); \
         create index on public.a (k); create index on public.b (k)",
    )
    .await
    .expect("tables");
    let later = handle(&db, "0.3.0", &[("TableAlias", "0.3.0")]).await;
    later
        .apply(
            "TRANSFORM aliased FROM a AS x JOIN b y ON x.k = y.k GROUP BY x.g \
             WITHOUT SOURCE INDEX SELECT x.g AS g, COUNT(*) AS n",
        )
        .await
        .expect("define the aliased JOIN");
    later
        .apply(
            "TRANSFORM plain FROM a JOIN b ON a.k = b.k GROUP BY a.g \
             WITHOUT SOURCE INDEX SELECT a.g AS g, COUNT(*) AS n",
        )
        .await
        .expect("define the plain JOIN");
    assert_eq!(floor(&raw, &later, "aliased").await, "0.3.0");
    assert_eq!(floor(&raw, &later, "plain").await, "0.2.0");
}

/// A `MIN`/`MAX` aggregate writes the group-basis layout with extremes
/// (`Layout(GroupBasisExtremes)`), which the key space alone doesn't show:
/// under a release that added it in 0.3.0 its definition is stamped 0.3.0,
/// which 0.2.0 refuses to drop, while an aggregate without `MIN`/`MAX` keeps
/// the plain layout's floor and drops there.
#[tokio::test]
async fn a_min_max_aggregate_carries_the_extremes_layouts_release() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, g integer, v integer); \
         create index on public.s (g, v)",
    )
    .await
    .expect("source");
    let later = [("Layout(GroupBasisExtremes)", "0.3.0")];
    let newer = handle(&db, "0.3.0", &later).await;
    newer
        .apply("TRANSFORM mm FROM s GROUP BY g SELECT MIN(v) AS lo, MAX(v) AS hi")
        .await
        .expect("define the MIN/MAX");
    newer
        .apply("TRANSFORM total FROM s GROUP BY g SELECT SUM(v) AS t, COUNT(*) AS n")
        .await
        .expect("define the SUM");
    assert_eq!(floor(&raw, &newer, "mm").await, "0.3.0");
    assert_eq!(floor(&raw, &newer, "total").await, "0.2.0");

    let older = handle(&db, "0.2.0", &later).await;
    for target in ["mm", "total"] {
        older
            .apply(&format!("PAUSE TRANSFORM {target}"))
            .await
            .expect("pause");
    }
    let refused = older
        .apply("DROP TRANSFORM mm")
        .await
        .expect_err("0.2.0 can't run the MIN/MAX")
        .to_string();
    assert!(refused.contains("0.3.0"), "{refused}");
    older
        .apply("DROP TRANSFORM total")
        .await
        .expect("0.2.0 runs the SUM");
}
