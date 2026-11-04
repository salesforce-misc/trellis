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
