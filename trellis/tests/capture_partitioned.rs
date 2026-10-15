//! Capture installs on every table of a partitioned source's tree, staging
//! under the root's identity (ADR-0017 P2a, issue #1059).
//!
//! A statement trigger fires only for the table a statement names, so the
//! root, each middle and each leaf carries the five capture triggers, all
//! calling the root's one function set. Nothing here waits for convergence
//! (#297): a writer held open stands in for contention, and the install's
//! deadline stops it.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{self, Installed, Progress, SINGLE_TRANSACTION_TABLES};
use trellis::capture::sql::{CaptureSpec, drop_node_trigger_statements};
use trellis::config::DEFAULT_SCHEMA;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

fn spec(table: &str) -> CaptureSpec {
    CaptureSpec::new(table, vec!["id".to_string()], ["v".to_string()], Vec::new())
        .expect("valid spec")
        .with_partitioned(true)
}

/// `public.r` partitioned by list on `region`: `m1` and `m2`, each hash
/// partitioned on `id` into two leaves.
const TWO_LEVEL: &str = "create table public.r (id int, region int, v text, \
        primary key (id, region)) partition by list (region); \
     create table public.m1 partition of public.r for values in (1) partition by hash (id); \
     create table public.m2 partition of public.r for values in (2) partition by hash (id); \
     create table public.m1_0 partition of public.m1 for values with (modulus 2, remainder 0); \
     create table public.m1_1 partition of public.m1 for values with (modulus 2, remainder 1); \
     create table public.m2_0 partition of public.m2 for values with (modulus 2, remainder 0); \
     create table public.m2_1 partition of public.m2 for values with (modulus 2, remainder 1)";

fn flat(leaves: usize) -> String {
    let mut ddl =
        String::from("create table public.r (id int primary key, v text) partition by hash (id);");
    for i in 0..leaves {
        ddl.push_str(&format!(
            "create table public.r_{i} partition of public.r \
             for values with (modulus {leaves}, remainder {i});"
        ));
    }
    ddl
}

fn landed<T>(progress: Progress<T>) -> T {
    match progress {
        Progress::Done(value) => value,
        Progress::Waiting(wait) => panic!("an operation without a deadline landed: {wait}"),
    }
}

/// Every table carrying any of the instance's capture triggers, with how
/// many, and the distinct functions they call.
async fn trigger_map(client: &Client) -> Vec<(String, i64, i64)> {
    client
        .query(
            "select c.oid::regclass::text, count(*), count(distinct t.tgfoid) \
             from pg_trigger t join pg_class c on c.oid = t.tgrelid \
             where t.tgname like 'trellis\\_capture\\_%' and not t.tgisinternal \
             group by 1 order by 1",
            &[],
        )
        .await
        .expect("read triggers")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

async fn assert_every_node_carries_capture(client: &Client, tables: usize) {
    let map = trigger_map(client).await;
    assert_eq!(map.len(), tables, "{map:?}");
    for (table, triggers, _) in &map {
        assert_eq!(*triggers, 5, "{table} carries all five triggers");
    }
    // One function per event, shared by every node: the root's.
    let functions: i64 = client
        .query_one(
            "select count(distinct t.tgfoid) from pg_trigger t \
             where t.tgname like 'trellis\\_capture\\_%' and not t.tgisinternal",
            &[],
        )
        .await
        .expect("count functions")
        .get(0);
    assert_eq!(functions, 5);
    let foreign: i64 = client
        .query_one(
            "select count(*) from pg_trigger t join pg_proc p on p.oid = t.tgfoid \
             where t.tgname like 'trellis\\_capture\\_%' and not t.tgisinternal \
               and p.proname not like '%\\_r\\_%'",
            &[],
        )
        .await
        .expect("count foreign")
        .get(0);
    assert_eq!(foreign, 0, "every trigger calls the root's function");
}

/// Ring rows staged for `public.r` (the root's identity) and for anything
/// else.
async fn staged(client: &Client) -> (i64, i64) {
    let row = client
        .query_one(
            "select count(*) filter (where src_table = 'public.r'), \
                    count(*) filter (where src_table <> 'public.r') from ( \
                 select src_table from seg_0 union all select src_table from seg_1 \
                 union all select src_table from seg_2 \
                 union all select src_table from seg_3) s",
            &[],
        )
        .await
        .expect("read the ring");
    (row.get(0), row.get(1))
}

#[tokio::test]
async fn every_node_of_a_two_level_tree_carries_capture_under_the_roots_identity() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(TWO_LEVEL).await.expect("tree");
    let spec = spec("public.r");

    assert!(landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install")
    ));
    // The root, two middles, four leaves.
    assert_every_node_carries_capture(&client, 7).await;
    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.r")
            .await
            .expect("installed"),
        Installed::Complete {
            spec: spec.clone(),
            current: true
        }
    );

    // A write named on the root, a middle and a leaf each stages under the
    // root's identity.
    client
        .batch_execute(
            "insert into public.r values (1, 1, 'root'); \
             insert into public.m2 values (2, 2, 'middle')",
        )
        .await
        .expect("writes");
    let leaf: String = client
        .query_one(
            "select tableoid::regclass::text from public.r where id = 1",
            &[],
        )
        .await
        .expect("route")
        .get(0);
    client
        .batch_execute(&format!("update {leaf} set v = 'leaf'"))
        .await
        .expect("a write named on the leaf");
    let (under_root, elsewhere) = staged(&client).await;
    assert_eq!((under_root, elsewhere), (3, 0));
}

#[tokio::test]
async fn one_function_set_serves_a_leaf_with_different_attribute_numbers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    // The leaf is built apart, with a dropped column and a different column
    // order, and attached.
    client
        .batch_execute(
            "create table public.r (id int primary key, v text) partition by hash (id); \
             create table public.r_0 partition of public.r \
                 for values with (modulus 2, remainder 0); \
             create table public.r_1 (junk int, v text, id int not null); \
             alter table public.r_1 drop column junk; \
             alter table public.r_1 add primary key (id); \
             alter table public.r attach partition public.r_1 \
                 for values with (modulus 2, remainder 1)",
        )
        .await
        .expect("tree");
    let attnums: Vec<(i16, i16)> = client
        .query(
            "select (select attnum from pg_attribute where attrelid = 'public.r'::regclass \
                      and attname = 'id'), \
                    (select attnum from pg_attribute where attrelid = 'public.r_1'::regclass \
                      and attname = 'id')",
            &[],
        )
        .await
        .expect("attnums")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_ne!(
        attnums[0].0, attnums[0].1,
        "the leaf numbers id differently"
    );

    let spec = spec("public.r");
    assert!(landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install")
    ));
    client
        .batch_execute(
            "insert into public.r (id, v) select i, 'x' || i from generate_series(1, 20) i; \
             update public.r_1 set v = 'y'; \
             delete from public.r_1 where id > 10",
        )
        .await
        .expect("writes, the last two through the odd leaf");
    // Every ring row carries the root's identity and an image keyed by name.
    let row = client
        .query_one(
            "select count(*) filter (where src_table <> 'public.r' \
                        or (op = 'insert' and new_image ->> 'v' is distinct from 'x' || key) \
                        or (op = 'update' and new_image ->> 'v' is distinct from 'y')), \
                    count(*) filter (where op = 'update'), \
                    count(*) filter (where op = 'delete') from ( \
                 select * from seg_0 union all select * from seg_1 \
                 union all select * from seg_2 union all select * from seg_3) s",
            &[],
        )
        .await
        .expect("check images");
    assert_eq!(row.get::<_, i64>(0), 0);
    assert!(row.get::<_, i64>(1) > 0, "the leaf's updates were captured");
    assert!(row.get::<_, i64>(2) > 0, "the leaf's deletes were captured");
}

#[tokio::test]
async fn a_tree_of_one_hundred_tables_installs_in_one_transaction_and_reinstalls_idempotently() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(&flat(SINGLE_TRANSACTION_TABLES - 1))
        .await
        .expect("99 leaves");
    let spec = spec("public.r");
    assert!(landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install")
    ));
    assert_every_node_carries_capture(&client, SINGLE_TRANSACTION_TABLES).await;
    let before = trigger_oids(&client).await;
    assert!(
        !landed(
            install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("repeat")
        ),
        "a repeat install changes nothing"
    );
    assert_eq!(trigger_oids(&client).await, before);

    // A node that lost its triggers makes the install partial, and a repeat
    // restores only that node.
    client
        .batch_execute(
            &drop_node_trigger_statements(DEFAULT_SCHEMA, "public.r_7")
                .unwrap()
                .join(";"),
        )
        .await
        .expect("drop one node's triggers");
    assert!(matches!(
        install::installed(&client, DEFAULT_SCHEMA, "public.r")
            .await
            .expect("installed"),
        Installed::Partial { .. }
    ));
    assert!(landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("repair")
    ));
    assert_every_node_carries_capture(&client, SINGLE_TRANSACTION_TABLES).await;
}

async fn trigger_oids(client: &Client) -> Vec<(String, String)> {
    client
        .query(
            "select c.oid::regclass::text, string_agg(t.oid::text, ',' order by t.tgname) \
             from pg_trigger t join pg_class c on c.oid = t.tgrelid \
             where t.tgname like 'trellis\\_capture\\_%' and not t.tgisinternal \
               and c.relname <> 'r_7' group by 1 order by 1",
            &[],
        )
        .await
        .expect("read trigger oids")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
async fn a_tree_of_one_hundred_and_one_leaves_installs_root_first_then_node_by_node() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(&flat(101)).await.expect("101 leaves");
    let spec = spec("public.r");

    // A writer holds leaf r_50 open, so the install lands the root and the
    // leaves before it, then waits on that one and stops at its deadline.
    let writer = connect(db.dsn()).await;
    writer
        .batch_execute("begin; lock table public.r_50 in row exclusive mode")
        .await
        .expect("hold the leaf");

    let progress = install::install(
        &mut client,
        DEFAULT_SCHEMA,
        &spec,
        Some(Instant::now() + Duration::from_millis(300)),
    )
    .await
    .expect("install");
    let wait = match progress {
        Progress::Waiting(wait) => wait,
        Progress::Done(_) => panic!("the install can't land past a held leaf"),
    };
    assert_eq!(
        wait.table, "public.r",
        "the wait reports the captured table"
    );
    // The root was first, and the leaves ahead of r_50 followed.
    let map = trigger_map(&client).await;
    assert!(map.iter().any(|(t, n, _)| t == "r" && *n == 5), "{map:?}");
    assert!(map.iter().any(|(t, _, _)| t == "r_0"), "{map:?}");
    assert!(!map.iter().any(|(t, _, _)| t == "r_50"), "{map:?}");
    assert!(matches!(
        install::installed(&client, DEFAULT_SCHEMA, "public.r")
            .await
            .expect("installed"),
        Installed::Partial { .. }
    ));
    // No join marker until the install completes.
    let markers: i64 = client
        .query_one(
            "select count(*) from pending_backfill where table_name = 'public.r'",
            &[],
        )
        .await
        .expect("markers")
        .get(0);
    assert_eq!(markers, 0);

    // Released, the next call resumes with the missing nodes only.
    writer.batch_execute("rollback").await.expect("release");
    assert!(landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("resume")
    ));
    assert_every_node_carries_capture(&client, 102).await;
    let markers: i64 = client
        .query_one(
            "select count(*) from pending_backfill where table_name = 'public.r'",
            &[],
        )
        .await
        .expect("markers")
        .get(0);
    assert_eq!(
        markers, 1,
        "one join marker, parked by the last transaction"
    );

    // Uninstalling removes the triggers from every node.
    assert!(landed(
        install::uninstall(&mut client, DEFAULT_SCHEMA, "public.r", None)
            .await
            .expect("uninstall")
    ));
    assert!(trigger_map(&client).await.is_empty());
    assert_eq!(
        install::installed(&client, DEFAULT_SCHEMA, "public.r")
            .await
            .expect("installed"),
        Installed::Absent
    );
}

#[tokio::test]
async fn an_uninstall_removes_capture_from_every_node_and_the_reconcile_pass_keeps_nodes_of_a_captured_root()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(TWO_LEVEL).await.expect("tree");
    let spec = spec("public.r");
    landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install"),
    );
    // A node is not an installed table in its own right: the root names the
    // capture, so a pass doesn't uninstall a leaf the root still owns.
    let tables = trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
        .await
        .expect("installed tables");
    assert_eq!(
        tables.into_iter().collect::<Vec<_>>(),
        vec!["public.r".to_string()]
    );
    assert!(landed(
        install::uninstall(&mut client, DEFAULT_SCHEMA, "public.r", None)
            .await
            .expect("uninstall")
    ));
    assert!(trigger_map(&client).await.is_empty());
    let functions: i64 = client
        .query_one(
            "select count(*) from pg_proc where proname like 'cap\\_%'",
            &[],
        )
        .await
        .expect("functions")
        .get(0);
    assert_eq!(functions, 0);
    // A table dropped by the application takes its triggers; only the
    // functions go.
    landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install again"),
    );
    client
        .batch_execute("drop table public.r")
        .await
        .expect("drop the tree");
    landed(
        install::uninstall(&mut client, DEFAULT_SCHEMA, "public.r", None)
            .await
            .expect("uninstall a dropped tree"),
    );
}

/// A table detached from the tree, stripped of its capture triggers and
/// attached again is a bare node, and the next install gives it its triggers
/// back.
#[tokio::test]
async fn a_leaf_detached_stripped_and_reattached_gets_its_triggers_back() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(&flat(4)).await.expect("tree");
    let spec = spec("public.r");
    landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install"),
    );
    client
        .batch_execute("alter table public.r detach partition public.r_2")
        .await
        .expect("detach");
    client
        .batch_execute(
            &drop_node_trigger_statements(DEFAULT_SCHEMA, "public.r_2")
                .unwrap()
                .join(";"),
        )
        .await
        .expect("strip");
    client
        .batch_execute(
            "alter table public.r attach partition public.r_2 \
             for values with (modulus 4, remainder 2)",
        )
        .await
        .expect("attach");
    assert!(matches!(
        install::installed(&client, DEFAULT_SCHEMA, "public.r")
            .await
            .expect("installed"),
        Installed::Partial { .. }
    ));
    assert!(landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install"),
    ));
    assert_every_node_carries_capture(&client, 5).await;
}

/// Measurement, not a gate (run with `--ignored --nocapture`): the worst
/// commit a writer sees while 100 tables install in one transaction.
#[tokio::test]
#[ignore = "a measurement; prints the writer's worst commit during an install"]
async fn measure_writer_stall_during_a_one_hundred_table_install() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client
        .batch_execute(&flat(SINGLE_TRANSACTION_TABLES - 1))
        .await
        .expect("tree");
    let spec = spec("public.r");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = stop.clone();
    let dsn = db.dsn().to_string();
    let writer = tokio::spawn(async move {
        let writer = connect(&dsn).await;
        let mut worst = Duration::ZERO;
        let mut id = 0i32;
        while !flag.load(std::sync::atomic::Ordering::Relaxed) {
            id += 1;
            let started = Instant::now();
            writer
                .execute("insert into public.r values ($1, 'w')", &[&id])
                .await
                .expect("write");
            worst = worst.max(started.elapsed());
        }
        worst
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("install"),
    );
    let took = started.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let worst = writer.await.expect("writer");
    println!("install {took:?}; worst writer commit {worst:?}");
}

/// A source root attached under another table (its readers pause, and its
/// capture is left as it is, #707) keeps its nodes out of the installed
/// tables: they're still the captured root's, not orphans to uninstall.
#[tokio::test]
async fn the_nodes_of_a_root_attached_under_another_table_are_not_installed_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(TWO_LEVEL).await.expect("tree");
    landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec("public.r"), None)
            .await
            .expect("install"),
    );
    client
        .batch_execute(
            "create table public.x (id int, region int, v text, primary key (id, region)) \
                 partition by range (id); \
             alter table public.x attach partition public.r for values from (0) to (1000)",
        )
        .await
        .expect("attach the root under another table");
    let tables = trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
        .await
        .expect("installed tables");
    assert_eq!(
        tables.into_iter().collect::<Vec<_>>(),
        vec!["public.r".to_string()]
    );
}

/// A leaf detached from the tree keeps its capture triggers, which call the
/// root's functions. Uninstalling the root drops them too, rather than
/// failing on the functions' dependents, and the reconcile pass lists the
/// detached leaf as a table to uninstall until then.
#[tokio::test]
async fn uninstalling_a_root_drops_the_triggers_of_a_leaf_detached_since() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(&flat(4)).await.expect("tree");
    landed(
        install::install(&mut client, DEFAULT_SCHEMA, &spec("public.r"), None)
            .await
            .expect("install"),
    );
    client
        .batch_execute("alter table public.r detach partition public.r_2")
        .await
        .expect("detach");
    let mut tables: Vec<String> =
        trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
            .await
            .expect("installed tables")
            .into_iter()
            .collect();
    tables.sort();
    assert_eq!(
        tables,
        vec!["public.r".to_string(), "public.r_2".to_string()]
    );
    assert!(landed(
        install::uninstall(&mut client, DEFAULT_SCHEMA, "public.r", None)
            .await
            .expect("uninstall")
    ));
    assert!(trigger_map(&client).await.is_empty());
}

/// A writer that names a leaf locks only the leaf, but a one-transaction
/// install's `LOCK TABLE` of the root waits for it: the wait report names it.
#[tokio::test]
async fn a_whole_tree_install_waiting_on_a_leaf_writer_reports_that_writer() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(&flat(4)).await.expect("tree");
    let writer = connect(db.dsn()).await;
    writer
        .batch_execute("begin; lock table public.r_1 in row exclusive mode")
        .await
        .expect("hold the leaf as a writer does");
    let pid: i32 = writer
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    let progress = install::install(
        &mut client,
        DEFAULT_SCHEMA,
        &spec("public.r"),
        Some(Instant::now() + Duration::from_millis(300)),
    )
    .await
    .expect("install");
    let wait = match progress {
        Progress::Waiting(wait) => wait,
        Progress::Done(_) => panic!("the install can't land past a leaf writer"),
    };
    assert_eq!(wait.table, "public.r");
    let pids: Vec<Option<i32>> = wait.blockers.iter().map(|b| b.pid).collect();
    assert_eq!(pids, vec![Some(pid)], "{wait}");
    assert!(trigger_map(&client).await.is_empty(), "nothing installed");
    // A plain table's report is unchanged by the tree lookup.
    let plain = install::blockers(&client, "public.r_1", &["RowExclusiveLock"])
        .await
        .expect("blockers");
    assert_eq!(plain.len(), 1);
    writer.batch_execute("rollback").await.expect("release");
}
