//! A source that joins a partition or inheritance hierarchy after its
//! definitions were accepted pauses them (issue #707, case 3).
//!
//! Define refuses a table in a hierarchy (`trellis::defs::hierarchy`): a
//! statement trigger fires only for the table a statement names, so capture
//! on a partition misses every write routed through its parent, and so on.
//! Nothing stops the `ATTACH PARTITION`, `INHERIT` or drop-and-recreate that
//! puts an accepted source in one later, so the staging worker's capture
//! pass checks every table it captures, pauses the definitions that read
//! one, and leaves its capture alone.
//!
//! Nothing polls for convergence (#297): every pass is stepped by hand.

use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::TransformStatus;
use trellis::defs::hierarchy::Hierarchy;
use trellis::intake::markers;
use trellis::staging::apply::ApplyError;
use trellis::{CatalogError, Config, Trellis, TrellisError, TrellisOptions};

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

struct Instance {
    db: TestDatabase,
    raw: Client,
    trellis: Trellis,
}

/// A database with `public.p` (3 rows) and `public.c` (6 rows, `pid`
/// referencing `p`), a relationship `parent FROM c.pid TO p.id`, and the
/// given definitions, live.
async fn instance(cluster: &TestCluster, definitions: &[&str]) -> Instance {
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    raw.batch_execute(
        "create table public.p (id int primary key, name text); \
         create table public.c (id int primary key, pid int, amount int); \
         insert into public.p select i, 'n' || i from generate_series(1, 3) i; \
         insert into public.c select i, 1 + i % 3, i from generate_series(1, 6) i;",
    )
    .await
    .expect("the application's tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");
    for definition in definitions {
        trellis.apply(definition).await.expect("define");
    }
    capture_pass(&mut raw, &db.pool).await;
    markers::settle_registrations(&db.pool).await;
    Instance { db, raw, trellis }
}

/// The capture half of one staging-worker pass.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        raw,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn status(trellis: &Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

/// The names of the triggers on `table`.
async fn triggers(raw: &Client, table: &str) -> Vec<String> {
    raw.query(
        "select tgname::text from pg_catalog.pg_trigger \
         where tgrelid = pg_catalog.to_regclass($1) order by 1",
        &[&table],
    )
    .await
    .expect("read triggers")
    .into_iter()
    .map(|row| row.get(0))
    .collect()
}

/// Asserts `target` is paused for a capture failure on `table` whose
/// sentence starts with `starts`, and names the resume.
async fn assert_paused(trellis: &Trellis, target: &str, table: &str, starts: &str) {
    let reported = status(trellis, target).await;
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target}'s reason is reported"));
    assert_eq!(failure.source_table, table, "{failure:?}");
    assert!(failure.columns.is_empty(), "{failure:?}");
    assert!(
        failure.error.starts_with(starts) && failure.error.contains("resume"),
        "{failure:?}"
    );
}

async fn assert_live(trellis: &Trellis, target: &str) {
    let reported = status(trellis, target).await;
    assert_eq!(reported.status, TransformStatus::Live, "{target}");
    assert_eq!(reported.capture_failure, None, "{target}");
}

/// A source attached as a partition after define pauses its 1-1 and its
/// aggregate readers on the next pass, and a later pass leaves them paused.
/// A definition that doesn't read it stays live. Once the table is detached,
/// resuming rebuilds each to `live`; a resume while it is still a partition
/// is refused.
#[tokio::test]
async fn attaching_a_source_as_a_partition_pauses_its_readers() {
    let cluster = TestCluster::start();
    let mut it = instance(
        &cluster,
        &[
            "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
            "TRANSFORM c_sums FROM public.c GROUP BY pid SELECT SUM(amount) AS total",
            "TRANSFORM p_copy FROM public.p SELECT name AS name",
        ],
    )
    .await;
    for target in ["c_copy", "c_sums", "p_copy"] {
        assert_live(&it.trellis, target).await;
    }

    it.raw
        .batch_execute(
            "create table public.c_all (id int not null, pid int, amount int) \
                 partition by range (id); \
             alter table public.c_all attach partition public.c \
                 for values from (minvalue) to (maxvalue)",
        )
        .await
        .expect("attach the source as a partition");
    capture_pass(&mut it.raw, &it.db.pool).await;
    for target in ["c_copy", "c_sums"] {
        assert_paused(
            &it.trellis,
            target,
            "public.c",
            "public.c became a partition of public.c_all",
        )
        .await;
    }
    assert_live(&it.trellis, "p_copy").await;
    capture_pass(&mut it.raw, &it.db.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Paused,
        "a later pass leaves it paused"
    );

    let err = it
        .trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("a resume while the source is still a partition is refused");
    assert!(
        matches!(
            &err,
            TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })
                if matches!(**reason, CatalogError::SourceNotChangeKeyed { .. })
        ),
        "{err:?}"
    );

    it.raw
        .batch_execute("alter table public.c_all detach partition public.c")
        .await
        .expect("detach the source");
    for target in ["c_copy", "c_sums"] {
        it.trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect("resume");
    }
    capture_pass(&mut it.raw, &it.db.pool).await;
    markers::settle_registrations(&it.db.pool).await;
    for target in ["c_copy", "c_sums", "p_copy"] {
        assert_live(&it.trellis, target).await;
    }
    let copied: i64 = it
        .raw
        .query_one("select count(*) from public.c_copy", &[])
        .await
        .expect("read the target")
        .get(0);
    assert_eq!(copied, 6, "the rebuild read every row");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A relationship's to-side that becomes an inheritance child pauses the
/// definition that reads through the relationship, and one that becomes an
/// inheritance parent pauses its own reader.
#[tokio::test]
async fn an_inheritance_child_or_parent_pauses_its_readers() {
    let cluster = TestCluster::start();
    let mut it = instance(
        &cluster,
        &[
            "TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name",
            "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
        ],
    )
    .await;
    it.raw
        .batch_execute(
            "create table public.p_base (id int, name text); \
             alter table public.p inherit public.p_base",
        )
        .await
        .expect("make the to-side an inheritance child");
    capture_pass(&mut it.raw, &it.db.pool).await;
    assert_paused(
        &it.trellis,
        "c_named",
        "public.p",
        "public.p inherits from public.p_base",
    )
    .await;
    assert_live(&it.trellis, "c_copy").await;

    it.raw
        .batch_execute("create table public.c_more (extra text) inherits (public.c)")
        .await
        .expect("make the source an inheritance parent");
    capture_pass(&mut it.raw, &it.db.pool).await;
    assert_paused(
        &it.trellis,
        "c_copy",
        "public.c",
        "public.c_more inherits from public.c",
    )
    .await;
    it.trellis.shutdown().await.expect("shutdown");
}

/// A source dropped and recreated under the same name in a hierarchy, as a
/// partitioned table, a partition or an inheritance child, has none of
/// Trellis's capture: the pass would install it from scratch. It pauses the
/// readers instead, and installs nothing on the table, on that pass or a
/// later one.
#[tokio::test]
async fn a_source_recreated_in_a_hierarchy_pauses_its_readers_and_gets_no_capture() {
    let cluster = TestCluster::start();
    let recreated = [
        (
            "create table public.c (id int primary key, pid int, amount int) \
                 partition by range (id); \
             create table public.c_1 partition of public.c \
                 for values from (minvalue) to (maxvalue)",
            "public.c is a partitioned table",
        ),
        (
            "create table public.c_all (id int primary key, pid int, amount int) \
                 partition by range (id); \
             create table public.c partition of public.c_all \
                 for values from (minvalue) to (maxvalue)",
            "public.c became a partition of public.c_all",
        ),
        (
            "create table public.c_base (id int, pid int, amount int); \
             create table public.c (id int primary key) inherits (public.c_base)",
            "public.c inherits from public.c_base",
        ),
    ];
    for (ddl, reason) in recreated {
        let mut it = instance(
            &cluster,
            &["TRANSFORM c_sums FROM public.c GROUP BY pid SELECT SUM(amount) AS total"],
        )
        .await;
        assert_live(&it.trellis, "c_sums").await;
        it.raw
            .batch_execute(&format!("drop table public.c; {ddl}"))
            .await
            .expect("recreate the source in a hierarchy");
        capture_pass(&mut it.raw, &it.db.pool).await;
        assert_paused(&it.trellis, "c_sums", "public.c", reason).await;
        capture_pass(&mut it.raw, &it.db.pool).await;
        assert_eq!(
            triggers(&it.raw, "public.c").await,
            Vec::<String>::new(),
            "{reason}: no capture is installed on it"
        );
        it.trellis.shutdown().await.expect("shutdown");
    }
}

/// The false-positive gate: hierarchy membership is a catalog fact that only
/// DDL naming the hierarchy changes. Routine maintenance that rewrites the
/// heap or its indexes, and Trellis's own widen, narrow and repair of the
/// table's capture, each followed by a pass, pause nothing.
#[tokio::test]
async fn routine_maintenance_and_trellis_capture_ddl_pause_nothing() {
    let cluster = TestCluster::start();
    let mut it = instance(
        &cluster,
        &[
            "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
            "TRANSFORM c_sums FROM public.c GROUP BY pid SELECT SUM(amount) AS total",
        ],
    )
    .await;
    // Each on its own: `VACUUM` and `REINDEX CONCURRENTLY` refuse a
    // transaction block.
    for statement in [
        "vacuum full public.c",
        "cluster public.c using c_pkey",
        "reindex table public.c",
        "reindex table concurrently public.c",
        "vacuum (freeze) pg_catalog.pg_class",
        "vacuum (freeze) pg_catalog.pg_inherits",
        "analyze public.c",
        "update public.c set amount = amount + 1",
    ] {
        it.raw.batch_execute(statement).await.expect(statement);
        capture_pass(&mut it.raw, &it.db.pool).await;
        assert_unpaused(&it, statement).await;
    }

    // Trellis's own capture DDL on the table: a widen for a definition that
    // reads a new column, the narrow when it's dropped, and the repair of a
    // disabled trigger.
    it.trellis
        .apply("TRANSFORM c_pids FROM public.c SELECT pid AS pid")
        .await
        .expect("define a reader of a new column");
    capture_pass(&mut it.raw, &it.db.pool).await;
    markers::settle_registrations(&it.db.pool).await;
    assert_unpaused(&it, "widen").await;
    for statement in ["PAUSE TRANSFORM c_pids", "DROP TRANSFORM c_pids"] {
        it.trellis.apply(statement).await.expect(statement);
    }
    capture_pass(&mut it.raw, &it.db.pool).await;
    assert_unpaused(&it, "narrow").await;
    it.raw
        .batch_execute("alter table public.c disable trigger all")
        .await
        .expect("disable capture");
    capture_pass(&mut it.raw, &it.db.pool).await;
    assert_unpaused(&it, "repair").await;
    it.trellis.shutdown().await.expect("shutdown");
}

/// Asserts the check finds nothing for `public.c` and neither of its readers
/// is paused or has a `capture_failure`, after `step`.
async fn assert_unpaused(it: &Instance, step: &str) {
    assert_eq!(
        trellis::defs::hierarchy::hierarchy(&it.raw, "public.c")
            .await
            .expect("the check"),
        vec![],
        "{step}"
    );
    for target in ["c_copy", "c_sums"] {
        let reported = status(&it.trellis, target).await;
        assert_ne!(reported.status, TransformStatus::Paused, "{step}: {target}");
        assert_eq!(reported.capture_failure, None, "{step}: {target}");
    }
}

/// The shared check reads every way a table is in a hierarchy: a
/// partitioned table once (not once per partition), a sub-partitioned
/// partition both ways, and nothing for a plain table or one that doesn't
/// exist.
#[tokio::test]
async fn the_check_reports_each_way_a_table_is_in_a_hierarchy() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.plain (id int primary key); \
         create table public.top (id int not null) partition by range (id); \
         create table public.mid partition of public.top \
             for values from (0) to (100) partition by range (id); \
         create table public.leaf partition of public.mid for values from (0) to (10); \
         create table public.other partition of public.top for values from (100) to (200); \
         create table public.base (id int); \
         create table public.kid_b () inherits (public.base); \
         create table public.kid_a () inherits (public.base);",
    )
    .await
    .expect("the hierarchies");
    let check = |table: &'static str| {
        let raw = &raw;
        async move {
            trellis::defs::hierarchy::hierarchy(raw, table)
                .await
                .expect("the check")
        }
    };
    assert_eq!(check("public.plain").await, vec![]);
    assert_eq!(check("public.missing").await, vec![]);
    assert_eq!(
        check("public.top").await,
        vec![Hierarchy::Partitioned {
            table: "public.top".to_string()
        }]
    );
    assert_eq!(
        check("public.mid").await,
        vec![
            Hierarchy::Partitioned {
                table: "public.mid".to_string()
            },
            Hierarchy::Partition {
                table: "public.mid".to_string(),
                parent: "public.top".to_string()
            },
        ]
    );
    assert_eq!(
        check("public.leaf").await,
        vec![Hierarchy::Partition {
            table: "public.leaf".to_string(),
            parent: "public.mid".to_string()
        }]
    );
    assert_eq!(
        check("public.base").await,
        vec![
            Hierarchy::InheritanceParent {
                table: "public.base".to_string(),
                child: "public.kid_a".to_string()
            },
            Hierarchy::InheritanceParent {
                table: "public.base".to_string(),
                child: "public.kid_b".to_string()
            },
        ]
    );
    assert_eq!(
        check("public.kid_a").await,
        vec![Hierarchy::InheritanceChild {
            table: "public.kid_a".to_string(),
            parent: "public.base".to_string()
        }]
    );
}
