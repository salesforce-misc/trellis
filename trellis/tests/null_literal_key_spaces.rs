//! The `TRUE`, `FALSE` and `NULL` literals in every key space: a 1-1 field
//! (a field that is only `NULL`, read by other fields, among them), an
//! aggregate argument and a composed aggregate field, a `JOIN` transform's
//! one-side argument and composed field, and `ALTER TRANSFORM ... ADD`.
//!
//! Postgres computes every value (ADR-0002), so each test runs a real pipeline
//! over a workload that inserts, updates and deletes rows (with `NULL` in the
//! data too), and compares the target with the same query written by hand
//! (ADR-0013): a symmetric difference of zero rows. Each column's type is
//! compared with the type `CREATE TABLE AS` gives the hand-written query,
//! which is the type define must infer for a `NULL` from its context
//! (ADR-0022). Each test then runs `self_check` in strict mode, which renders
//! every field with its own audit renderer.

#[path = "support/wait_live.rs"]
mod wait_live;

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::self_check::{CheckPageExt, SelfCheckScope};
use trellis::{Config, SelfCheckMode, SelfCheckOutcome, Trellis, TrellisOptions};

async fn connect_raw(dsn: &str) -> Client {
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

async fn define(dsn: &str, statements: &[&str]) {
    let definer = Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect definer");
    for statement in statements {
        definer
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    definer.shutdown().await.expect("shut the definer down");
}

async fn run(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect the running trellis")
}

async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("the pipeline converges");
}

/// Rows in the target that the oracle does not have, plus rows the oracle has
/// that the target does not. `columns` selects the same list from both_ok.
async fn differences(raw: &Client, target: &str, columns: &str, oracle: &str) -> i64 {
    raw.query_one(
        &format!(
            "with expected as ({oracle}), actual as (select {columns} from {target}) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)"
        ),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("difference query for {target}: {e:?}"))
    .get(0)
}

async fn audit_converges(trellis: &Trellis, target: &str) {
    let page = trellis
        .self_check_page(
            target,
            SelfCheckScope {
                after: None,
                limit: 1000,
            },
            SelfCheckMode::Strict,
            Duration::from_secs(60),
        )
        .await
        .unwrap_or_else(|e| panic!("self_check {target}: {e}"));
    assert!(
        matches!(page.outcome, SelfCheckOutcome::Converged),
        "self_check {target}: {:?}",
        page.outcome
    );
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get(0)
}

/// One column of each comparable family, with two of each exact width so a
/// mixed-width comparison is drawn, and `NULL`s and a `NaN` in the data.
const SOURCE: &str = "create table s ( \
     id bigint primary key, a2 smallint, a4 integer, a8 bigint, n numeric, \
     rl real, f double precision, t text, d date, ts timestamp, tz timestamptz, \
     tm time, b boolean, c boolean, u uuid, g integer)";

const WORKLOAD: &[&str] = &[
    "insert into s values \
       (1, 1, 100, 5000000000, 12.345, 1.5, 2.5, 'abc', date '2024-02-29', \
          timestamp '2024-02-29 12:00:00', timestamptz '2024-02-29 12:00:00+00', \
          time '12:00:00', true, false, '00000000-0000-0000-0000-000000000001', 1), \
       (2, 1, -7, -3, -0.5, 'NaN', 'NaN', 'abd', date '2024-03-01', \
          timestamp '2024-02-29 12:00:00', timestamptz '2024-03-01 00:00:00+00', \
          time '12:00:00', false, true, '00000000-0000-0000-0000-000000000002', 1), \
       (3, 2, 21474830, 7, 99.995, 0.25, 0.25, 'abc', date '2024-02-29', \
          timestamp '2023-01-01 00:00:00', timestamptz '2024-02-29 12:00:00+00', \
          time '00:00:00', null, true, '00000000-0000-0000-0000-000000000001', 2), \
       (4, 2, 0, 0, 0, 0, 0, '', date '1999-12-31', null, null, null, true, null, null, 2), \
       (5, 3, 31, 9, 1234567.891, 1e10, 1e10, 'ABC', date '2024-02-29', \
          timestamp '2024-02-29 12:00:00', timestamptz '2024-02-29 12:00:00+00', \
          time '12:00:01', false, false, '00000000-0000-0000-0000-000000000003', 3), \
       (6, null, 5, 1, null, null, null, null, null, null, null, null, null, null, null, 3)",
    "update s set a4 = a4 - 50, n = n * 2 where id = 1",
    "update s set a2 = 2, t = 'abd' where id = 5",
    "delete from s where id = 3",
    "insert into s values (7, 1, -32768, 11, 5.5, -0.0, -0.0, 'abc', date '2024-02-29', \
       timestamp '2024-02-29 12:00:00', timestamptz '2024-02-29 12:00:00+00', time '12:00:00', \
       true, true, '00000000-0000-0000-0000-000000000001', 1)",
    "update s set a2 = null, b = null where id = 2",
    "delete from s where id = 4",
];

/// Each column of `target` named in `columns` has the type `CREATE TABLE AS`
/// gives the same column of `oracle`.
async fn assert_types_match(raw: &Client, target: &str, columns: &str, oracle: &str) {
    raw.batch_execute(&format!(
        "drop table if exists __oracle_types; create temp table __oracle_types as {oracle} limit 0"
    ))
    .await
    .unwrap_or_else(|e| panic!("oracle types for {target}: {e:?}"));
    let names: Vec<&str> = columns.split(',').map(str::trim).collect();
    let rows = raw
        .query(
            &format!(
                "select a.attname::text, format_type(a.atttypid, a.atttypmod), \
                        format_type(o.atttypid, o.atttypmod) \
                 from pg_attribute a \
                 join pg_attribute o on o.attrelid = '__oracle_types'::regclass \
                   and o.attnum = array_position($1::text[], a.attname::text) \
                 where a.attrelid = '{target}'::regclass and a.attname = any($1::text[])"
            ),
            &[&names],
        )
        .await
        .unwrap_or_else(|e| panic!("column types of {target}: {e:?}"));
    assert_eq!(
        rows.len(),
        names.len(),
        "every column of {target} is compared"
    );
    for row in rows {
        let (name, actual, expected): (String, String, String) =
            (row.get(0), row.get(1), row.get(2));
        assert_eq!(actual, expected, "{target}.{name}'s type");
    }
}

/// Every form in a 1-1 field: the literals bare, `NULL` beside each kind of
/// operand, three-valued logic, `NULL` among `CASE` results and `COALESCE` and
/// `NULLIF` arguments, `CAST(NULL AS ...)`, and fields read by other fields.
#[tokio::test]
async fn a_one_to_one_field_computes_every_literal_form() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    // Existing rows go through the build, the rest through apply.
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(
        db.dsn(),
        &["TRANSFORM t FROM s SELECT \
             TRUE AS k1, FALSE AS k2, NULL AS k3, b = TRUE AS k4, \
             b IS DISTINCT FROM FALSE AS k5, NOT FALSE AND b AS k6, \
             a4 = NULL AS e1, NULL <> t AS e2, a2 IS NOT DISTINCT FROM NULL AS e3, \
             NULL IS DISTINCT FROM d AS e4, a4 + NULL AS e5, NULL * n AS e6, f - NULL AS e7, \
             d - NULL AS e8, a2 % NULL AS e9, NULL = NULL AS e10, \
             b AND NULL AS l1, NULL OR b AS l2, NULL AND FALSE AS l3, NULL OR TRUE AS l4, \
             NOT NULL AS l5, NULL IS NULL AS l6, NULL IS NOT NULL AS l7, \
             COALESCE(a4, NULL) AS c1, COALESCE(NULL, t, 'none') AS c2, \
             COALESCE(NULL, NULL) AS c3, CASE WHEN b THEN NULL ELSE a8 END AS c4, \
             CASE WHEN NULL THEN 1 ELSE 2 END AS c5, CASE WHEN b THEN NULL END AS c6, \
             NULLIF(a4, NULL) AS c7, NULLIF(NULL, n) AS c8, NULLIF(NULL, NULL) AS c9, \
             CASE WHEN c THEN rl WHEN b THEN NULL ELSE f END AS c10, \
             CAST(NULL AS integer) AS x1, CAST(NULL AS date) AS x2, CAST(NULL AS boolean) AS x3, \
             abs(CAST(NULL AS integer)) AS x4, CAST(NULL AS double precision) AS x5, \
             k3 AS r1, COALESCE(k3, 'none') AS r2, a4 + k3 AS r3, k1 AND b AS r4, \
             CASE WHEN k4 THEN k3 ELSE t END AS r5"],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "t").await;
    for sql in &WORKLOAD[1..] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;

    let columns = "id, k1, k2, k3, k4, k5, k6, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, \
                   l1, l2, l3, l4, l5, l6, l7, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, \
                   x1, x2, x3, x4, x5, r1, r2, r3, r4, r5";
    let oracle = "select id, true as k1, false as k2, null as k3, b = true as k4, \
                  b is distinct from false as k5, not false and b as k6, \
                  a4 = null as e1, null <> t as e2, a2 is not distinct from null as e3, \
                  null is distinct from d as e4, a4 + null as e5, null * n as e6, f - null as e7, \
                  d - null as e8, a2 % null as e9, null = null as e10, \
                  b and null as l1, null or b as l2, null and false as l3, null or true as l4, \
                  not null as l5, null is null as l6, null is not null as l7, \
                  coalesce(a4, null) as c1, coalesce(null, t, 'none') as c2, \
                  coalesce(null, null) as c3, case when b then null else a8 end as c4, \
                  case when null then 1 else 2 end as c5, case when b then null end as c6, \
                  nullif(a4, null) as c7, nullif(null, n) as c8, nullif(null, null) as c9, \
                  case when c then rl when b then null else f end as c10, \
                  cast(null as integer) as x1, cast(null as date) as x2, \
                  cast(null as boolean) as x3, abs(cast(null as integer)) as x4, \
                  cast(null as double precision) as x5, \
                  null as r1, coalesce(null, 'none') as r2, a4 + null as r3, true and b as r4, \
                  case when b = true then null else t end as r5 from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    assert_eq!(
        count(&raw, "select count(*) from t").await,
        count(&raw, "select count(*) from s").await
    );
    assert_types_match(&raw, "t", columns, oracle).await;
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// The literals inside an aggregate's argument, composed around aggregates,
/// and as a field of their own.
#[tokio::test]
async fn an_aggregate_argument_and_composed_field_compute_the_literals() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    define(
        db.dsn(),
        &[
            "TRANSFORM g FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, \
              SUM(CASE WHEN b THEN a4 ELSE NULL END) AS s1, \
              COUNT(CASE WHEN b = TRUE THEN TRUE END) AS c1, \
              COUNT(COALESCE(b, TRUE)) AS b1, \
              COUNT(c AND NULL) AS b2, \
              SUM(COALESCE(a2, NULL)) AS s2, \
              AVG(NULLIF(a4, NULL)) AS m1, \
              SUM(a8 + NULL) AS s3, \
              CASE WHEN COUNT(*) > 1 THEN SUM(a4) ELSE NULL END AS multi, \
              SUM(a4) = NULL AS eqnull, \
              SUM(a4) IS NOT DISTINCT FROM NULL AS nosum, \
              COALESCE(SUM(a8), NULL, 0) AS total, \
              COUNT(*) > 0 AND TRUE AS anyrows, \
              TRUE AS yes, NULL AS nothing, CAST(NULL AS bigint) AS nb, \
              COALESCE(nothing, SUM(a4)) AS ny, SUM(COALESCE(a8, nothing)) AS sn, \
              CASE WHEN nothing THEN 1 ELSE SUM(a2) END AS cn",
        ],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "g").await;
    for sql in WORKLOAD {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "g, s1, c1, b1, b2, s2, m1, s3, multi, eqnull, nosum, total, anyrows, yes, \
                   nothing, nb, ny, sn, cn";
    let oracle = "select g, sum(case when b then a4 else null end) as s1, \
                  count(case when b = true then true end) as c1, \
                  count(coalesce(b, true)) as b1, count(c and null) as b2, \
                  sum(coalesce(a2, null)) as s2, avg(nullif(a4, null)) as m1, \
                  sum(a8 + null) as s3, \
                  case when count(*) > 1 then sum(a4) else null end as multi, \
                  sum(a4) = null as eqnull, sum(a4) is not distinct from null as nosum, \
                  coalesce(sum(a8), null, 0) as total, count(*) > 0 and true as anyrows, \
                  true as yes, null as nothing, cast(null as bigint) as nb, \
                  coalesce(null, sum(a4)) as ny, sum(coalesce(a8, null)) as sn, \
                  case when null then 1 else sum(a2) end as cn \
                  from s group by g";
    assert_eq!(differences(&raw, "g", columns, oracle).await, 0);
    assert_types_match(&raw, "g", columns, oracle).await;
    audit_converges(&trellis, "g").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A `JOIN` transform's one-side arguments and composed fields.
#[tokio::test]
async fn a_join_computes_the_literals_in_arguments_and_composed_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.fa (id integer primary key, g integer, k integer, v integer, \
           ok boolean); \
         create table public.db (id integer primary key, k integer, y integer, w integer); \
         create index on public.db (k)",
    )
    .await
    .expect("tables");
    define(
        db.dsn(),
        &[
            "TRANSFORM j FROM public.fa JOIN public.db ON fa.k = db.k GROUP BY fa.g \
           WITHOUT SOURCE INDEX SELECT fa.g AS g, \
           SUM(CASE WHEN fa.ok = TRUE THEN fa.v ELSE NULL END) AS pv, \
           COUNT(NULLIF(db.w, NULL)) AS cw, \
           SUM(db.y + NULL) AS nothing, \
           COUNT(fa.ok OR NULL) AS anyok, \
           CASE WHEN COUNT(*) > 2 THEN SUM(fa.v) ELSE NULL END AS capped, \
           SUM(fa.v) IS NOT DISTINCT FROM NULL AS empty, \
           COALESCE(SUM(db.w), NULL) AS sw, \
           NOT (SUM(db.y) > SUM(db.w)) OR FALSE AS ordered",
        ],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "j").await;
    for sql in [
        "insert into public.fa values (1, 1, 10, 5, true), (2, 1, 10, -3, false), \
           (3, 2, 20, 8, true), (4, 2, 30, 1, null)",
        "insert into public.db values (1, 10, 7, 2), (2, 10, -4, 9), (3, 20, 100, 1), \
           (4, 30, 5, null)",
        "update public.fa set v = v * 3 where id = 2",
        "update public.db set y = y - 1 where id = 3",
        "insert into public.db values (5, 20, 2, 0)",
        "delete from public.fa where id = 1",
        "update public.fa set k = 30, ok = true where id = 3",
        "delete from public.db where id = 4",
    ] {
        raw.execute(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "g, pv, cw, nothing, anyok, capped, empty, sw, ordered";
    let oracle = "select fa.g as g, sum(case when fa.ok = true then fa.v else null end) as pv, \
                  count(nullif(db.w, null)) as cw, sum(db.y + null) as nothing, \
                  count(fa.ok or null) as anyok, \
                  case when count(*) > 2 then sum(fa.v) else null end as capped, \
                  sum(fa.v) is not distinct from null as empty, \
                  coalesce(sum(db.w), null) as sw, \
                  not (sum(db.y) > sum(db.w)) or false as ordered \
                  from public.fa join public.db on fa.k = db.k group by fa.g";
    assert_eq!(differences(&raw, "j", columns, oracle).await, 0);
    assert_types_match(&raw, "j", columns, oracle).await;
    audit_converges(&trellis, "j").await;
    trellis.shutdown().await.expect("shutdown");
}

/// `ALTER TRANSFORM ... ADD` of literal fields backfills them and keeps them
/// current, and a definition holding the literals is re-read from its stored
/// text (the ALTER, and the restart that follows it).
#[tokio::test]
async fn alter_transform_adds_literal_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(
        db.dsn(),
        &["TRANSFORM t FROM s SELECT a4 AS a4, COALESCE(t, NULL) AS tn, TRUE AS yes"],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "t").await;
    trellis
        .apply(
            "ALTER TRANSFORM t ADD NULL AS z, ADD a4 = NULL AS eq, ADD yes AND b AS both_ok, \
             ADD CASE WHEN b THEN NULL ELSE a4 END AS pick, ADD COALESCE(z, t) AS zt, \
             ADD CAST(NULL AS bigint) AS nb",
        )
        .await
        .expect("add the fields");
    wait_live::wait_for_live(&trellis, "t").await;
    for sql in &WORKLOAD[1..3] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    trellis.shutdown().await.expect("shutdown");
    // A fresh process reads the stored definition back, and a field added
    // now reads `z`, a `text` column, as the literal it is.
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "t").await;
    trellis
        .apply("ALTER TRANSFORM t ADD COALESCE(z, a4) AS za, ADD z + a2 AS z2")
        .await
        .expect("add fields reading z");
    wait_live::wait_for_live(&trellis, "t").await;
    for sql in &WORKLOAD[3..] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "id, a4, tn, yes, z, eq, both_ok, pick, zt, nb, za, z2";
    let oracle = "select id, a4, coalesce(t, null) as tn, true as yes, null as z, \
                  a4 = null as eq, true and b as both_ok, \
                  case when b then null else a4 end as pick, coalesce(null, t) as zt, \
                  cast(null as bigint) as nb, coalesce(null, a4) as za, null + a2 as z2 \
                  from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    assert_types_match(&raw, "t", columns, oracle).await;
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A `NULL` define can't type is refused, naming the `CAST` remedy, and the
/// remedy defines.
#[tokio::test]
async fn an_untypable_null_is_refused_at_define_with_the_remedy() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    let definer = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    for formula in [
        "NULL + NULL",
        "-NULL",
        "abs(NULL)",
        "d + NULL",
        "NULL AT TIME ZONE 'UTC'",
    ] {
        let err = definer
            .apply(&format!("TRANSFORM bad FROM s SELECT {formula} AS v"))
            .await
            .expect_err(formula)
            .to_string();
        assert!(err.contains("CAST(NULL AS <type>)"), "{formula}: {err}");
    }
    let err = definer
        .apply("TRANSFORM bad FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(NULL) AS v")
        .await
        .expect_err("SUM(NULL)")
        .to_string();
    assert!(err.contains("CAST(NULL AS <type>)"), "SUM(NULL): {err}");
    definer
        .apply("TRANSFORM good FROM s SELECT abs(CAST(NULL AS integer)) AS v")
        .await
        .expect("the remedy defines");
    definer.shutdown().await.expect("shutdown");
}
