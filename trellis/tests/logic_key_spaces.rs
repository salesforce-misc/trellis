//! Comparison and logic (`=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT]
//! DISTINCT FROM`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, searched `CASE`,
//! `NULLIF`) in every key space: a 1-1 field (a boolean among them, read by
//! other fields), an aggregate argument and a composed aggregate field, a
//! `JOIN` transform's one-side argument and composed field, and `ALTER
//! TRANSFORM ... ADD`.
//!
//! Postgres computes every value (ADR-0002), so each test runs a real pipeline
//! over a workload that inserts, updates, moves and deletes rows (with `NULL`
//! on either side of a comparison and a `NaN`), and compares the target with
//! the same query written by hand (ADR-0013): a symmetric difference of zero
//! rows. Each test then runs `self_check` in strict mode, which renders every
//! field with its own audit renderer, independent of the write path.

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

/// Every comparison and logic form in a 1-1 field: each family under each
/// operator, three-valued logic, `IS NULL`, `CASE`, `NULLIF`, and boolean
/// fields read by other fields.
#[tokio::test]
async fn a_one_to_one_field_computes_every_comparison_and_logic_form() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    // Existing rows go through the build, the rest through apply.
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(
        db.dsn(),
        &["TRANSFORM t FROM s SELECT \
             a2 = a8 AS e1, a4 <> a8 AS e2, a4 != a2 AS e3, a2 < a4 AS e4, a4 <= a8 AS e5, \
             a8 > a4 AS e6, a8 >= a4 AS e7, a2 IS DISTINCT FROM a4 AS e8, \
             a2 IS NOT DISTINCT FROM a4 AS e9, n = a4 AS e10, n < a8 AS e11, \
             rl = rl AS f1, rl < f AS f2, f >= rl AS f3, f IS DISTINCT FROM rl AS f4, \
             f = DOUBLE PRECISION 'NaN' AS f5, f > 1 AS f6, 0.5 <= rl AS f7, f <> 2.5 AS f8, \
             t = 'abc' AS t1, t <> t AS t2, t IS DISTINCT FROM 'abd' AS t3, \
             t IS NOT DISTINCT FROM 'abc' AS t4, \
             d = date '2024-02-29' AS d1, d < date '2024-03-01' AS d2, ts >= ts AS d3, \
             tz > tz AS d4, tm <= time '12:00:00' AS d5, d IS DISTINCT FROM d AS d6, \
             b = c AS b1, b < c AS b2, b IS DISTINCT FROM c AS b3, u = u AS u1, \
             u >= u AS u2, \
             b AND c AS l1, b OR c AS l2, NOT b AS l3, NOT b AND c AS l4, \
             NOT (b OR c) AS l5, b AND c OR NOT b AS l6, \
             a2 IS NULL AS n1, t IS NOT NULL AS n2, (a2 = a4) IS NULL AS n3, \
             a2 = a4 IS NOT NULL AS n4, NOT a2 IS NULL AS n5, b IS NULL AS n6, \
             CASE WHEN a4 > 0 THEN 'pos' WHEN a4 < 0 THEN 'neg' ELSE 'zero' END AS c1, \
             CASE WHEN b THEN a4 END AS c2, CASE WHEN c THEN a2 ELSE a8 END AS c3, \
             CASE WHEN b THEN 1 ELSE 2.5 END AS c4, CASE WHEN b THEN rl ELSE n END AS c5, \
             CASE WHEN a2 IS NULL THEN 0 ELSE a2 END AS c6, \
             NULLIF(a4, 100) AS q1, NULLIF(t, 'abc') AS q2, n / NULLIF(a4, 0) AS q3, \
             NULLIF(rl, f) AS q4, NULLIF(d, date '2024-02-29') AS q5, NULLIF(b, c) AS q6, \
             COALESCE(NULLIF(a2, 1), -1) AS q7, \
             a4 > 0 AS p1, p1 AND b AS p2, CASE WHEN p2 THEN a4 END AS p3, NOT p1 AS p4, \
             p2 IS NULL AS p5"],
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

    let columns = "id, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, f1, f2, f3, f4, f5, f6, f7, f8, \
                   t1, t2, t3, t4, d1, d2, d3, d4, d5, d6, b1, b2, b3, u1, u2, l1, l2, l3, l4, l5, l6, \
                   n1, n2, n3, n4, n5, n6, c1, c2, c3, c4, c5, c6, q1, q2, q3, q4, q5, q6, q7, \
                   p1, p2, p3, p4, p5";
    let oracle = "select id, a2 = a8, a4 <> a8, a4 <> a2, a2 < a4, a4 <= a8, a8 > a4, a8 >= a4, \
                  a2 is distinct from a4, a2 is not distinct from a4, n = a4, n < a8, \
                  rl = rl, rl < f, f >= rl, f is distinct from rl, f = 'NaN'::float8, f > 1, 0.5 <= rl, f <> 2.5, \
                  t = 'abc', t <> t, t is distinct from 'abd', t is not distinct from 'abc', \
                  d = date '2024-02-29', d < date '2024-03-01', ts >= ts, tz > tz, \
                  tm <= time '12:00:00', d is distinct from d, \
                  b = c, b < c, b is distinct from c, u = u, \
                  u >= u, \
                  b and c, b or c, not b, not b and c, not (b or c), b and c or not b, \
                  a2 is null, t is not null, (a2 = a4) is null, a2 = a4 is not null, \
                  not a2 is null, b is null, \
                  case when a4 > 0 then 'pos' when a4 < 0 then 'neg' else 'zero' end, \
                  case when b then a4 end, case when c then a2 else a8 end, \
                  case when b then 1 else 2.5 end, case when b then rl else n end, \
                  case when a2 is null then 0 else a2 end, \
                  nullif(a4, 100), nullif(t, 'abc'), n / nullif(a4, 0), nullif(rl, f), \
                  nullif(d, date '2024-02-29'), nullif(b, c), coalesce(nullif(a2, 1), -1), \
                  a4 > 0, (a4 > 0) and b, case when (a4 > 0) and b then a4 end, not (a4 > 0), \
                  ((a4 > 0) and b) is null from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    assert_eq!(
        count(&raw, "select count(*) from t").await,
        count(&raw, "select count(*) from s").await
    );
    // A boolean field is a boolean column in the target.
    let pg_type: String = raw
        .query_one(
            "select format_type(atttypid, atttypmod) from pg_attribute \
             where attrelid = 't'::regclass and attname = 'p2'",
            &[],
        )
        .await
        .expect("p2's type")
        .get(0);
    assert_eq!(pg_type, "boolean");
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// `CASE` inside an aggregate's argument, composed around aggregates, and
/// comparisons of aggregates.
#[tokio::test]
async fn an_aggregate_argument_and_composed_field_compute_logic() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    define(
        db.dsn(),
        &[
            "TRANSFORM g FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, \
              COUNT(CASE WHEN b THEN 1 END) AS cb, \
              SUM(CASE WHEN b THEN a4 ELSE 0 END) AS sb, \
              SUM(CASE WHEN a4 > 0 THEN a2 END) AS sp, \
              AVG(CASE WHEN c THEN a4 ELSE 0 END) AS ac, \
              COUNT(NULLIF(a2, 1)) AS cn, \
              SUM(CASE WHEN a4 > 0 AND NOT b THEN a8 ELSE -a8 END) AS cw, \
              COUNT(a2 IS NULL) AS ci, \
              SUM(a4) > COUNT(*) AS big, \
              COUNT(*) = 0 AS none, \
              CASE WHEN COUNT(*) > 1 THEN SUM(a4) END AS multi, \
              CASE WHEN SUM(a4) > 100 THEN 'high' ELSE 'low' END AS band, \
              SUM(a4) IS NULL AS nosum, \
              COALESCE(SUM(a4), 0) <> 0 AS nonzero, \
              NOT (COUNT(*) > 2) AS few, \
              NULLIF(COUNT(*), 1) AS notone, \
              CAST(SUM(CASE WHEN b THEN a4 ELSE 0 END) AS numeric) / NULLIF(COUNT(*), 0) AS mean",
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
    let columns = "g, cb, sb, sp, ac, cn, cw, ci, big, none, multi, band, nosum, nonzero, few, \
                   notone, mean";
    let oracle = "select g, count(case when b then 1 end), sum(case when b then a4 else 0 end), \
                  sum(case when a4 > 0 then a2 end), avg(case when c then a4 else 0 end), \
                  count(nullif(a2, 1)), sum(case when a4 > 0 and not b then a8 else -a8 end), \
                  count(a2 is null), sum(a4) > count(*), count(*) = 0, \
                  case when count(*) > 1 then sum(a4) end, \
                  case when sum(a4) > 100 then 'high' else 'low' end, sum(a4) is null, \
                  coalesce(sum(a4), 0) <> 0, not (count(*) > 2), nullif(count(*), 1), \
                  cast(sum(case when b then a4 else 0 end) as numeric) / nullif(count(*), 0) \
                  from s group by g";
    assert_eq!(differences(&raw, "g", columns, oracle).await, 0);
    audit_converges(&trellis, "g").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A `JOIN` transform's one-side arguments and composed fields.
#[tokio::test]
async fn a_join_computes_logic_in_arguments_and_composed_fields() {
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
           SUM(CASE WHEN fa.v > 0 THEN fa.v ELSE 0 END) AS pv, \
           COUNT(CASE WHEN fa.ok THEN 1 END) AS nok, \
           SUM(CASE WHEN db.y > db.w THEN db.y END) AS sy, \
           COUNT(NULLIF(db.w, 0)) AS cw, \
           SUM(CASE WHEN fa.v IS NULL OR NOT fa.ok THEN 1 ELSE 0 END) AS bad, \
           COUNT(*) > 2 AS wide, \
           SUM(fa.v) IS NULL AS empty, \
           CASE WHEN COUNT(*) > 2 THEN SUM(fa.v) ELSE 0 END AS capped, \
           NOT (SUM(db.y) > SUM(db.w)) AS ordered, \
           CAST(SUM(CASE WHEN fa.ok THEN fa.v ELSE 0 END) AS numeric) / NULLIF(COUNT(*), 0) AS mean",
        ],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "j").await;
    for sql in [
        "insert into public.fa values (1, 1, 10, 5, true), (2, 1, 10, -3, false), \
           (3, 2, 20, 8, true), (4, 2, 30, 1, null)",
        "insert into public.db values (1, 10, 7, 2), (2, 10, -4, 9), (3, 20, 100, 1), \
           (4, 30, 5, 0)",
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
    let columns = "g, pv, nok, sy, cw, bad, wide, empty, capped, ordered, mean";
    let oracle = "select fa.g, sum(case when fa.v > 0 then fa.v else 0 end), \
                  count(case when fa.ok then 1 end), \
                  sum(case when db.y > db.w then db.y end), count(nullif(db.w, 0)), \
                  sum(case when fa.v is null or not fa.ok then 1 else 0 end), \
                  count(*) > 2, sum(fa.v) is null, \
                  case when count(*) > 2 then sum(fa.v) else 0 end, \
                  not (sum(db.y) > sum(db.w)), \
                  cast(sum(case when fa.ok then fa.v else 0 end) as numeric) / nullif(count(*), 0) \
                  from public.fa join public.db on fa.k = db.k group by fa.g";
    assert_eq!(differences(&raw, "j", columns, oracle).await, 0);
    audit_converges(&trellis, "j").await;
    trellis.shutdown().await.expect("shutdown");
}

/// `ALTER TRANSFORM ... ADD` of comparison and logic fields backfills them
/// and keeps them current.
#[tokio::test]
async fn alter_transform_adds_comparison_and_logic_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(db.dsn(), &["TRANSFORM t FROM s SELECT a4 AS a4"]).await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "t").await;
    trellis
        .apply(
            "ALTER TRANSFORM t ADD a4 > 0 AS pos, ADD pos AND b AS both_ok, \
             ADD CASE WHEN both_ok THEN a4 ELSE -a4 END AS mag, ADD NULLIF(t, 'abc') AS tn, \
             ADD a2 IS DISTINCT FROM a4 AS dist, ADD NOT both_ok IS NULL AS known",
        )
        .await
        .expect("add the fields");
    wait_live::wait_for_live(&trellis, "t").await;
    for sql in &WORKLOAD[1..] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "id, a4, pos, both_ok, mag, tn, dist, known";
    let oracle = "select id, a4, a4 > 0, (a4 > 0) and b, \
                  case when (a4 > 0) and b then a4 else -a4 end, nullif(t, 'abc'), \
                  a2 is distinct from a4, not ((a4 > 0) and b) is null from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// `SUM(CASE WHEN c THEN amount END)` over `numeric(10,2)` is refused for its
/// undeclared scale, and says why; the integer forms define.
#[tokio::test]
async fn a_case_over_a_scaled_numeric_is_refused_in_a_sum_with_the_reason() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table o (id bigint primary key, g integer, ok boolean, qty integer, \
           amount numeric(10,2))",
    )
    .await
    .expect("source");
    let definer = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    for formula in [
        "SUM(CASE WHEN ok THEN amount END)",
        "SUM(CASE WHEN ok THEN amount ELSE 0 END)",
        "AVG(CASE WHEN ok THEN amount END)",
    ] {
        let err = definer
            .apply(&format!(
                "TRANSFORM bad FROM o GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, {formula} AS v"
            ))
            .await
            .expect_err(formula)
            .to_string();
        assert!(
            err.contains("no declared scale") && err.contains("mix scales"),
            "{formula}: {err}"
        );
    }
    for (n, formula) in [
        "COUNT(CASE WHEN ok THEN 1 END)",
        "SUM(CASE WHEN ok THEN qty ELSE 0 END)",
        "COUNT(CASE WHEN ok THEN amount END)",
    ]
    .iter()
    .enumerate()
    {
        definer
            .apply(&format!(
                "TRANSFORM good{n} FROM o GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, {formula} AS v"
            ))
            .await
            .unwrap_or_else(|e| panic!("{formula}: {e}"));
    }
    definer.shutdown().await.expect("shutdown");
}
