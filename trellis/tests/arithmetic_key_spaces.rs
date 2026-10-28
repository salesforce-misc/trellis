//! Arithmetic (`-`, `*`, `/`, `%`, prefix `-`, `ABS`, `CEIL`, `FLOOR`,
//! `ROUND`, `TRUNC`, the widening `CAST`) in every key space: a 1-1 field, an
//! aggregate argument and a composed aggregate field, a `JOIN` transform's
//! one-side argument and composed field, and `ALTER TRANSFORM ... ADD`.
//!
//! Postgres computes every value (ADR-0002), so each test runs a real
//! pipeline over a workload that inserts, updates, moves and deletes rows,
//! and compares the target with the same query written by hand
//! (ADR-0013): a symmetric difference of zero rows.

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
/// that the target does not. `columns` selects the same list from both.
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
    .unwrap_or_else(|e| panic!("difference query for {target}: {e}"))
    .get(0)
}

/// `self_check` renders each field with its own audit renderer (ADR-0013),
/// independent of the write path, and must agree with what apply wrote.
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

const SOURCE: &str = "create table s ( \
     id bigint primary key, a2 smallint, a4 integer, a8 bigint, n numeric, f double precision)";

const WORKLOAD: &[&str] = &[
    "insert into s (id, a2, a4, a8, n, f) values \
       (1, 1, 100, 5000000000, 12.345, 2.5), (2, 1, -7, -3, -0.5, -1.5), \
       (3, 2, 21474830, 7, 99.995, 0.25), (4, 2, 0, 0, 0, 0), \
       (5, 3, 31, 9, 1234567.891, 1e10), (6, null, 5, 1, null, null)",
    "update s set a4 = a4 - 50, n = n * 2 where id = 1",
    "update s set a2 = 2 where id = 5",
    "delete from s where id = 3",
    "insert into s (id, a2, a4, a8, n, f) values (7, 1, -32768, 11, 5.5, 3)",
    "update s set a2 = null where id = 2",
    "delete from s where id = 4",
];

/// `-`, `*`, `/`, `%`, prefix `-`, `ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNC` and
/// the widening `CAST` in a 1-1 field, applied incrementally.
#[tokio::test]
async fn a_one_to_one_field_computes_every_arithmetic_form() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    // Existing rows go through the build, the rest through apply.
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(
        db.dsn(),
        &["TRANSFORM t FROM s SELECT a4 - a2 AS d, a4 * a2 AS p, a4 % 7 AS m, -a4 AS neg, ABS(a4 - 100) AS ab, \
              n / 4 AS q, n - a2 * 3 AS e, CEIL(n) AS c, FLOOR(n) AS fl, ROUND(n) AS r0, \
              ROUND(n, 1) AS r1, ROUND(n, -1) AS rm, TRUNC(n) AS t0, TRUNC(n, 2) AS t2, \
              f / 4 AS fq, -f AS fneg, ABS(f) AS fab, CEIL(f) AS fc, TRUNC(f) AS ft, \
              CAST(a2 AS bigint) * 3000000000 AS wide, CAST(a4 AS numeric) / 3 AS third, \
              -2147483648 AS lowest, a8 % -1 AS mm, - -a2 AS twice"],
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

    let columns = "id, d, p, m, neg, ab, q, e, c, fl, r0, r1, rm, t0, t2, fq, fneg, fab, fc, \
                   ft, wide, third, lowest, mm, twice";
    let oracle = "select id, a4 - a2, a4 * a2, a4 % 7, -a4, abs(a4 - 100), n / 4, n - a2 * 3, \
                  ceil(n), floor(n), round(n), round(n, 1), round(n, -1), trunc(n), \
                  trunc(n, 2), f / 4, -f, abs(f), ceil(f), trunc(f), \
                  cast(a2 as bigint) * 3000000000, cast(a4 as numeric) / 3, \
                  -2147483648, a8 % -1, - -a2 from s";
    // Rows the arithmetic cannot compute for (an overflow) are quarantined,
    // not written; the workload has none, so every source row is a target row.
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    assert_eq!(
        count(&raw, "select count(*) from t").await,
        count(&raw, "select count(*) from s").await
    );
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// Arithmetic inside an aggregate's argument, and composed around aggregates.
#[tokio::test]
async fn an_aggregate_argument_and_composed_field_compute_arithmetic() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    define(
        db.dsn(),
        &[
            "TRANSFORM g FROM s GROUP BY a2 WITHOUT SOURCE INDEX SELECT a2 AS a2, \
              SUM(a4 * 2 - a2) AS s1, AVG(-a4) AS av, SUM(a4 % 5) AS md, SUM(ABS(a4)) AS ab, \
              SUM(CAST(a4 AS bigint) * 4) AS wide, \
              SUM(a4) - COUNT(*) AS c1, -SUM(a2) AS c2, COALESCE(SUM(a4), 0) * 2 AS c3, \
              ROUND(AVG(a4), 2) AS c4, CAST(SUM(a4) AS numeric) / COUNT(*) AS c5, \
              ABS(SUM(a4 - 100)) AS c6",
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
    let columns = "a2, s1, av, md, ab, wide, c1, c2, c3, c4, c5, c6";
    let oracle = "select a2, sum(a4 * 2 - a2), avg(-a4), sum(a4 % 5), sum(abs(a4)), \
                  sum(cast(a4 as bigint) * 4), sum(a4) - count(*), -sum(a2), \
                  coalesce(sum(a4), 0) * 2, round(avg(a4), 2), \
                  cast(sum(a4) as numeric) / count(*), abs(sum(a4 - 100)) \
                  from s group by a2";
    assert_eq!(differences(&raw, "g", columns, oracle).await, 0);
    audit_converges(&trellis, "g").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A `JOIN` transform's one-side arguments and composed fields.
#[tokio::test]
async fn a_join_computes_arithmetic_in_arguments_and_composed_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.fa (id integer primary key, g integer, k integer, v integer); \
         create table public.db (id integer primary key, k integer, y integer, w integer); \
         create index on public.db (k)",
    )
    .await
    .expect("tables");
    define(
        db.dsn(),
        &[
            "TRANSFORM j FROM public.fa JOIN public.db ON fa.k = db.k GROUP BY fa.g \
           WITHOUT SOURCE INDEX SELECT fa.g AS g, SUM(fa.v * 2 - 1) AS sv, \
           SUM(db.y - db.w) AS sy, AVG(-db.y) AS ay, SUM(ABS(db.w)) AS aw, \
           -COUNT(*) AS nn, SUM(fa.v) * 2 - COUNT(*) AS c1, \
           ROUND(AVG(db.y), 1) AS c2, CAST(SUM(fa.v) AS numeric) / COUNT(*) AS c3, \
           SUM(CAST(db.w AS bigint) * 3) AS cw, ROUND(AVG(fa.v * 10), -1) AS c4, \
           COUNT(ROUND(CAST(db.y AS numeric) / 3, 1)) AS mr",
        ],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "j").await;
    for sql in [
        "insert into public.fa values (1, 1, 10, 5), (2, 1, 10, -3), (3, 2, 20, 8), (4, 2, 30, 1)",
        "insert into public.db values (1, 10, 7, 2), (2, 10, -4, 9), (3, 20, 100, 1), (4, 30, 5, -5)",
        "update public.fa set v = v * 3 where id = 2",
        "update public.db set y = y - 1 where id = 3",
        "insert into public.db values (5, 20, 2, 2)",
        "delete from public.fa where id = 1",
        "update public.fa set k = 30 where id = 3",
        "delete from public.db where id = 4",
    ] {
        raw.execute(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "g, sv, sy, ay, aw, nn, c1, c2, c3, cw, c4, mr";
    let oracle = "select fa.g, sum(fa.v * 2 - 1), sum(db.y - db.w), avg(-db.y), \
                  sum(abs(db.w)), -count(*), sum(fa.v) * 2 - count(*), round(avg(db.y), 1), \
                  cast(sum(fa.v) as numeric) / count(*), sum(cast(db.w as bigint) * 3), \
                  round(avg(fa.v * 10), -1), count(round(cast(db.y as numeric) / 3, 1)) \
                  from public.fa join public.db on fa.k = db.k group by fa.g";
    assert_eq!(differences(&raw, "j", columns, oracle).await, 0);
    audit_converges(&trellis, "j").await;
    trellis.shutdown().await.expect("shutdown");
}

/// `ALTER TRANSFORM ... ADD` of an arithmetic field backfills it and keeps it
/// current.
#[tokio::test]
async fn alter_transform_adds_an_arithmetic_field() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(db.dsn(), &["TRANSFORM t FROM s SELECT a4 AS a4"]).await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "t").await;
    trellis
        .apply("ALTER TRANSFORM t ADD a4 * 2 - a2 AS x, ADD -n / 3 AS y, ADD ROUND(n, 1) AS z, ADD CAST(a2 AS bigint) AS w")
        .await
        .expect("add the fields");
    wait_live::wait_for_live(&trellis, "t").await;
    for sql in &WORKLOAD[1..] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "id, a4, x, y, z, w";
    let oracle = "select id, a4, a4 * 2 - a2, -n / 3, round(n, 1), cast(a2 as bigint) from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}
