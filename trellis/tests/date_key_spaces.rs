//! Dates (`date ± integer`, `date - date`, `DATE_TRUNC`, `EXTRACT`, `AT TIME
//! ZONE 'UTC'`, `CAST(date AS timestamp)`) in every key space: a 1-1 field, an
//! aggregate argument and a composed aggregate field, a `JOIN` transform's
//! one-side argument and composed field, `ALTER TRANSFORM ... ADD`, and a 1-1
//! date bucket feeding an aggregate's `GROUP BY` through a chain.
//!
//! Postgres computes every value (ADR-0002), so each test runs a real pipeline
//! over a workload that inserts, updates, moves and deletes rows (with `NULL`s,
//! leap days, year boundaries and an `infinity` date), and compares the target
//! with the same query written by hand (ADR-0013): a symmetric difference of
//! zero rows. Each test then runs `self_check` in strict mode, which renders
//! every field with its own audit renderer, independent of the write path.

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

/// Dates around the leap day, a year boundary and ISO week 53; `di` holds an
/// `infinity` where the arithmetic that rejects an infinite date is not
/// applied to it.
const SOURCE: &str = "create table s ( \
     id bigint primary key, d date, d2 date, di date, ts timestamp, tz timestamptz, \
     n integer, g integer)";

const WORKLOAD: &[&str] = &[
    "insert into s values \
       (1, date '2024-02-29', date '2024-01-01', date '2024-02-29', \
          timestamp '2024-02-29 12:34:56.789', timestamptz '2024-02-29 23:30:00-05', 5, 1), \
       (2, date '2020-12-31', date '2021-01-01', 'infinity', \
          timestamp '2021-01-03 12:00:00', timestamptz '2024-03-01 00:30:00+05', -7, 1), \
       (3, date '2021-01-01', date '2020-12-31', '-infinity', \
          timestamp '1970-01-01 00:00:00.5', timestamptz '2020-12-31 23:59:59+00', 21, 2), \
       (4, date '0001-01-01', date '9999-12-31', null, null, null, 0, 2), \
       (5, date '2023-12-31', date '2024-12-31', date '2023-12-31', \
          timestamp '2023-12-31 23:59:59', timestamptz '2023-12-31 23:59:59+00', 31, 3), \
       (6, null, date '2024-02-29', null, null, null, null, 3)",
    "update s set d = d + 1, n = n * 2 where id = 1",
    "update s set d2 = date '2024-03-01', ts = timestamp '2024-03-03 03:03:03' where id = 5",
    "delete from s where id = 3",
    "insert into s values (7, date '2024-02-28', date '2024-02-28', date '2024-02-28', \
       timestamp '2024-02-28 00:00:00', timestamptz '2024-02-28 00:00:00+00', 1, 1)",
    "update s set d = null, ts = null where id = 2",
    "delete from s where id = 4",
];

/// Every date form in a 1-1 field, and fields read by other fields.
#[tokio::test]
async fn a_one_to_one_field_computes_every_date_form() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    // Existing rows go through the build, the rest through apply.
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(
        db.dsn(),
        &["TRANSFORM t FROM s SELECT \
             d + 1 AS p1, 1 + d AS p2, d - 7 AS p3, d - d2 AS p4, di + 30 AS p5, d + n AS p6, \
             (d - d2) + 1 AS p7, \
             CAST(d AS timestamp) AS c1, DATE_TRUNC('month', CAST(d AS timestamp)) AS c2, \
             DATE_TRUNC('week', ts) AS w1, DATE_TRUNC('year', ts) AS w2, \
             DATE_TRUNC('milliseconds', ts) AS w3, date_trunc('QUARTER', ts) AS w4, \
             EXTRACT(year FROM d) AS e1, EXTRACT(week FROM d) AS e2, \
             EXTRACT(isodow FROM d) AS e3, EXTRACT(epoch FROM d) AS e4, \
             EXTRACT(isoyear FROM d) AS e5, EXTRACT(hour FROM ts) AS e6, \
             EXTRACT(second FROM ts) AS e7, EXTRACT(microseconds FROM ts) AS e8, \
             EXTRACT(year FROM di) AS e9, EXTRACT(day FROM di) AS e10, \
             extract(century from d) AS e11, EXTRACT(epoch FROM ts) AS e12, \
             tz AT TIME ZONE 'UTC' AS u1, DATE_TRUNC('day', tz AT TIME ZONE 'UTC') AS u2, \
             EXTRACT(epoch FROM tz AT TIME ZONE 'UTC') - EXTRACT(epoch FROM ts) AS dur, \
             EXTRACT(hour FROM tz AT TIME ZONE 'UTC') AS u3, \
             CASE WHEN d - d2 > 30 THEN d ELSE d2 END AS k1, (d + 1) > d2 AS k2, \
             d IS NULL AS k3, NULLIF(d, d2) AS k4, COALESCE(d, DATE '1970-01-01') + 1 AS k5, \
             p1 - d AS f1, DATE_TRUNC('day', c1) = c1 AS f2"],
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

    let columns = "id, p1, p2, p3, p4, p5, p6, p7, c1, c2, w1, w2, w3, w4, e1, e2, e3, e4, e5, e6, \
                   e7, e8, e9, e10, e11, e12, u1, u2, dur, u3, k1, k2, k3, k4, k5, f1, f2";
    let oracle = "select id, d + 1, 1 + d, d - 7, d - d2, di + 30, d + n, (d - d2) + 1, \
                  d::timestamp, date_trunc('month', d::timestamp), date_trunc('week', ts), \
                  date_trunc('year', ts), date_trunc('milliseconds', ts), \
                  date_trunc('quarter', ts), \
                  extract(year from d), extract(week from d), extract(isodow from d), \
                  extract(epoch from d), extract(isoyear from d), extract(hour from ts), \
                  extract(second from ts), extract(microseconds from ts), \
                  extract(year from di), extract(day from di), extract(century from d), \
                  extract(epoch from ts), \
                  tz at time zone 'UTC', date_trunc('day', tz at time zone 'UTC'), \
                  extract(epoch from tz at time zone 'UTC') - extract(epoch from ts), \
                  extract(hour from tz at time zone 'UTC'), \
                  case when d - d2 > 30 then d else d2 end, (d + 1) > d2, d is null, \
                  nullif(d, d2), coalesce(d, date '1970-01-01') + 1, \
                  (d + 1) - d, date_trunc('day', d::timestamp) = d::timestamp from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    assert_eq!(
        count(&raw, "select count(*) from t").await,
        count(&raw, "select count(*) from s").await
    );
    // The target columns have the types Postgres gives the same expressions.
    for (column, expected) in [
        ("p1", "date"),
        ("p4", "integer"),
        ("c1", "timestamp without time zone"),
        ("w1", "timestamp without time zone"),
        ("e1", "numeric"),
        ("u1", "timestamp without time zone"),
        ("dur", "numeric"),
        ("f1", "integer"),
        ("k2", "boolean"),
    ] {
        let found: String = raw
            .query_one(
                "select format_type(atttypid, atttypmod) from pg_attribute \
                 where attrelid = 't'::regclass and attname = $1",
                &[&column],
            )
            .await
            .expect("column type")
            .get(0);
        assert_eq!(found, expected, "column {column}");
    }
    // `infinity` follows Postgres: it stays infinite through arithmetic, and
    // `EXTRACT` gives `Infinity` for a monotonic field and `NULL` for a cyclic one.
    assert_eq!(
        count(
            &raw,
            "select count(*) from t where id = 2 and p5 = 'infinity' and e9 = 'Infinity' \
               and e10 is null"
        )
        .await,
        1
    );
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// Date forms inside an aggregate's argument, composed around aggregates.
#[tokio::test]
async fn an_aggregate_argument_and_composed_field_compute_dates() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    define(
        db.dsn(),
        &[
            "TRANSFORM g FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, \
              SUM(d - d2) AS days, AVG(d - d2) AS avg_days, SUM((d + 1) - d2) AS shifted, \
              COUNT(d + n) AS c1, COUNT(EXTRACT(year FROM d)) AS c2, \
              COUNT(DATE_TRUNC('day', ts)) AS c3, COUNT(tz AT TIME ZONE 'UTC') AS c4, \
              COUNT(CAST(d AS timestamp)) AS c5, \
              SUM(CASE WHEN d - d2 > 0 THEN d - d2 ELSE 0 END) AS late, \
              CAST(SUM(d - d2) AS numeric) / NULLIF(COUNT(*), 0) AS per_row, \
              SUM(d - d2) > 100 AS wide, COUNT(*) = COUNT(d) AS dense, \
              CASE WHEN SUM(d - d2) < 0 THEN 'early' ELSE 'late' END AS side",
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
    let columns =
        "g, days, avg_days, shifted, c1, c2, c3, c4, c5, late, per_row, wide, dense, side";
    let oracle = "select g, sum(d - d2), avg(d - d2), sum((d + 1) - d2), count(d + n), \
                  count(extract(year from d)), count(date_trunc('day', ts)), \
                  count(tz at time zone 'UTC'), count(d::timestamp), \
                  sum(case when d - d2 > 0 then d - d2 else 0 end), \
                  cast(sum(d - d2) as numeric) / nullif(count(*), 0), sum(d - d2) > 100, count(*) = count(d), \
                  case when sum(d - d2) < 0 then 'early' else 'late' end \
                  from s group by g";
    assert_eq!(differences(&raw, "g", columns, oracle).await, 0);
    audit_converges(&trellis, "g").await;
    trellis.shutdown().await.expect("shutdown");
}

/// An `EXTRACT` is an unconstrained `numeric`, so summing it is refused for
/// its undeclared scale; counting it, and summing an integer date difference,
/// define.
#[tokio::test]
async fn summing_an_extract_is_refused_for_its_scale_and_counting_it_defines() {
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
    for formula in ["SUM(EXTRACT(year FROM d))", "AVG(EXTRACT(dow FROM d))"] {
        let err = definer
            .apply(&format!(
                "TRANSFORM bad FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, {formula} AS v"
            ))
            .await
            .expect_err(formula)
            .to_string();
        assert!(err.contains("no declared scale"), "{formula}: {err}");
    }
    definer
        .apply(
            "TRANSFORM ok FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, \
             COUNT(EXTRACT(year FROM d)) AS v, SUM(d - d2) AS w",
        )
        .await
        .expect("counting an EXTRACT and summing a date difference define");
    definer.shutdown().await.expect("shutdown");
}

/// A `JOIN` transform's one-side arguments and composed fields; an argument
/// mixing the sides stays refused.
#[tokio::test]
async fn a_join_computes_dates_in_arguments_and_composed_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.fa (id integer primary key, g integer, k integer, \
           due date, paid date, at timestamptz); \
         create table public.db (id integer primary key, k integer, opened date, closed date, \
           ts timestamp); \
         create index on public.db (k)",
    )
    .await
    .expect("tables");
    define(
        db.dsn(),
        &[
            "TRANSFORM j FROM public.fa JOIN public.db ON fa.k = db.k GROUP BY fa.g \
           WITHOUT SOURCE INDEX SELECT fa.g AS g, \
           SUM(fa.paid - fa.due) AS late, AVG(fa.paid - fa.due) AS avg_late, \
           SUM(db.closed - db.opened) AS open_days, \
           COUNT(EXTRACT(year FROM db.opened)) AS c1, COUNT(DATE_TRUNC('day', db.ts)) AS c2, \
           COUNT(fa.at AT TIME ZONE 'UTC') AS c3, COUNT(CAST(fa.due AS timestamp)) AS c4, \
           SUM(CASE WHEN fa.paid > fa.due + 3 THEN 1 ELSE 0 END) AS strict, \
           CAST(SUM(fa.paid - fa.due) AS numeric) / NULLIF(COUNT(*), 0) AS per_row, \
           SUM(db.closed - db.opened) > 10 AS long",
        ],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "j").await;
    for sql in [
        "insert into public.fa values \
           (1, 1, 10, date '2024-02-01', date '2024-02-29', timestamptz '2024-02-29 12:00:00+00'), \
           (2, 1, 10, date '2024-02-01', null, null), \
           (3, 2, 20, date '2023-12-31', date '2024-01-01', timestamptz '2024-01-01 00:00:00-05'), \
           (4, 2, 30, date '2024-03-01', date '2024-03-02', null)",
        "insert into public.db values \
           (1, 10, date '2024-01-01', date '2024-01-20', timestamp '2024-01-01 10:00:00'), \
           (2, 10, date '2023-12-31', date '2024-01-02', null), \
           (3, 20, date '2024-02-29', date '2024-03-05', timestamp '2024-02-29 23:59:59'), \
           (4, 30, date '2024-01-01', date '2024-01-01', null)",
        "update public.fa set paid = date '2024-03-10' where id = 2",
        "update public.db set closed = closed + 5 where id = 3",
        "insert into public.db values (5, 20, date '2020-02-29', date '2020-03-01', null)",
        "delete from public.fa where id = 1",
        "update public.fa set k = 30 where id = 3",
        "delete from public.db where id = 4",
    ] {
        raw.execute(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let columns = "g, late, avg_late, open_days, c1, c2, c3, c4, strict, per_row, long";
    let oracle = "select fa.g, sum(fa.paid - fa.due), avg(fa.paid - fa.due), \
                  sum(db.closed - db.opened), count(extract(year from db.opened)), \
                  count(date_trunc('day', db.ts)), count(fa.at at time zone 'UTC'), \
                  count(fa.due::timestamp), \
                  sum(case when fa.paid > fa.due + 3 then 1 else 0 end), \
                  cast(sum(fa.paid - fa.due) as numeric) / nullif(count(*), 0), sum(db.closed - db.opened) > 10 \
                  from public.fa join public.db on fa.k = db.k group by fa.g";
    assert_eq!(differences(&raw, "j", columns, oracle).await, 0);
    audit_converges(&trellis, "j").await;
    // An argument that mixes the two tables stays refused.
    let err = trellis
        .apply(
            "TRANSFORM mixed FROM public.fa JOIN public.db ON fa.k = db.k GROUP BY fa.g \
             WITHOUT SOURCE INDEX SELECT fa.g AS g, SUM(db.closed - fa.due) AS v",
        )
        .await
        .expect_err("a mixed argument")
        .to_string();
    assert!(err.to_lowercase().contains("both"), "{err}");
    trellis.shutdown().await.expect("shutdown");
}

/// `ALTER TRANSFORM ... ADD` of date fields backfills them and keeps them
/// current.
#[tokio::test]
async fn alter_transform_adds_date_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(db.dsn(), &["TRANSFORM t FROM s SELECT n AS n"]).await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "t").await;
    trellis
        .apply(
            "ALTER TRANSFORM t ADD d + 1 AS nd, ADD d - d2 AS gap, \
             ADD DATE_TRUNC('month', CAST(d AS timestamp)) AS mo, \
             ADD EXTRACT(year FROM d) AS yr, ADD tz AT TIME ZONE 'UTC' AS utc, \
             ADD CASE WHEN gap > 0 THEN nd END AS later",
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
    let columns = "id, n, nd, gap, mo, yr, utc, later";
    let oracle = "select id, n, d + 1, d - d2, date_trunc('month', d::timestamp), \
                  extract(year from d), tz at time zone 'UTC', \
                  case when d - d2 > 0 then d + 1 end from s";
    assert_eq!(differences(&raw, "t", columns, oracle).await, 0);
    audit_converges(&trellis, "t").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A 1-1 date bucket feeding an aggregate's `GROUP BY` through a chain: the
/// bucket is a `timestamp` (or an `EXTRACT`ed `numeric`) key of the second
/// transform.
#[tokio::test]
async fn a_one_to_one_date_bucket_feeds_an_aggregates_group_by() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    define(
        db.dsn(),
        &[
            "TRANSFORM b FROM s SELECT DATE_TRUNC('month', ts) AS month, \
             EXTRACT(year FROM d) AS yr, n AS n, d - d2 AS gap",
        ],
    )
    .await;
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "b").await;
    for statement in [
        "TRANSFORM by_month FROM b GROUP BY month WITHOUT SOURCE INDEX SELECT month AS month, \
           COUNT(*) AS c, SUM(n) AS total, SUM(gap) AS gaps",
        "TRANSFORM by_year FROM b GROUP BY yr WITHOUT SOURCE INDEX SELECT yr AS yr, \
           COUNT(*) AS c, SUM(n) AS total",
    ] {
        trellis
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    wait_live::wait_for_live(&trellis, "by_month").await;
    wait_live::wait_for_live(&trellis, "by_year").await;
    for sql in &WORKLOAD[1..] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;
    let oracle = "select date_trunc('month', ts) as month, count(*) as c, sum(n) as total, \
                  sum(d - d2) as gaps from s group by 1";
    assert_eq!(
        differences(&raw, "by_month", "month, c, total, gaps", oracle).await,
        0
    );
    let oracle = "select extract(year from d) as yr, count(*) as c, sum(n) as total \
                  from s group by 1";
    assert_eq!(
        differences(&raw, "by_year", "yr, c, total", oracle).await,
        0
    );
    for target in ["b", "by_month", "by_year"] {
        audit_converges(&trellis, target).await;
    }
    trellis.shutdown().await.expect("shutdown");
}

/// `AT TIME ZONE 'UTC'` parses its zone name under `timezone_abbreviations`
/// (an abbreviation set that redefined `UTC` would shift every value), so a
/// Trellis connection pins it to the shipped `Default`, whatever the
/// database's own default.
#[tokio::test]
async fn a_databases_timezone_abbreviations_do_not_reach_a_trellis_session() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let name: String = raw
        .query_one("select current_database()", &[])
        .await
        .expect("database name")
        .get(0);
    raw.batch_execute(&format!(
        "alter database \"{name}\" set timezone_abbreviations to 'Australia'"
    ))
    .await
    .expect("set the database default");
    // A fresh, unpinned session sees the database default.
    let fresh = connect_raw(db.dsn()).await;
    let seen: String = fresh
        .query_one("select current_setting('timezone_abbreviations')", &[])
        .await
        .expect("setting")
        .get(0);
    assert_eq!(seen, "Australia");
    // Trellis's own sessions do not.
    let pooled = db.pool.get().await.expect("a pooled connection");
    let seen: String = pooled
        .query_one("select current_setting('timezone_abbreviations')", &[])
        .await
        .expect("setting")
        .get(0);
    assert_eq!(seen, "Default");
    let seen: String = pooled
        .query_one(
            "select (timestamptz '2024-02-29 12:00:00+00' at time zone 'UTC')::text",
            &[],
        )
        .await
        .expect("value")
        .get(0);
    assert_eq!(seen, "2024-02-29 12:00:00");
}
