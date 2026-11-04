//! A `CAST` to the operand's own type (#1642): `smallint`/`integer`/`bigint`
//! to the same width, `numeric` to `numeric` and `timestamp` to `timestamp`.
//!
//! Against a scratch Postgres (ADR-0022): each admitted pair defines, and the
//! target holds the same value with the same text as the source column,
//! including a `numeric(12,2)`'s scale, a `timestamp(3)`'s fractional digits
//! and each integer width's bounds. The generative conformance harness draws
//! these casts too, since it takes its pairs from `cast_result_type`.

#[path = "support/wait_live.rs"]
mod wait_live;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::{Config, Trellis, TrellisOptions};

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

/// The identity casts, each over the source column of its own type, beside
/// the `numeric(12,2)` and `timestamp(3)` columns whose modifiers the cast
/// target does not carry.
const IDENTITY_FIELDS: &str = "\
    CAST(i2 AS integer) AS i2_as_i4, \
    CAST(i4 AS integer) AS i4_as_i4, \
    CAST(i4 AS int4) AS i4_as_int4, \
    CAST(i8 AS bigint) AS i8_as_i8, \
    CAST(i8 AS int8) AS i8_as_int8, \
    CAST(n AS numeric) AS n_as_n, \
    CAST(n AS decimal) AS n_as_decimal, \
    CAST(i8 AS numeric) AS i8_as_n, \
    CAST(ts AS timestamp) AS ts_as_ts";

/// `(target column, source column)` of each field above, to compare text.
const TEXT_PAIRS: &[(&str, &str)] = &[
    ("i2_as_i4", "i2"),
    ("i4_as_i4", "i4"),
    ("i4_as_int4", "i4"),
    ("i8_as_i8", "i8"),
    ("i8_as_int8", "i8"),
    ("n_as_n", "n"),
    ("n_as_decimal", "n"),
    ("i8_as_n", "i8"),
    ("ts_as_ts", "ts"),
];

#[tokio::test(flavor = "multi_thread")]
async fn a_cast_to_the_operands_own_type_keeps_the_value_and_its_text() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table s (id bigint primary key, i2 smallint, i4 integer, i8 bigint, \
             n numeric(12,2), ts timestamp(3)); \
         insert into s values \
           (1, 0, 0, 0, 1.50, '2024-02-29 12:34:56.700'), \
           (2, -32768, -2147483648, -9223372036854775808, -9999999999.99, \
              '4713-01-01 00:00:00.001 BC'), \
           (3, 32767, 2147483647, 9223372036854775807, 9999999999.99, \
              '294276-12-31 23:59:59.999'), \
           (4, 1, 1, 1, 0.10, '2024-01-01 00:00:00'), \
           (5, 2, 2, 2, 1.00, 'infinity'), \
           (6, 3, 3, 3, 12.30, '-infinity');",
    )
    .await
    .expect("source");

    // Postgres itself: each pair runs, and leaves the text as it was.
    for (_, source) in TEXT_PAIRS {
        let target = match *source {
            "i2" | "i4" => "integer",
            "i8" => "bigint",
            "n" => "numeric",
            _ => "timestamp",
        };
        let changed: i64 = raw
            .query_one(
                &format!(
                    "select count(*) from s where cast({source} as {target})::text \
                     is distinct from {source}::text"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("cast({source} as {target}) runs: {e}"))
            .get(0);
        assert_eq!(changed, 0, "cast({source} as {target}) changed the text");
    }

    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect the running trellis");
    trellis
        .apply(&format!("TRANSFORM t FROM s SELECT {IDENTITY_FIELDS}"))
        .await
        .expect("an identity cast defines");
    wait_live::wait_for_live(&trellis, "t").await;

    for (field, source) in TEXT_PAIRS {
        let mismatched: i64 = raw
            .query_one(
                &format!(
                    "select count(*) from {DEFAULT_TARGET_SCHEMA}.t t join s using (id) \
                     where t.{field}::text is distinct from s.{source}::text"
                ),
                &[],
            )
            .await
            .expect("compare")
            .get(0);
        assert_eq!(mismatched, 0, "{field} differs from {source}");
    }
    // The scale and the fractional digits survive, literally.
    let row = raw
        .query_one(
            &format!(
                "select n_as_n::text, ts_as_ts::text, pg_typeof(i2_as_i4)::text, \
                 pg_typeof(i4_as_i4)::text, pg_typeof(i8_as_i8)::text, \
                 pg_typeof(n_as_n)::text, pg_typeof(ts_as_ts)::text \
                 from {DEFAULT_TARGET_SCHEMA}.t where id = 1"
            ),
            &[],
        )
        .await
        .expect("read the target");
    assert_eq!(row.get::<_, String>(0), "1.50");
    assert_eq!(row.get::<_, String>(1), "2024-02-29 12:34:56.7");
    let types: Vec<String> = (2..7).map(|i| row.get(i)).collect();
    assert_eq!(
        types,
        [
            "integer",
            "integer",
            "bigint",
            "numeric",
            "timestamp without time zone"
        ]
    );
}

/// The reason for the rule: `SUM(CAST(qty AS bigint))` over a `bigint`
/// column defines, as it did while `qty` was an `integer`.
#[tokio::test(flavor = "multi_thread")]
async fn a_sum_over_a_cast_to_the_columns_own_type_defines() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table s (id bigint primary key, grp integer, qty bigint); \
         create index on s (grp); \
         insert into s values (1, 1, 9223372036854775800), (2, 1, 7), (3, 2, 5);",
    )
    .await
    .expect("source");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect the running trellis");
    trellis
        .apply(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, \
             SUM(CAST(qty AS bigint)) AS total",
        )
        .await
        .expect("an aggregate over an identity cast defines");
    wait_live::wait_for_live(&trellis, "t").await;
    let rows = raw
        .query(
            &format!("select grp, total::text from {DEFAULT_TARGET_SCHEMA}.t order by grp"),
            &[],
        )
        .await
        .expect("read the target");
    let totals: Vec<(i32, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(
        totals,
        [(1, "9223372036854775807".to_string()), (2, "5".to_string())]
    );
}
