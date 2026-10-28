//! `CAST` target aliases (#1455): `int` and `int4` for `integer`, `int8` for
//! `bigint`, `decimal` for `numeric`, accepted in the target position only.
//!
//! Against a real pipeline: the aliased cast is typed as its canonical
//! target by Postgres, columns named like the aliases still read as columns,
//! the stored definition text (what `ALTER TRANSFORM` re-renders and reads
//! back) spells the canonical names, and `decimal(p,s)` is refused like
//! `numeric(p,s)`.

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

async fn running(dsn: &str) -> Trellis {
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

async fn definition_text(raw: &Client) -> String {
    raw.query_one(
        "select definition_text from transform_definitions \
         where split_part(target_table, '.', 2) = 't'",
        &[],
    )
    .await
    .expect("read definition_text")
    .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn cast_aliases_are_typed_canonically_stored_canonically_and_leave_columns_alone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    // Columns named like the aliases: they stay columns everywhere else,
    // including as the operand of a cast to the same word.
    raw.batch_execute(
        "create table s (id bigint primary key, \"int\" smallint, int4 smallint, \
             int8 integer, \"decimal\" bigint); \
         insert into s values (1, 7, 8, 9, 10), (2, -7, -8, -9, -10);",
    )
    .await
    .expect("source");
    let trellis = running(db.dsn()).await;

    // `decimal(10, 2)` is refused exactly as `numeric(10, 2)` is.
    let refusal = |sql: &str| {
        let trellis = &trellis;
        let sql = sql.to_string();
        async move {
            trellis
                .apply(&sql)
                .await
                .expect_err("a modifier is refused")
                .to_string()
        }
    };
    let numeric = refusal("TRANSFORM bad FROM s SELECT CAST(id AS numeric(10, 2)) AS x").await;
    let decimal = refusal("TRANSFORM bad FROM s SELECT CAST(id AS decimal(10, 2)) AS x").await;
    assert_eq!(
        numeric.replace("'numeric(...)'", "'X'"),
        decimal.replace("'decimal(...)'", "'X'")
    );
    // Narrowing through an alias is refused like the canonical spelling.
    let alias = refusal("TRANSFORM bad FROM s SELECT CAST(int8 AS INT2) AS x").await;
    assert!(alias.contains("INT2"), "{alias}");
    let narrowing = refusal("TRANSFORM bad FROM s SELECT CAST(decimal AS Int) AS x").await;
    let canonical = refusal("TRANSFORM bad FROM s SELECT CAST(decimal AS integer) AS x").await;
    assert_eq!(narrowing, canonical);

    trellis
        .apply(
            "TRANSFORM t FROM s SELECT CAST(int4 AS INT) AS a, \
             CAST(int AS Int4) AS b, cast(int8 as int8) + 1 AS c, \
             CAST(decimal AS DECIMAL) / 4 AS d, int8 AS e, decimal AS f",
        )
        .await
        .expect("define with aliases");
    wait_live::wait_for_live(&trellis, "t").await;

    // Typed as the canonical targets, valued as Postgres computes them.
    let row = raw
        .query_one(
            &format!(
                "select pg_typeof(a)::text, pg_typeof(b)::text, pg_typeof(c)::text, \
                 pg_typeof(d)::text, a, b, c::text, d::text, e, f \
                 from {DEFAULT_TARGET_SCHEMA}.t where id = 1"
            ),
            &[],
        )
        .await
        .expect("read the target");
    let types: Vec<String> = (0..4).map(|i| row.get(i)).collect();
    assert_eq!(types, ["integer", "integer", "bigint", "numeric"]);
    assert_eq!((row.get::<_, i32>(4), row.get::<_, i32>(5)), (8, 7));
    assert_eq!(row.get::<_, String>(6), "10");
    assert_eq!(row.get::<_, String>(7), "2.5000000000000000");
    assert_eq!((row.get::<_, i32>(8), row.get::<_, i64>(9)), (9, 10));

    // Define stores the user's text unchanged.
    let stored = definition_text(&raw).await;
    assert!(
        stored.contains("CAST(int4 AS INT) AS a") && stored.contains("cast(int8 as int8) + 1"),
        "{stored}"
    );

    // `ALTER TRANSFORM` re-renders the definition, aliases included.
    trellis
        .apply("ALTER TRANSFORM t ADD CAST(int4 AS Int8) AS g, ADD CAST(int AS decimal) AS h")
        .await
        .expect("alter with aliases");
    wait_live::wait_for_live(&trellis, "t").await;
    let altered = definition_text(&raw).await.to_lowercase();
    for canonical in [
        "cast(int4 as integer)",
        "cast(int as integer)",
        "cast(int8 as bigint)",
        "cast(decimal as numeric)",
        "cast(int4 as bigint)",
        "cast(int as numeric)",
    ] {
        assert!(altered.contains(canonical), "`{canonical}` in {altered}");
    }
    for alias in [" as int)", " as int4)", " as int8)", " as decimal)"] {
        assert!(!altered.contains(alias), "`{alias}` in {altered}");
    }
    let row = raw
        .query_one(
            &format!(
                "select pg_typeof(g)::text, pg_typeof(h)::text \
                 from {DEFAULT_TARGET_SCHEMA}.t where id = 2"
            ),
            &[],
        )
        .await
        .expect("read the altered target");
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        ("bigint".to_string(), "numeric".to_string())
    );
    trellis.shutdown().await.expect("shutdown");
}
