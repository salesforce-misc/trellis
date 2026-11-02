//! A bare `FROM` (or `JOIN`) table resolves as it always has: the first of
//! Trellis's schema, the configured target schema and `public` that holds
//! it (ADR-0011), although no Trellis session has the target schema or
//! `public` on its `search_path` (#1477).

use testkit::TestCluster;
use tokio_postgres::NoTls;
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, Trellis, TrellisOptions};

#[tokio::test]
async fn a_bare_name_finds_the_target_schema_then_public() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    raw.batch_execute(
        "create schema tgt; \
         create table tgt.only_tgt (id integer primary key, v integer); \
         create table public.only_public (id integer primary key, k integer, v integer); \
         create table tgt.dup (id integer primary key, k integer, v integer); \
         create table public.dup (id integer primary key, k integer, v integer); \
         create index on tgt.dup (k); \
         create index on public.dup (k)",
    )
    .await
    .expect("tables");

    let config = Config::from_dsn(db.dsn().to_string())
        .expect("valid dsn")
        .with_target_schema("tgt")
        .expect("valid target schema");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");
    for statement in [
        "TRANSFORM from_tgt FROM only_tgt SELECT v AS v",
        "TRANSFORM from_public FROM only_public SELECT v AS v",
        "TRANSFORM from_dup FROM dup SELECT v AS v",
        "TRANSFORM joined FROM only_public JOIN dup ON only_public.k = dup.k \
         GROUP BY only_public.k WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
    ] {
        trellis
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }

    let rows = raw
        .query(
            &format!(
                "select target_table, source_table, coalesce(join_table, '') \
                 from {DEFAULT_SCHEMA}.transform_definitions order by target_table"
            ),
            &[],
        )
        .await
        .expect("read the catalog");
    let resolved: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        resolved,
        [
            ("tgt.from_dup", "tgt.dup", ""),
            ("tgt.from_public", "public.only_public", ""),
            ("tgt.from_tgt", "tgt.only_tgt", ""),
            ("tgt.joined", "public.only_public", "tgt.dup"),
        ]
        .map(|(t, s, j)| (t.to_string(), s.to_string(), j.to_string()))
    );
    trellis.shutdown().await.expect("shutdown");
}
