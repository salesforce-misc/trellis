//! Integration tests for the `Trellis` facade's CDC source-table seeding
//! (issue #83 WI3), run against a real, ephemeral Postgres instance via the
//! shared harness (`testkit::TestCluster`).

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::app::qualified_source_tables;
use trellis::defs::{create_definition, create_relationship};
use trellis::{Config, Trellis, TrellisOptions};

/// A bare table with an integer primary key named `id`.
async fn create_table_with_pk(pool: &trellis::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!("create table {name} (id serial primary key)"))
        .await
        .expect("create table with pk");
}

/// A bare table with its own primary key plus a plain (non-unique) integer
/// `fk_col` column, suitable as a to-many relationship's to-side.
async fn create_table_with_fk_column(pool: &trellis::pool::Pool, name: &str, fk_col: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (id serial primary key, {fk_col} integer)"
        ))
        .await
        .expect("create table with fk column");
}

/// Issue #83 WI3: `qualified_source_tables` (the seed for
/// [`trellis::ClientOptions::source_tables`] at staging startup) must seed the
/// full transitive closure of source tables (`trellis::defs::all_source_tables`),
/// not just each definition's direct anchor. A definition anchored on
/// `authors` with a to-many relationship to `posts` (the shape a
/// `count(posts.id)`-style calculated field on `authors` reads through) must
/// seed both `authors` and `posts`, schema-qualified — otherwise a live write
/// to `posts` before the maintenance-reconcile loop catches up wouldn't be
/// captured by the CDC publication.
#[tokio::test]
async fn includes_relationship_to_tables_not_just_direct_anchors() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_pk(&db.pool, "authors").await;
    create_table_with_fk_column(&db.pool, "posts", "author").await;
    // To-many to-side prerequisite (#41): the join key must survive into
    // delete/re-parent pre-images.
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute("alter table posts replica identity full")
        .await
        .expect("set replica identity full");

    create_definition(
        &db.pool,
        "TRANSFORM authors_calc FROM authors SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author",
    )
    .await
    .expect("valid to-many relationship should be stored");

    let mut tables = qualified_source_tables(&db.pool)
        .await
        .expect("seeding query should succeed");
    tables.sort();
    assert_eq!(
        tables,
        vec!["trellis.authors".to_string(), "trellis.posts".to_string()]
    );
}

/// Issue #108 regression: [`Trellis::define`]'s own source-column
/// introspection (`Trellis::source_columns`, private) used to classify a
/// column's type by matching `information_schema.columns.data_type` text
/// against a small hardcoded list that didn't even include `"uuid"` — so a
/// `uuid` source column was silently *dropped* from the type map the
/// validator sees, and referencing it in a definition failed with an
/// unresolved-column error before type-checking ever ran. A `jsonb` column
/// fared no better: also absent from that list, also dropped.
///
/// Now that introspection classifies every column via the PG-OID registry
/// (`pg_type::value_type_for_oid`), both a bare `uuid` passthrough and a
/// bare `jsonb` passthrough must `define()` successfully through the real
/// public facade — and the `jsonb` column's target must land as genuine
/// `jsonb`, not `text`.
#[tokio::test]
async fn define_accepts_a_uuid_and_a_jsonb_passthrough_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute(
            "create table events (
                 id integer primary key,
                 tag uuid not null,
                 payload jsonb not null
             )",
        )
        .await
        .expect("seed source table");

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    trellis
        .define("TRANSFORM events_calc FROM events SELECT tag AS tag, payload AS payload")
        .await
        .expect(
            "a uuid/jsonb passthrough must define successfully — pre-#108 the uuid column \
             would have been silently dropped from source_columns and failed as an \
             unresolved column reference",
        );

    let client = db.pool.get().await.expect("connection");
    let columns: Vec<(String, String)> = client
        .query(
            "select column_name, data_type from information_schema.columns \
             where table_name = 'events_calc' order by ordinal_position",
            &[],
        )
        .await
        .expect("introspect target columns")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("tag".to_string(), "uuid".to_string()),
            ("payload".to_string(), "jsonb".to_string()),
        ],
        "the jsonb column must keep its native type, not collapse to text"
    );
}
