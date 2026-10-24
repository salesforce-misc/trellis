//! Issue #864 (ADR-0018): every stored definition carries the grammar version
//! its text was written under, and every read of stored text checks it.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::grammar::CURRENT_GRAMMAR_VERSION;
use trellis::defs::{CatalogError, ParseError, ValueType, install_definition};

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

fn cols(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

#[tokio::test]
async fn definitions_are_stamped_with_the_current_grammar() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols(&["a"]),
        "public",
    )
    .await
    .expect("define");

    let v: i32 = raw
        .query_one("select grammar_version from transform_definitions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, CURRENT_GRAMMAR_VERSION);
}

#[tokio::test]
async fn a_definition_with_a_newer_grammar_is_refused_and_left_untouched() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols(&["a"]),
        "public",
    )
    .await
    .expect("define");
    let newer = CURRENT_GRAMMAR_VERSION + 1;
    raw.execute(
        "update transform_definitions set grammar_version = $1",
        &[&newer],
    )
    .await
    .unwrap();
    let before = raw
        .query_one(
            "select definition_text, status, definition_version, grammar_version \
             from transform_definitions",
            &[],
        )
        .await
        .unwrap();

    match trellis::defs::catalog::definition_by_target(&db.pool, "t").await {
        Err(CatalogError::Parse(ParseError::GrammarVersionTooNew { stored, current })) => {
            assert_eq!((stored, current), (newer, CURRENT_GRAMMAR_VERSION));
        }
        other => panic!("expected GrammarVersionTooNew, got {other:?}"),
    }

    let after = raw
        .query_one(
            "select definition_text, status, definition_version, grammar_version \
             from transform_definitions",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(before.get::<_, String>(0), after.get::<_, String>(0));
    assert_eq!(before.get::<_, String>(1), after.get::<_, String>(1));
    assert_eq!(before.get::<_, i64>(2), after.get::<_, i64>(2));
    assert_eq!(after.get::<_, i32>(3), newer);
}

#[tokio::test]
async fn attaching_over_a_newer_definition_is_refused_naming_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols(&["a"]),
        "public",
    )
    .await
    .expect("define");
    raw.execute(
        "update transform_definitions set grammar_version = $1",
        &[&(CURRENT_GRAMMAR_VERSION + 1)],
    )
    .await
    .unwrap();

    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    let err = trellis::migrate(&db.pool, &config)
        .await
        .expect_err("attach must refuse");
    let msg = err.to_string();
    assert!(msg.contains("public.t") && msg.contains("grammar"), "{msg}");
}

#[tokio::test]
async fn attaching_pauses_a_definition_older_than_the_window_and_names_the_fix() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols(&["a"]),
        "public",
    )
    .await
    .expect("define");
    raw.execute("update transform_definitions set grammar_version = 0", &[])
        .await
        .unwrap();

    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    trellis::migrate(&db.pool, &config)
        .await
        .expect("an older definition pauses; it doesn't refuse the attach");

    let status: String = raw
        .query_one("select status from transform_definitions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "paused");
    let error: String = raw
        .query_one("select error from capture_failures", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        error.contains("grammar version 0")
            && error.contains("1 and later")
            && error.contains("drop the transform and define it again"),
        "{error}"
    );
    match trellis::defs::catalog::definition_by_target(&db.pool, "t").await {
        Err(CatalogError::Parse(ParseError::GrammarVersionTooOld {
            stored: 0,
            oldest: 1,
        })) => {}
        other => panic!("expected GrammarVersionTooOld, got {other:?}"),
    }
    trellis::dev::capture::load_catalog(&raw, DEFAULT_SCHEMA)
        .await
        .expect("capture keeps working beside the paused definition");
}
