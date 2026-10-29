//! ADR-0018: every stored definition carries the grammar version
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
    trellis::dev::capture::load_catalog(&raw)
        .await
        .expect("capture keeps working beside the paused definition");
}

/// Stored text inside the window that this release refuses (written by an
/// earlier release that accepted it, here a bare reserved word, ADR-0022)
/// pauses that one transform at attach (`migrate`) with a `capture_failure` naming the
/// refusal and the fix. The other transforms keep running, `RESUME` refuses
/// with the same refusal, and `DROP TRANSFORM` clears it.
#[tokio::test]
async fn stored_text_this_release_refuses_pauses_that_transform_naming_the_fix() {
    use trellis::{TransformStatus, Trellis, TrellisOptions};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table public.rw_src (id integer primary key, val integer, \"order\" integer); \
         insert into public.rw_src values (1, 5, 10)",
    )
    .await
    .unwrap();
    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect");
    definer
        .apply("TRANSFORM healthy FROM rw_src SELECT val AS v")
        .await
        .expect("define healthy");
    definer
        .apply("TRANSFORM legacy FROM rw_src SELECT rw_src.order + 1 AS o")
        .await
        .expect("define legacy");
    definer.shutdown().await.expect("shutdown definer");
    // The text an earlier release accepted and stored.
    raw.execute(
        "update transform_definitions \
         set definition_text = 'TRANSFORM legacy FROM rw_src SELECT order + 1 AS o' \
         where target_table = 'public.legacy'",
        &[],
    )
    .await
    .unwrap();
    // The upgraded binary attaches before it runs.
    trellis::migrate(&db.pool, &config)
        .await
        .expect("an unreadable definition pauses; it doesn't refuse the attach");

    let running = Trellis::connect(
        config.clone(),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("one unreadable definition doesn't stop the instance");
    let legacy = running
        .status("legacy")
        .await
        .expect("status")
        .expect("legacy is registered");
    assert_eq!(legacy.status, TransformStatus::Paused);
    let failure = legacy.capture_failure.expect("a capture failure says why");
    assert!(
        failure.error.contains("public.legacy")
            && failure.error.contains("'order' is a reserved word")
            && failure
                .error
                .contains("drop the transform and define it again"),
        "{}",
        failure.error
    );

    raw.execute("insert into public.rw_src values (2, 7, 11)", &[])
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let status = running.status("healthy").await.unwrap().unwrap().status;
        if status == TransformStatus::Live {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "healthy never went live: {status:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let token = running.watermark_token().await.expect("token");
    running
        .await_converged(token, std::time::Duration::from_secs(30))
        .await
        .expect("the other transform keeps converging");
    let values: Vec<i32> = raw
        .query("select v from public.healthy order by id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(values, [5, 7]);

    let err = running
        .apply("RESUME TRANSFORM legacy")
        .await
        .expect_err("RESUME can't read the stored text");
    assert!(err.to_string().contains("reserved word"), "{err}");
    running
        .apply("DROP TRANSFORM legacy")
        .await
        .expect("a paused, unreadable transform drops");
    assert!(running.status("legacy").await.unwrap().is_none());
    running.shutdown().await.expect("shutdown");
}

/// A grammar change that refuses text Postgres also refuses needs no bump
/// (ADR-0018): a stored definition whose text the current grammar refuses
/// pauses at attach, naming the refusal, and capture keeps working for the
/// others instead of failing on it.
#[tokio::test]
async fn attaching_pauses_a_definition_whose_text_the_grammar_now_refuses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    for text in [
        "TRANSFORM t FROM s SELECT a + 1 AS x",
        "TRANSFORM healthy FROM s SELECT a AS a",
    ] {
        install_definition(&db.pool, text, &cols(&["a"]), "public")
            .await
            .expect("define");
    }
    // Text an earlier release accepted: a number directly followed by a word.
    raw.execute(
        "update transform_definitions \
         set definition_text = 'TRANSFORM t FROM s SELECT a+1AS x' \
         where target_table = 'public.t'",
        &[],
    )
    .await
    .unwrap();

    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    trellis::migrate(&db.pool, &config)
        .await
        .expect("refused text pauses; it doesn't refuse the attach");

    let statuses: Vec<(String, String)> = raw
        .query(
            "select target_table, status from transform_definitions order by id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(statuses[0], ("public.t".to_string(), "paused".to_string()));
    assert_ne!(statuses[1].1, "paused", "{statuses:?}");
    let error: String = raw
        .query_one("select error from capture_failures", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        error.contains("trailing junk after numeric literal")
            && error.contains("'AS' directly follows the number '1'")
            && error.contains("drop the transform and define it again"),
        "{error}"
    );
    match trellis::defs::catalog::definition_by_target(&db.pool, "t").await {
        Err(CatalogError::Parse(ParseError::NumericTrailingJunk { number, junk })) => {
            assert_eq!((number.as_str(), junk.as_str()), ("1", "AS"));
        }
        other => panic!("expected NumericTrailingJunk, got {other:?}"),
    }
    let catalog = trellis::dev::capture::load_catalog(&raw)
        .await
        .expect("capture keeps working beside the paused definition");
    let readers: Vec<&str> = catalog
        .definitions
        .iter()
        .map(|d| d.target.as_str())
        .collect();
    assert_eq!(readers, ["public.healthy"]);
    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("a second attach over the paused definition succeeds");
    let refused = trellis
        .apply("RESUME TRANSFORM t")
        .await
        .expect_err("RESUME re-reads the refused text");
    assert!(
        refused
            .to_string()
            .contains("trailing junk after numeric literal"),
        "{refused}"
    );
    trellis
        .apply("DROP TRANSFORM t")
        .await
        .expect("DROP doesn't need the text to parse");
}
