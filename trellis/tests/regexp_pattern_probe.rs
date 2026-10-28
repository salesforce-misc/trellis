//! Postgres alone judges a `REGEXP_COUNT` pattern literal: define, `ALTER
//! TRANSFORM` and `RESUME` probe each one with `regexp_count('', $pattern)`
//! and refuse on `2201B`. A pattern Postgres accepts but Rust's `regex` crate
//! rejects (back-references, lookahead, `\m`) is admitted; one Rust accepts
//! but Postgres rejects (`(?P<n>a)`, `a++`, `\pL`) is refused in every key
//! space.

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::{FieldDef, assert_regexp_patterns_accepted, parse};
use trellis::{Config, Trellis, TrellisOptions};

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

const SETUP: &str = "create table public.s (id int primary key, g int, txt text); \
     create table public.j (id int primary key, g int, name text)";

/// Patterns Rust's `regex` crate accepts and Postgres rejects with `2201B`.
const PG_REJECTS: &[&str] = &[
    "(?P<n>a)", "(?<n>a)", r"a\z", "a++", r"\pL", "(?i:a)", "(", /* both */
];
/// Patterns Postgres accepts and Rust's `regex` crate rejects.
const PG_ACCEPTS: &[&str] = &[r"(a)\1", "a(?=b)", r"\mfoo"];

fn quoted(pattern: &str) -> String {
    format!("'{}'", pattern.replace('\'', "''"))
}

/// One definition per key space, each reaching `pattern` a different way.
fn definitions(pattern: &str) -> Vec<(&'static str, String)> {
    let p = quoted(pattern);
    vec![
        (
            "one_to_one",
            format!("TRANSFORM d1 FROM public.s SELECT REGEXP_COUNT(txt, {p}) AS n"),
        ),
        (
            "one_to_one_through_another_field",
            format!("TRANSFORM d2 FROM public.s SELECT REGEXP_COUNT(txt, {p}) AS n, n + 1 AS m"),
        ),
        (
            "aggregate_argument",
            format!(
                "TRANSFORM d3 FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
                 SELECT SUM(REGEXP_COUNT(txt, {p})) AS n"
            ),
        ),
        (
            "aggregate_composed",
            format!(
                "TRANSFORM d4 FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
                 SELECT SUM(id) AS t, COUNT(*) AS c, SUM(REGEXP_COUNT(txt, {p})) + 1 AS n"
            ),
        ),
        (
            "join_argument",
            format!(
                "TRANSFORM d5 FROM public.s JOIN public.j ON s.id = j.id GROUP BY s.g \
                 WITHOUT SOURCE INDEX SELECT SUM(REGEXP_COUNT(j.name, {p})) AS n"
            ),
        ),
        (
            "join_composed",
            format!(
                "TRANSFORM d6 FROM public.s JOIN public.j ON s.id = j.id GROUP BY s.g \
                 WITHOUT SOURCE INDEX SELECT COUNT(*) AS c, SUM(REGEXP_COUNT(j.name, {p})) + 1 AS n"
            ),
        ),
    ]
}

#[tokio::test]
async fn define_refuses_a_pattern_postgres_rejects_in_every_key_space() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SETUP).await.expect("setup");
    let trellis = definer(db.dsn()).await;
    for pattern in PG_REJECTS {
        for (space, text) in definitions(pattern) {
            let err = trellis
                .apply(&text)
                .await
                .expect_err(&format!("{space}: {pattern} must be refused"));
            let message = err.to_string();
            assert!(
                message.contains("regexp_count") && message.contains(pattern),
                "{space}: {message}"
            );
            assert!(message.contains("invalid regular expression"), "{message}");
            assert!(message.contains("'n'"), "names the field: {message}");
        }
    }
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn define_admits_a_pattern_only_postgres_accepts() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SETUP).await.expect("setup");
    let trellis = definer(db.dsn()).await;
    for pattern in PG_ACCEPTS {
        assert!(
            regex::Regex::new(pattern).is_err(),
            "{pattern} is one Rust rejects"
        );
        for (space, text) in definitions(pattern) {
            let text = text.replacen("TRANSFORM d", &format!("TRANSFORM a{}_d", id(pattern)), 1);
            trellis
                .apply(&text)
                .await
                .unwrap_or_else(|err| panic!("{space}: {pattern}: {err}"));
        }
    }
    trellis.shutdown().await.expect("shutdown");
}

fn id(pattern: &str) -> String {
    pattern.bytes().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn alter_refuses_a_pattern_postgres_rejects_and_admits_one_it_accepts() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SETUP).await.expect("setup");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM t FROM public.s SELECT txt AS txt")
        .await
        .expect("define");
    // ALTER needs a live definition; mark it so rather than wait for a build.
    raw.batch_execute("update trellis.transform_definitions set status = 'live'")
        .await
        .expect("mark live");
    for pattern in PG_REJECTS {
        let err = trellis
            .apply(&format!(
                "ALTER TRANSFORM t ADD REGEXP_COUNT(txt, {}) AS n",
                quoted(pattern)
            ))
            .await
            .expect_err(&format!("{pattern} must be refused"));
        assert!(
            err.to_string().contains("invalid regular expression"),
            "{err}"
        );
    }
    trellis
        .apply(&format!(
            "ALTER TRANSFORM t ADD REGEXP_COUNT(txt, {}) AS n",
            quoted(r"(a)\1")
        ))
        .await
        .expect("a back-reference is admitted");
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn resume_refuses_a_planted_pattern_postgres_rejects() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SETUP).await.expect("setup");
    let trellis = definer(db.dsn()).await;
    for (space, text) in definitions("zzz") {
        trellis.apply(&text).await.expect(space);
    }
    for (space, text) in definitions("zzz") {
        let target = text
            .strip_prefix("TRANSFORM ")
            .and_then(|t| t.split_whitespace().next())
            .expect("name");
        trellis
            .apply(&format!("PAUSE TRANSFORM {target}"))
            .await
            .expect(space);
        raw.execute(
            "update trellis.transform_definitions \
             set definition_text = replace(definition_text, '''zzz''', '''(?P<n>a)''') \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("plant");
        let err = trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect_err(&format!("{space}: the resume must be refused"));
        assert!(
            err.to_string().contains("invalid regular expression"),
            "{space}: {err}"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}

/// A refused probe rolls back to its savepoint, so the caller's transaction
/// stays usable, and a later pattern in the same transaction is still judged.
#[tokio::test]
async fn a_refused_probe_leaves_the_callers_transaction_usable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let txn = raw.transaction().await.expect("begin");
    let bad = parse("TRANSFORM t FROM s SELECT REGEXP_COUNT(txt, '(?P<n>a)') AS n").expect("parse");
    let err = assert_regexp_patterns_accepted(&txn, &bad, None)
        .await
        .expect_err("refused");
    assert!(
        err.to_string().contains("invalid regular expression"),
        "{err}"
    );
    txn.query_one("select 1", &[])
        .await
        .expect("the transaction is still usable");
    let good = parse("TRANSFORM t FROM s SELECT REGEXP_COUNT(txt, '(a)\\1') AS n").expect("parse");
    assert_regexp_patterns_accepted(&txn, &good, None)
        .await
        .expect("accepted");
    // `only_fields` skips a field outside the edit.
    let only: Vec<FieldDef> = vec![];
    assert_regexp_patterns_accepted(&txn, &bad, Some(&only))
        .await
        .expect("an unedited field is not probed");
}

/// Only `2201B` is a refusal: any other error the probe hits (here a role
/// that may not execute `regexp_count`, `42501`) propagates as a database
/// error, never as a pattern Postgres rejects, and still leaves the caller's
/// transaction usable.
#[tokio::test]
async fn a_probe_error_other_than_an_invalid_pattern_is_not_a_refusal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create role regexp_probe_denied; \
         revoke execute on function pg_catalog.regexp_count(text, text) from public",
    )
    .await
    .expect("setup");
    let txn = raw.transaction().await.expect("begin");
    txn.batch_execute("set local role regexp_probe_denied")
        .await
        .expect("set role");
    let def = parse("TRANSFORM t FROM s SELECT REGEXP_COUNT(txt, 'a') AS n").expect("parse");
    let err = assert_regexp_patterns_accepted(&txn, &def, None)
        .await
        .expect_err("the probe is denied");
    match &err {
        trellis::defs::CatalogError::Db(db_err) => assert_eq!(
            db_err.code(),
            Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE),
            "{err}"
        ),
        other => panic!("expected the database error, got {other:?}"),
    }
    txn.query_one("select 1", &[])
        .await
        .expect("the transaction is still usable");
}

/// The patterns the admitted set counts agree with Postgres's own
/// `regexp_count`, which is the only evaluator.
#[tokio::test]
async fn admitted_patterns_count_as_postgres_counts() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    let row = raw
        .query_one(
            "select regexp_count('abab', '(ab)\\1'), regexp_count('ab ac', 'a(?=b)'), \
              regexp_count('foo food', '\\mfoo')",
            &[],
        )
        .await;
    let row = row.expect("query");
    assert_eq!(row.get::<_, i32>(0), 1);
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, i32>(2), 2);
}
