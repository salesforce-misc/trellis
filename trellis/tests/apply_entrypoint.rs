//! Integration tests for the unified, grammar-driven
//! [`trellis::Trellis::apply`] entrypoint every definition-changing operation
//! now goes through.
//!
//! The *grammar* is covered by unit tests next to the parser
//! (`defs::statement_grammar_tests`), and each operation's own semantics by
//! `pause_and_drop.rs` (whose every call site is an `apply` statement) and
//! `column_quarantine.rs`. What's left — and what this file covers — is the
//! part only a real database can answer: that dispatch actually routes each
//! statement to the operation it names, that `Applied` reports the right
//! outcome for each, and that text outside the grammar changes nothing.
//!
//! - every statement form dispatches, and reports itself
//!   (`every_statement_form_dispatches_through_the_one_entrypoint`)
//! - `RELATIONSHIP`, `DROP RELATIONSHIP` and `PAUSE`/`RESUME RELATIONSHIP` are
//!   not statements: each is a parse error that registers nothing
//!   (`relationship_statements_are_parse_errors_that_register_nothing`)
//! - a statement outside the grammar changes nothing
//!   (`statements_outside_the_grammar_are_parse_errors_that_change_nothing`)

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Applied, Config, ErrorCode, TransformStatus, Trellis, TrellisError, TrellisOptions};

/// Connects directly to `dsn` with `search_path` pinned, like every other
/// integration test here, so assertions read Postgres rather than the engine.
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

async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// An aggregate-shaped source.
async fn seed(raw: &Client) {
    raw.batch_execute(
        "create table orders (id bigint primary key, g bigint, a numeric(20,0)); \
         insert into orders (id, g, a) select s, s % 2, s from generate_series(1, 6) s;",
    )
    .await
    .expect("seed sources");
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count query").get(0)
}

/// One pass over the statement forms in the order an operator would actually
/// use them — define, pause, resume, pause, drop — asserting the [`Applied`]
/// variant each one reports.
///
/// The point isn't any single operation (each has its own suite); it's that
/// *parsing* is what picked the operation every time, through one method whose
/// signature never mentioned any of them.
#[tokio::test]
async fn every_statement_form_dispatches_through_the_one_entrypoint() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw).await;

    let trellis = define_only(db.dsn()).await;

    // TRANSFORM — reports the target table it registered.
    let applied = trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define an aggregate transform");
    let target_table = applied
        .into_transform()
        .expect("a TRANSFORM statement registers a transform");
    assert!(target_table.ends_with(".order_rollup"), "{target_table}");
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .expect("registered")
            .status,
        TransformStatus::WaitingToBackfill,
        "registration returns before the build (ADR-0002)"
    );

    // PAUSE TRANSFORM — freezes it.
    assert!(
        matches!(
            trellis
                .apply("PAUSE TRANSFORM order_rollup")
                .await
                .expect("pause"),
            Applied::Paused
        ),
        "a PAUSE statement must report a pause"
    );
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .map(|s| s.status),
        Some(TransformStatus::Paused)
    );

    // RESUME TRANSFORM — rebuilds by backfill, with no per-column result.
    let applied = trellis
        .apply("RESUME TRANSFORM order_rollup")
        .await
        .expect("resume");
    assert!(
        matches!(&applied, Applied::Resumed { columns } if columns.is_empty()),
        "a whole-transform resume reports no columns, got {applied:?}"
    );
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .map(|s| s.status),
        Some(TransformStatus::WaitingToBackfill),
        "RESUME rebuilds rather than catching up (ADR-0014)"
    );

    // DROP TRANSFORM — needs the pause first, then removes it.
    let err = trellis
        .apply("DROP TRANSFORM order_rollup")
        .await
        .expect_err("there is no live-to-gone edge");
    assert_eq!(err.code(), ErrorCode::Conflict);
    trellis
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause before dropping");
    assert!(
        matches!(
            trellis
                .apply("DROP TRANSFORM order_rollup")
                .await
                .expect("drop"),
            Applied::Dropped
        ),
        "a DROP statement must report a drop"
    );
    assert_eq!(
        count(&raw, "select count(*) from transform_definitions").await,
        0
    );
}

/// `DROP TRANSFORM <target>.<column>` is not a form of this grammar (dropping
/// one field is `ALTER TRANSFORM`). Through the facade, that has to look like
/// any other invalid statement — a parse error, raised before any connection
/// is used, leaving the catalog untouched — with no half-run operation and no
/// dispatch arm that has nothing to call.
#[tokio::test]
async fn statements_outside_the_grammar_are_parse_errors_that_change_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g WITHOUT SOURCE INDEX SELECT sum(a) AS total")
        .await
        .expect("define a transform to aim the invalid statements at");

    for statement in [
        "DROP TRANSFORM public.order_rollup.total",
        // A transform's `<transform>.<column>`: DROP takes no column.
        "DROP TRANSFORM order_rollup.total",
    ] {
        let err = trellis
            .apply(statement)
            .await
            .expect_err(&format!("{statement:?} is not a statement of this grammar"));
        assert_eq!(
            err.code(),
            ErrorCode::Parse,
            "a statement outside the grammar is a rejection of the text: {err}"
        );
    }

    // Nothing ran: the transform stands, untouched.
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .map(|s| s.status),
        Some(TransformStatus::WaitingToBackfill),
        "an invalid statement must leave the catalog exactly as it was"
    );
}

/// `RELATIONSHIP`, `DROP RELATIONSHIP` and `PAUSE`/`RESUME RELATIONSHIP` are
/// not statements:
/// `apply` refuses each as text it can't parse, before registering anything,
/// and `statement_kind` gives the same answer without a database.
#[tokio::test]
async fn relationship_statements_are_parse_errors_that_register_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table users (id bigint primary key, name text); \
         create table widgets (id bigint primary key, owner_id bigint);",
    )
    .await
    .expect("seed sources");

    let trellis = define_only(db.dsn()).await;
    for statement in [
        "RELATIONSHIP owner FROM widgets.owner_id TO users.id",
        "DROP RELATIONSHIP widgets.owner",
        "PAUSE RELATIONSHIP widgets.owner",
        "RESUME RELATIONSHIP widgets.owner",
    ] {
        let err = trellis
            .apply(statement)
            .await
            .expect_err(&format!("{statement:?} is not a statement of this grammar"));
        assert_eq!(err.code(), ErrorCode::Parse, "{statement}: {err}");
        let TrellisError::Parse(parse) = &err else {
            panic!("{statement}: expected TrellisError::Parse, got {err:?}");
        };
        assert_eq!(
            trellis::statement_kind(statement).expect_err(statement),
            *parse,
            "{statement}: statement_kind must refuse it the same way"
        );
    }

    assert!(
        trellis.definitions().await.expect("definitions").is_empty(),
        "a refused statement registers nothing"
    );
}
