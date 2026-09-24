//! Integration tests for issue #227 — the unified, grammar-driven
//! [`trellis::Trellis::apply`] entrypoint every definition-changing operation
//! now goes through.
//!
//! The *grammar* is covered by unit tests next to the parser
//! (`defs::statement_grammar_tests`), and each operation's own semantics by
//! `pause_and_drop.rs` (whose every call site is an `apply` statement) and
//! `column_quarantine.rs`. What's left — and what this file covers — is the
//! part only a real database can answer: that dispatch actually routes each
//! statement to the operation it names, that `Applied` reports the right
//! outcome for each, and the two addressing rules whose resolution issue #228
//! left to this implementation.
//!
//! - every statement form dispatches, and reports itself
//!   (`every_statement_form_dispatches_through_the_one_entrypoint`)
//! - a relationship address may carry its from-table's schema
//!   (`a_schema_qualified_relationship_address_resolves_to_the_same_relationship`)
//! - a schema that names a different table names no relationship, which a drop
//!   treats as its own idempotent no-op
//!   (`a_relationship_address_with_the_wrong_schema_is_a_drop_no_op`)
//! - and that holds when the wrong schema names a *registered* same-named table
//!   too, checked against the relationship's own recorded from-table schema
//!   (`a_relationship_is_not_dropped_by_a_same_named_table_in_another_schema`,
//!   issue #285)
//! - `PAUSE`/`RESUME RELATIONSHIP` is not a statement of this grammar, and a
//!   statement outside the grammar changes nothing
//!   (`statements_outside_the_grammar_are_parse_errors_that_change_nothing`)

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Applied, Config, ErrorCode, TransformStatus, Trellis, TrellisOptions};

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

/// `authors`/`posts`, the from/to pair a relationship needs, plus an
/// aggregate-shaped source. Aggregates build synchronously, so a test that
/// needs a definition to actually be `live` doesn't have to stand up drain
/// workers to get there.
async fn seed(raw: &Client) {
    raw.batch_execute(
        "create table authors (id bigint primary key, name text); \
         create table posts (id bigint primary key, author bigint); \
         create table orders (id bigint primary key, g bigint, a numeric); \
         alter table authors replica identity full; \
         alter table posts replica identity full; \
         alter table orders replica identity full; \
         insert into authors (id, name) values (1, 'a'), (2, 'b'); \
         insert into posts (id, author) values (1, 1), (2, 1), (3, 2); \
         insert into orders (id, g, a) select s, s % 2, s from generate_series(1, 6) s;",
    )
    .await
    .expect("seed sources");
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count query").get(0)
}

/// One pass over all six statement forms in the order an operator would
/// actually use them — define, define, pause, resume, pause, drop, drop —
/// asserting the [`Applied`] variant each one reports.
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

    // RELATIONSHIP — reports the declaration it registered.
    let applied = trellis
        .apply("RELATIONSHIP posts FROM authors.id TO posts.author")
        .await
        .expect("declare a relationship");
    let relationship = applied
        .into_relationship()
        .expect("a RELATIONSHIP statement registers a relationship");
    assert_eq!(relationship.def.name, "posts");

    // TRANSFORM — reports the definition it registered, with its status.
    let applied = trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define an aggregate transform");
    let definition = applied
        .into_transform()
        .expect("a TRANSFORM statement registers a transform");
    assert_eq!(definition.def.target, "order_rollup");
    assert_eq!(
        definition.status,
        TransformStatus::Live,
        "an aggregate builds synchronously"
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

    // DROP RELATIONSHIP — scoped to its from-table.
    assert!(
        matches!(
            trellis
                .apply("DROP RELATIONSHIP authors.posts")
                .await
                .expect("drop the relationship"),
            Applied::Dropped
        ),
        "a DROP RELATIONSHIP statement must report a drop"
    );
    assert_eq!(
        count(&raw, "select count(*) from relationship_definitions").await,
        0
    );
}

/// Issue #228 decision 1 allows the optional `<schema>.` qualifier on a
/// relationship address. A relationship is keyed on the schema its bare
/// from-table resolved to when declared (`from_schema`, issues #285/#288), so
/// the qualified address is an exact lookup; with only one schema declaring
/// `authors.posts`, the bare address names that same relationship. The two
/// spellings must therefore reach the same relationship.
#[tokio::test]
async fn a_schema_qualified_relationship_address_resolves_to_the_same_relationship() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("RELATIONSHIP posts FROM authors.id TO posts.author")
        .await
        .expect("declare the relationship");

    trellis
        .apply(&format!("DROP RELATIONSHIP {DEFAULT_SCHEMA}.authors.posts"))
        .await
        .expect("the schema-qualified spelling addresses the same relationship");
    assert_eq!(
        count(&raw, "select count(*) from relationship_definitions").await,
        0,
        "the qualified address must have actually dropped it, not no-op'd"
    );
}

/// The other side of that check: a qualifier naming a table this relationship
/// was *not* declared against addresses no registered relationship — and "no
/// such relationship" is precisely a drop's idempotent no-op, the same answer a
/// never-declared name gets. What must not happen is the qualifier being
/// silently ignored and the relationship dropped anyway.
#[tokio::test]
async fn a_relationship_address_with_the_wrong_schema_is_a_drop_no_op() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("RELATIONSHIP posts FROM authors.id TO posts.author")
        .await
        .expect("declare the relationship");

    trellis
        .apply("DROP RELATIONSHIP nowhere.authors.posts")
        .await
        .expect("an address naming nothing is a drop's own no-op success");
    assert_eq!(
        count(&raw, "select count(*) from relationship_definitions").await,
        1,
        "a qualifier that doesn't match must not be ignored — the relationship stands"
    );
}

/// Issue #285, the sharp edge of that same check: the mismatched schema names a
/// table Trellis *has* registered — a real same-named table in another schema,
/// the case `V24__schema_nodes_qualified_identity.sql` exists to support.
///
/// `authors` lives in both the default schema and `shop`, both are registered
/// `schema_nodes` (`shop.authors` via an explicitly-qualified transform source,
/// issue #76's grammar), and the relationship is declared against the default
/// schema's `authors`. `DROP RELATIONSHIP shop.authors.posts` must therefore
/// name nothing — `shop.authors` declared no relationship.
///
/// Before this fix the qualifier was checked by asking whether *some*
/// `schema_nodes` row named `shop.authors` existed; it did, so the check passed
/// and the drop fell through to a bare `(from_table, name)` lookup that found
/// and silently destroyed the *other* schema's relationship. `DROP` is
/// destructive and takes its projection table with it, so the address the
/// caller believed was scoping the drop has to actually scope it.
#[tokio::test]
async fn a_relationship_is_not_dropped_by_a_same_named_table_in_another_schema() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw).await;

    // A second, independent `authors` — same bare name, different schema.
    raw.batch_execute(
        "create schema shop; \
         create table shop.authors (id bigint primary key, name text, rank bigint); \
         alter table shop.authors replica identity full; \
         insert into shop.authors (id, name, rank) values (1, 'z', 7);",
    )
    .await
    .expect("seed shop.authors");

    let trellis = define_only(db.dsn()).await;

    // Declared against the default schema's `authors`: `search_path` is pinned
    // there, and the `RELATIONSHIP` grammar has no qualified endpoint spelling
    // to say otherwise.
    trellis
        .apply("RELATIONSHIP posts FROM authors.id TO posts.author")
        .await
        .expect("declare the relationship on the default schema's authors");

    // Registers `shop.authors` as a node of its own, so the wrong-schema
    // address below names a table Trellis really knows about — without this the
    // test would only re-cover `nowhere.authors.posts` above.
    trellis
        .apply("TRANSFORM shop_author_ranks FROM shop.authors SELECT rank AS total_rank")
        .await
        .expect("register shop.authors via an explicitly-qualified source");
    assert_eq!(
        count(
            &raw,
            "select count(*) from schema_nodes where table_name = 'shop.authors'"
        )
        .await,
        1,
        "shop.authors must be a registered node for this test to mean anything"
    );

    trellis
        .apply("DROP RELATIONSHIP shop.authors.posts")
        .await
        .expect("an address naming a schema that declared nothing is a drop's own no-op");
    assert_eq!(
        count(
            &raw,
            &format!(
                "select count(*) from relationship_definitions \
                 where from_schema = '{DEFAULT_SCHEMA}' and from_table = 'authors' \
                   and name = 'posts'"
            )
        )
        .await,
        1,
        "shop.authors's qualifier must not drop the relationship declared on \
         {DEFAULT_SCHEMA}.authors"
    );

    // And the correct qualifier still reaches it — the fix narrows the address,
    // it doesn't break the spelling that was always right.
    trellis
        .apply(&format!("DROP RELATIONSHIP {DEFAULT_SCHEMA}.authors.posts"))
        .await
        .expect("the declaring schema's own qualifier addresses the relationship");
    assert_eq!(
        count(&raw, "select count(*) from relationship_definitions").await,
        0,
        "the matching qualifier must actually drop it"
    );
}

/// `PAUSE`/`RESUME` are transform-only: a relationship is a reusable component
/// of a transform, not something that does work of its own, so there is nothing
/// for a pause to suspend and `PAUSE RELATIONSHIP` is not a form of this
/// grammar at all. Through the facade, that has to look like any other invalid
/// statement — a parse error, raised before any connection is used, leaving the
/// catalog untouched — with no half-run operation and no dispatch arm that has
/// nothing to call.
///
/// `DROP TRANSFORM <target>.<column>` is checked alongside it for the same
/// reason: also not a form (dropping one field is `ALTER TRANSFORM`), also an
/// ordinary parse error.
#[tokio::test]
async fn statements_outside_the_grammar_are_parse_errors_that_change_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw).await;

    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("RELATIONSHIP posts FROM authors.id TO posts.author")
        .await
        .expect("declare a relationship to aim the invalid statements at");
    trellis
        .apply("TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define a transform to aim the invalid statements at");

    for statement in [
        "PAUSE RELATIONSHIP authors.posts",
        "RESUME RELATIONSHIP authors.posts",
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

    // Nothing ran: both definitions stand, and the transform is untouched by
    // the pause/resume that never happened.
    assert_eq!(
        count(&raw, "select count(*) from relationship_definitions").await,
        1
    );
    assert_eq!(
        trellis
            .status("order_rollup")
            .await
            .expect("status")
            .map(|s| s.status),
        Some(TransformStatus::Live),
        "an invalid statement must leave the catalog exactly as it was"
    );
}
