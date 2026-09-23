//! Integration tests for the `Trellis` facade's CDC source-table seeding
//! (issue #83 WI3), run against a real, ephemeral Postgres instance via the
//! shared harness (`testkit::TestCluster`).

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{all_source_tables, create_definition, create_relationship};
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

/// Issue #83 WI3: `defs::all_source_tables` (which `app`'s crate-private
/// `qualified_source_tables` passes straight through as the seed for
/// [`trellis::ClientOptions::source_tables`] at staging startup) must seed the
/// full transitive closure of source tables, not just each definition's
/// direct anchor. A definition anchored on
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

    let mut tables = all_source_tables(&db.pool)
        .await
        .expect("seeding query should succeed");
    tables.sort();
    assert_eq!(
        tables,
        vec!["trellis.authors".to_string(), "trellis.posts".to_string()]
    );
}

/// Issue #108 regression: [`Trellis::apply`]'s own source-column
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
        .apply("TRANSFORM events_calc FROM events SELECT tag AS tag, payload AS payload")
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

/// Issue #108 review regression: a source column whose Postgres type the OID
/// registry can't place at all — an array, range, composite, domain, or
/// `citext` — must stay out of the validator's type map, exactly as the
/// pre-#108 `pg_value_type` dropped it.
///
/// Classifying it as `ValueType::Other(PgType::Unrecognized)` and admitting
/// it was strictly worse than dropping it: `PgType::name`'s `"unrecognized"`
/// token is not a Postgres type, so it leaked into generated DDL and casts.
/// `GROUP BY <col>` failed at `create table` with a raw `type "unrecognized"
/// does not exist` (SQLSTATE 42704) instead of a named validation error, and
/// a bare passthrough `define()`d *successfully* only to fail later in
/// `apply_target`'s `$n::text::<type>` cast — a runtime pipeline failure in
/// place of a define-time rejection. Both must be the clean
/// [`ValidationError::UnresolvedColumn`] instead.
///
/// Issue #117 promoted enum types out of this bucket (they gained their own
/// `PgType::Enum` classification and key/`GROUP BY`/`MIN`/`MAX` roles — see
/// `trellis/tests/defs_enum.rs`), so this regression pin moved to an array
/// column, which — like a range, composite, domain, or `citext` — is still
/// genuinely `PgType::Unrecognized` today (`docs/type-support.md` defers
/// these to #122). The original regression this test guards is about the
/// *fallback* behavior for whatever is still unrecognized, not about enums
/// specifically, so swapping the concrete example preserves its intent.
#[tokio::test]
async fn a_column_of_an_unrecognized_type_is_not_admitted_to_the_validators_view() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute(
            "create table people (id integer primary key, m integer[] not null, n numeric)",
        )
        .await
        .expect("seed source table");

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let passthrough = trellis
        .apply("TRANSFORM people_calc FROM people SELECT m AS m")
        .await
        .expect_err("an array passthrough must be rejected at define time, not at apply time");
    let rendered = passthrough.to_string();
    assert!(
        rendered.contains("m") && !rendered.contains("unrecognized"),
        "expected a named unresolved-column error, got: {rendered}"
    );

    let grouped = trellis
        .apply("TRANSFORM mood_totals FROM people GROUP BY m SELECT SUM(n) AS total")
        .await
        .expect_err("an array GROUP BY key must be rejected at define time");
    let rendered = grouped.to_string();
    assert!(
        !rendered.contains("unrecognized"),
        "a fake `unrecognized` pg type must never reach generated SQL, got: {rendered}"
    );

    // Neither rejected definition may have left a half-built target table
    // behind.
    let leftovers: i64 = db
        .pool
        .get()
        .await
        .expect("connection")
        .query_one(
            "select count(*) from information_schema.tables \
             where table_name in ('people_calc', 'mood_totals')",
            &[],
        )
        .await
        .expect("count target tables")
        .get(0);
    assert_eq!(leftovers, 0);
}

/// Issue #108 review regression, the *to-side* half of the one above: an
/// unrecognized-typed column on a relationship's to-table is reached by a
/// different producer — `catalog::resolve_relationships`, which types every
/// to-side column through the same OID registry but (unlike
/// `Trellis::source_columns`) legitimately keeps them, since a
/// `<rel>.<column>` enrichment field is a pure text projection
/// (`jsonb_each_text(jsonb_build_object(..., <col>::text, ...))` since issue
/// #248, `to_jsonb(p.*)` before it), never a typed read.
///
/// Pre-#108 those columns typed as `ValueType::Text` via the `_ => Text`
/// fallthrough, so the target column was created as `text` and the
/// enrichment worked. Tagging them `Other(Unrecognized)` and rendering
/// `PgType::name` into DDL broke that outright — `create table ... (mood
/// unrecognized)`. `PgType::sql_type_name` restores the `text` rendering for
/// exactly this case.
///
/// Issue #117 promoted enum types out of `PgType::Unrecognized` (an enum
/// to-side enrichment column now keeps its own real type, `USER-DEFINED` per
/// `information_schema.columns.data_type`, not `text` — see
/// `trellis/tests/defs_enum.rs`), so this regression pin moved to an array
/// column, which — like a range, composite, domain, or `citext` — is still
/// genuinely `PgType::Unrecognized` and so still exercises the fallback this
/// test guards.
#[tokio::test]
async fn an_unrecognized_to_side_enrichment_column_still_lands_as_text() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute(
            "create table authors (id integer primary key, m integer[] not null);
             create table posts (id integer primary key, author_id integer);
             alter table authors replica identity full;
             alter table posts replica identity full;",
        )
        .await
        .expect("seed tables");

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.author_id TO authors.id",
    )
    .await
    .expect("to-one relationship should be stored");

    trellis
        .apply("TRANSFORM posts_calc FROM posts SELECT author.m AS mood")
        .await
        .expect("an array to-side enrichment column must keep working as a text projection");

    let data_type: String = db
        .pool
        .get()
        .await
        .expect("connection")
        .query_one(
            "select data_type from information_schema.columns \
             where table_name = 'posts_calc' and column_name = 'mood'",
            &[],
        )
        .await
        .expect("introspect target column")
        .get(0);
    assert_eq!(data_type, "text");
}

/// The enum counterpart of the array-based regression above: since issue
/// #117, an enum to-side enrichment column keeps its own real Postgres
/// type — `USER-DEFINED` per `information_schema.columns.data_type`,
/// reported that way for any user-defined type — not `text`, because
/// `catalog::resolve_relationships` now classifies it via
/// `pg_type::value_type_for_oid`'s enum-aware lookup rather than falling
/// through to `Other(Unrecognized)`.
#[tokio::test]
async fn an_enum_to_side_enrichment_column_keeps_its_own_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute(
            "create type mood as enum ('sad', 'ok', 'happy');
             create table authors (id integer primary key, m mood not null);
             create table posts (id integer primary key, author_id integer);
             alter table authors replica identity full;
             alter table posts replica identity full;",
        )
        .await
        .expect("seed tables");

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.author_id TO authors.id",
    )
    .await
    .expect("to-one relationship should be stored");

    trellis
        .apply("TRANSFORM posts_calc FROM posts SELECT author.m AS mood")
        .await
        .expect("an enum to-side enrichment column must be recognized (issue #117)");

    let data_type: String = db
        .pool
        .get()
        .await
        .expect("connection")
        .query_one(
            "select data_type from information_schema.columns \
             where table_name = 'posts_calc' and column_name = 'mood'",
            &[],
        )
        .await
        .expect("introspect target column")
        .get(0);
    assert_eq!(
        data_type, "USER-DEFINED",
        "an enum to-side enrichment column must keep its own real type, not collapse to text"
    );
}

/// Issue #234, the missing front-door pin for `Trellis::start_client`'s half
/// of "a configured schema must be carried, not re-resolved": a `Trellis`
/// must run its **background client** in its own `Config`'s schema, not in
/// whatever `TRELLIS_SCHEMA`/`DEFAULT_SCHEMA` the *process* environment
/// resolves to.
///
/// `start_client` used to hand only `config.dsn()` to `Client::start`, which
/// re-resolved a `Config` from the environment — so a `Trellis` built with
/// `Config::with_schema(dsn, "…")` read its catalog and answered
/// `await_converged` out of its configured instance while its staging
/// worker, intake and drain workers all ran against a *different* instance's
/// ring. `Client::start_with_config` fixes it; this is the regression pin.
///
/// The database is an *isolated* (already-migrated) one, so `DEFAULT_SCHEMA`
/// exists and a wrongly-resolved client would start up perfectly happily.
/// What gives the bug away is which ring the write actually flows through:
/// the configured instance's own `replication_progress`/ring never see it,
/// so `await_converged` — which asks that instance's own predicate — cannot
/// return `Ok`, and the target row never appears.
#[tokio::test]
async fn the_background_client_runs_in_the_configured_schema_not_the_process_default() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let config =
        Config::with_schema(db.dsn().to_string(), "instance_x").expect("test schema name is valid");
    let pool = trellis::Pool::new(&config).expect("build pool for the configured instance");
    trellis::migrate(&pool, &config)
        .await
        .expect("migrate the configured instance's schema");

    // Source table and definition both live in (and are read through) the
    // configured instance, never the default one.
    pool.get()
        .await
        .expect("connection")
        .batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect definer");
    definer
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    let running = Trellis::connect(
        config.clone(),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect running trellis in the configured schema");

    pool.get()
        .await
        .expect("connection")
        .execute("insert into widgets (id, price) values (1, 9)", &[])
        .await
        .expect("insert source row");

    // Per `Trellis::watermark_token`'s contract: taken after the write's own
    // commit has already returned.
    let token = running.watermark_token().await.expect("watermark_token");
    running
        .await_converged(token, std::time::Duration::from_secs(30))
        .await
        .expect(
            "the background client must stage and drain through the *configured* instance's \
             ring — before issue #234 it ran against DEFAULT_SCHEMA's ring instead, leaving this \
             instance's own convergence predicate permanently unsatisfiable",
        );

    let price: Option<i32> = pool
        .get()
        .await
        .expect("connection")
        .query_opt("select price from public.widget_prices where id = 1", &[])
        .await
        .expect("read target table")
        .map(|row| row.get(0));
    assert_eq!(
        price,
        Some(9),
        "the configured instance's own pipeline must have applied the write, not merely have \
         flipped a predicate"
    );

    running.shutdown().await.expect("shutdown running trellis");
}

/// Issues #311/#367 review: `request_backfill` parks through the shared
/// `park_marker` upsert now, and a database failure there must still surface
/// as a plain [`trellis::TrellisError::Db`]. It briefly surfaced as
/// `TrellisError::Publication`, whose message claims a definition was just
/// dropped.
#[tokio::test]
async fn request_backfill_parks_a_marker_and_reports_a_park_failure_as_db() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table widgets (id bigint primary key); \
             create publication trellis_pub for table widgets;",
        )
        .await
        .expect("seed a published source table");

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    trellis
        .request_backfill("widgets")
        .await
        .expect("request_backfill on a published table");
    let parked: i64 = client
        .query_one(
            "select count(*) from pending_backfill where table_name = 'trellis.widgets'",
            &[],
        )
        .await
        .expect("read markers")
        .get(0);
    assert_eq!(parked, 1, "request_backfill parks one marker");

    // Make the park itself fail: its upsert now violates a check constraint.
    client
        .batch_execute(
            "alter table pending_backfill \
             add constraint refuse_widgets check (table_name <> 'trellis.widgets') not valid",
        )
        .await
        .expect("add refusing constraint");
    let err = trellis
        .request_backfill("widgets")
        .await
        .expect_err("the park violates the constraint");
    assert!(
        matches!(err, trellis::TrellisError::Db(_)),
        "a failed park is a plain database error, got {err:?}: {err}"
    );
}
