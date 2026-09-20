//! Front-door integration tests for relationship enrichment (issue #40):
//! everything goes through the PUBLIC catalog API — `create_relationship`,
//! `create_definition`, `create_target_table` — with no placeholder-def +
//! `definition_text` rewrite hack. Issue #40 removed the two blockers that
//! forced that hack: the validator now accepts a `<rel>.<column>` enrichment
//! (checking cardinality and the referenced column's type against
//! catalog-resolved relationship metadata), and the parser accepts an
//! aggregate over a relationship path in a row-grain (OneToOne) target.
//!
//! Positive cases drive the real staging pipeline to quiescence and assert the
//! target converges to the Postgres LEFT JOIN / correlated-aggregate oracle
//! (`render_relationship_select_sql`). Negative cases assert `create_definition`
//! rejects the ADR-0006-forbidden shapes at validation time, before any catalog
//! row is written.
//!
//! The staging harness (connect, stage a CDC row, seal/drain to quiescence)
//! mirrors `apply_relationships.rs`; see that file for the ring/seal mechanics.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{
    Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef, ValueType,
};
use trellis::defs::{
    CatalogError, ValidationError, create_definition, create_relationship, create_target_table,
    relationship_projection, render_relationship_select_sql, require_single_column_pk,
    source_primary_key,
};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// Stages one image-bearing (CDC-shaped) change into the active ring segment.
async fn stage_cdc(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring.
/// Reverse recompute appends fresh `Recompute` rows into the (new) active
/// segment as it drains, so convergence takes more than one seal.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark — this helper
    // has no live `Intake` running (these tests stage CDC rows by hand),
    // and none of this file's tests exercise guard (a) specifically, so a
    // real watermark would only ever make guard (a) reject spuriously.
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "frontdoor_test",
            1,
            "trellis_frontdoor_test",
            watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

// ---------------------------------------------------------------------
// To-one: a bare `category.name` enrichment, defined entirely through the
// public API and converged against the LEFT JOIN oracle.
// ---------------------------------------------------------------------

fn to_one_def() -> TransformDef {
    TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::RelationshipPath {
                rel: "category".to_string(),
                column: "name".to_string(),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn category_rel() -> HashMap<String, RelationshipDef> {
    HashMap::from([(
        "category".to_string(),
        RelationshipDef {
            name: "category".to_string(),
            from_table: "articles".to_string(),
            from_col: "category_id".to_string(),
            to_table: "categories".to_string(),
            to_col: "id".to_string(),
        },
    )])
}

/// The oracle SELECT the target must equal, keyed by `id`. Unlike the def the
/// front door creates (`category_name` only), the oracle def also projects the
/// source PK `id` so the outer wrapper can read it back — the target table
/// carries `id` as its own PK column, added by `create_target_table`.
fn to_one_oracle_def() -> TransformDef {
    let mut def = to_one_def();
    def.fields.insert(
        0,
        FieldDef {
            name: "id".to_string(),
            expr: Expr::Column("id".to_string()),
        },
    );
    def
}

async fn oracle_to_one(client: &Client) -> HashMap<String, Option<String>> {
    let base = render_relationship_select_sql(&to_one_oracle_def(), &category_rel());
    let sql = format!("select id::text, category_name::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query to-one oracle")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn target_to_one(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select id::text, category_name::text from article_cat", &[])
        .await
        .expect("read article_cat")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test]
async fn frontdoor_to_one_enrichment_converges_to_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full; \
             insert into categories (id, name) values (10, 'Tech'), (20, 'News'); \
             insert into articles (id, category_id, title) values \
             (1, 10, 'a1'), (2, 20, 'a2'), (3, 99, 'a3')",
        )
        .await
        .expect("create + seed tables");

    // Front door: declare the relationship, then create the enrichment
    // definition directly (no placeholder). Issue #40's validator accepts the
    // bare to-one `category.name` path.
    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
        ("title", ValueType::Text),
    ]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
    )
    .await
    .expect("create to-one enrichment definition through the front door");

    let pk = require_single_column_pk(
        source_primary_key(&db.pool, "articles")
            .await
            .expect("introspect articles pk"),
        "articles",
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &to_one_def(),
        "public",
        &pk,
        &source_columns,
        &to_one_def().source,
    )
    .await
    .expect("create target table for enrichment definition");

    // `create_definition`'s initial backfill staged a Recompute per article;
    // drain it and confirm the enrichment matches the LEFT JOIN oracle
    // (article 3 -> NULL: category 99 does not exist).
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after initial backfill"
    );

    // A to-side (related) row change re-derives the from-side rows that read
    // it — the reverse recompute, exercised end-to-end through the same
    // catalog-resolved relationship the front door wrote.
    client
        .execute(
            "update categories set name = 'Technology' where id = 10",
            &[],
        )
        .await
        .expect("update category name");

    // Issue #130 (epic #127) moved the *forward* to-one read off a live
    // to-side lookup onto the settled parent projection (#129) — but nothing
    // yet advances a projection row's *data* columns when its underlying
    // parent changes; that's #131's job, still unbuilt. Without it, the
    // projection's `name` for category 10 would stay frozen at `'Tech'`
    // forever after `create_definition`'s initial widen/catch-up, and the
    // reverse-recompute convergence this test exercises (issue #30, entirely
    // unchanged by #130) would have nothing meaningful left to assert against
    // the live-truth oracle below. Stand in for #131 directly, mirroring
    // exactly the mutation just made to `categories` — delete this once #131
    // lands and keeps the projection itself in sync.
    let projection = relationship_projection(&db.pool, relationship.id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection");
    client
        .execute(
            &format!(
                "update {} set name = 'Technology' where id = 10",
                projection.projection_table
            ),
            &[],
        )
        .await
        .expect("stand in for #131: advance the projection's own data column");

    stage_cdc(
        &client,
        "categories",
        "10",
        "update",
        Some("{\"id\":10,\"name\":\"Tech\"}"),
        Some("{\"id\":10,\"name\":\"Technology\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after related-row update"
    );
}

// ---------------------------------------------------------------------
// To-many: a `sum(comments.word_count)` aggregate enrichment, defined through
// the public API and converged against the correlated-aggregate oracle.
// ---------------------------------------------------------------------

fn to_many_def() -> TransformDef {
    TransformDef {
        target: "article_stats".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total_words".to_string(),
            expr: Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![Expr::RelationshipPath {
                    rel: "comments".to_string(),
                    column: "word_count".to_string(),
                }],
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn comments_rel() -> HashMap<String, RelationshipDef> {
    HashMap::from([(
        "comments".to_string(),
        RelationshipDef {
            name: "comments".to_string(),
            from_table: "articles".to_string(),
            from_col: "id".to_string(),
            to_table: "comments".to_string(),
            to_col: "article_id".to_string(),
        },
    )])
}

/// Oracle counterpart of [`to_many_def`], projecting the source PK `id` too so
/// the outer wrapper can key on it (see [`to_one_oracle_def`]).
fn to_many_oracle_def() -> TransformDef {
    let mut def = to_many_def();
    def.fields.insert(
        0,
        FieldDef {
            name: "id".to_string(),
            expr: Expr::Column("id".to_string()),
        },
    );
    def
}

async fn oracle_to_many(client: &Client) -> HashMap<String, Option<String>> {
    let base = render_relationship_select_sql(&to_many_oracle_def(), &comments_rel());
    let sql = format!("select id::text, total_words::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query to-many oracle")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn target_to_many(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select id::text, total_words::text from article_stats", &[])
        .await
        .expect("read article_stats")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test]
async fn frontdoor_to_many_aggregate_enrichment_converges_to_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // A to-many's join column (`comments.article_id`) is a non-PK column on the
    // to-side, so the reverse resolver needs it in DELETE/UPDATE pre-images:
    // REPLICA IDENTITY FULL (issue #41; `create_relationship` enforces it).
    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full; \
             insert into articles (id, title) values (1, 'a1'), (2, 'a2'); \
             insert into comments (id, article_id, word_count) values \
             (100, 1, 5), (101, 1, 7)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create to-many relationship");

    // Front door: an aggregate over a to-many path in a OneToOne target — the
    // exact shape issue #40's parser change admits and its validator accepts.
    let source_columns = columns(&[("id", ValueType::Numeric), ("title", ValueType::Text)]);
    create_definition(
        &db.pool,
        "TRANSFORM article_stats FROM articles SELECT sum(comments.word_count) AS total_words",
        &source_columns,
    )
    .await
    .expect("create to-many enrichment definition through the front door");

    let pk = require_single_column_pk(
        source_primary_key(&db.pool, "articles")
            .await
            .expect("introspect articles pk"),
        "articles",
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &to_many_def(),
        "public",
        &pk,
        &source_columns,
        &to_many_def().source,
    )
    .await
    .expect("create target table for aggregate enrichment");

    // Backfill: article 1 sums to 12, article 2 has no comments (SUM -> NULL).
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_many(&client).await,
        oracle_to_many(&client).await,
        "after initial backfill"
    );

    // Re-parent a comment from article 1 to article 2: both articles re-derive.
    client
        .execute("update comments set article_id = 2 where id = 101", &[])
        .await
        .expect("re-parent comment");
    stage_cdc(
        &client,
        "comments",
        "101",
        "update",
        Some("{\"id\":101,\"article_id\":1,\"word_count\":7}"),
        Some("{\"id\":101,\"article_id\":2,\"word_count\":7}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_many(&client).await,
        oracle_to_many(&client).await,
        "after re-parenting a comment"
    );
}

// ---------------------------------------------------------------------
// Negatives: ADR-0006-forbidden shapes rejected at `create_definition` time.
// ---------------------------------------------------------------------

#[tokio::test]
async fn frontdoor_rejects_bare_to_many_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full",
        )
        .await
        .expect("create tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create to-many relationship");

    // A bare to-many path denotes a *set*; ADR-0006 requires an aggregate.
    let err = create_definition(
        &db.pool,
        "TRANSFORM article_stats FROM articles SELECT comments.word_count AS wc",
        &columns(&[("id", ValueType::Numeric), ("title", ValueType::Text)]),
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::RelationshipToManyRequiresAggregate {
            field,
            rel,
            column,
        }) => {
            assert_eq!(field, "wc");
            assert_eq!(rel, "comments");
            assert_eq!(column, "word_count");
        }
        other => panic!("expected RelationshipToManyRequiresAggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn frontdoor_rejects_aggregate_over_to_one_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full",
        )
        .await
        .expect("create tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // A to-one path already resolves to a single value; aggregating it is
    // meaningless (ADR-0006).
    let err = create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT min(category.name) AS x",
        &columns(&[
            ("id", ValueType::Numeric),
            ("category_id", ValueType::Numeric),
            ("title", ValueType::Text),
        ]),
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::RelationshipToOneWrappedInAggregate {
            field,
            rel,
            column,
        }) => {
            assert_eq!(field, "x");
            assert_eq!(rel, "category");
            assert_eq!(column, "name");
        }
        other => panic!("expected RelationshipToOneWrappedInAggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn frontdoor_rejects_unknown_to_side_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full",
        )
        .await
        .expect("create tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // The relationship exists and is to-one (bare path is allowed), but
    // `categories` has no `headline` column: rejected when resolving the
    // referenced column's type (ADR-0005: check, don't assume).
    let err = create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.headline AS x",
        &columns(&[
            ("id", ValueType::Numeric),
            ("category_id", ValueType::Numeric),
            ("title", ValueType::Text),
        ]),
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::UnknownRelationshipColumn { table, column }) => {
            assert_eq!(table, "categories");
            assert_eq!(column, "headline");
        }
        other => panic!("expected UnknownRelationshipColumn, got {other:?}"),
    }
}
