//! Front-door tests for ADR-0006's **nullability semantics** (issue #33), in
//! the *incremental* direction that the backfill-time tests
//! (`defs_backfill_relationship.rs`) and the convergence tests
//! (`defs_relationship_frontdoor.rs`) leave uncovered.
//!
//! ADR-0006 ("Nullability") pins two rules:
//!
//! * A **to-one** relationship with no matching related row yields `NULL` for
//!   its enrichment columns (left-join semantics) — and, crucially, *does not
//!   affect whether the referencing row exists*. The from-row survives a
//!   matching to-row appearing and disappearing again; only its enrichment
//!   columns move.
//! * A **to-many** relationship with no related rows yields the aggregate's
//!   *empty result* — `COUNT` -> `0`, `SUM` -> `NULL` — matching PostgreSQL.
//!   `SUM` over zero rows is `NULL`, not `0`.
//!
//! Both tests drive the real front door (`create_relationship` +
//! `install_definition`) and then the real staging pipeline, asserting the
//! exact expected values at each transition *and* byte-equality with the
//! Postgres oracle (`render_relationship_select_sql`), so a change that broke
//! the engine and the oracle identically would still trip the pinned values.
//!
//! The staging harness (connect, stage a CDC row, seal/drain to quiescence)
//! mirrors `defs_relationship_frontdoor.rs`; see that file for the ring/seal
//! mechanics.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{
    Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef, ValueType,
};
use trellis::defs::{create_relationship, install_definition, render_relationship_select_sql};
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
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
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
            "nullability_test",
            1,
            "trellis_nullability_test",
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
// To-one: a matching related row appearing, then disappearing again.
// ---------------------------------------------------------------------

const ARTICLE_CAT: &str =
    "TRANSFORM article_cat FROM articles SELECT category.name AS category_name";

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

/// [`ARTICLE_CAT`] plus the source PK projected, so the oracle wrapper can key
/// its rows the same way the target table does.
fn to_one_oracle_def() -> TransformDef {
    TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                expr: Expr::Column("id".to_string()),
            },
            FieldDef {
                name: "category_name".to_string(),
                expr: Expr::RelationshipPath {
                    rel: "category".to_string(),
                    column: "name".to_string(),
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
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

/// ADR-0006's to-one nullability rule, across the full incremental arc:
/// no match -> NULL, the match *appearing* -> populated, the match
/// *disappearing* -> back to NULL. At every step the from-row itself must
/// still be present: a missing related row nulls enrichment columns, it never
/// removes (or withholds) the referencing row.
///
/// `defs_relationship_frontdoor.rs` covers front-door to-one *convergence*;
/// `apply_relationships.rs` covers this appear/disappear arc, but through the
/// placeholder-def + `definition_text` rewrite hack rather than
/// `install_definition`. This is the arc through the real front door.
#[tokio::test]
async fn to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Article 1 points at category 10, which does not exist yet — the FK is
    // unresolved from the very first build, not merely orphaned later. The
    // to-side's own join-key-only needs would be satisfied by Postgres's
    // DEFAULT replica identity (a to-one's join key *is* the to-side primary
    // key), but issue #129's settled parent projection needs `REPLICA
    // IDENTITY FULL` unconditionally regardless — see
    // `assert_replica_identity_supports_projection`.
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full; \
             insert into articles (id, category_id, title) values (1, 10, 'a1')",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
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
    install_definition(&db.pool, ARTICLE_CAT, &source_columns, "public")
        .await
        .expect("install the to-one enrichment definition through the front door");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge enumerates the source (ADR-0016)");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Initial derivation: no matching category -> NULL enrichment, but the
    // article's own row exists.
    assert_eq!(
        target_to_one(&client).await,
        HashMap::from([("1".to_string(), None)]),
        "no matching related row must yield a present row with NULL enrichment"
    );
    assert_eq!(target_to_one(&client).await, oracle_to_one(&client).await);

    // The match APPEARS: reverse recompute re-derives article 1.
    client
        .execute("insert into categories (id, name) values (10, 'Tech')", &[])
        .await
        .expect("insert category 10");
    stage_cdc(
        &client,
        "categories",
        "10",
        "insert",
        None,
        Some("{\"id\":10,\"name\":\"Tech\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        HashMap::from([("1".to_string(), Some("Tech".to_string()))]),
        "the related row appearing must populate the enrichment column"
    );
    assert_eq!(target_to_one(&client).await, oracle_to_one(&client).await);

    // The match DISAPPEARS: the enrichment falls back to NULL and — the easy
    // thing to get wrong — article 1's row is untouched, not deleted.
    client
        .execute("delete from categories where id = 10", &[])
        .await
        .expect("delete category 10");
    stage_cdc(
        &client,
        "categories",
        "10",
        "delete",
        Some("{\"id\":10,\"name\":\"Tech\"}"),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        HashMap::from([("1".to_string(), None)]),
        "the related row disappearing must null the enrichment, not the row"
    );
    assert_eq!(target_to_one(&client).await, oracle_to_one(&client).await);

    let rows: i64 = client
        .query_one("select count(*) from article_cat", &[])
        .await
        .expect("count article_cat")
        .get(0);
    assert_eq!(
        rows, 1,
        "row existence is unaffected by the relationship resolving or not (ADR-0006)"
    );
}

// ---------------------------------------------------------------------
// To-many: the last related row disappearing, incrementally.
// ---------------------------------------------------------------------

const ARTICLE_STATS: &str = "TRANSFORM article_stats FROM articles SELECT \
     COUNT(comments.id) AS comment_count, SUM(comments.word_count) AS total_words";

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

fn agg(name: &str, column: &str) -> Expr {
    Expr::FunctionCall {
        name: name.to_string(),
        args: vec![Expr::RelationshipPath {
            rel: "comments".to_string(),
            column: column.to_string(),
        }],
    }
}

/// [`ARTICLE_STATS`] plus the source PK projected (see [`to_one_oracle_def`]).
fn to_many_oracle_def() -> TransformDef {
    TransformDef {
        target: "article_stats".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                expr: Expr::Column("id".to_string()),
            },
            FieldDef {
                name: "comment_count".to_string(),
                expr: agg("COUNT", "id"),
            },
            FieldDef {
                name: "total_words".to_string(),
                expr: agg("SUM", "word_count"),
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

type Stats = HashMap<String, (Option<String>, Option<String>)>;

async fn oracle_to_many(client: &Client) -> Stats {
    let base = render_relationship_select_sql(&to_many_oracle_def(), &comments_rel());
    let sql = format!("select id::text, comment_count::text, total_words::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query to-many oracle")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

async fn target_to_many(client: &Client) -> Stats {
    client
        .query(
            "select id::text, comment_count::text, total_words::text from article_stats",
            &[],
        )
        .await
        .expect("read article_stats")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

fn stats(count: &str, sum: Option<&str>) -> Stats {
    HashMap::from([(
        "1".to_string(),
        (Some(count.to_string()), sum.map(str::to_string)),
    )])
}

/// ADR-0006's to-many nullability rule, in the transition nobody covered: the
/// *last* related row being deleted incrementally. Every to-many empty-set case
/// tested elsewhere is either initial derivation
/// (`defs_backfill_relationship.rs`) or inside a `GROUP BY` fold
/// (`defs_aggregate_relationship.rs`); here a plain to-many-enriched 1-1
/// definition is walked down to zero related rows one delete at a time, and
/// back up again.
///
/// The exact pinned values matter: at zero related rows `COUNT` is `0` and
/// `SUM` is `NULL` — Postgres's empty-aggregate semantics, *not* `0` for both
/// and *not* `NULL` for both. Collapsing either one is the bug this guards.
#[tokio::test]
async fn to_many_aggregate_returns_the_empty_set_when_the_last_related_row_goes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // A to-many's join column (`comments.article_id`) is a non-PK column on the
    // to-side, so DELETE pre-images need `REPLICA IDENTITY FULL` to carry it —
    // exactly what makes the "last child deleted" case resolvable at all.
    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full; \
             insert into articles (id, title) values (1, 'a1'); \
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

    let source_columns = columns(&[("id", ValueType::Numeric), ("title", ValueType::Text)]);
    install_definition(&db.pool, ARTICLE_STATS, &source_columns, "public")
        .await
        .expect("install the to-many enrichment definition through the front door");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_to_many(&client).await,
        stats("2", Some("12")),
        "two related rows: COUNT 2, SUM 12"
    );
    assert_eq!(target_to_many(&client).await, oracle_to_many(&client).await);

    // Delete the first child: still one related row left, so nothing about the
    // empty-set semantics is exercised yet — this step exists to prove the
    // walk-down is incremental rather than a single jump to zero.
    client
        .execute("delete from comments where id = 100", &[])
        .await
        .expect("delete comment 100");
    stage_cdc(
        &client,
        "comments",
        "100",
        "delete",
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_many(&client).await,
        stats("1", Some("7")),
        "one related row left: COUNT 1, SUM 7"
    );
    assert_eq!(target_to_many(&client).await, oracle_to_many(&client).await);

    // Delete the LAST child: the aggregate's empty result. COUNT -> 0, SUM ->
    // NULL, matching Postgres exactly (ADR-0006).
    client
        .execute("delete from comments where id = 101", &[])
        .await
        .expect("delete comment 101");
    stage_cdc(
        &client,
        "comments",
        "101",
        "delete",
        Some("{\"id\":101,\"article_id\":1,\"word_count\":7}"),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_many(&client).await,
        stats("0", None),
        "no related rows: COUNT must be 0 and SUM must be NULL, not 0"
    );
    assert_eq!(target_to_many(&client).await, oracle_to_many(&client).await);

    let rows: i64 = client
        .query_one("select count(*) from article_stats", &[])
        .await
        .expect("count article_stats")
        .get(0);
    assert_eq!(
        rows, 1,
        "losing every related row must not remove the referencing row"
    );

    // ...and it recovers: a new related row lifts both aggregates back out of
    // the empty set, so the zero/NULL state above is not absorbing.
    client
        .execute(
            "insert into comments (id, article_id, word_count) values (102, 1, 4)",
            &[],
        )
        .await
        .expect("insert comment 102");
    stage_cdc(
        &client,
        "comments",
        "102",
        "insert",
        None,
        Some("{\"id\":102,\"article_id\":1,\"word_count\":4}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_many(&client).await,
        stats("1", Some("4")),
        "a related row reappearing must lift the aggregate back out of the empty set"
    );
    assert_eq!(target_to_many(&client).await, oracle_to_many(&client).await);
}
