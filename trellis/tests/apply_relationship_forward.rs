//! Integration tests for the forward-path change issue #130 (epic #127) makes
//! to `staging::apply::build_relationship_context`'s to-one branch: a to-one
//! relationship value now resolves against the settled parent projection
//! (#129), never a live read of the parent — see
//! `trellis/tests/spikes/issue-102-PLAN-DRAFT.md` §2 for the mechanism this
//! replaces `force_every_group`-style live recompute with.
//!
//! Three things this file proves that no other test file does:
//!
//! 1. A to-one relationship read genuinely comes from the projection, not
//!    live state — constructed so live resolution would give the *wrong*
//!    answer and projection-backed resolution gives the *right* one (the
//!    projection is manually desynced from live truth, standing in for
//!    #131's reverse path, which doesn't exist yet).
//! 2. The settled parent projection's `__trellis_gen` bookkeeping column is
//!    bumped exactly once per touched parent per applying transaction, even
//!    when more than one from-side row in the same batch touches the same
//!    parent.
//! 3. A re-point (a from-side row's join column changing within one already
//!    folded change) bumps `gen` for *both* the old and the new parent — the
//!    best-available signal #130 uses, ahead of #133's raw pre-fold fix.
//!
//! To-many relationship reads are covered by
//! `defs_relationship_frontdoor.rs`'s `frontdoor_to_many_aggregate_enrichment_converges_to_oracle`,
//! which already proves (unmodified by #130) that a to-many enrichment keeps
//! reading live — this file's `to_many_relationship_context_has_no_projection_and_stays_live`
//! adds a smaller, targeted check of the same claim.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::defs::{
    create_definition, create_relationship, create_target_table, relationship_projection,
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

fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

async fn stage_cdc(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let src_table = qualify_fixture_table(src_table);
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
/// Every batch in this file is small (well under `claim::MIN_ROWS_TO_SPLIT`),
/// so it seals to a single bucket — one `apply::drain_once` call claims the
/// whole batch and applies it inside exactly one Phase 3 transaction, which
/// is what makes this file's gen-bump assertions meaningful.
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
            "forward_test",
            1,
            "trellis_forward_test",
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

async fn target_category_name(client: &Client, id: i32) -> Option<String> {
    client
        .query_one(
            "select category_name from article_cat where id = $1",
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read article_cat id {id}: {e}"))
        .get(0)
}

async fn projection_table_for(pool: &trellis::Pool, relationship_id: i64) -> String {
    relationship_projection(pool, relationship_id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection")
        .projection_table
}

async fn projection_gen(client: &Client, projection_table: &str, id: i32) -> i64 {
    client
        .query_one(
            &format!("select __trellis_gen from {projection_table} where id = $1"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read {projection_table}'s gen for id {id}: {e}"))
        .get(0)
}

// ---------------------------------------------------------------------
// 1. Forward reads come from the projection, not live state.
// ---------------------------------------------------------------------

/// A forward (from-side) change resolves its to-one relationship value from
/// the settled parent projection, not a live read of the parent — proved by
/// a case where the two disagree: the live `categories` row is renamed
/// *after* the projection has already settled on the original name, with no
/// reverse recompute or projection advance in between (#131 isn't built
/// yet). Live resolution would report the rename immediately; projection
/// resolution can't see it at all.
#[tokio::test]
async fn forward_to_one_resolves_from_the_projection_not_live_parent_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // `create_definition`'s widen (#129) catches category 10's name into the
    // projection right here, while 'Tech' is still its only-ever value.
    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
    ]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &to_one_def(),
        "public",
        &pk,
        &source_columns,
        &to_one_def().source,
    )
    .await
    .expect("create target table");

    // Live-only mutation: no CDC staged, no reverse recompute, nothing
    // advances the projection. Only #131 (not built) would keep it in step.
    client
        .execute("update categories set name = 'Renamed' where id = 10", &[])
        .await
        .expect("rename the live category without touching the projection");

    // A brand-new article, forward-applied for the first time, referencing
    // the now-desynced category.
    client
        .execute("insert into articles (id, category_id) values (1, 10)", &[])
        .await
        .expect("insert article");
    stage_cdc(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"category_id\":10}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_category_name(&client, 1).await,
        Some("Tech".to_string()),
        "forward resolution must read the settled projection (still 'Tech'), not the live \
         category row (renamed to 'Renamed' after the projection settled)"
    );
}

// ---------------------------------------------------------------------
// 2. gen bump semantics.
// ---------------------------------------------------------------------

/// Two different from-side rows touching the same parent in one applying
/// transaction bump that parent's `gen` by exactly 1, not once per touching
/// row — guard (b) (#132) only needs "did anything land since I captured
/// this," not a count.
#[tokio::test]
async fn forward_apply_bumps_gen_once_per_touched_parent_even_with_two_touching_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full",
        )
        .await
        .expect("create + seed tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
    ]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &to_one_def(),
        "public",
        &pk,
        &source_columns,
        &to_one_def().source,
    )
    .await
    .expect("create target table");

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    assert_eq!(
        projection_gen(&client, &projection_table, 10).await,
        0,
        "gen starts at 0, unbumped by definition creation itself"
    );

    // Two articles, both pointing at category 10, staged into the SAME
    // active segment before sealing — one batch, one Phase 3 transaction.
    client
        .batch_execute("insert into articles (id, category_id) values (2, 10), (3, 10)")
        .await
        .expect("insert two articles pointing at the same category");
    stage_cdc(
        &client,
        "articles",
        "2",
        "insert",
        None,
        Some("{\"id\":2,\"category_id\":10}"),
    )
    .await;
    stage_cdc(
        &client,
        "articles",
        "3",
        "insert",
        None,
        Some("{\"id\":3,\"category_id\":10}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_category_name(&client, 2).await,
        Some("Tech".to_string())
    );
    assert_eq!(
        target_category_name(&client, 3).await,
        Some("Tech".to_string())
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 10).await,
        1,
        "category 10 was touched by two from-side rows in the same batch, but its gen must \
         advance by exactly 1 for the whole transaction, not once per touching row"
    );
}

/// A from-side row re-pointed from one parent to another *within a single
/// already-folded change* (a genuine single `UPDATE`, not an insert-then-
/// repoint the fold would erase — see this module's own doc comment and
/// `RelationshipGenBump`'s doc comment on the #133 gap) bumps `gen` for
/// *both* the old and the new parent — both are visible on the folded
/// change's own old/new images, even though #130 doesn't yet look past the
/// fold to a raw pre-fold history (#133's job).
#[tokio::test]
async fn forward_apply_re_point_bumps_gen_for_both_old_and_new_parent() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'), (20, 'News'); \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full; \
             insert into articles (id, category_id) values (1, 10)",
        )
        .await
        .expect("create + seed tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
    ]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &to_one_def(),
        "public",
        &pk,
        &source_columns,
        &to_one_def().source,
    )
    .await
    .expect("create target table");

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    // Settle article 1 -> category 10 in its own, earlier batch, so the
    // re-point batch below folds to a genuine single `UPDATE` carrying a
    // real old image (`category_id: 10`) rather than an insert whose old
    // image is absent.
    stage_cdc(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"category_id\":10}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let gen_10_before = projection_gen(&client, &projection_table, 10).await;
    let gen_20_before = projection_gen(&client, &projection_table, 20).await;

    // Re-point: a single UPDATE, category_id 10 -> 20.
    client
        .execute("update articles set category_id = 20 where id = 1", &[])
        .await
        .expect("re-point article 1");
    stage_cdc(
        &client,
        "articles",
        "1",
        "update",
        Some("{\"id\":1,\"category_id\":10}"),
        Some("{\"id\":1,\"category_id\":20}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_category_name(&client, 1).await,
        Some("News".to_string()),
        "article 1 must now read category 20's projected name"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 10).await,
        gen_10_before + 1,
        "the OLD parent (10) must be bumped once — it's visible on the folded change's own \
         old image, even though the child no longer points at it"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 20).await,
        gen_20_before + 1,
        "the NEW parent (20) must be bumped once — the from-side row's current join key"
    );
}

// ---------------------------------------------------------------------
// 3. To-many relationships are unaffected: no projection, still live.
// ---------------------------------------------------------------------

/// A to-many relationship gets no settled parent projection at all (Phase 1
/// of this epic is to-one relationship *values* only) — its context still
/// resolves via a live read, so a parent-side (to-side) mutation is visible
/// on the very next forward apply with no reverse recompute needed, unlike
/// the to-one case above.
#[tokio::test]
async fn to_many_relationship_context_has_no_projection_and_stays_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full",
        )
        .await
        .expect("create tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create to-many relationship");

    assert!(
        relationship_projection(&db.pool, relationship.id)
            .await
            .expect("read projection catalog row")
            .is_none(),
        "a to-many relationship must get no settled parent projection"
    );

    let source_columns = columns(&[("id", ValueType::Numeric), ("title", ValueType::Text)]);
    create_definition(
        &db.pool,
        "TRANSFORM article_stats FROM articles SELECT sum(comments.word_count) AS total_words",
        &source_columns,
    )
    .await
    .expect("create to-many enrichment definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    let def = TransformDef {
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
    };
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .batch_execute(
            "insert into articles (id, title) values (1, 'a1'); \
             insert into comments (id, article_id, word_count) values (100, 1, 5)",
        )
        .await
        .expect("seed article and comment");
    stage_cdc(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"title\":\"a1\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let total_words: Option<String> = client
        .query_one(
            "select total_words::text from article_stats where id = 1",
            &[],
        )
        .await
        .expect("read article_stats")
        .get(0);
    assert_eq!(total_words, Some("5".to_string()));

    // A second comment, live, with no reverse recompute involved — this test
    // forces a *forward* re-evaluation of article 1 directly (a fresh
    // image-less recompute), not through the to-side reverse resolver, to
    // isolate the to-many context's own live-read behavior.
    client
        .execute(
            "insert into comments (id, article_id, word_count) values (101, 1, 9)",
            &[],
        )
        .await
        .expect("insert second comment");
    stage_cdc(&client, "articles", "1", "recompute", None, None).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let total_words: Option<String> = client
        .query_one(
            "select total_words::text from article_stats where id = 1",
            &[],
        )
        .await
        .expect("read article_stats")
        .get(0);
    assert_eq!(
        total_words,
        Some("14".to_string()),
        "a to-many context must resolve the new comment immediately, live — no projection, \
         no staleness window"
    );
}
