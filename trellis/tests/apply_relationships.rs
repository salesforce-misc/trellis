//! Integration tests for the relationship *reverse* recompute (issue #30):
//! when a to-side (related) row changes, every from-side row whose enrichment
//! reads it must be re-derived, converging to a Postgres LEFT JOIN (to-one) or
//! correlated aggregate (to-many).
//!
//! Like `apply.rs`, each test builds its own source/to-side tables and target
//! by hand and stages changes directly into the ring. The relationship-enriched
//! transform is created via a valid placeholder definition (so all the catalog
//! plumbing — nodes, edges, target table — is set up the normal way) and its
//! `definition_text` is then rewritten to the relationship form: the validator
//! still rejects a `<rel>.<column>` path at `create_definition` time (that
//! front-door wiring is a later epic issue), but `compute` re-parses the stored
//! text, which is all this issue's staging path needs.
//!
//! Reverse recompute stages *new* `Recompute` rows into the active segment, so
//! a single seal+drain never settles: `drain_to_quiescence` re-seals and drains
//! until nothing is pending.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{
    Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef, ValueType,
};
use trellis::defs::{
    create_definition, create_relationship, create_target_table, render_relationship_select_sql,
    source_primary_key,
};
use trellis::staging::{
    StagedWatermark, TRUNCATE_SENTINEL_KEY, has_pending, retire_drained_segments,
};
use trellis::staging::{apply, claim, fold};

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

/// The ring table backing the currently-active segment (where `append` — and
/// so reverse recompute — writes). The ring is a fixed 4-slot set `seg_0..3`.
async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every bare
/// `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `{DEFAULT_SCHEMA}, public` and never
/// qualifies its own DDL. Mirrors `apply.rs`'s `qualify_fixture_table`: a
/// real CDC producer always stages a fully-qualified `src_table` (issue
/// #76), and `compute`'s forward-propagation lookup
/// (`catalog::transforms_for_source`) now requires that exact qualified
/// identity to match `schema_nodes`/`schema_edges` (issue #74, ADR-0007) —
/// an unqualified `src_table` here silently finds no subscribed definitions
/// rather than erroring. Already-qualified input (containing a `.`) passes
/// through unchanged.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
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

/// Stages one image-bearing (CDC-shaped) change into the active ring
/// segment with an explicit `src_changed`, for tests exercising the
/// reverse-recompute fan-in tie-break (issues #51/#52's multi-hop gap):
/// `min(src_changed)` wins across every to-side change that resolves to the
/// same from-side key in one batch. [`stage_cdc`] above leaves `src_changed`
/// `NULL`, which is fine for tests that only care about *which* keys get
/// staged, not their origin timestamps.
async fn stage_cdc_with_src_changed(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    src_changed: std::time::SystemTime,
) {
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen, \
                 src_changed) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, $7)"
            ),
            &[
                &src_table,
                &key,
                &op,
                &lsn,
                &old_image,
                &new_image,
                &src_changed,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring.
/// Reverse recompute appends fresh `Recompute` rows into the (new) active
/// segment as it drains, so convergence takes more than one seal.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark — no live
    // `Intake` runs in this test, and this file isn't exercising guard (a).
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "reverse_test",
            1,
            "trellis_apply_test",
            &watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        // Free the drained ring slots so repeated seals don't exhaust the ring.
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
// To-one: a bare `category.name` enrichment
// ---------------------------------------------------------------------

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

/// The real definition, created directly through the front door (issue #40's
/// validator accepts a bare to-one `<rel>.<column>` path, so no
/// placeholder-def-then-rewrite hack is needed here any more). `id` is left
/// out — `create_target_table` adds the source's own PK column itself, same
/// as `defs_relationship_frontdoor.rs`'s identically-shaped `to_one_def`.
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

/// The Postgres LEFT JOIN authority: `article_cat` must equal this after every
/// mutation, keyed by id (text) to `category_name` (text).
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

/// The quoted, schema-qualified table name of `relationship_id`'s settled
/// parent projection (issue #129), so it never depends on `search_path` order. Issue #130 (epic #127) moved the
/// forward to-one read off a live to-side lookup onto this projection, but
/// nothing yet advances a projection row's *data* columns when its
/// underlying parent changes — that's #131's job, not built yet. This test
/// exercises the reverse-recompute *staging* mechanism (issue #30, entirely
/// unchanged by #130) across a sequence of parent mutations, so it stands in
/// for #131 itself below, directly mirroring each mutation it makes to
/// `categories` onto the projection — delete every such call once #131 lands
/// and keeps the projection itself in sync.
async fn projection_table_name(pool: &trellis::Pool, relationship_id: i64) -> String {
    trellis::defs::relationship_projection(pool, relationship_id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection")
        .qualified_table()
}

#[tokio::test]
async fn reverse_recompute_to_one_converges_across_related_row_mutations() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // The image-less reverse-recompute path this test exercises (staging a
    // `Recompute` marker per affected from-side row, re-reading the *current*
    // parent row rather than comparing against an old image) itself needs no
    // `REPLICA IDENTITY FULL` on the to-side — a to-one's `to_col` already
    // *is* the primary key, which the DEFAULT identity always carries. But
    // issue #129 (epic #127's settled parent projection) added an
    // unconditional `REPLICA IDENTITY FULL` requirement at
    // `create_relationship` time for every to-one relationship regardless of
    // which apply mechanism ends up consuming it — the projection's own
    // future reverse-applied advance (#131) needs the to-side row's *entire*
    // old image, not just its key — so this table needs it set too, even
    // though nothing in this specific test's own code path reads it yet.
    // Issue #158 extends that same unconditional requirement to the
    // from-side (`articles`): its `category_id` is an ordinary non-PK
    // column, so under the default (PK-only) replica identity a re-pointing
    // `UPDATE` would ship no old image at all.
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full",
        )
        .await
        .expect("create tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // Front door (issue #40): declare the definition directly against the
    // real relationship path — no placeholder-def-then-rewrite hack needed
    // now that the validator accepts it. `create_definition`'s own widen
    // call (issue #129) adds `name` to the projection right here, while
    // `categories` is still empty, matching this test's own step 1 (both
    // articles' categories don't exist yet).
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

    let projection_table = projection_table_name(&db.pool, relationship.id).await;

    // Step 1 — insert two articles pointing at not-yet-existent categories
    // (forward eval: enrichment resolves to NULL, LEFT JOIN no-match).
    client
        .batch_execute(
            "insert into articles (id, category_id, title) values \
             (1, 10, 'a1'), (2, 20, 'a2')",
        )
        .await
        .expect("insert articles");
    stage_cdc(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"category_id\":10,\"title\":\"a1\"}"),
    )
    .await;
    stage_cdc(
        &client,
        "articles",
        "2",
        "insert",
        None,
        Some("{\"id\":2,\"category_id\":20,\"title\":\"a2\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after article inserts"
    );

    // Step 2 — insert category 10: reverse recompute re-derives article 1.
    client
        .execute("insert into categories (id, name) values (10, 'Tech')", &[])
        .await
        .expect("insert category 10");
    client
        .execute(
            &format!(
                "insert into {projection_table} (id, __trellis_gen, __trellis_lsn, name) \
                 values (10, 0, pg_current_wal_lsn(), 'Tech')"
            ),
            &[],
        )
        .await
        .expect("stand in for #131: add the projection row category insert created");
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
        oracle_to_one(&client).await,
        "after category insert"
    );

    // Step 3 — update category 10's name: reverse recompute re-derives article 1.
    client
        .execute(
            "update categories set name = 'Technology' where id = 10",
            &[],
        )
        .await
        .expect("update category name");
    client
        .execute(
            &format!("update {projection_table} set name = 'Technology' where id = 10"),
            &[],
        )
        .await
        .expect("stand in for #131: advance the projection's own name column");
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
        "after category name update"
    );

    // Step 4 — re-parent the category's own key 10 -> 20: article 1 (still
    // pointing at 10) is orphaned; article 2 (pointing at 20) now matches.
    // The OLD key (10) rides in the update's pre-image, the NEW key (20) in the
    // post-image — both from the default replica identity.
    client
        .execute("update categories set id = 20 where id = 10", &[])
        .await
        .expect("re-parent category id");
    client
        .execute(
            &format!("update {projection_table} set id = 20 where id = 10"),
            &[],
        )
        .await
        .expect("stand in for #131: advance the projection's own key column");
    stage_cdc(
        &client,
        "categories",
        "20",
        "update",
        Some("{\"id\":10,\"name\":\"Technology\"}"),
        Some("{\"id\":20,\"name\":\"Technology\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after category re-parent"
    );

    // Step 5 — delete category 20: article 2 falls back to NULL.
    client
        .execute("delete from categories where id = 20", &[])
        .await
        .expect("delete category 20");
    client
        .execute(
            &format!("delete from {projection_table} where id = 20"),
            &[],
        )
        .await
        .expect("stand in for #131: remove the projection row category delete removed");
    stage_cdc(
        &client,
        "categories",
        "20",
        "delete",
        Some("{\"id\":20,\"name\":\"Technology\"}"),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after category delete"
    );
}

// ---------------------------------------------------------------------
// To-many: reverse resolver stages the right from-side recomputes
// ---------------------------------------------------------------------
//
// The aggregate-enrichment eval (`sum(<rel>.<col>)`, #29) can't yet be exercised
// end-to-end here: the parser rejects an aggregate call in a OneToOne target, so
// no `definition_text` produces that AST (a separate grammar/validate wiring
// issue). What #30 owns for to-many — the reverse *resolver* — is cardinality-
// agnostic and testable directly: a to-side (related) row change must stage an
// image-less recompute of exactly the from-side keys whose join key matches,
// which this test asserts by reading the staged `Recompute` rows.

/// The from-side `Recompute` keys the reverse resolver staged into the active
/// ring segment, distinct and sorted.
///
/// `from_table` is accepted bare, for callers' readability, and qualified
/// through [`qualify_fixture_table`] before the probe — issue #267: reverse
/// recompute now stages its `src_table` under the same qualified identity CDC
/// intake uses (`relationship_definitions.from_table` is bare, so `compute`
/// canonicalizes it at the staging boundary), so a bare probe here matches
/// nothing at all rather than reporting the rows that really were staged.
async fn staged_from_side_recomputes(client: &Client, from_table: &str) -> Vec<String> {
    let from_table = qualify_fixture_table(from_table);
    let seg = active_seg_table(client).await;
    let sql = format!("select distinct key from {seg} where src_table = $1 order by key");
    client
        .query(sql.as_str(), &[&from_table])
        .await
        .expect("read staged recomputes")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// Seals the segment holding the just-staged to-side change and drains it; the
/// reverse recompute the resolver stages lands in the NEW active segment, whose
/// from-side keys are returned. Then flushes to quiescence so the next step
/// starts from a clean, retired ring.
async fn reverse_keys_for_to_side_change(
    pool: &trellis::Pool,
    client: &mut Client,
    from_table: &str,
) -> Vec<String> {
    let seg = seal_active_segment(client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        pool,
        seg,
        "reverse_test",
        1,
        "trellis_apply_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .is_some()
    {}
    let keys = staged_from_side_recomputes(client, from_table).await;
    drain_to_quiescence(pool, client).await;
    keys
}

#[tokio::test]
async fn reverse_recompute_to_many_stages_from_side_recomputes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // A to-many's join column (`comments.article_id`) is NOT the to-side primary
    // key, so it is absent from a delete/re-parent's DEFAULT replica-identity
    // pre-image. REPLICA IDENTITY FULL puts it in the old image — the same
    // requirement issue #7 already imposes on aggregate sources whose old image
    // a derivation needs. (A to-*one*'s join column IS the primary key, which is
    // why the to-one test above needs no FULL.) The from-side needs only live
    // rows: the reverse resolver stages recomputes off the relationship graph,
    // independent of whether a from-side transform exists yet.
    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full; \
             insert into articles (id, title) values (1, 'a1'), (2, 'a2')",
        )
        .await
        .expect("create tables and seed from-side rows");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create to-many relationship");

    // Insert: a comment on article 1 -> recompute article 1.
    client
        .execute(
            "insert into comments (id, article_id, word_count) values (100, 1, 5)",
            &[],
        )
        .await
        .expect("insert comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["1".to_string()],
        "comment insert re-derives its article"
    );

    // Update word_count (same article) -> recompute article 1.
    client
        .execute("update comments set word_count = 9 where id = 100", &[])
        .await
        .expect("update comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "update",
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
        Some("{\"id\":100,\"article_id\":1,\"word_count\":9}"),
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["1".to_string()],
        "comment update re-derives its article"
    );

    // Re-parent the comment from article 1 to article 2 -> recompute BOTH. The
    // OLD article_id (1) comes from the FULL pre-image, the NEW (2) from the
    // post-image.
    client
        .execute("update comments set article_id = 2 where id = 100", &[])
        .await
        .expect("re-parent comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "update",
        Some("{\"id\":100,\"article_id\":1,\"word_count\":9}"),
        Some("{\"id\":100,\"article_id\":2,\"word_count\":9}"),
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["1".to_string(), "2".to_string()],
        "re-parent re-derives both old and new article"
    );

    // Delete the comment (now on article 2) -> recompute article 2, join key
    // from the FULL pre-image.
    client
        .execute("delete from comments where id = 100", &[])
        .await
        .expect("delete comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "delete",
        Some("{\"id\":100,\"article_id\":2,\"word_count\":9}"),
        None,
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["2".to_string()],
        "comment delete re-derives its article"
    );
}

// ---------------------------------------------------------------------
// Issue #79: two to-many relationships sharing one from_table must dedupe
// ---------------------------------------------------------------------

/// The number of `Recompute` rows staged for `from_table`/`key` in the active
/// segment — deliberately *not* `distinct`, unlike [`staged_from_side_recomputes`]
/// above: issue #79 is exactly a case where the same key is staged more than
/// once, which a `distinct` read would silently hide.
/// `from_table` is qualified through [`qualify_fixture_table`] for the same
/// reason [`staged_from_side_recomputes`] does it (issue #267).
async fn staged_recompute_count(client: &Client, from_table: &str, key: &str) -> i64 {
    let from_table = qualify_fixture_table(from_table);
    let seg = active_seg_table(client).await;
    let sql = format!("select count(*) from {seg} where src_table = $1 and key = $2");
    client
        .query_one(sql.as_str(), &[&from_table, &key])
        .await
        .expect("count staged recomputes")
        .get(0)
}

#[tokio::test]
async fn reverse_recompute_dedupes_across_relationships_sharing_from_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Two independent to-many relationships into the same from-table
    // (`articles`), mirroring issue #79's `posts`/`comments` -> `authors`
    // shape: a single batch that touches article 1 through *both* relationships
    // must stage exactly one recompute for article 1, not one per relationship.
    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             create table likes (id integer primary key, article_id integer); \
             alter table comments replica identity full; \
             alter table likes replica identity full; \
             insert into articles (id, title) values (1, 'a1')",
        )
        .await
        .expect("create tables and seed from-side rows");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create comments relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP likes FROM articles.id TO likes.article_id",
    )
    .await
    .expect("create likes relationship");

    client
        .execute(
            "insert into comments (id, article_id, word_count) values (100, 1, 5)",
            &[],
        )
        .await
        .expect("insert comment");
    client
        .execute("insert into likes (id, article_id) values (200, 1)", &[])
        .await
        .expect("insert like");

    // Both to-side changes land in the *same* batch (one seal drains both),
    // so `compute`'s single call sees inbound relationships from two
    // different source tables (`comments`, `likes`) that share `articles`
    // as their common from_table.
    stage_cdc(
        &client,
        "comments",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
    )
    .await;
    stage_cdc(
        &client,
        "likes",
        "200",
        "insert",
        None,
        Some("{\"id\":200,\"article_id\":1}"),
    )
    .await;

    let seg = seal_active_segment(&mut client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        &db.pool,
        seg,
        "reverse_test",
        1,
        "trellis_apply_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .is_some()
    {}

    assert_eq!(
        staged_recompute_count(&client, "articles", "1").await,
        1,
        "article 1 must be staged exactly once even though two relationships \
         (comments, likes) both touched it in this batch"
    );

    drain_to_quiescence(&db.pool, &mut client).await;
}

/// Issue #173 phase 4, closing a gap `docs/relationship-propagation.md`
/// named but left open: "No test combines a `TRUNCATE` with two
/// shared-from-table relationships; the dedupe is structural (the container
/// types)." That doc entry classified the cell "Handled by reuse" on
/// code-reading alone — the `truncated` loop pushes into the very same
/// `reverse_recomputes` accumulator [`reverse_recompute_dedupes_across_relationships_sharing_from_table`]
/// above pins for two ordinary row-driven changes — but nothing had actually
/// driven a `TRUNCATE` through it. This test is that missing assertion, not
/// a new mechanism: `comments` is `TRUNCATE`d (a key-less
/// `ReverseTrigger::WholeKeyspace` sentinel, resolved via
/// `catalog::relationships_to_table`) in the same batch as an ordinary
/// `likes` insert (a row-keyed `ReverseTrigger::Keys` resolution) — both
/// paths resolve to the same `(articles, "1")` from-side key, and the
/// accumulator must still dedupe them to one `Recompute`, exactly as it does
/// for two row-driven changes.
#[tokio::test]
async fn reverse_recompute_dedupes_a_truncate_against_a_relationship_sharing_the_same_from_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             create table likes (id integer primary key, article_id integer); \
             alter table comments replica identity full; \
             alter table likes replica identity full; \
             insert into articles (id, title) values (1, 'a1')",
        )
        .await
        .expect("create tables and seed from-side rows");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create comments relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP likes FROM articles.id TO likes.article_id",
    )
    .await
    .expect("create likes relationship");

    client
        .execute(
            "insert into comments (id, article_id, word_count) values (100, 1, 5)",
            &[],
        )
        .await
        .expect("insert comment");
    client
        .execute("insert into likes (id, article_id) values (200, 1)", &[])
        .await
        .expect("insert like");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Both changes land in the same batch: `comments` gets a `TRUNCATE`
    // sentinel (`ReverseTrigger::WholeKeyspace` resolves it to every
    // non-NULL `article_id`, here just article 1 — see `insert_truncate_row`
    // in `apply.rs` for the same staged-sentinel-without-an-actual-SQL-
    // `TRUNCATE` convention) while a fresh `likes` row also points at
    // article 1 through the *other* relationship (an ordinary
    // `ReverseTrigger::Keys` resolution). If the two triggers' resolutions
    // were staged through independent accumulators rather than the one
    // shared `reverse_recomputes` map, article 1 would be staged twice.
    stage_cdc(
        &client,
        "comments",
        TRUNCATE_SENTINEL_KEY,
        "truncate",
        None,
        None,
    )
    .await;
    client
        .execute("insert into likes (id, article_id) values (201, 1)", &[])
        .await
        .expect("insert second like");
    stage_cdc(
        &client,
        "likes",
        "201",
        "insert",
        None,
        Some("{\"id\":201,\"article_id\":1}"),
    )
    .await;

    let seg = seal_active_segment(&mut client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        &db.pool,
        seg,
        "reverse_test",
        1,
        "trellis_apply_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .is_some()
    {}

    assert_eq!(
        staged_recompute_count(&client, "articles", "1").await,
        1,
        "article 1 must be staged exactly once even though it was reached both by \
         truncating comments (WholeKeyspace) and by a fresh likes row (Keys) in the \
         same batch"
    );

    drain_to_quiescence(&db.pool, &mut client).await;
}

// ---------------------------------------------------------------------
// Issues #51/#52's multi-hop gap: the fan-in tie-break on `src_changed`
// ---------------------------------------------------------------------

/// The `src_changed` column of the (sole, deduped) `Recompute` row staged
/// for `(from_table, key)` in the active segment.
async fn staged_recompute_src_changed(
    client: &Client,
    from_table: &str,
    key: &str,
) -> Option<std::time::SystemTime> {
    // Qualified for the same reason [`staged_from_side_recomputes`] does it
    // (issue #267) — and this one is a `query_one`, so a bare probe would fail
    // outright on "no rows" rather than quietly return a zero count.
    let from_table = qualify_fixture_table(from_table);
    let seg = active_seg_table(client).await;
    let sql = format!(
        "select src_changed from {seg} where src_table = $1 and key = $2 and op = 'recompute'"
    );
    client
        .query_one(sql.as_str(), &[&from_table, &key])
        .await
        .expect("read staged recompute's src_changed")
        .get(0)
}

/// Two to-many to-side changes (comments on the same article) land in one
/// batch with two different `src_changed` origins. Reverse recompute
/// resolves both to the same from-side key (article 1) and must dedupe them
/// into one `Recompute` row (issue #79) carrying the **earlier** of the two
/// origins (issues #51/#52's multi-hop gap: `min(src_changed)` wins on
/// fan-in — deliberately the opposite merge direction from `hop_gen`'s own
/// fan-in tie-break, which takes the max; see `staging::apply`'s
/// `earliest_src_changed` doc comment).
#[tokio::test]
async fn reverse_recompute_fan_in_keeps_the_earliest_src_changed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full; \
             insert into articles (id, title) values (1, 'a1')",
        )
        .await
        .expect("create tables and seed from-side rows");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create to-many relationship");

    client
        .execute(
            "insert into comments (id, article_id, word_count) values (100, 1, 5), (101, 1, 9)",
            &[],
        )
        .await
        .expect("insert two comments on the same article");

    let now = std::time::SystemTime::now();
    let earlier = now - std::time::Duration::from_secs(120);
    let later = now - std::time::Duration::from_secs(5);

    // Staged deliberately out of chronological order — the tie-break must
    // pick the earlier origin regardless of which row this batch happens to
    // process first.
    stage_cdc_with_src_changed(
        &client,
        "comments",
        "101",
        "insert",
        None,
        Some("{\"id\":101,\"article_id\":1,\"word_count\":9}"),
        later,
    )
    .await;
    stage_cdc_with_src_changed(
        &client,
        "comments",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
        earlier,
    )
    .await;

    let seg = seal_active_segment(&mut client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        &db.pool,
        seg,
        "reverse_test",
        1,
        "trellis_apply_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .is_some()
    {}

    assert_eq!(
        staged_recompute_count(&client, "articles", "1").await,
        1,
        "both comments resolve to the same from-side key and must dedupe to one recompute"
    );

    let observed = staged_recompute_src_changed(&client, "articles", "1").await;
    // Postgres's `timestamptz` column only has microsecond precision, so
    // compare via `duration_since(UNIX_EPOCH)` truncated the same way rather
    // than requiring exact `SystemTime` equality.
    let expected_micros = earlier
        .duration_since(std::time::UNIX_EPOCH)
        .expect("earlier is after the epoch")
        .as_micros();
    let observed_micros = observed
        .expect("the staged recompute must carry a src_changed")
        .duration_since(std::time::UNIX_EPOCH)
        .expect("observed is after the epoch")
        .as_micros();
    assert_eq!(
        observed_micros, expected_micros,
        "the fan-in tie-break must keep the earlier (min) of the two contributing origins, \
         not the later one and not the order they happened to be staged/processed in"
    );

    drain_to_quiescence(&db.pool, &mut client).await;
}

/// Issue #344's fallback path: a batch computed from a source row that
/// changed before its Phase 3 ran can't be re-evaluated there when its
/// definition reads a relationship (the related rows are only loaded in
/// Phase 2). It must neither write its stale value over the newer batch's
/// nor drop the key: it is re-staged as a recompute instead.
#[tokio::test]
async fn a_stale_relationship_enriched_write_is_restaged_rather_than_applied() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full; \
             insert into categories (id, name) values (10, 'Tech'), (20, 'Sci')",
        )
        .await
        .expect("create tables");
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
    .expect("create definition");
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
    drain_to_quiescence(&db.pool, &mut client).await;
    let projection_table = projection_table_name(&db.pool, relationship.id).await;
    client
        .batch_execute(&format!(
            "insert into {projection_table} (id, __trellis_gen, __trellis_lsn, name) values \
             (10, 0, pg_current_wal_lsn(), 'Tech'), (20, 0, pg_current_wal_lsn(), 'Sci') \
             on conflict (id) do update set name = excluded.name"
        ))
        .await
        .expect("settle the parent projection");

    // Batch 1: article 1 is inserted pointing at 'Tech', and computed but
    // not applied.
    client
        .execute(
            "insert into articles (id, category_id, title) values (1, 10, 'a1')",
            &[],
        )
        .await
        .expect("insert article");
    stage_cdc(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some(r#"{"id":"1","category_id":"10","title":"a1"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg1, "slow_worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg1, "slow_worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg1, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    let stale_plan = apply::compute(&db.pool, &folded).await.expect("compute");

    // Batch 2: article 1 moves to 'Sci' and drains completely first.
    client
        .execute("update articles set category_id = 20 where id = 1", &[])
        .await
        .expect("re-point article");
    stage_cdc(
        &client,
        "articles",
        "1",
        "update",
        Some(r#"{"id":"1","category_id":"10","title":"a1"}"#),
        Some(r#"{"id":"1","category_id":"20","title":"a1"}"#),
    )
    .await;
    // Not `drain_to_quiescence`: batch 1 stays pending until its Phase 3.
    let seg2 = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg2,
        "fast_worker",
        1,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain batch 2")
    .expect("batch 2 must drain");
    let sci = HashMap::from([("1".to_string(), Some("Sci".to_string()))]);
    assert_eq!(target_to_one(&client).await, sci);

    // Batch 1's Phase 3 runs last.
    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg1,
        "slow_worker",
        &stale_plan,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply batch 1");
    txn.commit().await.expect("commit phase 3");

    assert_eq!(
        target_to_one(&client).await,
        sci,
        "the stale batch must not overwrite the newer value"
    );
    assert_eq!(
        staged_recompute_count(&client, "articles", "1").await,
        1,
        "the stale batch must re-stage its key rather than drop it"
    );
    retire_drained_segments(&mut client)
        .await
        .expect("free the ring slots batches 1 and 2 used");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(target_to_one(&client).await, sci);
    assert_eq!(target_to_one(&client).await, oracle_to_one(&client).await);
}
