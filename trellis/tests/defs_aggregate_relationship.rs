//! Front-door integration tests for issue #94: aggregating a **to-one**
//! relationship path inside a `GROUP BY` definition
//! (`TRANSFORM tag_totals FROM post_tags GROUP BY tag SELECT COUNT(*) AS
//! post_count, SUM(post.word_count) AS total_words`).
//!
//! Everything goes through the public front door — `create_relationship` +
//! `install_definition` — rather than the placeholder-def/`definition_text`
//! rewrite hack older tests used, so the DDL, direct backfill, and incremental
//! apply paths are all exercised exactly as a real caller would hit them.
//!
//! The positive cases converge the real staging pipeline and compare the
//! target against Postgres's own `LEFT JOIN … GROUP BY`
//! (`render_aggregate_relationship_select_sql`), in both propagation
//! directions: a from-side (`post_tags`) row changing, and a to-side (`posts`)
//! row changing, the latter travelling through ADR-0006's reverse recompute.
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
    Expr, FieldDef, GroupByKey, KeySpace, Predicate, RelationshipDef, TransformDef, ValueType,
};
use trellis::defs::{
    CatalogError, ValidationError, create_relationship, install_definition,
    relationship_projection, render_aggregate_relationship_select_sql,
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

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every bare
/// `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `{DEFAULT_SCHEMA}, public` and never
/// qualifies its own DDL. Mirrors `apply.rs`'s `qualify_fixture_table`: a
/// real CDC producer always stages a fully-qualified `src_table` (issue
/// #76), and `compute`'s forward-propagation lookup
/// (`catalog::transforms_for_source`) now requires that exact qualified
/// identity to match `schema_nodes`/`schema_edges` (issue #74, ADR-0007).
/// Already-qualified input (containing a `.`) passes through unchanged.
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
            "agg_rel_test",
            1,
            "trellis_agg_rel_test",
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

fn post_tags_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("post", ValueType::Numeric),
        ("tag", ValueType::Text),
    ])
}

const TAG_TOTALS: &str = "TRANSFORM tag_totals FROM post_tags GROUP BY tag \
     SELECT COUNT(*) AS post_count, SUM(post.word_count) AS total_words";

/// Issue #94's exact schema. `post_tags` needs `REPLICA IDENTITY FULL` because
/// it is an *aggregate* source (the delta/recompute path needs the old image to
/// locate the group a changed row is leaving) — unrelated to the relationship.
/// `posts` needs it too, as of issue #129: it's the to-side of a to-one
/// relationship, whose settled parent projection now requires `REPLICA
/// IDENTITY FULL` unconditionally (`assert_replica_identity_supports_projection`),
/// independent of and in addition to the aggregate-source reason above.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table posts (id integer primary key, word_count integer); \
             create table post_tags (id integer primary key, post integer, tag text); \
             alter table post_tags replica identity full; \
             alter table posts replica identity full; \
             create index on post_tags (post); \
             insert into posts (id, word_count) values (1, 100), (2, 250), (3, null); \
             insert into post_tags (id, post, tag) values \
               (10, 1, 'rust'), (11, 2, 'rust'), (12, 1, 'db'), \
               (13, 999, 'rust'), (14, 3, 'db')",
        )
        .await
        .expect("create + seed issue #94's schema");
}

/// The oracle's version of the definition: identical, plus the grouping column
/// projected so the wrapper can key rows by it (the target table carries `tag`
/// as its primary key, added by the aggregate DDL).
fn oracle_def() -> TransformDef {
    TransformDef {
        target: "tag_totals".to_string(),
        source: "post_tags".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec![GroupByKey::Column("tag".to_string())],
        },
        fields: vec![
            FieldDef {
                name: "tag".to_string(),
                expr: Expr::Column("tag".to_string()),
            },
            FieldDef {
                name: "post_count".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            },
            FieldDef {
                name: "total_words".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::RelationshipPath {
                        rel: "post".to_string(),
                        column: "word_count".to_string(),
                    }],
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn post_rel() -> HashMap<String, RelationshipDef> {
    HashMap::from([(
        "post".to_string(),
        RelationshipDef {
            name: "post".to_string(),
            from_table: "post_tags".to_string(),
            from_col: "post".to_string(),
            to_table: "posts".to_string(),
            to_col: "id".to_string(),
        },
    )])
}

type Totals = HashMap<String, (Option<String>, Option<String>)>;

async fn oracle_totals(client: &Client) -> Totals {
    let base = render_aggregate_relationship_select_sql(&oracle_def(), &post_rel());
    let sql = format!("select tag, post_count::text, total_words::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query aggregate relationship oracle")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

async fn target_totals(client: &Client) -> Totals {
    client
        .query(
            "select tag, post_count::text, total_words::text from tag_totals",
            &[],
        )
        .await
        .expect("read tag_totals")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

async fn table_exists(client: &Client, table: &str) -> bool {
    client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{table}")],
        )
        .await
        .expect("to_regclass probe")
        .get(0)
}

// ---------------------------------------------------------------------
// The issue's shape, end to end.
// ---------------------------------------------------------------------

/// Issue #94's headline case: a to-one relationship path folded by `SUM` in a
/// `GROUP BY` definition installs, and its backfill over pre-existing data
/// matches Postgres's own `LEFT JOIN … GROUP BY`.
///
/// The seed data deliberately covers ADR-0006's nullability rules inside the
/// fold: tag `rust` includes a row whose FK (`post = 999`) resolves to no post
/// at all, and tag `db` includes a row whose post exists but has a `NULL`
/// `word_count`. Both contribute `NULL` to `SUM` — skipped, per Postgres's
/// aggregate semantics — rather than erroring or nulling out the whole group,
/// while still counting toward `COUNT(*)`.
#[tokio::test]
async fn aggregate_over_a_to_one_relationship_backfills_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");

    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");

    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(totals, oracle_totals(&client).await, "after backfill");
    // Pinned explicitly, not just against the oracle, so a change that broke
    // *both* identically would still be caught: `rust` = 100 + 250 + (no such
    // post -> NULL); `db` = 100 + (post 3's NULL word_count -> NULL).
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("350".to_string())))
    );
    assert_eq!(
        totals.get("db"),
        Some(&(Some("2".to_string()), Some("100".to_string())))
    );
}

/// Forward propagation: a new `post_tags` row folds its related post's
/// `word_count` into the right group.
#[tokio::test]
async fn inserting_a_from_side_row_updates_its_groups_total() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute(
            "insert into post_tags (id, post, tag) values (15, 2, 'db')",
            &[],
        )
        .await
        .expect("insert a new post_tag");
    stage_cdc(
        &client,
        "post_tags",
        "15",
        "insert",
        None,
        Some("{\"id\":15,\"post\":2,\"tag\":\"db\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals,
        oracle_totals(&client).await,
        "after inserting a from-side row"
    );
    // `db` gains post 2's 250 words and one more row.
    assert_eq!(
        totals.get("db"),
        Some(&(Some("3".to_string()), Some("350".to_string())))
    );
}

/// Issue #136 (epic #127): an ordinary from-side insert into a
/// relationship-reading aggregate resolves the relationship's value from the
/// *settled parent projection* (issue #130's mechanism), never a live read
/// of the to-side table — mirroring
/// `apply_relationship_forward.rs`'s `forward_to_one_resolves_from_the_projection_not_live_parent_state`
/// for the analogous `KeySpace::OneToOne` case, but for a `KeySpace::Aggregate`
/// field wrapped in `SUM`. Before #136, this went through `accumulate_changes`'s
/// now-removed `force_every_group`, which forced a **live** `LEFT JOIN`
/// recompute for the whole touched group — so this exact scenario (a live
/// rename with no reverse recompute in between) would have picked up the
/// *new* live value immediately. Proved by manufacturing the same
/// projection/live desync `apply_relationship_forward.rs` uses: renaming
/// post 1's `word_count` live, with no CDC staged and no reverse recompute
/// run, then inserting a brand-new `post_tags` row pointing at it.
#[tokio::test]
async fn inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    // `install_definition`'s widen (#129) catches post 1's word_count into
    // the projection right here, while 100 is still its only-ever value.
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = relationship_projection(&db.pool, relationship.id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection")
        .projection_table;
    let projection_word_count: Option<i32> = client
        .query_one(
            &format!("select word_count from {projection_table} where id = 1"),
            &[],
        )
        .await
        .expect("read projected word_count")
        .get(0);
    assert_eq!(
        projection_word_count,
        Some(100),
        "sanity: the projection settled on post 1's original word_count"
    );

    // Live-only mutation: no CDC staged, no reverse recompute, nothing
    // advances the projection.
    client
        .execute("update posts set word_count = 999 where id = 1", &[])
        .await
        .expect("rename the live post's word_count without touching the projection");

    // A brand-new post_tags row, forward-applied for the first time,
    // referencing the now-desynced post.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (15, 1, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tag");
    stage_cdc(
        &client,
        "post_tags",
        "15",
        "insert",
        None,
        Some("{\"id\":15,\"post\":1,\"tag\":\"rust\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("4".to_string()), Some("450".to_string()))),
        "the new row's contribution must come from the settled projection \
         (still 100: 100 via row 10 + 250 via row 11 + null via row 13 + 100 \
         via the new row 15), not the live post row (renamed to 999 after \
         the projection settled)"
    );
}

/// Reverse propagation (ADR-0006's to-side direction, issue #30's mechanism):
/// changing the *related* `posts` row's `word_count` must re-derive every group
/// whose members read it — here post 1 is referenced by both the `rust` and
/// `db` groups, so both move.
#[tokio::test]
async fn updating_a_to_side_row_updates_every_dependent_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals,
        oracle_totals(&client).await,
        "after updating a to-side row"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string())))
    );
    assert_eq!(
        totals.get("db"),
        Some(&(Some("2".to_string()), Some("400".to_string())))
    );
}

/// Issue #136 (epic #127): a from-side row re-pointed from one parent to
/// another *within a single already-folded change* (a genuine single
/// `UPDATE`, not an insert-then-repoint the fold would erase) correctly
/// subtracts its contribution under the *old* parent and adds it under the
/// *new* one, in one change — mirroring issue #131's reverse-side test for
/// the analogous case (`apply_relationship_forward.rs`'s
/// `forward_apply_re_point_bumps_gen_for_both_old_and_new_parent`), but
/// proving the forward *aggregate delta's own value*, not just the
/// projection's `gen` bookkeeping.
///
/// Row 10 (`post = 1`, tag `rust`) is re-pointed to `post = 2` — the same
/// `GROUP BY` group (`tag` is unchanged, so this is a same-group in-place
/// update, not a grain migration), but `accumulate_changes` must still
/// resolve the row's relationship read *twice*, once per side, off each
/// side's own `post` value (`row_contribution` under `old_row`'s `post = 1`
/// vs. `new_row`'s `post = 2`) — exactly the case a naive single-resolution
/// implementation (resolving once off, say, the new row only) would get
/// wrong.
#[tokio::test]
async fn a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals_before = target_totals(&client).await;
    assert_eq!(
        totals_before.get("rust"),
        Some(&(Some("3".to_string()), Some("350".to_string()))),
        "sanity: 'rust' starts at 100 (row 10, post 1) + 250 (row 11, post 2) \
         + null (row 13, post 999)"
    );

    // Re-point row 10 from post 1 (word_count 100) to post 2 (word_count
    // 250) — same group (`tag` stays 'rust'), one folded UPDATE carrying
    // both a real old image and a real new image.
    client
        .execute("update post_tags set post = 2 where id = 10", &[])
        .await
        .expect("re-point row 10");
    stage_cdc(
        &client,
        "post_tags",
        "10",
        "update",
        Some("{\"id\":10,\"post\":1,\"tag\":\"rust\"}"),
        Some("{\"id\":10,\"post\":2,\"tag\":\"rust\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals,
        oracle_totals(&client).await,
        "after a same-group from-side re-point"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("500".to_string()))),
        "row 10 must subtract its OLD contribution (100, via post 1) and add \
         its NEW one (250, via post 2) in one delta — 250 (row 10, now post \
         2) + 250 (row 11, post 2) + null (row 13, post 999) = 500, not 350 \
         (no-op, as if the repoint were never resolved) or some other value \
         a single-sided resolution would produce"
    );
}

/// Issue #173 phase 4, closing Known Gap 1 from
/// `docs/relationship-propagation.md`'s obligation table: "no test drives an
/// aggregate delta-path re-point across the *nonexistent*-parent boundary."
/// The test above proves the delta path resolves a from-side re-point's old
/// and new sides independently when *both* resolve to a real parent; this is
/// its missing sibling — the new side resolves to nothing at all.
///
/// Byte-for-byte the same shape as
/// [`a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions`],
/// except row 10 is re-pointed from post 1 (a real parent, `word_count`
/// 100) to post 999 — the fixture's own standing nonexistent-parent id
/// (already used by row 13's `rust`-tagged, permanently-unmatched row). The
/// old side must still subtract its real contribution; the new side must
/// resolve to `NULL` and contribute nothing, exactly as `to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears`
/// (`defs_relationship_nullability.rs`) pins for the *forward-read* path —
/// this is that same "resolves to nothing" outcome, but reached by
/// `accumulate_changes`'s own old/new resolution rather than the reverse
/// path re-deriving the group from scratch.
#[tokio::test]
async fn a_from_side_re_point_to_a_nonexistent_parent_subtracts_the_old_contribution() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals_before = target_totals(&client).await;
    assert_eq!(
        totals_before.get("rust"),
        Some(&(Some("3".to_string()), Some("350".to_string()))),
        "sanity: 'rust' starts at 100 (row 10, post 1) + 250 (row 11, post 2) \
         + null (row 13, post 999)"
    );

    // Re-point row 10 from post 1 (word_count 100, a real parent) to post
    // 999 (the fixture's standing nonexistent parent) — same group (`tag`
    // stays 'rust'), one folded UPDATE carrying both a real old image and a
    // new image whose FK resolves to nothing.
    client
        .execute("update post_tags set post = 999 where id = 10", &[])
        .await
        .expect("re-point row 10 to a nonexistent parent");
    stage_cdc(
        &client,
        "post_tags",
        "10",
        "update",
        Some("{\"id\":10,\"post\":1,\"tag\":\"rust\"}"),
        Some("{\"id\":10,\"post\":999,\"tag\":\"rust\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals,
        oracle_totals(&client).await,
        "after a from-side re-point to a nonexistent parent"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("250".to_string()))),
        "row 10 must subtract its OLD contribution (100, via post 1) and add \
         nothing for its NEW side (post 999 does not exist) — 250 (row 11, \
         post 2) alone = 250, not 350 (no-op) or NULL (as if the whole group \
         lost its match). post_count stays 3: row 10 still exists in \
         post_tags, an unmatched relationship target only nulls the \
         aggregated value, never the row's own group membership"
    );
}

/// Issue #136 review follow-up: `AVG` over a relationship-read column
/// (`AVG(post.word_count)`) — code-reading confirmed `contribution_def`'s
/// `AVG`-as-`SUM` rewrite only touches a field's own top-level
/// `FunctionCall` and runs *after* `build_forward_relationship_shape`'s
/// `RelationshipPath`-to-synthetic-`Column` substitution, so the two
/// rewrites should compose regardless of order — but nothing exercised
/// `AVG` combined with a relationship read before this test (every existing
/// `AVG` test is relationship-free, and #94's own aggregate-relationship
/// tests only cover `SUM`/`COUNT`). Proves both backfill and the ordinary
/// forward delta path maintain `AVG`'s hidden sum/count partials correctly
/// when the averaged value itself comes from a relationship, including the
/// `NULL`-skipping rule (`post = 999` resolves to no post at all; post 3's
/// `word_count` is a genuine SQL `NULL` — both must be excluded from the
/// average, not treated as zero), by comparing against Postgres's own
/// `LEFT JOIN … GROUP BY … AVG(...)` oracle rather than a hardcoded
/// (scale-sensitive) literal.
#[tokio::test]
async fn avg_over_a_relationship_read_column_maintains_through_backfill_and_forward_insert() {
    fn oracle_avg_def() -> TransformDef {
        TransformDef {
            target: "tag_avg".to_string(),
            source: "post_tags".to_string(),
            key_space: KeySpace::Aggregate {
                group_by: vec![GroupByKey::Column("tag".to_string())],
            },
            fields: vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: Expr::Column("tag".to_string()),
                },
                FieldDef {
                    name: "avg_words".to_string(),
                    expr: Expr::FunctionCall {
                        name: "AVG".to_string(),
                        args: vec![Expr::RelationshipPath {
                            rel: "post".to_string(),
                            column: "word_count".to_string(),
                        }],
                    },
                },
            ],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    async fn oracle_avg_totals(client: &Client) -> HashMap<String, Option<String>> {
        let base = render_aggregate_relationship_select_sql(&oracle_avg_def(), &post_rel());
        let sql = format!("select tag, avg_words::text from ({base}) t");
        client
            .query(sql.as_str(), &[])
            .await
            .expect("query avg-over-relationship oracle")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    async fn target_avg_totals(client: &Client) -> HashMap<String, Option<String>> {
        client
            .query("select tag, avg_words::text from tag_avg", &[])
            .await
            .expect("read tag_avg")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(
        &db.pool,
        "TRANSFORM tag_avg FROM post_tags GROUP BY tag SELECT AVG(post.word_count) AS avg_words",
        &post_tags_columns(),
        "public",
    )
    .await
    .expect("install the AVG-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_avg_totals(&client).await,
        oracle_avg_totals(&client).await,
        "after backfill"
    );

    // Forward delta: a new post_tags row pointing at post 2 (250 words)
    // joins 'db' (previously just post 1's 100, with post 3's NULL
    // skipped) — post-#136 this resolves via the settled projection and
    // must still fold into AVG's running sum/count partials correctly.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (15, 2, 'db')",
            &[],
        )
        .await
        .expect("insert a new post_tag");
    stage_cdc(
        &client,
        "post_tags",
        "15",
        "insert",
        None,
        Some("{\"id\":15,\"post\":2,\"tag\":\"db\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_avg_totals(&client).await;
    assert_eq!(
        totals,
        oracle_avg_totals(&client).await,
        "after a forward delta touching an AVG field's relationship read"
    );
    assert_eq!(
        totals.get("db").cloned().flatten(),
        Some("175.0000000000000000".to_string()),
        "'db' must now average post 1 (100) and post 2 (250) = 175 (at \
         Postgres's own avg() scale), folded in via the forward delta, not \
         left at the backfilled 100"
    );
}

/// Issue #136 review follow-up: two distinct relationships referenced by the
/// same aggregate definition, both happening to read a to-side column with
/// the same *name* (`author.score`/`editor.score`, both relationships
/// pointing at `users`) — code-reading confirmed
/// `forward_relationship_synthetic_column(rel, column)`'s namespacing by
/// *both* relationship name and column avoids the collision a naming scheme
/// keyed on column alone would hit (two different relationships' resolved
/// values overwriting the same synthetic column on the augmented row), but
/// no test actually constructed two relationships sharing a to-side column
/// name before this one. Proves both totals resolve correctly, independently
/// of each other, through backfill and an ordinary forward delta — the SQL
/// oracle comparison would itself corrupt silently (one field's value
/// leaking into the other) if the collision this test targets were ever
/// reintroduced.
#[tokio::test]
async fn two_relationships_sharing_a_to_side_column_name_resolve_independently() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table users (id integer primary key, score numeric); \
             alter table users replica identity full; \
             insert into users (id, score) values (1, 10), (2, 20), (3, 30); \
             create table reviews \
               (id integer primary key, article_id integer, author_id integer, \
                editor_id integer); \
             alter table reviews replica identity full; \
             insert into reviews (id, article_id, author_id, editor_id) values \
               (1, 100, 1, 2), (2, 100, 2, 3)",
        )
        .await
        .expect("create + seed two-relationship schema");

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM reviews.author_id TO users.id",
    )
    .await
    .expect("create the author relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP editor FROM reviews.editor_id TO users.id",
    )
    .await
    .expect("create the editor relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("article_id", ValueType::Numeric),
        ("author_id", ValueType::Numeric),
        ("editor_id", ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM review_totals FROM reviews GROUP BY article_id \
         SELECT COUNT(*) AS review_count, SUM(author.score) AS author_total, \
         SUM(editor.score) AS editor_total",
        &source_columns,
        "public",
    )
    .await
    .expect("install the two-relationship aggregate definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    async fn review_totals(
        client: &Client,
    ) -> HashMap<String, (Option<String>, Option<String>, Option<String>)> {
        client
            .query(
                "select article_id::text, review_count::text, author_total::text, \
                 editor_total::text from review_totals",
                &[],
            )
            .await
            .expect("read review_totals")
            .into_iter()
            .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3))))
            .collect()
    }

    // After backfill: author scores 10 (user 1, row 1) + 20 (user 2, row 2)
    // = 30; editor scores 20 (user 2, row 1) + 30 (user 3, row 2) = 50 —
    // each field must resolve from its *own* relationship, never the
    // other's.
    let totals = review_totals(&client).await;
    assert_eq!(
        totals.get("100"),
        Some(&(
            Some("2".to_string()),
            Some("30".to_string()),
            Some("50".to_string())
        )),
        "after backfill: author_total (30) and editor_total (50) must not \
         collide or leak into each other despite both relationships reading \
         a to-side column named 'score'"
    );

    // Forward delta: a new review row (author 3 -> score 30, editor 1 ->
    // score 10) folds into the same group.
    client
        .execute(
            "insert into reviews (id, article_id, author_id, editor_id) \
             values (3, 100, 3, 1)",
            &[],
        )
        .await
        .expect("insert a new review");
    stage_cdc(
        &client,
        "reviews",
        "3",
        "insert",
        None,
        Some("{\"id\":3,\"article_id\":100,\"author_id\":3,\"editor_id\":1}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        review_totals(&client).await.get("100"),
        Some(&(
            Some("3".to_string()),
            Some("60".to_string()),
            Some("60".to_string())
        )),
        "after the forward delta: author_total = 30 + 30 (user 3) = 60, \
         editor_total = 50 + 10 (user 1) = 60 — each still resolved \
         independently via its own synthetic column, not aliased onto the \
         other's"
    );
}

// ---------------------------------------------------------------------
// Whole-branch review regression (epic #127): an invertible field mixed with
// a relationship-reading `RecomputeOnly` field on the same target, driven
// through an *ordinary* forward delta (never image-less, so never
// `force_full_recompute`).
//
// Before issue #136, every group of a relationship-reading aggregate target
// was forced onto `apply_forced_groups_bulk` (`force_every_group`), which
// already joined the to-side table correctly — so `upsert_group`'s
// `probe_field_value` and `apply_delta_groups_bulk`'s
// `probe_recompute_fields_bulk` could never actually be reached for such a
// target, regardless of how many groups one batch touched. #136 deleted that
// routing: `SUM`/`AVG`/`COUNT` fields (even ones reading a relationship path)
// now fold incrementally through the ordinary ("delta") path, but a `MIN`/
// `MAX` field on that same target is still `RecomputeOnly` and gets probed
// live — through whichever of those two functions `apply_aggregate_target`
// picks based on how many groups the batch touched (`upsert_group` for
// exactly one, `apply_delta_groups_bulk` for more than one). Both functions
// rendered the `RecomputeOnly` expression through the relationship-unaware
// `oracle::render_expr_sql` and built no `JOIN` for it at all, so either path
// panicked (`render_expr_sql called on an unresolved relationship path`) the
// moment a real batch reached it — `apply_delta_groups_bulk`'s corner is what
// the generative fuzz suite caught; `upsert_group`'s single-group corner is
// the same defect, uncovered by close reading during this fix.
//
// `MIXED_TOTALS` gives every group both kinds at once: `SUM(id)` (`id` is a
// plain, non-relationship column — invertible, folds via the delta path
// unconditionally) and `MIN(post.word_count)` (a to-one relationship read —
// `RecomputeOnly`, so it's the one that must probe live and correctly).

const MIXED_TOTALS: &str = "TRANSFORM tag_mixed FROM post_tags GROUP BY tag \
     SELECT SUM(id) AS id_sum, MIN(post.word_count) AS min_word_count";

fn mixed_oracle_def() -> TransformDef {
    TransformDef {
        target: "tag_mixed".to_string(),
        source: "post_tags".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec![GroupByKey::Column("tag".to_string())],
        },
        fields: vec![
            FieldDef {
                name: "tag".to_string(),
                expr: Expr::Column("tag".to_string()),
            },
            FieldDef {
                name: "id_sum".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("id".to_string())],
                },
            },
            FieldDef {
                name: "min_word_count".to_string(),
                expr: Expr::FunctionCall {
                    name: "MIN".to_string(),
                    args: vec![Expr::RelationshipPath {
                        rel: "post".to_string(),
                        column: "word_count".to_string(),
                    }],
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

async fn mixed_oracle_totals(client: &Client) -> Totals {
    let base = render_aggregate_relationship_select_sql(&mixed_oracle_def(), &post_rel());
    let sql = format!("select tag, id_sum::text, min_word_count::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query mixed sum/relationship-min oracle")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

async fn mixed_target_totals(client: &Client) -> Totals {
    client
        .query(
            "select tag, id_sum::text, min_word_count::text from tag_mixed",
            &[],
        )
        .await
        .expect("read tag_mixed")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

/// Adds post 4 (`word_count = 5`, lower than every seeded post) unreferenced
/// by any existing `post_tags` row — so backfill's totals are unaffected, and
/// each test below only picks it up via the specific forward insert(s) it
/// stages, making a `min_word_count` change a direct signal that the new
/// row's relationship read was actually folded in (not just "didn't panic").
async fn add_low_word_count_post(client: &Client) {
    client
        .execute("insert into posts (id, word_count) values (4, 5)", &[])
        .await
        .expect("insert post 4");
}

/// The single-touched-group corner: one forward insert, one group touched,
/// so `apply_aggregate_target` routes it through `upsert_group` —
/// `probe_field_value`'s corner of this regression.
#[tokio::test]
async fn sum_and_relationship_min_mixed_single_group_forward_insert() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    add_low_word_count_post(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, MIXED_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the mixed sum/relationship-min definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        mixed_target_totals(&client).await,
        mixed_oracle_totals(&client).await,
        "after backfill"
    );
    assert_eq!(
        mixed_target_totals(&client).await.get("rust"),
        Some(&(Some("34".to_string()), Some("100".to_string()))),
        "sanity: rust = 10 + 11 + 13 = 34, min(100, 250, missing-post) = 100"
    );

    client
        .execute(
            "insert into post_tags (id, post, tag) values (20, 4, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tag");
    stage_cdc(
        &client,
        "post_tags",
        "20",
        "insert",
        None,
        Some("{\"id\":20,\"post\":4,\"tag\":\"rust\"}"),
    )
    .await;
    // Only 'rust' is touched by this batch — must not panic (pre-fix:
    // `render_expr_sql called on an unresolved relationship path 'post.word_count'`).
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = mixed_target_totals(&client).await;
    assert_eq!(
        totals,
        mixed_oracle_totals(&client).await,
        "after a single-group forward delta mixing an invertible field with \
         a relationship-reading RecomputeOnly field"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("54".to_string()), Some("5".to_string()))),
        "id_sum = 34 + 20 = 54; min_word_count drops to post 4's 5, proving \
         the new row's relationship read was actually folded into the live \
         MIN probe, not just non-panicking"
    );
}

/// The multi-touched-group corner: two forward inserts landing in different
/// groups within the same drain batch, so `apply_aggregate_target` routes it
/// through `apply_delta_groups_bulk` — `probe_recompute_fields_bulk`'s corner
/// of this regression, and the one the generative fuzz suite's
/// `property_convergence_holds_across_a_mid_stream_scale_out` caught.
#[tokio::test]
async fn sum_and_relationship_min_mixed_two_groups_in_one_batch() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    add_low_word_count_post(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, MIXED_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the mixed sum/relationship-min definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute(
            "insert into post_tags (id, post, tag) values (21, 4, 'rust'), (22, 4, 'db')",
            &[],
        )
        .await
        .expect("insert two new post_tags rows in different groups");
    stage_cdc(
        &client,
        "post_tags",
        "21",
        "insert",
        None,
        Some("{\"id\":21,\"post\":4,\"tag\":\"rust\"}"),
    )
    .await;
    stage_cdc(
        &client,
        "post_tags",
        "22",
        "insert",
        None,
        Some("{\"id\":22,\"post\":4,\"tag\":\"db\"}"),
    )
    .await;
    // Both 'rust' and 'db' are touched in the same batch (two groups, so the
    // bulk path) — must not panic (pre-fix: same `render_expr_sql` panic as
    // the single-group corner above, from `probe_recompute_fields_bulk`
    // instead of `probe_field_value`).
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = mixed_target_totals(&client).await;
    assert_eq!(
        totals,
        mixed_oracle_totals(&client).await,
        "after a two-group forward delta mixing an invertible field with a \
         relationship-reading RecomputeOnly field"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("55".to_string()), Some("5".to_string()))),
        "id_sum = 34 + 21 = 55; min_word_count drops to post 4's 5"
    );
    assert_eq!(
        totals.get("db"),
        Some(&(Some("48".to_string()), Some("5".to_string()))),
        "id_sum = 26 + 22 = 48; min_word_count drops from 100 to post 4's 5"
    );
}

// ---------------------------------------------------------------------
// Rejections that must still fire.
// ---------------------------------------------------------------------

/// A to-one path referenced *bare* in an aggregate definition is still
/// per-source-row within its group, so it has nowhere to go in the group's
/// single target row — the relationship-path twin of `UngroupedColumnReference`.
#[tokio::test]
async fn a_bare_to_one_path_in_an_aggregate_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");

    let err = install_definition(
        &db.pool,
        "TRANSFORM tag_totals FROM post_tags GROUP BY tag SELECT post.word_count AS wc",
        &post_tags_columns(),
        "public",
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::UngroupedRelationshipReference {
            field,
            rel,
            column,
        }) => {
            assert_eq!(field, "wc");
            assert_eq!(rel, "post");
            assert_eq!(column, "word_count");
        }
        other => panic!("expected UngroupedRelationshipReference, got {other:?}"),
    }
    assert!(
        !table_exists(&client, "tag_totals").await,
        "a rejected definition must leave no target table behind"
    );
}

/// A *to-many* path inside a `GROUP BY` aggregate is aggregating an aggregate —
/// out of scope for #94 and still rejected outright.
#[tokio::test]
async fn a_to_many_path_in_an_aggregate_is_still_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table posts (id integer primary key, word_count integer, tag text); \
             create table comments (id integer primary key, post_id integer, length integer); \
             alter table posts replica identity full; \
             alter table comments replica identity full",
        )
        .await
        .expect("create tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM posts.id TO comments.post_id",
    )
    .await
    .expect("create to-many relationship");

    let err = install_definition(
        &db.pool,
        "TRANSFORM tag_lengths FROM posts GROUP BY tag SELECT sum(comments.length) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("word_count", ValueType::Numeric),
            ("tag", ValueType::Text),
        ]),
        "public",
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::RelationshipPathInAggregate {
            field,
            rel,
            column,
        }) => {
            assert_eq!(field, "total");
            assert_eq!(rel, "comments");
            assert_eq!(column, "length");
        }
        other => panic!("expected RelationshipPathInAggregate, got {other:?}"),
    }
    assert!(
        !table_exists(&client, "tag_lengths").await,
        "a rejected definition must leave no target table behind"
    );
}

/// Regression for the DDL-before-validate ordering bug #94 also turned up:
/// `install_definition` used to emit the target table's DDL *before* running
/// `validate()`, so a definition rejected for any reason still left an orphan
/// table behind (and, for a relationship path, reported a misleading
/// `UnknownRelationship` DDL error instead of the real validation error). The
/// failure here is plain `UngroupedColumnReference` — nothing to do with
/// relationships — precisely because the fix is about ordering for *every*
/// key-space, not a special case for this feature.
#[tokio::test]
async fn an_invalid_aggregate_definition_leaves_no_target_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let err = install_definition(
        &db.pool,
        "TRANSFORM tag_totals FROM post_tags GROUP BY tag SELECT post AS p",
        &post_tags_columns(),
        "public",
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::UngroupedColumnReference { field, column }) => {
            assert_eq!(field, "p");
            assert_eq!(column, "post");
        }
        other => panic!("expected UngroupedColumnReference, got {other:?}"),
    }
    assert!(
        !table_exists(&client, "tag_totals").await,
        "validation must run before any DDL is issued"
    );
}

// ---------------------------------------------------------------------
// Issue #120: `COUNT(<rel>.<column>)` — a to-one relationship path wrapped
// in `COUNT` rather than `SUM`, inside a `GROUP BY` definition.
//
// Before #120 this shape couldn't even parse (`COUNT` only accepted `*` in
// an aggregate definition). The parser/validator/`apply_aggregate` fixes
// #120 landed are all generic over *which* function wraps a relationship
// path — `build_forward_relationship_shape`'s `substitute_relationship_path`
// call rewrites every field's expression the same way regardless of the
// wrapping function name (issue #94/#136 machinery, unmodified by #120) —
// but #120's own test files only exercise `COUNT(<plain column>)`, never
// `COUNT(<rel>.<column>)`. This is the front-door regression test that
// shape actually landed working, through all three propagation directions
// this file's `SUM(post.word_count)` tests already cover for `SUM`.
// ---------------------------------------------------------------------

const TAG_WORD_COUNTS: &str = "TRANSFORM tag_word_counts FROM post_tags GROUP BY tag \
     SELECT COUNT(*) AS post_count, COUNT(post.word_count) AS non_null_word_counts, \
     SUM(post.word_count) AS total_words";

/// [`oracle_def`]'s twin, with an added `COUNT(post.word_count)` field —
/// counts related rows whose `word_count` is non-`NULL`, unlike `post_count`
/// (`COUNT(*)`, every related row) and unlike `total_words` (`SUM`, which
/// skips the same NULLs but adds rather than counts).
fn count_rel_oracle_def() -> TransformDef {
    let mut def = oracle_def();
    def.target = "tag_word_counts".to_string();
    def.fields.insert(
        2,
        FieldDef {
            name: "non_null_word_counts".to_string(),
            expr: Expr::FunctionCall {
                name: "COUNT".to_string(),
                args: vec![Expr::RelationshipPath {
                    rel: "post".to_string(),
                    column: "word_count".to_string(),
                }],
            },
        },
    );
    def
}

type CountRelTotals = HashMap<String, (Option<String>, Option<String>, Option<String>)>;

async fn count_rel_oracle_totals(client: &Client) -> CountRelTotals {
    let base = render_aggregate_relationship_select_sql(&count_rel_oracle_def(), &post_rel());
    let sql = format!(
        "select tag, post_count::text, non_null_word_counts::text, total_words::text from ({base}) t"
    );
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query aggregate relationship oracle")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3))))
        .collect()
}

async fn count_rel_target_totals(client: &Client) -> CountRelTotals {
    client
        .query(
            "select tag, post_count::text, non_null_word_counts::text, total_words::text \
             from tag_word_counts",
            &[],
        )
        .await
        .expect("read tag_word_counts")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3))))
        .collect()
}

/// Backfill, a forward from-side insert, and a reverse to-side update — the
/// same three propagation directions [`aggregate_over_a_to_one_relationship_backfills_to_the_oracle`]/
/// [`inserting_a_from_side_row_updates_its_groups_total`]/
/// [`updating_a_to_side_row_updates_every_dependent_group`] each prove for
/// `SUM(post.word_count)`, proven here for `COUNT(post.word_count)` instead
/// — including the reverse-delta path (`super::apply`'s `ReverseAggregateShape`,
/// `diff_contributions`/`add_contributions`/`sub_contributions`), which
/// #120's own review called out as the subtlest part of `COUNT`'s "0 means
/// skip this row" rule and had no relationship-path coverage at all before
/// this test.
#[tokio::test]
async fn count_of_a_relationship_path_folds_through_backfill_forward_and_reverse() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_WORD_COUNTS, &post_tags_columns(), "public")
        .await
        .expect("install COUNT(<rel>.<column>) in a GROUP BY definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Backfill: `rust` = rows 10 (post 1, wc 100), 11 (post 2, wc 250), 13
    // (post 999, no such post -> NULL); `db` = rows 12 (post 1, wc 100), 14
    // (post 3, wc NULL). `non_null_word_counts` excludes both NULL-producing
    // rows (the unmatched FK and the genuinely NULL column), unlike
    // `post_count` which counts every related row regardless.
    let totals = count_rel_target_totals(&client).await;
    assert_eq!(
        totals,
        count_rel_oracle_totals(&client).await,
        "after backfill"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(
            Some("3".to_string()),
            Some("2".to_string()),
            Some("350".to_string())
        ))
    );
    assert_eq!(
        totals.get("db"),
        Some(&(
            Some("2".to_string()),
            Some("1".to_string()),
            Some("100".to_string())
        ))
    );

    // Forward delta: a new post_tags row (post 2, wc 250) joins tag `db`.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (15, 2, 'db')",
            &[],
        )
        .await
        .expect("insert a new post_tag");
    stage_cdc(
        &client,
        "post_tags",
        "15",
        "insert",
        None,
        Some("{\"id\":15,\"post\":2,\"tag\":\"db\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = count_rel_target_totals(&client).await;
    assert_eq!(
        totals,
        count_rel_oracle_totals(&client).await,
        "after a forward delta touching a COUNT field's relationship read"
    );
    assert_eq!(
        totals.get("db"),
        Some(&(
            Some("3".to_string()),
            Some("2".to_string()),
            Some("350".to_string())
        )),
        "'db' must now count post 1 (non-null) and post 2 (non-null) = 2, \
         folded in via the forward delta"
    );

    // Reverse delta: the to-side post's word_count changes value (non-NULL
    // -> non-NULL, never crossing the NULL boundary) — every dependent
    // group's COUNT(post.word_count) must stay unchanged while SUM tracks
    // the new value, proving the reverse path's per-field diff doesn't
    // conflate "the resolved value changed" with "non-null-ness changed".
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = count_rel_target_totals(&client).await;
    assert_eq!(
        totals,
        count_rel_oracle_totals(&client).await,
        "after a reverse delta touching a COUNT field's relationship read"
    );
    assert_eq!(
        totals.get("rust"),
        Some(&(
            Some("3".to_string()),
            Some("2".to_string()),
            Some("650".to_string())
        )),
        "rust's non_null_word_counts stays 2 (post 1's value changed but is \
         still non-null); total_words tracks the new value (400 + 250)"
    );
    assert_eq!(
        totals.get("db"),
        Some(&(
            Some("3".to_string()),
            Some("2".to_string()),
            Some("650".to_string())
        )),
        "db's non_null_word_counts stays 2 for the same reason; total_words \
         is now 400 (post 1) + 250 (post 2)"
    );
}
