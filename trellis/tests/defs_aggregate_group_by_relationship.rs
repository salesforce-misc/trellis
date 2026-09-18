//! Front-door integration tests for issue #137: a `GROUP BY` key may itself
//! be a to-one relationship path (`rel.column`), not just a plain source
//! column — e.g. `TRANSFORM author_tag_totals FROM post_tags GROUP BY tag,
//! post.author SELECT count(*) AS post_count, sum(post.word_count) AS
//! total_words` groups `post_tags` rows by their own `tag` *and* by their
//! linked post's `author`, even though `author` never appears as a
//! `post_tags` column at all.
//!
//! This is issue #94's aggregated-relationship-*field* precedent
//! (`defs_aggregate_relationship.rs`) turned into a relationship-*group-key*
//! precedent instead — same harness (connect, stage a CDC row, seal/drain to
//! quiescence), same oracle convention
//! (`render_aggregate_relationship_select_sql`, with the oracle's own `def`
//! carrying an explicit field for every implicit `GROUP BY` column, exactly
//! like `defs_aggregate_relationship.rs`'s `oracle_def()` adds one for
//! `tag`), just exercising the harder "which group does this row belong to"
//! question instead of "what value does this row contribute".
//!
//! Everything goes through the public front door (`create_relationship` +
//! `install_definition`), so DDL, direct backfill, and incremental apply
//! (forward inserts, and the reverse-fanout path a changed `posts.author`
//! drives) are all exercised exactly as a real caller would hit them.

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
    render_aggregate_relationship_select_sql,
};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

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

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — mirrors
/// `defs_aggregate_relationship.rs`'s own helper of the same name; see that
/// file's doc comment for why this qualification matters.
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
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark — see
    // `defs_aggregate_relationship.rs`'s identical helper for why this is
    // safe for a hand-staged-CDC test file with no live `Intake` running.
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "agg_group_by_rel_test",
            1,
            "trellis_agg_group_by_rel_test",
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

const AUTHOR_TAG_TOTALS: &str = "TRANSFORM author_tag_totals FROM post_tags GROUP BY tag, post.author \
     SELECT count(*) AS post_count, sum(post.word_count) AS total_words";

/// The running schema for this file: `posts.author` is the relationship
/// group-key column — `post_tags` itself never carries an `author` column at
/// all. Both tables need `REPLICA IDENTITY FULL` for the same two reasons
/// `defs_aggregate_relationship.rs`'s `create_schema` documents: `post_tags`
/// is an *aggregate* source (the delta/recompute path needs the old image to
/// locate the group a changed row is leaving), and `posts` is the to-side of
/// a to-one relationship (the settled parent projection requires it
/// unconditionally, issue #129).
///
/// Seed data deliberately covers the same nullability corners issue #94's
/// own fixture does: tag `rust` includes a row whose FK (`post = 999`)
/// resolves to no post at all (both `author` and `word_count` are `NULL` for
/// it), and tag `db` includes a row whose post exists but has a `NULL`
/// `word_count` (post 3) — both must still show up under their post's real
/// `author` and contribute `NULL`/skip appropriately to `SUM`, while still
/// counting toward `COUNT(*)`.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table posts (id integer primary key, author text, word_count integer); \
             create table post_tags (id integer primary key, post integer, tag text); \
             alter table post_tags replica identity full; \
             alter table posts replica identity full; \
             create index on post_tags (post); \
             insert into posts (id, author, word_count) values \
               (1, 'alice', 100), (2, 'bob', 250), (3, 'alice', null); \
             insert into post_tags (id, post, tag) values \
               (10, 1, 'rust'), (11, 2, 'rust'), (12, 1, 'db'), \
               (13, 999, 'rust'), (14, 3, 'db')",
        )
        .await
        .expect("create + seed issue #137's schema");
}

async fn create_post_relationship(pool: &trellis::Pool) {
    create_relationship(pool, "RELATIONSHIP post FROM post_tags.post TO posts.id")
        .await
        .expect("create to-one relationship");
}

/// The oracle's version of [`AUTHOR_TAG_TOTALS`]: identical, plus an
/// explicit field for *every* implicit `GROUP BY` column (`tag` and, issue
/// #137's new case, the relationship-path key `post.author`) — mirroring
/// `defs_aggregate_relationship.rs`'s `oracle_def()`, which adds one for
/// `tag` alone. The real installed definition's own `SELECT` list never
/// names either column; DDL adds them as target columns unconditionally, so
/// the oracle's own `def` must too, or `render_aggregate_relationship_select_sql`
/// would render a `GROUP BY` clause with nothing in the `SELECT` list to
/// compare against.
fn oracle_def() -> TransformDef {
    TransformDef {
        target: "author_tag_totals".to_string(),
        source: "post_tags".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec![
                GroupByKey::Column("tag".to_string()),
                GroupByKey::RelationshipPath {
                    rel: "post".to_string(),
                    column: "author".to_string(),
                },
            ],
        },
        fields: vec![
            FieldDef {
                name: "tag".to_string(),
                expr: Expr::Column("tag".to_string()),
            },
            FieldDef {
                name: "author".to_string(),
                expr: Expr::RelationshipPath {
                    rel: "post".to_string(),
                    column: "author".to_string(),
                },
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

/// `(tag, author) -> (post_count, total_words)`, both as text so a `NULL`
/// author/total compares as a plain `None` rather than needing a sentinel.
type Totals = HashMap<(Option<String>, Option<String>), (Option<String>, Option<String>)>;

async fn oracle_totals(client: &Client) -> Totals {
    let base = render_aggregate_relationship_select_sql(&oracle_def(), &post_rel());
    let sql = format!("select tag, author, post_count::text, total_words::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query aggregate relationship-group-by oracle")
        .into_iter()
        .map(|r| ((r.get(0), r.get(1)), (r.get(2), r.get(3))))
        .collect()
}

async fn target_totals(client: &Client) -> Totals {
    client
        .query(
            "select tag, author, post_count::text, total_words::text from author_tag_totals",
            &[],
        )
        .await
        .expect("read author_tag_totals")
        .into_iter()
        .map(|r| ((r.get(0), r.get(1)), (r.get(2), r.get(3))))
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

fn some(s: &str) -> Option<String> {
    Some(s.to_string())
}

// ---------------------------------------------------------------------
// The issue's shape, end to end.
// ---------------------------------------------------------------------

/// The headline case: a `GROUP BY` key that's a to-one relationship path
/// installs, and its direct-build backfill over pre-existing data matches
/// Postgres's own `LEFT JOIN … GROUP BY`.
#[tokio::test]
async fn aggregate_over_a_relationship_group_by_key_backfills_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    create_post_relationship(&db.pool).await;

    install_definition(&db.pool, AUTHOR_TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the relationship-group-by-key definition");

    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(totals, oracle_totals(&client).await, "after backfill");
    // Pinned explicitly, not just against the oracle (see
    // `defs_aggregate_relationship.rs`'s own rationale for this
    // belt-and-suspenders style): `(rust, alice)` = row 10 (100 words);
    // `(rust, bob)` = row 11 (250 words); `(rust, NULL)` = row 13 (no such
    // post, so both `author` and `word_count` resolve to `NULL`);
    // `(db, alice)` = rows 12 (100 words) + 14 (post 3's `word_count` is
    // `NULL`, skipped by `SUM` but still counted).
    assert_eq!(
        totals.get(&(some("rust"), some("alice"))),
        Some(&(some("1"), some("100")))
    );
    assert_eq!(
        totals.get(&(some("rust"), some("bob"))),
        Some(&(some("1"), some("250")))
    );
    assert_eq!(totals.get(&(some("rust"), None)), Some(&(some("1"), None)));
    assert_eq!(
        totals.get(&(some("db"), some("alice"))),
        Some(&(some("2"), some("100")))
    );
}

/// Forward propagation: a new `post_tags` row resolves its own group through
/// the linked post's `author`, even though `post_tags` itself has no
/// `author` column at all — proving the row's group key resolves through
/// the relationship correctly at apply time, not just at backfill time.
#[tokio::test]
async fn inserting_a_from_side_row_lands_in_the_relationship_group_by_keys_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    create_post_relationship(&db.pool).await;

    install_definition(&db.pool, AUTHOR_TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the relationship-group-by-key definition");
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
    // Post 2 is bob's, 250 words — a brand-new `(db, bob)` group, not folded
    // into any existing `db` group.
    assert_eq!(
        totals.get(&(some("db"), some("bob"))),
        Some(&(some("1"), some("250")))
    );
    // Every pre-existing group is untouched by an insert into a different
    // group.
    assert_eq!(
        totals.get(&(some("db"), some("alice"))),
        Some(&(some("2"), some("100")))
    );
}

/// Reverse propagation, the sharpest test of this issue: changing the
/// *related* `posts` row's `author` — the `GROUP BY` value itself, not just
/// a field a group folds — must move every affected `post_tags` row's
/// contribution from its *old* group to its *new* one. Post 1 (`alice`,
/// renamed to `carol`) is referenced by both the `rust` (row 10) and `db`
/// (row 12) groups, so four groups are affected: two emptied (and, for
/// `rust`, deleted outright — `db`'s old group survives via row 14, a
/// different post entirely), two created.
#[tokio::test]
async fn updating_a_to_side_rows_group_by_column_moves_affected_rows_to_the_new_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    create_post_relationship(&db.pool).await;

    install_definition(&db.pool, AUTHOR_TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the relationship-group-by-key definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals_before = target_totals(&client).await;
    assert_eq!(
        totals_before.get(&(some("rust"), some("alice"))),
        Some(&(some("1"), some("100"))),
        "sanity: row 10 (post 1, alice) starts 'rust's alice group"
    );
    assert_eq!(
        totals_before.get(&(some("db"), some("alice"))),
        Some(&(some("2"), some("100"))),
        "sanity: rows 12 (post 1) and 14 (post 3) start 'db's alice group"
    );

    client
        .execute("update posts set author = 'carol' where id = 1", &[])
        .await
        .expect("rename post 1's author");
    stage_cdc(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"author\":\"alice\",\"word_count\":100}"),
        Some("{\"id\":1,\"author\":\"carol\",\"word_count\":100}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals,
        oracle_totals(&client).await,
        "after updating the related post's group-by column"
    );

    // `rust`'s old `alice` group loses its only row (row 10) and is deleted
    // outright.
    assert_eq!(totals.get(&(some("rust"), some("alice"))), None);
    // A brand-new `rust`/`carol` group appears, carrying row 10's
    // contribution.
    assert_eq!(
        totals.get(&(some("rust"), some("carol"))),
        Some(&(some("1"), some("100")))
    );
    // `db`'s old `alice` group loses row 12 but survives via row 14 (post 3,
    // a different post entirely, still authored by alice) — count drops from
    // 2 to 1, and the total goes from 100 (row 12 only, row 14 was NULL and
    // skipped) to NULL (only row 14 remains, itself NULL).
    assert_eq!(
        totals.get(&(some("db"), some("alice"))),
        Some(&(some("1"), None))
    );
    // A brand-new `db`/`carol` group appears, carrying row 12's contribution.
    assert_eq!(
        totals.get(&(some("db"), some("carol"))),
        Some(&(some("1"), some("100")))
    );
    // Untouched groups (no row referencing post 1) are unaffected.
    assert_eq!(
        totals.get(&(some("rust"), some("bob"))),
        Some(&(some("1"), some("250")))
    );
    assert_eq!(totals.get(&(some("rust"), None)), Some(&(some("1"), None)));
}

/// Chaining: a further aggregate `GROUP BY tag` on top of `author_tag_totals`
/// (itself grouped partly by a relationship path, and so keyed by a genuine
/// **composite** `(tag, author)` pair) converges correctly — exercising
/// `derive_group_key`'s multi-column (`author_tag_totals`'s own key) vs.
/// single-column (`tag_totals2`'s own `tag` key) encoding split from issue
/// #103, which this issue must not disturb.
///
/// Every interesting #137 mechanic (forward relationship resolution, the
/// reverse-fanout grain migration) is driven directly against
/// `author_tag_totals` *before* `tag_totals2` is installed — which, when
/// this test was written, was load-bearing rather than merely simpler:
/// propagating a *live incremental* change on a **multi-column** `GROUP BY`
/// aggregate target *downstream* to a further chained definition was broken
/// on `main`, independently of #137, because
/// `apply_aggregate::derive_group_key`'s then length-prefixed composite-key
/// encoding leaked into the downstream `Recompute` marker's `key` field,
/// which the live re-fetch path (`intake::extract_key`'s U+001F-joined
/// convention) then failed to parse (`DdlError::MalformedCompositeKey`).
/// Issue #103 had only ever fixed that encoding's *single*-column case (see
/// `chaining_onto_a_single_group_by_column_aggregate_target_does_not_misread_the_group_key`
/// in `apply_aggregate.rs`). **Issue #171 has since closed the multi-column
/// case** the same way — a composite group key is now emitted as an ordinary
/// U+001F-joined composite primary key — and pins it with live updates,
/// inserts and grain migrations across a two-column chain in
/// `defs_aggregate_chained_composite_group_key.rs`.
///
/// This test is deliberately left as it was: `tag_totals2` is installed (and
/// does its own from-scratch backfill, a plain table scan with no per-row
/// key string involved at all) only *after* `author_tag_totals` has already
/// reached its final, fully-migrated state — proving the chain converges
/// correctly over a *relationship-derived* composite key, which is #137's
/// own concern and is orthogonal to #171's encoding fix.
#[tokio::test]
async fn chaining_a_further_aggregate_onto_the_relationship_group_by_target_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    create_post_relationship(&db.pool).await;

    install_definition(&db.pool, AUTHOR_TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the relationship-group-by-key definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Drive both of this issue's interesting mechanics directly against
    // `author_tag_totals` — a forward insert (new `(db, bob)` group) and the
    // reverse-fanout grain migration (post 1's author, `alice` -> `carol`) —
    // before `tag_totals2` exists at all, so `author_tag_totals` reaches its
    // final shape entirely through paths this file's other tests already
    // prove correct in isolation.
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

    client
        .execute("update posts set author = 'carol' where id = 1", &[])
        .await
        .expect("rename post 1's author");
    stage_cdc(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"author\":\"alice\",\"word_count\":100}"),
        Some("{\"id\":1,\"author\":\"carol\",\"word_count\":100}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    // Sanity: `author_tag_totals` reached the fully-migrated shape this
    // test's expectations below assume — (rust, alice) gone, (rust, carol)/
    // (db, carol)/(db, bob) new, (db, alice) reduced to row 14 alone.
    let totals = target_totals(&client).await;
    assert_eq!(totals.get(&(some("rust"), some("alice"))), None);
    assert_eq!(
        totals.get(&(some("db"), some("bob"))),
        Some(&(some("1"), some("250")))
    );

    // `author_tag_totals` becomes an *aggregate source* for `tag_totals2`
    // below, which needs the old image to locate the group a changed row is
    // leaving — same requirement `create_schema`'s own tables have, just
    // applied after the fact since `author_tag_totals` didn't exist before
    // `install_definition` created it. `tag_totals2` never actually sees a
    // *live* change to `author_tag_totals` in this test (see the doc
    // comment above), but the install-time replica-identity check
    // (`assert_replica_identity_supports_aggregate`) still requires it
    // unconditionally for any aggregate source.
    client
        .batch_execute("alter table author_tag_totals replica identity full")
        .await
        .expect("widen author_tag_totals's replica identity");

    const TAG_TOTALS_2: &str = "TRANSFORM tag_totals2 FROM author_tag_totals GROUP BY tag \
         SELECT sum(post_count) AS post_count, sum(total_words) AS total_words";
    let author_tag_totals_columns = columns(&[
        ("tag", ValueType::Text),
        ("author", ValueType::Text),
        ("post_count", ValueType::Numeric),
        ("total_words", ValueType::Numeric),
    ]);
    install_definition(&db.pool, TAG_TOTALS_2, &author_tag_totals_columns, "public")
        .await
        .expect("install the chained plain-column aggregate");
    drain_to_quiescence(&db.pool, &mut client).await;

    let tag_totals_2: HashMap<String, (String, Option<String>)> = client
        .query(
            "select tag, post_count::text, total_words::text from tag_totals2",
            &[],
        )
        .await
        .expect("read tag_totals2")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect();

    // rust: (rust, carol) 1/100 + (rust, bob) 1/250 + (rust, NULL) 1/NULL =
    // 3 posts, 350 words. db: (db, alice) 1/NULL + (db, carol) 1/100 +
    // (db, bob) 1/250 = 3 posts, 350 words.
    assert_eq!(
        tag_totals_2.get("rust"),
        Some(&("3".to_string(), some("350"))),
        "chained backfill over author_tag_totals's fully-migrated, \
         relationship-derived composite-key state"
    );
    assert_eq!(
        tag_totals_2.get("db"),
        Some(&("3".to_string(), some("350"))),
        "chained backfill over author_tag_totals's fully-migrated, \
         relationship-derived composite-key state"
    );
}

// ---------------------------------------------------------------------
// Rejections that must still fire.
// ---------------------------------------------------------------------

/// A to-many relationship path used as a `GROUP BY` key is rejected at
/// validation time with a clear, `GROUP BY`-specific error: grouping by "the
/// many child rows on the other end of a to-many relationship" has no
/// defined semantics.
#[tokio::test]
async fn a_to_many_relationship_path_as_a_group_by_key_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table posts2 (id integer primary key, tag text); \
             create table comments2 (id integer primary key, post_id integer, author text); \
             alter table posts2 replica identity full; \
             alter table comments2 replica identity full",
        )
        .await
        .expect("create tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM posts2.id TO comments2.post_id",
    )
    .await
    .expect("create to-many relationship");

    let err = install_definition(
        &db.pool,
        "TRANSFORM comment_author_totals FROM posts2 GROUP BY comments.author \
         SELECT COUNT(*) AS post_count",
        &columns(&[("id", ValueType::Numeric), ("tag", ValueType::Text)]),
        "public",
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::GroupByRelationshipMustBeToOne { rel, column }) => {
            assert_eq!(rel, "comments");
            assert_eq!(column, "author");
        }
        other => panic!("expected GroupByRelationshipMustBeToOne, got {other:?}"),
    }
    assert!(
        !table_exists(&client, "comment_author_totals").await,
        "a rejected definition must leave no target table behind"
    );
}

/// A `GROUP BY` key naming an undeclared relationship is rejected with a
/// clear, `GROUP BY`-specific error (the `GROUP BY` twin of
/// `UnknownRelationship`, which blames a field).
#[tokio::test]
async fn a_group_by_key_naming_an_unknown_relationship_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let err = install_definition(
        &db.pool,
        "TRANSFORM x FROM post_tags GROUP BY tag, nope.author SELECT COUNT(*) AS post_count",
        &post_tags_columns(),
        "public",
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(ValidationError::UnknownGroupByRelationship { rel }) => {
            assert_eq!(rel, "nope");
        }
        other => panic!("expected UnknownGroupByRelationship, got {other:?}"),
    }
    assert!(
        !table_exists(&client, "x").await,
        "a rejected definition must leave no target table behind"
    );
}

/// A definition that, unlike [`AUTHOR_TAG_TOTALS`], adds an *explicit*
/// bare-passthrough field for the relationship `GROUP BY` key
/// (`SELECT post.author AS author, ...`) — legal per
/// `validate::a_group_by_relationship_path_bare_passthrough_field_is_allowed`,
/// and the only shape that puts a `RelationshipPath` expression on a *field*
/// that also happens to be a `GROUP BY`-passthrough (every other field in
/// this file's fixtures either wraps the path in an aggregate call or leaves
/// it unmentioned entirely — DDL derives the `author` column from the
/// `GROUP BY` key alone either way, so this field is legal but redundant).
const AUTHOR_TAG_TOTALS_WITH_PASSTHROUGH: &str = "TRANSFORM author_tag_totals_pt FROM post_tags GROUP BY tag, post.author \
     SELECT post.author AS author, count(*) AS post_count, sum(post.word_count) AS total_words";

/// Regression pin (found in review): [`build_reverse_relationship_shape`]'s
/// per-field `substitute_relationship_path` rewrite left
/// `rewritten.key_space`'s own `GroupByKey::RelationshipPath` entry
/// unrewritten, unlike `apply_aggregate::build_forward_relationship_shape`'s
/// matching rewrite on the forward path. `eval::evaluate_aggregate` keys its
/// `group_by` set off each key's *target* column name (`author`), so a field
/// substituted to `Column("__trellis_rev_author")` never matched it, and
/// `eval_aggregate_expr` fell through to its "must be another calculated
/// field" branch and returned `EvalError::MissingColumn` — aborting the
/// entire reverse-fanout apply for *any* definition with an explicit
/// bare-passthrough field for a relationship `GROUP BY` key, the moment the
/// related row actually changed. [`AUTHOR_TAG_TOTALS`]'s own reverse-fanout
/// test never caught this because it has no such field at all (DDL derives
/// the `author` column from the `GROUP BY` key regardless) — this test adds
/// the one field shape that did trigger it.
#[tokio::test]
async fn updating_a_to_sides_row_with_a_passthrough_field_for_the_group_by_key_still_migrates_groups()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    create_post_relationship(&db.pool).await;

    install_definition(
        &db.pool,
        AUTHOR_TAG_TOTALS_WITH_PASSTHROUGH,
        &post_tags_columns(),
        "public",
    )
    .await
    .expect("install the passthrough-field definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute("update posts set author = 'carol' where id = 1", &[])
        .await
        .expect("rename post 1's author");
    stage_cdc(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"author\":\"alice\",\"word_count\":100}"),
        Some("{\"id\":1,\"author\":\"carol\",\"word_count\":100}"),
    )
    .await;
    // Before the fix, this call panics: `drain_once` propagates
    // `ApplyError::Eval(EvalError::MissingColumn { field: "author", column:
    // "__trellis_rev_author" })` the moment it tries to compute row 10's (or
    // row 12's) contribution under the renamed author.
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals: Totals = client
        .query(
            "select tag, author, post_count::text, total_words::text from author_tag_totals_pt",
            &[],
        )
        .await
        .expect("read author_tag_totals_pt")
        .into_iter()
        .map(|r| ((r.get(0), r.get(1)), (r.get(2), r.get(3))))
        .collect();

    // Same grain migration as `updating_a_to_side_rows_group_by_column_moves_affected_rows_to_the_new_group`:
    // `rust`'s old `alice` group (row 10 only) is deleted outright, and a new
    // `rust`/`carol` group takes its place.
    assert_eq!(totals.get(&(some("rust"), some("alice"))), None);
    assert_eq!(
        totals.get(&(some("rust"), some("carol"))),
        Some(&(some("1"), some("100")))
    );
    // `db`'s old `alice` group survives via row 14 (a different post), with
    // row 12 subtracted out: count drops 2 -> 1, sum drops to NULL (only
    // row 14, itself NULL, remains).
    assert_eq!(
        totals.get(&(some("db"), some("alice"))),
        Some(&(some("1"), None))
    );
    assert_eq!(
        totals.get(&(some("db"), some("carol"))),
        Some(&(some("1"), some("100")))
    );
}
