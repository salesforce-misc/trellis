//! Integration tests for the relationship-enriched 1-1 direct backfill
//! (`defs::backfill::backfill_definition`'s to-many-aggregate path, issue #63
//! C2), run against a real ephemeral Postgres via `testkit::TestCluster`.
//!
//! The correctness bar is byte-identity with the LEFT-JOIN/correlated-subquery
//! oracle (`render_relationship_select_sql`), which is itself pinned to the
//! per-row evaluator in `apply_relationships.rs`. These exercise the no-match
//! (zero related rows) and all-NULL-argument cases that decide the empty-set
//! semantics (`COUNT` -> 0, `SUM`/`MIN`/`MAX`/`AVG` -> NULL), a parent with
//! several related rows, two distinct to-many relationships joined at once,
//! and a re-run after the children are mutated (insert/update/delete)
//! between the two builds.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{RelationshipDef, TransformDef};
use trellis::defs::{
    ValueType, backfill_definition, create_relationship, create_target_table, parse,
    render_relationship_select_sql, source_primary_key,
};

/// The definition the target table + backfill are built from. Its primary key
/// (`id`, inherited from `authors`) is implicit, so it lists only the enriched
/// aggregate fields — a 1-1 target never lists its own PK as a field.
const BUILD_SRC: &str = "TRANSFORM author_totals FROM authors SELECT \
     SUM(posts.words) AS word_sum, \
     COUNT(posts.words) AS post_count, \
     AVG(posts.words) AS word_avg, \
     MIN(posts.words) AS lo, \
     MAX(posts.words) AS hi, \
     COUNT(comments.id) AS comment_count";

/// The same definition with an explicit `id AS id` field, used only to render
/// the oracle SELECT (which needs the key column projected for comparison —
/// rendering neither creates a table nor minds the PK-passthrough shape).
const ORACLE_SRC: &str = "TRANSFORM author_totals FROM authors SELECT \
     id AS id, \
     SUM(posts.words) AS word_sum, \
     COUNT(posts.words) AS post_count, \
     AVG(posts.words) AS word_avg, \
     MIN(posts.words) AS lo, \
     MAX(posts.words) AS hi, \
     COUNT(comments.id) AS comment_count";

const VALUE_COLS: &[&str] = &[
    "word_sum",
    "post_count",
    "word_avg",
    "lo",
    "hi",
    "comment_count",
];

fn relationships() -> HashMap<String, RelationshipDef> {
    HashMap::from([
        (
            "posts".to_string(),
            RelationshipDef {
                name: "posts".to_string(),
                from_table: "authors".to_string(),
                from_col: "id".to_string(),
                to_table: "posts".to_string(),
                to_col: "author_id".to_string(),
            },
        ),
        (
            "comments".to_string(),
            RelationshipDef {
                name: "comments".to_string(),
                from_table: "authors".to_string(),
                from_col: "id".to_string(),
                to_table: "comments".to_string(),
                to_col: "author_id".to_string(),
            },
        ),
    ])
}

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([("id".to_string(), ValueType::Numeric)])
}

/// Reads `(id, <VALUE_COLS...>)` as text, keyed by id — an order-independent
/// image of a whole target/oracle result.
async fn read_rows(
    client: &trellis::pool::Client,
    sql: &str,
) -> HashMap<String, Vec<Option<String>>> {
    client
        .query(sql, &[])
        .await
        .expect("query")
        .into_iter()
        .map(|r| {
            let id: String = r.get(0);
            let vals: Vec<Option<String>> = (1..=VALUE_COLS.len())
                .map(|i| r.get::<_, Option<String>>(i))
                .collect();
            (id, vals)
        })
        .collect()
}

fn select_list() -> String {
    let cols = VALUE_COLS
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("id::text, {cols}")
}

/// Asserts the built target equals the correlated-subquery oracle over the same
/// data, comparing every parent's key + enriched columns byte-for-byte.
async fn assert_matches_oracle(db: &testkit::TestDatabase) {
    let oracle_def = parse(ORACLE_SRC).expect("parse oracle def");
    let oracle_sql = render_relationship_select_sql(&oracle_def, &relationships());

    let client = db.pool.get().await.expect("get connection");
    let sel = select_list();
    let target = read_rows(&client, &format!("select {sel} from public.author_totals")).await;
    let oracle = read_rows(&client, &format!("select {sel} from ({oracle_sql}) o")).await;
    assert_eq!(
        target, oracle,
        "built relationship target must match the correlated-subquery oracle"
    );
}

async fn setup(db: &testkit::TestDatabase) -> TransformDef {
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer, words integer); \
             create table comments (id integer primary key, author_id integer); \
             alter table posts replica identity full; \
             alter table comments replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'), (3, 'c'); \
             insert into posts (id, author_id, words) values \
               (100, 1, 10), (101, 1, 20), (102, 2, null); \
             insert into comments (id, author_id) values (200, 1), (201, 1)",
        )
        .await
        .expect("seed source + related tables");
    drop(client);

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create comments relationship");

    let def = parse(BUILD_SRC).expect("parse build def");
    let pk = source_primary_key(&db.pool, "authors").await.expect("pk");
    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &source_columns(),
        &def.source,
    )
    .await
    .expect("create relationship-enriched target");
    def
}

#[tokio::test]
async fn relationship_build_matches_oracle_including_no_match_and_multi_child() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let def = setup(&db).await;
    backfill_definition(&db.pool, &def, "public", &def.source, &source_columns())
        .await
        .expect("backfill");

    // Every author is built (including author 3 with no related rows at all).
    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one("select count(*) from public.author_totals", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 3, "one target row per source author");

    // Spot-check the empty-set semantics directly: author 3 has no related rows,
    // so every SUM/MIN/MAX/AVG is NULL and every COUNT is 0.
    let row = client
        .query_one(
            "select word_sum is null, post_count = 0, word_avg is null, \
                    lo is null, hi is null, comment_count = 0 \
             from public.author_totals where id = 3",
            &[],
        )
        .await
        .unwrap();
    for i in 0..6 {
        assert!(row.get::<_, bool>(i), "author 3 empty-set column {i}");
    }

    // Author 1: two posts summed/counted, two comments counted.
    let row = client
        .query_one(
            "select word_sum = 30, post_count = 2, word_avg = 15, \
                    lo = 10, hi = 20, comment_count = 2 \
             from public.author_totals where id = 1",
            &[],
        )
        .await
        .unwrap();
    for i in 0..6 {
        assert!(row.get::<_, bool>(i), "author 1 aggregate column {i}");
    }

    drop(client);
    assert_matches_oracle(&db).await;
}

#[tokio::test]
async fn relationship_build_rerun_reflects_source_mutations() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let def = setup(&db).await;
    backfill_definition(&db.pool, &def, "public", &def.source, &source_columns())
        .await
        .expect("first backfill");

    // Mutate the to-many children directly in Postgres between runs: insert a
    // new post for author 2 (previously one post with all-NULL words, so its
    // aggregates were the empty-set NULL/0 values), update an existing post's
    // value for author 1, and delete one of author 1's comments. A re-run
    // must recompute from this new state, not just replay the first run's
    // values via `ON CONFLICT` — the gap the old same-data-rerun version of
    // this test didn't cover.
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "insert into posts (id, author_id, words) values (103, 2, 50); \
             update posts set words = 99 where id = 101; \
             delete from comments where id = 201",
        )
        .await
        .expect("mutate child tables between runs");
    drop(client);

    backfill_definition(&db.pool, &def, "public", &def.source, &source_columns())
        .await
        .expect("second backfill after mutation");

    let client = db.pool.get().await.expect("get connection");

    // Author 1: post 101's words changed 20 -> 99, moving the sum/avg/max;
    // one of its two comments was deleted.
    let row = client
        .query_one(
            "select word_sum = 109, post_count = 2, word_avg = 54.5, \
                    lo = 10, hi = 99, comment_count = 1 \
             from public.author_totals where id = 1",
            &[],
        )
        .await
        .unwrap();
    for i in 0..6 {
        assert!(row.get::<_, bool>(i), "author 1 post-mutation column {i}");
    }

    // Author 2: the new post moves it out of the all-NULL empty-set case for
    // SUM/AVG/MIN/MAX into a real aggregate.
    let row = client
        .query_one(
            "select word_sum = 50, post_count = 1, word_avg = 50, \
                    lo = 50, hi = 50 \
             from public.author_totals where id = 2",
            &[],
        )
        .await
        .unwrap();
    for i in 0..5 {
        assert!(row.get::<_, bool>(i), "author 2 post-mutation column {i}");
    }

    drop(client);
    assert_matches_oracle(&db).await;
}
