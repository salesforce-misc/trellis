//! Benchmark scenario for the relationship-aggregate direct build (issue
//! #63, C3): a `KeySpace::OneToOne` `authors` transform that `SUM`/`COUNT`s
//! over two to-many children (`posts`, `comments`), matching the real
//! workload that motivated `backfill_relationship_one_to_one`
//! (`trellis::dev::defs::backfill`) — 100k authors, 1M posts, 4.5M comments,
//! creating the target table took ~1 minute on the old ring path.
//!
//! Unlike [`crate::scenario`]'s plain `GROUP BY` pipeline, this measures the
//! *whole* `install_definition` call (relationship creation is untimed setup;
//! the target table's creation + direct build is what's timed) — the real
//! front door a caller uses, not `backfill_definition` directly — so the
//! number reported here is what an end user actually experiences.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::dev::defs::{ValueType, create_relationship, install_definition};

use crate::generate;
use crate::scenario::connect_raw;

/// One relationship-aggregate scenario's measurements: the untimed load, the
/// timed `install_definition` call, and an independent correctness check.
#[derive(Debug)]
pub struct RelationshipBenchResult {
    pub scenario: String,
    pub authors: i64,
    pub posts: i64,
    pub comments: i64,
    pub load_ms: u128,
    pub backfill_ms: u128,
    pub correctness_ok: bool,
    pub ceiling_ms: u128,
    pub within_ceiling: bool,
}

impl RelationshipBenchResult {
    /// Renders as one line of machine-readable JSON, hand-rolled for the
    /// same reason [`crate::scenario::BenchResult::to_json`] is: every field
    /// is an integer, bool, or a string with no characters that need
    /// escaping.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"{}\",\"authors\":{},\"posts\":{},\"comments\":{},\
             \"load_ms\":{},\"backfill_ms\":{},\"correctness_ok\":{},\
             \"ceiling_ms\":{},\"within_ceiling\":{}}}",
            self.scenario,
            self.authors,
            self.posts,
            self.comments,
            self.load_ms,
            self.backfill_ms,
            self.correctness_ok,
            self.ceiling_ms,
            self.within_ceiling,
        )
    }
}

/// Runs the relationship-aggregate scenario end to end against a fresh,
/// ephemeral Postgres instance: loads `authors` deterministic parent rows
/// plus `posts`/`comments` to-many children distributed evenly over them,
/// declares both relationships, installs a `SUM`/`COUNT`-over-both-children
/// definition through the real `install_definition` front door, and checks
/// the result against an independent `GROUP BY`/`LEFT JOIN` oracle.
pub async fn run(
    name: &str,
    authors: i64,
    posts: i64,
    comments: i64,
    ceiling: Duration,
) -> RelationshipBenchResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    generate::create_authors_table(&db.pool).await;
    generate::create_posts_table(&db.pool).await;
    generate::create_comments_table(&db.pool).await;
    // A to-many relationship's to-side table needs a way to identify a row
    // being deleted/updated in its CDC stream (`RelationshipToManyRequiresReplicaIdentity`);
    // full replica identity satisfies that, matching the fixture tables in
    // `trellis/tests/defs_backfill_relationship.rs`.
    raw.batch_execute(
        "alter table posts replica identity full; alter table comments replica identity full",
    )
    .await
    .expect("set replica identity on relationship to-side tables");

    let authors_load = generate::load_authors(&db.pool, authors).await;
    // `_with_holdout`, not the plain loaders: deliberately leaves every
    // `ZERO_CHILDREN_MODULUS`-th author with zero posts/comments, so the
    // no-match (`COUNT -> 0`/`SUM -> NULL`) path is exercised at benchmark
    // scale, not just by the small-fixture engine tests.
    let posts_load = generate::load_posts_with_holdout(&db.pool, posts, authors).await;
    let comments_load = generate::load_comments_with_holdout(&db.pool, comments, authors).await;
    let load_ms = (authors_load + posts_load + comments_load).as_millis();

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author",
    )
    .await
    .expect("create comments relationship");

    let source_text = "TRANSFORM author_totals FROM authors SELECT \
         SUM(posts.word_count) AS word_sum, \
         COUNT(posts.word_count) AS post_count, \
         COUNT(comments.id) AS comment_count";
    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("id".to_string(), ValueType::Numeric)]);

    // The real end-to-end path a caller hits: target-table creation + the
    // direct set-based build (or ring fallback, if this shape ever regresses
    // to `Unsupported`), through `install_definition` — not
    // `backfill_definition` called directly.
    let backfill_start = Instant::now();
    install_definition(&db.pool, source_text, &source_columns, "public")
        .await
        .expect("install author_totals definition");
    let backfill_ms = backfill_start.elapsed().as_millis();

    let correctness_ok = check_correctness(&raw).await;
    let within_ceiling = backfill_ms <= ceiling.as_millis();

    RelationshipBenchResult {
        scenario: name.to_string(),
        authors,
        posts,
        comments,
        load_ms,
        backfill_ms,
        correctness_ok,
        ceiling_ms: ceiling.as_millis(),
        within_ceiling,
    }
}

/// Compares the backfilled `author_totals` against an independently written
/// `GROUP BY`/`LEFT JOIN` oracle (not [`trellis::dev::defs::render_relationship_select_sql`],
/// whose per-row correlated-subquery shape is meant for the small fixtures in
/// `trellis/tests` and doesn't scale to this benchmark's row counts) — an
/// exact-value check over every author, not just a row count or a sample.
/// Per-author `(word_sum, post_count, comment_count)`, keyed by author id.
type AuthorTotals = HashMap<String, (Option<String>, Option<String>, Option<String>)>;

async fn check_correctness(raw: &RawClient) -> bool {
    let oracle_sql = "select a.id::text, p.word_sum::text, coalesce(p.post_count, 0)::text, \
             coalesce(c.comment_count, 0)::text \
         from authors a \
         left join ( \
             select author, sum(word_count) as word_sum, count(word_count) as post_count \
             from posts group by author \
         ) p on p.author = a.id \
         left join ( \
             select author, count(id) as comment_count from comments group by author \
         ) c on c.author = a.id";
    let oracle: AuthorTotals = raw
        .query(oracle_sql, &[])
        .await
        .expect("run relationship oracle query")
        .into_iter()
        .map(|row| {
            let id: String = row.get(0);
            (id, (row.get(1), row.get(2), row.get(3)))
        })
        .collect();

    let target: AuthorTotals = raw
        .query(
            "select id::text, word_sum::text, post_count::text, comment_count::text \
             from author_totals",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|row| {
            let id: String = row.get(0);
            (id, (row.get(1), row.get(2), row.get(3)))
        })
        .collect();

    target == oracle
}
