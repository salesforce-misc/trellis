//! Correctness tests for the evaluator (issue #24) against a real,
//! ephemeral Postgres instance via `testkit::TestCluster`. The bar per the
//! issue: `a + b` (and literal-involving additions) must be byte-identical
//! to Postgres's own `SELECT a + b`, across a matrix of scale/precision
//! edge cases, NULL propagation included — not a hand-checked assumption.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::eval::{
    RegexCache, RelationshipContext, Row, ToManyRelationship, ToOneRelationship, evaluate,
    evaluate_with_relationships,
};
use trellis::defs::model::RelationshipCardinality;

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn add_def() -> TransformDef {
    TransformDef {
        target: "t".to_string(),
        source: "s".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            },
        }],
        predicate: Predicate::True,
        without_source_index: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn row(a: Option<&str>, b: Option<&str>) -> Row {
    let mut row: Row = HashMap::new();
    row.insert("a".to_string(), a.map(|s| s.to_string()));
    row.insert("b".to_string(), b.map(|s| s.to_string()));
    row
}

/// Runs `SELECT (a::numeric + b::numeric)::text` against real Postgres,
/// binding both operands as text so no Rust-side numeric type mapping is
/// needed.
async fn pg_add(pool: &trellis::Pool, a: Option<&str>, b: Option<&str>) -> Option<String> {
    let client = pool.get().await.expect("connection");
    let row = client
        .query_one(
            "select ($1::text::numeric + $2::text::numeric)::text",
            &[&a, &b],
        )
        .await
        .expect("query");
    row.get(0)
}

fn evaluator_result(a: Option<&str>, b: Option<&str>) -> Option<String> {
    let def = add_def();
    let result = evaluate(
        &def,
        &row(a, b),
        &numeric_columns(&["a", "b"]),
        &mut RegexCache::new(),
    )
    .expect("evaluation succeeds");
    result.get("total").unwrap().as_ref().map(|n| n.to_string())
}

/// The correctness-bar matrix: scale/precision edge cases, negatives, large
/// values, and differing scales, each compared string-for-string against
/// real Postgres.
const CASES: &[(&str, &str)] = &[
    ("1.5", "2.25"),
    ("10", "0.001"),
    ("0", "0"),
    ("-5", "5"),
    ("-1.5", "2.25"),
    ("1.5", "-2.25"),
    ("-1.5", "-2.25"),
    ("1.50", "1.5"),
    ("0.1", "0.2"),
    ("100.00", "0.001"),
    ("999999999999999999999999999999", "1"),
    ("-999999999999999999999999999999", "-1"),
    ("0.0000000001", "0.0000000002"),
    ("123456789.123456789", "987654321.987654321"),
    ("5", "-5"),
    ("-0", "0"),
];

#[tokio::test]
async fn addition_matches_postgres_across_the_numeric_matrix() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    for (a, b) in CASES {
        let expected = pg_add(&db.pool, Some(a), Some(b)).await;
        let actual = evaluator_result(Some(a), Some(b));
        assert_eq!(
            actual, expected,
            "mismatch for {a} + {b}: evaluator={actual:?} postgres={expected:?}"
        );
    }
}

#[tokio::test]
async fn null_propagation_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let cases: &[(Option<&str>, Option<&str>)] =
        &[(Some("5"), None), (None, Some("5")), (None, None)];

    for (a, b) in cases {
        let expected = pg_add(&db.pool, *a, *b).await;
        let actual = evaluator_result(*a, *b);
        assert_eq!(actual, expected, "mismatch for {a:?} + {b:?}");
        assert_eq!(expected, None, "sanity: postgres NULL propagation");
    }
}

#[tokio::test]
async fn evaluation_is_deterministic_across_repeated_calls() {
    let def = add_def();
    let r = row(Some("10.5"), Some("2.25"));
    let first = evaluate(
        &def,
        &r,
        &numeric_columns(&["a", "b"]),
        &mut RegexCache::new(),
    )
    .unwrap();
    let second = evaluate(
        &def,
        &r,
        &numeric_columns(&["a", "b"]),
        &mut RegexCache::new(),
    )
    .unwrap();
    assert_eq!(first, second);
}

/// Evaluating a manually-constructed row image and evaluating the
/// equivalent row read back from a live Postgres table land on the same
/// result — the property the ticket calls "staged image equals live read".
#[tokio::test]
async fn staged_image_matches_evaluation_of_the_equivalent_live_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table orders (id integer primary key, a numeric, b numeric);
             insert into orders (id, a, b) values (1, 12.345, 6.7)",
        )
        .await
        .expect("seed table");

    let live_row = client
        .query_one("select a::text, b::text from orders where id = 1", &[])
        .await
        .expect("read back live row");
    let live_a: String = live_row.get(0);
    let live_b: String = live_row.get(1);

    let live_image = row(Some(&live_a), Some(&live_b));
    let manual_image = row(Some("12.345"), Some("6.7"));

    let def = add_def();
    let from_live = evaluate(
        &def,
        &live_image,
        &numeric_columns(&["a", "b"]),
        &mut RegexCache::new(),
    )
    .unwrap();
    let from_manual = evaluate(
        &def,
        &manual_image,
        &numeric_columns(&["a", "b"]),
        &mut RegexCache::new(),
    )
    .unwrap();
    assert_eq!(from_live, from_manual);
}

/// A to-one relationship path (`category.name`, issue #28) evaluates to the
/// same enrichment a Postgres `LEFT JOIN` produces — for from-rows that match
/// a to-side row, ones that don't (NULL enrichment, from-row survives), a NULL
/// FK, and a matched to-side row whose referenced column is itself NULL.
#[tokio::test]
async fn to_one_relationship_matches_postgres_left_join() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table categories (id integer primary key, name text);
             insert into categories (id, name) values (10, 'Widgets'), (20, null);
             create table products (id integer primary key, category_id integer);
             insert into products (id, category_id)
                 values (1, 10), (2, 20), (3, 99), (4, null)",
        )
        .await
        .expect("seed tables");

    // Postgres's own LEFT JOIN is the oracle: product id -> joined category name.
    let expected: HashMap<i32, Option<String>> = {
        let rows = client
            .query(
                "select p.id, c.name
                   from products p
                   left join categories c on p.category_id = c.id",
                &[],
            )
            .await
            .expect("left join");
        rows.into_iter()
            .map(|r| (r.get::<_, i32>(0), r.get::<_, Option<String>>(1)))
            .collect()
    };

    // Build the to-one context from the categories table read back as text —
    // the same text image the staging path would carry (issue #30 wires this).
    let mut to_columns = HashMap::new();
    to_columns.insert("name".to_string(), ValueType::Text);
    let mut to_rows_by_key = HashMap::new();
    for cat in client
        .query("select id::text, name::text from categories", &[])
        .await
        .expect("read categories")
    {
        let id: String = cat.get(0);
        let name: Option<String> = cat.get(1);
        let mut r: Row = HashMap::new();
        r.insert("id".to_string(), Some(id.clone()));
        r.insert("name".to_string(), name);
        to_rows_by_key.insert(id, r);
    }
    let mut by_name = HashMap::new();
    by_name.insert(
        "category".to_string(),
        ToOneRelationship {
            from_col: "category_id".to_string(),
            cardinality: RelationshipCardinality::ToOne,
            to_columns,
            to_rows_by_key,
        },
    );
    let rels = RelationshipContext::new(by_name);

    let def = TransformDef {
        target: "enriched_products".to_string(),
        source: "products".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::RelationshipPath {
                rel: "category".to_string(),
                column: "name".to_string(),
            },
        }],
        predicate: Predicate::True,
        without_source_index: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    // Each product's FK read back as text, matching a staged image.
    for product in client
        .query("select id, category_id::text from products", &[])
        .await
        .expect("read products")
    {
        let id: i32 = product.get(0);
        let category_id: Option<String> = product.get(1);
        let mut from_row: Row = HashMap::new();
        from_row.insert("category_id".to_string(), category_id);

        let result = evaluate_with_relationships(
            &def,
            &from_row,
            &numeric_columns(&["category_id"]),
            &rels,
            &mut RegexCache::new(),
        )
        .expect("evaluation succeeds");
        let actual = result["category_name"].as_ref().map(|v| v.to_string());

        assert_eq!(
            actual, expected[&id],
            "product {id}: evaluator={actual:?} left-join={:?}",
            expected[&id]
        );
    }
}

/// Builds a to-many `comments` context (join column `post_id`, referenced
/// numeric column `word_count`) from the current `comments` table, read back
/// as text exactly as a staged image would carry it (issue #30 wires this).
async fn comments_to_many(client: &tokio_postgres::Client) -> RelationshipContext {
    let mut to_columns = HashMap::new();
    to_columns.insert("word_count".to_string(), ValueType::Numeric);

    let mut to_rows_by_key: HashMap<String, Vec<Row>> = HashMap::new();
    for c in client
        .query("select post_id::text, word_count::text from comments", &[])
        .await
        .expect("read comments")
    {
        // A comment with a NULL post_id joins to no post — omit it, just as
        // SQL's `NULL` join key never matches.
        let Some(post_id): Option<String> = c.get(0) else {
            continue;
        };
        let word_count: Option<String> = c.get(1);
        let mut r: Row = HashMap::new();
        r.insert("word_count".to_string(), word_count);
        to_rows_by_key.entry(post_id).or_default().push(r);
    }

    let mut to_many = HashMap::new();
    to_many.insert(
        "comments".to_string(),
        ToManyRelationship {
            from_col: "id".to_string(),
            to_columns,
            to_rows_by_key,
        },
    );
    RelationshipContext::default().with_to_many(to_many)
}

/// Asserts the evaluator's to-many aggregate enrichment for every post matches
/// Postgres's correlated aggregate subqueries, byte for byte, given whatever
/// state `comments` is currently in.
async fn assert_to_many_matches_postgres(client: &tokio_postgres::Client) {
    // Postgres's own correlated aggregates are the oracle, rendered to text.
    let expected: HashMap<i32, (Option<String>, Option<String>)> = client
        .query(
            "select p.id,
                    (select sum(c.word_count) from comments c where c.post_id = p.id)::text,
                    (select count(c.word_count) from comments c where c.post_id = p.id)::text
               from posts p",
            &[],
        )
        .await
        .expect("correlated aggregates")
        .into_iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                (r.get::<_, Option<String>>(1), r.get::<_, Option<String>>(2)),
            )
        })
        .collect();

    let rels = comments_to_many(client).await;
    let types = numeric_columns(&["id"]);

    let sum_def = TransformDef {
        target: "post_stats".to_string(),
        source: "posts".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "total_words".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::RelationshipPath {
                        rel: "comments".to_string(),
                        column: "word_count".to_string(),
                    }],
                },
            },
            FieldDef {
                name: "comment_count".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: vec![Expr::RelationshipPath {
                        rel: "comments".to_string(),
                        column: "word_count".to_string(),
                    }],
                },
            },
        ],
        predicate: Predicate::True,
        without_source_index: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    for post in client
        .query("select id from posts", &[])
        .await
        .expect("read posts")
    {
        let id: i32 = post.get(0);
        let mut from_row: Row = HashMap::new();
        from_row.insert("id".to_string(), Some(id.to_string()));

        let result =
            evaluate_with_relationships(&sum_def, &from_row, &types, &rels, &mut RegexCache::new())
                .expect("evaluation succeeds");
        let actual_sum = result["total_words"].as_ref().map(|v| v.to_string());
        let actual_count = result["comment_count"].as_ref().map(|v| v.to_string());
        let (expected_sum, expected_count) = &expected[&id];

        assert_eq!(
            &actual_sum, expected_sum,
            "post {id}: SUM evaluator={actual_sum:?} correlated={expected_sum:?}"
        );
        assert_eq!(
            &actual_count, expected_count,
            "post {id}: COUNT evaluator={actual_count:?} correlated={expected_count:?}"
        );
    }
}

#[tokio::test]
async fn to_many_aggregate_matches_postgres_correlated_aggregate_across_mutations() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table posts (id integer primary key);
             insert into posts (id) values (1), (2), (3);
             create table comments (
                 id integer primary key,
                 post_id integer,
                 word_count integer
             );
             insert into comments (id, post_id, word_count) values
                 (100, 1, 10), (101, 1, 20), (102, 1, null),
                 (103, 2, 5);",
        )
        .await
        .expect("seed tables");

    // Initial state: post 1 has {10, 20, null}, post 2 has {5}, post 3 empty.
    assert_to_many_matches_postgres(&client).await;

    // Insert: a new comment on the previously-empty post 3.
    client
        .execute(
            "insert into comments (id, post_id, word_count) values (104, 3, 7)",
            &[],
        )
        .await
        .expect("insert");
    assert_to_many_matches_postgres(&client).await;

    // Update: change a comment's word_count.
    client
        .execute("update comments set word_count = 99 where id = 100", &[])
        .await
        .expect("update");
    assert_to_many_matches_postgres(&client).await;

    // Re-parent: move a comment from post 1 to post 2.
    client
        .execute("update comments set post_id = 2 where id = 101", &[])
        .await
        .expect("re-parent");
    assert_to_many_matches_postgres(&client).await;

    // Delete: remove the last comment from post 2's original single comment.
    client
        .execute("delete from comments where id = 103", &[])
        .await
        .expect("delete");
    assert_to_many_matches_postgres(&client).await;

    // Delete every comment on post 1 — back to the empty set (COUNT 0, SUM NULL).
    client
        .execute("delete from comments where post_id = 1", &[])
        .await
        .expect("empty post 1");
    assert_to_many_matches_postgres(&client).await;
}
