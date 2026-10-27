//! Correctness tests for the evaluator against a real,
//! ephemeral Postgres instance via `testkit::TestCluster`. The bar per the
//! issue: `a + b` (and literal-involving additions) must be byte-identical
//! to Postgres's own `SELECT a + b`, across a matrix of scale/precision
//! edge cases, NULL propagation included — not a hand-checked assumption.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::eval::{RegexCache, Row, evaluate};

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
