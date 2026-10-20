//! Correctness tests for the four text functions (issue #64) against a real,
//! ephemeral Postgres instance via `testkit::TestCluster`. Follows the same
//! pattern as `defs_eval.rs`'s `pg_add`: bind the operands as text, run the
//! equivalent Postgres expression, and assert the evaluator's result is
//! byte-identical.
//!
//! `regexp_count` uses the `regex` crate (added as a direct dependency once
//! the epic owner approved it — see the issue #64 report) and is
//! cross-checked with both literal and real-metacharacter patterns,
//! including ones that can match the empty string.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::defs::eval::{RegexCache, Row, Value, evaluate};

fn text_types(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Text))
        .collect()
}

fn call_def(function: &str, arg_names: &[&str]) -> TransformDef {
    TransformDef {
        target: "t".to_string(),
        source: "s".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: function.to_string(),
                args: arg_names
                    .iter()
                    .map(|n| Expr::Column(n.to_string()))
                    .collect(),
            },
        }],
        predicate: Predicate::True,
        without_source_index: false,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn row1(name: &str, value: Option<&str>) -> Row {
    let mut row: Row = HashMap::new();
    row.insert(name.to_string(), value.map(|s| s.to_string()));
    row
}

fn row2(a_name: &str, a: Option<&str>, b_name: &str, b: Option<&str>) -> Row {
    let mut row: Row = HashMap::new();
    row.insert(a_name.to_string(), a.map(|s| s.to_string()));
    row.insert(b_name.to_string(), b.map(|s| s.to_string()));
    row
}

/// The evaluator's result for a single `out` field, as text.
///
/// Issue #111 gave these four functions their real Postgres return type
/// (`integer`, per `pg_proc.prorettype`) instead of the `Numeric` they
/// collapsed into before, so the value arrives as `Value::Integer`. The
/// comparisons below are against Postgres's own `::text` rendering, and an
/// exact integer's text is identical on both sides, so reading the value out
/// as text is exactly the right granularity here.
fn evaluator_numeric(def: &TransformDef, row: &Row, arg_names: &[&str]) -> String {
    let result = evaluate(def, row, &text_types(arg_names), &mut RegexCache::new())
        .expect("evaluation succeeds");
    match result.get("out").unwrap().as_ref().unwrap() {
        value @ (Value::Numeric(_) | Value::Integer(..)) => value.to_string(),
        other => panic!("expected an exact-numeric value, got {other:?}"),
    }
}

/// Runs a single-argument text function against real Postgres, casting the
/// result to text so no Rust-side integer-width mapping is needed (Postgres
/// returns these as `integer`, not `bigint`).
async fn pg_query_unary(pool: &trellis::Pool, sql: &str, arg: &str) -> String {
    let client = pool.get().await.expect("connection");
    let row = client
        .query_one(&format!("select ({sql})::text"), &[&arg])
        .await
        .expect("query");
    row.get(0)
}

/// As [`pg_query_unary`], for a two-argument function.
async fn pg_query_binary(pool: &trellis::Pool, sql: &str, a: &str, b: &str) -> String {
    let client = pool.get().await.expect("connection");
    let row = client
        .query_one(&format!("select ({sql})::text"), &[&a, &b])
        .await
        .expect("query");
    row.get(0)
}

const OCTET_LENGTH_CASES: &[&str] = &["hello", "", "café", "🎉", "a mix of café and 🎉 text"];

#[tokio::test]
async fn octet_length_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let def = call_def("OCTET_LENGTH", &["text_col"]);

    for text in OCTET_LENGTH_CASES {
        let expected = pg_query_unary(&db.pool, "octet_length($1::text)", text).await;
        let actual = evaluator_numeric(&def, &row1("text_col", Some(text)), &["text_col"]);
        assert_eq!(actual, expected, "mismatch for octet_length({text:?})");
    }
}

#[tokio::test]
async fn char_length_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let def = call_def("CHAR_LENGTH", &["text_col"]);

    for text in OCTET_LENGTH_CASES {
        let expected = pg_query_unary(&db.pool, "char_length($1::text)", text).await;
        let actual = evaluator_numeric(&def, &row1("text_col", Some(text)), &["text_col"]);
        assert_eq!(actual, expected, "mismatch for char_length({text:?})");
    }
}

#[tokio::test]
async fn octet_length_and_char_length_diverge_on_multibyte_text_in_postgres_too() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let bytes = pg_query_unary(&db.pool, "octet_length($1::text)", "café").await;
    let chars = pg_query_unary(&db.pool, "char_length($1::text)", "café").await;
    assert_eq!(bytes, "5");
    assert_eq!(chars, "4");
}

const STRPOS_CASES: &[(&str, &str)] = &[
    ("hello world", "world"),
    ("hello world", "xyz"),
    ("hello world", ""),
    ("café bar", "bar"),
    ("café bar", "é"),
    ("", "x"),
    ("abcabc", "abc"),
];

#[tokio::test]
async fn strpos_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let def = call_def("STRPOS", &["haystack", "needle"]);

    for (haystack, needle) in STRPOS_CASES {
        let expected =
            pg_query_binary(&db.pool, "strpos($1::text, $2::text)", haystack, needle).await;
        let actual = evaluator_numeric(
            &def,
            &row2("haystack", Some(haystack), "needle", Some(needle)),
            &["haystack", "needle"],
        );
        assert_eq!(
            actual, expected,
            "mismatch for strpos({haystack:?}, {needle:?})"
        );
    }
}

#[tokio::test]
async fn strpos_absent_substring_returns_zero_not_an_error() {
    let def = call_def("STRPOS", &["haystack", "needle"]);
    let actual = evaluator_numeric(
        &def,
        &row2("haystack", Some("hello"), "needle", Some("xyz")),
        &["haystack", "needle"],
    );
    assert_eq!(actual, "0");
}

/// Literal (metacharacter-free) patterns — a subset of what a real regex
/// engine accepts, still worth cross-checking on their own.
const REGEXP_COUNT_LITERAL_CASES: &[(&str, &str)] = &[
    ("abcabcabc", "abc"),
    ("hello world", "o"),
    ("hello world", "xyz"),
    ("café café café", "café"),
    ("aaaa", "aa"),
];

/// Patterns using real regex metacharacters (issue #64/#65 follow-up: once
/// `regex` became an approved direct dependency, `regexp_count` moved off
/// literal-only matching onto a global-match loop over the `regex` crate,
/// replicating Postgres's exact non-overlapping counting rule, including for
/// a pattern that can match the empty string — see `eval::regexp_count`).
/// Restricted to syntax common to Rust's `regex` crate and Postgres's
/// default ARE dialect (`.`, `*`, `+`, `?`, `[...]`, `|`, `^`, `$`), so a
/// mismatch here is a real semantic divergence, not a dialect quirk.
const REGEXP_COUNT_METACHARACTER_CASES: &[(&str, &str)] = &[
    ("abc adc aec xyz", "a.c"),
    ("aaa", "a*"),
    ("b", "a*"),
    ("color colour", "colou?r"),
    ("cat dog cat bird", "cat|dog"),
    ("abc123def456", "[0-9]+"),
    ("apple banana apple", "^apple"),
    ("one two three", "e$"),
];

#[tokio::test]
async fn regexp_count_matches_postgres_for_literal_patterns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let def = call_def("REGEXP_COUNT", &["text_col", "pattern"]);

    for (text, pattern) in REGEXP_COUNT_LITERAL_CASES {
        let expected =
            pg_query_binary(&db.pool, "regexp_count($1::text, $2::text)", text, pattern).await;
        let actual = evaluator_numeric(
            &def,
            &row2("text_col", Some(text), "pattern", Some(pattern)),
            &["text_col", "pattern"],
        );
        assert_eq!(
            actual, expected,
            "mismatch for regexp_count({text:?}, {pattern:?})"
        );
    }
}

#[tokio::test]
async fn regexp_count_matches_postgres_for_metacharacter_patterns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let def = call_def("REGEXP_COUNT", &["text_col", "pattern"]);

    for (text, pattern) in REGEXP_COUNT_METACHARACTER_CASES {
        let expected =
            pg_query_binary(&db.pool, "regexp_count($1::text, $2::text)", text, pattern).await;
        let actual = evaluator_numeric(
            &def,
            &row2("text_col", Some(text), "pattern", Some(pattern)),
            &["text_col", "pattern"],
        );
        assert_eq!(
            actual, expected,
            "mismatch for regexp_count({text:?}, {pattern:?})"
        );
    }
}

#[tokio::test]
async fn function_call_over_a_live_row_matches_evaluation_of_a_manual_image() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table docs (id integer primary key, body text);
             insert into docs (id, body) values (1, 'hello café world')",
        )
        .await
        .expect("seed table");

    let live_row = client
        .query_one("select body::text from docs where id = 1", &[])
        .await
        .expect("read back live row");
    let live_body: String = live_row.get(0);

    let def = call_def("CHAR_LENGTH", &["body"]);
    let live_image = row1("body", Some(&live_body));
    let manual_image = row1("body", Some("hello café world"));

    let from_live = evaluator_numeric(&def, &live_image, &["body"]);
    let from_manual = evaluator_numeric(&def, &manual_image, &["body"]);
    assert_eq!(from_live, from_manual);
}
