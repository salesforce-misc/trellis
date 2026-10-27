//! Correctness tests for the four text functions against a real,
//! ephemeral Postgres instance via `testkit::TestCluster`. Each row goes
//! through the real 1-1 apply path (Postgres evaluates the functions in the
//! upsert that writes the target) and the target's text is compared with the
//! equivalent Postgres expression run directly over the same operands.
//!
//! `REGEXP_COUNT` is cross-checked with both literal and real-metacharacter
//! patterns, including ones that can match the empty string.

#[path = "support/render_lab.rs"]
mod render_lab;

use render_lab::{Lab, Outcome};
use testkit::TestCluster;
use trellis::defs::ast::ValueType;
use trellis::integer::IntWidth;

const TRANSFORM: &str = "TRANSFORM t FROM s SELECT OCTET_LENGTH(txt) AS o, CHAR_LENGTH(txt) AS c, \
     STRPOS(txt, pat) AS s";

async fn lab(db: &testkit::TestDatabase) -> Lab<'_> {
    Lab::new(
        db,
        "txt text, pat text",
        TRANSFORM,
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("txt", ValueType::Text),
            ("pat", ValueType::Text),
        ],
    )
    .await
}

/// What Postgres says for the three functions over `(txt, pat)`, as text.
async fn pg_expected(lab: &Lab<'_>, txt: Option<&str>, pat: Option<&str>) -> Vec<Option<String>> {
    let row = lab
        .client()
        .query_one(
            "select octet_length($1::text)::text, char_length($1::text)::text, \
                    strpos($1::text, $2::text)::text",
            &[&txt, &pat],
        )
        .await
        .expect("query");
    (0..3).map(|i| row.get(i)).collect()
}

async fn assert_matches_postgres(cases: &[(Option<&str>, Option<&str>)]) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut lab = lab(&db).await;
    for (txt, pat) in cases {
        let expected = pg_expected(&lab, *txt, *pat).await;
        let actual = lab.apply(&[("txt", *txt), ("pat", *pat)]).await;
        assert_eq!(
            actual,
            Outcome::Values(expected),
            "mismatch for ({txt:?}, {pat:?})"
        );
    }
}

/// `OCTET_LENGTH` and `CHAR_LENGTH` over ASCII, empty, multibyte and
/// astral text, and over `NULL`.
#[tokio::test]
async fn octet_length_and_char_length_match_postgres() {
    let cases: Vec<(Option<&str>, Option<&str>)> =
        ["hello", "", "café", "🎉", "a mix of café and 🎉 text"]
            .iter()
            .map(|t| (Some(*t), None))
            .chain([(None, None)])
            .collect();
    assert_matches_postgres(&cases).await;
}

#[tokio::test]
async fn octet_length_and_char_length_diverge_on_multibyte_text_in_postgres_too() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut lab = lab(&db).await;
    let Outcome::Values(values) = lab.apply(&[("txt", Some("café")), ("pat", None)]).await else {
        panic!("a data exception");
    };
    assert_eq!(values[0].as_deref(), Some("5"));
    assert_eq!(values[1].as_deref(), Some("4"));
}

#[tokio::test]
async fn strpos_matches_postgres() {
    let cases: Vec<(Option<&str>, Option<&str>)> = [
        ("hello world", "world"),
        ("hello world", "xyz"),
        ("hello world", ""),
        ("café bar", "bar"),
        ("café bar", "é"),
        ("", "x"),
        ("abcabc", "abc"),
    ]
    .iter()
    .map(|(h, n)| (Some(*h), Some(*n)))
    .collect();
    assert_matches_postgres(&cases).await;
}

#[tokio::test]
async fn strpos_absent_substring_returns_zero_not_an_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut lab = lab(&db).await;
    let Outcome::Values(values) = lab
        .apply(&[("txt", Some("hello")), ("pat", Some("xyz"))])
        .await
    else {
        panic!("a data exception");
    };
    assert_eq!(values[2].as_deref(), Some("0"));
}

/// `REGEXP_COUNT` takes a literal pattern, so each pattern is its own field.
/// Literal (metacharacter-free) patterns come first, then patterns using
/// real regex metacharacters, including ones that can match the empty
/// string. Postgres counts non-overlapping matches under its own ARE
/// dialect.
#[tokio::test]
async fn regexp_count_matches_postgres() {
    const PATTERNS: &[&str] = &[
        "abc", "o", "xyz", "café", "aa", "a.c", "a*", "colou?r", "cat|dog", "[0-9]+", "^apple",
        "e$",
    ];
    const TEXTS: &[&str] = &[
        "abcabcabc",
        "hello world",
        "café café café",
        "aaaa",
        "abc adc aec xyz",
        "aaa",
        "b",
        "color colour",
        "cat dog cat bird",
        "abc123def456",
        "apple banana apple",
        "one two three",
    ];
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let fields: Vec<String> = PATTERNS
        .iter()
        .enumerate()
        .map(|(i, p)| format!("REGEXP_COUNT(txt, '{p}') AS r{i}"))
        .collect();
    let mut lab = Lab::new(
        &db,
        "txt text",
        &format!("TRANSFORM t FROM s SELECT {}", fields.join(", ")),
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("txt", ValueType::Text),
        ],
    )
    .await;
    for text in TEXTS {
        let mut expected = Vec::new();
        for pattern in PATTERNS {
            let count: Option<String> = lab
                .client()
                .query_one(
                    "select regexp_count($1::text, $2::text)::text",
                    &[text, pattern],
                )
                .await
                .expect("query")
                .get(0);
            expected.push(count);
        }
        assert_eq!(
            lab.apply(&[("txt", Some(text))]).await,
            Outcome::Values(expected),
            "mismatch for regexp_count over {text:?}"
        );
    }
}
