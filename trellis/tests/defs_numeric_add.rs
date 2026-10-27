//! `a + b` over `numeric` columns computed by the real 1-1 apply path must be
//! byte-identical to Postgres's own `SELECT a + b`, across a matrix of
//! scale/precision edge cases, NULL propagation included. The row travels as
//! the staged image's text, is cast back to `numeric` by the rendered upsert,
//! and the target column's text is compared with independently written SQL.

#[path = "support/render_lab.rs"]
mod render_lab;

use render_lab::{Lab, Outcome};
use testkit::TestCluster;
use trellis::defs::ast::ValueType;

const TRANSFORM: &str = "TRANSFORM t FROM s SELECT a + b AS total";

/// Runs `SELECT (a::numeric + b::numeric)::text` against real Postgres,
/// binding both operands as text so no Rust-side numeric type mapping is
/// needed.
async fn pg_add(lab: &Lab<'_>, a: Option<&str>, b: Option<&str>) -> Option<String> {
    lab.client()
        .query_one(
            "select ($1::text::numeric + $2::text::numeric)::text",
            &[&a, &b],
        )
        .await
        .expect("query")
        .get(0)
}

async fn lab(db: &testkit::TestDatabase) -> Lab<'_> {
    Lab::new(
        db,
        "a numeric, b numeric",
        TRANSFORM,
        &[
            ("id", ValueType::Integer(trellis::integer::IntWidth::Int4)),
            ("a", ValueType::Numeric),
            ("b", ValueType::Numeric),
        ],
    )
    .await
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
    let mut lab = lab(&db).await;

    for (a, b) in CASES {
        let expected = pg_add(&lab, Some(a), Some(b)).await;
        let actual = lab.apply(&[("a", Some(a)), ("b", Some(b))]).await;
        assert_eq!(
            actual,
            Outcome::Values(vec![expected.clone()]),
            "mismatch for {a} + {b}: postgres={expected:?}"
        );
    }
}

#[tokio::test]
async fn null_propagation_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut lab = lab(&db).await;

    let cases: &[(Option<&str>, Option<&str>)] =
        &[(Some("5"), None), (None, Some("5")), (None, None)];

    for (a, b) in cases {
        let expected = pg_add(&lab, *a, *b).await;
        let actual = lab.apply(&[("a", *a), ("b", *b)]).await;
        assert_eq!(
            actual,
            Outcome::Values(vec![expected.clone()]),
            "mismatch for {a:?} + {b:?}"
        );
        assert_eq!(expected, None, "sanity: postgres NULL propagation");
    }
}
