//! Expression conformance: random typed expressions checked against a real
//! Postgres (`generative::expr_conformance`), plus the deterministic pins of
//! each precedence tier and each refusal.
//!
//! The named tests run in the normal suite. The randomized properties are
//! `#[ignore]`d like the suite's other generative properties; a smoke
//! version of each runs at a small case count in the normal suite. Larger
//! sweeps: `PROPTEST_CASES=2000 cargo test -p generative --test
//! expr_conformance -- --ignored`.

use std::cell::Cell;

use generative::expr_conformance::{Agreement, Divergence, Fixture, columns};
use generative::expr_gen::strategy::{Draw, any_expr, near_miss};
use generative::expr_gen::{self, Profile, TextStyle, near_misses, render_text};
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed, TestRunner};
use testkit::{TestCluster, TestDatabase};
use trellis::dev::defs::ast::{Expr, Operator, ValueType};
use trellis::dev::defs::{parse, registry};

const ROWS: usize = 48;

struct Harness {
    // Field order is drop order: the connections close before the database
    // is dropped, and the runtime that drives them outlives all of it.
    fixture: Fixture,
    _db: TestDatabase,
    _cluster: TestCluster,
    runtime: tokio::runtime::Runtime,
    admitted: Cell<usize>,
    refused: Cell<usize>,
    gaps: Cell<usize>,
}

thread_local! {
    static HARNESS: Harness = {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let cluster = TestCluster::start().long_lived();
        let db = runtime.block_on(cluster.create_isolated_database());
        let fixture = runtime.block_on(Fixture::new(&db, ROWS));
        Harness {
            runtime,
            _cluster: cluster,
            _db: db,
            fixture,
            admitted: Cell::new(0),
            refused: Cell::new(0),
            gaps: Cell::new(0),
        }
    };
}

fn check_text(text: &str, intended: Option<&Expr>) -> Result<Agreement, Divergence> {
    HARNESS.with(|h| {
        let result = h.runtime.block_on(h.fixture.check(text, intended));
        match &result {
            Ok(Agreement::Admitted) => h.admitted.set(h.admitted.get() + 1),
            Ok(Agreement::RefusedAndRaises(_)) => h.refused.set(h.refused.get() + 1),
            Ok(Agreement::KnownGap(_)) => h.gaps.set(h.gaps.get() + 1),
            Err(_) => {}
        }
        result
    })
}

fn report() {
    HARNESS.with(|h| {
        eprintln!(
            "expression conformance: {} admitted and agreeing, {} refused and raising, \
             {} tolerated as known gaps",
            h.admitted.get(),
            h.refused.get(),
            h.gaps.get()
        );
    });
}

fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The seed of the normal suite's runs: they draw the same cases every
/// time, so a red `verify` is never a matter of luck. The `#[ignore]`d
/// properties draw fresh cases each run (or `PROPTEST_RNG_SEED`'s). A failure
/// prints the expression text, which replays as a pin in
/// `hard_cases_agree_with_postgres`.
const SMOKE_SEED: u64 = 1413;

fn runner(cases: u32, seed: Option<u64>) -> TestRunner {
    let mut config = Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    };
    if let Some(seed) = seed {
        config.rng_seed = RngSeed::Fixed(seed);
    }
    TestRunner::new(config)
}

/// Type, Value and Round trip over `cases` random expressions, drawn over
/// `columns` (none: constants only, which Postgres folds at plan time).
fn run_expression_property(cases: u32, columns: &[expr_gen::Column], seed: Option<u64>) {
    let strategy = any_expr(columns, Profile::CONFORMANCE);
    let result = runner(cases, seed).run(&strategy, |draw: Draw| {
        let text = draw.text();
        check_text(&text, Some(&draw.expr))
            .map(|_| ())
            .map_err(|d| TestCaseError::fail(d.to_string()))
    });
    report();
    if let Err(e) = result {
        panic!("{e}");
    }
}

/// Refusal over `cases` random near misses.
fn run_refusal_property(cases: u32, seed: Option<u64>) {
    let strategy = near_miss(&columns());
    let result = runner(cases, seed).run(&strategy, |miss| {
        HARNESS
            .with(|h| h.fixture.check_refusal(&miss))
            .map(|_| ())
            .map_err(|d| TestCaseError::fail(d.to_string()))
    });
    if let Err(e) = result {
        panic!("{e}");
    }
}

#[test]
fn expressions_conform_to_postgres_smoke() {
    run_expression_property(cases(60), &columns(), Some(SMOKE_SEED));
}

#[test]
fn constant_expressions_conform_to_postgres_smoke() {
    run_expression_property(cases(60), &[], Some(SMOKE_SEED));
}

#[test]
fn near_misses_are_refused_smoke() {
    run_refusal_property(cases(60), Some(SMOKE_SEED));
}

#[test]
#[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
fn property_expressions_conform_to_postgres() {
    run_expression_property(cases(1000), &columns(), None);
}

/// No column in sight: every subexpression is a constant Postgres folds at
/// plan time, which is where the define-time range refusals live.
#[test]
#[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
fn property_constant_expressions_conform_to_postgres() {
    run_expression_property(cases(1000), &[], None);
}

#[test]
#[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
fn property_near_misses_are_refused() {
    run_refusal_property(cases(1000), None);
}

// ---------------------------------------------------------------------------
// Deterministic pins
// ---------------------------------------------------------------------------

fn field_expr(text: &str) -> Expr {
    parse(&format!("TRANSFORM t FROM x SELECT {text} AS v"))
        .unwrap_or_else(|e| panic!("`{text}` does not parse: {e}"))
        .fields
        .remove(0)
        .expr
}

fn bin(op: Operator, lhs: Expr, rhs: Expr) -> Expr {
    Expr::BinaryOp {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

/// Every pair of registry operators groups by the precedence table: the
/// tighter operator takes its operands first, and equal precedence groups
/// to the left. Generic over the registry, so a new operator is pinned the
/// day it is added.
#[test]
fn every_precedence_tier_groups_as_the_registry_says() {
    for p in registry::OPERATORS {
        for q in registry::OPERATORS {
            let text = format!("a {} b {} c", p.symbol, q.symbol);
            let (a, b, c) = (
                Expr::Column("a".into()),
                Expr::Column("b".into()),
                Expr::Column("c".into()),
            );
            // Two comparisons of one level do not chain, in Postgres or here.
            if p.precedence == q.precedence && p.operator.is_non_associative() {
                let err = parse(&format!("TRANSFORM t FROM x SELECT {text} AS v"))
                    .expect_err(&format!("`{text}` should not parse"));
                assert!(err.to_string().contains("do not chain"), "`{text}`: {err}");
                continue;
            }
            let expected = if p.precedence < q.precedence {
                bin(p.operator, a, bin(q.operator, b, c))
            } else {
                bin(q.operator, bin(p.operator, a, b), c)
            };
            assert_eq!(field_expr(&text), expected, "`{text}`");
        }
    }
}

/// Parentheses override the table, on either side.
#[test]
fn parentheses_override_precedence() {
    for p in registry::OPERATORS {
        for q in registry::OPERATORS {
            let left = format!("(a {} b) {} c", p.symbol, q.symbol);
            let right = format!("a {} (b {} c)", p.symbol, q.symbol);
            let (a, b, c) = (
                Expr::Column("a".into()),
                Expr::Column("b".into()),
                Expr::Column("c".into()),
            );
            assert_eq!(
                field_expr(&left),
                bin(q.operator, bin(p.operator, a.clone(), b.clone()), c.clone())
            );
            assert_eq!(
                field_expr(&right),
                bin(p.operator, a, bin(q.operator, b, c))
            );
        }
    }
}

/// The text a user writes with the minimal parentheses, and the fully
/// parenthesized text, parse to the same tree (the tree written from).
#[test]
fn both_spellings_of_a_tree_parse_back_to_it() {
    let tree = bin(
        Operator::GreaterThan,
        bin(
            Operator::Add,
            Expr::Column("ia".into()),
            bin(
                Operator::Add,
                Expr::Column("ib".into()),
                Expr::NumberLiteral("1".into()),
            ),
        ),
        Expr::Column("ic".into()),
    );
    for minimal in [true, false] {
        let text = render_text(
            &tree,
            TextStyle {
                minimal,
                ..TextStyle::default()
            },
        );
        assert_eq!(field_expr(&text), tree, "`{text}`");
    }
    // The minimal spelling keeps the parentheses that carry meaning.
    let text = render_text(
        &tree,
        TextStyle {
            minimal: true,
            ..TextStyle::default()
        },
    );
    assert_eq!(text, "ia + (ib + 1) > ic");
}

/// Expressions Postgres and the engine must agree on, each a known hard
/// case: boundary arithmetic, mixed widths, floats against literals,
/// `COALESCE` unification, and the plan-time constant folds.
#[test]
fn hard_cases_agree_with_postgres() {
    let admitted = [
        "i4a + 2147483647",
        "i2a + i2b",
        "i2a + 1",
        "i4a + i8a",
        "i8a + na",
        "f4a + f4b",
        "f4a + i4a",
        "f8a + na",
        "f4a > 0.5",
        "f4a > f8b",
        "na > i8a",
        "i2a > i8b",
        "STRPOS(ta, 'b') > 0",
        "CHAR_LENGTH(ta) + OCTET_LENGTH(tb)",
        "REGEXP_COUNT(ta, 'a|b')",
        "COALESCE(i4a, na)",
        "COALESCE(f4a, na)",
        "COALESCE(i2a, i8a, 1)",
        "COALESCE(ta, 'z')",
        "COALESCE(da, DATE '2024-01-01')",
        "COALESCE(tsa, tsb)",
        "COALESCE(ba, bb)",
        // The planner keeps only the first constant argument, so the
        // overflowing one is never evaluated.
        "COALESCE(5, 2147483647 + 1)",
        "COALESCE(i4a, 5, 2147483647 + 1)",
        "REAL '1.5' + f4a",
        // A NaN divided by zero is NaN, not an error.
        "DOUBLE PRECISION 'NaN' / 0",
        "(REAL 'NaN' + REAL '0') / -(REAL '0') > 0",
        "DOUBLE PRECISION 'NaN' > f8a",
        "CAST('Infinity' AS REAL) + 1",
        "DATE '2024-02-29'",
        "(1 + 2) > 3",
        "1 + 2 + 3 > 4 + 5",
        "9223372036854775807 + 1.5",
        "99999999999999999999 + i2a",
        "f8a + 99999999999999999999",
    ];
    for text in admitted {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            Ok(other) => panic!("`{text}` should be admitted, got {other:?}"),
            Err(d) => panic!("{d}"),
        }
    }
}

/// Each row of the arithmetic table, by Postgres's own answer: the type, the
/// value on every row (the checker compares the user's text with both
/// renderers) and the round trip.
#[test]
fn arithmetic_rows_agree_with_postgres() {
    let admitted = [
        // `-` and `*`, every width pairing and the float rule.
        "i2a - i2b",
        "i2a * i2b",
        "i2a - i4b",
        "i4a * i8b",
        "i8a - na",
        "na * nb",
        "f4a - f4b",
        "f4a * f8b",
        "f4a * i2a",
        "na - f4a",
        "i4a - 1",
        "i4a * -1",
        "1 - i4a",
        // `/`
        "na / nb",
        "i2a / na",
        "na / i8a",
        "f4a / f4b",
        "f4a / i4a",
        "i4a / f8b",
        "na / f4a",
        "na / 2",
        "na / -3",
        // `%`
        "i2a % i2b",
        "i2a % i8b",
        "i4a % na",
        "na % nb",
        "i4a % -1",
        "i8a % -1",
        "i4a % 7",
        "-7 % 2",
        "na % 3",
        // Prefix minus, `ABS`
        "-i2a",
        "-i4a",
        "-i8a",
        "-na",
        "-f4a",
        "-f8a",
        "- -i4a",
        "-(-na)",
        "-REAL '1.5'",
        "ABS(i2a)",
        "ABS(i4a)",
        "ABS(i8a)",
        "ABS(na)",
        "ABS(f4a)",
        "ABS(f8a)",
        "ABS(-5)",
        "ABS(-2147483647)",
        // `CEIL`, `FLOOR`, `TRUNC`, `ROUND`
        "CEIL(na)",
        "CEIL(f8a)",
        "FLOOR(na)",
        "FLOOR(f8a)",
        "TRUNC(na)",
        "TRUNC(f8a)",
        "ROUND(na)",
        "ROUND(na, 2)",
        "ROUND(na, 0)",
        "ROUND(na, -2)",
        "ROUND(na, 20)",
        "TRUNC(na, 1)",
        "TRUNC(na, -1)",
        "ROUND(-2.5)",
        "ROUND(-2.5, 0)",
        "ROUND(na) + 2147483647",
        // Widening casts
        "CAST(i2a AS integer)",
        "CAST(i2a AS bigint)",
        "CAST(i4a AS bigint)",
        "CAST(i2a AS numeric)",
        "CAST(i4a AS numeric)",
        "CAST(i8a AS numeric)",
        "CAST(i4a AS numeric) / i4b",
        "CAST(i4a AS bigint) * 4294967296",
        "CAST(5 AS bigint)",
        // Literals typed by value
        "-2147483648",
        "-2147483649",
        "-9223372036854775808",
        "-9223372036854775809",
        "i4a + -2147483648",
        "i4a * -2147483648",
        // Precedence
        "i4a - i2a * i2b",
        "-i4a * i2b",
        "i4a - i2a - i2b",
        "i4a * i2a % i2b",
        "i4a % i2a * i2b",
        "na - nb * nb > nb",
        // Data errors are left to the rows
        "i4a * i4b",
        "i8a * i8b",
        "i2a * i2b * i2a",
        "-i4a",
        "ABS(i4a)",
        "na / nb",
        "f8a / f8b",
        "f8a * f8b",
        "f4a * f4b",
    ];
    for text in admitted {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            Ok(other) => panic!("`{text}` should be admitted, got {other:?}"),
            Err(d) => panic!("{d}"),
        }
    }
}

/// Each comparison and logic row of the issue's table, by Postgres's own
/// answer: the type, the value on every row (the checker compares the user's
/// text with both renderers, so `NULL` on either side, `NaN` and the
/// boundary rows are all covered) and the round trip.
#[test]
fn comparison_and_logic_rows_agree_with_postgres() {
    let mut admitted: Vec<String> = Vec::new();
    // Every operator over one pair of each family, with a column on each
    // side and against a literal.
    let pairs = [
        ("i2a", "i2b"),
        ("i2a", "i8b"),
        ("i4a", "nb"),
        ("na", "nb"),
        ("na", "i4b"),
        ("f4a", "f4b"),
        ("f4a", "f8b"),
        ("f8a", "f8b"),
        ("da", "db"),
        ("tsa", "tsb"),
        ("tza", "tzb"),
        ("tma", "tmb"),
        ("tta", "ttb"),
        ("ba", "bb"),
        ("ua", "ub"),
        ("ta", "tb"),
        ("ta", "'abc'"),
        ("i4a", "1000"),
        ("f4a", "1.5"),
        ("f8a", "0"),
        ("1.5", "f4a"),
        ("na", "1.5"),
        ("da", "DATE '2024-02-29'"),
        ("tsa", "TIMESTAMP '1970-01-01 00:00:00.5'"),
    ];
    for (a, b) in pairs {
        for op in [
            "=",
            "<>",
            "!=",
            "<",
            "<=",
            ">",
            ">=",
            "IS DISTINCT FROM",
            "IS NOT DISTINCT FROM",
        ] {
            // Text orders by collation: refused (pinned below).
            if a == "ta" && ["<", "<=", ">", ">="].contains(&op) {
                continue;
            }
            admitted.push(format!("{a} {op} {b}"));
        }
    }
    admitted.extend(
        [
            // NaN is equal to itself and sorts above everything; -0 equals 0.
            "f8a = DOUBLE PRECISION 'NaN'",
            "DOUBLE PRECISION 'NaN' = DOUBLE PRECISION 'NaN'",
            "REAL 'NaN' >= f4a",
            "DOUBLE PRECISION 'NaN' > DOUBLE PRECISION 'Infinity'",
            "REAL '-0' = REAL '0'",
            "f4a IS NOT DISTINCT FROM REAL 'NaN'",
            "f8a < DOUBLE PRECISION 'Infinity'",
            "na = nb",
            // A float against a bare literal: Postgres widens the literal.
            "f4a = 0.1",
            "f4a <> 16777217",
            "f8a = 9007199254740993",
            "0.1 = f4a",
            "f8a IS DISTINCT FROM 1",
            // NULL on either side, in each form.
            "i4a = i4b",
            "i4a IS DISTINCT FROM i4b",
            "i4a IS NOT DISTINCT FROM i4b",
            // Three-valued logic.
            "ba AND bb",
            "ba OR bb",
            "NOT ba",
            "ba AND NOT bb",
            "NOT ba OR bb",
            "NOT (ba AND bb)",
            "NOT ba AND NOT bb",
            "(i4a > 0) AND (i4b > 0)",
            "i4a > 0 OR i4b > 0",
            "i4a > 0 AND ba OR bb",
            "ba IS NULL",
            "ba IS NOT NULL",
            "i4a IS NULL",
            "ta IS NOT NULL",
            "da IS NULL",
            "f8a IS NULL",
            "(i4a > i4b) IS NULL",
            "i4a > i4b IS NOT NULL",
            "ba IS NULL IS NULL",
            "ba IS NULL = bb",
            "NOT ba IS NULL",
            "NOT i4a IS NULL",
            "NOT i4a = i4b",
            "NOT i4a IS DISTINCT FROM i4b",
            "i4a IS NULL OR i4b IS NULL",
            "i4a + 1 > i4b * 2",
            "i4a - i4b = 0",
            "-i4a < 0",
            "ba = NOT bb",
            "ba = (NOT bb)",
            "ba = bb IS NULL",
            "(ba = bb) = (bb = ba)",
            "ba IS DISTINCT FROM bb",
            // CASE.
            "CASE WHEN ba THEN i4a END",
            "CASE WHEN ba THEN i4a ELSE i4b END",
            "CASE WHEN ba THEN 1 ELSE 2 END",
            "CASE WHEN ba THEN 1 ELSE 2.5 END",
            "CASE WHEN ba THEN 1 END",
            "CASE WHEN ba THEN i2a ELSE i8a END",
            "CASE WHEN ba THEN f4a ELSE na END",
            "CASE WHEN ba THEN na ELSE f4a END",
            "CASE WHEN ba THEN f4a ELSE f8a END",
            "CASE WHEN ba THEN i2a ELSE i4a END",
            "CASE WHEN ba THEN ta ELSE 'x' END",
            "CASE WHEN ba THEN da ELSE DATE '2024-01-01' END",
            "CASE WHEN ba THEN bb ELSE NOT bb END",
            "CASE WHEN ba THEN i4a WHEN bb THEN i4b ELSE 0 END",
            "CASE WHEN i4a > 0 THEN 'pos' WHEN i4a < 0 THEN 'neg' ELSE 'zero' END",
            "CASE WHEN i4a IS NULL THEN 0 ELSE i4a END",
            "CASE WHEN ba AND bb THEN i4a END",
            "CASE WHEN CASE WHEN ba THEN bb END THEN 1 END",
            "CASE WHEN ba THEN CASE WHEN bb THEN 1 ELSE 2 END ELSE 3 END",
            "CASE WHEN ba THEN i4a END + 1",
            "1 + CASE WHEN ba THEN i4a ELSE 0 END",
            "CASE WHEN ba THEN i4a END IS NULL",
            "CASE WHEN ba THEN i4a END > 0",
            "COALESCE(CASE WHEN ba THEN i4a END, 0)",
            "CASE WHEN i4a <> 0 THEN na / i4a END",
            "CASE WHEN i4b <> 0 THEN i4a % i4b END",
            "CASE WHEN ba THEN 'a' ELSE 'c' END",
            // NULLIF.
            "NULLIF(i4a, 0)",
            "NULLIF(i4a, i4b)",
            "NULLIF(i2a, i8b)",
            "NULLIF(i8a, i2b)",
            "NULLIF(na, 0)",
            "NULLIF(na, i4a)",
            "NULLIF(na, nb)",
            "NULLIF(f4a, f8b)",
            "NULLIF(f8a, 0)",
            "NULLIF(f4a, 0.5)",
            "NULLIF(ta, '')",
            "NULLIF(da, db)",
            "NULLIF(ba, bb)",
            "NULLIF(ua, ub)",
            "NULLIF(tsa, tsb)",
            "na / NULLIF(nb, 0)",
            "na / NULLIF(i4a, 0)",
            "i4a % NULLIF(i4b, 0)",
            "NULLIF(i4a, 0) IS NULL",
            "COALESCE(na / NULLIF(nb, 0), 0)",
            "CASE WHEN NULLIF(i4a, 0) IS NULL THEN 0 ELSE 1 END",
            "NULLIF(NULLIF(i4a, 0), 1)",
            // Constants.
            "1 = 1",
            "1 < 2 AND 2 < 3",
            "NOT (1 = 2)",
            "CASE WHEN 1 = 1 THEN 1 ELSE 2 END",
            "CASE WHEN 1 = 2 THEN 1 END",
            "CASE WHEN 1 = 2 THEN 1 ELSE 5 END",
            "NULLIF(1, 1)",
            "NULLIF(1, 2)",
            "1 IS NULL",
            "NULLIF(1, 1) IS NULL",
            "NULLIF(1.5, 1.5) / 0",
            "1.5 / NULLIF(2, 2) + 2147483647",
            "COALESCE(NULLIF(1, 1), 7)",
            "COALESCE(CASE WHEN 1 = 2 THEN 1 END, 7)",
            "2147483647 > 1",
            "CASE WHEN ba THEN 1 ELSE 2 END = 2",
        ]
        .map(String::from),
    );
    let mut failures = Vec::new();
    for text in &admitted {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            Ok(other) => failures.push(format!("`{text}` should be admitted, got {other:?}")),
            Err(d) => failures.push(d.to_string()),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Plan-time constants inside CASE branches and AND/OR operands: Postgres
/// simplifies a branch's result whatever its condition (so `CASE WHEN x > 0
/// THEN 1 / 0 END` fails every row), and stops at the first constant
/// condition (so a branch after `WHEN true`, or after a false `AND`
/// operand, is never simplified). Define refuses what Postgres rejects and
/// admits what it drops.
#[test]
fn constants_in_branches_are_refused_exactly_where_postgres_folds_them() {
    let refused = [
        "CASE WHEN i4a > 0 THEN 1.5 / 0 END",
        "CASE WHEN i4a > 0 THEN 1 ELSE 1.5 / 0 END",
        "CASE WHEN i4a > 0 THEN 1 WHEN i4b > 0 THEN 5 % 0 END",
        "CASE WHEN na / 0 > 0 THEN 1 END",
        "CASE WHEN i4a > 0 THEN 2147483647 + 1 END",
        "CASE WHEN ba THEN i4a ELSE 2147483647 * 2 END",
        "CASE WHEN 1 = 1 THEN 1.5 / 0 END",
        "CASE WHEN 1 = 2 THEN 1 WHEN ba THEN 1.5 / 0 END",
        "CASE WHEN ba THEN f4a ELSE 100000000000000000000000000000000000000000 END",
        "ba AND 1.5 / 0 = 1",
        "1 = 1 AND 1.5 / 0 = 1",
        "1.5 / 0 = 1 AND 1 = 2",
        "ba OR 2147483647 + 1 > 0",
        "1 = 2 OR 1.5 / 0 = 1",
        "NOT (ba AND 1.5 / 0 = 1)",
        "NULLIF(i4a, 2147483647 + 1)",
        "NULLIF(1.5 / 0, na)",
        "i4a IS NULL OR 1.5 / 0 = 1",
    ];
    for text in refused {
        match check_text(text, None) {
            Ok(Agreement::RefusedAndRaises(_)) => {}
            Ok(other) => panic!("`{text}` should be refused and raise, got {other:?}"),
            Err(d) => panic!("{d}"),
        }
    }
    let dropped = [
        "CASE WHEN 1 = 1 THEN 1 ELSE 1.5 / 0 END",
        "CASE WHEN 1 = 2 THEN 1.5 / 0 ELSE 2 END",
        "CASE WHEN NULLIF(1, 1) = 1 THEN 1.5 / 0 ELSE 2 END",
        "CASE WHEN 1 = 1 THEN 1 WHEN ba THEN 1.5 / 0 END",
        "CASE WHEN 1 < 2 THEN 1 WHEN 1.5 / 0 > 0 THEN 3 END",
        "CASE WHEN ba THEN 1 WHEN 1 = 1 THEN 2 ELSE 1.5 / 0 END",
        "CASE WHEN ba THEN f4a WHEN 1 = 2 THEN 100000000000000000000000000000000000000000 END",
        "1 = 2 AND 1.5 / 0 = 1",
        "ba AND 1 = 2 AND 1.5 / 0 = 1",
        "1 = 1 OR 1.5 / 0 = 1",
        "ba OR 1 = 1 OR 1.5 / 0 = 1",
        "NOT (1 = 2 AND 1.5 / 0 = 1)",
        "(1 = 2 AND 1.5 / 0 = 1) OR ba",
        "COALESCE(i4a, CASE WHEN 1 = 1 THEN 1 ELSE 1.5 / 0 END)",
        // A runtime divisor is left to the rows.
        "CASE WHEN i4a > 0 THEN na / i4a END",
        "i4a <> 0 AND na / i4a > 1",
        "CASE WHEN ba THEN i4a * i4b END",
    ];
    for text in dropped {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            Ok(other) => panic!("`{text}` should be admitted, got {other:?}"),
            Err(d) => panic!("{d}"),
        }
    }
}

/// Constants Postgres folds into an error at plan time are refused at
/// define: range overflow under each operator, the smallest integer under
/// prefix minus and `ABS`, float overflow and underflow, and a constant zero
/// divisor.
#[test]
fn arithmetic_constants_postgres_rejects_are_refused() {
    for text in [
        "2147483647 - -1",
        "-2147483648 - 1",
        "2147483647 * 2",
        "46341 * 46341",
        "-(-2147483647 - 1)",
        "ABS(-2147483648)",
        "-(ABS(-2147483647) + 1)  * 2147483647",
        "-9223372036854775808 - 1",
        "9223372036854775807 * 2",
        "-(-9223372036854775807 - 1)",
        "ABS(-9223372036854775807 - 1)",
        "CAST(2147483647 AS bigint) * 4294967296 * 4294967296",
        "REAL '3.4028235e+38' * REAL '2'",
        "REAL '3.4028235e+38' - REAL '-3.4028235e+38'",
        "REAL '1e-30' * REAL '1e-30'",
        "DOUBLE PRECISION '1e+308' * DOUBLE PRECISION '10'",
        "DOUBLE PRECISION '1e-200' * DOUBLE PRECISION '1e-200'",
        "DOUBLE PRECISION '1e-300' / DOUBLE PRECISION '1e+300'",
        "DOUBLE PRECISION '1e+308' / DOUBLE PRECISION '0.1'",
        "DOUBLE PRECISION '1e+308' * 10",
        "f8a + DOUBLE PRECISION '1e+308' * 10",
        "na / 0",
        "na / 0.0",
        "i4a % 0",
        "f8a / 0",
        "f4a / REAL '0'",
        "f4a / REAL '-0'",
        "na / (1 - 1)",
        "na % (2 - 2)",
        "1.0 / 0 + na",
        "1.5 / 0",
        "7 % 0",
        "DOUBLE PRECISION 'Infinity' / 0",
    ] {
        match check_text(text, None) {
            Ok(Agreement::RefusedAndRaises(_)) => {}
            Ok(other) => panic!("`{text}` should be refused and raise, got {other:?}"),
            Err(d) => panic!("{d}"),
        }
    }
}

/// Every leaf a rule can take: the table's column of that type and every
/// literal the conformance profile draws at it.
fn leaves(ty: ValueType) -> Vec<Expr> {
    let mut out: Vec<Expr> = columns()
        .into_iter()
        .filter(|(_, t)| *t == ty)
        .take(1)
        .map(|(name, _)| Expr::Column(name))
        .collect();
    out.extend(expr_gen::literals(ty, Profile::CONFORMANCE.magnitude));
    out
}

/// Every argument list whose `i`th argument is one of `options[i]`.
fn product(options: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    options.iter().fold(vec![Vec::new()], |acc, choices| {
        acc.iter()
            .flat_map(|prefix| {
                choices.iter().map(move |c| {
                    let mut next = prefix.clone();
                    next.push(c.clone());
                    next
                })
            })
            .collect()
    })
}

/// Every instance of `rule` one level deep: each operand or argument is a
/// column or a literal of its type, in every combination.
fn instances(rule: &expr_gen::Rule) -> Vec<Expr> {
    use expr_gen::Rule;
    match rule {
        Rule::Op { op, lhs, rhs, .. } => product(&[
            leaves(*lhs),
            match expr_gen::LITERAL_RHS_OPERATORS
                .iter()
                .find(|(o, _)| o == op)
            {
                Some((_, zone)) => vec![Expr::StringLiteral((*zone).to_string())],
                None => leaves(*rhs),
            },
        ])
        .into_iter()
        .map(|mut v| {
            let r = v.pop().expect("rhs");
            let l = v.pop().expect("lhs");
            bin(*op, l, r)
        })
        .collect(),
        Rule::FloatVsLiteral { op, float } => {
            let literals = expr_gen::comparison_literals(Profile::CONFORMANCE.magnitude);
            let mut out = Vec::new();
            for f in leaves(ValueType::Float(*float)) {
                for lit in &literals {
                    out.push(bin(*op, f.clone(), lit.clone()));
                    out.push(bin(*op, lit.clone(), f.clone()));
                }
            }
            out
        }
        Rule::Negate { ty } => leaves(*ty)
            .into_iter()
            .filter(|e| !matches!(e, Expr::NumberLiteral(_)))
            .map(|e| Expr::Negate(Box::new(e)))
            .collect(),
        Rule::Cast { from, to } => leaves(*from)
            .into_iter()
            .map(|e| Expr::Cast {
                expr: Box::new(e),
                target: *to,
            })
            .collect(),
        Rule::Call { name, args, .. } => {
            let options: Vec<Vec<Expr>> = args
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    if registry::INTEGER_LITERAL_ARGS.contains(&(*name, i)) {
                        expr_gen::SCALE_LITERALS
                            .iter()
                            .map(|n| Expr::NumberLiteral((*n).to_string()))
                            .collect()
                    } else if *name == "DATE_TRUNC" && i == 0 {
                        registry::DATE_TRUNC_UNITS
                            .iter()
                            .map(|u| Expr::StringLiteral((*u).to_string()))
                            .collect()
                    } else if expr_gen::LITERAL_ONLY_ARGS.contains(&(*name, i)) {
                        expr_gen::REGEX_PATTERNS
                            .iter()
                            .chain(expr_gen::REGEX_DIALECT_PATTERNS)
                            .map(|p| Expr::StringLiteral((*p).to_string()))
                            .collect()
                    } else {
                        leaves(*ty)
                    }
                })
                .collect();
            product(&options)
                .into_iter()
                .map(|args| Expr::FunctionCall {
                    name: (*name).to_string(),
                    args,
                })
                .collect()
        }
        Rule::Extract { operand } => {
            let fields = registry::extract_fields(*operand).expect("a rule's type has fields");
            leaves(*operand)
                .into_iter()
                .flat_map(|e| {
                    fields.iter().map(move |field| Expr::FunctionCall {
                        name: "EXTRACT".to_string(),
                        args: vec![Expr::StringLiteral((*field).to_string()), e.clone()],
                    })
                })
                .collect()
        }
        Rule::Coalesce { ty } => product(&[leaves(*ty), leaves(*ty)])
            .into_iter()
            .map(|args| Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args,
            })
            .collect(),
        Rule::Not => leaves(ValueType::Boolean)
            .into_iter()
            .map(|e| Expr::Not(Box::new(e)))
            .collect(),
        Rule::IsNull { ty } => leaves(*ty)
            .into_iter()
            .flat_map(|e| {
                [false, true].map(|negated| Expr::IsNull {
                    expr: Box::new(e.clone()),
                    negated,
                })
            })
            .collect(),
        Rule::Case { ty } => {
            let conditions = leaves(ValueType::Boolean);
            let results = leaves(*ty);
            let mut out = Vec::new();
            for condition in &conditions {
                for result in &results {
                    out.push(Expr::Case {
                        branches: vec![(condition.clone(), result.clone())],
                        otherwise: None,
                    });
                    for otherwise in &results {
                        out.push(Expr::Case {
                            branches: vec![(condition.clone(), result.clone())],
                            otherwise: Some(Box::new(otherwise.clone())),
                        });
                    }
                }
            }
            out
        }
        Rule::CaseMixed { then, other, .. } => {
            let conditions = leaves(ValueType::Boolean);
            let mut out = Vec::new();
            for condition in &conditions {
                for t in leaves(*then) {
                    for o in leaves(*other) {
                        out.push(Expr::Case {
                            branches: vec![(condition.clone(), t.clone())],
                            otherwise: Some(Box::new(o)),
                        });
                    }
                }
            }
            out
        }
        Rule::NullIf { lhs, rhs } => product(&[leaves(*lhs), leaves(*rhs)])
            .into_iter()
            .map(|args| Expr::FunctionCall {
                name: "NULLIF".to_string(),
                args,
            })
            .collect(),
        Rule::NullIfLiteral { float } => {
            let literals = expr_gen::comparison_literals(Profile::CONFORMANCE.magnitude);
            leaves(ValueType::Float(*float))
                .into_iter()
                .flat_map(|f| {
                    literals.iter().map(move |lit| Expr::FunctionCall {
                        name: "NULLIF".to_string(),
                        args: vec![f.clone(), lit.clone()],
                    })
                })
                .collect()
        }
    }
}

/// Checks every instance of every rule of `kind` (one level deep, over
/// every leaf), deterministically: what the random properties reach only
/// by luck (a renderer's cast for one literal, one refusal's boundary) is
/// reached on every run.
fn every_instance_conforms(kind: fn(&expr_gen::Rule) -> bool) {
    let mut failures = Vec::new();
    let mut checked = 0;
    for rule in expr_gen::rules().iter().filter(|r| kind(r)) {
        for expr in instances(rule) {
            let text = render_text(&expr, TextStyle::default());
            checked += 1;
            if let Err(d) = check_text(&text, Some(&expr)) {
                failures.push(d.to_string());
            }
        }
    }
    assert!(checked > 0, "no rule of this kind");
    assert!(
        failures.is_empty(),
        "{} of {checked} one-level expressions diverge:\n{}",
        failures.len(),
        failures[..failures.len().min(10)].join("\n")
    );
}

#[test]
fn every_operator_over_every_leaf_conforms() {
    every_instance_conforms(|r| {
        matches!(
            r,
            expr_gen::Rule::Op { .. } | expr_gen::Rule::FloatVsLiteral { .. }
        )
    });
}

#[test]
fn every_function_over_every_leaf_conforms() {
    every_instance_conforms(|r| {
        matches!(
            r,
            expr_gen::Rule::Call { .. } | expr_gen::Rule::Extract { .. }
        )
    });
}

#[test]
fn every_coalesce_over_every_leaf_conforms() {
    every_instance_conforms(|r| matches!(r, expr_gen::Rule::Coalesce { .. }));
}

/// `CAST(x AS int)`, `AS int4`, `AS int8` and `AS decimal` (#1455) are typed
/// like their canonical targets by define and by Postgres, and parse back to
/// the canonical tree (the checker's round trip). Over every leaf, in each
/// alias spelling and both cases.
#[test]
fn every_cast_alias_over_every_leaf_conforms() {
    let mut checked = 0;
    let mut failures = Vec::new();
    for rule in expr_gen::rules()
        .iter()
        .filter(|r| matches!(r, expr_gen::Rule::Cast { .. }))
    {
        for expr in instances(rule) {
            for cast_alias in 1..=2 {
                for lower_names in [true, false] {
                    let style = TextStyle {
                        cast_alias,
                        lower_names,
                        ..TextStyle::default()
                    };
                    let text = render_text(&expr, style);
                    checked += 1;
                    if let Err(d) = check_text(&text, Some(&expr)) {
                        failures.push(d.to_string());
                    }
                }
            }
        }
    }
    assert!(checked > 0);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn every_logic_form_over_every_leaf_conforms() {
    every_instance_conforms(|r| {
        matches!(
            r,
            expr_gen::Rule::Not
                | expr_gen::Rule::IsNull { .. }
                | expr_gen::Rule::Case { .. }
                | expr_gen::Rule::CaseMixed { .. }
                | expr_gen::Rule::NullIf { .. }
                | expr_gen::Rule::NullIfLiteral { .. }
        )
    });
}

/// An expression Postgres folds and rejects for every row is refused at
/// define, and Postgres raises on the where-false probe: agreement.
#[test]
fn constants_postgres_rejects_at_plan_time_are_refused_at_define() {
    for text in [
        "2147483647 + 1",
        "2147483647 + 2147483647",
        "1 + 2147483647",
        "REAL '3.4028235e+38' + REAL '3.4028235e+38'",
        "DOUBLE PRECISION '1.7976931348623157e+308' + DOUBLE PRECISION '1.7976931348623157e+308'",
        "COALESCE(f4a, 100000000000000000000000000000000000000000)",
    ] {
        match check_text(text, None) {
            Ok(Agreement::RefusedAndRaises(_)) => {}
            Ok(other) => panic!("`{text}` should be refused and raise, got {other:?}"),
            Err(d) => panic!("{d}"),
        }
    }
}

/// The open gap the properties tolerate: Postgres evaluates a function call
/// over constants while planning, then the sum around it, so an overflow
/// there raises for every row, while define (which has no value for the
/// call) admits it. This pin fails the day define refuses it, which is when
/// `KNOWN_GAP_CONSTANT_CALL` and the tolerance in the checker go.
#[test]
fn a_range_overflow_around_a_constant_call_is_a_known_gap() {
    for text in [
        "2147483647 + CHAR_LENGTH('abc')",
        "9223372036854775807 + REGEXP_COUNT('a', 'a*')",
        "CHAR_LENGTH('abc') + 2147483647",
    ] {
        match check_text(text, None) {
            Ok(Agreement::KnownGap(_)) => {}
            other => panic!("`{text}`: expected the known gap, got {other:?}"),
        }
    }
}

/// A pattern Postgres rejects (`2201B`) is refused at define, by the probe
/// that asks Postgres; a pattern only Postgres accepts is admitted.
#[test]
fn a_pattern_postgres_rejects_is_refused() {
    for pattern in expr_gen::REGEX_DIALECT_PATTERNS {
        let text = format!("REGEXP_COUNT(ta, '{pattern}')");
        match check_text(&text, None) {
            Ok(Agreement::RefusedAndRaises(message)) => {
                assert!(message.contains(pattern), "{message}");
            }
            other => panic!("`{text}`: expected the refusal, got {other:?}"),
        }
    }
    for pattern in [r"(a)\1", "a(?=b)", r"\ma"] {
        let text = format!("REGEXP_COUNT(ta, '{pattern}')");
        match check_text(&text, None) {
            Ok(Agreement::Admitted) => {}
            other => panic!("`{text}`: expected it admitted, got {other:?}"),
        }
    }
}

/// The date forms with the values Postgres gives them: leap days, year
/// boundaries, ISO week 53, the Monday a week truncates to, `infinity`. The
/// column forms run over the table's dates (`infinity`, the earliest and the
/// latest date among them); the constant forms are pinned to their answers.
#[test]
fn date_rows_agree_with_postgres() {
    let pins: &[(&str, &str)] = &[
        ("DATE '2024-02-28' + 1", "2024-02-29"),
        ("1 + DATE '2023-02-28'", "2023-03-01"),
        ("DATE '2024-12-31' + 1", "2025-01-01"),
        ("DATE '2024-03-01' - 1", "2024-02-29"),
        ("DATE '2024-03-01' - DATE '2024-02-01'", "29"),
        ("DATE '2025-01-01' - DATE '2024-01-01'", "366"),
        ("DATE '2024-02-29' + 0", "2024-02-29"),
        ("EXTRACT(week FROM DATE '2020-12-31')", "53"),
        ("EXTRACT(isoyear FROM DATE '2021-01-01')", "2020"),
        ("EXTRACT(year FROM DATE '2021-01-01')", "2021"),
        ("EXTRACT(dow FROM DATE '2024-02-25')", "0"),
        ("EXTRACT(isodow FROM DATE '2024-02-25')", "7"),
        ("EXTRACT(doy FROM DATE '2024-12-31')", "366"),
        ("EXTRACT(quarter FROM DATE '2024-11-30')", "4"),
        ("EXTRACT(decade FROM DATE '2020-01-01')", "202"),
        ("EXTRACT(century FROM DATE '2000-12-31')", "20"),
        ("EXTRACT(century FROM DATE '2001-01-01')", "21"),
        ("EXTRACT(millennium FROM DATE '2000-12-31')", "2"),
        ("EXTRACT(epoch FROM DATE '1970-01-02')", "86400"),
        (
            "EXTRACT(epoch FROM TIMESTAMP '1970-01-01 00:00:01.5')",
            "1.500000",
        ),
        (
            "EXTRACT(second FROM TIMESTAMP '2024-02-29 12:00:30.25')",
            "30.250000",
        ),
        (
            "EXTRACT(milliseconds FROM TIMESTAMP '2024-02-29 12:00:30.25')",
            "30250.000",
        ),
        (
            "EXTRACT(microseconds FROM TIMESTAMP '2024-02-29 12:00:30.25')",
            "30250000",
        ),
        (
            "DATE_TRUNC('week', TIMESTAMP '2024-02-29 12:34:56.789')",
            "2024-02-26 00:00:00",
        ),
        (
            "DATE_TRUNC('week', TIMESTAMP '2021-01-03 12:00:00')",
            "2020-12-28 00:00:00",
        ),
        (
            "DATE_TRUNC('quarter', TIMESTAMP '2024-11-30 12:00:00')",
            "2024-10-01 00:00:00",
        ),
        (
            "DATE_TRUNC('decade', TIMESTAMP '2024-11-30 12:00:00')",
            "2020-01-01 00:00:00",
        ),
        (
            "DATE_TRUNC('century', TIMESTAMP '2024-11-30 12:00:00')",
            "2001-01-01 00:00:00",
        ),
        (
            "DATE_TRUNC('millennium', TIMESTAMP '2024-11-30 12:00:00')",
            "2001-01-01 00:00:00",
        ),
        (
            "DATE_TRUNC('milliseconds', TIMESTAMP '2024-11-30 12:00:00.123456')",
            "2024-11-30 12:00:00.123",
        ),
        (
            "DATE_TRUNC('year', TIMESTAMP '1970-01-01 00:00:00.5')",
            "1970-01-01 00:00:00",
        ),
        (
            "CAST(DATE '2024-02-29' AS timestamp)",
            "2024-02-29 00:00:00",
        ),
        (
            "CAST(DATE '0001-01-01' AS timestamp)",
            "0001-01-01 00:00:00",
        ),
        (
            "DATE_TRUNC('month', CAST(DATE '2024-02-29' + 1 AS timestamp))",
            "2024-03-01 00:00:00",
        ),
    ];
    // The column forms, over `infinity` and the extremes among the rows.
    let columns = [
        "da + 1",
        "1 + da",
        "da - 1",
        "da + i2a",
        "i2b + da",
        "da - i4a",
        "da - db",
        "(da - db) + 1",
        "da + (da - db)",
        "da + 2147483647",
        "da - 2147483647",
        "CAST(da AS timestamp)",
        "CAST(da + 1 AS timestamp) = tsa",
        "DATE_TRUNC('month', CAST(da AS timestamp))",
        "DATE_TRUNC('week', tsa)",
        "DATE_TRUNC('millennium', tsa)",
        "DATE_TRUNC('microseconds', tsa)",
        "DATE_TRUNC('day', tza AT TIME ZONE 'UTC')",
        "tza AT TIME ZONE 'UTC'",
        "tza AT TIME ZONE 'UTC' > tsa",
        "EXTRACT(year FROM da)",
        "EXTRACT(week FROM da) + EXTRACT(dow FROM db)",
        "EXTRACT(epoch FROM da)",
        "EXTRACT(epoch FROM tza AT TIME ZONE 'UTC') - EXTRACT(epoch FROM tsa)",
        "EXTRACT(hour FROM tsa)",
        "EXTRACT(microseconds FROM tsa)",
        "EXTRACT(isoyear FROM DATE_TRUNC('week', tsa))",
        "CASE WHEN da > db THEN da ELSE db + 7 END",
        "COALESCE(da, DATE '2024-02-29') - DATE '1970-01-01'",
        "NULLIF(da, db)",
        "da IS NULL",
        "da + 1 = db",
        "DATE_TRUNC('day', tsa) = CAST(da AS timestamp)",
    ];
    for (text, answer) in pins {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            other => panic!("`{text}`: expected it admitted, got {other:?}"),
        }
        let found = HARNESS.with(|h| h.runtime.block_on(h.fixture.scalar(text)));
        assert_eq!(found, Ok(Some((*answer).to_string())), "`{text}`");
    }
    for text in columns {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            other => panic!("`{text}`: expected it admitted, got {other:?}"),
        }
    }
}

/// Infinite and out-of-range dates are data: a row that holds one is
/// quarantined (class 22) or follows Postgres (`infinity + 1` is `infinity`;
/// `EXTRACT(year FROM 'infinity')` is `Infinity`; `EXTRACT(day FROM
/// 'infinity')` is `NULL`), and define neither refuses nor evaluates it.
#[test]
fn infinite_and_out_of_range_dates_follow_postgres() {
    HARNESS.with(|h| {
        let value = |sql: &str| h.runtime.block_on(h.fixture.scalar(sql));
        assert_eq!(value("'infinity'::date + 1"), Ok(Some("infinity".into())));
        assert_eq!(value("'-infinity'::date - 1"), Ok(Some("-infinity".into())));
        assert_eq!(value("1 + '-infinity'::date"), Ok(Some("-infinity".into())));
        assert_eq!(
            value("EXTRACT(year FROM 'infinity'::date)"),
            Ok(Some("Infinity".into()))
        );
        assert_eq!(
            value("EXTRACT(epoch FROM '-infinity'::date)"),
            Ok(Some("-Infinity".into()))
        );
        assert_eq!(value("EXTRACT(day FROM 'infinity'::date)"), Ok(None));
        assert_eq!(value("EXTRACT(dow FROM 'infinity'::timestamp)"), Ok(None));
        assert_eq!(value("EXTRACT(hour FROM 'infinity'::timestamp)"), Ok(None));
        assert_eq!(
            value("DATE_TRUNC('week', 'infinity'::timestamp)"),
            Ok(Some("infinity".into()))
        );
        assert_eq!(
            value("'infinity'::timestamptz AT TIME ZONE 'UTC'"),
            Ok(Some("infinity".into()))
        );
        assert_eq!(
            value("CAST('-infinity'::date AS timestamp)"),
            Ok(Some("-infinity".into()))
        );
        // The errors a row can hold: class 22.
        for sql in [
            "'infinity'::date - '2024-01-01'::date",
            "'5874897-12-31'::date + 1",
            "'4714-11-24 BC'::date - 1",
            "CAST('5874897-12-31'::date AS timestamp)",
            "DATE_TRUNC('millennium', '4713-01-01 BC'::timestamp)",
            "'2024-01-01'::date + 2147483647",
        ] {
            match value(sql) {
                Err(code) => assert_eq!(&code[..2], "22", "{sql}: {code}"),
                other => panic!("`{sql}` should raise class 22, got {other:?}"),
            }
        }
    });
    // Over a column, define admits what Postgres raises for some rows only.
    for text in [
        "da + 2147483647",
        "da - db",
        "CAST(da + 1 AS timestamp)",
        "DATE_TRUNC('millennium', tsa)",
    ] {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            other => panic!("`{text}`: expected it admitted, got {other:?}"),
        }
    }
}

/// A constant date operation Postgres folds and rejects is refused at define,
/// exactly at the boundary Postgres rejects it, and a cast of a constant past
/// `timestamp`'s range likewise. The boundaries are the engine's own
/// (`date_const`), so these are what pin them against Postgres.
#[test]
fn date_constants_postgres_rejects_are_refused_at_the_same_boundary() {
    let refused = [
        "DATE '9999-12-31' + 2142110010",
        "2142110010 + DATE '9999-12-31'",
        "DATE '2024-01-01' + 2147483647",
        "DATE '2024-01-01' - 2147483647",
        "DATE '0001-01-01' - 1721427",
        "DATE '0001-01-01' + -1721427",
        "CAST(DATE '9999-12-31' + 106751982 - 2921939 + 1 AS timestamp)",
        "CAST(DATE '9999-12-31' + 2142110009 AS timestamp)",
        "(DATE '9999-12-31' + 2142110009) + 1",
        "EXTRACT(year FROM DATE '9999-12-31' + 2142110010)",
        "DATE_TRUNC('day', CAST(DATE '9999-12-31' + 2142110009 AS timestamp))",
    ];
    let admitted = [
        "DATE '9999-12-31' + 2142110009",
        "DATE '0001-01-01' - 1721426",
        "DATE '0001-01-01' + -1721426",
        "CAST(DATE '9999-12-31' + 106751982 - 2921939 AS timestamp)",
        "CAST(DATE '0001-01-01' - 1721426 AS timestamp)",
        "(DATE '9999-12-31' + 2142110009) - 1",
        "EXTRACT(year FROM DATE '9999-12-31' + 2142110009)",
        "DATE '2024-01-01' - DATE '0001-01-01'",
    ];
    for text in refused {
        match check_text(text, None) {
            Ok(Agreement::RefusedAndRaises(_)) => {}
            other => panic!("`{text}` should be refused and raise, got {other:?}"),
        }
    }
    for text in admitted {
        match check_text(text, None) {
            Ok(Agreement::Admitted) => {}
            other => panic!("`{text}`: expected it admitted, got {other:?}"),
        }
    }
}

/// Each refusal, by name: the near miss, and that the error names it.
#[test]
fn each_refusal_names_its_construct() {
    let pins: &[(&str, &str)] = &[
        // A wrong-typed operand.
        ("ta + 1", "text"),
        ("1 + ba", "boolean"),
        // Text orders by the collation, which is not evaluated.
        ("ta > tb", "orders text"),
        ("ta < 'x'", "orders text"),
        ("ta <= tb", "orders text"),
        ("ta >= tb", "orders text"),
        // The comparison refusals: each names its reason and remedy.
        ("f4a = i4a", "round the exact side"),
        ("f8a <> na", "round the exact side"),
        ("f4a IS DISTINCT FROM i2a", "round the exact side"),
        ("da = tsa", "cast one side"),
        ("tsa < tza", "cast one side"),
        ("tma >= tta", "cast one side"),
        ("da IS NOT DISTINCT FROM tza", "cast one side"),
        ("ta = i4a", "cannot compare"),
        ("ba = i4a", "cannot compare"),
        ("ua = ta", "cannot compare"),
        // Logic over a non-boolean.
        ("i4a AND ba", "'and'"),
        ("ba OR ta", "'or'"),
        ("NOT i4a", "'not'"),
        // The truth-value tests and the simple CASE.
        ("ba IS TRUE", "is true"),
        ("ba IS NOT FALSE", "is not false"),
        ("ba IS UNKNOWN", "is unknown"),
        ("CASE i4a WHEN 1 THEN 2 END", "simple case"),
        ("CASE WHEN i4a THEN 1 END", "case condition"),
        ("CASE WHEN ba THEN ta ELSE 1 END", "share a type"),
        // NULLIF where Postgres casts the first argument.
        ("NULLIF(i4a, na)", "casts the first argument"),
        ("NULLIF(0, f4a)", "casts the first argument"),
        ("NULLIF(f4a, i4a)", "round the exact side"),
        ("NULLIF(ta, 1)", "cannot compare"),
        // Comparisons do not chain.
        ("na > nb > na", "do not chain"),
        ("na = nb = na", "do not chain"),
        ("na < nb = na", "do not chain"),
        ("na IS DISTINCT FROM nb IS DISTINCT FROM na", "do not chain"),
        ("na IS DISTINCT FROM nb IS NULL", "do not chain"),
        // A float against an exact numeric, in a column or an expression.
        ("f4a > i4a", ">"),
        ("na > f8a", ">"),
        // A wrong-typed function argument.
        ("OCTET_LENGTH(1)", "octet_length"),
        ("STRPOS(ta, 1)", "strpos"),
        ("CHAR_LENGTH(i4a)", "char_length"),
        // The pattern must be a literal.
        ("REGEXP_COUNT(ta, tb)", "regexp_count"),
        // Operators and functions outside the registry.
        ("ta || tb", "|"),
        ("LENGTH(ta)", "length"),
        // The arithmetic refusals: each says what Postgres would do and
        // names the construct.
        ("i4a / i4b", "/"),
        ("i2a / i8a", "/"),
        ("f4a % i4a", "%"),
        ("na % f8a", "%"),
        ("-ta", "'-'"),
        ("ta * 2", "'*'"),
        ("CEIL(i4a)", "ceil"),
        ("FLOOR(i2a)", "floor"),
        ("ROUND(i8a)", "round"),
        ("TRUNC(i4a)", "trunc"),
        ("ROUND(f8a)", "round"),
        ("ROUND(f8a, 2)", "round"),
        ("CEIL(f4a)", "ceil"),
        ("ROUND(na, i4a)", "round"),
        ("ROUND(na, 1.5)", "round"),
        ("ABS(ta)", "abs"),
        ("CAST(i8a AS integer)", "integer"),
        ("CAST(na AS bigint)", "bigint"),
        ("CAST(f8a AS numeric)", "numeric"),
        ("CAST(i4a AS smallint)", "smallint"),
        ("CAST(i4a AS numeric(10, 2))", "numeric"),
        ("na / 0", "zero"),
        ("i4a % 0", "zero"),
        ("f8a / 0.0", "zero"),
        ("na / (1 - 1)", "zero"),
        // Date arithmetic with an operand Postgres has no operator for.
        ("da + i8a", "smallint or integer"),
        ("i8a + da", "smallint or integer"),
        ("da - na", "smallint or integer"),
        ("da + f8a", "smallint or integer"),
        ("da + db", "two dates"),
        ("i4a - da", "number minus a date"),
        ("da * 2", "'*'"),
        ("tsa + 1", "date takes a number of days"),
        // Intervals are out of scope, with the remedy for a duration.
        ("tsa - tsb", "extract(epoch"),
        ("tza - tzb", "extract(epoch"),
        ("da - tsa", "interval"),
        ("tsa - da", "interval"),
        ("tma - tmb", "interval"),
        // The STABLE overloads Postgres picks silently.
        ("DATE_TRUNC('month', da)", "cast(d as timestamp)"),
        ("DATE_TRUNC('day', tza)", "at time zone 'utc'"),
        ("EXTRACT(year FROM tza)", "at time zone 'utc'"),
        ("EXTRACT(hour FROM tma)", "date or a timestamp"),
        ("CAST(tza AS timestamp)", "at time zone 'utc'"),
        ("CAST(tsa AS timestamp)", "timestamp"),
        ("CAST(da AS timestamptz)", "timestamptz"),
        // A field or unit the type does not have, or is not a literal.
        ("EXTRACT(hour FROM da)", "cast(d as timestamp)"),
        ("EXTRACT(julian FROM da)", "julian"),
        ("EXTRACT(timezone FROM tsa)", "timezone"),
        ("EXTRACT(years FROM da)", "fields admitted"),
        ("DATE_TRUNC('days', tsa)", "units admitted"),
        ("DATE_TRUNC(ta, tsa)", "literal"),
        ("DATE_PART('year', da)", "date_part"),
        // `AT TIME ZONE` is the literal 'UTC' over a timestamptz.
        ("tza AT TIME ZONE 'America/New_York'", "'utc'"),
        ("tza AT TIME ZONE 'utc'", "'utc'"),
        ("tza AT TIME ZONE ta", "literal"),
        ("tsa AT TIME ZONE 'UTC'", "timestamptz"),
        ("da AT TIME ZONE 'UTC'", "cast(d as timestamp)"),
        // Not immutable: these change between calls.
        ("NOW()", "now"),
        ("RANDOM()", "random"),
        // Reads a session setting.
        ("TO_CHAR(da, 'YYYY')", "to_char"),
        ("i4a::bigint", "::"),
    ];
    HARNESS.with(|h| {
        for (text, needle) in pins {
            let miss = expr_gen::NearMiss {
                kind: "pin",
                text: (*text).to_string(),
                needles: vec![(*needle).to_string()],
            };
            if let Err(d) = h.fixture.check_refusal(&miss) {
                panic!("{d}");
            }
        }
    });
}

/// Every near miss the generator can draw is refused (the property draws
/// from this list; this walks all of it once).
#[test]
fn every_near_miss_is_refused() {
    let misses = near_misses(&columns());
    assert!(misses.len() > 50, "only {} near misses", misses.len());
    HARNESS.with(|h| {
        for miss in &misses {
            if let Err(d) = h.fixture.check_refusal(miss) {
                panic!("{d}");
            }
        }
    });
}

/// Operator lexing mirrors Postgres: split comparisons and adjacent runs
/// with no operator are refused (by name) exactly where Postgres rejects
/// them. The same cases are in `every_near_miss_is_refused`; this one fails
/// on its own if the lexer drifts.
#[test]
fn operator_lexing_near_misses_are_refused() {
    let misses: Vec<_> = near_misses(&columns())
        .into_iter()
        .filter(|m| m.kind == "operator-lexing")
        .collect();
    assert!(
        misses.len() >= 8,
        "only {} operator-lexing misses",
        misses.len()
    );
    HARNESS.with(|h| {
        for miss in &misses {
            if let Err(d) = h.fixture.check_refusal(miss) {
                panic!("{d}");
            }
        }
    });
}

/// A word Postgres reserves is refused as a column in an expression, and
/// Postgres itself refuses it as an unquoted column name: the live server is
/// the oracle for every word on the list, and a non-reserved keyword is not
/// refused.
#[test]
fn reserved_word_near_misses_are_refused_and_postgres_agrees() {
    let misses: Vec<_> = near_misses(&columns())
        .into_iter()
        .filter(|m| m.kind == "reserved-word")
        .collect();
    assert!(misses.len() >= 200, "only {} misses", misses.len());
    HARNESS.with(|h| {
        for miss in &misses {
            if let Err(d) = h.fixture.check_refusal(miss) {
                panic!("{d}");
            }
        }
        for word in trellis::dev::defs::registry::RESERVED_WORDS {
            let outcome = h.runtime.block_on(h.fixture.unquoted_column_name(word));
            // `system_user` is reserved from PostgreSQL 16; 15 takes it.
            if *word == "system_user" && outcome.is_ok() {
                continue;
            }
            // A syntax error, or for `like` (which starts a LIKE clause) a
            // missing table: never a created column.
            assert!(
                outcome.is_err(),
                "Postgres should refuse `{word}` as an unquoted column name"
            );
        }
        for word in [
            "extract", "position", "values", "between", "coalesce", "nullif", "char",
        ] {
            h.runtime
                .block_on(h.fixture.unquoted_column_name(word))
                .unwrap_or_else(|e| panic!("Postgres accepts `{word}` as a column name: {e}"));
        }
    });
}

/// The types the conformance table's columns carry are the ones the
/// generator reasons about (a new column type needs a generator entry).
#[test]
fn the_tables_columns_are_types_the_generator_knows() {
    let universe = expr_gen::universe();
    for (name, ty) in columns() {
        assert!(
            universe.contains(&ty),
            "{name}: {ty} is not in the universe"
        );
    }
    let _ = ValueType::Numeric;
}

/// The generator is not vacuous: over a few thousand draws it reaches every
/// registry operator and function, `COALESCE`, every typed-literal type, a
/// float against a bare literal, and expressions nested to the full depth.
#[test]
fn the_generator_reaches_every_registry_entry() {
    use proptest::strategy::{Strategy, ValueTree};
    use std::collections::BTreeSet;

    fn walk(expr: &Expr, seen: &mut BTreeSet<String>, depth: usize, deepest: &mut usize) {
        *deepest = (*deepest).max(depth);
        match expr {
            Expr::BinaryOp { op, lhs, rhs } => {
                seen.insert(format!("op {}", registry::operator_spec(*op).symbol));
                walk(lhs, seen, depth + 1, deepest);
                walk(rhs, seen, depth + 1, deepest);
            }
            Expr::FunctionCall { name, args } => {
                seen.insert(format!("fn {name}"));
                for arg in args {
                    walk(arg, seen, depth + 1, deepest);
                }
            }
            Expr::Negate(operand) => {
                seen.insert("prefix -".to_string());
                walk(operand, seen, depth + 1, deepest);
            }
            Expr::Cast { expr, target } => {
                seen.insert(format!("cast to {target}"));
                walk(expr, seen, depth + 1, deepest);
            }
            Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                seen.insert(
                    match expr {
                        Expr::Not(_) => "NOT",
                        Expr::IsNull { .. } => "IS NULL",
                        _ => "CASE",
                    }
                    .to_string(),
                );
                for child in expr.logic_children() {
                    walk(child, seen, depth + 1, deepest);
                }
            }
            Expr::TypedLiteral { value_type, .. } => {
                seen.insert(format!("literal {value_type}"));
            }
            Expr::NumberLiteral(_) => {
                seen.insert("number literal".to_string());
            }
            Expr::StringLiteral(_) => {
                seen.insert("string literal".to_string());
            }
            Expr::Column(_) | Expr::SourceColumn { .. } => {
                seen.insert("column".to_string());
            }
        }
    }

    let strategy = any_expr(&columns(), Profile::CONFORMANCE);
    let mut test_runner = runner(1, Some(SMOKE_SEED));
    let mut seen = BTreeSet::new();
    let mut deepest = 0;
    for _ in 0..4000 {
        let draw = strategy.new_tree(&mut test_runner).expect("tree").current();
        walk(&draw.expr, &mut seen, 0, &mut deepest);
    }
    let mut expected: Vec<String> = registry::OPERATORS
        .iter()
        .map(|o| format!("op {}", o.symbol))
        .chain(registry::FUNCTIONS.iter().map(|f| format!("fn {}", f.name)))
        .chain([
            "fn COALESCE".to_string(),
            "fn NULLIF".to_string(),
            "fn EXTRACT".to_string(),
            "column".to_string(),
            "prefix -".to_string(),
            "NOT".to_string(),
            "IS NULL".to_string(),
            "CASE".to_string(),
        ])
        .chain(
            registry::CAST_TARGETS
                .iter()
                .map(|t| format!("cast to {t}")),
        )
        .collect();
    expected.extend(
        trellis::dev::defs::typed_literal::TYPED_LITERALS
            .iter()
            .map(|spec| format!("literal {}", spec.value_type)),
    );
    for want in &expected {
        assert!(seen.contains(want), "never drew {want}; saw {seen:?}");
    }
    assert!(deepest >= 3, "deepest draw was {deepest}");
}
