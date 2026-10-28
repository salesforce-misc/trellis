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
use proptest::test_runner::{Config, TestRunner};
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
        }
    };
}

fn check_text(text: &str, intended: Option<&Expr>) -> Result<Agreement, Divergence> {
    HARNESS.with(|h| {
        let result = h.runtime.block_on(h.fixture.check(text, intended));
        match &result {
            Ok(Agreement::Admitted) => h.admitted.set(h.admitted.get() + 1),
            Ok(Agreement::RefusedAndRaises(_)) => h.refused.set(h.refused.get() + 1),
            Err(_) => {}
        }
        result
    })
}

fn report() {
    HARNESS.with(|h| {
        eprintln!(
            "expression conformance: {} admitted and agreeing, {} refused and raising",
            h.admitted.get(),
            h.refused.get()
        );
    });
}

fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn runner(cases: u32) -> TestRunner {
    TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    })
}

/// Type, Value and Round trip over `cases` random expressions.
fn run_expression_property(cases: u32) {
    let strategy = any_expr(&columns(), Profile::CONFORMANCE);
    let result = runner(cases).run(&strategy, |draw: Draw| {
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
fn run_refusal_property(cases: u32) {
    let strategy = near_miss(&columns());
    let result = runner(cases).run(&strategy, |miss| {
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
    run_expression_property(cases(60));
}

#[test]
fn near_misses_are_refused_smoke() {
    run_refusal_property(cases(60));
}

#[test]
#[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
fn property_expressions_conform_to_postgres() {
    run_expression_property(cases(1000));
}

#[test]
#[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
fn property_near_misses_are_refused() {
    run_refusal_property(cases(1000));
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

/// Each refusal, by name: the near miss, and that the error names it.
#[test]
fn each_refusal_names_its_construct() {
    let pins: &[(&str, &str)] = &[
        // A wrong-typed operand.
        ("ta + 1", "text"),
        ("1 + ba", "boolean"),
        ("ta > tb", "text"),
        ("da + 1", "date"),
        // A chained comparison compares a boolean.
        ("na > nb > na", "boolean"),
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
        ("i4a - i4b", "-"),
        ("i4a * i4b", "*"),
        ("i4a / i4b", "/"),
        ("i4a = i4b", "="),
        ("i4a < i4b", "<"),
        ("ta || tb", "|"),
        ("ABS(i4a)", "abs"),
        // Not immutable: these change between calls.
        ("NOW()", "now"),
        ("RANDOM()", "random"),
        // Reads a session setting.
        ("TO_CHAR(da, 'YYYY')", "to_char"),
        ("i4a::bigint", "::"),
        ("CAST(i4a AS bigint)", "cast"),
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
