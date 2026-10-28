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
        Rule::Op { op, lhs, rhs, .. } => product(&[leaves(*lhs), leaves(*rhs)])
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
        Rule::Coalesce { ty } => product(&[leaves(*ty), leaves(*ty)])
            .into_iter()
            .map(|args| Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args,
            })
            .collect(),
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
    every_instance_conforms(|r| matches!(r, expr_gen::Rule::Call { .. }));
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
        ("i4a = i4b", "="),
        ("i4a < i4b", "'<'"),
        ("i4a >= i4b", "'>='"),
        ("i4a <= i4b", "'<='"),
        ("i4a <> i4b", "'<>'"),
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
            "column".to_string(),
            "prefix -".to_string(),
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
