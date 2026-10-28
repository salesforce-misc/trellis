//! Arithmetic in the expression grammar (`-`, `*`, `/`, `%`, prefix `-`, the
//! numeric functions, the widening `CAST`): how it parses, how it types, and
//! what define refuses. The same constructs are checked against a real
//! Postgres by the conformance harness (`generative/tests/expr_conformance.rs`);
//! these pin the rules that need no database.

use std::collections::HashMap;

use super::ast::{Expr, ValueType};
use super::error::ParseError;
use super::parse;
use super::validate::{ValidationError, validate};
use crate::float::FloatWidth;
use crate::integer::IntWidth;

fn expr_of(text: &str) -> Expr {
    parse(&format!("TRANSFORM t FROM s SELECT {text} AS x"))
        .unwrap_or_else(|e| panic!("`{text}` should parse: {e}"))
        .fields
        .remove(0)
        .expr
}

fn parse_err(text: &str) -> ParseError {
    parse(&format!("TRANSFORM t FROM s SELECT {text} AS x"))
        .expect_err(&format!("`{text}` should not parse"))
}

/// The tree as an s-expression, to pin groupings at a glance.
fn sexp(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::BinaryOp { op, lhs, rhs } => format!(
            "({} {} {})",
            super::registry::operator_spec(*op).symbol,
            sexp(lhs),
            sexp(rhs)
        ),
        Expr::Negate(operand) => format!("(neg {})", sexp(operand)),
        Expr::Cast { expr, target } => format!("(cast {} {target})", sexp(expr)),
        Expr::FunctionCall { name, args } => format!(
            "({} {})",
            name.to_lowercase(),
            args.iter().map(sexp).collect::<Vec<_>>().join(" ")
        ),
        other => format!("{other:?}"),
    }
}

fn columns() -> HashMap<String, ValueType> {
    let mut map = HashMap::new();
    for (name, ty) in [
        ("i2", ValueType::Integer(IntWidth::Int2)),
        ("i4", ValueType::Integer(IntWidth::Int4)),
        ("i8", ValueType::Integer(IntWidth::Int8)),
        ("n", ValueType::Numeric),
        ("f4", ValueType::Float(FloatWidth::Float4)),
        ("f8", ValueType::Float(FloatWidth::Float8)),
        ("t", ValueType::Text),
        // Names that are also function names (ADR-0018).
        ("trunc", ValueType::Numeric),
        ("round", ValueType::Numeric),
        ("abs", ValueType::Integer(IntWidth::Int4)),
        ("cast", ValueType::Integer(IntWidth::Int4)),
        // Type-name aliases that are also valid column names (#1455).
        ("int", ValueType::Integer(IntWidth::Int2)),
        ("int4", ValueType::Integer(IntWidth::Int2)),
        ("int8", ValueType::Integer(IntWidth::Int2)),
        ("decimal", ValueType::Integer(IntWidth::Int2)),
    ] {
        map.insert(name.to_string(), ty);
    }
    map
}

/// The type of `text` as a calculated field, or the refusal.
fn type_of(text: &str) -> Result<ValueType, ValidationError> {
    let def = parse(&format!("TRANSFORM t FROM s SELECT {text} AS x"))
        .unwrap_or_else(|e| panic!("`{text}` should parse: {e}"));
    validate(&def, &columns())?;
    Ok(super::validate::infer_field_types(&def, &columns())?["x"])
}

#[track_caller]
fn assert_type(text: &str, expected: ValueType) {
    match type_of(text) {
        Ok(found) => assert_eq!(found, expected, "`{text}`"),
        Err(e) => panic!("`{text}` was refused: {e}"),
    }
}

#[track_caller]
fn assert_refused(text: &str, needle: &str) {
    match type_of(text) {
        Ok(found) => panic!("`{text}` was admitted as {found}"),
        Err(e) => assert!(
            e.to_string()
                .to_lowercase()
                .contains(&needle.to_lowercase()),
            "`{text}` was refused without naming `{needle}`: {e}"
        ),
    }
}

const I2: ValueType = ValueType::Integer(IntWidth::Int2);
const I4: ValueType = ValueType::Integer(IntWidth::Int4);
const I8: ValueType = ValueType::Integer(IntWidth::Int8);
const N: ValueType = ValueType::Numeric;
const F4: ValueType = ValueType::Float(FloatWidth::Float4);
const F8: ValueType = ValueType::Float(FloatWidth::Float8);

#[test]
fn precedence_binds_multiplicative_over_additive_and_groups_left() {
    for (text, grouping) in [
        ("a - b * c", "(- a (* b c))"),
        ("a * b - c", "(- (* a b) c)"),
        ("a - b - c", "(- (- a b) c)"),
        ("a * b % c", "(% (* a b) c)"),
        ("a % b * c", "(* (% a b) c)"),
        ("a / b * c", "(* (/ a b) c)"),
        ("a * b / c % d", "(% (/ (* a b) c) d)"),
        ("a + b * c", "(+ a (* b c))"),
        ("a - b + c", "(+ (- a b) c)"),
        ("a > b - c * d", "(> a (- b (* c d)))"),
        ("a * (b - c)", "(* a (- b c))"),
        ("(a - b) - c", "(- (- a b) c)"),
        ("a - (b - c)", "(- a (- b c))"),
    ] {
        assert_eq!(sexp(&expr_of(text)), grouping, "`{text}`");
    }
}

#[test]
fn prefix_minus_binds_tighter_than_every_binary_operator() {
    for (text, grouping) in [
        ("-a * b", "(* (neg a) b)"),
        ("-a - b", "(- (neg a) b)"),
        ("a - -b", "(- a (neg b))"),
        ("a * -b", "(* a (neg b))"),
        ("-a % -b", "(% (neg a) (neg b))"),
        ("- - a", "(neg (neg a))"),
        ("-(a - b)", "(neg (- a b))"),
        ("-abs(a)", "(neg (abs a))"),
        ("-a > b", "(> (neg a) b)"),
    ] {
        assert_eq!(sexp(&expr_of(text)), grouping, "`{text}`");
    }
}

#[test]
fn a_minus_before_a_number_is_one_negative_literal() {
    for (text, grouping) in [
        ("-5", "-5"),
        ("- 5", "-5"),
        ("-(5)", "-5"),
        ("-1.50", "-1.50"),
        ("- - 5", "5"),
        ("-(-5)", "5"),
        ("- - - 5", "-5"),
        ("-5 * a", "(* -5 a)"),
        ("a - -5", "(- a -5)"),
        ("-0", "-0"),
    ] {
        assert_eq!(sexp(&expr_of(text)), grouping, "`{text}`");
    }
    // Typed by value, as Postgres reads the constant.
    assert_type("-2147483648", I4);
    assert_type("-2147483649", I8);
    assert_type("-9223372036854775808", I8);
    assert_type("-9223372036854775809", N);
    assert_type("-0.5", N);
    assert_type("- -2147483648", I8);
}

#[test]
fn two_adjacent_minuses_are_refused_as_a_sql_comment() {
    // In SQL `a--5` is `a` followed by a comment, not `a - -5`.
    for text in ["a--5", "a--b", "--a", "ROUND(n, --2)", "a - --5"] {
        assert!(
            matches!(parse_err(text), ParseError::DoubleMinus),
            "`{text}`"
        );
    }
    assert_eq!(sexp(&expr_of("a - -5")), "(- a -5)");
    assert_eq!(sexp(&expr_of("-(-a)")), "(neg (neg a))");
}

#[test]
fn a_function_name_is_a_column_where_it_is_not_called() {
    assert_eq!(sexp(&expr_of("trunc")), "trunc");
    assert_eq!(sexp(&expr_of("round - 1")), "(- round 1)");
    assert_eq!(sexp(&expr_of("abs * cast")), "(* abs cast)");
    assert_eq!(sexp(&expr_of("trunc(round)")), "(trunc round)");
    assert_eq!(sexp(&expr_of("round(round, 2)")), "(round round 2)");
    assert_type("trunc", N);
    assert_type("round - 1", N);
    assert_type("abs * cast", I4);
    assert_type("trunc(round)", N);
}

#[test]
fn a_run_of_prefix_minuses_is_bounded_and_costs_no_stack() {
    // 2 MiB: the default for a spawned thread, well under the main thread's.
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            for levels in [5_000usize, 100_000] {
                let spaced = "- ".repeat(levels);
                for operand in ["a", "5", "(a + b)"] {
                    let err = parse_err(&format!("{spaced}{operand}"));
                    assert!(
                        matches!(err, ParseError::PrefixMinusChainTooLong { .. }),
                        "{levels} minuses before {operand}: {err:?}"
                    );
                }
            }
            // A chain within the bound parses, and is dropped, validated and
            // rendered without recursing past the bound.
            // (99 minuses: the leaf is a level too, and the whole tree may be
            // at most `MAX_EXPRESSION_DEPTH` high.)
            let text = format!("{}i4", "- ".repeat(99));
            let expr = expr_of(&text);
            let mut depth = 0;
            let mut node = &expr;
            while let Expr::Negate(inner) = node {
                depth += 1;
                node = inner;
            }
            assert_eq!(depth, 99);
            let err = parse_err(&format!("{}i4", "- ".repeat(100)));
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            assert_type(&text, I4);
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}

#[test]
fn casts_parse_to_a_cast_node_with_the_named_target() {
    assert_eq!(sexp(&expr_of("CAST(i2 AS bigint)")), "(cast i2 bigint)");
    assert_eq!(sexp(&expr_of("cast(i2 as INTEGER)")), "(cast i2 integer)");
    assert_eq!(
        sexp(&expr_of("CAST(i4 + 1 AS numeric)")),
        "(cast (+ i4 1) numeric)"
    );
    assert_eq!(
        sexp(&expr_of("CAST(CAST(i2 AS integer) AS bigint)")),
        "(cast (cast i2 integer) bigint)"
    );
    // A quoted operand is still a typed literal.
    assert!(matches!(
        expr_of("CAST('1.5' AS real)"),
        Expr::TypedLiteral { .. }
    ));
}

#[test]
fn cast_aliases_parse_to_the_canonical_target_in_any_case() {
    for (alias, canonical) in [
        ("int", "integer"),
        ("int4", "integer"),
        ("int8", "bigint"),
        ("decimal", "numeric"),
    ] {
        for spelling in [
            alias.to_string(),
            alias.to_uppercase(),
            alias[..1].to_uppercase() + &alias[1..],
        ] {
            assert_eq!(
                sexp(&expr_of(&format!("CAST(i2 AS {spelling})"))),
                format!("(cast i2 {canonical})"),
                "{spelling}"
            );
        }
    }
}

#[test]
fn a_cast_alias_types_as_its_canonical_target_and_narrows_alike() {
    assert_type("CAST(i2 AS int)", I4);
    assert_type("CAST(i2 AS INT4)", I4);
    assert_type("CAST(i4 AS int8)", I8);
    assert_type("CAST(i4 AS Decimal)", N);
    assert_refused("CAST(i8 AS int)", "integer");
    assert_refused("CAST(i8 AS int4)", "integer");
    assert_refused("CAST(i4 AS int)", "integer");
    assert_refused("CAST(n AS int4)", "numeric value to integer");
    assert_refused("CAST(n AS decimal)", "numeric value to numeric");
    assert_refused("CAST(i8 AS int8)", "bigint");
}

#[test]
fn a_cast_alias_modifier_is_refused_exactly_like_numerics() {
    let canonical = parse_err("CAST(i2 AS numeric(10, 2))");
    let alias = parse_err("CAST(i2 AS decimal(10, 2))");
    let (
        ParseError::UnsupportedCastTarget { name: a },
        ParseError::UnsupportedCastTarget { name: b },
    ) = (&canonical, &alias)
    else {
        panic!("{canonical:?} / {alias:?}");
    };
    assert_eq!(a, "numeric(...)");
    assert_eq!(b, "decimal(...)");
    assert_eq!(
        canonical.to_string().replace(a.as_str(), "X"),
        alias.to_string().replace(b.as_str(), "X")
    );
}

#[test]
fn alias_spellings_stay_columns_outside_the_cast_target() {
    // `columns()` has smallint columns named `int`, `int4`, `int8`, `decimal`.
    for name in ["int", "int4", "int8", "decimal"] {
        assert_type(name, I2);
        assert_type(&format!("{name} + 1"), I4);
        assert_type(
            &format!("CAST({name} AS {name})"),
            super::registry::cast_target(name).unwrap(),
        );
        assert_type(&format!("-{name}"), I2);
    }
}

#[test]
fn a_cast_to_any_other_type_or_with_a_modifier_is_refused_by_name() {
    for target in [
        "smallint",
        "int2",
        "bigserial",
        "float8",
        // Postgres's `dec` is a `numeric` alias #1455 did not admit.
        "dec",
        "text",
        "real",
        "double precision",
        "numeric(10, 2)",
        "decimal(10, 2)",
        "int(4)",
        "date",
    ] {
        let err = parse_err(&format!("CAST(i2 AS {target})"));
        let ParseError::UnsupportedCastTarget { name } = &err else {
            panic!("CAST(i2 AS {target}): {err:?}");
        };
        let message = err.to_string();
        assert!(
            name.starts_with(target.split(['(', ' ']).next().unwrap()),
            "{name}"
        );
        assert!(
            message.contains("bigint") && message.contains("numeric"),
            "{message}"
        );
    }
}

#[test]
fn every_operator_row_has_the_postgres_result_type() {
    // `-` and `*`: as `+`.
    for op in ["-", "*"] {
        assert_type(&format!("i2 {op} i2"), I2);
        assert_type(&format!("i2 {op} i4"), I4);
        assert_type(&format!("i4 {op} i8"), I8);
        assert_type(&format!("i8 {op} n"), N);
        assert_type(&format!("n {op} n"), N);
        assert_type(&format!("f4 {op} f4"), F4);
        assert_type(&format!("f4 {op} f8"), F8);
        assert_type(&format!("f4 {op} i2"), F8);
        assert_type(&format!("n {op} f4"), F8);
    }
    // `/`: at least one numeric or float.
    assert_type("n / n", N);
    assert_type("i2 / n", N);
    assert_type("n / i8", N);
    assert_type("f4 / f4", F4);
    assert_type("f4 / i4", F8);
    assert_type("i4 / f8", F8);
    assert_type("n / f4", F8);
    // `%`: the exact family only.
    assert_type("i2 % i2", I2);
    assert_type("i2 % i8", I8);
    assert_type("i4 % n", N);
    assert_type("n % n", N);
    // Prefix minus, `ABS`: the operand's type.
    for (col, ty) in [
        ("i2", I2),
        ("i4", I4),
        ("i8", I8),
        ("n", N),
        ("f4", F4),
        ("f8", F8),
    ] {
        assert_type(&format!("-{col}"), ty);
        assert_type(&format!("ABS({col})"), ty);
    }
    // `CEIL`, `FLOOR`, `TRUNC`: numeric and double precision.
    for f in ["CEIL", "FLOOR", "TRUNC"] {
        assert_type(&format!("{f}(n)"), N);
        assert_type(&format!("{f}(f8)"), F8);
    }
    // The scale forms give numeric.
    assert_type("ROUND(n)", N);
    assert_type("ROUND(n, 2)", N);
    assert_type("ROUND(n, -2)", N);
    assert_type("TRUNC(n, 0)", N);
    assert_type("TRUNC(n, -1)", N);
    // Widening casts.
    assert_type("CAST(i2 AS integer)", I4);
    assert_type("CAST(i2 AS bigint)", I8);
    assert_type("CAST(i4 AS bigint)", I8);
    assert_type("CAST(i2 AS numeric)", N);
    assert_type("CAST(i4 AS numeric)", N);
    assert_type("CAST(i8 AS numeric)", N);
    assert_type("CAST(5 AS bigint)", I8);
}

#[test]
fn what_postgres_would_do_differently_is_refused_with_the_remedy() {
    assert_refused("i4 / i4", "CAST(<dividend> AS numeric)");
    assert_refused("i2 / i8", "numeric");
    assert_refused("f4 % i2", "no modulo");
    assert_refused("i2 % f8", "no modulo");
    assert_refused("f8 % f8", "no modulo");
    for f in ["CEIL", "FLOOR", "ROUND", "TRUNC"] {
        assert_refused(&format!("{f}(i4)"), &f.to_lowercase());
        assert_refused(&format!("{f}(i2)"), "already whole");
    }
    assert_refused("ROUND(f8)", "half to even");
    assert_refused("ROUND(f4)", "half to even");
    assert_refused("ROUND(f8, 2)", "half to even");
    assert_refused("CEIL(f4)", "real");
    assert_refused("FLOOR(f4)", "real");
    assert_refused("TRUNC(f4)", "real");
    assert_refused("ROUND(n, i4)", "integer literal");
    assert_refused("ROUND(n, 1.5)", "integer literal");
    assert_refused("TRUNC(n, 99999999999)", "integer literal");
    assert_refused("ABS(t)", "abs");
    assert_refused("CEIL(t)", "ceil");
    assert_refused("-t", "'-'");
    assert_refused("t * 2", "'*'");
    // Casts that narrow, round or leave the integers.
    assert_refused("CAST(i8 AS integer)", "integer");
    assert_refused("CAST(i4 AS integer)", "integer");
    assert_refused("CAST(i8 AS bigint)", "bigint");
    assert_refused("CAST(n AS bigint)", "numeric value to bigint");
    assert_refused("CAST(n AS numeric)", "numeric value to numeric");
    assert_refused("CAST(f8 AS numeric)", "double precision value to numeric");
    assert_refused("CAST(t AS bigint)", "text value to bigint");
}

#[test]
fn a_constant_zero_divisor_is_refused_however_it_is_spelled() {
    for text in [
        "n / 0",
        "n / 0.0",
        "n / -0",
        "i4 % 0",
        "n % 0",
        "f8 / 0",
        "f8 / 0.0",
        "f4 / REAL '0'",
        "f8 / DOUBLE PRECISION '-0'",
        "n / (1 - 1)",
        "i8 % (2 - 2)",
        "n / (0 * 5)",
        "n / (4 % 2)",
        "n / -(0)",
        "i4 % CAST(0 AS bigint)",
        "1 % 0",
        "n / COALESCE(0, 1)",
        "-n / (3 - 3.0)",
    ] {
        assert_refused(text, "zero");
    }
    // A nonzero or non-constant divisor is fine.
    for text in [
        "n / 1",
        "n / -1",
        "n / 0.5",
        "i4 % 7",
        "n / n",
        "n / (1 + 1)",
        "i4 % (i4 - i4)",
        // Postgres divides a NaN by zero to NaN.
        "DOUBLE PRECISION 'NaN' / 0",
        "(REAL 'NaN' + REAL '0') / -(REAL '0')",
    ] {
        assert!(type_of(text).is_ok(), "`{text}`: {:?}", type_of(text));
    }
}

#[test]
fn a_constant_that_overflows_is_refused_like_the_sum() {
    for text in [
        "2147483647 + 1",
        "2147483647 * 2",
        "-2147483648 - 1",
        "-(-2147483647 - 1)",
        "ABS(-2147483648)",
        "-2147483647 * 2",
        "CAST(2147483647 AS bigint) * 4294967296 * 4294967296",
        "9223372036854775807 + 1",
        "-9223372036854775808 - 1",
        "-(-9223372036854775807 - 1)",
        "ABS(-9223372036854775807 - 1)",
        "9223372036854775807 * 2",
        "DOUBLE PRECISION '1e+308' * 10",
        "DOUBLE PRECISION '1e+308' * DOUBLE PRECISION '10'",
        "DOUBLE PRECISION '1e-200' * DOUBLE PRECISION '1e-200'",
        "DOUBLE PRECISION '1e-300' / DOUBLE PRECISION '1e+300'",
        "REAL '3e+38' * REAL '3'",
        "-DOUBLE PRECISION '1.7976931348623157e+308' - DOUBLE PRECISION '1e+308'",
        "f8 + (DOUBLE PRECISION '1e+308' * 10)",
    ] {
        assert_refused(text, "out of range");
    }
    // The same operations on data are not refused: they fail per row.
    for text in [
        "i4 + 2147483647",
        "i4 * 2",
        "-i4",
        "ABS(i4)",
        "i8 * i8",
        "i2 * i2",
    ] {
        assert!(type_of(text).is_ok(), "`{text}`");
    }
    // Values that fit.
    for text in [
        "2147483647 - 1",
        "-2147483648 + 1",
        "-(-2147483647)",
        "ABS(-2147483647)",
        "46340 * 46340",
        "DOUBLE PRECISION '1e+308' - DOUBLE PRECISION '1e+308'",
        "DOUBLE PRECISION '0' * DOUBLE PRECISION '1e-300'",
        "DOUBLE PRECISION '1' / DOUBLE PRECISION 'Infinity'",
        "DOUBLE PRECISION 'Infinity' * DOUBLE PRECISION '0'",
        "REAL '1e-30' * REAL '1'",
    ] {
        assert!(type_of(text).is_ok(), "`{text}`: {:?}", type_of(text));
    }
}

#[test]
fn constants_under_a_dropped_coalesce_branch_are_not_refused() {
    // Postgres drops the arguments after a constant one unevaluated.
    for text in [
        "COALESCE(5, 2147483647 * 2)",
        "COALESCE(5, n / 0)",
        "COALESCE(1, -(-2147483647 - 1))",
    ] {
        assert!(type_of(text).is_ok(), "`{text}`: {:?}", type_of(text));
    }
    // ... but one before the constant is planned.
    assert_refused("COALESCE(i4, 2147483647 * 2)", "out of range");
    assert_refused("COALESCE(i4, n / 0)", "zero");
}

#[test]
fn a_negation_or_cast_renders_in_both_sql_renderers() {
    use super::oracle::{render_expr_sql, render_qualified_expr_sql};
    let expr = expr_of("-i4 * CAST(i2 AS bigint) - ABS(-5) % 3");
    assert_eq!(
        render_expr_sql(&expr),
        "(((-(\"i4\")) * cast(\"i2\" as bigint)) - (abs((-5)::integer) % 3::integer))"
    );
    assert_eq!(
        render_qualified_expr_sql(&expr, "s"),
        "(((-(s.\"i4\")) * cast(s.\"i2\" as bigint)) - (abs((-5)::integer) % 3::integer))"
    );
    // The scale of ROUND is an `integer`: Postgres has no round(numeric, numeric).
    assert_eq!(
        render_expr_sql(&expr_of("ROUND(n, -2)")),
        "round(\"n\", (-2)::integer)"
    );
}

#[test]
fn definition_text_round_trips_for_every_new_construct() {
    use super::ast::render_field_expr;
    for text in [
        "a - b * c",
        "-a * b",
        "- - a",
        "-(a - b)",
        "-5 - -5",
        "a / b % c",
        "CAST(a AS bigint) + 1",
        "ROUND(a, -2)",
        "TRUNC(a)",
        "ABS(-a)",
        "-REAL '1.5'",
        "- - - - -a",
    ] {
        let first = expr_of(text);
        let rendered = render_field_expr(&first);
        let again = expr_of(&rendered);
        assert_eq!(first, again, "`{text}` rendered `{rendered}`");
    }
}

/// Operators are lexed as Postgres lexes them (verified on Postgres 17): a
/// run of adjacent operator characters is one token, and a run gives back
/// trailing `+`/`-` unless it holds one of `~ ! @ # ^ & | ` ? %`.
#[test]
fn adjacent_comparison_operators_are_accepted() {
    for (text, want) in [
        ("a<=b", "(<= a b)"),
        ("a>=b", "(>= a b)"),
        ("a<>b", "(<> a b)"),
        ("a!=b", "(<> a b)"),
        ("a<-1", "(< a -1)"),
        ("a>-1", "(> a -1)"),
        ("a>=-1", "(>= a -1)"),
        ("a<=-1", "(<= a -1)"),
        ("a<>-1", "(<> a -1)"),
        ("a=-1", "(= a -1)"),
        ("a*-1", "(* a -1)"),
        ("a/-1", "(/ a -1)"),
        ("a+-1", "(+ a -1)"),
    ] {
        assert_eq!(sexp(&expr_of(text)), want, "`{text}`");
    }
}

#[test]
fn a_comparison_split_by_whitespace_is_refused_by_name() {
    for (text, found, joined) in [
        ("a < = b", "< =", "<="),
        ("a > = b", "> =", ">="),
        ("a ! = b", "! =", "!="),
        ("a < > b", "< >", "<>"),
    ] {
        match parse_err(text) {
            ParseError::SplitOperator {
                found: f,
                joined: j,
            } => {
                assert_eq!((f.as_str(), j.as_str()), (found, joined), "`{text}`");
            }
            other => panic!("`{text}`: {other:?}"),
        }
    }
}

/// Postgres lexes these runs as one operator it has no definition for (or,
/// for `=>`, a syntax error); splitting them would give the text a meaning
/// Postgres refuses, so each is refused whole.
#[test]
fn an_operator_run_without_an_operator_is_refused_whole() {
    for (text, run) in [
        ("a<=>b", "<=>"),
        ("a==b", "=="),
        ("a<<b", "<<"),
        ("a>>b", ">>"),
        ("a=>b", "=>"),
        ("a!=-1", "!=-"),
        ("a%-1", "%-"),
        ("a><b", "><"),
        ("a=<b", "=<"),
        ("a=!b", "=!"),
        ("a<>=b", "<>="),
        ("a%+1", "%+"),
        ("a*/b", "*/"),
    ] {
        match parse_err(text) {
            ParseError::UnsupportedOperator { operator } => {
                assert_eq!(operator, run, "`{text}`");
            }
            other => panic!("`{text}`: {other:?}"),
        }
    }
}
