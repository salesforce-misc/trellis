//! Comparison and logic in the expression grammar (`=`, `<>`, `!=`, `<`, `<=`,
//! `>`, `>=`, `IS [NOT] DISTINCT FROM`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`,
//! searched `CASE`, `NULLIF`): how it parses, how it types, and what define
//! refuses. The same constructs are checked against a real Postgres by the
//! conformance harness (`generative/tests/expr_conformance.rs`); these pin the
//! rules that need no database.

use std::collections::HashMap;

use super::ast::{Expr, ValueType};
use super::error::ParseError;
use super::parse;
use super::pg_type::PgType;
use super::validate::{ValidationError, validate};
use crate::float::FloatWidth;
use crate::integer::IntWidth;

fn parse_text(text: &str) -> Result<super::ast::TransformDef, ParseError> {
    parse(&format!("TRANSFORM t FROM s SELECT {text} AS x"))
}

fn expr_of(text: &str) -> Expr {
    parse_text(text)
        .unwrap_or_else(|e| panic!("`{text}` should parse: {e}"))
        .fields
        .remove(0)
        .expr
}

fn parse_err(text: &str) -> ParseError {
    parse_text(text).expect_err(&format!("`{text}` should not parse"))
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
        Expr::Not(operand) => format!("(not {})", sexp(operand)),
        Expr::IsNull { expr, negated } => {
            format!(
                "({} {})",
                if *negated { "notnull" } else { "isnull" },
                sexp(expr)
            )
        }
        Expr::Case {
            branches,
            otherwise,
        } => {
            let mut text = String::from("(case");
            for (condition, result) in branches {
                text.push_str(&format!(" [{} {}]", sexp(condition), sexp(result)));
            }
            if let Some(otherwise) = otherwise {
                text.push_str(&format!(" else {}", sexp(otherwise)));
            }
            text.push(')');
            text
        }
        Expr::FunctionCall { name, args } => format!(
            "({} {})",
            name.to_lowercase(),
            args.iter().map(sexp).collect::<Vec<_>>().join(" ")
        ),
        other => format!("{other:?}"),
    }
}

const I2: ValueType = ValueType::Integer(IntWidth::Int2);
const I4: ValueType = ValueType::Integer(IntWidth::Int4);
const I8: ValueType = ValueType::Integer(IntWidth::Int8);
const N: ValueType = ValueType::Numeric;
const F4: ValueType = ValueType::Float(FloatWidth::Float4);
const F8: ValueType = ValueType::Float(FloatWidth::Float8);
const B: ValueType = ValueType::Boolean;
const T: ValueType = ValueType::Text;

fn columns() -> HashMap<String, ValueType> {
    let mut map = HashMap::new();
    for (name, ty) in [
        ("i2", I2),
        ("i4", I4),
        ("i8", I8),
        ("n", N),
        ("f4", F4),
        ("f8", F8),
        ("b", B),
        ("b2", B),
        ("t", T),
        ("t2", T),
        ("u", ValueType::Uuid),
        ("u2", ValueType::Uuid),
        ("d", ValueType::Other(PgType::Date)),
        ("d2", ValueType::Other(PgType::Date)),
        ("ts", ValueType::Other(PgType::Timestamp)),
        ("tz", ValueType::Other(PgType::TimestampTz)),
        ("tm", ValueType::Other(PgType::Time)),
        ("tt", ValueType::Other(PgType::TimeTz)),
        ("j", ValueType::Other(PgType::Jsonb)),
        // Names that are also keywords (ADR-0018).
        ("not", B),
        ("case", I4),
        ("end", I4),
        ("and", B),
        ("or", B),
        ("is", B),
        ("when", I4),
        ("then", I4),
        ("else", I4),
        ("nullif", I4),
    ] {
        map.insert(name.to_string(), ty);
    }
    map
}

/// The type of `text` as a calculated field, or the refusal.
fn type_of(text: &str) -> Result<ValueType, ValidationError> {
    let def = parse_text(text).unwrap_or_else(|e| panic!("`{text}` should parse: {e}"));
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

#[track_caller]
fn assert_grouping(text: &str, grouping: &str) {
    assert_eq!(sexp(&expr_of(text)), grouping, "`{text}`");
}

#[test]
fn precedence_is_or_and_not_is_comparison_additive_multiplicative() {
    for (text, grouping) in [
        // OR < AND
        ("a OR b AND c", "(OR a (AND b c))"),
        ("a AND b OR c", "(OR (AND a b) c)"),
        ("a OR b OR c", "(OR (OR a b) c)"),
        ("a AND b AND c", "(AND (AND a b) c)"),
        // AND < NOT
        ("NOT a AND b", "(AND (not a) b)"),
        ("a AND NOT b", "(AND a (not b))"),
        ("NOT a OR NOT b", "(OR (not a) (not b))"),
        // NOT < IS < comparison
        ("NOT a = b", "(not (= a b))"),
        ("NOT a IS NULL", "(not (isnull a))"),
        ("a = b IS NULL", "(isnull (= a b))"),
        ("a = b IS NOT NULL", "(notnull (= a b))"),
        ("a < b IS DISTINCT FROM c", "(IS DISTINCT FROM (< a b) c)"),
        ("a IS DISTINCT FROM b < c", "(IS DISTINCT FROM a (< b c))"),
        (
            "a IS NOT DISTINCT FROM b = c",
            "(IS NOT DISTINCT FROM a (= b c))",
        ),
        // comparison < additive < multiplicative < unary minus
        ("a + b = c * d", "(= (+ a b) (* c d))"),
        ("a = b + c", "(= a (+ b c))"),
        ("-a * b < c", "(< (* (neg a) b) c)"),
        ("a - b <> c - d", "(<> (- a b) (- c d))"),
        // a postfix IS NULL applies to everything tighter, then more can follow
        ("a IS NULL = b", "(= (isnull a) b)"),
        ("a IS NULL IS NULL", "(isnull (isnull a))"),
        (
            "a IS NULL AND b IS NOT NULL",
            "(AND (isnull a) (notnull b))",
        ),
        ("a = b AND c <> d OR e", "(OR (AND (= a b) (<> c d)) e)"),
        // a NOT after an infix operator is that operator's right operand
        ("a = NOT b", "(= a (not b))"),
        ("a = NOT b = c", "(= a (not (= b c)))"),
        ("NOT NOT a", "(not (not a))"),
        ("NOT (a AND b)", "(not (AND a b))"),
        ("a != b", "(<> a b)"),
        ("a <= b", "(<= a b)"),
        ("a >= b", "(>= a b)"),
    ] {
        assert_grouping(text, grouping);
    }
}

#[test]
fn comparisons_do_not_chain() {
    for text in [
        "a < b < c",
        "a = b = c",
        "a < b = c",
        "a <> b <> c",
        "a >= b <= c",
        "a IS DISTINCT FROM b IS DISTINCT FROM c",
        "a IS DISTINCT FROM b IS NULL",
        "a IS NOT DISTINCT FROM b IS NOT DISTINCT FROM c",
        "a = b != c",
    ] {
        let err = parse_err(text);
        assert!(
            matches!(err, ParseError::NonAssociativeComparison { .. }),
            "`{text}`: {err:?}"
        );
        assert!(
            err.to_string().contains("AND"),
            "the message names the remedy: {err}"
        );
    }
    // Parentheses, or a different level between them, make them legal.
    for text in [
        "(a < b) = c",
        "a < b IS DISTINCT FROM c",
        "a = (b = c)",
        "(a < b) AND (b < c)",
        "a IS NULL IS DISTINCT FROM b",
    ] {
        parse_text(text).unwrap_or_else(|e| panic!("`{text}` should parse: {e}"));
    }
}

#[test]
fn an_is_form_other_than_null_and_distinct_from_is_refused_by_name() {
    for (text, form) in [
        ("a IS TRUE", "IS TRUE"),
        ("a IS NOT TRUE", "IS NOT TRUE"),
        ("a IS FALSE", "IS FALSE"),
        ("a IS UNKNOWN", "IS UNKNOWN"),
        ("a IS NOT UNKNOWN", "IS NOT UNKNOWN"),
        ("a IS DISTINCT b", "IS DISTINCT"),
    ] {
        let err = parse_err(text);
        match &err {
            ParseError::UnsupportedIsForm { found } => assert_eq!(found, form, "`{text}`"),
            other => panic!("`{text}`: {other:?}"),
        }
        assert!(err.to_string().contains("COALESCE"), "{err}");
    }
}

#[test]
fn the_simple_case_form_is_refused_with_the_searched_form_as_the_remedy() {
    for text in [
        "CASE a WHEN 1 THEN 2 END",
        "CASE a + 1 WHEN 1 THEN 2 ELSE 3 END",
        "CASE (a) WHEN 1 THEN 2 END",
        "CASE 1 WHEN 1 THEN 2 END",
    ] {
        let err = parse_err(text);
        assert!(
            matches!(err, ParseError::SimpleCaseUnsupported),
            "`{text}`: {err:?}"
        );
        assert!(err.to_string().contains("CASE WHEN"), "{err}");
    }
}

#[test]
fn a_malformed_case_is_a_parse_error() {
    for text in [
        "CASE WHEN a THEN b",
        "CASE WHEN a END",
        "CASE WHEN a THEN b ELSE END",
        "CASE WHEN a THEN b ELSE c",
        "CASE WHEN THEN b END",
        "CASE WHEN a THEN b ELSE c ELSE d END",
    ] {
        parse_text(text).expect_err(&format!("`{text}`"));
    }
}

#[test]
fn case_parses_with_one_or_many_branches_and_an_optional_else() {
    assert_grouping("CASE WHEN a THEN 1 END", "(case [a 1])");
    assert_grouping("CASE WHEN a THEN 1 ELSE 2 END", "(case [a 1] else 2)");
    assert_grouping(
        "case when a then 1 when b then 2 else 3 end",
        "(case [a 1] [b 2] else 3)",
    );
    assert_grouping(
        "CASE WHEN a = 1 AND b THEN x + 1 ELSE y * 2 END + 1",
        "(+ (case [(AND (= a 1) b) (+ x 1)] else (* y 2)) 1)",
    );
    assert_grouping(
        "CASE WHEN CASE WHEN a THEN b END THEN 1 END",
        "(case [(case [a b]) 1])",
    );
}

/// `NOT` and `CASE` are reserved in Postgres, so a `-` after either begins
/// the operand (verified on Postgres 17: `NOT -1 > 0` is `NOT ((-1) > 0)`).
#[test]
fn not_and_case_before_a_minus_are_the_keywords() {
    assert_grouping("NOT -a > 0", "(not (> (neg a) 0))");
    assert_grouping("NOT - a > 0", "(not (> (neg a) 0))");
    assert_grouping("NOT NOT -a > 0", "(not (not (> (neg a) 0)))");
    assert_grouping("NOT (-a > 0)", "(not (> (neg a) 0))");
    assert_type("NOT -i4 > 0", B);
    assert_type("NOT NOT -i4 > 0", B);
    assert_type("NOT (-i4 > 0)", B);
    assert!(
        matches!(
            parse_err("CASE -a WHEN 1 THEN 2 END"),
            ParseError::SimpleCaseUnsupported
        ),
        "CASE -x is a simple CASE"
    );
    assert!(matches!(
        parse_err("case - 1"),
        ParseError::SimpleCaseUnsupported
    ));
}

/// `NOT` or `CASE` with no operand after it reads as a column name; where no
/// column has that name the message says why the word was not the keyword.
#[test]
fn an_unresolved_not_or_case_column_says_why_it_is_a_column() {
    for name in ["NOT", "CASE"] {
        assert_refused(&format!("{name} AND b"), "no operand follows it");
        assert_refused(&format!("{name} + i4 > 0"), "no operand follows it");
    }
}

#[test]
fn nullif_takes_two_arguments() {
    assert_grouping("NULLIF(a, 0)", "(nullif a 0)");
    for text in ["NULLIF(a)", "NULLIF(a, b, c)", "NULLIF()"] {
        let err = parse_err(text);
        assert!(
            matches!(err, ParseError::FunctionArityMismatch { .. }),
            "`{text}`: {err:?}"
        );
    }
}

/// ADR-0018: a keyword added to the grammar must not change what the
/// grammar accepted before. `AND`, `OR` and `IS` are infix, `END` only closes
/// a `CASE`, `NOT` and `CASE` are prefix and count as keywords only where an
/// operand follows. A column of each name still resolves.
#[test]
fn a_column_named_like_a_keyword_is_still_a_column() {
    for name in [
        "not", "case", "end", "and", "or", "is", "when", "then", "else", "nullif",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {name} AS c"))
            .unwrap_or_else(|e| panic!("`SELECT {name} AS c`: {e}"));
        assert!(
            matches!(&def.fields[0].expr, Expr::Column(found) if found == name),
            "{name}: {:?}",
            def.fields[0].expr
        );
    }
    // Where an operand cannot follow, a prefix keyword is a column too.
    assert_grouping("not + 1", "(+ not 1)");
    assert_grouping("case * end", "(* case end)");
    assert_grouping("not = and", "(= not and)");
    assert_grouping("case IS NULL", "(isnull case)");
    assert_grouping("not IS NULL", "(isnull not)");
    assert_grouping("not AND or", "(AND not or)");
    assert_grouping("not OR not", "(OR not not)");
    assert_grouping("end + case", "(+ end case)");
    // And a keyword is a keyword where an operand follows, whatever its case.
    assert_grouping("not not", "(not not)");
    assert_grouping("NOT not AND is", "(AND (not not) is)");
    assert_grouping(
        "case WHEN not THEN end ELSE else END",
        "(case [not end] else else)",
    );
    assert_grouping(
        "CASE WHEN end = 1 THEN case ELSE when END",
        "(case [(= end 1) case] else when)",
    );
    assert_grouping("nullif(nullif, end)", "(nullif nullif end)");
    assert_type("not", B);
    assert_type("case", I4);
    assert_type("NOT not", B);
    assert_type("end + case", I4);
    assert_type("CASE WHEN and THEN end ELSE case END", I4);
    assert_type("not AND or", B);
    assert_type("is IS NULL", B);
    assert_type("when = then", B);
}

#[test]
fn every_comparison_types_by_family() {
    let ops = [
        "=",
        "<>",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM",
    ];
    let ordering = ["<", "<=", ">", ">="];
    // (lhs, rhs) of one family: every operator is admitted.
    for (a, b) in [
        ("i2", "i8"),
        ("i4", "n"),
        ("n", "i2"),
        ("n", "n"),
        ("i4", "5"),
        ("f4", "f8"),
        ("f8", "f8"),
        ("f4", "0.5"),
        ("f8", "1"),
        ("0.5", "f4"),
        ("d", "d2"),
        ("ts", "ts"),
        ("tz", "tz"),
        ("tm", "tm"),
        ("tt", "tt"),
        ("b", "b2"),
        ("u", "u2"),
        ("d", "DATE '2024-01-01'"),
    ] {
        for op in ops {
            assert_type(&format!("{a} {op} {b}"), B);
        }
    }
    // Text: equality only.
    for op in ops {
        let text = format!("t {op} t2");
        if ordering.contains(&op) {
            assert_refused(&text, "collation");
            assert_refused(&format!("t {op} 'x'"), "orders text");
        } else {
            assert_type(&text, B);
            assert_type(&format!("t {op} 'x'"), B);
        }
    }
}

#[test]
fn a_comparison_across_families_is_refused_with_the_reason() {
    for op in [
        "=",
        "<>",
        "<",
        "<=",
        ">",
        ">=",
        "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM",
    ] {
        // A float with an exact numeric (bar a bare literal).
        for (a, b) in [
            ("f8", "i4"),
            ("i2", "f4"),
            ("f8", "n"),
            ("n", "f4"),
            ("f8", "(i4 + 1)"),
        ] {
            assert_refused(&format!("{a} {op} {b}"), "round the exact side to a float");
        }
        assert_refused(&format!("f8 {op} (0.5 + 0)"), "round the exact side");
        // Two different temporal types.
        for (a, b) in [
            ("d", "ts"),
            ("d", "tz"),
            ("ts", "tz"),
            ("tm", "tt"),
            ("d", "tm"),
        ] {
            assert_refused(&format!("{a} {op} {b}"), "cast one side");
        }
        // No family at all.
        for (a, b) in [
            ("t", "i4"),
            ("b", "i4"),
            ("u", "t"),
            ("j", "j"),
            ("b", "t"),
            ("d", "i4"),
        ] {
            assert_refused(&format!("{a} {op} {b}"), "cannot compare");
        }
    }
}

#[test]
fn logic_is_over_boolean_and_yields_boolean() {
    assert_type("b AND b2", B);
    assert_type("b OR b2", B);
    assert_type("NOT b", B);
    assert_type("i4 > 0 AND f8 > 0.5 OR NOT t = 'x'", B);
    assert_type("i4 IS NULL", B);
    assert_type("t IS NOT NULL", B);
    assert_type("j IS NULL", B);
    assert_type("(b AND b2) IS NULL", B);
    for text in ["i4 AND b", "b OR t", "NOT i4", "b AND 1", "NOT t"] {
        assert_refused(text, "boolean");
    }
}

#[test]
fn case_unifies_its_results_the_way_coalesce_does() {
    assert_type("CASE WHEN b THEN 1 ELSE 2 END", I4);
    assert_type("CASE WHEN b THEN 1 ELSE 2.5 END", N);
    assert_type("CASE WHEN b THEN 1 END", I4);
    assert_type("CASE WHEN b THEN i2 ELSE i8 END", I8);
    assert_type("CASE WHEN b THEN i4 WHEN b2 THEN i2 ELSE 5 END", I4);
    assert_type("CASE WHEN b THEN f4 ELSE n END", F4);
    assert_type("CASE WHEN b THEN n ELSE f4 END", F4);
    assert_type("CASE WHEN b THEN f4 ELSE f8 END", F8);
    assert_type("CASE WHEN b THEN t ELSE 'x' END", T);
    assert_type("CASE WHEN b THEN b2 ELSE NOT b2 END", B);
    assert_type("CASE WHEN b THEN d END", ValueType::Other(PgType::Date));
    assert_type("CASE WHEN i4 > 0 THEN n / NULLIF(i2, 0) ELSE 0 END", N);
    assert_type("CASE WHEN CASE WHEN b THEN b2 END THEN 1 END", I4);
    assert_refused("CASE WHEN b THEN 1 ELSE 'x' END", "share a type");
    assert_refused("CASE WHEN b THEN t ELSE i4 END", "share a type");
    assert_refused("CASE WHEN b THEN d ELSE ts END", "share a type");
    assert_refused("CASE WHEN i4 THEN 1 END", "must be a boolean");
    assert_refused("CASE WHEN b THEN 1 WHEN t THEN 2 END", "must be a boolean");
}

#[test]
fn nullif_is_typed_by_its_first_argument() {
    assert_type("NULLIF(i4, 0)", I4);
    assert_type("NULLIF(i2, i8)", I2);
    assert_type("NULLIF(i8, i2)", I8);
    assert_type("NULLIF(n, 0)", N);
    assert_type("NULLIF(n, i4)", N);
    assert_type("NULLIF(f4, f8)", F4);
    assert_type("NULLIF(f8, 0)", F8);
    assert_type("NULLIF(t, '')", T);
    assert_type("NULLIF(d, d2)", ValueType::Other(PgType::Date));
    assert_type("n / NULLIF(i4, 0)", N);
    assert_type("n / NULLIF(n, 0)", N);
    assert_type("i4 % NULLIF(i4, 0)", I4);
    // Postgres types NULLIF(integer, numeric) `numeric`, not the integer: refused.
    assert_refused("NULLIF(i4, n)", "casts the first argument");
    assert_refused("NULLIF(i8, 1.5)", "casts the first argument");
    // And an exact first argument beside a float is a double precision.
    assert_refused("NULLIF(0, f4)", "double precision");
    assert_refused("NULLIF(n, f8)", "round the exact side");
    assert_type("NULLIF(f4, 0)", F4);
    assert_type("NULLIF(f4, 0.5)", F4);
    assert_type("NULLIF(CAST(i4 AS numeric), n)", N);
    // The comparison families of `=` apply.
    assert_refused("NULLIF(f8, i4)", "round the exact side");
    assert_refused("NULLIF(d, ts)", "cast one side");
    assert_refused("NULLIF(t, 1)", "cannot compare");
}

#[test]
fn constant_failures_inside_a_branch_are_refused_wherever_postgres_folds_them() {
    // Postgres folds `1 / 0` in a `THEN` whatever the condition, so every row fails.
    for text in [
        "CASE WHEN i4 > 0 THEN n / 0 END",
        "CASE WHEN i4 > 0 THEN 1 ELSE n / 0 END",
        "CASE WHEN i4 > 0 THEN 1 WHEN b THEN 5 % 0 END",
        "CASE WHEN n / 0 > 0 THEN 1 END",
        "CASE WHEN i4 > 0 THEN 2147483647 + 1 END",
        "CASE WHEN i4 > 0 THEN 2147483647 * 2 ELSE 1 END",
    ] {
        match type_of(text) {
            Err(
                ValidationError::ConstantDivisorZero { .. }
                | ValidationError::ConstantOutOfRange { .. },
            ) => {}
            other => panic!("`{text}`: {other:?}"),
        }
    }
    // After a branch whose condition is constant true, or in one whose
    // condition is constant false, the planner never simplifies the result.
    for (text, ty) in [
        ("CASE WHEN 1 = 1 THEN 1 ELSE n / 0 END", N),
        ("CASE WHEN 1 = 0 THEN n / 0 ELSE 2 END", N),
        // A condition that folds to `NULL` selects nothing, like a false one.
        ("CASE WHEN NULLIF(1, 1) = 1 THEN n / 0 ELSE 2 END", N),
        ("CASE WHEN 1 = 1 THEN 1 WHEN i4 > 0 THEN n / 0 END", N),
        ("CASE WHEN 1 < 2 THEN n / 0 + 1 END", N),
    ] {
        if text.ends_with("n / 0 + 1 END") {
            // A true constant condition still simplifies its own result.
            assert!(
                matches!(
                    type_of(text),
                    Err(ValidationError::ConstantDivisorZero { .. })
                ),
                "{text}"
            );
        } else {
            assert_type(text, ty);
        }
    }
    // The same short-circuit in AND and OR.
    assert_type("1 = 0 AND 1.5 / 0 = 1", B);
    assert_type("1 = 1 OR 1.5 / 0 = 1", B);
    assert_type("b AND 1 = 0 AND 1.5 / 0 = 1", B);
    assert_type("NOT (1 = 0 AND 1.5 / 0 = 1)", B);
    for text in [
        "b AND 1.5 / 0 = 1",
        "1 = 1 AND 1.5 / 0 = 1",
        "1 = 0 OR 1.5 / 0 = 1",
        "1.5 / 0 = 1 AND 1 = 0",
        "i4 = 1 OR 2147483647 + 1 > 0",
    ] {
        assert!(
            matches!(
                type_of(text),
                Err(ValidationError::ConstantDivisorZero { .. }
                    | ValidationError::ConstantOutOfRange { .. })
            ),
            "{text}: {:?}",
            type_of(text)
        );
    }
    // A strict operator with a `NULL` constant operand is `NULL` whatever the
    // other operand reads (a column, or a call define has no value for), so
    // the planner drops the branch it conditions, and a `NULL` dividend
    // divides by zero without an error (#1471).
    for (text, ty) in [
        (
            "CASE WHEN (STRPOS('','') <= NULLIF(-2147483649, -2147483649)) \
             THEN ((9223372036854775807 % -9223372036854775808) * 2) ELSE (REAL '0') END",
            F4,
        ),
        (
            "CASE WHEN i4 > NULLIF(1, 1) THEN 2147483647 + 1 ELSE 0 END",
            I4,
        ),
        (
            "CASE WHEN (i4 + NULLIF(1, 1)) IS NULL THEN 1 ELSE 2147483647 + 1 END",
            I4,
        ),
        ("(n * NULLIF(1, 1)) / 0", N),
    ] {
        assert_type(text, ty);
    }
    // `IS DISTINCT FROM` is not strict: its other side decides.
    assert_refused(
        "CASE WHEN i4 IS DISTINCT FROM NULLIF(1, 1) THEN 2147483647 + 1 ELSE 0 END",
        "out of range",
    );
    // A runtime failure (a divisor read from a column) is not a define-time refusal.
    assert_type("CASE WHEN i4 > 0 THEN n / i4 END", N);
    assert_type("CASE WHEN i4 <> 0 THEN i8 % i4 END", I8);
    assert_type("i4 <> 0 AND n / i4 > 1", B);
}

#[test]
fn a_constant_cast_out_of_float_range_in_a_case_is_refused() {
    let text = "CASE WHEN b THEN f4 ELSE 1e300 * 1e300 END";
    // `e` notation is not in the grammar; use a literal that is.
    let _ = text;
    let huge = format!("1{}", "0".repeat(60));
    assert!(
        matches!(
            type_of(&format!("CASE WHEN b THEN f4 ELSE {huge} END")),
            Err(ValidationError::CaseLiteralOutOfRange { .. })
        ),
        "{:?}",
        type_of(&format!("CASE WHEN b THEN f4 ELSE {huge} END"))
    );
    // In the dropped branch it is never cast.
    assert_type(&format!("CASE WHEN 1 = 1 THEN f4 ELSE {huge} END"), F4);
    assert_type(&format!("CASE WHEN b THEN f8 ELSE {huge} END"), F8);
}

/// Stack discipline: an expression is bounded in height by the parser, and
/// every walk recurses once per level. Each of these runs on the 2 MiB stack a
/// spawned thread gets.
#[test]
fn long_chains_and_deep_nesting_cost_no_stack() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            // A run of prefix NOT: counted in a loop, capped.
            for levels in [5_000usize, 100_000] {
                let nots = "NOT ".repeat(levels);
                for operand in ["b", "(b AND b2)", "i4 = 1"] {
                    let err = parse_err(&format!("{nots}{operand}"));
                    assert!(
                        matches!(err, ParseError::NotChainTooLong { .. }),
                        "{levels} NOTs before {operand}: {err:?}"
                    );
                }
            }
            // Within the cap, the tree is still bounded in height.
            let err = parse_err(&format!("{}b", "NOT ".repeat(100)));
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            assert_type(&format!("{}b", "NOT ".repeat(99)), B);
            // NOT at every parenthesis level.
            let err = parse_err(&format!("{}b{}", "NOT (".repeat(5_000), ")".repeat(5_000)));
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            // 50 NOTs at each of 99 levels is the shape that once slipped a
            // bound for minus: ~5,000 nodes deep.
            let mut text = String::from("b");
            for _ in 0..99 {
                text = format!("{}({text})", "NOT ".repeat(50));
            }
            let err = parse_err(&text);
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            // `a + NOT b + NOT c ...` recurses once per NOT in the operand.
            let text = format!("a{}", " + NOT b".repeat(5_000));
            let err = parse_err(&text);
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            // A long AND/OR chain is a left-leaning tree as tall as it is long.
            for op in ["AND", "OR"] {
                for operands in [101usize, 5_000, 100_000] {
                    let text = vec!["b"; operands].join(&format!(" {op} "));
                    let err = parse_err(&text);
                    assert!(
                        matches!(err, ParseError::ExpressionTooDeep { .. }),
                        "{operands} operands of {op}: {err:?}"
                    );
                }
                assert_type(&vec!["b"; 100].join(&format!(" {op} ")), B);
                let err = parse_err(&vec!["b"; 101].join(&format!(" {op} ")));
                assert!(
                    matches!(err, ParseError::ExpressionTooDeep { .. }),
                    "{err:?}"
                );
            }
            // A wide CASE is flat: it parses, validates and renders without
            // recursing per branch. Past the size budget every definition
            // shares, it is refused.
            for (branches, admitted) in [(100usize, true), (1_000, true), (100_000, false)] {
                let mut text = String::from("CASE");
                for i in 0..branches {
                    text.push_str(&format!(" WHEN i4 = {i} THEN {i}"));
                }
                text.push_str(" ELSE -1 END");
                let def = parse_text(&text).expect("a wide CASE parses");
                let validated = validate(&def, &columns());
                if admitted {
                    assert_eq!(validated, Ok(()));
                    let rendered = super::oracle::render_expr_sql(&def.fields[0].expr);
                    assert!(rendered.len() > branches);
                    let again = parse_text(&super::ast::render_field_expr(&def.fields[0].expr))
                        .expect("the rendering parses");
                    assert_eq!(again.fields[0].expr, def.fields[0].expr);
                } else {
                    assert_eq!(validated, Err(ValidationError::FieldExpansionTooLarge));
                }
                drop(def);
            }
            // CASE nested inside CASE conditions and results is a level each.
            let nested = |levels: usize| {
                let mut text = String::from("1");
                for _ in 0..levels {
                    text = format!("CASE WHEN b THEN {text} END");
                }
                text
            };
            assert_type(&nested(99), I4);
            let err = parse_err(&nested(100));
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            let mut text = String::from("b");
            for _ in 0..100 {
                text = format!("CASE WHEN {text} THEN b END");
            }
            let err = parse_err(&text);
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            // IS NULL chains are postfix and counted.
            let text = format!("b{}", " IS NULL".repeat(5_000));
            let err = parse_err(&text);
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
            assert_type(&format!("b{}", " IS NULL".repeat(98)), B);
            // NULLIF nests like any call.
            let mut text = String::from("i4");
            for _ in 0..100 {
                text = format!("NULLIF({text}, 0)");
            }
            let err = parse_err(&text);
            assert!(
                matches!(err, ParseError::ExpressionTooDeep { .. }),
                "{err:?}"
            );
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}

#[test]
fn a_field_chain_through_logic_nodes_is_bounded_too() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            // 400 fields, each reading the one before through a CASE, NOT and IS NULL:
            // the inlined height is what is bounded.
            let mut text = String::from("TRANSFORM t FROM s SELECT b AS f0");
            for i in 1..400 {
                text.push_str(&format!(
                    ", CASE WHEN NOT (f{p} IS NULL) THEN f{p} END AS f{i}",
                    p = i - 1
                ));
            }
            let def = parse(&text).expect("each field is shallow");
            let err = validate(&def, &columns()).expect_err("the chain is too tall to inline");
            assert!(
                err.to_string().contains("deep") || err.to_string().contains("expression"),
                "{err}"
            );
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}

#[test]
fn a_boolean_field_is_a_column_and_an_input_of_another_field() {
    let def = parse(
        "TRANSFORM t FROM s SELECT i4 > 0 AS positive, positive AND b AS both, \
         CASE WHEN both THEN i4 END AS v, NOT positive AS neg, both IS NULL AS unknown",
    )
    .unwrap();
    validate(&def, &columns()).unwrap();
    let types = super::validate::infer_field_types(&def, &columns()).unwrap();
    assert_eq!(types["positive"], B);
    assert_eq!(types["both"], B);
    assert_eq!(types["v"], I4);
    assert_eq!(types["neg"], B);
    assert_eq!(types["unknown"], B);
}

#[test]
fn rendering_round_trips_through_the_parser() {
    for text in [
        "NOT a",
        "NOT a = b",
        "a AND b OR c",
        "a IS NULL",
        "a IS NOT NULL",
        "a IS DISTINCT FROM b",
        "a IS NOT DISTINCT FROM b + 1",
        "CASE WHEN a THEN 1 WHEN b THEN 2 ELSE 3 END",
        "CASE WHEN a THEN 1 END",
        "NULLIF(a, 0)",
        "NOT NOT a",
        "a = NOT b",
        "a <> b",
        "(a < b) IS NULL",
    ] {
        let expr = expr_of(text);
        let rendered = super::ast::render_field_expr(&expr);
        assert_eq!(
            expr_of(&rendered),
            expr,
            "`{text}` rendered as `{rendered}`"
        );
    }
}

#[test]
fn the_sql_renderers_spell_the_new_nodes_alike() {
    for (text, sql) in [
        ("NOT b", "(not \"b\")"),
        ("b IS NULL", "(\"b\" is null)"),
        ("b IS NOT NULL", "(\"b\" is not null)"),
        ("b AND b2", "(\"b\" AND \"b2\")"),
        (
            "i4 IS DISTINCT FROM 1",
            "(\"i4\" IS DISTINCT FROM 1::integer)",
        ),
        ("i4 != 1", "(\"i4\" <> 1::integer)"),
        (
            "CASE WHEN b THEN 1 ELSE 2 END",
            "(case when \"b\" then 1::integer else 2::integer end)",
        ),
        ("NULLIF(i4, 0)", "nullif(\"i4\", 0::integer)"),
    ] {
        assert_eq!(
            super::oracle::render_expr_sql(&expr_of(text)),
            sql,
            "`{text}`"
        );
    }
    assert_eq!(
        super::oracle::render_qualified_expr_sql(&expr_of("CASE WHEN b THEN i4 END"), "s"),
        "(case when s.\"b\" then s.\"i4\" end)"
    );
}
