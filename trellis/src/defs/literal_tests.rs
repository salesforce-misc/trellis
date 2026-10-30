//! The `TRUE`, `FALSE` and `NULL` literals: how they parse, how they type
//! (a `NULL` from its context, as Postgres types `unknown`), how the
//! planner-folding checks read them, and what define refuses. Each expected
//! type and each plan-time refusal here was read off PostgreSQL 15, 17 and 18
//! (`CREATE TABLE AS` for the types, `EXPLAIN` for the plan-time errors); the
//! conformance harness (`generative/tests/expr_conformance.rs`) and
//! `tests/null_literal.rs` check the same against a live server.

use std::collections::HashMap;

use super::ast::{Expr, Operator, ValueType, render_field_expr};
use super::error::ParseError;
use super::parse;
use super::pg_type::PgType;
use super::validate::{ValidationError, infer_field_types, validate};
use crate::float::FloatWidth;
use crate::integer::IntWidth;

const I2: ValueType = ValueType::Integer(IntWidth::Int2);
const I4: ValueType = ValueType::Integer(IntWidth::Int4);
const I8: ValueType = ValueType::Integer(IntWidth::Int8);
const N: ValueType = ValueType::Numeric;
const F4: ValueType = ValueType::Float(FloatWidth::Float4);
const F8: ValueType = ValueType::Float(FloatWidth::Float8);
const B: ValueType = ValueType::Boolean;
const T: ValueType = ValueType::Text;
const D: ValueType = ValueType::Other(PgType::Date);
const TS: ValueType = ValueType::Other(PgType::Timestamp);

fn columns() -> HashMap<String, ValueType> {
    [
        ("i2", I2),
        ("i4", I4),
        ("i8", I8),
        ("n", N),
        ("f4", F4),
        ("f8", F8),
        ("b", B),
        ("t", T),
        ("u", ValueType::Uuid),
        ("d", D),
        ("ts", TS),
        ("tz", ValueType::Other(PgType::TimestampTz)),
        ("j", ValueType::Other(PgType::Jsonb)),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

fn parse_fields(fields: &str) -> Result<super::ast::TransformDef, ParseError> {
    parse(&format!("TRANSFORM t FROM s SELECT {fields}"))
}

fn expr_of(text: &str) -> Expr {
    parse_fields(&format!("{text} AS x"))
        .unwrap_or_else(|e| panic!("`{text}` should parse: {e}"))
        .fields
        .remove(0)
        .expr
}

/// The type of field `x` among `fields`, or the refusal.
fn type_in(fields: &str) -> Result<ValueType, ValidationError> {
    let def = parse_fields(fields).unwrap_or_else(|e| panic!("`{fields}` should parse: {e}"));
    validate(&def, &columns())?;
    Ok(infer_field_types(&def, &columns())?["x"])
}

#[track_caller]
fn assert_type(text: &str, expected: ValueType) {
    match type_in(&format!("{text} AS x")) {
        Ok(found) => assert_eq!(found, expected, "`{text}`"),
        Err(e) => panic!("`{text}` was refused: {e}"),
    }
}

#[track_caller]
fn assert_refused(text: &str, needle: &str) {
    match type_in(&format!("{text} AS x")) {
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
fn assert_untyped(text: &str, context: &str) {
    match type_in(&format!("{text} AS x")) {
        Err(ValidationError::UntypedNull { context: found, .. }) => {
            assert_eq!(found, context, "`{text}`");
        }
        other => panic!("`{text}` should be an untyped NULL: {other:?}"),
    }
}

#[test]
fn the_literals_parse_in_any_case_and_are_never_columns() {
    for (text, expected) in [
        ("TRUE", Expr::BooleanLiteral(true)),
        ("true", Expr::BooleanLiteral(true)),
        ("False", Expr::BooleanLiteral(false)),
        ("NULL", Expr::Null),
        ("null", Expr::Null),
        ("(NULL)", Expr::Null),
    ] {
        assert_eq!(expr_of(text), expected, "`{text}`");
    }
    assert_eq!(expr_of("NOT NULL"), Expr::Not(Box::new(Expr::Null)));
    assert_eq!(
        expr_of("i4 = NULL"),
        Expr::BinaryOp {
            op: Operator::Equal,
            lhs: Box::new(Expr::Column("i4".to_string())),
            rhs: Box::new(Expr::Null),
        }
    );
    // `IS NULL` is still the postfix test, not a comparison with the literal.
    assert_eq!(
        expr_of("NULL IS NULL"),
        Expr::IsNull {
            expr: Box::new(Expr::Null),
            negated: false
        }
    );
    // Postgres refuses each of these as a syntax error.
    for text in [
        "null(1)",
        "true(1)",
        "NULL 'x'",
        "true.i4",
        "null.i4",
        "NULL::integer",
        "true false",
    ] {
        assert!(
            parse_fields(&format!("{text} AS x")).is_err(),
            "`{text}` should be refused"
        );
    }
    // The label after AS and the name after a dot are still any word.
    parse_fields("i4 AS null, s.true AS t").unwrap();
}

#[test]
fn cast_of_null_names_any_type_the_grammar_has_a_value_of() {
    for (text, expected) in [
        ("CAST(NULL AS integer)", I4),
        ("CAST(NULL AS int)", I4),
        ("CAST(NULL AS bigint)", I8),
        ("CAST(NULL AS smallint)", I2),
        ("CAST(NULL AS numeric)", N),
        ("CAST(NULL AS decimal)", N),
        ("CAST(NULL AS real)", F4),
        ("CAST(NULL AS double precision)", F8),
        ("CAST(NULL AS text)", T),
        ("CAST(NULL AS boolean)", B),
        ("CAST(NULL AS uuid)", ValueType::Uuid),
        ("CAST(NULL AS date)", D),
        ("CAST(NULL AS timestamp)", TS),
        ("CAST(NULL AS jsonb)", ValueType::Other(PgType::Jsonb)),
        (
            "CAST(NULL AS timestamptz)",
            ValueType::Other(PgType::TimestampTz),
        ),
        (
            "CAST(NULL AS timestamptz) AT TIME ZONE 'UTC'",
            ValueType::Other(PgType::Timestamp),
        ),
        ("CAST((NULL) AS text)", T),
    ] {
        assert_type(text, expected);
        // The stored text parses back to the same tree.
        let expr = expr_of(text);
        assert_eq!(expr_of(&render_field_expr(&expr)), expr, "`{text}`");
    }
    // A type with a modifier, one this grammar has no value of, and a
    // conversion of a value are refused as before.
    for text in [
        "CAST(NULL AS numeric(10,2))",
        "CAST(NULL AS varchar)",
        "CAST(NULL AS interval)",
        "CAST(i4 AS text)",
    ] {
        assert!(
            matches!(
                parse_fields(&format!("{text} AS x")),
                Err(ParseError::UnsupportedCastTarget { .. })
            ),
            "`{text}`"
        );
    }
}

/// Each type is the one `CREATE TABLE AS` gives the same expression in
/// PostgreSQL 15, 17 and 18.
#[test]
fn null_takes_its_type_from_its_context_as_postgres_types_it() {
    for (text, expected) in [
        ("TRUE", B),
        ("FALSE", B),
        ("b = TRUE", B),
        ("TRUE AND b", B),
        ("NOT FALSE", B),
        // A field that is only NULL is a text column.
        ("NULL", T),
        ("COALESCE(NULL, NULL)", T),
        ("COALESCE(i4, NULL)", I4),
        ("COALESCE(NULL, n)", N),
        ("COALESCE(NULL, 1)", I4),
        ("COALESCE(NULL, NULL, 'x')", T),
        ("COALESCE(NULL, f8)", F8),
        ("CASE WHEN b THEN NULL END", T),
        ("CASE WHEN b THEN NULL ELSE i4 END", I4),
        ("CASE WHEN b THEN i4 ELSE NULL END", I4),
        ("CASE WHEN b THEN NULL WHEN NOT b THEN f4 ELSE NULL END", F4),
        ("CASE WHEN NULL THEN 1 END", I4),
        ("NULLIF(i4, NULL)", I4),
        ("NULLIF(NULL, i4)", I4),
        ("NULLIF(NULL, NULL)", T),
        ("NULLIF(f8, NULL)", F8),
        ("i4 + NULL", I4),
        ("NULL + n", N),
        ("i2 + NULL", I2),
        ("f8 + NULL", F8),
        ("f4 * NULL", F4),
        ("NULL % 2", I4),
        ("d - NULL", I4),
        ("i4 = NULL", B),
        ("NULL = NULL", B),
        ("d = NULL", B),
        ("t = NULL", B),
        ("u <> NULL", B),
        ("i4 IS NOT DISTINCT FROM NULL", B),
        ("NULL IS DISTINCT FROM NULL", B),
        ("NULL AND b", B),
        ("b OR NULL", B),
        ("NOT NULL", B),
        ("NULL IS NULL", B),
        ("NULL IS NOT NULL", B),
    ] {
        assert_type(text, expected);
    }
}

/// A field that is only `NULL` is a `text` column of its own, but every
/// renderer inlines a field it reads, so Postgres sees the literal where the
/// field is read and types it there.
#[test]
fn a_field_that_is_only_null_is_read_as_the_literal() {
    for (fields, expected) in [
        ("NULL AS z, COALESCE(z, 1) AS x", I4),
        ("NULL AS z, z AS m, i4 + m AS x", I4),
        ("NULL AS z, z AS x", T),
        ("NULL AS z, CASE WHEN b THEN z ELSE d END AS x", D),
    ] {
        assert_eq!(type_in(fields).unwrap(), expected, "`{fields}`");
        let def = parse_fields(fields).unwrap();
        assert_eq!(
            infer_field_types(&def, &columns()).unwrap()["z"],
            T,
            "`{fields}`"
        );
    }
    assert!(matches!(
        type_in("NULL AS z, abs(z) AS x"),
        Err(ValidationError::UntypedNull { .. })
    ));
    assert!(matches!(
        type_in("NULL AS z, -z AS x"),
        Err(ValidationError::UntypedNull { .. })
    ));
}

/// Where Postgres finds the `NULL` ambiguous, or types it by function
/// candidate rules this grammar does not model, define refuses it and names
/// the `CAST` remedy.
#[test]
fn an_untypable_null_is_refused_naming_the_cast() {
    assert_untyped("NULL + NULL", "both operands of +");
    assert_untyped("NULL * NULL", "both operands of *");
    assert_untyped("-NULL", "the operand of prefix -");
    assert_untyped("NULL AT TIME ZONE 'UTC'", "an operand of AT TIME ZONE");
    assert_untyped("tz AT TIME ZONE NULL", "an operand of AT TIME ZONE");
    // `date + date` doesn't exist, so Postgres finds `date + unknown`
    // ambiguous.
    assert_untyped("d + NULL", "an operand of + beside a date");
    assert_untyped("t + NULL", "an operand of + beside a text");
    // Postgres types these by its candidate rules (`abs(NULL)` is `double
    // precision`, `sum(NULL)` is ambiguous); define refuses them all.
    assert_untyped("abs(NULL)", "an argument of abs");
    assert_untyped("round(n, NULL)", "an argument of round");
    assert_untyped("char_length(NULL)", "an argument of char_length");
    assert_untyped("strpos(t, NULL)", "an argument of strpos");
    assert_untyped("EXTRACT(year FROM NULL)", "an argument of extract");
    let message = type_in("abs(NULL) AS x").unwrap_err().to_string();
    assert!(message.contains("CAST(NULL AS <type>)"), "{message}");
    // The CAST remedy types it.
    assert_type("abs(CAST(NULL AS integer))", I4);
    assert_type("CAST(NULL AS date) + 1", D);
    assert_type("-CAST(NULL AS numeric)", N);
    // The other refusals keep their own reason.
    assert_refused("NULL < NULL", "text");
    assert_refused("t < NULL", "text");
    assert_refused("TRUE + 1", "+");
    assert_refused("COALESCE(NULL, i4, t)", "coalesce");
    assert_refused(
        "CASE WHEN b THEN NULL WHEN NOT b THEN i4 ELSE t END",
        "case",
    );
    assert_refused("NULLIF(NULL, f8) = i4", "=");
    assert_refused("i4 / NULL", "integer");
    assert_untyped("ts - NULL", "an operand of - beside a timestamp");
}

#[test]
fn an_aggregate_of_null_is_refused() {
    for call in [
        "COUNT(NULL)",
        "SUM(NULL)",
        "MIN(NULL)",
        "BOOL_AND(NULL)",
        "STRING_AGG(NULL, ',')",
    ] {
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY i4 SELECT {call} AS x"
        ))
        .unwrap_or_else(|e| panic!("`{call}`: {e}"));
        let err = validate(&def, &columns()).unwrap_err();
        assert!(
            matches!(err, ValidationError::UntypedNull { .. }),
            "`{call}`: {err:?}"
        );
    }
    let def = parse("TRANSFORM t FROM s GROUP BY i4 SELECT SUM(COALESCE(i8, NULL)) AS x").unwrap();
    validate(&def, &columns()).unwrap();
}

/// The planner folds `NULL` and the booleans with three-valued logic and
/// drops what a decided `CASE`, `AND` or `OR` never evaluates: a constant
/// that overflows there is refused only where Postgres plans it.
#[test]
fn plan_time_checks_fold_the_literals_with_three_valued_logic() {
    const OVERFLOW: &str = "2147483647 + 1";
    for text in [
        format!("CASE WHEN NULL OR TRUE THEN 1 ELSE {OVERFLOW} END"),
        format!("CASE WHEN NULL AND FALSE THEN {OVERFLOW} ELSE 1 END"),
        format!("CASE WHEN i4 = NULL THEN {OVERFLOW} ELSE 1 END"),
        format!("CASE WHEN NOT NULL THEN {OVERFLOW} ELSE 1 END"),
        format!("CASE WHEN NULL THEN {OVERFLOW} ELSE 1 END"),
        format!("CASE WHEN NULL IS NULL THEN 1 ELSE {OVERFLOW} END"),
        format!("CASE WHEN TRUE THEN 1 ELSE {OVERFLOW} END"),
        format!("CASE WHEN FALSE THEN {OVERFLOW} ELSE 1 END"),
        format!("TRUE OR ({OVERFLOW} > 0)"),
        format!("FALSE AND ({OVERFLOW} > 0)"),
    ] {
        type_in(&format!("{text} AS x")).unwrap_or_else(|e| panic!("`{text}`: {e}"));
    }
    for text in [
        format!("CASE WHEN NULL AND TRUE THEN 1 ELSE {OVERFLOW} END"),
        format!("CASE WHEN i4 IS NOT DISTINCT FROM NULL THEN {OVERFLOW} ELSE 1 END"),
        format!("COALESCE(NULL, {OVERFLOW})"),
        format!("NULLIF(NULL, {OVERFLOW})"),
        format!("NULL + ({OVERFLOW})"),
        format!("(NULL = 1) OR ({OVERFLOW} > 0)"),
        format!("NULL AND ({OVERFLOW} > 0)"),
        format!("CASE WHEN i4 > 0 THEN NULL ELSE {OVERFLOW} END"),
    ] {
        assert!(
            matches!(
                type_in(&format!("{text} AS x")),
                Err(ValidationError::ConstantOutOfRange { .. })
            ),
            "`{text}` should be refused as Postgres plans it"
        );
    }
}

#[test]
fn the_literals_round_trip_through_the_stored_text() {
    let text = "TRANSFORM t FROM s SELECT TRUE AS f1, FALSE AS f2, NULL AS f3, \
                COALESCE(i4, NULL) AS f4x, CASE WHEN NULL THEN TRUE ELSE NULL END AS f5, \
                NULLIF(NULL, i4) IS NOT DISTINCT FROM NULL AS f6, CAST(NULL AS text) AS f7";
    let def = parse(text).unwrap();
    let rendered = super::ast::render_definition_text(&def);
    assert_eq!(parse(&rendered).unwrap(), def, "{rendered}");
    validate(&def, &columns()).unwrap();
}

#[test]
fn the_sql_renderers_emit_the_bare_keywords() {
    let expr = expr_of("CASE WHEN b = TRUE THEN NULL ELSE FALSE END");
    assert_eq!(
        super::oracle::render_expr_sql(&expr),
        r#"(case when ("b" = true) then null else false end)"#
    );
    assert_eq!(
        super::oracle::render_qualified_expr_sql(&expr, "s"),
        r#"(case when (s."b" = true) then null else false end)"#
    );
    assert_eq!(
        super::oracle::render_expr_sql(&expr_of("CAST(NULL AS text)")),
        "cast(null as text)"
    );
}
