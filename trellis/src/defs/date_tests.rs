//! Dates in the expression grammar (`date ± integer`, `date - date`,
//! `DATE_TRUNC`, `EXTRACT`, `AT TIME ZONE 'UTC'`, `CAST(date AS timestamp)`):
//! how they parse, type and refuse, and what define folds. The same
//! constructs are checked against a real Postgres by the conformance harness
//! (`generative/tests/expr_conformance.rs`); these pin the rules that need no
//! database.

use std::collections::HashMap;

use super::ast::{Expr, Operator, ValueType, render_field_expr};
use super::error::ParseError;
use super::parse;
use super::pg_type::PgType;
use super::validate::{ValidationError, validate};
use crate::integer::IntWidth;

const DATE: ValueType = ValueType::Other(PgType::Date);
const TS: ValueType = ValueType::Other(PgType::Timestamp);
const TZ: ValueType = ValueType::Other(PgType::TimestampTz);
const I2: ValueType = ValueType::Integer(IntWidth::Int2);
const I4: ValueType = ValueType::Integer(IntWidth::Int4);
const I8: ValueType = ValueType::Integer(IntWidth::Int8);

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

fn sexp(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{text}'"),
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
        ("i2", I2),
        ("i4", I4),
        ("i8", I8),
        ("n", ValueType::Numeric),
        ("f8", ValueType::Float(crate::float::FloatWidth::Float8)),
        ("t", ValueType::Text),
        ("d", DATE),
        ("d2", DATE),
        ("ts", TS),
        ("ts2", TS),
        ("tz", TZ),
        ("tm", ValueType::Other(PgType::Time)),
        ("iv", ValueType::Other(PgType::Interval)),
        // Names that are also words of the date grammar (ADR-0018).
        ("at", I4),
        ("time", I4),
        ("zone", I4),
        ("extract", I4),
        ("date_trunc", I4),
        ("year", I4),
        ("from", I4),
    ] {
        map.insert(name.to_string(), ty);
    }
    map
}

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
fn assert_refused(text: &str, needles: &[&str]) {
    match type_of(text) {
        Ok(found) => panic!("`{text}` was admitted as {found}"),
        Err(e) => {
            let message = e.to_string().to_lowercase();
            for needle in needles {
                assert!(
                    message.contains(&needle.to_lowercase()),
                    "`{text}` was refused without `{needle}`: {e}"
                );
            }
        }
    }
}

#[track_caller]
fn assert_grouping(text: &str, grouping: &str) {
    assert_eq!(sexp(&expr_of(text)), grouping, "`{text}`");
}

#[test]
fn at_time_zone_binds_between_multiplicative_and_unary_minus() {
    for (text, grouping) in [
        ("a - b AT TIME ZONE 'UTC'", "(- a (AT TIME ZONE b 'UTC'))"),
        ("a * b AT TIME ZONE 'UTC'", "(* a (AT TIME ZONE b 'UTC'))"),
        ("-a AT TIME ZONE 'UTC'", "(AT TIME ZONE (neg a) 'UTC')"),
        (
            "a AT TIME ZONE 'UTC' AT TIME ZONE 'UTC'",
            "(AT TIME ZONE (AT TIME ZONE a 'UTC') 'UTC')",
        ),
        ("a AT TIME ZONE 'UTC' + 1", "(+ (AT TIME ZONE a 'UTC') 1)"),
        ("a at time zone 'UTC'", "(AT TIME ZONE a 'UTC')"),
        ("(a + b) AT TIME ZONE 'UTC'", "(AT TIME ZONE (+ a b) 'UTC')"),
        ("a + b AT TIME ZONE 'UTC'", "(+ a (AT TIME ZONE b 'UTC'))"),
    ] {
        assert_grouping(text, grouping);
    }
}

#[test]
fn date_arithmetic_groups_left_to_right_at_the_additive_level() {
    assert_grouping("d + 1 - d2", "(- (+ d 1) d2)");
    assert_grouping("1 + d - 2", "(- (+ 1 d) 2)");
    assert_grouping("d - d2 + 1", "(+ (- d d2) 1)");
}

#[test]
fn extract_and_date_trunc_parse_to_calls_with_the_unit_first() {
    assert_grouping("EXTRACT(year FROM d)", "(extract 'year' d)");
    assert_grouping("extract(YEAR from d)", "(extract 'year' d)");
    assert_grouping("EXTRACT(epoch FROM d + 1)", "(extract 'epoch' (+ d 1))");
    assert_grouping("DATE_TRUNC('MONTH', ts)", "(date_trunc 'month' ts)");
    assert_grouping("date_trunc('month', ts)", "(date_trunc 'month' ts)");
    assert_grouping(
        "EXTRACT(year FROM DATE_TRUNC('year', ts)) + 1",
        "(+ (extract 'year' (date_trunc 'year' ts)) 1)",
    );
    // The field is a bare word and `FROM` is required.
    for text in [
        "EXTRACT('year' FROM d)",
        "EXTRACT(year, d)",
        "EXTRACT(year d)",
        "EXTRACT(year FROM d, 1)",
        "EXTRACT(FROM d)",
        "EXTRACT(year FROM)",
        "EXTRACT()",
        "EXTRACT(1 FROM d)",
    ] {
        parse_err(text);
    }
    // `DATE_TRUNC` is a plain two-argument call.
    for text in ["DATE_TRUNC('month')", "DATE_TRUNC('month', ts, 1)"] {
        let err = parse_err(text);
        assert!(
            matches!(err, ParseError::FunctionArityMismatch { .. }),
            "`{text}`: {err:?}"
        );
    }
}

/// Every form the parser builds renders back to text that parses to the same
/// tree.
#[test]
fn date_expressions_round_trip_through_their_rendering() {
    for text in [
        "d + 1",
        "1 + d",
        "d - 1",
        "d - d2",
        "tz AT TIME ZONE 'UTC'",
        "EXTRACT(year FROM d)",
        "EXTRACT(hour FROM ts)",
        "DATE_TRUNC('week', ts)",
        "CAST(d AS timestamp)",
        "cast(d as timestamp)",
        "EXTRACT(epoch FROM tz AT TIME ZONE 'UTC') - EXTRACT(epoch FROM ts)",
        "DATE_TRUNC('day', tz AT TIME ZONE 'UTC') = CAST(d AS timestamp)",
    ] {
        let tree = expr_of(text);
        let rendered = render_field_expr(&tree);
        let again = expr_of(&rendered);
        assert_eq!(again, tree, "`{text}` rendered as `{rendered}`");
    }
    assert_eq!(
        render_field_expr(&expr_of("extract(year from d)")),
        "EXTRACT(year FROM d)"
    );
    assert_eq!(
        super::oracle::render_expr_sql(&expr_of("extract(year from d)")),
        "extract(year from \"d\")"
    );
    assert_eq!(
        super::oracle::render_qualified_expr_sql(&expr_of("DATE_TRUNC('day', ts)"), "s"),
        "date_trunc('day'::text, s.\"ts\")"
    );
    assert_eq!(
        super::oracle::render_expr_sql(&expr_of("tz AT TIME ZONE 'UTC'")),
        "(\"tz\" AT TIME ZONE 'UTC'::text)"
    );
    assert_eq!(
        super::oracle::render_expr_sql(&expr_of("CAST(d AS timestamp)")),
        "cast(\"d\" as timestamp)"
    );
}

/// ADR-0018: a word the date grammar adds must not change what the grammar
/// accepted before. `AT` is a keyword only after an operand and before `TIME
/// ZONE`; `EXTRACT` only before `(`, where it was an unknown function.
#[test]
fn a_column_named_like_a_date_word_is_still_a_column() {
    for name in [
        "at",
        "time",
        "zone",
        "extract",
        "date_trunc",
        "year",
        "from",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {name} AS c"))
            .unwrap_or_else(|e| panic!("`SELECT {name} AS c`: {e}"));
        assert!(
            matches!(&def.fields[0].expr, Expr::Column(found) if found == name),
            "{name}: {:?}",
            def.fields[0].expr
        );
    }
    assert_grouping("at + time", "(+ at time)");
    assert_grouping("at - zone", "(- at zone)");
    assert_grouping("extract + year", "(+ extract year)");
    assert_grouping("year * from", "(* year from)");
    assert_grouping("at AT TIME ZONE 'UTC'", "(AT TIME ZONE at 'UTC')");
    assert_grouping("(at) AT TIME ZONE 'UTC'", "(AT TIME ZONE at 'UTC')");
    assert_type("at + time", I4);
    assert_type("extract + year + zone", I4);
    assert_type("at - from", I4);
    // After an operand, `AT` without `TIME ZONE` is the error it always was.
    for text in ["a AT b", "a AT TIME b", "a AT TIME", "a AT"] {
        parse_err(text);
    }
    // A name stays usable as a call's name where it is not one of ours.
    assert!(matches!(
        parse_err("at(1)"),
        ParseError::UnsupportedFunction { .. }
    ));
}

#[test]
fn every_admitted_date_form_types_as_postgres_does() {
    // `date ± integer` is a date, in either order for `+`; a smallint widens.
    for text in [
        "d + 1",
        "1 + d",
        "d - 1",
        "d + i2",
        "i2 + d",
        "d - i2",
        "d + i4",
        "i4 + d",
        "d - i4",
        "d + -1",
        "d + (i4 + 1)",
        "(d + 1) + 1",
    ] {
        assert_type(text, DATE);
    }
    // Two dates subtract to the days between, an `integer`.
    assert_type("d - d2", I4);
    assert_type("(d - d2) + 1", I4);
    assert_type("d + (d - d2)", DATE);
    assert_type("d - DATE '2024-01-01'", I4);
    // `DATE_TRUNC` keeps the timestamp.
    for unit in super::registry::DATE_TRUNC_UNITS {
        assert_type(&format!("DATE_TRUNC('{unit}', ts)"), TS);
        assert_type(&format!("date_trunc('{}', ts)", unit.to_uppercase()), TS);
    }
    // `EXTRACT` gives an unconstrained numeric, for every field its type has.
    for field in super::registry::EXTRACT_DATE_FIELDS {
        assert_type(&format!("EXTRACT({field} FROM d)"), ValueType::Numeric);
        assert_type(&format!("EXTRACT({field} FROM ts)"), ValueType::Numeric);
    }
    for field in super::registry::EXTRACT_CLOCK_FIELDS {
        assert_type(&format!("EXTRACT({field} FROM ts)"), ValueType::Numeric);
    }
    assert_type("EXTRACT(year FROM d) + 1", ValueType::Numeric);
    assert_type("EXTRACT(year FROM d) > 2000", ValueType::Boolean);
    // `AT TIME ZONE 'UTC'` turns a timestamptz into a timestamp.
    assert_type("tz AT TIME ZONE 'UTC'", TS);
    assert_type(
        "EXTRACT(hour FROM tz AT TIME ZONE 'UTC')",
        ValueType::Numeric,
    );
    assert_type("DATE_TRUNC('day', tz AT TIME ZONE 'UTC')", TS);
    // `CAST(date AS timestamp)`.
    assert_type("CAST(d AS timestamp)", TS);
    assert_type("CAST(d + 1 AS TIMESTAMP)", TS);
    assert_type("DATE_TRUNC('month', CAST(d AS timestamp))", TS);
    // They compose with comparison.
    assert_type("d + 1 > d2", ValueType::Boolean);
    assert_type("CAST(d AS timestamp) = ts", ValueType::Boolean);
    assert_type("DATE_TRUNC('day', ts) = ts", ValueType::Boolean);
    assert_type("CASE WHEN d - d2 > 7 THEN d + 7 ELSE d END", DATE);
}

#[test]
fn date_arithmetic_refusals_name_the_construct_and_the_remedy() {
    // Not an integer day count.
    assert_refused("d + i8", &["'+'", "bigint", "smallint or integer"]);
    assert_refused("i8 + d", &["'+'", "bigint"]);
    assert_refused("d - i8", &["'-'", "bigint"]);
    assert_refused("d + n", &["'+'", "numeric"]);
    assert_refused("d + 1.5", &["'+'", "numeric"]);
    assert_refused("d + f8", &["'+'", "double precision"]);
    // No such operator.
    assert_refused("d + d2", &["'+'", "two dates", "subtract"]);
    assert_refused("i4 - d", &["'-'", "number minus a date"]);
    assert_refused("d * 2", &["'*'"]);
    // Intervals.
    assert_refused("ts - ts2", &["'-'", "interval", "EXTRACT(EPOCH"]);
    assert_refused("tz - tz", &["interval", "EXTRACT(EPOCH"]);
    assert_refused("d - ts", &["interval"]);
    assert_refused("ts - d", &["interval"]);
    assert_refused("ts + iv", &["interval"]);
    assert_refused("d + iv", &["interval"]);
    assert_refused("iv + iv", &["interval"]);
    assert_refused("ts + 1", &["'+'", "date takes a number of days"]);
    assert_refused("1 + tz", &["'+'"]);
    assert_refused("tm + 1", &["'+'"]);
    assert_refused("tm - tm", &["interval"]);
    assert_refused("d + t", &["'+'"]);
}

#[test]
fn date_function_refusals_name_the_construct_and_the_remedy() {
    // `DATE_TRUNC` on a date or a timestamptz resolves to a STABLE overload.
    assert_refused(
        "DATE_TRUNC('month', d)",
        &["DATE_TRUNC", "CAST(d AS timestamp)"],
    );
    assert_refused(
        "DATE_TRUNC('day', tz)",
        &["DATE_TRUNC", "AT TIME ZONE 'UTC'"],
    );
    assert_refused("DATE_TRUNC('day', tm)", &["DATE_TRUNC", "timestamp"]);
    assert_refused("DATE_TRUNC('day', t)", &["DATE_TRUNC"]);
    // `EXTRACT` on a timestamptz, a time or an interval.
    assert_refused("EXTRACT(hour FROM tz)", &["EXTRACT", "AT TIME ZONE 'UTC'"]);
    assert_refused("EXTRACT(hour FROM tm)", &["EXTRACT", "date or a timestamp"]);
    assert_refused("EXTRACT(year FROM iv)", &["EXTRACT", "date or a timestamp"]);
    assert_refused("EXTRACT(year FROM i4)", &["EXTRACT"]);
    assert_refused("EXTRACT(year FROM t)", &["EXTRACT"]);
    // A field the type does not have.
    for field in ["hour", "minute", "second", "milliseconds", "microseconds"] {
        assert_refused(
            &format!("EXTRACT({field} FROM d)"),
            &["EXTRACT", field, "CAST(d AS timestamp)"],
        );
    }
    for field in ["julian", "timezone", "timezone_hour", "timezone_minute"] {
        assert_refused(&format!("EXTRACT({field} FROM ts)"), &["EXTRACT", field]);
    }
    for field in ["years", "y", "dayofweek", "bogus", "month_"] {
        assert_refused(
            &format!("EXTRACT({field} FROM d)"),
            &["EXTRACT", field, "year"],
        );
    }
    // A unit that is not a literal, or not one of the units.
    assert_refused("DATE_TRUNC(t, ts)", &["DATE_TRUNC", "literal"]);
    for unit in ["days", "mon", "dow", "", "timezone", "week "] {
        assert_refused(
            &format!("DATE_TRUNC('{unit}', ts)"),
            &["DATE_TRUNC", "units admitted"],
        );
    }
    // `AT TIME ZONE` takes a timestamptz and the literal 'UTC'.
    assert_refused("ts AT TIME ZONE 'UTC'", &["AT TIME ZONE", "timestamptz"]);
    assert_refused(
        "d AT TIME ZONE 'UTC'",
        &["AT TIME ZONE", "CAST(d AS timestamp)"],
    );
    assert_refused("i4 AT TIME ZONE 'UTC'", &["AT TIME ZONE"]);
    assert_refused(
        "tz AT TIME ZONE 'America/New_York'",
        &["AT TIME ZONE", "America/New_York", "'UTC'"],
    );
    assert_refused("tz AT TIME ZONE 'utc'", &["AT TIME ZONE", "'UTC'"]);
    assert_refused("tz AT TIME ZONE '+05:00'", &["AT TIME ZONE"]);
    assert_refused("tz AT TIME ZONE t", &["AT TIME ZONE", "literal"]);
    assert_refused("tz AT TIME ZONE 1", &["AT TIME ZONE", "literal"]);
    assert_refused("tz AT TIME ZONE ''", &["AT TIME ZONE"]);
    assert_refused(
        "(tz AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'",
        &["AT TIME ZONE", "timestamptz"],
    );
}

#[test]
fn casts_to_timestamp_admit_only_a_date() {
    for operand in ["ts", "i4", "t", "tm"] {
        assert_refused(&format!("CAST({operand} AS timestamp)"), &["timestamp"]);
    }
    assert_refused("CAST(tz AS timestamp)", &["CAST", "AT TIME ZONE 'UTC'"]);
    // The other targets are unchanged.
    assert_type("CAST(i2 AS integer)", I4);
    assert_refused("CAST(d AS integer)", &["integer"]);
    // A cast to a timestamp spelled another way is not a target.
    for target in ["timestamptz", "timestamp(3)", "date"] {
        parse_err(&format!("CAST(d AS {target})"));
    }
}

#[test]
fn a_constant_date_operation_postgres_cannot_compute_is_refused_at_define() {
    // The latest date is 5874897-12-31: 2145031948 days after 2000-01-01.
    // 9999-12-31 is 2921939 days after it.
    assert_type("DATE '9999-12-31' + 2142110009", DATE);
    assert_refused(
        "DATE '9999-12-31' + 2142110010",
        &["out of range for date", "9999-12-31"],
    );
    assert_refused("DATE '2024-01-01' + 2147483647", &["out of range for date"]);
    assert_refused("2147483647 + DATE '2024-01-01'", &["out of range for date"]);
    // The earliest date is 4714-11-24 BC: 2451545 days before 2000-01-01.
    // 0001-01-01 is 730119 days before it.
    assert_type("DATE '0001-01-01' - 1721426", DATE);
    assert_refused("DATE '0001-01-01' - 1721427", &["out of range for date"]);
    assert_refused("DATE '2024-01-01' - 2147483647", &["out of range for date"]);
    assert_type("DATE '0001-01-01' + -1721426", DATE);
    assert_refused("DATE '0001-01-01' + -1721427", &["out of range for date"]);
    // A constant is folded through calculated fields, nesting and sums.
    assert_refused(
        "(DATE '9999-12-31' + 2142110009) + 1",
        &["out of range for date"],
    );
    assert_type("(DATE '9999-12-31' + 2142110009) - 1", DATE);
    assert_refused(
        "DATE '2024-01-01' + (2147483647 - 1) + 2147483647",
        &["out of range for date"],
    );
    // A column is not a constant: the rows that overflow are quarantined.
    assert_type("d + 2147483647", DATE);
    assert_type("DATE '2024-01-01' + i4", DATE);
    assert_type("d - 2147483647", DATE);
    // A cast of a constant date past `timestamp`'s range.
    assert_type("CAST(DATE '9999-12-31' AS timestamp)", TS);
    assert_type(
        "CAST(DATE '9999-12-31' + 106751982 - 2921939 AS timestamp)",
        TS,
    );
    assert_refused(
        "CAST(DATE '9999-12-31' + 106751982 - 2921939 + 1 AS timestamp)",
        &["out of range for timestamp"],
    );
    assert_type("CAST(DATE '0001-01-01' - 1721426 AS timestamp)", TS);
    // The planner drops what a decided CASE or COALESCE does not take.
    assert_type(
        "CASE WHEN DATE '2024-01-01' < DATE '2024-01-02' THEN d ELSE DATE '2024-01-01' + 2147483647 END",
        DATE,
    );
    assert_type(
        "COALESCE(DATE '2024-01-01', DATE '2024-01-01' + 2147483647)",
        DATE,
    );
    assert_refused(
        "COALESCE(d, DATE '2024-01-01' + 2147483647)",
        &["out of range for date"],
    );
}

/// A constant date folded to a value decides a comparison, as the planner
/// does, and the branch it skips is not checked.
#[test]
fn constant_dates_fold_through_comparisons() {
    assert_type(
        "CASE WHEN DATE '2024-01-01' + 1 = DATE '2024-01-02' THEN 1.5 ELSE 1.0 / 0 END",
        ValueType::Numeric,
    );
    assert_refused(
        "CASE WHEN DATE '2024-01-01' + 1 = DATE '2024-01-03' THEN 1.5 ELSE 1.0 / 0 END",
        &["zero"],
    );
    assert_type(
        "CASE WHEN DATE '2024-01-03' - DATE '2024-01-01' = 2 THEN 1.5 ELSE 1.0 / 0 END",
        ValueType::Numeric,
    );
    assert_refused(
        "CASE WHEN DATE '2024-01-03' - DATE '2024-01-01' = 3 THEN 1.5 ELSE 1.0 / 0 END",
        &["zero"],
    );
    assert_type(
        "CASE WHEN DATE '2024-01-01' < DATE '2024-01-02' THEN 1.5 ELSE 1.0 / 0 END",
        ValueType::Numeric,
    );
}

/// A cast of a constant date is a constant timestamp, so a `CASE` condition
/// that compares it decides which branch the planner keeps.
#[test]
fn a_constant_timestamp_from_a_cast_decides_a_case() {
    assert_type(
        "CASE WHEN TIMESTAMP '2024-02-29 00:00:00' = CAST(DATE '2024-02-29' AS timestamp) \
         THEN 1.5 ELSE 1.0 / 0 END",
        ValueType::Numeric,
    );
    assert_refused(
        "CASE WHEN TIMESTAMP '2024-02-29 12:00:00' IS DISTINCT FROM CAST(DATE '2024-02-29' AS timestamp) \
         THEN 1.0 / 0 END",
        &["zero"],
    );
    assert_refused(
        "CASE WHEN CAST(DATE '2024-02-29' AS timestamp) < TIMESTAMP '2024-02-29 00:00:00.5' \
         THEN 1.0 / 0 ELSE 1.5 END",
        &["zero"],
    );
    assert_type(
        "CASE WHEN CAST(DATE '2024-02-29' AS timestamp) > TIMESTAMP '2024-02-29 00:00:00.5' \
         THEN 1.0 / 0 ELSE 1.5 END",
        ValueType::Numeric,
    );
    // A `NULL` beside a call define has no value for (`DATE_TRUNC` of
    // constants) still makes the comparison `NULL`, so its branch is dropped.
    assert_type(
        "CASE WHEN NULLIF(TIMESTAMP '1970-01-01 00:00:00.5', TIMESTAMP '1970-01-01 00:00:00.5') \
         = DATE_TRUNC('microseconds', TIMESTAMP '2024-02-29 12:00:00') \
         THEN 0 + (9223372036854775807 * 2) ELSE 1 END",
        I8,
    );
    // The latest date that casts (294276-12-31) folds to a timestamp too, so
    // the false condition keeps the planner's `ELSE`.
    assert_refused(
        "CASE WHEN CAST(DATE '9999-12-31' + 106751982 - 2921939 AS timestamp) \
         < TIMESTAMP '9999-01-01 00:00:00' THEN 1.5 ELSE 1.0 / 0 END",
        &["zero"],
    );
}

/// Operators the date grammar does not touch keep their text meaning.
#[test]
fn a_date_expression_is_one_node_per_construct() {
    let tree = expr_of("d + 1");
    assert!(matches!(
        tree,
        Expr::BinaryOp {
            op: Operator::Add,
            ..
        }
    ));
    let tree = expr_of("tz AT TIME ZONE 'UTC'");
    assert!(matches!(
        tree,
        Expr::BinaryOp {
            op: Operator::AtTimeZone,
            ..
        }
    ));
    let tree = expr_of("EXTRACT(year FROM d)");
    assert!(
        matches!(&tree, Expr::FunctionCall { name, args } if name == "EXTRACT" && args.len() == 2)
    );
}

/// Stack discipline: the date forms count toward the parser's height bound
/// like any call or operator, so every walk stays shallow. Each of these runs
/// on the 2 MiB stack a spawned thread gets.
#[test]
fn long_chains_and_deep_nesting_cost_no_stack() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let too_deep = |text: &str| {
                let err = parse_err(text);
                assert!(
                    matches!(err, ParseError::ExpressionTooDeep { .. }),
                    "{err:?}"
                );
            };
            // `AT TIME ZONE` chains are left-leaning trees as tall as long.
            too_deep(&format!("tz{}", " AT TIME ZONE 'UTC'".repeat(5_000)));
            too_deep(&format!("tz{}", " AT TIME ZONE 'UTC'".repeat(101)));
            // `EXTRACT` and `DATE_TRUNC` nest like any call.
            let mut text = String::from("ts");
            for _ in 0..100 {
                text = format!("DATE_TRUNC('day', {text})");
            }
            too_deep(&text);
            let mut text = String::from("d");
            for _ in 0..100 {
                text = format!("EXTRACT(year FROM {text})");
            }
            too_deep(&text);
            // So does a date sum.
            too_deep(&format!("d{}", " + 1".repeat(5_000)));
            // Within the bound, the whole pipeline walks on this stack.
            let mut text = String::from("ts");
            for _ in 0..50 {
                text = format!("DATE_TRUNC('day', {text})");
            }
            assert_type(&text, TS);
            let def = parse_text(&text).expect("parses");
            let rendered = super::oracle::render_expr_sql(&def.fields[0].expr);
            assert!(rendered.starts_with("date_trunc("));
            let again = parse_text(&render_field_expr(&def.fields[0].expr)).expect("reparses");
            assert_eq!(again.fields[0].expr, def.fields[0].expr);
            let text = format!("d{}", " + 1".repeat(90));
            assert_type(&text, DATE);
            // A field chain through date constructs is bounded by the
            // inlined height.
            let mut text = String::from("TRANSFORM t FROM s SELECT d AS f0");
            for i in 1..400 {
                text.push_str(&format!(
                    ", CAST(EXTRACT(year FROM CAST(f{p} AS timestamp)) AS integer) + f{p} AS f{i}",
                    p = i - 1
                ));
            }
            let def = parse(&text).expect("each field is shallow");
            validate(&def, &columns()).expect_err("refused one way or another, without overflow");
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}
