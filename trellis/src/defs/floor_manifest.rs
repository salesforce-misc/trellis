//! The feature-floor manifest, `.github/feature-floors.txt` (ADR-0024): every
//! registry entry with its release, and every admission the validator's type
//! rules make over every operand type, with the case that admits it.
//!
//! The compiler makes each registry entry and each type-rule case declare a
//! release, but it can't see an admission that reuses an existing case: a
//! new `match` arm returning an old variant admits new operand types under an
//! old release, so a release that predates the arm would run the definition
//! and read it differently. The manifest makes every admission a visible
//! line. [`the_manifest_matches_the_registry`] fails when the file and the
//! code differ, and `.github/scripts/check-floors.py` compares the file with
//! the newest released tag's: a line the tag lacks must carry a release after
//! it, and none may be above the crate's release.
//!
//! It also fails an admission that records no case: every type the validator
//! admits goes through a [`TypeRule`] with a release.
//!
//! To accept a change: `TRELLIS_BLESS_FEATURE_FLOORS=1 cargo test -p trellis
//! --lib floor_manifest`.

use std::collections::HashMap;

use super::ast::{JoinKind, Operator, ValueType};
use super::floor::{ExprNode, Feature, KeySpaceKind, Layout, PredicateKind, collect};
use super::pg_type::PgType;
use super::registry::{
    AggregateAdmissionRule, AggregateRule, ArgValue, ArithmeticRule, CastRule, ComparisonRule,
    DateArithmeticRule, ExtractRule, TypeRule, UnifyRule,
};
use crate::float::FloatWidth;
use crate::integer::IntWidth;

const MANIFEST: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../.github/feature-floors.txt");
const BLESS: &str = "TRELLIS_BLESS_FEATURE_FLOORS";

/// `all`, after checking it holds each value of an enum exactly once:
/// `index` is an exhaustive `match` numbering the variants, so a new variant
/// doesn't compile until it has a number, and a number missing from `all`
/// fails here.
fn every<T: Copy + std::fmt::Debug>(all: &[T], index: fn(T) -> usize) -> Vec<T> {
    let mut seen: Vec<usize> = all.iter().map(|v| index(*v)).collect();
    seen.sort_unstable();
    assert_eq!(
        seen,
        (0..all.len()).collect::<Vec<_>>(),
        "the list of {all:?} misses or repeats a variant"
    );
    all.to_vec()
}

/// Every operand type a rule can see: each [`ValueType`] and [`PgType`],
/// with one stand-in enum type.
fn types() -> Vec<ValueType> {
    use ValueType as V;
    let all = [
        V::Numeric,
        V::Integer(IntWidth::Int2),
        V::Integer(IntWidth::Int4),
        V::Integer(IntWidth::Int8),
        V::Float(FloatWidth::Float4),
        V::Float(FloatWidth::Float8),
        V::Text,
        V::Boolean,
        V::Uuid,
        V::Other(PgType::Oid),
        V::Other(PgType::Bytea),
        V::Other(PgType::Date),
        V::Other(PgType::Time),
        V::Other(PgType::TimeTz),
        V::Other(PgType::Timestamp),
        V::Other(PgType::TimestampTz),
        V::Other(PgType::Interval),
        V::Other(PgType::Json),
        V::Other(PgType::Jsonb),
        V::Other(PgType::Inet),
        V::Other(PgType::Cidr),
        V::Other(PgType::MacAddr),
        V::Other(PgType::MacAddr8),
        V::Other(PgType::Bit),
        V::Other(PgType::VarBit),
        V::Other(PgType::Money),
        V::Other(PgType::Xml),
        V::Other(PgType::TsVector),
        V::Other(PgType::TsQuery),
        V::Other(PgType::TextArray),
        V::Other(PgType::Enum("enum:public.e")),
        V::Other(PgType::Unrecognized),
    ];
    every(&all, |t| match t {
        V::Numeric => 0,
        V::Integer(w) => match w {
            IntWidth::Int2 => 1,
            IntWidth::Int4 => 2,
            IntWidth::Int8 => 3,
        },
        V::Float(w) => match w {
            FloatWidth::Float4 => 4,
            FloatWidth::Float8 => 5,
        },
        V::Text => 6,
        V::Boolean => 7,
        V::Uuid => 8,
        V::Other(p) => match p {
            PgType::Oid => 9,
            PgType::Bytea => 10,
            PgType::Date => 11,
            PgType::Time => 12,
            PgType::TimeTz => 13,
            PgType::Timestamp => 14,
            PgType::TimestampTz => 15,
            PgType::Interval => 16,
            PgType::Json => 17,
            PgType::Jsonb => 18,
            PgType::Inet => 19,
            PgType::Cidr => 20,
            PgType::MacAddr => 21,
            PgType::MacAddr8 => 22,
            PgType::Bit => 23,
            PgType::VarBit => 24,
            PgType::Money => 25,
            PgType::Xml => 26,
            PgType::TsVector => 27,
            PgType::TsQuery => 28,
            PgType::TextArray => 29,
            PgType::Enum(_) => 30,
            PgType::Unrecognized => 31,
        },
    })
}

fn operators() -> Vec<Operator> {
    use Operator as O;
    let all = [
        O::Add,
        O::Subtract,
        O::Multiply,
        O::Divide,
        O::Modulo,
        O::GreaterThan,
        O::GreaterThanOrEqual,
        O::LessThan,
        O::LessThanOrEqual,
        O::Equal,
        O::NotEqual,
        O::IsDistinctFrom,
        O::IsNotDistinctFrom,
        O::And,
        O::Or,
        O::AtTimeZone,
    ];
    every(&all, |o| match o {
        O::Add => 0,
        O::Subtract => 1,
        O::Multiply => 2,
        O::Divide => 3,
        O::Modulo => 4,
        O::GreaterThan => 5,
        O::GreaterThanOrEqual => 6,
        O::LessThan => 7,
        O::LessThanOrEqual => 8,
        O::Equal => 9,
        O::NotEqual => 10,
        O::IsDistinctFrom => 11,
        O::IsNotDistinctFrom => 12,
        O::And => 13,
        O::Or => 14,
        O::AtTimeZone => 15,
    })
}

/// Every case of every type rule.
fn type_rules() -> Vec<TypeRule> {
    use TypeRule as T;
    let all = [
        T::Arithmetic(ArithmeticRule::Integer),
        T::Arithmetic(ArithmeticRule::Numeric),
        T::Arithmetic(ArithmeticRule::Float),
        T::Arithmetic(ArithmeticRule::FloatBesideExact),
        T::DateArithmetic(DateArithmeticRule::DateAndDays),
        T::DateArithmetic(DateArithmeticRule::DaysPlusDate),
        T::DateArithmetic(DateArithmeticRule::DateMinusDate),
        T::Logic,
        T::AtTimeZone,
        T::Comparison(ComparisonRule::ExactNumeric),
        T::Comparison(ComparisonRule::Float),
        T::Comparison(ComparisonRule::FloatBesideNumberLiteral),
        T::Comparison(ComparisonRule::Boolean),
        T::Comparison(ComparisonRule::Uuid),
        T::Comparison(ComparisonRule::TextEquality),
        T::Comparison(ComparisonRule::Temporal),
        T::Negate,
        T::Cast(CastRule::IntegerWidening),
        T::Cast(CastRule::IntegerIdentity),
        T::Cast(CastRule::IntegerToNumeric),
        T::Cast(CastRule::NumericIdentity),
        T::Cast(CastRule::DateToTimestamp),
        T::Cast(CastRule::TimestampIdentity),
        T::Unify(UnifyRule::Same),
        T::Unify(UnifyRule::Float),
        T::Unify(UnifyRule::FloatBesideExact),
        T::Unify(UnifyRule::Integer),
        T::Unify(UnifyRule::Numeric),
        T::Extract(ExtractRule::Date),
        T::Extract(ExtractRule::Timestamp),
        T::Aggregate(AggregateRule::SumNarrowInteger),
        T::Aggregate(AggregateRule::SumWideExact),
        T::Aggregate(AggregateRule::SumFloat),
        T::Aggregate(AggregateRule::SumInterval),
        T::Aggregate(AggregateRule::AvgFloat),
        T::Aggregate(AggregateRule::AvgExact),
        T::AggregateAdmission(AggregateAdmissionRule::Count),
        T::AggregateAdmission(AggregateAdmissionRule::SumAvgInteger),
        T::AggregateAdmission(AggregateAdmissionRule::SumAvgScaledNumeric),
    ];
    every(&all, |rule| match rule {
        T::Arithmetic(r) => match r {
            ArithmeticRule::Integer => 0,
            ArithmeticRule::Numeric => 1,
            ArithmeticRule::Float => 2,
            ArithmeticRule::FloatBesideExact => 3,
        },
        T::DateArithmetic(r) => match r {
            DateArithmeticRule::DateAndDays => 4,
            DateArithmeticRule::DaysPlusDate => 5,
            DateArithmeticRule::DateMinusDate => 6,
        },
        T::Logic => 7,
        T::AtTimeZone => 8,
        T::Comparison(r) => match r {
            ComparisonRule::ExactNumeric => 9,
            ComparisonRule::Float => 10,
            ComparisonRule::FloatBesideNumberLiteral => 11,
            ComparisonRule::Boolean => 12,
            ComparisonRule::Uuid => 13,
            ComparisonRule::TextEquality => 14,
            ComparisonRule::Temporal => 15,
        },
        T::Negate => 16,
        T::Cast(r) => match r {
            CastRule::IntegerWidening => 17,
            CastRule::IntegerIdentity => 18,
            CastRule::IntegerToNumeric => 19,
            CastRule::NumericIdentity => 20,
            CastRule::DateToTimestamp => 21,
            CastRule::TimestampIdentity => 22,
        },
        T::Unify(r) => match r {
            UnifyRule::Same => 23,
            UnifyRule::Float => 24,
            UnifyRule::FloatBesideExact => 25,
            UnifyRule::Integer => 26,
            UnifyRule::Numeric => 27,
        },
        T::Extract(r) => match r {
            ExtractRule::Date => 28,
            ExtractRule::Timestamp => 29,
        },
        T::Aggregate(r) => match r {
            AggregateRule::SumNarrowInteger => 30,
            AggregateRule::SumWideExact => 31,
            AggregateRule::SumFloat => 32,
            AggregateRule::SumInterval => 33,
            AggregateRule::AvgFloat => 34,
            AggregateRule::AvgExact => 35,
        },
        T::AggregateAdmission(r) => match r {
            AggregateAdmissionRule::Count => 36,
            AggregateAdmissionRule::SumAvgInteger => 37,
            AggregateAdmissionRule::SumAvgScaledNumeric => 38,
        },
    })
}

/// Every [`Feature`] a registry entry or an exhaustive match declares.
fn features() -> Vec<Feature> {
    use super::registry as r;
    let mut features = Vec::new();
    let key_spaces = [
        KeySpaceKind::OneToOne,
        KeySpaceKind::Aggregate,
        KeySpaceKind::Join,
        KeySpaceKind::JoinOneToOne,
    ];
    features.extend(
        every(&key_spaces, |k| match k {
            KeySpaceKind::OneToOne => 0,
            KeySpaceKind::Aggregate => 1,
            KeySpaceKind::Join => 2,
            KeySpaceKind::JoinOneToOne => 3,
        })
        .into_iter()
        .map(Feature::KeySpace),
    );
    let join_kinds = [JoinKind::Inner, JoinKind::Left];
    features.extend(
        every(&join_kinds, |k| match k {
            JoinKind::Inner => 0,
            JoinKind::Left => 1,
        })
        .into_iter()
        .map(Feature::JoinKind),
    );
    let predicates = [PredicateKind::True];
    features.extend(
        every(&predicates, |p| match p {
            PredicateKind::True => 0,
        })
        .into_iter()
        .map(Feature::Predicate),
    );
    let nodes = [
        ExprNode::Column,
        ExprNode::NumberLiteral,
        ExprNode::StringLiteral,
        ExprNode::BooleanLiteral,
        ExprNode::Null,
        ExprNode::SourceColumn,
        ExprNode::Negate,
        ExprNode::Not,
        ExprNode::IsNull,
        ExprNode::Case,
    ];
    features.extend(
        every(&nodes, |n| match n {
            ExprNode::Column => 0,
            ExprNode::NumberLiteral => 1,
            ExprNode::StringLiteral => 2,
            ExprNode::BooleanLiteral => 3,
            ExprNode::Null => 4,
            ExprNode::SourceColumn => 5,
            ExprNode::Negate => 6,
            ExprNode::Not => 7,
            ExprNode::IsNull => 8,
            ExprNode::Case => 9,
        })
        .into_iter()
        .map(Feature::Expr),
    );
    features.extend(operators().into_iter().map(Feature::Operator));
    let layouts = [
        Layout::DefinitionState,
        Layout::OneToOneLedger,
        Layout::GroupBasis,
        Layout::Join,
        Layout::JoinOneToOne,
    ];
    features.extend(
        every(&layouts, |l| match l {
            Layout::DefinitionState => 0,
            Layout::OneToOneLedger => 1,
            Layout::GroupBasis => 2,
            Layout::Join => 3,
            Layout::JoinOneToOne => 4,
        })
        .into_iter()
        .map(Feature::Layout),
    );
    // The unit features, listed through an exhaustive match of every
    // variant so a new one is listed here or doesn't compile.
    let units = [
        Feature::QualifiedTable,
        Feature::WithoutSourceIndex,
        Feature::AdoptTarget,
        Feature::PartitionedSource,
    ];
    features.extend(every(&units, |f| match f {
        Feature::QualifiedTable => 0,
        Feature::WithoutSourceIndex => 1,
        Feature::AdoptTarget => 2,
        Feature::PartitionedSource => 3,
        // Listed above or below.
        Feature::KeySpace(_)
        | Feature::JoinKind(_)
        | Feature::Predicate(_)
        | Feature::Expr(_)
        | Feature::Operator(_)
        | Feature::Function(_)
        | Feature::TypedLiteral(_)
        | Feature::CastTarget(_)
        | Feature::Layout(_)
        | Feature::TypeRule(_)
        | Feature::Value(_) => usize::MAX,
    }));
    let mut names: Vec<&'static str> = r::FUNCTIONS
        .iter()
        .chain(r::AGGREGATE_FUNCTION_SPECS)
        .map(|spec| spec.name)
        .chain(["COALESCE", "NULLIF", "EXTRACT"])
        .collect();
    let mut seen = std::collections::HashSet::new();
    names.retain(|name| seen.insert(*name));
    features.extend(names.into_iter().map(Feature::Function));
    features.extend(
        super::typed_literal::TYPED_LITERALS
            .iter()
            .map(|spec| Feature::TypedLiteral(spec.value_type)),
    );
    features.extend(
        r::CAST_TARGETS
            .iter()
            .chain(r::NULL_CAST_TARGETS)
            .map(|spec| Feature::CastTarget(spec.value_type)),
    );
    features.extend(type_rules().into_iter().map(Feature::TypeRule));
    let values: [(&[r::ArgValueSpec], fn(&'static str) -> ArgValue); 4] = [
        (r::EXTRACT_DATE_FIELDS, ArgValue::ExtractField),
        (r::EXTRACT_CLOCK_FIELDS, ArgValue::ExtractField),
        (r::DATE_TRUNC_UNITS, ArgValue::DateTruncUnit),
        (r::TIME_ZONES, ArgValue::TimeZone),
    ];
    for (specs, kind) in values {
        features.extend(specs.iter().map(|spec| Feature::Value(kind(spec.value))));
    }
    features
}

/// `call` as the validator runs it, and the type-rule cases it recorded.
/// Panics on an admission that records none: every admission goes through a
/// [`TypeRule`] with a release.
fn admissions<T: std::fmt::Debug>(
    lines: &mut Vec<String>,
    call: String,
    admitted: impl Fn(&T) -> bool,
    run: impl FnOnce() -> T,
) {
    let (result, used) = collect(run);
    // Sorted: a validation records in the order it meets them.
    let mut rules: Vec<String> = used
        .iter()
        .filter_map(|feature| match feature {
            Feature::TypeRule(rule) => Some(format!("{rule:?} {}", rule.since())),
            _ => None,
        })
        .collect();
    rules.sort();
    if !admitted(&result) {
        return;
    }
    assert!(
        !rules.is_empty(),
        "{call} is admitted ({result:?}) without a type rule: return an `Admitted` \
         naming a `TypeRule` case with its release"
    );
    for rule in rules {
        lines.push(format!("admit {call} = {result:?} by {rule}"));
    }
}

/// The manifest's text.
fn manifest() -> String {
    use super::registry as r;
    let mut lines = Vec::new();
    for feature in features() {
        let since = feature
            .since()
            .unwrap_or_else(|| panic!("{feature:?} has no release"));
        lines.push(format!("feature {feature:?} {since}"));
    }
    for spec in r::FUNCTIONS.iter().chain(r::AGGREGATE_FUNCTION_SPECS) {
        lines.push(format!(
            "overload {}{:?} = {:?} {}",
            spec.name, spec.arg_types, spec.return_type, spec.since
        ));
    }
    let types = types();
    for op in operators() {
        for &l in &types {
            for &r in &types {
                admissions(
                    &mut lines,
                    format!("operator({op:?}, {l:?}, {r:?})"),
                    Option::is_some,
                    || r::operator_result_type(op, l, r),
                );
            }
        }
    }
    for &t in &types {
        admissions(&mut lines, format!("negate({t:?})"), Option::is_some, || {
            r::negate_result_type(t)
        });
        admissions(
            &mut lines,
            format!("extract({t:?})"),
            Option::is_some,
            || r::extract_fields(t).map(|fields| fields.len()),
        );
        for &target in &types {
            admissions(
                &mut lines,
                format!("cast({t:?} as {target:?})"),
                Option::is_some,
                || r::cast_result_type(t, target),
            );
            admissions(
                &mut lines,
                format!("unify({t:?}, {target:?})"),
                Option::is_some,
                || super::validate::common_numeric_type(t, target),
            );
        }
    }
    for &name in r::AGGREGATE_FUNCTIONS {
        // `COUNT(*)`. Another aggregate without an argument is the family
        // check the validator runs before it types the argument, which
        // admits nothing: the call with the argument, below, does.
        if name == "COUNT" {
            admissions(
                &mut lines,
                format!("aggregate_admitted({name}, *)"),
                Result::is_ok,
                || super::validate::check_aggregate_admitted(name, None),
            );
        }
        for &t in &types {
            admissions(
                &mut lines,
                format!("aggregate({name}, {t:?})"),
                Option::is_some,
                || r::aggregate_result_type(name, t),
            );
            for typmod in [-1, 0x000c_0006] {
                let arg = super::validate::AggregateArg {
                    column: "c",
                    value_type: t,
                    pg_type: "t",
                    typmod,
                    trellis_target: false,
                };
                admissions(
                    &mut lines,
                    format!("aggregate_admitted({name}, {t:?} typmod {typmod})"),
                    Result::is_ok,
                    || super::validate::check_aggregate_admitted(name, Some(&arg)),
                );
            }
        }
    }
    // The one admission that reads the operand's syntax: a float beside a
    // bare number literal.
    for &t in &types {
        for spec in r::OPERATORS.iter().filter(|spec| spec.operator.is_comparison()) {
            let op = spec.operator;
            let text = format!(
                "TRANSFORM t FROM s SELECT id AS id, c {} 1.5 AS x",
                spec.symbol
            );
            let def = super::parse(&text).expect("parses");
            let columns = HashMap::from([
                ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
                ("c".to_string(), t),
            ]);
            admissions(
                &mut lines,
                format!("comparison({op:?}, {t:?}, number literal)"),
                Result::is_ok,
                || super::validate::validate(&def, &columns),
            );
        }
    }
    let mut out = String::from(
        "# The feature-floor manifest (ADR-0024), generated by trellis's\n\
         # `defs::floor_manifest` test: every registry entry and type-rule case\n\
         # with the release it arrived in, and every admission the validator's\n\
         # type rules make, with the case that admits it. check-floors.py\n\
         # compares it with the newest released tag's. Regenerate with\n\
         # TRELLIS_BLESS_FEATURE_FLOORS=1 cargo test -p trellis --lib floor_manifest\n",
    );
    for line in lines {
        out.push_str(&line);
        out.push('\n');
    }
    out
}

#[test]
fn the_manifest_matches_the_registry() {
    let want = manifest();
    if std::env::var_os(BLESS).is_some() {
        std::fs::write(MANIFEST, &want).expect("writes the manifest");
        return;
    }
    let have = std::fs::read_to_string(MANIFEST).unwrap_or_default();
    if have != want {
        let have_lines: std::collections::HashSet<&str> = have.lines().collect();
        let want_lines: std::collections::HashSet<&str> = want.lines().collect();
        let added: Vec<&str> = want.lines().filter(|l| !have_lines.contains(l)).collect();
        let removed: Vec<&str> = have.lines().filter(|l| !want_lines.contains(l)).collect();
        panic!(
            ".github/feature-floors.txt is not what the registry declares.\n\
             only in the code: {added:#?}\nonly in the file: {removed:#?}\n\
             If the change is intended, regenerate it with {BLESS}=1 cargo test -p trellis \
             --lib floor_manifest, and give every new feature or admission a release after \
             the newest released tag (CONTRIBUTING.md)."
        );
    }
}
