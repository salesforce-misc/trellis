//! The function/operator registry ADR-0004 requires: the grammar's accepted
//! operators and functions must equal the set the SQL renderer emits (the
//! renderer consumes this list too), so it lives as a plain, reusable
//! table rather than being baked into the parser's control flow.
//!
//! The text functions (`strpos`, `octet_length`, `char_length`,
//! `regexp_count`) each carry their arity and argument/
//! return [`ValueType`]s so the parser, validator, and renderer all read
//! the same source of truth rather than each hardcoding a function's shape.

use super::ast::{Operator, ValueType};
use super::pg_type::PgType;
use crate::float::FloatWidth;
use crate::integer::IntWidth;

/// One operator the grammar accepts, and the AST node it parses to.
#[derive(Debug, Clone, Copy)]
pub struct OperatorSpec {
    pub symbol: &'static str,
    pub operator: Operator,
    /// The operator's **canonical** operand types — the signature named in a
    /// [`super::validate::ValidationError::TypeMismatch`].
    ///
    /// This is *not* the admissibility test: exact integers
    /// have their own [`ValueType`], so each operator covers a family of
    /// Postgres operators rather than one signature, and
    /// [`operator_result_type`] is what decides both whether a pair of
    /// operands is admissible and what the result type is. There is
    /// correspondingly no `return_type` field any more — a `+`'s result
    /// depends on its operands (`int4 + int4` is `integer`, `int4 +
    /// numeric` is `numeric`), so a constant could only have been wrong.
    pub arg_types: (ValueType, ValueType),
    /// Binding strength [`super::parser`]'s precedence-climbing loop uses
    /// to group `a OP1 b OP2 c`: higher binds tighter, so an operator with
    /// a higher `precedence` grabs its operands before a lower-precedence
    /// one does. All operators here are left-associative, so operators at
    /// the *same* level still group left-to-right (`a + b + c` parses as
    /// `(a + b) + c`).
    ///
    /// Levels are spaced out (not packed as 0, 1, 2, ...) so a new operator
    /// can be slotted between two existing levels later without
    /// renumbering everything else. Use the [`precedence`] constants below
    /// rather than a raw number.
    pub precedence: u8,
}

/// Named precedence levels, standard SQL/Postgres order — highest binds
/// tightest. Gaps are left between levels for future operators (e.g. `NOT`
/// or `AND`/`OR`, both lower than comparison) without renumbering.
pub mod precedence {
    /// `OR`.
    pub const OR: u8 = 2;
    /// `AND`.
    pub const AND: u8 = 4;
    /// Prefix `NOT`: it binds looser than `IS` and every comparison, so
    /// `NOT a = b` is `NOT (a = b)`.
    pub const NOT: u8 = 6;
    /// `IS NULL`, `IS NOT NULL`, `IS [NOT] DISTINCT FROM`.
    pub const IS: u8 = 8;
    /// `=`, `<>`, `<`, `>`, `<=`, `>=` and similar comparisons.
    pub const COMPARISON: u8 = 10;
    /// Binary `+`, `-`.
    pub const ADDITIVE: u8 = 20;
    /// Binary `*`, `/`, `%`.
    pub const MULTIPLICATIVE: u8 = 30;
    /// `AT TIME ZONE`: tighter than `*` and looser than prefix `-`, as in
    /// Postgres.
    pub const AT_TIME_ZONE: u8 = 40;
}

/// The full set of operators this grammar supports.
///
/// # Precedence table
///
/// [`super::parser`] parses binary operators with a real precedence-climbing
/// loop keyed off each [`OperatorSpec::precedence`] here, so `a OP1 b OP2 c`
/// groups the way a precedence-aware grammar (e.g. Postgres's) would, not
/// flat-left-to-right. `+` sits at [`precedence::ADDITIVE`] and `>` at
/// [`precedence::COMPARISON`], so e.g. `a > b + c` parses as `a > (b + c)`
/// (`+` binds tighter and grabs `b` and `c` first), matching Postgres.
///
/// This matters beyond just matching Postgres's grouping: before this table
/// existed, correctness relied on an accident of the type lattice — `+` is
/// `(Numeric, Numeric) -> Numeric` and `>` is `(Numeric, Numeric) ->
/// Boolean`, so the *wrong* flat-parse regrouping of `a > b + c` (as
/// `(a > b) + c`) happened to get caught by [`super::validate`]'s type
/// checker rather than silently computing a wrong-but-plausible answer. See
/// `mod::tests::mixed_operators_respect_precedence` for the regression test
/// pinning real precedence (renamed from `flat_parse_of_mixed_operators_is_
/// caught_by_type_checking`, which pinned the old, wrong behavior).
///
/// Without a real precedence table, that safety net would have been only
/// an emergent property of the current operator set: the day a second
/// Numeric-returning operator (e.g. `-`, `*`) or a second Boolean-returning
/// operator (e.g. `<`, `=`) is added at a "compatible" spot in the type
/// lattice, both possible flat-parse regroupings of some `a OP1 b OP2 c`
/// could type-check (just to different results) with no type error to catch
/// the mistake. Assigning every new operator a real [`precedence`] level
/// (rather than leaving it flat) is what keeps that from happening.
pub const OPERATORS: &[OperatorSpec] = &[
    OperatorSpec {
        symbol: "+",
        operator: Operator::Add,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::ADDITIVE,
    },
    OperatorSpec {
        symbol: "-",
        operator: Operator::Subtract,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::ADDITIVE,
    },
    OperatorSpec {
        symbol: "*",
        operator: Operator::Multiply,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::MULTIPLICATIVE,
    },
    OperatorSpec {
        symbol: "/",
        operator: Operator::Divide,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::MULTIPLICATIVE,
    },
    OperatorSpec {
        symbol: "%",
        operator: Operator::Modulo,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::MULTIPLICATIVE,
    },
    OperatorSpec {
        symbol: ">",
        operator: Operator::GreaterThan,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
    OperatorSpec {
        symbol: ">=",
        operator: Operator::GreaterThanOrEqual,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
    OperatorSpec {
        symbol: "<",
        operator: Operator::LessThan,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
    OperatorSpec {
        symbol: "<=",
        operator: Operator::LessThanOrEqual,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
    OperatorSpec {
        symbol: "=",
        operator: Operator::Equal,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
    OperatorSpec {
        symbol: "<>",
        operator: Operator::NotEqual,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
    OperatorSpec {
        symbol: "IS DISTINCT FROM",
        operator: Operator::IsDistinctFrom,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::IS,
    },
    OperatorSpec {
        symbol: "IS NOT DISTINCT FROM",
        operator: Operator::IsNotDistinctFrom,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::IS,
    },
    OperatorSpec {
        symbol: "AT TIME ZONE",
        operator: Operator::AtTimeZone,
        arg_types: (ValueType::Other(PgType::TimestampTz), ValueType::Text),
        precedence: precedence::AT_TIME_ZONE,
    },
    OperatorSpec {
        symbol: "AND",
        operator: Operator::And,
        arg_types: (ValueType::Boolean, ValueType::Boolean),
        precedence: precedence::AND,
    },
    OperatorSpec {
        symbol: "OR",
        operator: Operator::Or,
        arg_types: (ValueType::Boolean, ValueType::Boolean),
        precedence: precedence::OR,
    },
];

impl Operator {
    /// Whether this operator compares its operands (every operator whose
    /// result is a `boolean` from two non-boolean-only operands).
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Operator::GreaterThan
                | Operator::GreaterThanOrEqual
                | Operator::LessThan
                | Operator::LessThanOrEqual
                | Operator::Equal
                | Operator::NotEqual
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
        )
    }

    /// Whether this comparison orders its operands (`<`, `<=`, `>`, `>=`),
    /// as opposed to testing equality (`=`, `<>`, `IS [NOT] DISTINCT FROM`).
    pub fn is_ordering(self) -> bool {
        matches!(
            self,
            Operator::GreaterThan
                | Operator::GreaterThanOrEqual
                | Operator::LessThan
                | Operator::LessThanOrEqual
        )
    }

    /// Whether Postgres's grammar refuses `a OP b OP c` for this operator
    /// instead of grouping it: the comparisons and `IS [NOT] DISTINCT FROM`
    /// are non-associative.
    pub fn is_non_associative(self) -> bool {
        self.is_comparison()
    }
}

/// Looks up an operator's spec by its own [`Operator`] variant (as opposed
/// to [`lookup_operator`], keyed by concrete syntax) — what
/// [`super::parser`] wants for [`OperatorSpec::precedence`], and what
/// [`super::validate`]'s type-checker names in a `TypeMismatch` once
/// [`operator_result_type`] has rejected an operand.
pub fn operator_spec(operator: Operator) -> &'static OperatorSpec {
    OPERATORS
        .iter()
        .find(|spec| spec.operator == operator)
        .expect("every Operator variant has a matching OperatorSpec in OPERATORS")
}

/// The result type of `op` applied to operands of `lhs`/`rhs` type, or
/// `None` if Postgres has no such operator or the grammar refuses it — the
/// **overload-resolution** replacement for the old "compare both operands to
/// [`OperatorSpec::arg_types`] for exact equality" check.
///
/// Exact equality was sufficient while every arithmetic/comparison operand
/// was the single [`ValueType::Numeric`] bucket. It stops being sufficient
/// the moment exact integers have their own type: Postgres has a *family* of
/// `+` operators (`int2pl`, `int4pl`, `int24pl`, `numeric_add`, …) plus an
/// implicit `int -> numeric` coercion, so `int4_col + 1` is `integer` while
/// `int4_col + numeric_col` is `numeric`. This function encodes exactly that
/// and nothing more — it is deliberately **not** a general coercion lattice
/// (`super::typed_literal` rejects building one for the same reason), just
/// the closed set of combinations the registered operators admit:
///
/// | lhs | rhs | `+` `-` `*` | `/` | `%` | `>` |
/// |---|---|---|---|---|---|
/// | `Integer(a)` | `Integer(b)` | `Integer(wider(a,b))` | refused | `Integer(wider(a,b))` | `Boolean` |
/// | `Integer(_)` | `Numeric` | `Numeric` | `Numeric` | `Numeric` | `Boolean` |
/// | `Numeric` | `Integer(_)` | `Numeric` | `Numeric` | `Numeric` | `Boolean` |
/// | `Numeric` | `Numeric` | `Numeric` | `Numeric` | `Numeric` | `Boolean` |
/// | `Float(a)` | `Float(b)` | `Float(wider(a,b))` | `Float(wider(a,b))` | refused | `Boolean` |
/// | `Float(_)` | `Integer(_)`/`Numeric` | `Float(Float8)` | `Float(Float8)` | refused | refused |
/// | `Integer(_)`/`Numeric` | `Float(_)` | `Float(Float8)` | `Float(Float8)` | refused | refused |
///
/// Any other combination is `None`: `Text`/`Boolean`/`Uuid`/`Other` have no
/// arithmetic or `>` in this grammar. `Integer / Integer` is refused because
/// Postgres truncates the quotient, and `%` has no float form in Postgres.
///
/// `Integer + Numeric -> Numeric` mirrors Postgres promoting the integer
/// operand through its implicit cast and then running `numeric_add`, which
/// is *unbounded* — so a mixed-type sum correctly cannot overflow, while an
/// all-integer one correctly can (the SQL raises on overflow).
/// The renderer emits the same operator, so Postgres types the sum as this
/// table does; if the two disagreed, the target column's declared type and
/// the value written into it would disagree too.
///
/// The float rows look asymmetric and are not: Postgres has
/// `float4pl`/`float8pl`/`float48pl`/`float84pl` and nothing else, so *any*
/// mix of a float with a non-float resolves by casting **both** operands to
/// `double precision`. Verified against a live server rather than reasoned
/// from the cast table — `select pg_typeof(1.5::real + 1::int)`,
/// `pg_typeof(1.5::real + 1.5::numeric)` and `pg_typeof(1.5::real +
/// 1::smallint)` are all `double precision`, while `pg_typeof(1.5::real +
/// 1.5::real)` is `real`. Note this differs from how `COALESCE` unifies the
/// same pair (`coalesce(1::real, 1::numeric)` is `real`); see `validate`'s
/// `common_numeric_type`, which models that separately because Postgres
/// does.
pub fn operator_result_type(op: Operator, lhs: ValueType, rhs: ValueType) -> Option<ValueType> {
    if op.is_comparison() {
        return comparison_result_type(op, lhs, rhs);
    }
    if matches!(op, Operator::And | Operator::Or) {
        return (lhs == ValueType::Boolean && rhs == ValueType::Boolean)
            .then_some(ValueType::Boolean);
    }
    if op == Operator::AtTimeZone {
        // The zone is a text literal (`validate` admits only `'UTC'`); the
        // `timestamptz` operand gives the same instant as a `timestamp`.
        return (lhs == ValueType::Other(PgType::TimestampTz) && rhs == ValueType::Text)
            .then_some(ValueType::Other(PgType::Timestamp));
    }
    if let Some(result) = date_arithmetic_result_type(op, lhs, rhs) {
        return result;
    }
    if !lhs.is_numeric_family() || !rhs.is_numeric_family() {
        return None;
    }
    // The type `+`, `-` and `*` give: two integers the wider width, a float
    // beside anything the float rule, otherwise `numeric`.
    let arithmetic = || match (lhs, rhs) {
        (ValueType::Float(a), ValueType::Float(b)) => ValueType::Float(a.wider(b)),
        // A float mixed with anything else is the `float8` operator on two
        // `double precision` operands.
        (ValueType::Float(_), _) | (_, ValueType::Float(_)) => ValueType::Float(FloatWidth::Float8),
        (ValueType::Integer(a), ValueType::Integer(b)) => ValueType::Integer(a.wider(b)),
        _ => ValueType::Numeric,
    };
    Some(match op {
        Operator::Add | Operator::Subtract | Operator::Multiply => arithmetic(),
        // Postgres truncates `integer / integer` (`7 / 2` is `3`), so the
        // quotient of two integers is refused rather than typed.
        Operator::Divide => match (lhs, rhs) {
            (ValueType::Integer(_), ValueType::Integer(_)) => return None,
            _ => arithmetic(),
        },
        // Postgres has no float modulo.
        Operator::Modulo => match (lhs, rhs) {
            (ValueType::Float(_), _) | (_, ValueType::Float(_)) => return None,
            _ => arithmetic(),
        },
        // Comparisons and `AND`/`OR` returned above.
        _ => unreachable!("comparisons and logic returned above"),
    })
}

/// `date ± integer`, `integer + date` and `date - date`, the date operators
/// `+`/`-` have (`date_pli`, `date_mii`, `date_mi`): `Some(Some(result))` for
/// an admitted pair, `Some(None)` for a pair with a date that is refused, and
/// `None` when neither operand is a date (the numeric rules apply).
///
/// | operands | `+` | `-` |
/// |---|---|---|
/// | `date`, `smallint`/`integer` | `date` | `date` |
/// | `smallint`/`integer`, `date` | `date` | refused |
/// | `date`, `date` | refused | `integer` (days) |
///
/// Postgres has no `date + bigint` or `date + numeric`; a `smallint` widens to
/// `integer`.
fn date_arithmetic_result_type(
    op: Operator,
    lhs: ValueType,
    rhs: ValueType,
) -> Option<Option<ValueType>> {
    const DATE: ValueType = ValueType::Other(PgType::Date);
    if !matches!(op, Operator::Add | Operator::Subtract) || (lhs != DATE && rhs != DATE) {
        return None;
    }
    let days = |t: ValueType| {
        matches!(
            t,
            ValueType::Integer(IntWidth::Int2) | ValueType::Integer(IntWidth::Int4)
        )
    };
    Some(match (op, lhs, rhs) {
        (_, DATE, n) if days(n) => Some(DATE),
        (Operator::Add, n, DATE) if days(n) => Some(DATE),
        (Operator::Subtract, DATE, DATE) => Some(ValueType::Integer(IntWidth::Int4)),
        _ => None,
    })
}

/// Whether `lhs` and `rhs` are two operands one family of comparison
/// operators takes, and `boolean` if so:
///
/// | operands | `=`, `<>`, `IS [NOT] DISTINCT FROM` | `<`, `<=`, `>`, `>=` |
/// |---|---|---|
/// | exact numeric (any integer width, `numeric`) | yes | yes |
/// | float beside float (either width) | yes | yes |
/// | float beside an exact numeric | refused | refused |
/// | the same temporal type (`date`, `timestamp`, `timestamptz`, `time`, `timetz`) | yes | yes |
/// | `boolean`, `uuid` beside the same | yes | yes |
/// | `text` beside `text` | yes | refused (collation order) |
///
/// Everything else is `None`. A float against a *bare number literal* is
/// admitted by [`super::validate`], which sees the operand's syntax.
pub fn comparison_result_type(op: Operator, lhs: ValueType, rhs: ValueType) -> Option<ValueType> {
    let comparable = match (lhs, rhs) {
        (l, r) if l.is_exact_numeric_family() && r.is_exact_numeric_family() => true,
        (ValueType::Float(_), ValueType::Float(_)) => true,
        (ValueType::Boolean, ValueType::Boolean) | (ValueType::Uuid, ValueType::Uuid) => true,
        (ValueType::Text, ValueType::Text) => !op.is_ordering(),
        (ValueType::Other(a), ValueType::Other(b)) => a == b && is_comparable_temporal(a),
        _ => false,
    };
    comparable.then_some(ValueType::Boolean)
}

/// The temporal types whose same-type comparison is `IMMUTABLE`.
pub fn is_comparable_temporal(pg_type: PgType) -> bool {
    matches!(
        pg_type,
        PgType::Date | PgType::Time | PgType::TimeTz | PgType::Timestamp | PgType::TimestampTz
    )
}

/// Whether `lhs` and `rhs` are two different temporal types of the
/// comparable set (`date` beside `timestamp`): the pair a comparison refuses
/// with the cast remedy.
pub fn is_mixed_temporal(lhs: ValueType, rhs: ValueType) -> bool {
    matches!((lhs, rhs), (ValueType::Other(a), ValueType::Other(b))
        if a != b && is_comparable_temporal(a) && is_comparable_temporal(b))
}

/// The result type of prefix `-` over an operand of type `operand`: the
/// operand's own type (`int2um`, `int4um`, `int8um`, `numeric_uminus`,
/// `float4um`, `float8um`), `None` for a non-numeric operand.
pub fn negate_result_type(operand: ValueType) -> Option<ValueType> {
    operand.is_numeric_family().then_some(operand)
}

/// The types `CAST(<expr> AS <type>)` can name, spelled as Postgres prints
/// them. A modifier (`numeric(10,2)`) is not a target.
pub const CAST_TARGETS: &[ValueType] = &[
    ValueType::Integer(IntWidth::Int4),
    ValueType::Integer(IntWidth::Int8),
    ValueType::Numeric,
    ValueType::Other(PgType::Timestamp),
];

/// Postgres's exact, permanent synonyms for the [`CAST_TARGETS`], accepted in
/// `CAST` target position only (issue #1455): `int` and `int4` for `integer`,
/// `int8` for `bigint`, `decimal` for `numeric`. Every other place a type name
/// appears (typed literals, column names) is unchanged, and rendering always
/// emits the canonical name.
pub const CAST_TARGET_ALIASES: &[(&str, ValueType)] = &[
    ("int", ValueType::Integer(IntWidth::Int4)),
    ("int4", ValueType::Integer(IntWidth::Int4)),
    ("int8", ValueType::Integer(IntWidth::Int8)),
    ("decimal", ValueType::Numeric),
];

/// The `CAST` target a type keyword names, matched case-insensitively: a
/// canonical spelling or an alias, resolved to its canonical [`ValueType`].
pub fn cast_target(keyword: &str) -> Option<ValueType> {
    CAST_TARGETS
        .iter()
        .copied()
        .find(|t| t.to_string().eq_ignore_ascii_case(keyword))
        .or_else(|| {
            CAST_TARGET_ALIASES
                .iter()
                .find(|(alias, _)| alias.eq_ignore_ascii_case(keyword))
                .map(|&(_, t)| t)
        })
}

/// The result type of `CAST(<operand> AS <target>)`, or `None` if the cast is
/// not one of the admitted ones: an integer to a strictly wider integer, any
/// integer to `numeric`, or a `date` to `timestamp`. Every other cast narrows
/// (and can overflow), rounds, goes to or from a float, or reads a session
/// setting (`timestamptz` to `timestamp`).
///
/// `date` to `timestamp` is exact and `IMMUTABLE`, but not total: a date past
/// 294276 AD is out of range for `timestamp`, which is a data error (class 22)
/// for the row that holds it.
pub fn cast_result_type(operand: ValueType, target: ValueType) -> Option<ValueType> {
    match (operand, target) {
        (ValueType::Integer(from), ValueType::Integer(to)) if from.range().1 < to.range().1 => {
            Some(target)
        }
        (ValueType::Integer(_), ValueType::Numeric) => Some(ValueType::Numeric),
        (ValueType::Other(PgType::Date), ValueType::Other(PgType::Timestamp)) => Some(target),
        _ => None,
    }
}

/// Whether one operand is a float and the other an exact numeric (an integer
/// of any width, or `numeric`): the pair `>` refuses at define time.
pub fn is_mixed_float_exact(lhs: ValueType, rhs: ValueType) -> bool {
    matches!(
        (lhs, rhs),
        (ValueType::Float(_), r) if r.is_exact_numeric_family()
    ) || matches!(
        (lhs, rhs),
        (l, ValueType::Float(_)) if l.is_exact_numeric_family()
    )
}

/// One function the grammar accepts, and the arity/types the parser and
/// validator check a call against.
#[derive(Debug, Clone, Copy)]
pub struct FunctionSpec {
    /// Canonical uppercased name, matched against the parser's uppercased
    /// identifier (see [`super::parser`]) and lowercased back to Postgres's
    /// own spelling when rendering a call to SQL (see [`super::oracle`]).
    pub name: &'static str,
    pub arg_types: &'static [ValueType],
    pub return_type: ValueType,
}

/// Function names the grammar accepts as calculated-field functions,
/// each rendered as the Postgres function of the same name (per ADR-0004:
/// the grammar and renderer function sets are one list).
///
/// The four text functions return Postgres `integer` (`int4`), not `numeric` —
/// checked against `pg_proc.prorettype` for `strpos`/`octet_length`/
/// `char_length`/`regexp_count`. The numeric functions (`ABS`, `CEIL`,
/// `FLOOR`, `ROUND`, `TRUNC`) are typed by their argument, so a name has one
/// entry per signature ([`function_overloads`], [`resolve_function`]). Declaring them as [`ValueType`] integers is what
/// makes `char_length(t) + 1` type — and overflow — the way Postgres does,
/// and what declares the derived column `integer` rather than `numeric`.
pub const FUNCTIONS: &[FunctionSpec] = &[
    FunctionSpec {
        name: "STRPOS",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "OCTET_LENGTH",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "CHAR_LENGTH",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "REGEXP_COUNT",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "DATE_TRUNC",
        arg_types: &[ValueType::Text, ValueType::Other(PgType::Timestamp)],
        return_type: ValueType::Other(PgType::Timestamp),
    },
    FunctionSpec {
        name: "ABS",
        arg_types: &[ValueType::Integer(IntWidth::Int2)],
        return_type: ValueType::Integer(IntWidth::Int2),
    },
    FunctionSpec {
        name: "ABS",
        arg_types: &[ValueType::Integer(IntWidth::Int4)],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "ABS",
        arg_types: &[ValueType::Integer(IntWidth::Int8)],
        return_type: ValueType::Integer(IntWidth::Int8),
    },
    FunctionSpec {
        name: "ABS",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "ABS",
        arg_types: &[ValueType::Float(FloatWidth::Float4)],
        return_type: ValueType::Float(FloatWidth::Float4),
    },
    FunctionSpec {
        name: "ABS",
        arg_types: &[ValueType::Float(FloatWidth::Float8)],
        return_type: ValueType::Float(FloatWidth::Float8),
    },
    FunctionSpec {
        name: "CEIL",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "CEIL",
        arg_types: &[ValueType::Float(FloatWidth::Float8)],
        return_type: ValueType::Float(FloatWidth::Float8),
    },
    FunctionSpec {
        name: "FLOOR",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "FLOOR",
        arg_types: &[ValueType::Float(FloatWidth::Float8)],
        return_type: ValueType::Float(FloatWidth::Float8),
    },
    FunctionSpec {
        name: "TRUNC",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "TRUNC",
        arg_types: &[ValueType::Float(FloatWidth::Float8)],
        return_type: ValueType::Float(FloatWidth::Float8),
    },
    FunctionSpec {
        name: "ROUND",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "ROUND",
        arg_types: &[ValueType::Numeric, ValueType::Integer(IntWidth::Int4)],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "TRUNC",
        arg_types: &[ValueType::Numeric, ValueType::Integer(IntWidth::Int4)],
        return_type: ValueType::Numeric,
    },
];

/// Every overload of the function named `name` (canonical uppercase).
///
/// A name with one signature has one entry. The numeric functions have
/// several, because Postgres types them by argument: `ABS` of a `smallint`
/// is `smallint`, of a `numeric` is `numeric`.
pub fn function_overloads(name: &str) -> impl Iterator<Item = &'static FunctionSpec> + '_ {
    FUNCTIONS.iter().filter(move |spec| spec.name == name)
}

/// Looks up a function by its canonical uppercased name: its first
/// overload.
pub fn lookup_function(name: &str) -> Option<&'static FunctionSpec> {
    function_overloads(name).next()
}

/// The overload of `name` whose declared argument types are exactly
/// `arg_types`.
pub fn resolve_function(name: &str, arg_types: &[ValueType]) -> Option<&'static FunctionSpec> {
    function_overloads(name).find(|spec| spec.arg_types == arg_types)
}

/// Function arguments the grammar requires to be an integer literal, as
/// `(function, argument index)`: the scale of `ROUND(a, n)` and
/// `TRUNC(a, n)`. A column there would make the result's scale depend on the
/// row; a literal may be negative (`ROUND(a, -2)`).
pub const INTEGER_LITERAL_ARGS: &[(&str, usize)] = &[("ROUND", 1), ("TRUNC", 1)];

/// The units `DATE_TRUNC('<unit>', <timestamp>)` takes, spelled as Postgres
/// prints them. `week` truncates to Monday.
pub const DATE_TRUNC_UNITS: &[&str] = &[
    "microseconds",
    "milliseconds",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "decade",
    "century",
    "millennium",
];

/// The `EXTRACT(<field> FROM <date>)` fields a `date` has. A `timestamp` has
/// these and [`EXTRACT_CLOCK_FIELDS`].
pub const EXTRACT_DATE_FIELDS: &[&str] = &[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "dow",
    "isodow",
    "doy",
    "isoyear",
    "decade",
    "century",
    "millennium",
    "epoch",
];

/// The `EXTRACT` fields only a `timestamp` has: Postgres raises for these on
/// a `date`.
pub const EXTRACT_CLOCK_FIELDS: &[&str] =
    &["hour", "minute", "second", "milliseconds", "microseconds"];

/// The `EXTRACT` fields Postgres accepts for a `date` or `timestamp` and the
/// grammar refuses: `julian` (a different epoch per type) and the zone
/// fields, which a zone-less value does not have.
pub const EXTRACT_REFUSED_FIELDS: &[&str] =
    &["julian", "timezone", "timezone_hour", "timezone_minute"];

/// The `EXTRACT` fields `operand` has, or `None` when `EXTRACT` does not
/// admit the type (`timestamptz` reads `TimeZone`; `time` and `interval` are
/// not admitted).
pub fn extract_fields(operand: ValueType) -> Option<Vec<&'static str>> {
    match operand {
        ValueType::Other(PgType::Date) => Some(EXTRACT_DATE_FIELDS.to_vec()),
        ValueType::Other(PgType::Timestamp) => Some(
            EXTRACT_DATE_FIELDS
                .iter()
                .chain(EXTRACT_CLOCK_FIELDS)
                .copied()
                .collect(),
        ),
        _ => None,
    }
}

/// Aggregate function names, called out specifically so a rejection can
/// explain that they need an aggregate key-space (`GROUP BY`), not that
/// they're simply unknown.
pub const AGGREGATE_FUNCTIONS: &[&str] = &[
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "BOOL_AND",
    "BOOL_OR",
    "BIT_AND",
    "BIT_OR",
    "JSONB_AGG",
    "ARRAY_AGG",
    "STRING_AGG",
];

/// The aggregate functions the grammar parses, each entry's `arg_types`
/// length its arity, which [`super::parser`] enforces. `COUNT`, `SUM` and
/// `AVG` are the ones an [`super::ast::KeySpace::Aggregate`] or `JOIN`
/// definition keeps; every other name is parsed only so
/// the validator can refuse it by family
/// (`validate::check_aggregate_admitted`), and its declared types are a
/// placeholder. Which argument types `SUM` and `AVG` accept, and what they
/// return for each, is [`aggregate_result_type`]'s job.
///
/// `COUNT` is the one entry whose arity isn't read from here. Its
/// `arg_types` is empty, and [`super::parser`] special-cases both forms:
/// `COUNT(*)` counts rows and `COUNT(<expr>)` counts non-null
/// values of any argument type. It is `bigint`, as Postgres
/// types `count(*)`.
pub const AGGREGATE_FUNCTION_SPECS: &[FunctionSpec] = &[
    FunctionSpec {
        name: "SUM",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "AVG",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "COUNT",
        arg_types: &[],
        return_type: ValueType::Integer(IntWidth::Int8),
    },
    FunctionSpec {
        name: "MIN",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "MAX",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "BOOL_AND",
        arg_types: &[ValueType::Boolean],
        return_type: ValueType::Boolean,
    },
    FunctionSpec {
        name: "BOOL_OR",
        arg_types: &[ValueType::Boolean],
        return_type: ValueType::Boolean,
    },
    FunctionSpec {
        name: "BIT_AND",
        arg_types: &[ValueType::Other(PgType::VarBit)],
        return_type: ValueType::Other(PgType::VarBit),
    },
    FunctionSpec {
        name: "BIT_OR",
        arg_types: &[ValueType::Other(PgType::VarBit)],
        return_type: ValueType::Other(PgType::VarBit),
    },
    FunctionSpec {
        name: "JSONB_AGG",
        arg_types: &[ValueType::Other(PgType::Jsonb)],
        return_type: ValueType::Other(PgType::Jsonb),
    },
    FunctionSpec {
        name: "ARRAY_AGG",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Other(PgType::TextArray),
    },
    FunctionSpec {
        name: "STRING_AGG",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Text,
    },
];

/// Looks up an aggregate function by its canonical uppercased name — the
/// [`AGGREGATE_FUNCTION_SPECS`] counterpart to [`lookup_function`].
pub fn lookup_aggregate_function(name: &str) -> Option<&'static FunctionSpec> {
    AGGREGATE_FUNCTION_SPECS
        .iter()
        .find(|spec| spec.name == name)
}

/// The result type of aggregate `name` over an argument of type `arg` —
/// [`operator_result_type`]'s aggregate twin, and for the same
/// reason: once exact integers are their own type, an aggregate's result
/// type is a *function of its argument's* type in Postgres, not a constant.
/// `None` for an argument type the aggregate doesn't accept.
///
/// The rules are Postgres's own (`pg_aggregate` → `pg_proc.prorettype`), and
/// two of them are the kind of thing that looks wrong until you check:
///
/// * `sum(smallint)` and `sum(integer)` return **`bigint`** — Postgres
///   widens, because a sum of many `int4`s routinely leaves `int4`. It does
///   still raise `22003` once the sum leaves `bigint`.
/// * `sum(bigint)` returns **`numeric`**, not `bigint` — the same reasoning
///   one step further up, and it is why a `bigint` sum cannot overflow.
/// * `avg(<any exact integer>)` returns `numeric`; the average of integers
///   is not an integer.
///
/// The float rows are *not* the integer rows'
/// shape — Postgres does not widen a float sum, because a binary float's
/// exponent range already covers what widening would buy:
///
/// * `sum(real)` returns **`real`** and `sum(double precision)` returns
///   `double precision` — each keeps its argument's type, unlike
///   `sum(int4) -> bigint`.
/// * `avg(real)` returns **`double precision`**, not `real` — Postgres
///   accumulates a `real` average in `float8`. `avg(double precision)` is
///   `double precision`.
///
/// All verified with `select pg_typeof(<agg>(x)) from (values
/// (1.5::real)) t(x)` against a live server, not read off the docs.
///
/// `COUNT` is deliberately absent and keeps the constant `Numeric` return
/// type its [`AGGREGATE_FUNCTION_SPECS`] row declares. It is arity-0
/// `COUNT(*)` row-counting in this grammar — type-agnostic by construction,
/// which is why `docs/type-support.md` omits it from every per-type
/// aggregate cell. Postgres types `count(*)` as `bigint`; aligning that is
/// orthogonal to the exact-integer split (it would change a derived
/// column's type for definitions that reference no integer at all) and is
/// not done here.
pub fn aggregate_result_type(name: &str, arg: ValueType) -> Option<ValueType> {
    // A float or `interval` sum is typed so a definition reaches its family
    // refusal (`validate::check_aggregate_admitted`, #1119) rather than a
    // type error; the group basis keeps neither.
    if let ValueType::Other(PgType::Interval) = arg {
        return (name == "SUM").then_some(arg);
    }
    if !arg.is_numeric_family() {
        return None;
    }
    Some(match (name, arg) {
        ("SUM", ValueType::Integer(IntWidth::Int2 | IntWidth::Int4)) => {
            ValueType::Integer(IntWidth::Int8)
        }
        ("SUM", ValueType::Integer(IntWidth::Int8) | ValueType::Numeric) => ValueType::Numeric,
        ("SUM", ValueType::Float(width)) => ValueType::Float(width),
        ("AVG", ValueType::Float(_)) => ValueType::Float(FloatWidth::Float8),
        ("AVG", _) => ValueType::Numeric,
        _ => return None,
    })
}

/// Identifiers known to be non-immutable (depend on database/session state
/// rather than solely their inputs), called out per
/// `docs/transforms.md#calculated-fields` so a rejection can name the
/// immutability rule rather than reporting a generic unknown-function error.
pub const NON_IMMUTABLE_NAMES: &[&str] = &[
    "NOW",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CLOCK_TIMESTAMP",
    "RANDOM",
    "STATEMENT_TIMESTAMP",
    "TRANSACTION_TIMESTAMP",
];

/// The words Postgres cannot take as an unquoted column name: those
/// `pg_get_keywords()` reports with category `R` (reserved) or `T` (reserved
/// as a type or function name), sorted. A bare identifier equal to one is
/// refused in expression position (ADR-0022); the same word after a `.`,
/// after `AS`, or before `(` is not a bare identifier and is unaffected. A
/// test compares this list with a live server's and fails when they drift.
/// `system_user` is reserved from PostgreSQL 16; 15 doesn't reserve it, and
/// Trellis refuses it there too.
pub const RESERVED_WORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

/// Whether `word` is one Postgres will not take as an unquoted column name.
pub fn is_reserved_word(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    RESERVED_WORDS.binary_search(&lower.as_str()).is_ok()
}

/// Looks up an operator by its concrete-syntax symbol.
pub fn lookup_operator(symbol: &str) -> Option<Operator> {
    // `!=` is Postgres's second spelling of `<>`.
    let symbol = if symbol == "!=" { "<>" } else { symbol };
    OPERATORS
        .iter()
        .find(|spec| spec.symbol == symbol)
        .map(|spec| spec.operator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reserved words PostgreSQL 15 lacks, so the drift test can compare
    /// the list with a 15 server.
    const RESERVED_WORDS_ABSENT_IN_15: &[&str] = &["system_user"];

    /// Every aggregate maps a float argument to a float result and an exact
    /// argument to an exact one.
    ///
    /// Pinned so a future aggregate that returns `numeric` for a float
    /// argument fails loudly.
    #[test]
    fn aggregate_results_stay_in_their_arguments_family() {
        let args = IntWidth::ALL
            .into_iter()
            .map(ValueType::Integer)
            .chain(std::iter::once(ValueType::Numeric))
            .chain(FloatWidth::ALL.into_iter().map(ValueType::Float));
        for arg in args {
            for name in ["SUM", "AVG"] {
                let result = aggregate_result_type(name, arg)
                    .unwrap_or_else(|| panic!("{name} over {arg} must resolve"));
                assert_eq!(
                    matches!(arg, ValueType::Float(_)),
                    matches!(result, ValueType::Float(_)),
                    "{name}({arg}) -> {result} crosses the float/exact boundary"
                );
            }
        }
    }

    /// The float result types, spelled out — the live-Postgres counterpart
    /// is `float_aggregate_column_types_match_postgres` in
    /// `trellis/tests/defs_floats.rs`.
    #[test]
    fn float_aggregate_result_types_match_postgres() {
        use FloatWidth::{Float4, Float8};
        // `sum(real)` is `real` — Postgres does *not* widen a float sum the
        // way it widens `sum(int4) -> bigint`.
        assert_eq!(
            aggregate_result_type("SUM", ValueType::Float(Float4)),
            Some(ValueType::Float(Float4))
        );
        // ...but `avg(real)` *is* `double precision`.
        assert_eq!(
            aggregate_result_type("AVG", ValueType::Float(Float4)),
            Some(ValueType::Float(Float8))
        );
    }

    /// `real + real` is `real`, but a float mixed with *anything* else is
    /// `double precision`, because Postgres has no `float4 + int4` or
    /// `float4 + numeric` operator — it casts both sides to `float8`.
    #[test]
    fn float_operator_result_types_match_postgres() {
        use FloatWidth::{Float4, Float8};
        let f4 = ValueType::Float(Float4);
        let f8 = ValueType::Float(Float8);
        assert_eq!(
            operator_result_type(Operator::Add, f4, f4),
            Some(ValueType::Float(Float4))
        );
        assert_eq!(
            operator_result_type(Operator::Add, f8, f8),
            Some(ValueType::Float(Float8))
        );
        for other in [
            f8,
            ValueType::Numeric,
            ValueType::Integer(IntWidth::Int2),
            ValueType::Integer(IntWidth::Int4),
            ValueType::Integer(IntWidth::Int8),
        ] {
            assert_eq!(
                operator_result_type(Operator::Add, f4, other),
                Some(ValueType::Float(Float8)),
                "real + {other}"
            );
            assert_eq!(
                operator_result_type(Operator::Add, other, f4),
                Some(ValueType::Float(Float8)),
                "{other} + real"
            );
        }
        // `>` admits float > float and exact > exact, and refuses the mix.
        assert_eq!(
            operator_result_type(Operator::GreaterThan, f4, f8),
            Some(ValueType::Boolean)
        );
        for exact in [
            ValueType::Numeric,
            ValueType::Integer(IntWidth::Int2),
            ValueType::Integer(IntWidth::Int4),
            ValueType::Integer(IntWidth::Int8),
        ] {
            for f in [f4, f8] {
                assert_eq!(operator_result_type(Operator::GreaterThan, f, exact), None);
                assert_eq!(operator_result_type(Operator::GreaterThan, exact, f), None);
            }
            assert_eq!(
                operator_result_type(Operator::GreaterThan, exact, ValueType::Numeric),
                Some(ValueType::Boolean)
            );
        }
        // A float still has no `+` with a non-numeric type.
        for other in [ValueType::Text, ValueType::Boolean, ValueType::Uuid] {
            assert_eq!(operator_result_type(Operator::Add, f8, other), None);
            assert_eq!(operator_result_type(Operator::Add, other, f8), None);
        }
    }

    /// `date ± integer`, `integer + date` and `date - date`, as Postgres's
    /// `date_pli`, `date_mii` and `date_mi` type them; everything else with a
    /// date is refused.
    #[test]
    fn date_arithmetic_result_types_match_postgres() {
        const DATE: ValueType = ValueType::Other(PgType::Date);
        const TS: ValueType = ValueType::Other(PgType::Timestamp);
        let (i2, i4, i8) = (
            ValueType::Integer(IntWidth::Int2),
            ValueType::Integer(IntWidth::Int4),
            ValueType::Integer(IntWidth::Int8),
        );
        for n in [i2, i4] {
            assert_eq!(operator_result_type(Operator::Add, DATE, n), Some(DATE));
            assert_eq!(operator_result_type(Operator::Add, n, DATE), Some(DATE));
            assert_eq!(
                operator_result_type(Operator::Subtract, DATE, n),
                Some(DATE)
            );
            assert_eq!(operator_result_type(Operator::Subtract, n, DATE), None);
        }
        assert_eq!(
            operator_result_type(Operator::Subtract, DATE, DATE),
            Some(i4)
        );
        assert_eq!(operator_result_type(Operator::Add, DATE, DATE), None);
        for other in [
            i8,
            ValueType::Numeric,
            ValueType::Float(FloatWidth::Float8),
            TS,
            ValueType::Text,
        ] {
            for op in [Operator::Add, Operator::Subtract] {
                assert_eq!(
                    operator_result_type(op, DATE, other),
                    None,
                    "date {op:?} {other}"
                );
                assert_eq!(
                    operator_result_type(op, other, DATE),
                    None,
                    "{other} {op:?} date"
                );
            }
        }
        for op in [Operator::Multiply, Operator::Divide, Operator::Modulo] {
            assert_eq!(operator_result_type(op, DATE, i4), None);
        }
        // Timestamps have no arithmetic here: their difference is an interval.
        assert_eq!(operator_result_type(Operator::Subtract, TS, TS), None);
        assert_eq!(operator_result_type(Operator::Add, TS, i4), None);
    }

    /// `AT TIME ZONE` takes a `timestamptz` and a text zone, and gives the
    /// `timestamp`; no other operand pair.
    #[test]
    fn at_time_zone_types_a_timestamptz_as_a_timestamp() {
        const TS: ValueType = ValueType::Other(PgType::Timestamp);
        const TZ: ValueType = ValueType::Other(PgType::TimestampTz);
        assert_eq!(
            operator_result_type(Operator::AtTimeZone, TZ, ValueType::Text),
            Some(TS)
        );
        for lhs in [TS, ValueType::Other(PgType::Date), ValueType::Text] {
            assert_eq!(
                operator_result_type(Operator::AtTimeZone, lhs, ValueType::Text),
                None
            );
        }
        assert_eq!(
            operator_result_type(Operator::AtTimeZone, TZ, ValueType::Integer(IntWidth::Int4)),
            None
        );
    }

    /// `date` and `timestamp` have the fields `EXTRACT` is admitted for, and
    /// nothing else has any; `timestamp` has the clock fields besides.
    #[test]
    fn extract_fields_are_a_dates_and_a_timestamps() {
        let date = extract_fields(ValueType::Other(PgType::Date)).expect("date");
        let timestamp = extract_fields(ValueType::Other(PgType::Timestamp)).expect("timestamp");
        assert_eq!(date, EXTRACT_DATE_FIELDS);
        assert_eq!(timestamp.len(), date.len() + EXTRACT_CLOCK_FIELDS.len());
        for field in EXTRACT_CLOCK_FIELDS {
            assert!(
                !date.contains(field) && timestamp.contains(field),
                "{field}"
            );
        }
        for field in EXTRACT_REFUSED_FIELDS {
            assert!(
                !date.contains(field) && !timestamp.contains(field),
                "{field}"
            );
        }
        for ty in [
            ValueType::Other(PgType::TimestampTz),
            ValueType::Other(PgType::Time),
            ValueType::Other(PgType::Interval),
            ValueType::Integer(IntWidth::Int4),
            ValueType::Text,
        ] {
            assert_eq!(extract_fields(ty), None, "{ty}");
        }
    }

    /// `CAST(date AS timestamp)` is admitted; the reverse and the same-type
    /// casts are not, and `timestamp` is a target by name.
    #[test]
    fn a_date_casts_to_a_timestamp_and_nothing_else_does() {
        const DATE: ValueType = ValueType::Other(PgType::Date);
        const TS: ValueType = ValueType::Other(PgType::Timestamp);
        assert_eq!(cast_result_type(DATE, TS), Some(TS));
        assert_eq!(cast_result_type(TS, TS), None);
        assert_eq!(cast_result_type(TS, DATE), None);
        assert_eq!(
            cast_result_type(ValueType::Other(PgType::TimestampTz), TS),
            None
        );
        assert_eq!(cast_target("TIMESTAMP"), Some(TS));
        assert_eq!(cast_target("timestamptz"), None);
        assert_eq!(cast_target("date"), None);
    }

    /// The list is sorted, lower-case and without repeats, which
    /// `is_reserved_word`'s binary search needs.
    #[test]
    fn the_reserved_word_list_is_sorted_lower_case_and_unique() {
        assert!(
            RESERVED_WORDS.windows(2).all(|pair| pair[0] < pair[1]),
            "RESERVED_WORDS must be sorted with no repeats"
        );
        assert!(
            RESERVED_WORDS
                .iter()
                .all(|word| word.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
        );
        assert!(is_reserved_word("ORDER"));
        assert!(is_reserved_word("order"));
        assert!(!is_reserved_word("extract"));
        assert!(!is_reserved_word("orders"));
    }

    /// `RESERVED_WORDS` is exactly the words a live server's `pg_get_keywords()`
    /// reports as `R` or `T`, less the ones an older supported major lacks
    /// (PostgreSQL 15 has no `system_user`). When a newer Postgres reserves a
    /// word, this fails: update `RESERVED_WORDS` (and
    /// `RESERVED_WORDS_ABSENT_IN_15` if a major earlier than 16 lacks it), then
    /// rerun the checks ADR-0022 lists.
    #[tokio::test]
    async fn the_reserved_word_list_matches_the_servers_keywords() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let version: i32 = client
            .query_one("select current_setting('server_version_num')::int", &[])
            .await
            .expect("version")
            .get(0);
        let live: Vec<String> = client
            .query(
                "select word from pg_get_keywords() where catcode in ('R', 'T') order by 1",
                &[],
            )
            .await
            .expect("keywords")
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        let expected: Vec<&str> = RESERVED_WORDS
            .iter()
            .copied()
            .filter(|word| version >= 160_000 || !RESERVED_WORDS_ABSENT_IN_15.contains(word))
            .collect();
        let live: Vec<&str> = live.iter().map(String::as_str).collect();
        assert_eq!(
            live, expected,
            "the server (version_num {version}) reserves different words than \
             RESERVED_WORDS in defs/registry.rs: update the list to match pg_get_keywords() \
             (category R or T)"
        );
    }
}
