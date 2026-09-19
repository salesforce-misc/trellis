//! The function/operator registry ADR-0004 requires: the grammar's accepted
//! operators and functions must equal the evaluator's re-implemented set
//! (issue #24 consumes this list too), so it lives as a plain, reusable
//! table rather than being baked into the parser's control flow.
//!
//! Issue #64 adds the first four functions (`strpos`, `octet_length`,
//! `char_length`, `regexp_count`), each carrying its arity and argument/
//! return [`ValueType`]s so the parser, validator, and evaluator all read
//! the same source of truth rather than each hardcoding a function's shape.

use super::ast::{Operator, ValueType};

/// One operator the grammar accepts, and the AST node it parses to.
#[derive(Debug, Clone, Copy)]
pub struct OperatorSpec {
    pub symbol: &'static str,
    pub operator: Operator,
    pub arg_types: (ValueType, ValueType),
    pub return_type: ValueType,
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
    /// `=`, `<>`, `<`, `>`, `<=`, `>=` and similar comparisons.
    pub const COMPARISON: u8 = 10;
    /// Binary `+`, `-`.
    pub const ADDITIVE: u8 = 20;
    /// `*`, `/`, `%`. Unused until those operators land — see this module's
    /// doc comment: the gap exists precisely so they can be added without
    /// renumbering the levels around them.
    #[allow(dead_code)]
    pub const MULTIPLICATIVE: u8 = 30;
}

/// The full set of operators this grammar/evaluator pairing supports.
///
/// # Precedence table (issue #67)
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
        return_type: ValueType::Numeric,
        precedence: precedence::ADDITIVE,
    },
    OperatorSpec {
        symbol: ">",
        operator: Operator::GreaterThan,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        return_type: ValueType::Boolean,
        precedence: precedence::COMPARISON,
    },
];

/// Looks up an operator's spec by its own [`Operator`] variant (as opposed
/// to [`lookup_operator`], keyed by concrete syntax) — what
/// [`super::validate`]'s type-checker wants once the parser has already
/// resolved a symbol to an [`Operator`].
pub fn operator_spec(operator: Operator) -> &'static OperatorSpec {
    OPERATORS
        .iter()
        .find(|spec| spec.operator == operator)
        .expect("every Operator variant has a matching OperatorSpec in OPERATORS")
}

/// One function the grammar accepts, and the arity/types the parser and
/// validator check a call against (issue #64).
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
/// each re-implemented in [`super::eval`] with identical Postgres 15+
/// semantics (per ADR-0004: the grammar and evaluator function sets are one
/// list).
pub const FUNCTIONS: &[FunctionSpec] = &[
    FunctionSpec {
        name: "STRPOS",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "OCTET_LENGTH",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "CHAR_LENGTH",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "REGEXP_COUNT",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Numeric,
    },
];

/// Looks up a function by its canonical uppercased name.
pub fn lookup_function(name: &str) -> Option<&'static FunctionSpec> {
    FUNCTIONS.iter().find(|spec| spec.name == name)
}

/// Aggregate function names, called out specifically so a rejection can
/// explain that they need an aggregate key-space (`GROUP BY`), not that
/// they're simply unknown.
pub const AGGREGATE_FUNCTIONS: &[&str] = &["SUM", "COUNT", "AVG", "MIN", "MAX"];

/// The aggregate functions an [`super::ast::KeySpace::Aggregate`] definition
/// may call in a calculated field (issue #11's groundwork), plus `COUNT`
/// (issue #75). `SUM`/`MIN`/`MAX`/`AVG` are numeric-only and unary; `COUNT`
/// is arity-0 (`COUNT(*)` — row-counting, not `COUNT(<column>)`), which
/// [`super::parser`] special-cases: it accepts the literal `*` in place of an
/// argument list and hands this spec's empty `arg_types` an empty `args`
/// vec, rather than teaching this table's shape a non-expression argument
/// syntax.
pub const AGGREGATE_FUNCTION_SPECS: &[FunctionSpec] = &[
    FunctionSpec {
        name: "SUM",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
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
        name: "AVG",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "COUNT",
        arg_types: &[],
        return_type: ValueType::Numeric,
    },
];

/// Looks up an aggregate function by its canonical uppercased name — the
/// [`AGGREGATE_FUNCTION_SPECS`] counterpart to [`lookup_function`].
pub fn lookup_aggregate_function(name: &str) -> Option<&'static FunctionSpec> {
    AGGREGATE_FUNCTION_SPECS
        .iter()
        .find(|spec| spec.name == name)
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

/// Looks up an operator by its concrete-syntax symbol.
pub fn lookup_operator(symbol: &str) -> Option<Operator> {
    OPERATORS
        .iter()
        .find(|spec| spec.symbol == symbol)
        .map(|spec| spec.operator)
}
