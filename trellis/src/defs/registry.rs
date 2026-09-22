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
    /// Since issue #111 this is *not* the admissibility test: exact integers
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
        precedence: precedence::ADDITIVE,
    },
    OperatorSpec {
        symbol: ">",
        operator: Operator::GreaterThan,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
];

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
/// `None` if Postgres has no such operator — the **overload-resolution**
/// replacement for the old "compare both operands to [`OperatorSpec::arg_types`]
/// for exact equality" check (issue #111).
///
/// Exact equality was sufficient while every arithmetic/comparison operand
/// was the single [`ValueType::Numeric`] bucket. It stops being sufficient
/// the moment exact integers have their own type: Postgres has a *family* of
/// `+` operators (`int2pl`, `int4pl`, `int24pl`, `numeric_add`, …) plus an
/// implicit `int -> numeric` coercion, so `int4_col + 1` is `integer` while
/// `int4_col + numeric_col` is `numeric`. This function encodes exactly that
/// and nothing more — it is deliberately **not** a general coercion lattice
/// (`super::typed_literal` rejects building one for the same reason), just
/// the closed set of combinations the two registered operators admit:
///
/// | lhs | rhs | `+` | `>` |
/// |---|---|---|---|
/// | `Integer(a)` | `Integer(b)` | `Integer(wider(a,b))` | `Boolean` |
/// | `Integer(_)` | `Numeric` | `Numeric` | `Boolean` |
/// | `Numeric` | `Integer(_)` | `Numeric` | `Boolean` |
/// | `Numeric` | `Numeric` | `Numeric` | `Boolean` |
/// | `Float(a)` | `Float(b)` | `Float(wider(a,b))` | `Boolean` |
/// | `Float(_)` | `Integer(_)`/`Numeric` | `Float(Float8)` | `Boolean` |
/// | `Integer(_)`/`Numeric` | `Float(_)` | `Float(Float8)` | `Boolean` |
///
/// Any other combination is `None`: `Text`/`Boolean`/`Uuid`/`Other` have no
/// `+` or `>` in this grammar, exactly as before.
///
/// `Integer + Numeric -> Numeric` mirrors Postgres promoting the integer
/// operand through its implicit cast and then running `numeric_add`, which
/// is *unbounded* — so a mixed-type sum correctly cannot overflow, while an
/// all-integer one correctly can (see [`crate::integer::checked_add`]).
/// [`super::eval`]'s `apply_operator` dispatches on the same shape, and has
/// to: if the two disagreed, the target column's declared type and the value
/// written into it would disagree too.
///
/// The float rows (issue #112) look asymmetric and are not: Postgres has
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
    if !lhs.is_numeric_family() || !rhs.is_numeric_family() {
        return None;
    }
    Some(match op {
        Operator::Add => match (lhs, rhs) {
            (ValueType::Float(a), ValueType::Float(b)) => ValueType::Float(a.wider(b)),
            // A float mixed with anything else is `float8pl` on two
            // `double precision` operands.
            (ValueType::Float(_), _) | (_, ValueType::Float(_)) => {
                ValueType::Float(FloatWidth::Float8)
            }
            (ValueType::Integer(a), ValueType::Integer(b)) => ValueType::Integer(a.wider(b)),
            _ => ValueType::Numeric,
        },
        Operator::GreaterThan => ValueType::Boolean,
    })
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
///
/// All four return Postgres `integer` (`int4`), not `numeric` — checked
/// against `pg_proc.prorettype` for `strpos`/`octet_length`/`char_length`/
/// `regexp_count`. They were declared `Numeric` only because, before issue
/// #111, [`ValueType`] had no way to say "integer"; saying it now is what
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
];

/// Looks up a function by its canonical uppercased name.
pub fn lookup_function(name: &str) -> Option<&'static FunctionSpec> {
    FUNCTIONS.iter().find(|spec| spec.name == name)
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
];

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
    // Issue #120: `count(*)` and `count(<anything>)` are both `bigint` in
    // Postgres (`pg_typeof(count(*))`/`pg_typeof(count(col))`), never
    // `numeric` — this row used to declare `Numeric` only because, before
    // issue #111, `ValueType` had no way to say "integer". `arg_types` stays
    // `&[]`: the parser/validator special-case `COUNT`'s arity (0 for `*`,
    // 0 or 1 for a real argument) rather than reading it from here — see
    // `super::parser`'s and `super::validate::infer_expr`'s own `COUNT`
    // branches.
    FunctionSpec {
        name: "COUNT",
        arg_types: &[],
        return_type: ValueType::Integer(IntWidth::Int8),
    },
    // Issue #119: `bool_and`/`bool_or` are Postgres's row-wise `AND`/`OR`
    // folds — `true` iff every/any non-NULL value in the group is `true`,
    // `NULL` over an all-NULL or empty group, exactly like `SUM`/`MIN`/
    // `MAX`/`AVG`'s "aggregate of zero non-NULL values is NULL" rule.
    // Unlike those four, `Boolean` is declared directly rather than
    // `Numeric`: `boolean` is not in the numeric family
    // (`ValueType::is_numeric_family`) and there is no widening/mixed-type
    // story to model the way there is for integers/floats (Postgres has
    // exactly one `boolean`, no widths), so admissibility is a plain exact
    // match on the declared arg type (`validate`'s `is_aggregate &&
    // *expected == ValueType::Numeric` widen-check never triggers), and
    // [`aggregate_result_type`]'s own `Boolean` arm below is what supplies
    // the result type the same way it does for every other aggregate.
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
    // Issue #118: `bit_and`/`bit_or` are Postgres's per-bit-position `AND`/
    // `OR` folds over a group's bit strings — `NULL` over an all-NULL or
    // empty group, exactly like every other aggregate's "aggregate of zero
    // non-NULL values is NULL" rule. Unlike `bool_and`/`bool_or`, whose
    // argument is one fixed `ValueType::Boolean`, `bit_and`/`bit_or` accept
    // *two* distinct `ValueType::Other` families — `PgType::Bit` (Postgres's
    // own `bit_and(bit)`/`bit_or(bit)`) and `PgType::VarBit` (a `bit
    // varying` argument implicitly widens to `bit` before the call —
    // verified live: `pg_aggregate` has exactly one row per name,
    // `aggfnoid = bit_and(bit)`, and `explain (verbose) select bit_and(v)
    // from (values ('1010'::varbit)) t(v)` shows `bit_and('1010'::"bit")`).
    // `arg_types` below names `Other(PgType::Bit)` as a representative
    // placeholder only — `validate::infer_expr`'s admissibility check for
    // this pair asks `aggregate_result_type` directly (the same widen-check
    // shape a `Numeric`-declared aggregate argument already gets), rather
    // than comparing against this one fixed slot, since neither
    // `PgType::Bit` alone nor `PgType::VarBit` alone would be the whole
    // truth.
    //
    // `return_type` is deliberately `Other(PgType::VarBit)`, **not**
    // `Other(PgType::Bit)` even though `bit_and(bit)` is the function
    // Postgres genuinely calls — see `aggregate_result_type`'s own `BIT_AND`/
    // `BIT_OR` arm for why the result is always widened to the unconstrained
    // family rather than mirrored back to the argument's own one, unlike
    // every other aggregate in this table.
    FunctionSpec {
        name: "BIT_AND",
        arg_types: &[ValueType::Other(PgType::Bit)],
        return_type: ValueType::Other(PgType::VarBit),
    },
    FunctionSpec {
        name: "BIT_OR",
        arg_types: &[ValueType::Other(PgType::Bit)],
        return_type: ValueType::Other(PgType::VarBit),
    },
    // Issue #115: `jsonb_agg` is `STABLE` in Postgres (`pg_proc.provolatile`),
    // not because it is order-sensitive (`array_agg`/`string_agg` are
    // equally order-sensitive and are `IMMUTABLE`) but because it is
    // polymorphic and, for *some* argument types (`timestamptz`, `money`),
    // its row-to-`jsonb` conversion reads a session GUC — see
    // `crate::jsonb`'s module doc for the live `pg_proc`/`TimeZone`/
    // `lc_monetary` evidence. Pinning the argument to `jsonb` itself (rather
    // than accepting it polymorphically the way Postgres's own grammar
    // does) excludes that hazard outright: converting an already-`jsonb`
    // value is the identity, with no GUC read at all. This is the one
    // aggregate spec in this table whose argument type is a passthrough
    // `Other` family rather than `Numeric`/`Boolean` — the same shape
    // `BIT_AND`/`BIT_OR` already established, just with an exact (not
    // widened) argument/result pair, the same as `BOOL_AND`/`BOOL_OR`.
    FunctionSpec {
        name: "JSONB_AGG",
        arg_types: &[ValueType::Other(PgType::Jsonb)],
        return_type: ValueType::Other(PgType::Jsonb),
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
/// [`operator_result_type`]'s aggregate twin (issue #111), and for the same
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
/// * `min`/`max` return their argument's own type exactly.
///
/// Issue #112 adds the float rows, and they are *not* the integer rows'
/// shape — Postgres does not widen a float sum, because a binary float's
/// exponent range already covers what widening would buy:
///
/// * `sum(real)` returns **`real`** and `sum(double precision)` returns
///   `double precision` — each keeps its argument's type, unlike
///   `sum(int4) -> bigint`.
/// * `avg(real)` returns **`double precision`**, not `real` — Postgres
///   accumulates a `real` average in `float8`. `avg(double precision)` is
///   `double precision`.
/// * `min`/`max` keep their argument's type, same as every other family.
///
/// All five verified with `select pg_typeof(<agg>(x)) from (values
/// (1.5::real)) t(x)` against a live server, not read off the docs.
///
/// `COUNT` is deliberately absent and keeps the constant `Numeric` return
/// type its [`AGGREGATE_FUNCTION_SPECS`] row declares. It is arity-0
/// `COUNT(*)` row-counting in this grammar — type-agnostic by construction,
/// which is why `docs/type-support.md` omits it from every per-type
/// aggregate cell. Postgres types `count(*)` as `bigint`; aligning that is
/// orthogonal to this issue's exact-integer split (it would change a derived
/// column's type for definitions that reference no integer at all) and is
/// left to #120.
pub fn aggregate_result_type(name: &str, arg: ValueType) -> Option<ValueType> {
    // Issue #113: the temporal families are the first non-numeric arguments
    // any aggregate accepts. They are `ValueType::Other` passthrough types
    // rather than a first-class variant (see `crate::temporal`'s module doc
    // for why they never needed one), so they are dispatched before the
    // numeric family rather than inside it. Postgres's own `pg_aggregate`
    // rows are the rule here as everywhere: `min`/`max` keep their
    // argument's type, and `sum(interval)` is `interval`.
    //
    // The two refusals are the reason this routes through
    // `crate::temporal`'s predicates rather than listing families inline.
    // `min`/`max` over `interval` is refused because Postgres's own answer
    // is not a function of its input — `max` over `{'1 day', '24 hours'}`
    // returns different *text* for different scan orders, verified live, so
    // ADR-0013's byte-exact recompute cross-check could never settle it.
    // `sum` over the other five is refused because Postgres simply has no
    // such aggregate: there is no `sum(timestamp)`.
    //
    // Issue #114: `bytea` reaches this same `Other` arm and falls all the
    // way through to `None` for `MIN`/`MAX` too, but for a *third* reason
    // distinct from both temporal refusals above — not a scan-order defect,
    // not a missing-aggregate-for-this-name gap. `bytea` has a full btree
    // opclass (`ORDER BY`/`<`/`>` all work, and are IMMUTABLE), but Postgres
    // simply never wired a `min(bytea)`/`max(bytea)` aggregate to it: `select
    // min(v) from (values ('\x00'::bytea)) t(v)` is `ERROR: function
    // min(bytea) does not exist` on a live server, and `pg_proc` has no
    // `min`/`max` row whose sole argument type is `bytea`. ADR-0004 admits
    // only a subset of Postgres's own grammar, so a `MIN`/`MAX(bytea)`
    // definition has nothing to be a subset *of* — there is no server-side
    // aggregate this crate could even be asked to reproduce. `bytea`'s join,
    // primary-key and `GROUP BY` key roles do not depend on this at all
    // (`catalog::TEXT_STABLE_JOIN_KEY_TYPES`, `validate::
    // reject_unsupported_group_by_key_type`) — they only need equality,
    // which `bytea` has and which is a separate question from whether an
    // aggregate exists.
    if let ValueType::Other(pg_type) = arg {
        return match name {
            // Issue #116: `inet` joins the temporal `MIN`/`MAX` families —
            // `min(inet)`/`max(inet)` are real Postgres aggregates that keep
            // their argument's own type (`pg_typeof(min(v))` is `inet`,
            // verified live). `cidr`/`macaddr`/`macaddr8` all fall through to
            // `None` here, each for its own reason (see `crate::netaddr`'s
            // module doc): `macaddr`/`macaddr8` have no `min`/`max`
            // aggregate at all (the `bytea` finding, #114); `cidr`'s only
            // reachable `min`/`max` is Postgres's own implicit upcast to
            // `inet`, which would silently change the result's type away
            // from `cidr` — a type-changing aggregate no other family in
            // this epic has needed, and this issue declines to introduce
            // speculatively.
            // Issue #117: `min(enum)`/`max(enum)` are real Postgres
            // aggregates — `anyenum` has a full btree opclass
            // (`enum_cmp`/`enum_lt`/...), ordered by each value's creation
            // position (`pg_enum.enumsortorder`), not alphabetically — and
            // `pg_typeof(min(v))` keeps the argument's own concrete enum
            // type, the same "own family, own terms" shape `inet`'s MIN/MAX
            // landed under (#116). Mirroring `arg` back unchanged is
            // therefore correct here exactly as it is for every other
            // family in this match. See `defs::eval::reduce_enum_aggregate`
            // for the one real wrinkle this family has that none of the
            // others do: creation-order isn't a fixed, universal fact about
            // the *family* the way date/inet ordering is, it's a per-type,
            // schema-defined, live-mutable fact — so the pure evaluator
            // fold this arm's admission enables cannot always answer it
            // without a live connection, and refuses rather than guess (a
            // naive lexicographic fallback here would be a silent,
            // wrong-order bug this variant exists to rule out).
            "MIN" | "MAX" if matches!(pg_type, PgType::Enum(_)) => Some(arg),
            "MIN" | "MAX"
                if crate::temporal::supports_min_max(pg_type)
                    || crate::netaddr::supports_min_max(pg_type) =>
            {
                Some(arg)
            }
            "SUM" if pg_type == PgType::Interval => Some(arg),
            // Issue #118: `bit_and`/`bit_or` accept either bit-string
            // family (`PgType::Bit` or `PgType::VarBit` — see
            // `AGGREGATE_FUNCTION_SPECS`'s own `BIT_AND`/`BIT_OR` doc
            // comment for the live `pg_aggregate`/`pg_cast` evidence that
            // Postgres only ever actually calls `bit_and(bit)`/`bit_or(bit)`
            // regardless of which family the argument started as).
            //
            // The result is always declared `Other(PgType::VarBit)` here,
            // deliberately **not** mirrored back to `arg`'s own family the
            // way every other arm in this function (`MIN`/`MAX`,
            // `SUM(interval)`) mirrors its argument type — this is the one
            // place in the registry that widens rather than preserves.
            // `ValueType`/`PgType` carry no length modifier, and
            // Postgres's *default* typmod for a **fixed-length** `bit`
            // column declared with no length is `bit(1)`, not
            // "unconstrained": a bare `pg_type_name(Other(PgType::Bit))`
            // target column (what `ddl::create_aggregate_target_table`/
            // `staging::apply_aggregate` would otherwise declare and cast
            // through) would only ever be able to hold a *1-bit* result,
            // failing on the very first apply for any wider group — verified
            // live (`create table t(x bit); insert into t values ('101')`
            // raises `bit string length 3 does not match type bit(1)`).
            // `bit varying` has no such trap: its bare default is genuinely
            // unconstrained, losslessly holding any width (verified live:
            // `'10100101'::bit varying` keeps all 8 bits) and rendering
            // identically to `bit`'s own `bit_out` (`bit_out`/`varbit_out`
            // both emit the bare `'0'`/`'1'` characters, no type-identifying
            // wrapper), so retargeting the *declared* result family costs
            // nothing observable downstream while sidestepping the DDL trap
            // entirely — the same reasoning
            // `validate::reject_unsupported_group_by_key_type`'s `VarBit`
            // arm documents for the `GROUP BY` key role. There is no
            // equivalent per-column-concrete-type escape hatch for an
            // aggregate's *result* column the way there is for a
            // passthrough or a primary key (issue #45's mechanism narrows a
            // bare source-column reference, not a computed aggregate's
            // output) — extending one is real, deferred scope, not
            // attempted here.
            "BIT_AND" | "BIT_OR" if matches!(pg_type, PgType::Bit | PgType::VarBit) => {
                Some(ValueType::Other(PgType::VarBit))
            }
            // Issue #115: unlike `BIT_AND`/`BIT_OR`'s deliberate widening,
            // `JSONB_AGG` mirrors its argument's own family straight back —
            // there is no DDL-typmod trap here the way fixed-length `bit`
            // has (`PgType::Jsonb` carries no modifier at all), so nothing
            // forces a different declared family the way it does there.
            "JSONB_AGG" if pg_type == PgType::Jsonb => Some(ValueType::Other(PgType::Jsonb)),
            _ => None,
        };
    }
    // Issue #119: `bool_and`/`bool_or` are the only aggregates whose
    // argument is `ValueType::Boolean`, which — like the `Other` temporal/
    // bytea families above — is not in the numeric family, so it is routed
    // before the numeric-family gate below rather than falling into it and
    // returning `None` for every boolean argument.
    if let ValueType::Boolean = arg {
        return match name {
            "BOOL_AND" | "BOOL_OR" => Some(ValueType::Boolean),
            _ => None,
        };
    }
    // Issue #120: `text`/`varchar` is a first-class (non-`Other`) `ValueType`
    // with a real Postgres btree ordering that `MIN`/`MAX` hadn't yet been
    // generalized to — `min(text)`/`max(text)` are real Postgres aggregates
    // that keep the argument's own type (`pg_typeof(min(v))` is `text`,
    // mirrored back exactly like every other family in this function).
    // `SUM`/`AVG` still fall through to `None` below — Postgres has no
    // `sum(text)` — and `text` is not in the numeric family, so this is
    // routed here rather than into the numeric gate.
    //
    // `uuid` was checked live alongside `text` (per #111-#119's playbook —
    // verify, don't assume from "has a full btree opclass") and turns out
    // **not** to belong in this arm at all: `select min(v) from (values
    // ('...'::uuid)) t(v)` is `ERROR: function min(uuid) does not exist` on
    // a live Postgres 17, and no `pg_proc`/`pg_aggregate` row names `min`/
    // `max` over a lone `uuid` argument — despite `uuid_ops` being a
    // complete, `IMMUTABLE` btree opclass (`ORDER BY`/`<`/`>` all work).
    // This is exactly `bytea`'s finding (#114) and `macaddr`/`macaddr8`'s
    // (#116): an opclass with no aggregate wired to it is not a
    // rendering/ordering hazard this crate could work around — there is no
    // server-side construct for `MIN`/`MAX(uuid)` to be a subset *of*, so it
    // falls through to `None` below (`uuid.is_numeric_family()` is `false`,
    // same as `Text`, and `uuid` gets no early-return arm of its own the way
    // `Boolean`/`Text` do — the `_ => None` path several lines down is what
    // actually returns `None` for it). `trellis/tests/defs_min_max_text_and_uuid.rs`
    // pins this live.
    //
    // Whether the *pure evaluator* (`defs::eval::reduce_numeric_aggregate`)
    // could honor `text`'s ordering with no live connection is a separate
    // question this admission alone does not answer — see
    // `reduce_text_aggregate`'s own doc comment. A `KeySpace::Aggregate`
    // field's own `MIN`/`MAX` never depends on the evaluator's fold at all:
    // `MIN`/`MAX` are unconditionally `Invertibility::RecomputeOnly`, so
    // `staging::apply_aggregate` always asks Postgres directly.
    if let ValueType::Text = arg {
        return match name {
            "MIN" | "MAX" => Some(arg),
            _ => None,
        };
    }
    if !arg.is_numeric_family() {
        return None;
    }
    Some(match (name, arg) {
        ("SUM", ValueType::Integer(IntWidth::Int2 | IntWidth::Int4)) => {
            ValueType::Integer(IntWidth::Int8)
        }
        ("SUM", ValueType::Integer(IntWidth::Int8) | ValueType::Numeric) => ValueType::Numeric,
        // Issue #112: a float sum keeps its argument's width.
        ("SUM", ValueType::Float(width)) => ValueType::Float(width),
        ("AVG", ValueType::Float(_)) => ValueType::Float(FloatWidth::Float8),
        ("AVG", _) => ValueType::Numeric,
        ("MIN" | "MAX", arg) => arg,
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

/// Looks up an operator by its concrete-syntax symbol.
pub fn lookup_operator(symbol: &str) -> Option<Operator> {
    OPERATORS
        .iter()
        .find(|spec| spec.symbol == symbol)
        .map(|spec| spec.operator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every aggregate maps a float argument to a float result and an exact
    /// argument to an exact one.
    ///
    /// This is the invariant `defs::backfill::classify_field` and
    /// `staging::apply_aggregate::classify_fields` rely on: both hold the
    /// field's *result* type (what the validator inferred) but need to ask
    /// `invertibility::classify` a question about the *argument*. Since the
    /// only distinction that gate draws inside the numeric family is
    /// float-vs-exact, the two are interchangeable — as long as this holds.
    /// Pinned here rather than commented, so a future aggregate that breaks
    /// it (e.g. one returning `numeric` for a float argument) fails loudly
    /// instead of silently putting a float `SUM` back on the delta path.
    #[test]
    fn aggregate_results_stay_in_their_arguments_family() {
        let args = IntWidth::ALL
            .into_iter()
            .map(ValueType::Integer)
            .chain(std::iter::once(ValueType::Numeric))
            .chain(FloatWidth::ALL.into_iter().map(ValueType::Float));
        for arg in args {
            for name in ["SUM", "AVG", "MIN", "MAX"] {
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
        assert_eq!(
            aggregate_result_type("MIN", ValueType::Float(Float4)),
            Some(ValueType::Float(Float4))
        );
        assert_eq!(
            aggregate_result_type("MAX", ValueType::Float(Float8)),
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
            assert_eq!(
                operator_result_type(Operator::GreaterThan, f4, other),
                Some(ValueType::Boolean)
            );
        }
        // A float still has no `+` with a non-numeric type.
        for other in [ValueType::Text, ValueType::Boolean, ValueType::Uuid] {
            assert_eq!(operator_result_type(Operator::Add, f8, other), None);
            assert_eq!(operator_result_type(Operator::Add, other, f8), None);
        }
    }
}
