//! Validator for the 1-1 transform-definition subset.
//!
//! Operates on the parsed [`TransformDef`] AST, independent of whether it
//! came from [`super::parse`] or was hand-built (e.g. in tests), per the
//! issue's requirement that the validator itself — not just the parser —
//! guards against invalid definitions. Two of the ticket's rejection
//! categories (aggregate/cross-join key-spaces, general predicates) are
//! actually unrepresentable in the AST today: [`super::ast::KeySpace`] and
//! [`super::ast::Predicate`] are closed enums with only the 1-1/`TRUE`
//! variant, so the exhaustive matches below are the "guard" — adding a
//! variant to either enum without updating this module fails to compile.
//! What genuinely needs runtime validation, because the AST *can* encode
//! it, is column resolution, cycle detection, and type-checking.

use std::collections::{HashMap, HashSet};
use std::fmt;

use super::ast::{
    Expr, FieldDef, JoinOn, JoinSpec, KeySpace, Predicate, QualifiedColumn, TransformDef,
    ValueType, render_field_expr,
};
use super::join::Side;
use super::parser::MAX_EXPRESSION_DEPTH;
use super::pg_type::PgType;
use crate::error_code::ErrorCode;
use crate::integer::IntWidth;

/// Which key a [`ValidationError::NondeterministicKeyCollation`] column
/// belongs to. Every one of them is matched by the engine as
/// exact text: apply keys changes and target rows by the source's
/// row-identity key, and an aggregate's group key is its `GROUP BY` values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyColumnRole {
    /// Part of the definition's source's row-identity key (its primary key,
    /// or the unique index `ddl::source_primary_key` falls back to).
    SourceKey,
    /// A `GROUP BY` key.
    GroupBy,
}

impl fmt::Display for KeyColumnRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyColumnRole::SourceKey => write!(f, "part of the source's row-identity key"),
            KeyColumnRole::GroupBy => write!(f, "a GROUP BY key"),
        }
    }
}

/// Why a `TransformDef` was rejected by the validator.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ValidationError {
    /// Two calculated fields on the same target share a name.
    DuplicateFieldName { name: String },
    /// A field's name starts with `RESERVED_COLUMN_PREFIX`, which is kept
    /// for the hidden columns Trellis adds to a target
    /// (`super::ddl::MEMBERS_COLUMN`, an `AVG`'s `__{field}_sum`, a
    /// `SUM`'s `__{field}_count`). Without the reservation such a name could
    /// equal one of them, and install failed with a raw Postgres
    /// duplicate-column error.
    ReservedFieldName { field: String },
    /// A `GROUP BY` key's target column name starts with
    /// `RESERVED_COLUMN_PREFIX` (see [`ValidationError::ReservedFieldName`]).
    /// `key` is the key as written, `column` the target column it would
    /// become.
    ReservedGroupByColumnName { key: String, column: String },
    /// A 1-1 field is named after one of the source's primary key columns,
    /// whether it passes that column through (`id AS id`) or
    /// computes something else under its name. The target already carries
    /// every key column as its own key, so the field would declare the column
    /// a second time. Raised by `reject_primary_key_named_fields`, which the
    /// catalog runs once it knows the source's key.
    FieldNamedAfterPrimaryKey { field: String },
    /// A field's expression references a name that is neither a known
    /// source column nor another calculated field on the same target.
    UnresolvedColumn { field: String, column: String },
    /// The calculated-column dependency graph contains a cycle.
    Cycle { cycle: Vec<String> },
    /// Inlining the calculated fields each field reads into it, as every
    /// build does, yields more expression nodes than a build can render: a
    /// chain of fields that each read the previous one more than once.
    FieldExpansionTooLarge,
    /// Adding a table-to-table dependency edge from a definition's source to
    /// its target would create a cycle in the two-level dependency graph
    /// (cycle detection runs over the whole graph of tables connected
    /// by `super::model::SchemaEdge`s — currently just `Source` edges, but
    /// checked generically over whatever edge kinds exist). Distinct
    /// from [`ValidationError::Cycle`], which names calculated *columns*
    /// within a single definition, since this names *tables* across
    /// definitions and is only ever detected once the persisted graph is
    /// consulted — see `super::catalog`.
    TableCycle { cycle: Vec<String> },
    /// The target table is the same as the source table. Calculated columns
    /// must live on a separate neighbor table — writing them back onto the
    /// source would feed our own writes back into capture (see
    /// `docs/data-flow.md`).
    TargetEqualsSource { table: String },
    /// `validate` was handed a `JOIN` definition. A `JOIN` is checked
    /// against both tables' columns, by `validate_join`; this entry point
    /// has only the `FROM` table's.
    JoinNeedsBothTables,
    /// The joined table is the `FROM` table. A self-join isn't supported,
    /// even under two aliases: one captured change would feed both sides.
    JoinSelf { table: String },
    /// An aggregate `JOIN` joins more than one table to its `FROM` table:
    /// it joins exactly two.
    JoinTooManyTables,
    /// A `JOIN` definition's column reference names a table that is neither
    /// the `FROM` table nor the joined one.
    JoinUnknownTable { reference: String },
    /// A column in a `JOIN` definition's field list isn't qualified with its
    /// table; with two tables in play every column must say which it is.
    JoinUnqualifiedColumn { field: String, column: String },
    /// A qualified column in a `JOIN` definition names no column of its table.
    JoinUnresolvedColumn { reference: String },
    /// An `ON` term compares two columns of the same table, or a column with
    /// itself. An equi-join term has one column from each side.
    JoinOnSameSide { term: String },
    /// An aggregate's argument reads columns of both tables
    /// (`SUM(a.x + b.y)`). Each argument becomes a partial on one side.
    JoinMixedSideArgument { field: String, argument: String },
    /// An `ON` term pairs the same two columns as an earlier one (in either
    /// order).
    JoinOnRepeatedPair { term: String },
    /// An `ON` term names a column an earlier term already names, paired with
    /// a different column of the other table. The join key would list that
    /// column twice, which the hidden tables cannot store.
    JoinOnRepeatedColumn { term: String, column: String },
    /// A key Trellis builds an index over spans more columns than Postgres
    /// allows in one (`INDEX_MAX_KEYS`, 32): the target's or a hidden table's
    /// unique key over a `GROUP BY`, a `JOIN`'s `ON` columns, or a `JOIN`
    /// side's `ON` and `GROUP BY` columns together. `clause` says which, as
    /// the subject of the message (`GROUP BY`, `each side of ON`, or a
    /// side's key); `columns` is that key's width.
    IndexKeyTooWide {
        clause: String,
        columns: usize,
        limit: usize,
    },
    /// A table Trellis creates for a definition would hold more columns than
    /// Postgres allows in one (`TABLE_MAX_COLUMNS`, 1600): the target, or a
    /// hidden table that carries columns per field (an aggregate's partials,
    /// a `JOIN`'s partials or ledgers). `table` names which, as the subject of
    /// the message; `columns` is the width it would have, counting `dropped`
    /// columns Postgres still counts (an `ALTER TRANSFORM` that dropped fields
    /// from a target leaves them behind).
    TableTooWide {
        table: String,
        columns: usize,
        limit: usize,
        dropped: usize,
    },
    /// The two columns of an `ON` term differ in type or collation (a type
    /// modifier never counts, and `text` and `varchar` are one type).
    /// Trellis never casts a join key.
    JoinKeyTypeMismatch {
        lhs: String,
        lhs_type: String,
        rhs: String,
        rhs_type: String,
    },
    /// A join column has a type that isn't text-stable (the join-key rule,
    /// `super::join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`).
    JoinUnsupportedKeyType { reference: String, pg_type: String },
    /// A join column has a nondeterministic collation, whose `=` Trellis
    /// can't reproduce.
    JoinNondeterministicCollation {
        reference: String,
        collation: String,
    },
    /// A `JOIN` definition's field is neither a `GROUP BY` key passthrough
    /// nor an admitted aggregate over a single column.
    JoinFieldShape { field: String, detail: String },
    /// A 1-1 `JOIN` field calls an aggregate. A 1-1 `JOIN` has one target row
    /// per `FROM` row and no `GROUP BY` to aggregate over.
    OneToOneJoinAggregate { field: String, function: String },
    /// The joined table's `ON` columns are not unique, so one `FROM` row could
    /// match several rows of it (ADR-0021).
    OneToOneJoinKeyNotUnique { table: String, columns: Vec<String> },
    /// A construct a 1-1 `JOIN` doesn't take, with the remedy.
    OneToOneJoinUnsupported { what: String, remedy: String },
    /// A 1-1 `JOIN` of more tables than a star admits: `tables` counts the
    /// `FROM` table and every joined one (#1459's cap).
    StarJoinTooManyTables { tables: usize },
    /// An `ON` term of the table joined as `table` reads `other`, another
    /// joined table: each `ON` reads only the `FROM` table and the table it
    /// joins (a star, not a snowflake).
    JoinOnReadsAnotherJoinedTable {
        table: String,
        term: String,
        other: String,
    },
    /// An aggregate in a deferred family (`MIN`/`MAX` in a `JOIN`,
    /// `bool_*`/`bit_*`, float or `interval` `SUM`/`AVG`,
    /// `array_agg`/`string_agg`/`jsonb_agg`). Only the aggregates the group
    /// basis keeps exact are admitted.
    DeferredAggregate {
        function: String,
        family: &'static str,
    },
    /// `SUM`/`AVG` over a column whose type is not an integer or a
    /// `numeric`.
    UnsupportedAggregateArgument {
        function: String,
        column: String,
        pg_type: String,
    },
    /// `MIN`/`MAX` over an argument whose type has no btree order or has a
    /// collation (`registry::extreme_type_admitted`): the group basis keeps
    /// an extreme only of a number, date or time type.
    UnsupportedExtremeArgument {
        function: String,
        argument: String,
        pg_type: String,
    },
    /// `MIN`/`MAX` over an expression rather than a plain source column: the
    /// probe that re-reads a retracted extreme reads an index on the column
    /// (#1117 rule 5).
    ExtremeOfExpression { function: String, argument: String },
    /// `MIN`/`MAX` over a column of a table Trellis creates (another
    /// transform's target), which has no index for the probe to read
    /// (#1117 rule 6).
    ChainedExtreme { function: String, column: String },
    /// A `MIN`/`MAX` definition declared `WITHOUT SOURCE INDEX`: the opt-out
    /// doesn't apply to `MIN`/`MAX`, whose probe needs an index on the
    /// `GROUP BY` columns then each argument (#1681). `indexes` are those
    /// indexes, on the source as the definition names it (qualified once
    /// define resolves it).
    ExtremesWithoutSourceIndex {
        target: String,
        indexes: Vec<super::source_index::SourceIndex>,
    },
    /// The target's name ends with `suffix`, one of
    /// `super::ledger::RESERVED_TARGET_SUFFIXES`, which name the tables
    /// Trellis keeps beside a target (a 1-1 target's ledger): this
    /// target would be one of another target's.
    ReservedTargetSuffix {
        target: String,
        suffix: &'static str,
    },
    /// The target's name is longer than `super::ledger::MAX_TARGET_NAME_LEN`
    /// bytes, so its ledger's name (the target's plus
    /// `super::ledger::LEDGER_SUFFIX`) would pass Postgres's 63-byte
    /// identifier limit and be silently truncated.
    TargetNameTooLong { target: String, max: usize },
    /// An operator was applied to an operand of the wrong `ValueType` —
    /// e.g. `+` given a `Text` operand (`+` stays Numeric-only,
    /// no implicit string concatenation). `operator` is its symbol.
    TypeMismatch {
        field: String,
        operator: &'static str,
        expected: ValueType,
        found: ValueType,
    },
    /// A comparison (or `NULLIF`) was given a float on one side and an exact
    /// numeric (an integer of any width, or `numeric`) on the other.
    /// Postgres casts the exact side to `double precision`, so the comparison
    /// inherits float rounding; Trellis refuses the pair rather than admit a
    /// comparison that doesn't compose. `operator` is its symbol.
    MixedFloatExactComparison {
        field: String,
        operator: &'static str,
        lhs: ValueType,
        rhs: ValueType,
    },
    /// A comparison between two different temporal types (`date` and
    /// `timestamp`): Postgres converts one side, and for `timestamptz` the
    /// conversion depends on the session's time zone.
    MixedTemporalComparison {
        field: String,
        operator: &'static str,
        lhs: ValueType,
        rhs: ValueType,
    },
    /// `<`, `<=`, `>` or `>=` over `text`, whose order depends on the
    /// collation.
    TextOrderingComparison {
        field: String,
        operator: &'static str,
    },
    /// A comparison (or `NULLIF`) between two types no comparison family
    /// covers.
    ComparisonTypeMismatch {
        field: String,
        operator: &'static str,
        lhs: ValueType,
        rhs: ValueType,
    },
    /// `NULLIF(a, b)` where Postgres casts `a` to compare it (an integer
    /// beside a `numeric`, an exact numeric beside a float), so the result
    /// has that cast's type rather than `a`'s.
    NullIfFirstArgumentCast {
        field: String,
        lhs: ValueType,
        rhs: ValueType,
        result: ValueType,
    },
    /// A `CASE` condition that is not a `boolean`.
    CaseConditionNotBoolean { field: String, found: ValueType },
    /// The results of a `CASE` (its `THEN`s and `ELSE`) have types Postgres
    /// cannot unify the way `COALESCE` does.
    CaseResultTypeMismatch {
        field: String,
        expected: ValueType,
        found: ValueType,
    },
    /// A `NULL` literal (or a field that is only one) where its context
    /// gives it no type: `NULL + NULL`, `-NULL`, a function or aggregate
    /// argument, an operand whose other side's type has no operator of that
    /// type on both sides. Postgres either refuses these as ambiguous or
    /// resolves them by its function-candidate rules, which this grammar
    /// does not model. `context` names the construct.
    UntypedNull { field: String, context: String },
    /// A `/` or `%` whose divisor is written as a literal zero (`a / 0`,
    /// `a / 0.0`, `a % -0`, `a / REAL '0'`, through `CAST` or prefix `-`),
    /// wherever it sits. Postgres raises `22012` for every row that reads a
    /// non-null dividend. Only the text is read: a divisor that computes to
    /// zero (`a / (1 - 1)`) defines and fails at run time. `text` is the
    /// operation as written.
    ConstantDivisorZero { field: String, text: String },
    /// A `/` between two integers. Postgres truncates (`7 / 2` is `3`).
    IntegerDivision {
        field: String,
        lhs: ValueType,
        rhs: ValueType,
    },
    /// A `%` with a float operand: Postgres has no float modulo.
    FloatModulo { field: String },
    /// A date or time construct (`+`/`-` on a temporal operand, `AT TIME
    /// ZONE`, `DATE_TRUNC`, `EXTRACT`, or a `CAST` to `timestamp`) over an
    /// operand type it does not admit. `construct` names it and `reason` says
    /// why and gives the remedy.
    TemporalOperand {
        field: String,
        construct: String,
        found: ValueType,
        reason: &'static str,
    },
    /// A literal a date construct needs to know at define time that is not
    /// one it admits: the unit of `DATE_TRUNC`, the field of `EXTRACT`, or the
    /// zone of `AT TIME ZONE`. `text` is what was written (`None` when it is
    /// not a literal) and `reason` says what is admitted.
    TemporalLiteral {
        field: String,
        construct: &'static str,
        text: Option<String>,
        reason: String,
    },
    /// A `CAST` the grammar does not admit: only an integer to the same or a
    /// wider integer, any integer or `numeric` to `numeric`, or a `date` or
    /// `timestamp` to `timestamp` converts exactly and without reading a
    /// setting.
    UnsupportedCast {
        field: String,
        from: ValueType,
        to: ValueType,
    },
    /// An argument of a numeric function (`ABS`, `CEIL`, `FLOOR`, `ROUND`,
    /// `TRUNC`) whose type the function does not admit, with the reason.
    NumericFunctionArgument {
        field: String,
        function: String,
        found: ValueType,
        reason: &'static str,
    },
    /// The scale of `ROUND(a, n)` / `TRUNC(a, n)` is not an integer literal.
    ScaleNotIntegerLiteral { field: String, function: String },
    /// A function call's argument had the wrong `ValueType` for its
    /// position — e.g. `octet_length(1)`. Distinct from
    /// [`ValidationError::TypeMismatch`] so the message can name the
    /// function and argument position; arity itself is a parser-time
    /// [`super::error::ParseError::FunctionArityMismatch`], since it needs
    /// no type information.
    FunctionArgTypeMismatch {
        field: String,
        function: String,
        arg_index: usize,
        expected: ValueType,
        found: ValueType,
    },
    /// `regexp_count`'s pattern argument was not a string literal. The pattern must be
    /// known at define time so Postgres can judge it once (`CatalogError::InvalidRegexPattern`)
    /// — a column-sourced pattern is out of scope.
    NonLiteralRegexPattern { field: String },
    /// A typed literal's text isn't in its family's **canonical**
    /// Postgres output spelling — `DATE '2024-1-5'`, `DATE 'today'`,
    /// `BYTEA '\xAB'`. See `super::typed_literal`'s module doc comment for
    /// why the bar is canonical form rather than "Postgres would parse it":
    /// in short, immutability (`date_in` is `STABLE` precisely because of
    /// `'today'`) and a validity check that needs no Postgres.
    InvalidTypedLiteral {
        field: String,
        value_type: ValueType,
        text: String,
        detail: String,
    },
    /// A hand-built `super::ast::Expr::TypedLiteral` names a `ValueType`
    /// outside `super::typed_literal::TYPED_LITERALS`'s allowlist. The
    /// parser can't produce one — it only builds the node from that same
    /// table — so this is the validator's defense-in-depth against an AST
    /// assembled directly, in the same spirit as
    /// the cycle check.
    UnsupportedLiteralType {
        field: String,
        value_type: ValueType,
    },
    /// An `super::ast::KeySpace::Aggregate` definition's `GROUP BY` names a
    /// column that isn't a real source column.
    UnresolvedGroupByColumn { column: String },
    /// An `super::ast::KeySpace::Aggregate` definition's `GROUP BY` names a
    /// column typed `ValueType::Other` — a Postgres
    /// family the OID registry recognizes but that `docs/type-support.md`
    /// still lists as passthrough-only, with no key role.
    ///
    /// This is the `GROUP BY` twin of the check the two *other* key roles
    /// already make on the raw pg type name
    /// (`super::join_key_types::is_text_stable_join_key_type`, gating `JOIN`
    /// keys and 1-1 primary keys). A `GROUP BY` key is
    /// matched by its `::text` rendering exactly like those are, so the same
    /// hazards apply and then some: `interval`'s native `=` holds
    /// `'1 day' = '24 hours'` while their renderings differ (two groups
    /// where Postgres's own `GROUP BY` has one), `money` renders under
    /// `lc_monetary`, and `json` has no `=` at all, so its key column can't
    /// even take the unique index the aggregate target needs. `bytea`
    /// used to be another name on this list; it turned out not
    /// to belong there — `byteaout` under the pinned `bytea_output = 'hex'`
    /// is a bijection with no session-GUC dependence at all, so it is
    /// admitted below alongside `oid`, and `timestamptz` is a
    /// second former name here now admitted the same way, once `TimeZone`
    /// was pinned identically on every connection Trellis opens rather than
    /// varying by session.
    ///
    /// The typed key index (docs/glossary.md) is what lifts it per family.
    UnsupportedGroupByKeyType {
        column: String,
        value_type: ValueType,
    },
    /// Two `GROUP BY` keys map to the same target column name:
    /// one key listed twice, or a `JOIN`'s `a.name` and `b.name`. A key's
    /// target column is named after its bare column, and the grammar has no
    /// `GROUP BY ... AS` alias yet, so the target DDL would declare that
    /// column twice. `first`/`second` are the clashing keys as written, in
    /// `GROUP BY` order.
    GroupByKeysShareTargetColumn {
        first: String,
        second: String,
        column: String,
    },
    /// An `super::ast::KeySpace::Aggregate` definition's field references a
    /// source column that is neither a grouping key nor wrapped in exactly
    /// one of `SUM`/`MIN`/`MAX`/`AVG` — every row in a group must be folded
    /// down to one value before it can appear in the target, and a bare
    /// reference to a non-grouping-key source column doesn't do that.
    UngroupedColumnReference { field: String, column: String },
    /// A field's aggregate call takes, as its argument, the name of another
    /// field that is itself an aggregate (`SUM(x) AS total, MAX(total)`), and
    /// no source column has that name. A name in an aggregate's argument
    /// resolves to a source column, never to an aggregate field, so there is
    /// nothing for it to read; reading the field would nest one aggregate in
    /// another.
    AggregateOfAggregateField { field: String, reference: String },
    /// A field reads a bare name that is both a source column and another
    /// field whose expression is not just that column, outside
    /// the one place that reads the source column: an aggregate field's name inside another
    /// aggregate's argument, which reads the source column. A qualified
    /// `<table>.<column>` reads the source column; renaming the field is how
    /// to read the field.
    AmbiguousColumnReference(Box<AmbiguousColumnReference>),
    /// A qualified `<table>.<column>` or `<schema>.<table>.<column>`
    /// reference names a table other than this definition's `FROM` table.
    /// `source` is the `FROM` table as the reference would
    /// have to spell it.
    QualifiedColumnNotFromSource {
        field: String,
        reference: String,
        source: String,
    },
    /// An `super::ast::KeySpace::Aggregate` definition has a calculated
    /// field whose name matches one of its grouping columns, but whose
    /// expression isn't a passthrough of that same column (e.g.
    /// `GROUP BY order_id SELECT SUM(order_id) AS order_id`). DDL generation
    /// treats a grouping-column-named field as that column's passthrough and
    /// gives it no separate target column, so a different expression under
    /// that name would compute a value with nowhere to go — silently
    /// dropped by DDL while eval still computed it. Rejecting this here
    /// keeps that assumption enforced at validation, not a silent DDL-time
    /// skip.
    GroupingColumnFieldMustBePassthrough { field: String },
    /// A `KeySpace::OneToOne` definition has a calculated field whose alias
    /// matches the name of a real source column, but whose expression isn't a
    /// passthrough of that same column (bare, or qualified with the `FROM`
    /// table). Without this guard, a *different*
    /// field referencing that name would resolve it via `fields_by_name`
    /// (inter-field composition) and get the calculated value
    /// instead of the real column, with the alias unconditionally — and
    /// silently — shadowing the column. Mirrors
    /// [`ValidationError::GroupingColumnFieldMustBePassthrough`]'s guard for
    /// the `Aggregate` arm.
    CalculatedFieldShadowsSourceColumn { field: String },
    /// A column the engine matches as a key by its exact text has a
    /// nondeterministic collation: the source's row-identity key
    /// or a `GROUP BY` key. Such a collation's `=` (and so `GROUP BY` and a
    /// unique index) treats distinct strings as equal (`'A'` and `'a'` under a
    /// case-insensitive one), so Postgres would fold rows the engine keeps
    /// apart. The key-column twin of
    /// [`ValidationError::JoinNondeterministicCollation`], refused at define
    /// time for the same reason (reject, never reinterpret).
    NondeterministicKeyCollation(Box<NondeterministicKeyCollation>),
    /// A calculated field passes a column with a nondeterministic collation
    /// to `STRPOS` or `REGEXP_COUNT`. Postgres
    /// refuses both under such a collation (`0A000`: no substring search, no
    /// regular expressions), while the engine would compute a value from the
    /// exact text, so the engine and Postgres would disagree. Refused at
    /// define time, as for join and key columns.
    NondeterministicCollationTextFunction(Box<NondeterministicCollationTextFunction>),
    /// An explicitly-qualified `FROM <schema>.<table>` source reference
    /// (ADR-0011) named a schema that does not
    /// actually contain a table by that name, as introspected live against
    /// `information_schema.tables`
    /// (`super::catalog::confirm_qualified_source`). Distinct
    /// from [`super::catalog::CatalogError::SourceTableNotFound`]: that
    /// variant means "no schema on `search_path` has this bare name",
    /// whereas this one means "the exact schema the definition named is
    /// real, or isn't, but either way doesn't have this table" — an explicit
    /// spelling resolves to that one relation, never a `search_path` walk.
    QualifiedSourceTableNotFound { schema: String, table: String },
    /// The `TRANSFORM <target>` twin of
    /// [`ValidationError::QualifiedSourceTableNotFound`]: an explicitly-qualified
    /// `TRANSFORM <schema>.<target>` reference named a schema
    /// that doesn't actually contain a table by that name. For the common
    /// `super::catalog::install_definition` path this schema is checked
    /// *after* the physical `CREATE TABLE` DDL already ran under the same
    /// explicit schema, so a miss here would mean that DDL itself failed
    /// silently rather than a genuine spelling error; for the ring-path entry
    /// points (`super::catalog::create_definition`/
    /// `super::catalog::create_definition_without_backfill`), which assume
    /// their caller already created the physical target table, this is the
    /// only check that a bogus explicit schema ever gets.
    QualifiedTargetTableNotFound { schema: String, table: String },
    /// `STRING_AGG`'s separator is not a string literal. The
    /// separator is fixed per definition so the ledger's contribution column
    /// is the value argument alone.
    StringAggSeparatorNotLiteral { field: String },
}

/// Payload of [`ValidationError::NondeterministicKeyCollation`], boxed out of
/// the enum so its fields don't inflate every error type that wraps
/// [`ValidationError`] (clippy::result_large_err).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NondeterministicKeyCollation {
    pub key: KeyColumnRole,
    /// The column's table as the check resolved it (`schema.table`).
    pub table: String,
    pub column: String,
    pub collation: String,
}

/// Payload of [`ValidationError::NondeterministicCollationTextFunction`],
/// boxed out of the enum for the same reason as
/// [`NondeterministicKeyCollation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NondeterministicCollationTextFunction {
    /// The calculated field whose own expression makes the call.
    pub field: String,
    /// `STRPOS` or `REGEXP_COUNT`.
    pub function: String,
    /// The column's table as the check resolved it (`schema.table`).
    pub table: String,
    pub column: String,
    pub collation: String,
}

/// The registry functions whose Postgres implementation refuses a
/// nondeterministic collation with `0A000`, so the engine can't compute them
/// the way Postgres would on such a column: `strpos` ("not supported for
/// substring searches") and `regexp_count` ("not supported for regular
/// expressions"). The registry's other text functions, `char_length` and
/// `octet_length`, don't depend on collation.
pub(crate) const NONDETERMINISTIC_COLLATION_REFUSING_FUNCTIONS: &[&str] =
    &["STRPOS", "REGEXP_COUNT"];

/// One read [`collation_refusing_function_reads`] found.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CollationRefusingRead {
    /// The field whose own expression makes the call.
    pub field: String,
    pub function: String,
    /// The source column read, directly or through `COALESCE` or another
    /// calculated field.
    pub column: String,
    /// The table qualifier the column was written with (`posts.author`), for
    /// a `JOIN` to resolve against the side it names; `None` for a bare
    /// name, which reads the `FROM` table.
    pub qualifier: Option<QualifiedColumn>,
}

/// A column an expression reads, with the qualifier it was written with.
type ReadColumn = (Option<QualifiedColumn>, String);

/// Every column an argument of a `STRPOS`/`REGEXP_COUNT` call in `def`'s
/// fields reads, for the catalog to check each
/// one's collation. In Postgres an argument's collation comes from the
/// columns under it: directly, through `COALESCE`, or through another
/// calculated field it references (whose expression Postgres would inline).
/// So this follows all three, resolving a bare name the way field-alias
/// substitution does (`super::backfill::substitute_all_fields`): another
/// field of that name first, unless the name is the enclosing
/// field's own (a passthrough of the source column it's named after). Each
/// read is reported once, in field order.
pub(crate) fn collation_refusing_function_reads(def: &TransformDef) -> Vec<CollationRefusingRead> {
    let fields_by_name: HashMap<&str, &FieldDef> =
        def.fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let mut reads = Vec::new();
    for field in &def.fields {
        collect_refusing_calls(&field.expr, &field.name, &fields_by_name, &mut reads);
    }
    let mut seen = HashSet::new();
    reads.retain(|read| seen.insert(read.clone()));
    reads
}

fn collect_refusing_calls(
    expr: &Expr,
    field: &str,
    fields_by_name: &HashMap<&str, &FieldDef>,
    out: &mut Vec<CollationRefusingRead>,
) {
    let aggregate_scope;
    let mut fields_by_name = fields_by_name;
    match expr {
        Expr::FunctionCall { name, args } => {
            // An aggregate's argument reads source columns, not aggregate
            // fields (see `aggregate_field_names`).
            if super::registry::is_aggregate_name(name) {
                let aggregates =
                    aggregate_field_names(fields_by_name.iter().map(|(n, f)| (*n, &f.expr)));
                aggregate_scope = without_aggregate_fields(fields_by_name, &aggregates);
                fields_by_name = &aggregate_scope;
            }
            if name == "NULLIF" {
                let operands: Vec<&Expr> = args.iter().collect();
                collect_text_comparison("NULLIF", &operands, field, fields_by_name, out);
            }
            if NONDETERMINISTIC_COLLATION_REFUSING_FUNCTIONS.contains(&name.as_str()) {
                let mut columns = Vec::new();
                let mut visited = HashSet::new();
                for arg in args {
                    collect_text_columns(arg, field, fields_by_name, &mut visited, &mut columns);
                }
                out.extend(
                    columns
                        .into_iter()
                        .map(|(qualifier, column)| CollationRefusingRead {
                            field: field.to_string(),
                            function: name.clone(),
                            column,
                            qualifier,
                        }),
                );
            }
        }
        Expr::BinaryOp { op, lhs, rhs } if op.is_comparison() && !op.is_ordering() => {
            collect_text_comparison(
                super::registry::operator_spec(*op).symbol,
                &[lhs, rhs],
                field,
                fields_by_name,
                out,
            );
        }
        _ => {}
    }
    for child in expr.children() {
        collect_refusing_calls(child, field, fields_by_name, out);
    }
}

/// The reads of a text comparison (`construct`: its operator, or `NULLIF`)
/// of `operands`: the columns whose text values reach it. Under a
/// nondeterministic collation `=` calls two different texts equal, which
/// Trellis's own exact-text comparisons do not.
fn collect_text_comparison(
    construct: &str,
    operands: &[&Expr],
    field: &str,
    fields_by_name: &HashMap<&str, &FieldDef>,
    out: &mut Vec<CollationRefusingRead>,
) {
    let mut columns = Vec::new();
    let mut visited = HashSet::new();
    for operand in operands {
        collect_value_columns(operand, field, fields_by_name, &mut visited, &mut columns);
    }
    out.extend(
        columns
            .into_iter()
            .map(|(qualifier, column)| CollationRefusingRead {
                field: field.to_string(),
                function: construct.to_string(),
                column,
                qualifier,
            }),
    );
}

/// The columns whose values `expr` can return as its own value: a column, a
/// calculated field it reads, and the arguments of `COALESCE` and `NULLIF`
/// and the results (not the conditions) of a `CASE`. Anything else (a call,
/// an operator) returns a value of another type or a computed one.
fn collect_value_columns<'a>(
    expr: &Expr,
    field: &str,
    fields_by_name: &HashMap<&str, &'a FieldDef>,
    visited: &mut HashSet<&'a str>,
    out: &mut Vec<ReadColumn>,
) {
    match expr {
        Expr::Column(name) => match fields_by_name.get(name.as_str()) {
            Some(other) if name != field => {
                if visited.insert(other.name.as_str()) {
                    collect_value_columns(&other.expr, &other.name, fields_by_name, visited, out);
                }
            }
            _ => out.push((None, name.clone())),
        },
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => out.push((
            Some(QualifiedColumn {
                schema: schema.clone(),
                table: table.clone(),
                column: column.clone(),
            }),
            column.clone(),
        )),
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            for arg in args {
                collect_value_columns(arg, field, fields_by_name, visited, out);
            }
        }
        Expr::FunctionCall { name, args } if name == "NULLIF" => {
            if let Some(first) = args.first() {
                collect_value_columns(first, field, fields_by_name, visited, out);
            }
        }
        Expr::Case {
            branches,
            otherwise,
        } => {
            for (_, result) in branches {
                collect_value_columns(result, field, fields_by_name, visited, out);
            }
            if let Some(otherwise) = otherwise {
                collect_value_columns(otherwise, field, fields_by_name, visited, out);
            }
        }
        _ => {}
    }
}

/// The columns under `expr`, an expression evaluated inside field `field`,
/// following references to other calculated fields (`visited` guards a
/// cycle, which [`validate`] has already refused).
fn collect_text_columns<'a>(
    expr: &Expr,
    field: &str,
    fields_by_name: &HashMap<&str, &'a FieldDef>,
    visited: &mut HashSet<&'a str>,
    out: &mut Vec<ReadColumn>,
) {
    match expr {
        Expr::Column(name) => match fields_by_name.get(name.as_str()) {
            Some(other) if name != field => {
                if visited.insert(other.name.as_str()) {
                    collect_text_columns(&other.expr, &other.name, fields_by_name, visited, out);
                }
            }
            _ => out.push((None, name.clone())),
        },
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => out.push((
            Some(QualifiedColumn {
                schema: schema.clone(),
                table: table.clone(),
                column: column.clone(),
            }),
            column.clone(),
        )),
        _ => {
            for child in expr.children() {
                collect_text_columns(child, field, fields_by_name, visited, out);
            }
        }
    }
}

/// Refuses a definition whose field reads, in a `STRPOS`/`REGEXP_COUNT`
/// call, a column with a nondeterministic collation
/// ([`collation_refusing_function_reads`]). Postgres raises `0A000` for both
/// under such a collation, while the engine computes them from the exact
/// text, so the two would disagree. `resolve` gives a read's table, as the
/// refusal names it, and live column; a read it can't resolve passes, since
/// the validators own that error. `only_fields`, if given, limits the check
/// to those fields (`ALTER TRANSFORM`'s added and altered ones). The plain
/// shape's catalog checks and the `JOIN` validators
/// ([`check_join_collation_reads`]) both call it, at define and on every
/// resume.
pub(crate) fn check_collation_reads<'a>(
    def: &TransformDef,
    only_fields: Option<&[FieldDef]>,
    resolve: impl Fn(&CollationRefusingRead) -> Option<(String, &'a JoinColumnInfo)>,
) -> Result<(), ValidationError> {
    for read in collation_refusing_function_reads(def) {
        if only_fields.is_some_and(|only| !only.iter().any(|field| field.name == read.field)) {
            continue;
        }
        let Some((table, info)) = resolve(&read) else {
            continue;
        };
        if !info.collation_deterministic {
            return Err(ValidationError::NondeterministicCollationTextFunction(
                Box::new(NondeterministicCollationTextFunction {
                    field: read.field,
                    function: read.function,
                    table,
                    column: read.column,
                    collation: info.collation.clone().unwrap_or_default(),
                }),
            ));
        }
    }
    Ok(())
}

/// [`check_collation_reads`] for a `JOIN`: each read resolves against the
/// side its table qualifier names, so a joined-table column is checked
/// against the joined table, whatever the `FROM` table holds. A bare read
/// passes. Shared by the aggregate and the 1-1 `JOIN` validators.
pub(crate) fn check_join_collation_reads(
    def: &TransformDef,
    join: &JoinSpec,
    root: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
) -> Result<(), ValidationError> {
    let resolve = join_resolver(def, join, root, joined);
    check_collation_reads(def, None, |read| {
        let qualifier = read.qualifier.as_ref()?;
        let (_, info) = resolve(qualifier).ok()?;
        Some((qualifier.table.clone(), info))
    })
}

/// Payload of [`ValidationError::AmbiguousColumnReference`], boxed out of
/// the enum like [`NondeterministicKeyCollation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousColumnReference {
    /// The field whose expression reads the name.
    pub field: String,
    /// The bare name, which is both a source column and `shadowing_field`.
    pub name: String,
    /// The other field's expression, as the grammar writes it.
    pub shadowing_expr: String,
    /// The `FROM` table's bare name.
    pub source_table: String,
    /// The `FROM` table's schema, when known: written in the `FROM` clause,
    /// or filled in by the catalog once it has resolved a bare one
    /// ([`ValidationError::with_source_schema`]).
    pub source_schema: Option<String>,
}

impl ValidationError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Every variant here is a rejected definition.
    pub fn code(&self) -> ErrorCode {
        ErrorCode::Validation
    }

    /// Fills in the `FROM` table's schema for an
    /// [`ValidationError::AmbiguousColumnReference`] that doesn't have it yet,
    /// so its message spells the three-part qualified name. `schema` comes
    /// from the catalog's resolution of a bare `FROM`. Any other error is
    /// returned as it is.
    pub(crate) fn with_source_schema(self, schema: &str) -> Self {
        match self {
            ValidationError::AmbiguousColumnReference(mut ambiguous)
                if ambiguous.source_schema.is_none() =>
            {
                ambiguous.source_schema = Some(schema.to_string());
                ValidationError::AmbiguousColumnReference(ambiguous)
            }
            ValidationError::ExtremesWithoutSourceIndex {
                target,
                mut indexes,
            } => {
                for index in &mut indexes {
                    if !index.table.contains('.') {
                        index.table = format!("{schema}.{}", index.table);
                    }
                }
                ValidationError::ExtremesWithoutSourceIndex { target, indexes }
            }
            other => other,
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::DuplicateFieldName { name } => {
                write!(f, "duplicate calculated field name '{name}'")
            }
            ValidationError::ReservedFieldName { field } => write!(
                f,
                "field name '{field}' starts with '__', which Trellis reserves for the hidden \
                 columns it keeps on a target table (such as an AVG's running sum); choose a \
                 name that doesn't start with '__'"
            ),
            ValidationError::ReservedGroupByColumnName { key, column } => write!(
                f,
                "GROUP BY key '{key}' would become target column '{column}', but Trellis \
                 reserves names starting with '__' for the hidden columns it keeps on a target \
                 table. GROUP BY keys can't be aliased yet, so group over a 1-1 transform that \
                 selects the column under a name that doesn't start with '__'"
            ),
            ValidationError::FieldNamedAfterPrimaryKey { field } => write!(
                f,
                "field '{field}' has the name of a primary key column of the source table, \
                 which the target already carries as its own key. Drop the field (the key \
                 column is in the target without it), or give it a name that isn't a key \
                 column's (e.g. `{field} AS source_{field}`)"
            ),
            ValidationError::UnresolvedColumn { field, column } => write!(
                f,
                "calculated field '{field}' references '{column}', which is neither a \
                 source column nor another calculated field on this target"
            ),
            ValidationError::Cycle { cycle } => {
                write!(f, "cycle among calculated columns: {}", cycle.join(" -> "))
            }
            ValidationError::FieldExpansionTooLarge => write!(
                f,
                "the calculated fields read each other so deeply or so many times, or one \
                 expression is so large (a CASE with thousands of branches), that inlining \
                 them into one expression yields more than a build can render; make the chain \
                 or the expression shorter"
            ),
            ValidationError::TableCycle { cycle } => {
                write!(f, "cycle among tables: {}", cycle.join(" -> "))
            }
            ValidationError::TargetEqualsSource { table } => write!(
                f,
                "target table '{table}' is the same as the source table; calculated \
                 columns must live on a separate neighbor table"
            ),
            ValidationError::JoinNeedsBothTables => write!(
                f,
                "a JOIN definition is validated against both tables' columns, not the FROM \
                 table's alone"
            ),
            ValidationError::JoinSelf { table } => write!(
                f,
                "'{table}' is joined to itself; a JOIN can't read one table on both sides \
                 (a self-join), even under two aliases, since one captured change would feed \
                 both sides"
            ),
            ValidationError::JoinTooManyTables => write!(
                f,
                "a JOIN transform joins exactly two tables; a second JOIN is not supported"
            ),
            ValidationError::JoinUnknownTable { reference } => write!(
                f,
                "'{reference}' names a table that is neither the FROM table nor the joined \
                 table, nor an alias of either"
            ),
            ValidationError::JoinUnqualifiedColumn { field, column } => write!(
                f,
                "field '{field}' reads column '{column}' without a table; in a JOIN every \
                 column is written `<table>.<column>`, or `<alias>.<column>` for an aliased \
                 table"
            ),
            ValidationError::JoinUnresolvedColumn { reference } => {
                write!(f, "'{reference}' is not a column of its table")
            }
            ValidationError::JoinOnSameSide { term } => write!(
                f,
                "ON term '{term}' must compare a column of the FROM table with a column of \
                 the joined table"
            ),
            ValidationError::JoinMixedSideArgument { field, argument } => write!(
                f,
                "field '{field}': {argument} reads columns of both tables; an aggregate's \
                 argument reads one table's columns only, so aggregate each table \
                 separately and compose the results"
            ),
            ValidationError::JoinOnRepeatedPair { term } => write!(
                f,
                "ON term '{term}' repeats a column pair an earlier term already joins"
            ),
            ValidationError::JoinOnRepeatedColumn { term, column } => write!(
                f,
                "ON term '{term}' names {column}, which an earlier term already names; a \
                 column appears in at most one ON term, so drop the term or join a \
                 different column"
            ),
            ValidationError::IndexKeyTooWide {
                clause,
                columns,
                limit,
            } => write!(
                f,
                "{clause} has {columns} columns, but Trellis keys a table on them and \
                 Postgres caps an index at {limit} columns; use at most {limit}"
            ),
            ValidationError::TableTooWide {
                table,
                columns,
                limit,
                dropped,
            } => {
                write!(
                    f,
                    "{table} would have {columns} columns, but Postgres caps a table at \
                     {limit} columns"
                )?;
                if *dropped > 0 {
                    write!(
                        f,
                        " (counting {dropped} dropped columns, which Postgres keeps counting \
                         until the table is created again; drop the transform and define it \
                         again to reclaim them)"
                    )?;
                }
                write!(f, "; use fewer fields or aggregate arguments")
            }
            ValidationError::JoinKeyTypeMismatch {
                lhs,
                lhs_type,
                rhs,
                rhs_type,
            } => write!(
                f,
                "ON joins {lhs} ({lhs_type}) to {rhs} ({rhs_type}), which don't match: join \
                 columns must have the same type and collation (a type modifier never \
                 counts, and text and varchar columns pair whatever their length), because \
                 Trellis never casts a join key. Alter one column to match the other"
            ),
            ValidationError::JoinUnsupportedKeyType { reference, pg_type } => write!(
                f,
                "ON joins on {reference} ({pg_type}), a type whose equality isn't text-stable, \
                 so the engine (which compares join keys as text) would silently diverge from \
                 Postgres's typed join; supported join key types: {}",
                super::join_key_types::supported_join_key_types()
            ),
            ValidationError::JoinNondeterministicCollation {
                reference,
                collation,
            } => write!(
                f,
                "ON joins on {reference}, whose collation \"{collation}\" is nondeterministic: \
                 Postgres treats distinct strings as equal under it (e.g. by case), but \
                 Trellis matches join keys by their exact text, so join on a column with a \
                 deterministic collation"
            ),
            ValidationError::JoinFieldShape { field, detail } => {
                write!(f, "field '{field}' of a JOIN definition: {detail}")
            }
            ValidationError::OneToOneJoinAggregate { field, function } => write!(
                f,
                "field '{field}' of a 1-1 JOIN calls {function}, an aggregate, but a JOIN \
                 without a GROUP BY has one target row per row of the FROM table and nothing \
                 to aggregate. Add a GROUP BY to aggregate, or compute the aggregate in a \
                 transform of the joined table and join to its target"
            ),
            ValidationError::OneToOneJoinKeyNotUnique { table, columns } => write!(
                f,
                "a 1-1 JOIN needs the ON columns of '{table}' ({}) to be unique, so a row of \
                 the FROM table matches at most one row of it, but no primary key, unique \
                 constraint or non-partial unique index without expressions on '{table}' \
                 lies within them. Add a unique constraint on those columns, or join on a \
                 column set that is unique (the group key of an aggregate target works)",
                columns.join(", ")
            ),
            ValidationError::OneToOneJoinUnsupported { what, remedy } => {
                write!(f, "a 1-1 JOIN doesn't take {what}: {remedy}")
            }
            ValidationError::StarJoinTooManyTables { tables } => write!(
                f,
                "this JOIN reads {tables} tables; a 1-1 JOIN reads at most {} (the FROM table \
                 and {} joined tables)",
                super::join::MAX_POSITIONS + 1,
                super::join::MAX_POSITIONS
            ),
            ValidationError::JoinOnReadsAnotherJoinedTable { table, term, other } => write!(
                f,
                "'{term}' in the ON of '{table}' reads '{other}', another joined table: each \
                 JOIN's ON reads only the FROM table and the table it joins (every joined \
                 table joins the FROM table directly, a star; joining one joined table to \
                 another, a snowflake, is not supported)"
            ),
            ValidationError::DeferredAggregate { function, family } => write!(
                f,
                "{function} is not supported: {family} is not an aggregate Trellis maintains; \
                 the supported aggregates are COUNT(*), COUNT(col), SUM/AVG over \
                 smallint, integer, bigint or numeric, and, in a GROUP BY without a \
                 JOIN, MIN/MAX of a number, date or time column (see \"Supported sources and \
                 targets\" in docs/transforms.md)"
            ),
            ValidationError::UnsupportedAggregateArgument {
                function,
                column,
                pg_type,
            } => write!(
                f,
                "{function}({column}) is over {pg_type}; SUM and AVG are supported over \
                 smallint, integer, bigint and numeric only"
            ),
            ValidationError::UnsupportedExtremeArgument {
                function,
                argument,
                pg_type,
            } => write!(
                f,
                "{function}({argument}) is over {pg_type}; MIN and MAX are \
                 supported over smallint, integer, bigint, numeric, real, double precision, \
                 date, time, timestamp, timestamptz, interval and oid only (a type with a \
                 collation, such as text, is not supported yet)"
            ),
            ValidationError::ExtremeOfExpression { function, argument } => write!(
                f,
                "{function}({argument}) is over an expression; MIN and MAX take a plain source \
                 column: when a group's extreme is deleted, the next one is re-read through an \
                 index on that column. To aggregate an expression, add it to the source as a stored \
                 generated column, index it after the GROUP BY columns, and take the MIN/MAX \
                 of that column"
            ),
            ValidationError::ChainedExtreme { function, column } => write!(
                f,
                "{function}({column}) is over a column of a table Trellis creates (another \
                 transform's target), which is not supported: a MIN/MAX re-reads a group's \
                 extreme from its source through an index on it. Take the MIN/MAX in the \
                 transform that reads the source"
            ),
            ValidationError::ExtremesWithoutSourceIndex { target, indexes } => write!(
                f,
                "{}",
                super::source_index::extreme_refusal_text(target, indexes, true)
            ),
            ValidationError::ReservedTargetSuffix { target, suffix } => write!(
                f,
                "target name '{target}' ends with '{suffix}', which Trellis reserves for \
                 the tables it keeps beside a target; choose a name that doesn't"
            ),
            ValidationError::TargetNameTooLong { target, max } => write!(
                f,
                "target name '{target}' is longer than {max} bytes; Trellis keeps a ledger \
                 table beside every target named '<target>{}', and Postgres identifiers are \
                 limited to 63 bytes",
                super::ledger::LEDGER_SUFFIX
            ),
            ValidationError::TypeMismatch {
                field,
                operator,
                expected,
                found,
            } => write!(
                f,
                "calculated field '{field}': operator '{operator}' expects a {expected} value \
                 here, found {found}"
            ),
            ValidationError::UntypedNull { field, context } => write!(
                f,
                "calculated field '{field}' has a NULL in {context}, where nothing gives it a \
                 type; write CAST(NULL AS <type>) with the type you mean (see \"Refusals\" in \
                 docs/expressions.md)"
            ),
            ValidationError::ConstantDivisorZero { field, text } => write!(
                f,
                "calculated field '{field}' divides by a literal zero in {text}; Postgres \
                 raises a division-by-zero error for every row that has a dividend. Use a \
                 nonzero divisor, or a column (see \"Refusals\" in docs/expressions.md)"
            ),
            ValidationError::IntegerDivision { field, lhs, rhs } => write!(
                f,
                "calculated field '{field}' divides a {lhs} value by a {rhs} value; Postgres \
                 truncates an integer quotient (7 / 2 is 3). Divide as numeric: \
                 CAST(<dividend> AS numeric) / <divisor>"
            ),
            ValidationError::FloatModulo { field } => write!(
                f,
                "calculated field '{field}' applies '%' to a float; Postgres has no \
                 modulo for real or double precision. '%' takes integers and numeric"
            ),
            ValidationError::UnsupportedCast { field, from, to } => write!(
                f,
                "calculated field '{field}' casts a {from} value to {to}; the only casts \
                 admitted are an integer to the same or a wider integer, any integer to \
                 numeric, numeric to numeric, and a date or timestamp to timestamp, because \
                 only those are exact and read no session setting (see \"Refusals\" in docs/expressions.md)"
            ),
            ValidationError::TemporalOperand {
                field,
                construct,
                found,
                reason,
            } => write!(
                f,
                "calculated field '{field}': {construct} does not take a {found} value: \
                 {reason} (see \"Refusals\" in docs/expressions.md)"
            ),
            ValidationError::TemporalLiteral {
                field,
                construct,
                text,
                reason,
            } => match text {
                Some(text) => write!(
                    f,
                    "calculated field '{field}': {construct} cannot take '{text}': {reason}"
                ),
                None => write!(
                    f,
                    "calculated field '{field}': {construct} needs a literal here, not a \
                     column or expression: {reason}"
                ),
            },
            ValidationError::NumericFunctionArgument {
                field,
                function,
                found,
                reason,
            } => write!(
                f,
                "calculated field '{field}': {function} does not take a {found} value: {reason}"
            ),
            ValidationError::ScaleNotIntegerLiteral { field, function } => write!(
                f,
                "calculated field '{field}': the scale argument of {function} must be an \
                 integer literal (it may be negative), not a column or expression"
            ),
            ValidationError::MixedFloatExactComparison {
                field,
                operator,
                lhs,
                rhs,
            } => write!(
                f,
                "calculated field '{field}' compares a {lhs} value with a {rhs} value using \
                 '{operator}'; a float and an exact numeric (an integer or numeric) cannot be \
                 compared, because Postgres would round the exact side to a float. Compare \
                 values of one family: rewrite so both sides are floats or both are exact \
                 (only a bare number literal such as 0.5 is admitted against a float)"
            ),
            ValidationError::MixedTemporalComparison {
                field,
                operator,
                lhs,
                rhs,
            } => write!(
                f,
                "calculated field '{field}' compares a {lhs} value with a {rhs} value using \
                 '{operator}'; Postgres converts one side first, and a conversion to or from \
                 timestamptz depends on the session time zone. Compare two values of one \
                 temporal type: cast one side, as in CAST(d AS timestamp)"
            ),
            ValidationError::TextOrderingComparison { field, operator } => write!(
                f,
                "calculated field '{field}' orders text with '{operator}'; the order of text \
                 depends on the column's collation, which Trellis does not evaluate. Only \
                 =, <>, != and IS [NOT] DISTINCT FROM compare text; order by a non-text value, \
                 or compare length(s) with CHAR_LENGTH"
            ),
            ValidationError::ComparisonTypeMismatch {
                field,
                operator,
                lhs,
                rhs,
            } => write!(
                f,
                "calculated field '{field}': '{operator}' cannot compare a {lhs} value with a \
                 {rhs} value; it compares two exact numerics, two floats, two values of one \
                 temporal type (date, timestamp, timestamptz, time, timetz), two booleans, two \
                 uuids, or two text values (text with =, <>, != and IS [NOT] DISTINCT FROM only)"
            ),
            ValidationError::NullIfFirstArgumentCast {
                field,
                lhs,
                rhs,
                result,
            } => write!(
                f,
                "calculated field '{field}': NULLIF of a {lhs} value and a {rhs} value is a \
                 {result} in Postgres, which casts the first argument to compare it, not the \
                 first argument's type. Cast the first argument to {result} yourself, or \
                 write the {rhs} value first"
            ),
            ValidationError::CaseConditionNotBoolean { field, found } => write!(
                f,
                "calculated field '{field}': a CASE condition must be a boolean, found {found}"
            ),
            ValidationError::CaseResultTypeMismatch {
                field,
                expected,
                found,
            } => write!(
                f,
                "calculated field '{field}': the results of a CASE must share a type, as the \
                 arguments of COALESCE do; found a {found} result beside {expected}"
            ),
            ValidationError::FunctionArgTypeMismatch {
                field,
                function,
                arg_index,
                expected,
                found,
            } => write!(
                f,
                "calculated field '{field}': argument {} of '{function}' expects a {expected} \
                 value, found {found}",
                arg_index + 1
            ),
            ValidationError::NonLiteralRegexPattern { field } => write!(
                f,
                "calculated field '{field}': regexp_count's pattern argument must be a string \
                 literal, not a column reference or expression"
            ),
            ValidationError::InvalidTypedLiteral {
                field,
                value_type,
                text,
                detail,
            } => write!(
                f,
                "calculated field '{field}': {value_type} literal '{text}' is not in canonical \
                 form: {detail}"
            ),
            ValidationError::UnsupportedLiteralType { field, value_type } => write!(
                f,
                "calculated field '{field}': '{value_type}' is not a type a literal can be \
                 spelled as (see docs/type-support.md)"
            ),
            ValidationError::UnresolvedGroupByColumn { column } => write!(
                f,
                "GROUP BY references '{column}', which is not a source column"
            ),
            ValidationError::UnsupportedGroupByKeyType { column, value_type } => write!(
                f,
                "GROUP BY key '{column}' is a {value_type} column, a type Trellis can only pass \
                 through today: GROUP BY keys are matched by their text rendering, which doesn't \
                 agree with this type's own equality (see docs/type-support.md); supported GROUP \
                 BY key types are {}",
                supported_group_by_key_types()
            ),
            ValidationError::GroupByKeysShareTargetColumn {
                first,
                second,
                column,
            } if first == second => write!(
                f,
                "GROUP BY lists '{first}' twice, which would declare target column '{column}' \
                 twice; list each GROUP BY key once"
            ),
            ValidationError::GroupByKeysShareTargetColumn {
                first,
                second,
                column,
            } => write!(
                f,
                "GROUP BY keys '{first}' and '{second}' would both become target column \
                 '{column}', and a target table can't have two columns with the same name. \
                 GROUP BY keys can't be aliased yet, so group by only one of them"
            ),
            ValidationError::UngroupedColumnReference { field, column } => write!(
                f,
                "calculated field '{field}' references source column '{column}' outside of \
                 SUM/MIN/MAX/AVG; a non-grouping-key column must be aggregated, not referenced \
                 bare"
            ),
            ValidationError::AggregateOfAggregateField { field, reference } => write!(
                f,
                "calculated field '{field}' aggregates '{reference}', which is another \
                 aggregate field, not a source column; an aggregate's argument can't be \
                 another aggregate (use the source column the aggregate reads instead)"
            ),
            ValidationError::AmbiguousColumnReference(ambiguous) => {
                let AmbiguousColumnReference {
                    field,
                    name,
                    shadowing_expr,
                    source_table,
                    source_schema,
                } = ambiguous.as_ref();
                let two_part = format!("{source_table}.{name}");
                let three_part = match source_schema {
                    Some(schema) => format!("{schema}.{source_table}.{name}"),
                    None => format!("<schema>.{source_table}.{name}"),
                };
                write!(
                    f,
                    "calculated field '{field}' reads '{name}', which is ambiguous: it names \
                     both the source column '{name}' of '{source_table}' and the calculated \
                     field '{name}' (`{shadowing_expr}`). Write `{two_part}` or \
                     `{three_part}` to read the source column, or rename the field '{name}' \
                     and read it by its new name"
                )
            }
            ValidationError::QualifiedColumnNotFromSource {
                field,
                reference,
                source,
            } => write!(
                f,
                "calculated field '{field}' reads '{reference}', but a qualified column must \
                 name this transform's FROM table ('{source}'); a second table is read with \
                 a JOIN"
            ),
            ValidationError::GroupingColumnFieldMustBePassthrough { field } => write!(
                f,
                "calculated field '{field}' shares its name with a GROUP BY column, so it must \
                 be a passthrough of that column (`{field}` or `<table>.{field}`), not another \
                 expression"
            ),
            ValidationError::CalculatedFieldShadowsSourceColumn { field } => write!(
                f,
                "calculated field '{field}' shares its name with a source column, so it must \
                 be a passthrough of that column (`{field}` or `<table>.{field}`), not another \
                 expression; a different name avoids shadowing the real column for other \
                 fields that reference it"
            ),
            ValidationError::NondeterministicKeyCollation(refused) => {
                let NondeterministicKeyCollation {
                    key,
                    table,
                    column,
                    collation,
                } = refused.as_ref();
                write!(
                    f,
                    "{table}.{column} is {key}, and its collation \"{collation}\" is \
                 nondeterministic; its `=` matches strings that differ (e.g. by case), but \
                 Trellis matches keys by their exact text, so key columns need a \
                 deterministic collation"
                )
            }
            ValidationError::NondeterministicCollationTextFunction(refused) => {
                let NondeterministicCollationTextFunction {
                    field,
                    function,
                    table,
                    column,
                    collation,
                } = refused.as_ref();
                let reason = if NONDETERMINISTIC_COLLATION_REFUSING_FUNCTIONS
                    .contains(&function.as_str())
                {
                    format!(
                        "Postgres refuses {function} under a nondeterministic collation, while \
                         Trellis would compute it from the exact text"
                    )
                } else {
                    format!(
                        "such a collation can call two different texts equal (a case-insensitive \
                         one equates 'a' and 'A'), which {function} would then also do while \
                         Trellis compares the exact text"
                    )
                };
                write!(
                    f,
                    "calculated field '{field}': {function} reads {table}.{column}, whose \
                     collation \"{collation}\" is nondeterministic; {reason}, so the column \
                     needs a deterministic collation"
                )
            }
            ValidationError::QualifiedSourceTableNotFound { schema, table } => write!(
                f,
                "FROM names '{schema}.{table}' explicitly, but schema '{schema}' has no table \
                 named '{table}'; an explicit schema.table spelling resolves to that exact \
                 relation, not a search_path walk (ADR-0011), so this is checked as written"
            ),
            ValidationError::QualifiedTargetTableNotFound { schema, table } => write!(
                f,
                "TRANSFORM names '{schema}.{table}' explicitly, but schema '{schema}' has no \
                 table named '{table}'; an explicit schema.table spelling resolves to that exact \
                 relation, not the configured target schema (ADR-0011), so this is checked as \
                 written"
            ),
            ValidationError::StringAggSeparatorNotLiteral { field } => write!(
                f,
                "calculated field '{field}': STRING_AGG's separator must be a string literal"
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validates `def` against the 1-1 subset. `source_columns` maps each
/// column name known to exist on `def.source` to its [`ValueType`];
/// resolving it against a real Postgres schema is out of scope here, so
/// callers supply it explicitly.
pub fn validate(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), ValidationError> {
    validate_target_names(def)?;

    // A JOIN is validated by [`validate_join`], which needs both tables'
    // columns; this entry point has only the source's.
    if let KeySpace::Join(_) | KeySpace::JoinOneToOne(_) = def.key_space {
        return Err(ValidationError::JoinNeedsBothTables);
    }

    validate_qualified_columns(def, source_columns, def.explicit_source_schema.as_deref())?;

    // Exhaustive matches: these are the "re-verify at this layer" guard for
    // the already-enforced 1-1/TRUE-only constructs (see module docs).
    // `Aggregate` has real runtime checks (below), since — unlike the other
    // arm here — the AST can encode an invalid one.
    match &def.key_space {
        KeySpace::Join(_) | KeySpace::JoinOneToOne(_) => {
            return Err(ValidationError::JoinNeedsBothTables);
        }
        KeySpace::OneToOne => {
            for field in &def.fields {
                // A calculated field aliased to a real source column's name
                // must be a passthrough of it, bare or qualified
                // (`Expr::reads_source_column`) — the same shape the
                // `Aggregate` arm below enforces for grouping columns. `is_self_passthrough`'s exemption in `infer_expr`
                // only covers a field referencing its *own* name;
                // without this check here, a different, non-passthrough
                // field could still claim the column's name, and any *third*
                // field referencing that name would then silently resolve to
                // the calculated value instead of the real column.
                if source_columns.contains_key(&field.name)
                    && !field.expr.reads_source_column(&field.name)
                {
                    return Err(ValidationError::CalculatedFieldShadowsSourceColumn {
                        field: field.name.clone(),
                    });
                }
            }
            // With every shadowing field refused above there is nothing
            // ambiguous left to find; kept so the rule holds in both arms.
            reject_ambiguous_names(def, source_columns)?;
        }
        KeySpace::Aggregate { group_by } => {
            // Checked ahead of the per-field checks below, so a bad
            // `GROUP BY` clause is never masked by a field error instead.
            // A key is its bare column name; its collation is the
            // catalog's to check.
            check_group_by_keys(group_by.iter().map(|key| {
                let value_type = source_columns.get(key).copied().ok_or_else(|| {
                    ValidationError::UnresolvedGroupByColumn {
                        column: key.clone(),
                    }
                })?;
                Ok(GroupByKey {
                    written: key.clone(),
                    column: key,
                    value_type,
                    collation: None,
                })
            }))?;
            for field in &def.fields {
                // DDL generation (`ddl::create_aggregate_target_table`)
                // treats a field named after a grouping column's target
                // column name as that key's passthrough and gives it no
                // target column of its own. Any other expression under that
                // name would compute a value with nowhere to go — enforce
                // the passthrough shape here rather than let DDL silently
                // drop it.
                if let Some(key) = group_by.iter().find(|k| k.as_str() == field.name)
                    && !expr_is_group_by_key_passthrough(&field.expr, key)
                {
                    return Err(ValidationError::GroupingColumnFieldMustBePassthrough {
                        field: field.name.clone(),
                    });
                }
            }
            // Ahead of the ungrouped-column check, which would
            // otherwise report a bare read of a shadowing field as an
            // ungrouped source column.
            reject_ambiguous_names(def, source_columns)?;
            for field in &def.fields {
                validate_aggregate_field_expr(
                    &field.expr,
                    &field.name,
                    group_by,
                    source_columns,
                    false,
                )?;
            }
        }
    }
    match def.predicate {
        Predicate::True => {}
    }

    let mut field_names = HashSet::with_capacity(def.fields.len());
    for field in &def.fields {
        if !field_names.insert(field.name.clone()) {
            return Err(ValidationError::DuplicateFieldName {
                name: field.name.clone(),
            });
        }
    }

    let aggregates = aggregate_field_names(def.fields.iter().map(|f| (f.name.as_str(), &f.expr)));
    let mut deps: HashMap<&str, FieldDeps> = HashMap::with_capacity(def.fields.len());
    for field in &def.fields {
        let mut refs = Vec::new();
        let mut aggregate_args = Vec::new();
        collect_reads(
            &field.expr,
            &aggregates,
            false,
            &mut refs,
            &mut aggregate_args,
        );
        // Inside an aggregate's argument, the name of an aggregate field is a
        // source column. When it isn't one, the argument would nest an
        // aggregate in an aggregate.
        if let Some(reference) = aggregate_args
            .into_iter()
            .find(|name| !source_columns.contains_key(name))
        {
            return Err(ValidationError::AggregateOfAggregateField {
                field: field.name.clone(),
                reference,
            });
        }

        let mut column_depths = HashMap::new();
        let own_height = column_depths_in(&field.expr, 1, &mut column_depths);
        let mut calc_deps = Vec::new();
        for column in refs {
            let is_source_column = source_columns.contains_key(&column);
            // A field referencing a source column of its own name (a plain
            // rename, e.g. `SELECT order_id AS order_id`) is not a
            // self-dependency — it's a passthrough of the source column.
            // Without this, `field_names.contains(&column)` would treat it
            // as the field depending on itself and report a spurious cycle.
            let is_self_passthrough = column == field.name && is_source_column;
            // A field referencing its own name that ISN'T a source column
            // (e.g. a passthrough of a column whose type isn't representable
            // by `ValueType`, or a plain typo) must not be treated as a
            // reference to a *different* calculated field of the same name —
            // `field_names` was built from `def.fields` before this loop, so
            // it always contains `field.name` itself. Without this check,
            // `field_names.contains(&column)` would be true purely because
            // `column == field.name`, misclassifying an unresolvable column
            // as a self-dependency and producing a spurious cycle instead of
            // `UnresolvedColumn`.
            let is_a_different_calc_field = column != field.name && field_names.contains(&column);
            let is_calc_field = !is_self_passthrough && is_a_different_calc_field;
            if !is_source_column && !is_calc_field {
                return Err(ValidationError::UnresolvedColumn {
                    field: field.name.clone(),
                    column,
                });
            }
            if is_calc_field {
                let depth = column_depths.get(&column).copied().unwrap_or(1);
                calc_deps.push((column, depth));
            }
        }
        deps.insert(
            field.name.as_str(),
            FieldDeps {
                own_height,
                reads: calc_deps,
            },
        );
    }

    detect_cycle(&deps)?;

    infer_field_types(def, source_columns)?;

    // Every build reads each field as one self-contained expression, with the
    // calculated fields it reads inlined. A chain that doubles at each level
    // inlines to more than any build can render.
    if let Err(super::backfill::BackfillError::Unsupported(_)) =
        super::backfill::substitute_all_fields(def)
    {
        return Err(ValidationError::FieldExpansionTooLarge);
    }

    if let KeySpace::Aggregate { .. } = &def.key_space {
        reject_extremes_without_source_index(def, source_columns)?;
        check_aggregate_table_widths(def, source_columns)?;
    }

    Ok(())
}

/// One resolved `GROUP BY` key, as [`check_group_by_keys`] checks it.
struct GroupByKey<'a> {
    /// The key as the definition spells it, for a refusal.
    written: String,
    /// Its column name, which is also the target column it becomes.
    column: &'a str,
    value_type: ValueType,
    /// The key's table and live column when its collation is checked here
    /// (a `JOIN`'s); a plain key's collation is the catalog's.
    collation: Option<(&'a str, &'a JoinColumnInfo)>,
}

/// The `GROUP BY` key rules a plain aggregate and an aggregate `JOIN`
/// share, refused in this order per key: it resolves (`keys` yields the
/// resolution's refusal in its place), it has an admitted key type, its
/// collation is deterministic (when checked), it doesn't share its target
/// column with an earlier key, and its column avoids the reserved prefix.
/// Then the keys together fit an index: the target's unique key. Returns
/// the keys' column names.
fn check_group_by_keys<'a>(
    keys: impl ExactSizeIterator<Item = Result<GroupByKey<'a>, ValidationError>>,
) -> Result<HashSet<&'a str>, ValidationError> {
    let width = keys.len();
    let mut seen: HashMap<&str, String> = HashMap::with_capacity(width);
    for key in keys {
        let key = key?;
        reject_unsupported_group_by_key_type(key.column, key.value_type)?;
        if let Some((table, info)) = key.collation {
            check_key_collation(KeyColumnRole::GroupBy, table, key.column, info)?;
        }
        // Each key becomes a target column named after its column, so two
        // keys of one column name would reach the target DDL as a duplicate
        // column. Checked after the key resolves, so an unknown column is
        // reported as such rather than as a clash.
        if let Some(first) = seen.get(key.column) {
            return Err(ValidationError::GroupByKeysShareTargetColumn {
                first: first.clone(),
                second: key.written,
                column: key.column.to_string(),
            });
        }
        // A key's column sits beside the target's hidden `__`-prefixed
        // columns, so it may not use their prefix.
        if key.column.starts_with(RESERVED_COLUMN_PREFIX) {
            return Err(ValidationError::ReservedGroupByColumnName {
                key: key.written,
                column: key.column.to_string(),
            });
        }
        seen.insert(key.column, key.written);
    }
    check_index_key_width(|| "GROUP BY".to_string(), width)?;
    Ok(seen.into_keys().collect())
}

/// Refuses `table.column`, a column the engine matches as a key in the role
/// `key`, if it has a nondeterministic collation: its `=` (and so its
/// `GROUP BY` and unique indexes) treats distinct strings as equal, while the
/// engine matches keys by their exact text, so Postgres would fold rows the
/// engine keeps apart. Refused, never reinterpreted.
///
/// A column with no collation, or the database default's, always passes:
/// Postgres has no nondeterministic database default (`CREATE DATABASE`
/// takes no `deterministic` option, and the `default` collation's
/// `collisdeterministic` is always true), which is also why a target Trellis
/// creates can't have a nondeterministic key. The check reads
/// `collisdeterministic` rather than assuming that, so it would still hold if
/// Postgres ever allowed one.
pub(crate) fn check_key_collation(
    key: KeyColumnRole,
    table: &str,
    column: &str,
    info: &JoinColumnInfo,
) -> Result<(), ValidationError> {
    if info.collation_deterministic {
        return Ok(());
    }
    Err(ValidationError::NondeterministicKeyCollation(Box::new(
        NondeterministicKeyCollation {
            key,
            table: table.to_string(),
            column: column.to_string(),
            collation: info.collation.clone().unwrap_or_default(),
        },
    )))
}

/// [`check_key_collation`] over the keys of `table` the catalog checks for
/// every shape: its row-identity key (`source_key`), which apply keys every
/// staged change by and a 1-1 target mirrors, then a plain aggregate's
/// `GROUP BY` keys (a `JOIN`'s are [`check_group_by_keys`]'s). `column`
/// looks a column up among the table's live columns; one that isn't there
/// passes, since the check that resolved it owns that error. The target's
/// own key columns need no check: Trellis creates the target with no
/// `COLLATE` clause, so they take the database default.
pub(crate) fn check_key_collations<'c>(
    def: &TransformDef,
    table: &str,
    source_key: &[&str],
    column: impl Fn(&str) -> Option<&'c JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let group_by = match &def.key_space {
        KeySpace::Aggregate { group_by } => group_by.iter().map(String::as_str).collect(),
        _ => Vec::new(),
    };
    for (role, names) in [
        (KeyColumnRole::SourceKey, source_key),
        (KeyColumnRole::GroupBy, &group_by),
    ] {
        for &name in names {
            if let Some(info) = column(name) {
                check_key_collation(role.clone(), table, name, info)?;
            }
        }
    }
    Ok(())
}

/// Refuses a `MIN`/`MAX` definition declared `WITHOUT SOURCE INDEX`
/// ([`ValidationError::ExtremesWithoutSourceIndex`]): a probe without its
/// index scans the group, and with no `GROUP BY` index either it scans the
/// whole source, once per probed group. Define, an edit and a resume all
/// validate through here, so the refusal holds for the definition's life.
fn reject_extremes_without_source_index(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), ValidationError> {
    if !def.without_source_index {
        return Ok(());
    }
    let indexes = super::source_index::extreme_indexes(def, source_columns, &def_source_name(def));
    if indexes.is_empty() {
        return Ok(());
    }
    Err(ValidationError::ExtremesWithoutSourceIndex {
        target: def.target.clone(),
        indexes,
    })
}

/// The source as `def` names it: `schema.table` when it spells the schema.
fn def_source_name(def: &TransformDef) -> String {
    match &def.explicit_source_schema {
        Some(schema) => format!("{schema}.{}", def.source),
        None => def.source.clone(),
    }
}

/// Refuses an aggregate whose target, or whose hidden table of partials (two
/// columns per distinct summed argument), would hold more columns than
/// Postgres allows in a table ([`TABLE_MAX_COLUMNS`]). Both widths are the
/// ones the tables' DDL creates ([`crate::staging::group_basis`]'s
/// `hidden_table_width` and `target_table_width`). An aggregate the group
/// basis doesn't take (one outside the admitted set) creates no table and is
/// refused where its function is checked.
fn check_aggregate_table_widths(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), ValidationError> {
    use crate::staging::group_basis::{hidden_table_width, route, target_table_width};
    let Some(shape) = route(def, source_columns) else {
        return Ok(());
    };
    check_table_width(
        || "the target table".to_string(),
        target_table_width(&shape),
    )?;
    check_table_width(
        || "the hidden table of the aggregate's partial sums".to_string(),
        hidden_table_width(&shape),
    )
}

/// The target-name and field-name rules every key-space shares.
fn validate_target_names(def: &TransformDef) -> Result<(), ValidationError> {
    if def.target == def.source {
        return Err(ValidationError::TargetEqualsSource {
            table: def.target.clone(),
        });
    }

    // A 1-1 target has a ledger named `<target>__ledger` beside it.
    if let Some(suffix) = super::ledger::RESERVED_TARGET_SUFFIXES
        .into_iter()
        .find(|suffix| def.target.ends_with(suffix))
    {
        return Err(ValidationError::ReservedTargetSuffix {
            target: def.target.clone(),
            suffix,
        });
    }
    if def.target.len() > super::ledger::MAX_TARGET_NAME_LEN {
        return Err(ValidationError::TargetNameTooLong {
            target: def.target.clone(),
            max: super::ledger::MAX_TARGET_NAME_LEN,
        });
    }

    // Checked ahead of every per-key-space rule, so a reserved
    // name is reported as such rather than as whatever shape error its
    // expression also happens to have.
    if let Some(field) = def
        .fields
        .iter()
        .find(|f| f.name.starts_with(RESERVED_COLUMN_PREFIX))
    {
        return Err(ValidationError::ReservedFieldName {
            field: field.name.clone(),
        });
    }
    Ok(())
}

/// One column of a `JOIN` definition's two tables, as the catalog reads it:
/// everything [`validate_join`] needs without touching the database. The
/// fields mirror what the join-key checks compare
/// (`catalog::JoinColumn`), plus the [`ValueType`] and whether the type is a
/// live enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinColumnInfo {
    pub value_type: ValueType,
    /// `format_type(atttypid, atttypmod)`, e.g. `numeric(10,2)`.
    pub pg_type: String,
    pub type_oid: u32,
    /// `atttypmod`: `-1` for no modifier.
    pub typmod: i32,
    pub collation_oid: u32,
    pub collation: Option<String>,
    pub collation_deterministic: bool,
    /// The type is a live enum, which is a text-stable join key too.
    pub is_enum: bool,
    /// The type a copy of the column is declared and cast as: no modifier,
    /// qualified ([`super::copy_type::type_sql`]).
    pub copy_type: String,
    /// The collation a copy declares (`collate <qualified name>`), `None`
    /// for the database default ([`super::copy_type::collation_sql`]).
    pub copy_collation: Option<String>,
    /// The column belongs to a table Trellis creates (a transform's target).
    pub trellis_target: bool,
}

/// The aggregate families a `JOIN` and a plain aggregate refuse, by name:
/// `(functions, family)`.
const DEFERRED_AGGREGATES: &[(&[&str], &str)] = &[
    (&["MIN", "MAX"], "MIN/MAX"),
    (
        &["BOOL_AND", "BOOL_OR", "BIT_AND", "BIT_OR"],
        "bool_and/bool_or/bit_and/bit_or",
    ),
    (
        &["ARRAY_AGG", "STRING_AGG", "JSONB_AGG"],
        "array_agg/string_agg/jsonb_agg",
    ),
];

/// Whether `name` is an aggregate the grammar parses only so the validator
/// can refuse it by family: a deferred name from [`DEFERRED_AGGREGATES`].
pub(crate) fn is_deferred_aggregate_name(name: &str) -> bool {
    DEFERRED_AGGREGATES
        .iter()
        .any(|(names, _)| names.contains(&name.to_ascii_uppercase().as_str()))
}

/// The one column argument of an aggregate call, as
/// [`check_aggregate_admitted`] needs it.
pub(crate) struct AggregateArg<'a> {
    pub column: &'a str,
    pub value_type: ValueType,
    /// `format_type` rendering, for the message.
    pub pg_type: &'a str,
    /// `atttypmod`; `-1` means no modifier, i.e. an unconstrained `numeric`.
    pub typmod: i32,
    /// The column is a column of a table Trellis creates.
    pub trellis_target: bool,
    /// The argument is an expression, not a plain column.
    pub expression: bool,
}

/// Where an aggregate call sits, which decides whether `MIN`/`MAX` is
/// admitted: a plain aggregate keeps them, a `JOIN` doesn't (#1117 rule 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateScope {
    /// A `GROUP BY` over one table.
    Plain,
    /// An aggregate `JOIN`.
    Join,
}

/// The per-family refusal of aggregates outside the admitted set: whether `function` (an
/// uppercased aggregate name) over `arg` is in the admitted set, `COUNT(*)`,
/// `COUNT(col)`, `SUM`/`AVG` over `smallint`/`integer`/`bigint`/`numeric`
/// (with or without a declared scale), and in a plain aggregate (`scope`)
/// `MIN`/`MAX` of a
/// plain source column of a type `registry::extreme_type_admitted` admits,
/// or else a named refusal. `arg` is `None` for `COUNT(*)` and for the
/// family check made before the argument is typed.
///
/// Shared so every key-space refuses the same way: a `JOIN` and a plain
/// aggregate both use it.
pub(crate) fn check_aggregate_admitted(
    function: &str,
    scope: AggregateScope,
    arg: Option<&AggregateArg<'_>>,
) -> Result<(), ValidationError> {
    let function = function.to_ascii_uppercase();
    let extreme = matches!(function.as_str(), "MIN" | "MAX");
    if let Some((_, family)) = DEFERRED_AGGREGATES
        .iter()
        .find(|(names, _)| names.contains(&function.as_str()))
        && !(extreme && scope == AggregateScope::Plain)
    {
        return Err(ValidationError::DeferredAggregate { function, family });
    }
    use super::registry::{AggregateAdmissionRule, TypeRule};
    let admitted = |rule| {
        super::floor::record(super::floor::Feature::TypeRule(
            TypeRule::AggregateAdmission(rule),
        ));
        Ok(())
    };
    match function.as_str() {
        "COUNT" => admitted(AggregateAdmissionRule::Count),
        "MIN" | "MAX" => {
            let Some(arg) = arg else { return Ok(()) };
            if arg.expression {
                Err(ValidationError::ExtremeOfExpression {
                    function,
                    argument: arg.column.to_string(),
                })
            } else if arg.trellis_target {
                Err(ValidationError::ChainedExtreme {
                    function,
                    column: arg.column.to_string(),
                })
            } else if super::registry::extreme_type_admitted(arg.value_type) {
                admitted(AggregateAdmissionRule::Extreme)
            } else {
                Err(ValidationError::UnsupportedExtremeArgument {
                    function,
                    argument: arg.column.to_string(),
                    pg_type: arg.pg_type.to_string(),
                })
            }
        }
        "SUM" | "AVG" => {
            let Some(arg) = arg else { return Ok(()) };
            match arg.value_type {
                ValueType::Integer(_) => admitted(AggregateAdmissionRule::SumAvgInteger),
                ValueType::Numeric => {
                    // Its partials keep a map of exceptions, a layout of
                    // their own.
                    super::floor::record(super::floor::Feature::Layout(
                        super::floor::Layout::NumericSums,
                    ));
                    admitted(if arg.typmod != -1 {
                        AggregateAdmissionRule::SumAvgScaledNumeric
                    } else {
                        AggregateAdmissionRule::SumAvgNumeric
                    })
                }
                ValueType::Float(_) | ValueType::Other(PgType::Interval) => {
                    Err(ValidationError::DeferredAggregate {
                        function,
                        family: "float and interval SUM/AVG",
                    })
                }
                _ => Err(ValidationError::UnsupportedAggregateArgument {
                    function,
                    column: arg.column.to_string(),
                    pg_type: arg.pg_type.to_string(),
                }),
            }
        }
        _ => Err(ValidationError::JoinFieldShape {
            field: function.clone(),
            detail: format!("{function} is not an aggregate this grammar knows"),
        }),
    }
}

/// Refuses a plain aggregate (`GROUP BY`) that calls
/// an aggregate outside the admitted set, by family
/// ([`check_aggregate_admitted`]): the group basis keeps every aggregate
/// definition, and has nothing for the rest. `columns` are the source's live
/// columns, whose declared modifier tells a `numeric(p,s)` argument from an
/// unconstrained one (admitted by different rules); an expression's
/// `numeric` result has no declared scale.
///
/// Any other key space passes.
pub(crate) fn validate_aggregate_admitted(
    def: &TransformDef,
    columns: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let KeySpace::Aggregate { .. } = &def.key_space else {
        return Ok(());
    };
    // Each field with the calculated fields it reads by name substituted, as
    // the group basis routes it: an argument that names a grouped calculated
    // field (`(qty + 1) AS q, SUM(q)`) is typed as that field's expression,
    // not as an unknown column. A definition substitution can't take is
    // refused by `validate` already.
    let Ok(substituted) = super::backfill::substituted_field_exprs(def) else {
        return Ok(());
    };
    for field in &def.fields {
        let expr = substituted.get(&field.name).unwrap_or(&field.expr);
        check_aggregate_calls(expr, columns)?;
    }
    Ok(())
}

/// [`validate`] over a table's live columns, then the aggregate family
/// check their declared modifiers make possible
/// ([`validate_aggregate_admitted`]): a plain definition's validation, as
/// [`validate_join`] is a `JOIN`'s. The catalog runs the two apart, with its
/// own refusals between them.
pub(crate) fn validate_live(
    def: &TransformDef,
    columns: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let value_types = columns
        .iter()
        .map(|(name, info)| (name.clone(), info.value_type))
        .collect();
    validate(def, &value_types)?;
    validate_aggregate_admitted(def, columns)
}

/// [`validate_aggregate_admitted`]'s walk of one field's expression.
fn check_aggregate_calls(
    expr: &Expr,
    columns: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    match expr {
        Expr::FunctionCall { name, args } if super::registry::is_aggregate_name(name) => {
            let upper = name.to_ascii_uppercase();
            check_aggregate_admitted(&upper, AggregateScope::Plain, None)?;
            let [arg] = args.as_slice() else {
                return Ok(());
            };
            // `validate` has typed the definition already; an argument it
            // can't type here is left to it.
            let Ok(value_type) = infer_expr_type_in("", arg, &ColumnTypes::Live(columns)) else {
                return Ok(());
            };
            check_aggregate_argument(&upper, AggregateScope::Plain, arg, value_type, columns)
        }
        _ => expr
            .children()
            .into_iter()
            .try_for_each(|child| check_aggregate_calls(child, columns)),
    }
}

/// [`check_aggregate_admitted`] for `function`'s one argument `arg`, typed
/// `value_type` over `columns` (the one table it reads): a lone column
/// carries its live type, declared modifier and owner; an expression has
/// its result type and no declared scale.
fn check_aggregate_argument(
    function: &str,
    scope: AggregateScope,
    arg: &Expr,
    value_type: ValueType,
    columns: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let column = match arg {
        Expr::Column(column) | Expr::SourceColumn { column, .. } => columns.get(column),
        _ => None,
    };
    let (pg_type, typmod, trellis_target) = match column {
        Some(info) => (info.pg_type.clone(), info.typmod, info.trellis_target),
        None => (super::ddl::pg_type_name(value_type).into_owned(), -1, false),
    };
    check_aggregate_admitted(
        function,
        scope,
        Some(&AggregateArg {
            column: &render_field_expr(arg),
            value_type,
            pg_type: &pg_type,
            typmod,
            trellis_target,
            expression: column.is_none(),
        }),
    )
}

fn written(column: &QualifiedColumn) -> String {
    match &column.schema {
        Some(schema) => format!("{schema}.{}.{}", column.table, column.column),
        None => format!("{}.{}", column.table, column.column),
    }
}

/// The resolver of a `JOIN` definition's qualified columns: the side each
/// belongs to and its live column, or the refusal naming what didn't resolve.
/// Shared by both `JOIN` validators. `root` is the `FROM` table's live
/// columns and `joined` each joined table's, by position.
pub(crate) fn join_resolver<'a>(
    def: &'a TransformDef,
    join: &'a JoinSpec,
    root: &'a HashMap<String, JoinColumnInfo>,
    joined: &'a [&'a HashMap<String, JoinColumnInfo>],
) -> impl Fn(&QualifiedColumn) -> Result<(Side, &'a JoinColumnInfo), ValidationError> + 'a {
    move |column: &QualifiedColumn| -> Result<(Side, &JoinColumnInfo), ValidationError> {
        let side = if column.table == def.source {
            Side::From
        } else if let Some((position, _)) =
            super::join::positions(join).find(|(_, table)| column.table == table.right)
        {
            Side::Joined(position)
        } else {
            return Err(ValidationError::JoinUnknownTable {
                reference: written(column),
            });
        };
        let schema_written = match side {
            Side::From => def.explicit_source_schema.as_deref(),
            Side::Joined(position) => super::join::joined(join, position)
                .explicit_right_schema
                .as_deref(),
        };
        if let (Some(used), Some(declared)) = (column.schema.as_deref(), schema_written)
            && used != declared
        {
            return Err(ValidationError::JoinUnknownTable {
                reference: written(column),
            });
        }
        let columns = side_columns(side, root, joined);
        columns
            .get(&column.column)
            .map(|info| (side, info))
            .ok_or_else(|| ValidationError::JoinUnresolvedColumn {
                reference: written(column),
            })
    }
}

/// `side`'s live columns: `root` for the `FROM` table, else its position's
/// entry of `joined`.
fn side_columns<'a>(
    side: Side,
    root: &'a HashMap<String, JoinColumnInfo>,
    joined: &'a [&'a HashMap<String, JoinColumnInfo>],
) -> &'a HashMap<String, JoinColumnInfo> {
    match side {
        Side::From => root,
        Side::Joined(position) => joined[position.index()],
    }
}

/// The rules every `JOIN` definition's tables and `ON` clause share, for the
/// aggregate and the 1-1 `JOIN` alike: the names are checked, the tables
/// differ, the target isn't the joined table, and each `ON` term is an
/// equality between one column of each table, of one type, modifier and
/// collation, with a text-stable key type.
pub(crate) fn validate_join_tables_and_on(
    def: &TransformDef,
    join: &JoinSpec,
    root: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
) -> Result<(), ValidationError> {
    validate_target_names(def)?;
    for table in &join.joins {
        if table.right == def.source {
            return Err(ValidationError::JoinSelf {
                table: def.source.clone(),
            });
        }
        // The joined table is a source too: writing the target into it would
        // feed Trellis's own writes back into capture.
        if def.target == table.right {
            return Err(ValidationError::TargetEqualsSource {
                table: def.target.clone(),
            });
        }
    }

    let resolve = join_resolver(def, join, root, joined);
    for (position, table) in super::join::positions(join) {
        validate_join_on(def, join, position, &table.on, &resolve)?;
    }
    Ok(())
}

/// The `ON` terms of the table joined at `position`: each an equality
/// between a column of the `FROM` table and one of that table (never of
/// another joined table), of one type, modifier and collation, with a
/// text-stable key type, each pair and each column named once.
fn validate_join_on<'a>(
    def: &TransformDef,
    join: &JoinSpec,
    position: super::join::Position,
    on: &'a [JoinOn],
    resolve: &impl Fn(&QualifiedColumn) -> Result<(Side, &'a JoinColumnInfo), ValidationError>,
) -> Result<(), ValidationError> {
    let check_key_type = |column: &QualifiedColumn, info: &JoinColumnInfo| {
        if !super::join_key_types::admit_join_key(info.type_oid, info.is_enum) {
            return Err(ValidationError::JoinUnsupportedKeyType {
                reference: written(column),
                pg_type: info.pg_type.clone(),
            });
        }
        if !info.collation_deterministic {
            return Err(ValidationError::JoinNondeterministicCollation {
                reference: written(column),
                collation: info.collation.clone().unwrap_or_default(),
            });
        }
        Ok(())
    };

    let mut pairs: HashSet<(&str, &str)> = HashSet::with_capacity(on.len());
    // A column named by two terms, each paired with another column, would
    // appear twice in a hidden table and in the target's key.
    let mut left_columns: HashSet<&str> = HashSet::with_capacity(on.len());
    let mut right_columns: HashSet<&str> = HashSet::with_capacity(on.len());
    for term in on {
        let (lhs_side, lhs) = resolve(&term.lhs)?;
        let (rhs_side, rhs) = resolve(&term.rhs)?;
        if lhs_side == rhs_side {
            return Err(ValidationError::JoinOnSameSide {
                term: format!("{} = {}", written(&term.lhs), written(&term.rhs)),
            });
        }
        // A star: every term pairs the `FROM` table with this position's.
        if let Some(other) = [lhs_side, rhs_side]
            .into_iter()
            .find(|side| !matches!(side, Side::From) && *side != Side::Joined(position))
        {
            return Err(ValidationError::JoinOnReadsAnotherJoinedTable {
                table: super::join::joined(join, position).right.clone(),
                term: format!("{} = {}", written(&term.lhs), written(&term.rhs)),
                other: super::join::side_table(def, join, other).to_string(),
            });
        }
        let pair = match lhs_side {
            Side::From => (term.lhs.column.as_str(), term.rhs.column.as_str()),
            Side::Joined(_) => (term.rhs.column.as_str(), term.lhs.column.as_str()),
        };
        if !pairs.insert(pair) {
            return Err(ValidationError::JoinOnRepeatedPair {
                term: format!("{} = {}", written(&term.lhs), written(&term.rhs)),
            });
        }
        let (left_column, right_column) = match lhs_side {
            Side::From => (&term.lhs, &term.rhs),
            Side::Joined(_) => (&term.rhs, &term.lhs),
        };
        for (seen, column) in [
            (&mut left_columns, left_column),
            (&mut right_columns, right_column),
        ] {
            if !seen.insert(column.column.as_str()) {
                return Err(ValidationError::JoinOnRepeatedColumn {
                    term: format!("{} = {}", written(&term.lhs), written(&term.rhs)),
                    column: written(column),
                });
            }
        }
        check_key_type(&term.lhs, lhs)?;
        check_key_type(&term.rhs, rhs)?;
        if !super::pg_type::columns_pair(
            (lhs.type_oid, lhs.collation_oid),
            (rhs.type_oid, rhs.collation_oid),
        ) {
            let with_collation = lhs.collation_oid != rhs.collation_oid;
            let describe = |info: &JoinColumnInfo| match (&info.collation, with_collation) {
                (Some(collation), true) => format!("{} COLLATE \"{collation}\"", info.pg_type),
                _ => info.pg_type.clone(),
            };
            return Err(ValidationError::JoinKeyTypeMismatch {
                lhs: written(&term.lhs),
                lhs_type: describe(lhs),
                rhs: written(&term.rhs),
                rhs_type: describe(rhs),
            });
        }
    }
    // Each side names every `ON` column once (a repeat is refused above), and
    // the hidden tables key and index a side on its `ON` columns.
    check_index_key_width(|| "each side of ON".to_string(), on.len())?;

    Ok(())
}

/// Validates a [`KeySpace::Join`] or [`KeySpace::JoinOneToOne`] definition
/// against the live columns of its tables (`left` is the `FROM` table's,
/// `joined` each joined table's, by position: one per `JOIN` written).
///
/// An aggregate join is admitted when: it joins one table; the tables differ;
/// every column is qualified and exists; each `ON` term is an equality between
/// one column of each table, of one type, modifier and collation (the
/// join-key rules); every `GROUP BY` key, from either table, is an admitted
/// key type; and each field is a key passthrough or composes admitted
/// aggregates (`COUNT(*)`, and `COUNT`/`SUM`/`AVG` of a column or an
/// expression over one table's columns, per [`check_aggregate_admitted`])
/// with literals, operators and scalar functions. An argument reading both
/// tables is refused as [`ValidationError::JoinMixedSideArgument`]. A 1-1
/// join is [`super::validate_join_one_to_one::validate`]'s.
pub fn validate_join(
    def: &TransformDef,
    left: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
) -> Result<(), ValidationError> {
    if let KeySpace::JoinOneToOne(_) = &def.key_space {
        return super::validate_join_one_to_one::validate(def, left, joined);
    }
    let KeySpace::Join(join) = &def.key_space else {
        return Ok(());
    };
    // An aggregate JOIN joins exactly two tables (ADR-0019).
    if join.joins.len() > 1 {
        return Err(ValidationError::JoinTooManyTables);
    }
    validate_join_tables_and_on(def, join, left, joined)?;
    let resolve = join_resolver(def, join, left, joined);

    // A key is qualified and resolves through either table, whose
    // collation is checked with it. Its column name is the target's and
    // `T`'s.
    let key_columns = check_group_by_keys(join.group_by.iter().map(|key| {
        let (_, info) = resolve(key)?;
        Ok(GroupByKey {
            written: written(key),
            column: &key.column,
            value_type: info.value_type,
            collation: Some((&key.table, info)),
        })
    }))?;
    // A side's hidden partials table is keyed by its `ON` columns, then its
    // `GROUP BY` columns not among them: wider than either clause alone.
    for side in super::join::sides(join) {
        let table = super::join::side_table(def, join, side);
        check_index_key_width(
            || format!("{table}'s key, its ON and GROUP BY columns together,"),
            super::join::side_key_columns(def, side).len(),
        )?;
    }

    let mut field_names = HashSet::with_capacity(def.fields.len());
    for field in &def.fields {
        if !field_names.insert(field.name.as_str()) {
            return Err(ValidationError::DuplicateFieldName {
                name: field.name.clone(),
            });
        }
        let shape = |detail: &str| ValidationError::JoinFieldShape {
            field: field.name.clone(),
            detail: detail.to_string(),
        };
        // A GROUP BY key selected on its own, under its column name.
        if let Expr::SourceColumn {
            schema,
            table,
            column,
        } = &field.expr
        {
            let reference = QualifiedColumn {
                schema: schema.clone(),
                table: table.clone(),
                column: column.clone(),
            };
            let (side, _) = resolve(&reference)?;
            let is_key = join
                .group_by
                .iter()
                .any(|key| key.column == *column && resolve(key).ok().map(|k| k.0) == Some(side));
            if !is_key {
                return Err(shape(
                    "a column outside an aggregate must be one of the GROUP BY keys; \
                     aggregate anything else",
                ));
            }
            if field.name != *column {
                return Err(shape(
                    "a GROUP BY key is selected under its own column name",
                ));
            }
            continue;
        }
        if key_columns.contains(field.name.as_str()) {
            return Err(shape(
                "only the GROUP BY key itself can take the name of a GROUP BY key's column",
            ));
        }
        let mut aggregates = 0;
        check_join_field_expr(
            &field.expr,
            &field.name,
            &resolve,
            None,
            left,
            joined,
            &mut aggregates,
        )?;
        if aggregates == 0 {
            return Err(shape(
                "a field is a GROUP BY key or composes at least one aggregate",
            ));
        }
        // The composed field's type, over both tables' columns: each column
        // is typed by its own side, since the two tables may share names.
        join_field_value_type(def, field, left, joined)?;
    }
    check_join_collation_reads(def, join, left, joined)?;
    // The target and each hidden table carry columns per field (two per
    // distinct aggregate argument in the partials).
    for (table, columns) in super::join::table_widths(def, left, joined[0]) {
        check_table_width(|| table, columns)?;
    }
    Ok(())
}

/// One column-read side of an aggregate argument, for the mixed-side check.
type ResolveJoinColumn<'a> =
    dyn Fn(&QualifiedColumn) -> Result<(Side, &'a JoinColumnInfo), ValidationError> + 'a;

/// Walks one `JOIN` field's expression. Outside an aggregate (`in_aggregate`
/// is `None`) it admits literals, operators and scalar functions over
/// aggregates; a column there must be a lone `GROUP BY` key (checked by the
/// caller), so any other is refused. Inside an aggregate's argument it
/// collects which sides the argument reads, so the caller can refuse a
/// mixed-side argument by name. Every aggregate is checked against
/// [`check_aggregate_admitted`] and counted in `aggregates`.
fn check_join_field_expr<'a>(
    expr: &Expr,
    field: &str,
    resolve: &ResolveJoinColumn<'a>,
    mut in_aggregate: Option<&mut Vec<Side>>,
    root: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
    aggregates: &mut usize,
) -> Result<(), ValidationError> {
    let shape = |detail: &str| ValidationError::JoinFieldShape {
        field: field.to_string(),
        detail: detail.to_string(),
    };
    match expr {
        Expr::Column(column) => Err(ValidationError::JoinUnqualifiedColumn {
            field: field.to_string(),
            column: column.clone(),
        }),
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => {
            let (side, _) = resolve(&QualifiedColumn {
                schema: schema.clone(),
                table: table.clone(),
                column: column.clone(),
            })?;
            match in_aggregate {
                Some(sides) => {
                    sides.push(side);
                    Ok(())
                }
                None => Err(shape(
                    "a column outside an aggregate must be a GROUP BY key selected on its own; \
                     aggregate anything else",
                )),
            }
        }
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BooleanLiteral(_)
        | Expr::Null
        | Expr::TypedLiteral { .. } => Ok(()),
        Expr::FunctionCall { name, args } if super::registry::is_aggregate_name(name) => {
            let upper = name.to_ascii_uppercase();
            if in_aggregate.is_some() {
                return Err(shape("an aggregate can't be nested in another aggregate"));
            }
            check_aggregate_admitted(&upper, AggregateScope::Join, None)?;
            *aggregates += 1;
            let arg = match args.as_slice() {
                [] if upper == "COUNT" => return Ok(()),
                [arg] => arg,
                _ => return Err(shape("an aggregate takes one argument (or * for COUNT)")),
            };
            let mut sides = Vec::new();
            check_join_field_expr(
                arg,
                field,
                resolve,
                Some(&mut sides),
                root,
                joined,
                aggregates,
            )?;
            let side = match sides.first() {
                None => {
                    return Err(shape(
                        "an aggregate's argument reads a column of one of the two tables",
                    ));
                }
                Some(side) if sides.iter().any(|other| other != side) => {
                    return Err(ValidationError::JoinMixedSideArgument {
                        field: field.to_string(),
                        argument: format!("{upper}({})", render_field_expr(arg)),
                    });
                }
                Some(side) => *side,
            };
            // The argument is typed over its one side. A column it reads
            // resolved there above, and a bare one was refused.
            let columns = side_columns(side, root, joined);
            let value_type = infer_expr_type_in(field, arg, &ColumnTypes::Live(columns))?;
            check_aggregate_argument(&upper, AggregateScope::Join, arg, value_type, columns)
        }
        Expr::FunctionCall { .. }
        | Expr::BinaryOp { .. }
        | Expr::Negate(_)
        | Expr::Cast { .. }
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::Case { .. } => {
            for child in expr.children() {
                check_join_field_expr(
                    child,
                    field,
                    resolve,
                    in_aggregate.as_deref_mut(),
                    root,
                    joined,
                    aggregates,
                )?;
            }
            Ok(())
        }
    }
}

/// The type of a `JOIN` definition's field that isn't a `GROUP BY` key, as
/// [`validate_join`] infers it: each column typed by its own side
/// ([`ColumnTypes::Join`]). The field is typed on its own: a `JOIN`'s field
/// reads no other field by name. Only meaningful for a definition
/// [`validate_join`] accepts.
pub(crate) fn join_field_value_type(
    def: &TransformDef,
    field: &super::ast::FieldDef,
    root: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
) -> Result<ValueType, ValidationError> {
    let columns = ColumnTypes::Join { def, root, joined };
    let types_by_field = infer_fields(std::slice::from_ref(field), &columns)?;
    Ok(types_by_field
        .get(&field.name)
        .copied()
        .unwrap_or(ValueType::Numeric))
}

/// Checks every qualified [`Expr::SourceColumn`] in `def`: it
/// names the `FROM` table, a schema that matches `source_schema` when one is
/// known (written in the `FROM` clause, or resolved by the catalog), and a
/// column the source has.
pub(crate) fn validate_qualified_columns(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_schema: Option<&str>,
) -> Result<(), ValidationError> {
    fn walk(
        expr: &Expr,
        field: &str,
        def: &TransformDef,
        source_columns: &HashMap<String, ValueType>,
        source_schema: Option<&str>,
    ) -> Result<(), ValidationError> {
        match expr {
            Expr::SourceColumn {
                schema,
                table,
                column,
            } => {
                let wrong_schema = matches!(
                    (schema.as_deref(), source_schema),
                    (Some(written), Some(actual)) if written != actual
                );
                if table != &def.source || wrong_schema {
                    return Err(ValidationError::QualifiedColumnNotFromSource {
                        field: field.to_string(),
                        reference: render_field_expr(expr),
                        source: match source_schema {
                            Some(actual) => format!("{actual}.{}", def.source),
                            None => def.source.clone(),
                        },
                    });
                }
                if !source_columns.contains_key(column) {
                    return Err(ValidationError::UnresolvedColumn {
                        field: field.to_string(),
                        column: render_field_expr(expr),
                    });
                }
                Ok(())
            }
            Expr::Column(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::Null
            | Expr::TypedLiteral { .. } => Ok(()),
            Expr::BinaryOp { .. }
            | Expr::FunctionCall { .. }
            | Expr::Negate(_)
            | Expr::Cast { .. }
            | Expr::Not(_)
            | Expr::IsNull { .. }
            | Expr::Case { .. } => expr
                .children()
                .into_iter()
                .try_for_each(|child| walk(child, field, def, source_columns, source_schema)),
        }
    }
    def.fields
        .iter()
        .try_for_each(|f| walk(&f.expr, &f.name, def, source_columns, source_schema))
}

/// Refuses a bare name that could mean either a source column or a field:
/// a name that is a source column and also a field whose
/// expression is not just that column (`(grp * 2) AS val` over a source with
/// a `val` column), read by any other field.
///
/// Two reads of such a name are not ambiguous, and keep their meaning:
/// * a field reading its own name reads the source column (`SUM(val) AS
///   val`), as it always has, since a field can't read itself;
/// * an aggregate field's name inside another aggregate's argument is the
///   source column ([`aggregate_field_names`]).
///
/// A field named after the column it passes through (`val AS val`) shadows
/// nothing, so it is never the other meaning.
fn reject_ambiguous_names(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), ValidationError> {
    let shadowing: HashMap<&str, &FieldDef> = def
        .fields
        .iter()
        .filter(|f| source_columns.contains_key(&f.name) && !f.expr.reads_source_column(&f.name))
        .map(|f| (f.name.as_str(), f))
        .collect();
    if shadowing.is_empty() {
        return Ok(());
    }
    let aggregates = aggregate_field_names(def.fields.iter().map(|f| (f.name.as_str(), &f.expr)));

    for reader in &def.fields {
        let mut reads = Vec::new();
        collect_reads(
            &reader.expr,
            &aggregates,
            false,
            &mut reads,
            &mut Vec::new(),
        );
        if let Some(name) = reads
            .iter()
            .find(|name| **name != reader.name && shadowing.contains_key(name.as_str()))
        {
            return Err(ValidationError::AmbiguousColumnReference(Box::new(
                AmbiguousColumnReference {
                    field: reader.name.clone(),
                    name: name.to_string(),
                    shadowing_expr: render_field_expr(&shadowing[name.as_str()].expr),
                    source_table: def.source.clone(),
                    source_schema: def.explicit_source_schema.clone(),
                },
            )));
        }
    }
    Ok(())
}

/// The name prefix reserved for the hidden columns Trellis keeps on a target
/// table. A user field or `GROUP BY` column may not start with it.
pub const RESERVED_COLUMN_PREFIX: &str = "__";

/// The most columns Postgres allows in one index (`INDEX_MAX_KEYS`, a
/// compile-time constant of the server that every supported version builds
/// with its default of 32). Trellis keys its aggregate and `JOIN` tables on a
/// definition's `GROUP BY` and `ON` columns, so a key wider than this fails
/// `DefineIndex` with `54011`; it is refused at validation instead.
pub const INDEX_MAX_KEYS: usize = 32;

/// Refuses an index key of `columns` columns, described by `clause`, when it
/// is wider than [`INDEX_MAX_KEYS`].
pub(crate) fn check_index_key_width(
    clause: impl FnOnce() -> String,
    columns: usize,
) -> Result<(), ValidationError> {
    if columns > INDEX_MAX_KEYS {
        return Err(ValidationError::IndexKeyTooWide {
            clause: clause(),
            columns,
            limit: INDEX_MAX_KEYS,
        });
    }
    Ok(())
}

/// The most columns Postgres allows in one table (`MaxHeapAttributeNumber`,
/// 1600, a compile-time constant of the server). A dropped column keeps
/// counting, and a system column does not. Every table Trellis creates for a
/// definition is held to it: a definition whose target or hidden table would
/// be wider fails `CREATE TABLE` with `54011`, so it is refused at validation
/// instead.
pub const TABLE_MAX_COLUMNS: usize = 1600;

/// Refuses a table of `columns` columns, described by `table`, when it is
/// wider than [`TABLE_MAX_COLUMNS`].
pub(crate) fn check_table_width(
    table: impl FnOnce() -> String,
    columns: usize,
) -> Result<(), ValidationError> {
    if columns > TABLE_MAX_COLUMNS {
        return Err(ValidationError::TableTooWide {
            table: table(),
            columns,
            limit: TABLE_MAX_COLUMNS,
            dropped: 0,
        });
    }
    Ok(())
}

/// Rejects any of a 1-1 definition's `fields` named after one of the
/// source's `primary_key` columns. The target already carries
/// every key column as its own key, so such a field would be declared a second
/// time, and install used to fail with a raw Postgres duplicate-column error.
/// `id AS id` is refused too, not quietly treated as the key column: the
/// field adds nothing, and refusing it keeps the stored definition exactly
/// what was written. A key column under another name (`id AS order_id`) is an
/// ordinary column and passes.
///
/// [`validate`] can't run this itself, since it doesn't know the source's
/// key; the catalog calls it at install and for an `ALTER TRANSFORM`'s added
/// fields, before [`validate`], so this error wins over
/// [`ValidationError::CalculatedFieldShadowsSourceColumn`], whose advice (use
/// a passthrough) would be wrong for a key column.
pub fn reject_primary_key_named_fields(
    fields: &[FieldDef],
    primary_key: &[&str],
) -> Result<(), ValidationError> {
    match fields
        .iter()
        .find(|f| primary_key.contains(&f.name.as_str()))
    {
        Some(field) => Err(ValidationError::FieldNamedAfterPrimaryKey {
            field: field.name.clone(),
        }),
        None => Ok(()),
    }
}

/// Walks a [`KeySpace::Aggregate`] field's expression, rejecting a bare
/// (not wrapped in `SUM`/`MIN`/`MAX`/`AVG`) reference to a source column
/// that isn't a grouping key. `in_aggregate_call` tracks whether the current
/// position is already inside such a call; only a real source column
/// reference is checked — a reference to another calculated field (already
/// a single value per group by the time it's used) is unaffected, the same
/// as inter-field composition in a 1-1 definition.
fn validate_aggregate_field_expr(
    expr: &Expr,
    field_name: &str,
    group_by: &[String],
    source_columns: &HashMap<String, ValueType>,
    in_aggregate_call: bool,
) -> Result<(), ValidationError> {
    match expr {
        Expr::Column(name) => {
            let is_group_key = group_by.contains(name);
            if source_columns.contains_key(name) && !is_group_key && !in_aggregate_call {
                return Err(ValidationError::UngroupedColumnReference {
                    field: field_name.to_string(),
                    column: name.clone(),
                });
            }
            Ok(())
        }
        // A qualified name is a source column even when a field
        // has its name, so the same rule applies without the field lookup.
        Expr::SourceColumn { column, .. } => {
            let is_group_key = group_by.contains(column);
            if !is_group_key && !in_aggregate_call {
                return Err(ValidationError::UngroupedColumnReference {
                    field: field_name.to_string(),
                    column: render_field_expr(expr),
                });
            }
            Ok(())
        }
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BooleanLiteral(_)
        | Expr::Null
        | Expr::TypedLiteral { .. } => Ok(()),
        Expr::BinaryOp { .. }
        | Expr::FunctionCall { .. }
        | Expr::Negate(_)
        | Expr::Cast { .. }
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::Case { .. } => {
            let in_aggregate_call = in_aggregate_call
                || matches!(expr, Expr::FunctionCall { name, .. }
                    if super::registry::is_aggregate_name(name));
            expr.children().into_iter().try_for_each(|child| {
                validate_aggregate_field_expr(
                    child,
                    field_name,
                    group_by,
                    source_columns,
                    in_aggregate_call,
                )
            })
        }
    }
}

/// Whether `expr` is exactly the passthrough shape `GROUP BY` key `key`
/// requires of a field named after it (see
/// [`ValidationError::GroupingColumnFieldMustBePassthrough`]): a
/// read of the key's column, bare or qualified with the `FROM` table
/// ([`Expr::reads_source_column`]) — there is no other expression shape
/// DDL generation gives a target column of its own for that name, so
/// anything else is rejected rather than silently dropped.
fn expr_is_group_by_key_passthrough(expr: &Expr, key: &str) -> bool {
    expr.reads_source_column(key)
}

/// Whether `expr` contains an aggregate call (`SUM`, `COUNT`, ...).
fn has_aggregate_call(expr: &Expr) -> bool {
    let mut found = false;
    expr.walk(&mut |e| {
        found |= matches!(e, Expr::FunctionCall { name, .. } if super::registry::is_aggregate_name(name));
    });
    found
}

/// The names of the calculated fields whose value is an aggregate: the ones
/// that contain an aggregate call, and the ones that read another such field
/// by name, however indirectly.
///
/// An aggregate's argument never reads one of these by name. Folding an
/// aggregate's value is not something another aggregate can do, so a name
/// that matches one of them inside an aggregate's argument is a source
/// column, as in SQL: `SUM(val) AS val, MIN(val) AS lo` takes the minimum of
/// the source column `val`, not of the sum. A field that is not an aggregate
/// (`(id + 1) AS adj, SUM(adj)`) is still read by name there.
pub(crate) fn aggregate_field_names<'a>(
    fields: impl IntoIterator<Item = (&'a str, &'a Expr)>,
) -> HashSet<&'a str> {
    let fields: Vec<(&str, &Expr)> = fields.into_iter().collect();
    let mut aggregates: HashSet<&str> = fields
        .iter()
        .filter(|(_, expr)| has_aggregate_call(expr))
        .map(|(name, _)| *name)
        .collect();
    loop {
        let mut grew = false;
        for (name, expr) in &fields {
            if aggregates.contains(name) {
                continue;
            }
            if expr
                .columns()
                .into_iter()
                .any(|read| read != *name && aggregates.contains(read))
            {
                aggregates.insert(name);
                grew = true;
            }
        }
        if !grew {
            return aggregates;
        }
    }
}

/// `fields` without the aggregate fields: what an aggregate's argument can
/// read by name (see [`aggregate_field_names`]).
pub(crate) fn without_aggregate_fields<'a, V: Copy>(
    fields: &HashMap<&'a str, V>,
    aggregates: &HashSet<&str>,
) -> HashMap<&'a str, V> {
    fields
        .iter()
        .filter(|(name, _)| !aggregates.contains(**name))
        .map(|(name, value)| (*name, *value))
        .collect()
}

/// [`Expr::columns`] split by what each column name resolves to. `fields`
/// gets the names read as another calculated field or as a source column
/// (the caller tells them apart); `aggregate_args` gets the names that sit
/// in an aggregate's argument and match an aggregate field, which resolve to
/// source columns (see [`aggregate_field_names`]).
pub(crate) fn collect_reads(
    expr: &Expr,
    aggregates: &HashSet<&str>,
    in_aggregate_arg: bool,
    fields: &mut Vec<String>,
    aggregate_args: &mut Vec<String>,
) {
    match expr {
        Expr::Column(name) if in_aggregate_arg && aggregates.contains(name.as_str()) => {
            aggregate_args.push(name.clone());
        }
        Expr::Column(name) => fields.push(name.clone()),
        // A `SourceColumn` is never a field; `validate` checks it names a
        // source column through `validate_qualified_columns`.
        _ => {
            let in_arg = in_aggregate_arg
                || matches!(expr, Expr::FunctionCall { name, .. }
                    if super::registry::is_aggregate_name(name));
            for child in expr.children() {
                collect_reads(child, aggregates, in_arg, fields, aggregate_args);
            }
        }
    }
}

/// Infers every calculated field's [`ValueType`], type-checking each
/// operator's operands along the way. Reused by [`super::ddl`] to pick each
/// target column's Postgres type, since a field's inferred type *is* its
/// target column's declared type — this grammar has no separate "declare a
/// target column's type" syntax.
///
/// Assumes `def`'s column references already resolved (this module's own
/// [`validate`] checks that first) and that `def` is acyclic; a definition
/// that reaches this function without either guarantee having been checked
/// gets a `Cycle` refusal as defense in depth, rather than overflowing the
/// stack.
/// Rejects an [`super::ast::KeySpace::Aggregate`] `GROUP BY` key whose
/// column is typed [`ValueType::Other`] — see
/// [`ValidationError::UnsupportedGroupByKeyType`] for why a key role needs
/// more than the passthrough classification.
pub(crate) fn reject_unsupported_group_by_key_type(
    column: &str,
    value_type: ValueType,
) -> Result<(), ValidationError> {
    match group_by_key_rule(value_type) {
        Some(rule) => {
            // The case that admits the key counts toward the definition's
            // floor, so the validator can't admit a key type without one.
            super::floor::record(super::floor::Feature::TypeRule(
                super::registry::TypeRule::GroupByKey(rule),
            ));
            Ok(())
        }
        None => Err(ValidationError::UnsupportedGroupByKeyType {
            column: column.to_string(),
            value_type,
        }),
    }
}

/// The [`GroupByKeyRule`] that admits a `GROUP BY` key of `value_type`, or
/// `None` when the gate refuses it. Each admitted type names the rule whose
/// release it arrived in; [`reject_unsupported_group_by_key_type`] records it.
fn group_by_key_rule(value_type: ValueType) -> Option<super::registry::GroupByKeyRule> {
    match value_type {
        // An exact integer's text rendering is canonical (no
        // leading zeros, no `+`, no whitespace), so it round-trips through
        // the still-text-keyed group-key path byte-for-byte — the same
        // property that already puts `smallint`/`integer`/`bigint` on
        // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`.
        //
        // `Numeric` is admitted here as it always has been, and that is
        // knowingly weaker: `1.0` and `1.00` are equal to Postgres but
        // distinct under `::text` matching, which is exactly why `numeric`
        // is *rejected* as a `JOIN` key and as a primary key.
        // Tightening the `GROUP BY` gate to match belongs with the
        // typed key index, not here.
        ValueType::Integer(_) => Some(super::registry::GroupByKeyRule::Integer),
        ValueType::Numeric => Some(super::registry::GroupByKeyRule::Numeric),
        ValueType::Text => Some(super::registry::GroupByKeyRule::Text),
        ValueType::Uuid => Some(super::registry::GroupByKeyRule::Uuid),
        // `boolean` was already admitted here (this arm
        // predates the issue), and it is genuinely safe, but not for the
        // reason every other admission on this function is — this is the
        // one type where the *raw-text* premise the function's own error
        // message states ("GROUP BY keys are matched by their text
        // rendering") is not actually what happens underneath.
        //
        // `boolean` is the only type with a `pg_cast`-registered `::text`
        // function distinct from its output function (`pg_catalog.text
        // (boolean)` renders `'true'`/`'false'`; `boolout` — what a capture
        // trigger's `format('%s', col)` writes into the ring — renders
        // `'t'`/`'f'`), so a `GROUP BY` key genuinely does have two
        // possible spellings depending on which code path produced it. See
        // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment for the live
        // `pg_cast` evidence and why that divergence is exactly why
        // `boolean` is *not* on that allowlist (the join/primary-key role
        // really does raw-text `{col}::text = $1` matching in places).
        //
        // What saves the `GROUP BY` role is that the group basis
        // (`staging::group_basis`) never matches a group by its text: a
        // change's group is parsed from the image's text into the source's
        // typed column (`jsonb_populate_record`), through `boolin` — a permissive *input* function that
        // accepts both `'t'`/`'f'` and `'true'`/`'false'` — and groups are
        // then matched natively, in SQL. Two renderings of one value always
        // resolve to the same group. `trellis/tests/defs_boolean.rs`'s
        // `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
        // pins it.
        ValueType::Boolean => Some(super::registry::GroupByKeyRule::Boolean),
        // `real`/`double precision` are rejected outright, and
        // this is a deliberate *tightening* — before the float split they
        // reached here as `ValueType::Numeric` and were waved through.
        //
        // Postgres's own float `=` is perfectly well-defined as a grouping
        // predicate (it is a total order: `NaN = NaN` is true, `-0 = 0` is
        // true), so the problem is not the type — it is that this path
        // still matches keys by raw `::text`, and float text is not stable
        // under that equality. `-0` and `0` are one value with two
        // renderings, so a text-keyed `GROUP BY` would split one Postgres
        // group into two target rows, and the ADR-0013 self-check would
        // (correctly) report a divergence against a server-side `GROUP BY`.
        // Admitting floats here is the typed key index's to grant, by
        // comparing decoded values; until
        // then, rejecting is the honest answer and `numeric` is the
        // steer-to. See `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`.
        ValueType::Float(_) => None,
        // `oid` is one `Other` family admitted as a key:
        // Postgres renders it as canonical unsigned decimal, so it is
        // text-stable in exactly the way `interval` is not. It stays an
        // `Other` rather than joining `ValueType::Integer` because Postgres
        // gives it no arithmetic at all — see `pg_type::PgType::Oid`.
        ValueType::Other(PgType::Oid) => Some(super::registry::GroupByKeyRule::Oid),
        // `bytea` is the other, on the same "text-stability is
        // a property of the rendering, not the operator set" reasoning:
        // `byteaout` under the pinned `bytea_output = 'hex'` is a bijection
        // on every byte string, with no session-GUC dependence at all (see
        // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment for the live
        // evidence). Unlike the temporal families it needs no per-value
        // predicate here — every `bytea` value clears the bar, not just a
        // subset — so it is a plain admit rather than a call into a sibling
        // module.
        ValueType::Other(PgType::Bytea) => Some(super::registry::GroupByKeyRule::Bytea),
        // `date`, `timestamp`, `time`, `timetz` and
        // `timestamptz` join `oid` on the same grounds, gated by
        // `crate::temporal`'s own per-family verdict rather than by a list
        // repeated here — see that module's doc comment for the live
        // evidence, and `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` for the
        // join-key/PK half of the same decision. `timestamp` is admitted
        // because its rendering is consistent across renderers;
        // `timestamptz` additionally needs a pinned `TimeZone` — its
        // rendering is `TimeZone`-dependent, so every renderer must pin
        // the same `TimeZone` (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`,
        // pinned on the pool and in every capture function).
        //
        // `interval` is the one temporal family still rejected here, for a
        // reason no GUC or renderer reconciliation touches: it has no
        // canonical rendering at all — `'24 hours'` and `'1 day'` are `=`
        // and render differently, so a text-matched `GROUP BY` would split
        // one Postgres group in two, exactly as a float `-0`/`0` key would.
        ValueType::Other(pg_type) if crate::temporal::is_text_stable(pg_type) => match pg_type {
            PgType::Date => Some(super::registry::GroupByKeyRule::Date),
            PgType::Time => Some(super::registry::GroupByKeyRule::Time),
            PgType::TimeTz => Some(super::registry::GroupByKeyRule::TimeTz),
            PgType::Timestamp => Some(super::registry::GroupByKeyRule::Timestamp),
            PgType::TimestampTz => Some(super::registry::GroupByKeyRule::TimestampTz),
            // A temporal family without a rule is refused: admitting it
            // takes a release.
            _ => None,
        },
        // `cidr`/`macaddr`/`macaddr8` join `oid`/`bytea` above —
        // each is on `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` (see that
        // constant's doc comment for the live `pg_cast`/renderer evidence),
        // and unlike the temporal families every value of each clears the
        // bar, not just a subset, so a plain admit is enough here too.
        ValueType::Other(PgType::Cidr) => Some(super::registry::GroupByKeyRule::Cidr),
        ValueType::Other(PgType::MacAddr) => Some(super::registry::GroupByKeyRule::MacAddr),
        ValueType::Other(PgType::MacAddr8) => Some(super::registry::GroupByKeyRule::MacAddr8),
        // `inet` is admitted here despite being deliberately *absent* from
        // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` — the same split
        // `boolean` has, for the same mechanical reason.
        //
        // `inet_out` (what a capture trigger's `format('%s', col)` writes
        // into the ring) and `network_show` (`<col>::text` — what this engine's
        // own live reads use) disagree on a bare-host `inet`
        // value's spelling (`192.168.1.5` vs `192.168.1.5/32` — see
        // `crate::netaddr`'s module doc for the live grid), the identical
        // "one value, two renderers" shape that keeps `inet` off the
        // join/primary-key allowlist. That divergence does not reach the
        // `GROUP BY` role, because the group basis (`staging::group_basis`)
        // never compares a group key's text — it casts it to the column's native
        // type, and `inet_in` is permissive enough to parse both spellings
        // back to the identical stored value (`'192.168.1.5'::inet =
        // '192.168.1.5/32'::inet` is `true`, verified live).
        ValueType::Other(PgType::Inet) => Some(super::registry::GroupByKeyRule::Inet),
        // `bit varying` is safe here for a *different* reason
        // than every arm above it — it isn't about `::text`-vs-native-`=`
        // rendering agreement at all. `bit`/`varbit` have no `pg_cast`
        // override the way `boolean` does (verified live; see
        // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment), so raw-text
        // matching was never the concern.
        //
        // The hazard here is purely a DDL one:
        // `ddl::create_aggregate_target_table` declares a `GROUP BY` key's
        // own target column from bare `ValueType` alone
        // (`ddl::pg_type_name`), with no per-column length carried anywhere
        // (`PgType::Bit`/`PgType::VarBit` are unit variants — no typmod).
        // For almost every family admitted above, a bare/unmodified
        // Postgres type name is a *superset* of any concretely-typed source
        // column (bare `numeric`, bare `text`: unconstrained, never
        // narrower). Fixed-length `bit` is the one family on this whole
        // gate where that assumption is false: Postgres's own default
        // typmod for a **fixed-length** `bit` column with no length given
        // is `bit(1)`, not "unconstrained" — verified live (`create table
        // t(x bit)` then `\d t` shows `x bit(1)`). A `bit(5)` source
        // column's `GROUP BY` key value would then fail to write into the
        // `bit(1)` column `pg_type_name(Other(Bit))` creates for it, on the
        // very first apply (`insert into t values ('101')` against a bare
        // `x bit` column raises `bit string length 3 does not match type
        // bit(1)` on a live server) — every group, not an edge case.
        //
        // `bit varying`'s bare default has no such trap: `bit varying` with
        // no length is genuinely unconstrained, losslessly holding any
        // width (verified live: `'10100101'::bit varying` keeps all 8
        // bits), so `pg_type_name(Other(VarBit))` is exactly as safe a
        // brand-new-column type here as bare `numeric`/`text` already are.
        // Fixed-length `bit` is *not* refused for any text-rendering
        // reason — it is perfectly safe as a `JOIN` key or a 1-1
        // primary key (`join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`), neither of
        // which ever asks a bare `ValueType` to conjure a new column: a
        // join key only ever compares against an already-existing column,
        // and a 1-1 primary key copies the source's own *concrete*
        // introspected type (`bit(5)`, not bare `bit`) verbatim
        // (`ddl::source_primary_key`). It is refused *only* here, and only
        // because this one role's DDL has no concrete-type-introspection
        // escape hatch to reach for. `trellis/tests/defs_bit.rs` pins both
        // halves of this split live.
        ValueType::Other(PgType::VarBit) => Some(super::registry::GroupByKeyRule::VarBit),
        // `jsonb` falls through to the reject arm below like
        // every other unlisted `Other` family, and — unlike `bit`/`oid`/
        // `bytea` — this is *not* a rendering-vs-key-order gap that turned
        // out to be already closed. `jsonb_out` genuinely does
        // canonicalize object key order (verified live), but that is
        // independent of a second, real hazard: an embedded JSON number
        // preserves its literal input scale (`numeric`'s own rendering),
        // so `'{"a":1}'::jsonb` and `'{"a":1.0}'::jsonb` are `=` but
        // `::text`-distinct — the same equivalence-class defect `real`/
        // `double precision`'s `-0`/`0` and `interval`'s `'1 day'`/
        // `'24 hours'` have, discovered one type deeper. See
        // `crate::jsonb`'s module doc for the full live evidence. The
        // unlock is the typed key index, same as `numeric`/`real`/
        // `double precision`/`timestamptz` above — not a jsonb-specific
        // mechanism.
        // An enum type is safe here on *both* of this gate's two
        // independent hazard axes, unlike `bit`'s split above.
        //
        // Text-stability: `enumout` (what `<col>::text` calls — no `pg_cast`
        // override the way `boolean` has, verified live in
        // `defs_enum.rs`) renders each value as its own label text
        // verbatim; an enum value's identity *is* that label (a `pg_enum`
        // row keyed by OID), so there is no deeper representation for two
        // distinct values to collide under — see `join_key_types::JoinKey::Enum`'s
        // own doc comment for the fuller version of this same reasoning
        // (the join-key/PK key roles' dynamic counterpart of this
        // static gate).
        //
        // DDL typmod: unlike fixed-length `bit`'s `bit(1)` narrowing
        // default, a bare enum type name has no narrower "default" to fall
        // back to at all — the type itself already names the complete,
        // exact value domain (there is no such thing as an "unconstrained"
        // vs. "narrowed" enum the way there is a `bit`/`bit(n)` split), so
        // `ddl::create_aggregate_target_table` declaring a `GROUP BY` key's
        // column as bare `pg_type_name(Other(Enum(_)))` can never lose
        // precision the way a bare `bit` column declaration can.
        ValueType::Other(PgType::Enum(_)) => Some(super::registry::GroupByKeyRule::Enum),
        ValueType::Other(_) => None,
    }
}

/// [`GROUP_BY_KEY_TYPE_NAMES`] rendered for a user-facing error message, so
/// the "supported GROUP BY key types are ..." list in
/// [`ValidationError::UnsupportedGroupByKeyType`]'s `Display` cannot drift
/// out of step with the allowlist it describes. A hand-maintained second copy
/// of the list is the failure mode already avoided for the join-key allowlist
/// via [`super::join_key_types::supported_join_key_types`].
pub(crate) fn supported_group_by_key_types() -> String {
    GROUP_BY_KEY_TYPE_NAMES.join(", ")
}

/// The names [`reject_unsupported_group_by_key_type`] actually admits, in
/// the same order as that function's `Ok` arms, kept here as the single
/// source [`supported_group_by_key_types`] renders instead of a
/// hand-written string.
///
/// This is **not** the same set as
/// [`super::join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`] — the two gates are
/// checked against each other, deliberately, not assumed identical:
///
/// * `numeric` and `boolean` are admitted *here* but refused as a
///   `JOIN`/primary key (`numeric`: its own text
///   rendering isn't a `1.0`/`1.00` bijection; `boolean`: a second
///   `pg_cast`-registered `::text` renderer disagrees with `boolout`). Both
///   are safe for this role for reasons specific to it — `boolean`'s
///   because the group basis (`staging::group_basis`) never does the raw-text
///   comparison that trips it up elsewhere.
/// * `inet` is admitted here for the identical reason `boolean` is (its own
///   `network_show`-vs-`inet_out` split, closed the same way), but stays off
///   the join-key list.
/// * Fixed-length `bit` is refused *here only*, for a DDL-only reason with
///   nothing to do with text-rendering: `ddl::create_aggregate_target_table`
///   would declare a brand-new `bit(1)` column from a bare `ValueType`,
///   truncating any wider source column (the `VarBit` arm has the
///   live evidence). It is a perfectly good join/primary key, where the
///   role only ever reuses an already-existing column's own concrete type.
///
/// See [`reject_unsupported_group_by_key_type`]'s own arm-by-arm comments
/// for the live evidence behind each entry; the
/// `unsupported_group_by_key_type_message_matches_the_real_admission_list`
/// test below drives that function directly so this list can't silently
/// fall out of sync with it again.
const GROUP_BY_KEY_TYPE_NAMES: &[&str] = &[
    "numeric",
    "text",
    "uuid",
    "boolean",
    "oid",
    "bytea",
    "date",
    "time without time zone",
    "time with time zone",
    "timestamp without time zone",
    "timestamp with time zone",
    "cidr",
    "macaddr",
    "macaddr8",
    "inet",
    "bit varying",
    "enum types",
];

/// The columns type inference reads: what a column reference is typed as.
/// Inference never refuses an unknown column (the validators resolve every
/// read first), so a read neither variant knows is typed `numeric`, matching
/// `eval`'s same fallback.
#[derive(Clone, Copy)]
pub(crate) enum ColumnTypes<'a> {
    /// One table's columns by name, for a bare or a qualified read alike.
    Source(&'a HashMap<String, ValueType>),
    /// One table's live columns, read the same way.
    Live(&'a HashMap<String, JoinColumnInfo>),
    /// A `JOIN`'s tables (`root` the `FROM` table's live columns, `joined`
    /// each joined table's, by position). A qualified read is typed by the
    /// side its table names, so two tables may share a column name. A bare
    /// read names no column: a `JOIN` qualifies every one.
    Join {
        def: &'a TransformDef,
        root: &'a HashMap<String, JoinColumnInfo>,
        joined: &'a [&'a HashMap<String, JoinColumnInfo>],
    },
}

impl ColumnTypes<'_> {
    /// The type of the bare column `name`, if it is one.
    fn bare(&self, name: &str) -> Option<ValueType> {
        match self {
            ColumnTypes::Source(columns) => columns.get(name).copied(),
            ColumnTypes::Live(columns) => columns.get(name).map(|info| info.value_type),
            ColumnTypes::Join { .. } => None,
        }
    }

    /// The type of `column` read through the table `table` (its bare name,
    /// as written).
    fn qualified(&self, table: &str, column: &str) -> Option<ValueType> {
        match self {
            ColumnTypes::Source(_) | ColumnTypes::Live(_) => self.bare(column),
            ColumnTypes::Join { def, root, joined } => {
                // A table that is no joined one is the validator's to
                // refuse; it is typed as the first joined table's here.
                let side = super::join::spec(def)
                    .and_then(|join| super::join::side_of_written(def, join, table))
                    .unwrap_or(super::join::JOINED);
                side_columns(side, root, joined)
                    .get(column)
                    .map(|info| info.value_type)
            }
        }
    }
}

pub(crate) fn infer_field_types(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<HashMap<String, ValueType>, ValidationError> {
    infer_fields(&def.fields, &ColumnTypes::Source(source_columns))
}

/// The type of each of `fields`, each read of another of them by name typed
/// as that field.
fn infer_fields(
    fields: &[super::ast::FieldDef],
    columns: &ColumnTypes<'_>,
) -> Result<HashMap<String, ValueType>, ValidationError> {
    let fields_by_name: HashMap<&str, &super::ast::FieldDef> =
        fields.iter().map(|f| (f.name.as_str(), f)).collect();

    let mut types: HashMap<String, ValueType> = HashMap::with_capacity(fields.len());
    let mut in_progress: HashSet<String> = HashSet::new();
    for field in fields {
        if !types.contains_key(&field.name) {
            let t = infer_field(
                field,
                columns,
                &fields_by_name,
                &mut types,
                &mut in_progress,
            )?;
            types.insert(field.name.clone(), t);
        }
    }
    Ok(types)
}

/// The type of `expr`, a self-contained expression over the source row (no
/// reference to another calculated field: a substituted field expression, or
/// a part of one). The ledger types its contribution columns with it:
/// each is one aggregate call's argument.
pub(crate) fn infer_expr_type(
    expr: &Expr,
    source_columns: &HashMap<String, ValueType>,
) -> Result<ValueType, ValidationError> {
    infer_expr_type_in("", expr, &ColumnTypes::Source(source_columns))
}

/// [`infer_expr_type`] over any [`ColumnTypes`], a refusal naming `field`.
pub(crate) fn infer_expr_type_in(
    field: &str,
    expr: &Expr,
    columns: &ColumnTypes<'_>,
) -> Result<ValueType, ValidationError> {
    infer_expr(
        expr,
        field,
        columns,
        &HashMap::new(),
        &mut HashMap::new(),
        &mut HashSet::new(),
    )
}

fn infer_field(
    field: &super::ast::FieldDef,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
) -> Result<ValueType, ValidationError> {
    if let Some(t) = types.get(&field.name) {
        return Ok(*t);
    }
    if !in_progress.insert(field.name.clone()) {
        return Err(ValidationError::Cycle {
            cycle: vec![field.name.clone()],
        });
    }
    // A field is its own target column, so Postgres plans it whatever reads
    // it. One that is only `NULL` is `text`, the type `CREATE TABLE AS`
    // gives an `unknown` column.
    let t = if is_null_literal(&field.expr, &field.name, columns, fields_by_name) {
        Ok(ValueType::Text)
    } else {
        infer_expr(
            &field.expr,
            &field.name,
            columns,
            fields_by_name,
            types,
            in_progress,
        )
    };
    in_progress.remove(&field.name);
    let t = t?;
    types.insert(field.name.clone(), t);
    Ok(t)
}

/// Whether `expr` is the `NULL` literal, written directly or read through
/// calculated fields that are each only a name for it. Every renderer
/// inlines a field it reads, so Postgres sees the literal itself where such
/// a field is read, and types it from that context, not as the field's own
/// `text` column.
fn is_null_literal(
    expr: &Expr,
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
) -> bool {
    let (mut expr, mut field_name) = (expr, field_name);
    // A chain of names visits each field at most once unless it cycles,
    // which inference refuses on its own.
    for _ in 0..=fields_by_name.len() {
        match expr {
            Expr::Null => return true,
            Expr::Column(name) => {
                let is_self_passthrough = name == field_name && columns.bare(name).is_some();
                let Some(field) = fields_by_name
                    .get(name.as_str())
                    .filter(|_| !is_self_passthrough)
                else {
                    return false;
                };
                (expr, field_name) = (&field.expr, &field.name);
            }
            _ => return false,
        }
    }
    false
}

/// [`infer_expr`] for an operand whose context types a `NULL` literal (an
/// operator's other operand, a `CASE`'s other results, a `COALESCE`'s or
/// `NULLIF`'s other arguments, `boolean` under logic): `None` for one, as
/// [`is_null_literal`] finds it, which the caller resolves.
#[allow(clippy::too_many_arguments)]
fn infer_operand(
    expr: &Expr,
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
) -> Result<Option<ValueType>, ValidationError> {
    if is_null_literal(expr, field_name, columns, fields_by_name) {
        return Ok(None);
    }
    infer_expr(
        expr,
        field_name,
        columns,
        fields_by_name,
        types,
        in_progress,
    )
    .map(Some)
}

#[cold]
#[inline(never)]
fn untyped_null(field_name: &str, context: &str) -> ValidationError {
    ValidationError::UntypedNull {
        field: field_name.to_string(),
        context: context.to_string(),
    }
}

/// The operand types a binary operator is resolved over when either operand
/// is a `NULL` literal (`None`), as Postgres resolves them: a `NULL` is
/// taken to be of the other operand's type, and two `NULL`s of a comparison
/// to be `text`. Two of an arithmetic operator are ambiguous to Postgres,
/// and `AT TIME ZONE` is a function call to it, whose candidate rules apply
/// instead; both are refused. Kept out of [`infer_expr`] so its frame, which
/// every level of an expression stacks, stays small.
#[inline(never)]
fn resolve_null_operands(
    op: super::ast::Operator,
    lhs_t: Option<ValueType>,
    rhs_t: Option<ValueType>,
    field_name: &str,
) -> Result<(ValueType, ValueType), ValidationError> {
    if op == super::ast::Operator::AtTimeZone && (lhs_t.is_none() || rhs_t.is_none()) {
        return Err(untyped_null(field_name, "an operand of AT TIME ZONE"));
    }
    match (lhs_t, rhs_t) {
        (Some(l), Some(r)) => Ok((l, r)),
        (Some(t), None) | (None, Some(t)) => Ok((t, t)),
        (None, None) if op.is_comparison() => Ok((ValueType::Text, ValueType::Text)),
        (None, None) => Err(untyped_null(
            field_name,
            &format!(
                "both operands of {}",
                super::registry::operator_spec(op).symbol
            ),
        )),
    }
}

/// The refusal of an operator with a `NULL` operand that no operator takes
/// with the other operand's type on both sides (or none this grammar
/// admits): Postgres's pick, if it has one, is not that type.
#[cold]
#[inline(never)]
fn null_operand_refusal(
    op: super::ast::Operator,
    other: ValueType,
    field_name: &str,
) -> ValidationError {
    untyped_null(
        field_name,
        &format!(
            "an operand of {} beside a {other}",
            super::registry::operator_spec(op).symbol
        ),
    )
}

/// The refusal of a function or aggregate call (other than `COALESCE` and
/// `NULLIF`) with a `NULL` argument. Postgres types such an argument by its
/// candidate rules (`abs(NULL)` is `double precision`, `sum(NULL)` is
/// ambiguous), which this grammar does not model.
#[inline(never)]
fn null_argument_refusal(
    name: &str,
    args: &[Expr],
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
) -> Option<ValidationError> {
    (name != "COALESCE"
        && name != "NULLIF"
        && args
            .iter()
            .any(|arg| is_null_literal(arg, field_name, columns, fields_by_name)))
    .then(|| {
        untyped_null(
            field_name,
            &format!("an argument of {}", name.to_ascii_lowercase()),
        )
    })
}

/// The operands of a chain of `op` (`a AND (b AND c) AND d` is `a, b, c,
/// d`).
fn flatten_logic<'a>(expr: &'a Expr, op: super::ast::Operator, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp { op: this, lhs, rhs } if *this == op => {
            flatten_logic(lhs, op, out);
            flatten_logic(rhs, op, out);
        }
        other => out.push(other),
    }
}

/// The type of a searched `CASE`: its conditions are `boolean`, and its
/// results unify the way `COALESCE`'s arguments do.
fn infer_case(
    branches: &[(Expr, Expr)],
    otherwise: Option<&Expr>,
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
) -> Result<ValueType, ValidationError> {
    let mut common: Option<ValueType> = None;
    let mut unify = |found: ValueType| -> Result<(), ValidationError> {
        match common {
            None => common = Some(found),
            Some(so_far) => match common_numeric_type(so_far, found) {
                Some(unified) => common = Some(unified),
                None => {
                    return Err(ValidationError::CaseResultTypeMismatch {
                        field: field_name.to_string(),
                        expected: so_far,
                        found,
                    });
                }
            },
        }
        Ok(())
    };
    for (condition, result) in branches {
        // A `NULL` condition is a `boolean` one, which never holds.
        let condition_t = infer_operand(
            condition,
            field_name,
            columns,
            fields_by_name,
            types,
            in_progress,
        )?
        .unwrap_or(ValueType::Boolean);
        if condition_t != ValueType::Boolean {
            return Err(ValidationError::CaseConditionNotBoolean {
                field: field_name.to_string(),
                found: condition_t,
            });
        }
        // A `NULL` result takes the type of the others.
        if let Some(result_t) = infer_operand(
            result,
            field_name,
            columns,
            fields_by_name,
            types,
            in_progress,
        )? {
            unify(result_t)?;
        }
    }
    if let Some(otherwise) = otherwise
        && let Some(otherwise_t) = infer_operand(
            otherwise,
            field_name,
            columns,
            fields_by_name,
            types,
            in_progress,
        )?
    {
        unify(otherwise_t)?;
    }
    // Results that are all `NULL` are `text`, as Postgres resolves a set of
    // `unknown` inputs.
    Ok(common.unwrap_or(ValueType::Text))
}

/// The result type of the comparison `op` over `lhs` and `rhs` (typed
/// `lhs_t` and `rhs_t`), or the refusal naming why the pair is not one a
/// comparison family covers ([`super::registry::comparison_result_type`]).
/// A float against a *bare number literal* is admitted: Postgres casts the
/// untyped literal to `double precision` (a `real` operand widens to it) and
/// runs the comparison as `float8`. Only the literal's own syntax is exempt:
/// the same value in a column, a calculated field, an expression or a typed
/// `NUMERIC '0.5'` literal still hits the refusal.
fn comparison_type(
    op: super::ast::Operator,
    lhs: &Expr,
    rhs: &Expr,
    lhs_t: ValueType,
    rhs_t: ValueType,
    field_name: &str,
) -> Result<ValueType, ValidationError> {
    if let Some(result) = super::registry::comparison_result_type(op, lhs_t, rhs_t) {
        return Ok(result);
    }
    let operator = super::registry::operator_spec(op).symbol;
    if super::registry::is_mixed_float_exact(lhs_t, rhs_t) {
        let beside_number_literal = match (lhs, rhs) {
            (Expr::NumberLiteral(_), _) => matches!(rhs_t, ValueType::Float(_)),
            (_, Expr::NumberLiteral(_)) => matches!(lhs_t, ValueType::Float(_)),
            _ => false,
        };
        if beside_number_literal {
            return Ok(super::registry::Admitted::new(
                ValueType::Boolean,
                super::registry::TypeRule::Comparison(
                    super::registry::ComparisonRule::FloatBesideNumberLiteral,
                ),
            )
            .into_type());
        }
        return Err(ValidationError::MixedFloatExactComparison {
            field: field_name.to_string(),
            operator,
            lhs: lhs_t,
            rhs: rhs_t,
        });
    }
    let field = field_name.to_string();
    Err(if super::registry::is_mixed_temporal(lhs_t, rhs_t) {
        ValidationError::MixedTemporalComparison {
            field,
            operator,
            lhs: lhs_t,
            rhs: rhs_t,
        }
    } else if lhs_t == ValueType::Text && rhs_t == ValueType::Text && op.is_ordering() {
        ValidationError::TextOrderingComparison { field, operator }
    } else {
        ValidationError::ComparisonTypeMismatch {
            field,
            operator,
            lhs: lhs_t,
            rhs: rhs_t,
        }
    })
}

/// The type of `expr`, or the refusal naming why it has none this grammar
/// admits. Inference never evaluates a constant: an expression Postgres
/// fails while planning it (`2147483647 + 1`) types like any other, and its
/// rows fail and are quarantined at run time with Postgres's own error.
fn infer_expr(
    expr: &Expr,
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
) -> Result<ValueType, ValidationError> {
    match expr {
        Expr::Column(name) => {
            // See the matching comment in `validate`: a field referencing a
            // source column of its own name is a passthrough, not a
            // self-dependency, so it's resolved via `columns` below
            // rather than recursing into its own (in-progress) inference.
            let is_self_passthrough = name == field_name && columns.bare(name).is_some();
            if !is_self_passthrough && let Some(calc_field) = fields_by_name.get(name.as_str()) {
                return infer_field(calc_field, columns, fields_by_name, types, in_progress);
            }
            // Column resolution against `columns` already happened
            // in `validate`; a name absent here (standalone use, e.g. tests
            // calling `infer_field_types` directly) defaults to Numeric,
            // matching `eval`'s same fallback.
            Ok(columns.bare(name).unwrap_or(ValueType::Numeric))
        }
        // A qualified name is always a column of the table it names.
        Expr::SourceColumn { table, column, .. } => Ok(columns
            .qualified(table, column)
            .unwrap_or(ValueType::Numeric)),
        // An unadorned literal is typed exactly as Postgres's
        // own lexer types it — `integer` if it fits, else `bigint`, else
        // (or if fractional) `numeric`.
        Expr::NumberLiteral(text) => Ok(number_literal_type(text)),
        Expr::StringLiteral(_) => Ok(ValueType::Text),
        Expr::BooleanLiteral(_) => Ok(ValueType::Boolean),
        // Each construct that types a `NULL` from its context reads its
        // operands through `infer_operand`; one reaching here has none.
        Expr::Null => Err(untyped_null(field_name, "this position")),
        Expr::FunctionCall { name, args }
            if let Some(refusal) =
                null_argument_refusal(name, args, field_name, columns, fields_by_name) =>
        {
            Err(refusal)
        }
        Expr::TypedLiteral { value_type, text } => {
            // Checked here, not in the parser, for the same reason
            // `regexp_count`'s pattern is (see `validate_regexp_pattern`):
            // this runs on every path into the catalog, including a
            // hand-built AST that never went through `parse`, so a literal
            // can't reach the renderer un-checked. See
            // `super::typed_literal` for why the bar is *canonical* form
            // rather than merely "Postgres would parse it".
            let spec = super::typed_literal::lookup_typed_literal(
                &super::ddl::pg_type_name(*value_type).to_ascii_uppercase(),
            )
            .ok_or_else(|| ValidationError::UnsupportedLiteralType {
                field: field_name.to_string(),
                value_type: *value_type,
            })?;
            (spec.canonical)(text).map_err(|detail| ValidationError::InvalidTypedLiteral {
                field: field_name.to_string(),
                value_type: *value_type,
                text: text.clone(),
                detail: detail.to_string(),
            })?;
            Ok(spec.value_type)
        }
        // `AND` and `OR` chains are typed operand by operand, flattened so a
        // long chain does not stack a frame per operand.
        Expr::BinaryOp {
            op: op @ (super::ast::Operator::And | super::ast::Operator::Or),
            ..
        } => {
            let mut operands = Vec::new();
            flatten_logic(expr, *op, &mut operands);
            for operand in operands {
                // A `NULL` operand is a `boolean` one.
                let operand_t = infer_operand(
                    operand,
                    field_name,
                    columns,
                    fields_by_name,
                    types,
                    in_progress,
                )?
                .unwrap_or(ValueType::Boolean);
                if operand_t != ValueType::Boolean {
                    return Err(ValidationError::TypeMismatch {
                        field: field_name.to_string(),
                        operator: super::registry::operator_spec(*op).symbol,
                        expected: ValueType::Boolean,
                        found: operand_t,
                    });
                }
            }
            Ok(ValueType::Boolean)
        }
        Expr::Not(operand) => {
            let operand_t = infer_operand(
                operand,
                field_name,
                columns,
                fields_by_name,
                types,
                in_progress,
            )?
            .unwrap_or(ValueType::Boolean);
            if operand_t != ValueType::Boolean {
                return Err(ValidationError::TypeMismatch {
                    field: field_name.to_string(),
                    operator: "NOT",
                    expected: ValueType::Boolean,
                    found: operand_t,
                });
            }
            Ok(ValueType::Boolean)
        }
        // `NULL IS NULL` needs no type.
        Expr::IsNull { expr: operand, .. } => {
            infer_operand(
                operand,
                field_name,
                columns,
                fields_by_name,
                types,
                in_progress,
            )?;
            Ok(ValueType::Boolean)
        }
        Expr::Case {
            branches,
            otherwise,
        } => infer_case(
            branches,
            otherwise.as_deref(),
            field_name,
            columns,
            fields_by_name,
            types,
            in_progress,
        ),
        Expr::FunctionCall { name, args } if name == "NULLIF" && args.len() == 2 => {
            let mut arg_types = [None; 2];
            for (slot, arg) in arg_types.iter_mut().zip(args) {
                *slot =
                    infer_operand(arg, field_name, columns, fields_by_name, types, in_progress)?;
            }
            // Postgres resolves `NULLIF` as the `=` over its arguments: a
            // `NULL` one takes the other's type, and two are `text`.
            let [lhs_t, rhs_t] = match arg_types {
                [Some(l), Some(r)] => [l, r],
                [Some(t), None] | [None, Some(t)] => [t, t],
                [None, None] => [ValueType::Text; 2],
            };
            comparison_type(
                super::ast::Operator::Equal,
                &args[0],
                &args[1],
                lhs_t,
                rhs_t,
                field_name,
            )?;
            // Postgres casts the first argument when no `=` takes the pair as
            // it is (there is no `integer = numeric` or `numeric = float8`
            // operator), so the result is that cast's type, not `lhs_t`.
            let cast = match (lhs_t, rhs_t) {
                (ValueType::Integer(_), ValueType::Numeric) => Some(ValueType::Numeric),
                (l, ValueType::Float(_)) if l.is_exact_numeric_family() => {
                    Some(ValueType::Float(crate::float::FloatWidth::Float8))
                }
                _ => None,
            };
            if let Some(result) = cast {
                return Err(ValidationError::NullIfFirstArgumentCast {
                    field: field_name.to_string(),
                    lhs: lhs_t,
                    rhs: rhs_t,
                    result,
                });
            }
            Ok(lhs_t)
        }
        Expr::FunctionCall { name, args } if name == "EXTRACT" || name == "DATE_TRUNC" => {
            infer_date_part_call(
                name,
                args,
                field_name,
                columns,
                fields_by_name,
                types,
                in_progress,
            )
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs_t =
                infer_operand(lhs, field_name, columns, fields_by_name, types, in_progress)?;
            let rhs_t =
                infer_operand(rhs, field_name, columns, fields_by_name, types, in_progress)?;
            infer_binary_op(expr, *op, lhs, rhs, lhs_t, rhs_t, field_name)
        }
        Expr::Negate(operand) => {
            // Postgres finds prefix `-` over `NULL` ambiguous.
            let operand_t = infer_operand(
                operand,
                field_name,
                columns,
                fields_by_name,
                types,
                in_progress,
            )?
            .ok_or_else(|| untyped_null(field_name, "the operand of prefix -"))?;
            let result = super::registry::negate_result_type(operand_t).ok_or_else(|| {
                ValidationError::TypeMismatch {
                    field: field_name.to_string(),
                    operator: "-",
                    expected: ValueType::Numeric,
                    found: operand_t,
                }
            })?;
            Ok(result)
        }
        Expr::Cast {
            expr: operand,
            target,
        } => {
            // `CAST(NULL AS <type>)` is a `NULL` of that type, whatever the
            // type: no value is converted.
            if is_null_literal(operand, field_name, columns, fields_by_name) {
                return Ok(*target);
            }
            let operand_t = infer_expr(
                operand,
                field_name,
                columns,
                fields_by_name,
                types,
                in_progress,
            )?;
            let result =
                super::registry::cast_result_type(operand_t, *target).ok_or_else(|| {
                    match (operand_t, *target) {
                        (
                            ValueType::Other(PgType::TimestampTz),
                            ValueType::Other(PgType::Timestamp),
                        ) => ValidationError::TemporalOperand {
                            field: field_name.to_string(),
                            construct: "CAST(... AS timestamp)".to_string(),
                            found: operand_t,
                            reason: "the conversion reads the session time zone; use \
                                    <value> AT TIME ZONE 'UTC', which gives the timestamp in UTC",
                        },
                        _ => ValidationError::UnsupportedCast {
                            field: field_name.to_string(),
                            from: operand_t,
                            to: *target,
                        },
                    }
                })?;
            Ok(result)
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            let mut common_type = None;
            for (i, arg) in args.iter().enumerate() {
                // A `NULL` argument takes the type of the others.
                let Some(arg_t) =
                    infer_operand(arg, field_name, columns, fields_by_name, types, in_progress)?
                else {
                    continue;
                };
                if let Some(ct) = common_type {
                    // `COALESCE`'s arguments used to have to be
                    // the identical `ValueType`. Splitting exact integers
                    // out of `Numeric` would otherwise have *newly rejected*
                    // `coalesce(int_col, numeric_col)` — and even
                    // `coalesce(smallint_col, bigint_col)` — both of which
                    // Postgres resolves happily, through the same implicit
                    // coercions `registry::operator_result_type` encodes.
                    // `common_numeric_type` is that rule applied pairwise;
                    // every other type still has to match exactly.
                    match common_numeric_type(ct, arg_t) {
                        Some(unified) => common_type = Some(unified),
                        None => {
                            return Err(ValidationError::FunctionArgTypeMismatch {
                                field: field_name.to_string(),
                                function: name.clone(),
                                arg_index: i,
                                expected: ct,
                                found: arg_t,
                            });
                        }
                    }
                } else {
                    common_type = Some(arg_t);
                }
            }
            // Arguments that are all `NULL` are `text`, as Postgres resolves
            // a set of `unknown` inputs.
            Ok(common_type.unwrap_or(ValueType::Text))
        }
        Expr::FunctionCall { name, args } => {
            // The parser only ever builds a `FunctionCall` node for a name
            // it already looked up in `registry::FUNCTIONS` and checked
            // arity against; a name absent from the registry here (a
            // hand-built AST bypassing the parser) has no argument types to
            // check against, so it type-checks as Numeric, matching this
            // function's own fallback for an unresolved column reference
            // just above.
            let aggregate_spec = super::registry::lookup_aggregate_function(name);
            let is_aggregate = aggregate_spec.is_some();
            if !is_aggregate && super::registry::function_overloads(name).nth(1).is_some() {
                return infer_overloaded_call(
                    expr,
                    field_name,
                    columns,
                    fields_by_name,
                    types,
                    in_progress,
                );
            }
            // An aggregate outside the admitted set is refused
            // by family, before its argument is typed: the grammar parses
            // these names only to refuse them.
            if is_deferred_aggregate_name(name) {
                check_aggregate_admitted(&name.to_ascii_uppercase(), AggregateScope::Plain, None)?;
            }
            let spec = super::registry::lookup_function(name).or(aggregate_spec);
            // An aggregate's argument reads source columns, not aggregate
            // fields (see `aggregate_field_names`).
            let aggregate_scope;
            let fields_by_name = if is_aggregate {
                let aggregates =
                    aggregate_field_names(fields_by_name.iter().map(|(n, f)| (*n, &f.expr)));
                aggregate_scope = without_aggregate_fields(fields_by_name, &aggregates);
                &aggregate_scope
            } else {
                fields_by_name
            };
            let Some(spec) = spec else {
                for arg in args {
                    infer_expr(arg, field_name, columns, fields_by_name, types, in_progress)?;
                }
                return Ok(ValueType::Numeric);
            };
            // `COUNT` is special-cased ahead of the generic
            // per-argument loop below, for both its accepted shapes —
            // `COUNT(*)` (`args` empty) and `COUNT(<expr>)` (`args` one
            // element). `AGGREGATE_FUNCTION_SPECS`'s `COUNT` row declares
            // `arg_types: &[]` (the parser, not this table, enforces
            // `COUNT`'s arity — see `super::parser`'s own `COUNT` branch), so
            // the generic `args.iter().zip(spec.arg_types)` loop just below
            // would `zip` a real one-element `args` against an empty
            // `arg_types` and silently produce *zero* iterations — meaning
            // `COUNT(<expr>)`'s argument would never be recursively
            // validated at all (an unresolved column etc. would sail
            // through unchecked). Postgres's own `count(x)`
            // accepts any argument type with no restriction, so there is
            // nothing to admit here beyond "the argument itself resolves" —
            // unlike `SUM`/`MIN`/`MAX`/`AVG`, whose argument type gates
            // through `registry::aggregate_result_type` below.
            if name == "COUNT" {
                for arg in args {
                    infer_expr(arg, field_name, columns, fields_by_name, types, in_progress)?;
                }
                return Ok(spec.return_type);
            }
            let mut arg_types: Vec<ValueType> = Vec::with_capacity(args.len());
            for (i, (arg, expected)) in args.iter().zip(spec.arg_types).enumerate() {
                let arg_t =
                    infer_expr(arg, field_name, columns, fields_by_name, types, in_progress)?;
                // A `Numeric`-declared *aggregate* argument now
                // means "any exact-numeric type" — `SUM`/`AVG`/`MIN`/`MAX`
                // accept every exact-integer width as well as `numeric`, and
                // the result type depends on which one
                // (`registry::aggregate_result_type`). A scalar function's
                // argument list stays an exact match: all four of those take
                // `Text`, and Postgres would not coerce an integer into one.
                // `MIN`/`MAX` is typed over the types the group basis keeps
                // an extreme of, and refused by name over any other.
                if is_aggregate
                    && matches!(name.as_str(), "MIN" | "MAX")
                    && !super::registry::extreme_type_admitted(arg_t)
                {
                    return Err(ValidationError::UnsupportedExtremeArgument {
                        function: name.clone(),
                        argument: render_field_expr(arg),
                        pg_type: super::ddl::pg_type_name(arg_t).into_owned(),
                    });
                }
                let admissible = if is_aggregate && *expected == ValueType::Numeric {
                    // A `Numeric`-declared *aggregate*
                    // argument means "any numeric type", floats included —
                    // Postgres has a `sum`/`avg`/`min`/`max` for `real` and
                    // `double precision` just as it does for the exact
                    // types. It extends past the numeric family
                    // entirely (`MIN`/`MAX` over the temporal types,
                    // `SUM(interval)`), at which point "what does this
                    // aggregate accept?" and "what does it return?" are one
                    // question with one answer, so the admissibility test
                    // *is* `registry::aggregate_result_type` resolving —
                    // rather than a predicate here that a future family
                    // could teach one of the two and not the other.
                    super::registry::aggregate_result_type(name, arg_t).is_some()
                } else if is_aggregate && matches!(name.as_str(), "BIT_AND" | "BIT_OR") {
                    // `bit_and`/`bit_or` accept *two* distinct
                    // `ValueType::Other` families (`PgType::Bit` and
                    // `PgType::VarBit`) — Postgres genuinely has both
                    // `bit_and(bit)` and an implicit `bit varying -> bit`
                    // widening that lets a `bit varying` argument reach it
                    // too (verified live against `pg_aggregate`/
                    // `pg_cast`). `bool_and`/`bool_or`'s single fixed
                    // `ValueType::Boolean` `expected` has no such split, so
                    // its plain exact-match `else` arm below was enough;
                    // this pair needs the same "ask the registry" widen-check
                    // the `Numeric` branch above uses, since one static
                    // `arg_types` slot can't name two acceptable types. The
                    // `AGGREGATE_FUNCTION_SPECS` row's own `arg_types` entry
                    // is therefore just a representative placeholder
                    // (`Other(PgType::Bit)`), never compared against
                    // directly.
                    super::registry::aggregate_result_type(name, arg_t).is_some()
                } else {
                    arg_t == *expected
                };
                if !admissible {
                    return Err(ValidationError::FunctionArgTypeMismatch {
                        field: field_name.to_string(),
                        function: name.clone(),
                        arg_index: i,
                        expected: *expected,
                        found: arg_t,
                    });
                }
                arg_types.push(arg_t);
            }
            if name == "STRING_AGG" && !matches!(args.get(1), Some(Expr::StringLiteral(_))) {
                return Err(ValidationError::StringAggSeparatorNotLiteral {
                    field: field_name.to_string(),
                });
            }
            if name == "REGEXP_COUNT"
                && let Some(pattern_arg) = args.get(1)
            {
                validate_regexp_pattern(field_name, pattern_arg)?;
            }
            if is_aggregate
                && let Some(&arg_t) = arg_types.first()
                && let Some(result) = super::registry::aggregate_result_type(name, arg_t)
            {
                return Ok(result);
            }
            Ok(spec.return_type)
        }
    }
}

/// The type of `lhs <op> rhs` (an operator other than `AND` and `OR`) from
/// its operands' types, `None` for a `NULL` literal operand. Kept out of
/// [`infer_expr`] so its frame, which every level of an expression stacks,
/// stays small.
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn infer_binary_op(
    expr: &Expr,
    op: super::ast::Operator,
    lhs: &Expr,
    rhs: &Expr,
    lhs_t: Option<ValueType>,
    rhs_t: Option<ValueType>,
    field_name: &str,
) -> Result<ValueType, ValidationError> {
    let null_operand = lhs_t.is_none() || rhs_t.is_none();
    let (lhs_t, rhs_t) = resolve_null_operands(op, lhs_t, rhs_t, field_name)?;
    // Operand admissibility *and* the result type both
    // come from `registry::operator_result_type`, which models
    // Postgres's family of `+`/`>` operators (`int4pl`,
    // `numeric_add`, and the implicit `int -> numeric` coercion
    // between them) rather than the single exact signature this used
    // to compare against. `OperatorSpec::arg_types` survives only as
    // the canonical shape a `TypeMismatch` names.
    if let Some(result) = super::registry::operator_result_type(op, lhs_t, rhs_t) {
        if op == super::ast::Operator::AtTimeZone {
            check_utc_zone(rhs, field_name)?;
        }
        if matches!(
            op,
            super::ast::Operator::Divide | super::ast::Operator::Modulo
        ) && is_literal_zero(rhs)
        {
            return Err(ValidationError::ConstantDivisorZero {
                field: field_name.to_string(),
                text: super::ast::render_field_expr(expr),
            });
        }
        return Ok(result);
    }
    if op.is_comparison() {
        return comparison_type(op, lhs, rhs, lhs_t, rhs_t, field_name);
    }
    // No operator takes the other operand's type on both sides (or
    // none this grammar admits), so Postgres's pick, if it has one,
    // is not this operand's type.
    if null_operand {
        return Err(null_operand_refusal(op, lhs_t, field_name));
    }
    if let Some(refusal) = temporal_operator_refusal(op, lhs_t, rhs_t, field_name) {
        return Err(refusal);
    }
    if lhs_t.is_numeric_family() && rhs_t.is_numeric_family() {
        match op {
            super::ast::Operator::Divide => {
                return Err(ValidationError::IntegerDivision {
                    field: field_name.to_string(),
                    lhs: lhs_t,
                    rhs: rhs_t,
                });
            }
            super::ast::Operator::Modulo => {
                return Err(ValidationError::FloatModulo {
                    field: field_name.to_string(),
                });
            }
            _ => {}
        }
    }
    // Blame the operand that actually isn't admissible. At least one
    // of them isn't (or resolution above would have succeeded), and
    // the left one is reported first so the error reads in source
    // order.
    let spec = super::registry::operator_spec(op);
    let (found, expected) = if lhs_t.is_numeric_family() {
        (rhs_t, spec.arg_types.1)
    } else {
        (lhs_t, spec.arg_types.0)
    };
    Err(ValidationError::TypeMismatch {
        field: field_name.to_string(),
        operator: spec.symbol,
        expected,
        found,
    })
}

const DATE_TYPE: ValueType = ValueType::Other(PgType::Date);
const TIMESTAMP_TYPE: ValueType = ValueType::Other(PgType::Timestamp);
const TIMESTAMPTZ_TYPE: ValueType = ValueType::Other(PgType::TimestampTz);

/// The date and time types, the ones `+` and `-` refuse by name.
fn is_temporal(t: ValueType) -> bool {
    matches!(
        t,
        ValueType::Other(
            PgType::Date
                | PgType::Time
                | PgType::TimeTz
                | PgType::Timestamp
                | PgType::TimestampTz
                | PgType::Interval
        )
    )
}

/// The named refusal for a `+`, `-` or `AT TIME ZONE` over operand types
/// [`super::registry::operator_result_type`] does not admit, when one of them
/// is a date or time type; `None` for any other pair (the caller's generic
/// type mismatch applies).
fn temporal_operator_refusal(
    op: super::ast::Operator,
    lhs_t: ValueType,
    rhs_t: ValueType,
    field_name: &str,
) -> Option<ValidationError> {
    use super::ast::Operator;
    let operand_error = |construct: &str, found: ValueType, reason: &'static str| {
        Some(ValidationError::TemporalOperand {
            field: field_name.to_string(),
            construct: construct.to_string(),
            found,
            reason,
        })
    };
    if op == Operator::AtTimeZone {
        if lhs_t == TIMESTAMPTZ_TYPE {
            return Some(ValidationError::TemporalLiteral {
                field: field_name.to_string(),
                construct: "AT TIME ZONE",
                text: None,
                reason: "the zone must be the string literal 'UTC'".to_string(),
            });
        }
        let reason = match lhs_t {
            TIMESTAMP_TYPE => {
                "on a timestamp it gives a timestamptz, which nothing here accepts; \
                 AT TIME ZONE takes a timestamptz and gives the timestamp it is in UTC"
            }
            DATE_TYPE => {
                "Postgres converts a date to timestamptz through the session time zone \
                 first; use CAST(d AS timestamp), which has no zone"
            }
            _ => "AT TIME ZONE takes a timestamptz",
        };
        return operand_error("AT TIME ZONE", lhs_t, reason);
    }
    if !matches!(op, Operator::Add | Operator::Subtract)
        || !(is_temporal(lhs_t) || is_temporal(rhs_t))
    {
        return None;
    }
    let symbol = super::registry::operator_spec(op).symbol;
    let construct = format!("'{symbol}'");
    let interval = |t: ValueType| t == ValueType::Other(PgType::Interval);
    let integer = |t: ValueType| matches!(t, ValueType::Integer(_));
    let (found, reason) = if interval(lhs_t) || interval(rhs_t) {
        (
            if interval(lhs_t) { lhs_t } else { rhs_t },
            "interval arithmetic is not admitted (a month is not a fixed number of days, so \
             results depend on the date they are added to). For a duration in seconds use \
             EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)",
        )
    } else if op == Operator::Subtract && is_temporal(lhs_t) && is_temporal(rhs_t) {
        (
            lhs_t,
            "the difference of two timestamps (or of a date and a timestamp) is an \
             interval, which is not admitted. For a duration in seconds use \
             EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a); two dates subtract to days",
        )
    } else if op == Operator::Add && lhs_t == DATE_TYPE && rhs_t == DATE_TYPE {
        (
            lhs_t,
            "adding two dates has no meaning. Subtract them for the days between, or add \
             a number of days to one",
        )
    } else if (lhs_t == DATE_TYPE && (rhs_t.is_numeric_family()))
        || (rhs_t == DATE_TYPE && lhs_t.is_numeric_family())
    {
        let other = if lhs_t == DATE_TYPE { rhs_t } else { lhs_t };
        if op == Operator::Subtract && lhs_t != DATE_TYPE {
            (
                lhs_t,
                "a number minus a date has no meaning; subtract the number from the date \
                 instead",
            )
        } else {
            (
                other,
                "date arithmetic takes a smallint or integer number of days; Postgres has \
                 no operator for any other numeric type",
            )
        }
    } else if (is_temporal(lhs_t) && integer(rhs_t)) || (is_temporal(rhs_t) && integer(lhs_t)) {
        (
            if is_temporal(lhs_t) { lhs_t } else { rhs_t },
            "Postgres has no operator that adds a number to this type; only a date takes \
             a number of days. Truncate with DATE_TRUNC, or take a part with EXTRACT",
        )
    } else {
        (
            if is_temporal(lhs_t) { lhs_t } else { rhs_t },
            "the only date arithmetic admitted is a date plus or minus a number of days \
             and the days between two dates",
        )
    };
    operand_error(&construct, found, reason)
}

/// Refuses an `AT TIME ZONE` whose zone is not the string literal `'UTC'`:
/// any other zone's offsets come from the tz database, which changes.
fn check_utc_zone(zone: &Expr, field_name: &str) -> Result<(), ValidationError> {
    let refused = |text: Option<String>| ValidationError::TemporalLiteral {
        field: field_name.to_string(),
        construct: "AT TIME ZONE",
        text,
        reason: "only the literal 'UTC' is admitted: any other zone's offsets come from the \
                 time zone database, which a release can change under past results"
            .to_string(),
    };
    match zone {
        Expr::StringLiteral(text) => super::registry::admit_value(
            super::registry::TIME_ZONES,
            text,
            super::registry::ArgValue::TimeZone,
        )
        .map(|_| ())
        .ok_or_else(|| refused(Some(text.clone()))),
        _ => Err(refused(None)),
    }
}

/// Whether `divisor` is written as a literal zero: a number literal (`0`,
/// `0.0`, `-0`) or a float typed literal (`REAL '0'`, `DOUBLE PRECISION
/// '-0'`), possibly under `CAST` or prefix `-`. This reads only the text: it
/// evaluates nothing, follows no calculated field and ignores where the
/// division sits, so a literal zero divisor is refused even in a `CASE`
/// branch no row reaches. A divisor that only computes to zero (`n / (1 -
/// 1)`) defines, and its rows fail and are quarantined at run time.
fn is_literal_zero(divisor: &Expr) -> bool {
    match divisor {
        Expr::NumberLiteral(text) => {
            let digits = text.strip_prefix('-').unwrap_or(text);
            digits.bytes().all(|b| b == b'0' || b == b'.') && digits.contains('0')
        }
        Expr::TypedLiteral {
            value_type: ValueType::Float(_),
            text,
        } => text.strip_prefix('-').unwrap_or(text) == "0",
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => is_literal_zero(operand),
        _ => false,
    }
}

/// Types `EXTRACT(<field> FROM <date or timestamp>)` (a `numeric`) and
/// `DATE_TRUNC('<unit>', <timestamp>)` (a `timestamp`), or names why the
/// call is refused.
#[allow(clippy::too_many_arguments)]
fn infer_date_part_call(
    name: &str,
    args: &[Expr],
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
) -> Result<ValueType, ValidationError> {
    let extract = name == "EXTRACT";
    let construct: &'static str = if extract { "EXTRACT" } else { "DATE_TRUNC" };
    let literal_error = |text: Option<String>, reason: String| ValidationError::TemporalLiteral {
        field: field_name.to_string(),
        construct,
        text,
        reason,
    };
    let [word, operand] = args else {
        return Err(literal_error(
            None,
            "it takes a unit or field and one operand".to_string(),
        ));
    };
    // The unit of `DATE_TRUNC` and the field of `EXTRACT` are literals the
    // parser keeps as lower-case string literals.
    let Expr::StringLiteral(word) = word else {
        return Err(literal_error(
            None,
            if extract {
                "the field is a bare word, as in EXTRACT(year FROM d)".to_string()
            } else {
                "the unit is a string literal, as in DATE_TRUNC('month', ts)".to_string()
            },
        ));
    };
    let operand_t = infer_expr(
        operand,
        field_name,
        columns,
        fields_by_name,
        types,
        in_progress,
    )?;
    let operand_error = |reason: &'static str| ValidationError::TemporalOperand {
        field: field_name.to_string(),
        construct: construct.to_string(),
        found: operand_t,
        reason,
    };
    if extract {
        let Some(fields) = super::registry::extract_fields(operand_t) else {
            return Err(operand_error(if operand_t == TIMESTAMPTZ_TYPE {
                "EXTRACT of a timestamptz reads the session time zone. Extract from the \
                 timestamp in UTC: EXTRACT(hour FROM ts AT TIME ZONE 'UTC')"
            } else {
                "EXTRACT takes a date or a timestamp"
            }));
        };
        if !fields.contains(&word.as_str()) {
            let reason = if super::registry::EXTRACT_REFUSED_FIELDS.contains(&word.as_str()) {
                "its value is not the same for every type, or needs a time zone this \
                 value does not have"
                    .to_string()
            } else if super::registry::values(super::registry::EXTRACT_CLOCK_FIELDS)
                .contains(&word.as_str())
            {
                format!(
                    "a date has no {word}; extract it from the timestamp: EXTRACT({word} FROM \
                     CAST(d AS timestamp))"
                )
            } else {
                format!(
                    "the fields admitted for a {operand_t} are {}",
                    fields.join(", ")
                )
            };
            return Err(literal_error(Some(word.clone()), reason));
        }
        super::registry::admit_value(
            super::registry::EXTRACT_DATE_FIELDS,
            word,
            super::registry::ArgValue::ExtractField,
        )
        .or_else(|| {
            super::registry::admit_value(
                super::registry::EXTRACT_CLOCK_FIELDS,
                word,
                super::registry::ArgValue::ExtractField,
            )
        });
        return Ok(ValueType::Numeric);
    }
    if super::registry::admit_value(
        super::registry::DATE_TRUNC_UNITS,
        word,
        super::registry::ArgValue::DateTruncUnit,
    )
    .is_none()
    {
        return Err(literal_error(
            Some(word.clone()),
            format!(
                "the units admitted are {}",
                super::registry::values(super::registry::DATE_TRUNC_UNITS).join(", ")
            ),
        ));
    }
    if operand_t != TIMESTAMP_TYPE {
        return Err(operand_error(match operand_t {
            DATE_TYPE => {
                "Postgres resolves a date to the timestamptz overload, which reads the session \
                 time zone. Truncate the timestamp: DATE_TRUNC('month', CAST(d AS timestamp))"
            }
            TIMESTAMPTZ_TYPE => {
                "DATE_TRUNC of a timestamptz reads the session time zone. Truncate the \
                 timestamp in UTC: DATE_TRUNC('day', ts AT TIME ZONE 'UTC')"
            }
            _ => "DATE_TRUNC takes a timestamp",
        }));
    }
    Ok(TIMESTAMP_TYPE)
}

/// Types a call of a function with several signatures (`ABS`, `CEIL`,
/// `FLOOR`, `ROUND`, `TRUNC`): the overload whose declared argument types
/// equal the arguments' types, or the refusal that says why none does.
fn infer_overloaded_call(
    expr: &Expr,
    field_name: &str,
    columns: &ColumnTypes<'_>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
) -> Result<ValueType, ValidationError> {
    let Expr::FunctionCall { name, args } = expr else {
        unreachable!("only a call is typed here");
    };
    let mut arg_types = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        if super::registry::INTEGER_LITERAL_ARGS.contains(&(name.as_str(), i)) {
            // The scale: an integer literal, possibly negative, in `integer`
            // range, so the result's scale never depends on a row.
            let literal = matches!(arg, Expr::NumberLiteral(text) if text.parse::<i32>().is_ok());
            if !literal {
                return Err(ValidationError::ScaleNotIntegerLiteral {
                    field: field_name.to_string(),
                    function: name.clone(),
                });
            }
        }
        arg_types.push(infer_expr(
            arg,
            field_name,
            columns,
            fields_by_name,
            types,
            in_progress,
        )?);
    }
    let Some(spec) = super::registry::resolve_function(name, &arg_types) else {
        let found = arg_types[0];
        if !found.is_numeric_family() {
            return Err(ValidationError::FunctionArgTypeMismatch {
                field: field_name.to_string(),
                function: name.clone(),
                arg_index: 0,
                expected: ValueType::Numeric,
                found,
            });
        }
        let reason = match (name.as_str(), found, args.len()) {
            (_, ValueType::Integer(_), _) => {
                "an integer is already whole, and Postgres would cast it to double precision, \
                 changing the result's type; use the integer itself"
            }
            ("ROUND", ValueType::Float(_), _) => {
                "a float rounds half to even where numeric rounds half away from zero, so one \
                 name would carry two rules; round a numeric value"
            }
            (_, ValueType::Float(_), 1) => {
                "only numeric and double precision are admitted; Postgres promotes a real to \
                 double precision"
            }
            _ => "the scale form takes a numeric value",
        };
        return Err(ValidationError::NumericFunctionArgument {
            field: field_name.to_string(),
            function: name.clone(),
            found,
            reason,
        });
    };
    Ok(spec.return_type)
}

/// The [`ValueType`] an unadorned numeric literal carries, matching
/// Postgres's own rule: an integral literal that fits `integer` is
/// `integer`, one that only fits `bigint` is `bigint`, and a fractional
/// literal (or an integral one too wide even for `bigint`) is `numeric`.
///
/// The SQL renderer casts the literal to this type (`defs::oracle`'s
/// `number_literal_sql`), so the target column type and the computed value
/// agree.
pub(crate) fn number_literal_type(text: &str) -> ValueType {
    if !text.contains('.')
        && let Ok(value) = text.parse::<i64>()
    {
        return ValueType::Integer(IntWidth::narrowest_for(value));
    }
    ValueType::Numeric
}

/// The type two operands unify to, mirroring Postgres's own implicit
/// coercions *within the exact-numeric family*: two integers
/// unify to the wider width, and any mix with `numeric` unifies to
/// `numeric`. `None` for anything outside that family, or for two genuinely
/// different types — this is deliberately not a general coercion lattice,
/// only the same closed set [`super::registry::operator_result_type`]
/// admits.
pub(super) fn common_numeric_type(a: ValueType, b: ValueType) -> Option<ValueType> {
    use super::registry::{Admitted, TypeRule, UnifyRule};
    let (value_type, rule) = if a == b {
        (a, UnifyRule::Same)
    } else if !a.is_numeric_family() || !b.is_numeric_family() {
        return None;
    } else {
        match (a, b) {
            // `COALESCE` resolves through Postgres's
            // `select_common_type`, which is *not* the same algorithm
            // operator overload resolution uses, and the difference is
            // visible: a live server types `coalesce(1::real, 1::numeric)`
            // (and `coalesce(1::numeric, 1::real)`, and both orders of
            // `real`/`integer`) as **`real`**, while `1::real + 1::numeric`
            // is `double precision`. So a float wins over every exact type
            // here without being widened, and only a genuine `double
            // precision` operand produces `double precision`.
            (ValueType::Float(x), ValueType::Float(y)) => {
                (ValueType::Float(x.wider(y)), UnifyRule::Float)
            }
            (ValueType::Float(x), _) | (_, ValueType::Float(x)) => {
                (ValueType::Float(x), UnifyRule::FloatBesideExact)
            }
            (ValueType::Integer(x), ValueType::Integer(y)) => {
                (ValueType::Integer(x.wider(y)), UnifyRule::Integer)
            }
            _ => (ValueType::Numeric, UnifyRule::Numeric),
        }
    };
    Some(Admitted::new(value_type, TypeRule::Unify(rule)).into_type())
}

/// Checks `regexp_count`'s pattern argument is a string literal. Whether
/// Postgres accepts it is the catalog's probe, which needs a literal to bind
/// — see [`ValidationError::NonLiteralRegexPattern`].
fn validate_regexp_pattern(field_name: &str, pattern_arg: &Expr) -> Result<(), ValidationError> {
    if !matches!(pattern_arg, Expr::StringLiteral(_)) {
        return Err(ValidationError::NonLiteralRegexPattern {
            field: field_name.to_string(),
        });
    }
    Ok(())
}

/// Every `(field, pattern)` of a `REGEXP_COUNT` call with a string-literal
/// pattern in `def`'s fields, once each, in field order. The catalog has
/// Postgres judge each one (`CatalogError::InvalidRegexPattern`); a
/// non-literal pattern is [`validate`]'s refusal.
pub(crate) fn regexp_pattern_literals(def: &TransformDef) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for field in &def.fields {
        field.expr.walk(&mut |expr| {
            if let Expr::FunctionCall { name, args } = expr
                && name == "REGEXP_COUNT"
                && let Some(Expr::StringLiteral(pattern)) = args.get(1)
            {
                out.push((field.name.clone(), pattern.clone()));
            }
        });
    }
    let mut seen = HashSet::new();
    out.retain(|entry| seen.insert(entry.clone()));
    out
}

/// A calculated field's reads of other calculated fields, for the cycle and
/// depth walk.
struct FieldDeps {
    /// The height of the field's own expression.
    own_height: usize,
    /// Each calculated field it reads, with the deepest level of the
    /// expression a read sits at (the root is level 1).
    reads: Vec<(String, usize)>,
}

/// The height of `expr` (a leaf is `depth`), recording in `out` the deepest
/// level each column is read at. The parser bounds an expression's height, so
/// the recursion is bounded.
fn column_depths_in(expr: &Expr, depth: usize, out: &mut HashMap<String, usize>) -> usize {
    match expr {
        Expr::Column(name) => {
            let level = out.entry(name.clone()).or_insert(depth);
            *level = (*level).max(depth);
            depth
        }
        _ => expr
            .children()
            .into_iter()
            .map(|child| column_depths_in(child, depth + 1, out))
            .max()
            .unwrap_or(depth),
    }
}

#[derive(PartialEq, Eq)]
enum Mark {
    Visiting,
    /// Finished, with the height of the field's expression once every
    /// calculated field it reads is inlined.
    Done(usize),
}

/// DFS with a visiting/done mark set, hand-rolled per the repo's convention
/// of avoiding a graph crate for a problem this small. It also bounds how
/// deep the fields' reads of one another nest: a field's expression, with the
/// calculated fields it reads inlined (each read costing at least one level),
/// may be at most [`MAX_EXPRESSION_DEPTH`] high, so the passes that walk the
/// fields' dependency graph or an inlined expression recurse a bounded number
/// of times.
fn detect_cycle(deps: &HashMap<&str, FieldDeps>) -> Result<(), ValidationError> {
    let mut marks: HashMap<&str, Mark> = HashMap::with_capacity(deps.len());
    let mut names: Vec<&str> = deps.keys().copied().collect();
    names.sort_unstable();
    for name in names {
        if !marks.contains_key(name) {
            let mut stack = Vec::new();
            visit(name, deps, &mut marks, &mut stack)?;
        }
    }
    Ok(())
}

fn visit<'a>(
    name: &'a str,
    deps: &'a HashMap<&'a str, FieldDeps>,
    marks: &mut HashMap<&'a str, Mark>,
    stack: &mut Vec<String>,
) -> Result<usize, ValidationError> {
    match marks.get(name) {
        Some(Mark::Done(height)) => return Ok(*height),
        Some(Mark::Visiting) => {
            stack.push(name.to_string());
            return Err(ValidationError::Cycle {
                cycle: stack.clone(),
            });
        }
        None => {}
    }
    if stack.len() >= MAX_EXPRESSION_DEPTH {
        return Err(ValidationError::FieldExpansionTooLarge);
    }

    marks.insert(name, Mark::Visiting);
    stack.push(name.to_string());

    let mut height = 1;
    if let Some(field) = deps.get(name) {
        height = field.own_height;
        for (child, depth) in &field.reads {
            let child_height = visit(child.as_str(), deps, marks, stack)?;
            height = height.max(depth + child_height);
        }
    }
    if height > MAX_EXPRESSION_DEPTH {
        return Err(ValidationError::FieldExpansionTooLarge);
    }

    stack.pop();
    marks.insert(name, Mark::Done(height));
    Ok(height)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::Operator;
    use crate::defs::pg_type::PgType;
    use crate::float::FloatWidth;

    fn def(fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields,
            predicate: Predicate::True,
            without_source_index: false,
            adopt_target: false,
            source_key: None,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
        names
            .iter()
            .map(|n| (n.to_string(), ValueType::Numeric))
            .collect()
    }

    #[test]
    fn valid_definition_passes() {
        let d = def(vec![
            FieldDef {
                name: "double_price".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(col("price")),
                    rhs: Box::new(col("price")),
                },
            },
            FieldDef {
                name: "total".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(col("double_price")),
                    rhs: Box::new(Expr::NumberLiteral("1".to_string())),
                },
            },
        ]);
        let source_columns = numeric_columns(&["price"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn unresolved_column_is_rejected() {
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: col("mystery"),
        }]);
        let err = validate(&d, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UnresolvedColumn {
                field: "x".to_string(),
                column: "mystery".to_string(),
            }
        );
    }

    #[test]
    fn two_node_cycle_is_rejected() {
        // Hand-built AST, bypassing the parser entirely: field `x` is
        // defined in terms of `y` and vice versa.
        let d = def(vec![
            FieldDef {
                name: "x".to_string(),
                expr: col("y"),
            },
            FieldDef {
                name: "y".to_string(),
                expr: col("x"),
            },
        ]);
        let err = validate(&d, &HashMap::new()).unwrap_err();
        match err {
            ValidationError::Cycle { .. } => {}
            other => panic!("expected Cycle, got {other:?}"),
        }
    }

    #[test]
    fn self_reference_to_a_nonexistent_column_is_unresolved_not_a_cycle() {
        // `x`'s only reference is to `x` itself, and `x` is not a source
        // column — the field's own name matching the referenced name is
        // coincidental (the bare-passthrough idiom), not evidence that this
        // is "another calculated field" to depend on: a field
        // is never "another" calculated field relative to itself, so this
        // must resolve as an unknown column rather than a self-cycle.
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: col("x"),
        }]);
        let err = validate(&d, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UnresolvedColumn {
                field: "x".to_string(),
                column: "x".to_string(),
            }
        );
    }

    #[test]
    fn diamond_dependency_is_not_a_false_positive_cycle() {
        // D depends on A and B; A depends on C; B depends on C. C is a plain
        // source column, so the DFS revisits it via both branches — this
        // proves the Done mark (not just Visiting) is checked, or the second
        // branch would wrongly report a cycle.
        let d = def(vec![
            FieldDef {
                name: "a".to_string(),
                expr: col("c"),
            },
            FieldDef {
                name: "b".to_string(),
                expr: col("c"),
            },
            FieldDef {
                name: "d".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(col("a")),
                    rhs: Box::new(col("b")),
                },
            },
        ]);
        let source_columns = numeric_columns(&["c"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn a_calc_field_named_after_a_real_column_must_be_a_bare_passthrough() {
        // The exact shape: `total_posted_words` is a calculated-field
        // alias, but a real source column of the same name also exists. A
        // third field (not exercised here, since this alone is already
        // rejected) referencing that name would otherwise silently resolve
        // to the alias instead of the real column via `fields_by_name`.
        let d = def(vec![FieldDef {
            name: "total_posted_words".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(col("word_count")),
                rhs: Box::new(Expr::NumberLiteral("1".to_string())),
            },
        }]);
        let source_columns = numeric_columns(&["word_count", "total_posted_words"]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::CalculatedFieldShadowsSourceColumn {
                field: "total_posted_words".to_string(),
            }
        );
    }

    #[test]
    fn a_calc_field_bare_passthrough_of_a_same_named_column_is_still_allowed() {
        // The self-passthrough idiom must remain legal: a field
        // named after a real column, whose expression is exactly that
        // column, is unambiguous and carries no shadowing risk.
        let d = def(vec![FieldDef {
            name: "price".to_string(),
            expr: col("price"),
        }]);
        let source_columns = numeric_columns(&["price"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn target_equals_source_is_rejected() {
        let mut d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: Expr::NumberLiteral("1".to_string()),
        }]);
        d.target = "s".to_string();
        d.source = "s".to_string();
        let err = validate(&d, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::TargetEqualsSource {
                table: "s".to_string()
            }
        );
    }

    #[test]
    fn duplicate_field_name_is_rejected() {
        let d = def(vec![
            FieldDef {
                name: "x".to_string(),
                expr: Expr::NumberLiteral("1".to_string()),
            },
            FieldDef {
                name: "x".to_string(),
                expr: Expr::NumberLiteral("2".to_string()),
            },
        ]);
        let err = validate(&d, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::DuplicateFieldName {
                name: "x".to_string()
            }
        );
    }

    #[test]
    fn text_column_passthrough_is_accepted() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: col("text_col"),
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn uuid_column_passthrough_is_accepted() {
        let d = def(vec![FieldDef {
            name: "author".to_string(),
            expr: col("author"),
        }]);
        let source_columns = HashMap::from([("author".to_string(), ValueType::Uuid)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn uuid_column_works_as_an_aggregate_group_by_key() {
        let d = aggregate_def(
            &["author"],
            vec![
                FieldDef {
                    name: "author".to_string(),
                    expr: col("author"),
                },
                FieldDef {
                    name: "total_words".to_string(),
                    expr: Expr::FunctionCall {
                        name: "SUM".to_string(),
                        args: vec![col("word_count")],
                    },
                },
            ],
        );
        let source_columns = HashMap::from([
            ("author".to_string(), ValueType::Uuid),
            ("word_count".to_string(), ValueType::Numeric),
        ]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    /// A `GROUP BY` key is matched by its `::text`
    /// rendering, so a `ValueType::Other` column — passthrough-only per
    /// `docs/type-support.md`, with no key role yet — must be rejected here
    /// rather than silently become a text-matched key. `interval` is the
    /// sharpest case: Postgres holds `'1 day' = '24 hours'`, but those
    /// render differently, so a text-matched key would split one real group
    /// into two and diverge from the Postgres oracle.
    ///
    /// The two other key roles already gate on
    /// `join_key_types::is_text_stable_join_key_type`.
    #[test]
    fn an_other_typed_column_is_rejected_as_an_aggregate_group_by_key() {
        // `PgType::Bit` (fixed-length) is deliberately in this
        // refusal list even though `PgType::VarBit` right next to it is
        // admitted — see this function's own `VarBit` arm for the DDL-only
        // reason (not a text-rendering one) the two split. `PgType::TimestampTz`
        // is admitted because `TimeZone` is pinned everywhere `DateStyle`
        // is — see `defs_temporal.rs`'s live coverage.
        for pg_type in [
            PgType::Interval,
            PgType::Json,
            // `jsonb` genuinely has canonical, sorted key
            // order (unlike `interval`/`timestamptz`'s outright rendering
            // hazards), but an embedded number's scale is not
            // renormalized — see `crate::jsonb`'s module doc and
            // `reject_unsupported_group_by_key_type`'s own `Jsonb` note.
            PgType::Jsonb,
            PgType::Bit,
        ] {
            let d = aggregate_def(
                &["k"],
                vec![
                    FieldDef {
                        name: "k".to_string(),
                        expr: col("k"),
                    },
                    FieldDef {
                        name: "total_words".to_string(),
                        expr: Expr::FunctionCall {
                            name: "SUM".to_string(),
                            args: vec![col("word_count")],
                        },
                    },
                ],
            );
            let source_columns = HashMap::from([
                ("k".to_string(), ValueType::Other(pg_type)),
                ("word_count".to_string(), ValueType::Numeric),
            ]);
            assert_eq!(
                validate(&d, &source_columns),
                Err(ValidationError::UnsupportedGroupByKeyType {
                    column: "k".to_string(),
                    value_type: ValueType::Other(pg_type),
                }),
                "{pg_type} must not be accepted as a GROUP BY key"
            );
        }
    }

    /// `UnsupportedGroupByKeyType`'s `Display` impl must not name a
    /// second, hand-maintained copy of this gate's admission list that can
    /// drift stale.
    ///
    /// This test drives [`reject_unsupported_group_by_key_type`] directly
    /// with one representative value per type family it actually
    /// classifies — every admitted family alongside every still-refused one
    /// (`interval`, `real`/`double precision`, `jsonb`, `json`, fixed-length
    /// `bit`, `money`, `xml`, `tsvector`/`tsquery`, `unrecognized`) — and
    /// checks that [`supported_group_by_key_types`]'s rendering names
    /// exactly the families that came back `Ok`. A future admission change to that
    /// function that isn't mirrored in [`GROUP_BY_KEY_TYPE_NAMES`] fails
    /// here, the same drift guard the sibling join-key message has.
    #[test]
    fn unsupported_group_by_key_type_message_matches_the_real_admission_list() {
        const DEMO_ENUM: PgType = PgType::Enum("enum:public.demo_enum");
        let candidates: &[(&str, ValueType)] = &[
            // Admitted today.
            ("numeric", ValueType::Integer(IntWidth::Int2)),
            ("numeric", ValueType::Integer(IntWidth::Int4)),
            ("numeric", ValueType::Integer(IntWidth::Int8)),
            ("numeric", ValueType::Numeric),
            ("text", ValueType::Text),
            ("uuid", ValueType::Uuid),
            ("boolean", ValueType::Boolean),
            ("oid", ValueType::Other(PgType::Oid)),
            ("bytea", ValueType::Other(PgType::Bytea)),
            ("date", ValueType::Other(PgType::Date)),
            ("time without time zone", ValueType::Other(PgType::Time)),
            ("time with time zone", ValueType::Other(PgType::TimeTz)),
            (
                "timestamp without time zone",
                ValueType::Other(PgType::Timestamp),
            ),
            (
                "timestamp with time zone",
                ValueType::Other(PgType::TimestampTz),
            ),
            ("cidr", ValueType::Other(PgType::Cidr)),
            ("macaddr", ValueType::Other(PgType::MacAddr)),
            ("macaddr8", ValueType::Other(PgType::MacAddr8)),
            ("inet", ValueType::Other(PgType::Inet)),
            ("bit varying", ValueType::Other(PgType::VarBit)),
            ("enum types", ValueType::Other(DEMO_ENUM)),
            // Still refused today — must NOT show up in the message.
            ("real", ValueType::Float(FloatWidth::Float4)),
            ("double precision", ValueType::Float(FloatWidth::Float8)),
            ("interval", ValueType::Other(PgType::Interval)),
            ("json", ValueType::Other(PgType::Json)),
            ("jsonb", ValueType::Other(PgType::Jsonb)),
            ("bit", ValueType::Other(PgType::Bit)),
            ("money", ValueType::Other(PgType::Money)),
            ("xml", ValueType::Other(PgType::Xml)),
            ("tsvector", ValueType::Other(PgType::TsVector)),
            ("tsquery", ValueType::Other(PgType::TsQuery)),
            // `PgType::Unrecognized` is a real reachable `ValueType` (any
            // builtin OID `pg_type::value_type_for_builtin_oid` doesn't
            // know), so it belongs here for the same reason as the rest:
            // with it present, every one of `PgType`'s variants has a row
            // in this table, and the gate's `Other(_)` catch-all is pinned
            // as the answer for an unknown type rather than left untested.
            ("unrecognized", ValueType::Other(PgType::Unrecognized)),
        ];

        let mut admitted_names: Vec<&str> = Vec::new();
        for (name, value_type) in candidates.iter().copied() {
            // Admitting a key goes through a registry rule with a release:
            // an admission records exactly one, a refusal none.
            let (result, recorded) = crate::defs::floor::collect(|| {
                reject_unsupported_group_by_key_type("k", value_type)
            });
            let rules: Vec<_> = recorded
                .iter()
                .filter(|feature| {
                    matches!(
                        feature,
                        crate::defs::floor::Feature::TypeRule(
                            crate::defs::registry::TypeRule::GroupByKey(_)
                        )
                    )
                })
                .collect();
            match result {
                Ok(()) => {
                    assert_eq!(rules.len(), 1, "{value_type} admitted without one rule");
                    assert!(rules[0].since().is_some(), "{value_type}: no release");
                    if !admitted_names.contains(&name) {
                        admitted_names.push(name);
                    }
                }
                Err(ValidationError::UnsupportedGroupByKeyType { .. }) => {
                    assert!(rules.is_empty(), "{value_type} refused but recorded");
                }
                Err(other) => panic!("unexpected error for {value_type}: {other:?}"),
            }
        }

        let message = supported_group_by_key_types();
        for name in GROUP_BY_KEY_TYPE_NAMES {
            assert!(
                admitted_names.contains(name),
                "{name} is listed in GROUP_BY_KEY_TYPE_NAMES but no candidate for it is \
                 actually admitted by reject_unsupported_group_by_key_type; the message and \
                 the gate have drifted apart"
            );
        }
        for name in &admitted_names {
            assert!(
                GROUP_BY_KEY_TYPE_NAMES.contains(name),
                "{name} is admitted by reject_unsupported_group_by_key_type but is missing \
                 from GROUP_BY_KEY_TYPE_NAMES, so UnsupportedGroupByKeyType's message would \
                 no longer name every type it should — the exact drift this test guards against"
            );
        }
        // Split on the join separator rather than a raw substring `contains`
        // check: "bit" (refused) would otherwise falsely match inside "bit
        // varying" (admitted), which is exactly the wrong kind of pass.
        let message_tokens: Vec<&str> = message.split(", ").collect();
        for (name, value_type) in candidates {
            if !admitted_names.contains(name) {
                assert!(
                    !message_tokens.contains(name),
                    "'{name}' ({value_type}) is still refused by \
                     reject_unsupported_group_by_key_type but appears in the supported-types \
                     message"
                );
            }
        }
    }

    /// The counterpart to the above: an `Other`-typed column is still a
    /// perfectly good bare *passthrough* field — only
    /// the key role is gated.
    #[test]
    fn an_other_typed_column_passthrough_is_still_accepted() {
        let d = def(vec![FieldDef {
            name: "payload".to_string(),
            expr: col("payload"),
        }]);
        let source_columns =
            HashMap::from([("payload".to_string(), ValueType::Other(PgType::Jsonb))]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn boolean_column_passthrough_is_accepted() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: col("flag"),
        }]);
        let source_columns = HashMap::from([("flag".to_string(), ValueType::Boolean)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn adding_a_text_column_is_a_type_mismatch() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(col("text_col")),
                rhs: Box::new(Expr::NumberLiteral("1".to_string())),
            },
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::TypeMismatch {
                field: "out".to_string(),
                operator: "+",
                expected: ValueType::Numeric,
                found: ValueType::Text,
            }
        );
    }

    #[test]
    fn string_literal_type_checks_as_text() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::StringLiteral("hi".to_string()),
        }]);
        assert_eq!(validate(&d, &HashMap::new()), Ok(()));
    }

    #[test]
    fn function_call_over_a_text_column_type_checks_as_numeric() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "OCTET_LENGTH".to_string(),
                args: vec![col("text_col")],
            },
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn function_call_with_a_numeric_argument_is_a_type_mismatch() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "OCTET_LENGTH".to_string(),
                args: vec![col("number_col")],
            },
        }]);
        let source_columns = HashMap::from([("number_col".to_string(), ValueType::Numeric)]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::FunctionArgTypeMismatch {
                field: "out".to_string(),
                function: "OCTET_LENGTH".to_string(),
                arg_index: 0,
                expected: ValueType::Text,
                found: ValueType::Numeric,
            }
        );
    }

    #[test]
    fn coalesce_infers_the_common_argument_type() {
        // All arguments share a type, so COALESCE's result type is that type
        // (here Text), matching Postgres's `COALESCE(text, text) -> text`.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![col("text_col"), Expr::StringLiteral("fallback".to_string())],
            },
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
        let types = infer_field_types(&d, &source_columns).unwrap();
        assert_eq!(types["out"], ValueType::Text);
    }

    #[test]
    fn coalesce_with_incompatible_argument_types_is_rejected() {
        // `COALESCE(numeric, text)` is rejected — and Postgres rejects the
        // same expression ("COALESCE types numeric and text cannot be
        // matched"), so this is faithful, not a divergence. It pins the
        // exact-type-match rule the arm enforces.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![col("number_col"), col("text_col")],
            },
        }]);
        let source_columns = HashMap::from([
            ("number_col".to_string(), ValueType::Numeric),
            ("text_col".to_string(), ValueType::Text),
        ]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::FunctionArgTypeMismatch {
                field: "out".to_string(),
                function: "COALESCE".to_string(),
                arg_index: 1,
                expected: ValueType::Numeric,
                found: ValueType::Text,
            }
        );
    }

    #[test]
    fn coalesce_rejects_a_string_literal_postgres_would_coerce() {
        // DIVERGENCE from Postgres (tracked toward full compatibility):
        // Postgres types an *unadorned* literal like `'0'` as `unknown` and
        // coerces it to the other arguments' type, so `COALESCE(number, '0')`
        // resolves to `numeric` and succeeds. This grammar has no `unknown`
        // literal — a quoted literal is always `Text` (see `ast.rs`) — so the
        // same expression is rejected as a type mismatch. Users must instead
        // write a numeric literal (`COALESCE(number, 0)`), which is accepted.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![col("number_col"), Expr::StringLiteral("0".to_string())],
            },
        }]);
        let source_columns = numeric_columns(&["number_col"]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::FunctionArgTypeMismatch {
                field: "out".to_string(),
                function: "COALESCE".to_string(),
                arg_index: 1,
                expected: ValueType::Numeric,
                found: ValueType::Text,
            }
        );

        // The numeric-literal spelling Postgres also accepts is accepted here.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![col("number_col"), Expr::NumberLiteral("0".to_string())],
            },
        }]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn greater_than_over_numeric_operands_type_checks_as_boolean() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(col("number_col")),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }]);
        let source_columns = numeric_columns(&["number_col"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn greater_than_over_a_text_operand_is_a_comparison_type_mismatch() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(col("text_col")),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::ComparisonTypeMismatch {
                field: "out".to_string(),
                operator: ">",
                lhs: ValueType::Text,
                rhs: ValueType::Integer(IntWidth::Int4),
            }
        );
    }

    fn gt(lhs: Expr, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    #[test]
    fn greater_than_refuses_a_float_against_an_exact_numeric_in_both_orders() {
        let source_columns = HashMap::from([
            ("f".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("n".to_string(), ValueType::Numeric),
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        for (float, exact) in [("f", "n"), ("f", "i"), ("d", "n"), ("d", "i")] {
            for (l, r) in [(float, exact), (exact, float)] {
                let d = def(vec![FieldDef {
                    name: "out".to_string(),
                    expr: gt(col(l), col(r)),
                }]);
                assert_eq!(
                    validate(&d, &source_columns),
                    Err(ValidationError::MixedFloatExactComparison {
                        field: "out".to_string(),
                        operator: ">",
                        lhs: source_columns[l],
                        rhs: source_columns[r],
                    }),
                    "{l} > {r}"
                );
            }
        }
        // float > float and exact > exact stay admitted.
        for (l, r) in [("f", "d"), ("d", "f"), ("n", "i"), ("i", "n")] {
            let d = def(vec![FieldDef {
                name: "out".to_string(),
                expr: gt(col(l), col(r)),
            }]);
            assert_eq!(validate(&d, &source_columns), Ok(()), "{l} > {r}");
        }
    }

    #[test]
    fn greater_than_refuses_the_mix_through_a_calculated_float_field() {
        // `s` is a float (`real + integer` is `double precision`), so `s > n`
        // is a float against an exact numeric even though no source column
        // on either side of the `>` is a float... on its own.
        let source_columns = HashMap::from([
            ("f".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("n".to_string(), ValueType::Numeric),
        ]);
        for flip in [false, true] {
            let (l, r) = if flip { ("n", "s") } else { ("s", "n") };
            let d = def(vec![
                FieldDef {
                    name: "s".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(col("f")),
                        rhs: Box::new(Expr::NumberLiteral("1".to_string())),
                    },
                },
                FieldDef {
                    name: "out".to_string(),
                    expr: gt(col(l), col(r)),
                },
            ]);
            let err = validate(&d, &source_columns).unwrap_err();
            assert!(
                matches!(err, ValidationError::MixedFloatExactComparison { ref field, .. } if field == "out"),
                "{err:?}"
            );
            let msg = err.to_string();
            assert!(
                msg.contains("'out'")
                    && msg.contains("double precision")
                    && msg.contains("numeric"),
                "{msg}"
            );
        }
    }

    #[test]
    fn greater_than_admits_a_bare_number_literal_against_a_float() {
        // Postgres casts an untyped literal to `double precision`
        // when it meets a float (`explain verbose select r > 0.5` shows
        // `r > '0.5'::double precision`), so a *bare* literal is admitted in
        // either order and stays compared as a float. The same value in a
        // column, a calculated field or an expression is still refused, and
        // so is anything exact-typed (the grammar has no `NUMERIC '..'` /
        // `INTEGER '..'` literal, so those fail to parse).
        let source_columns = HashMap::from([
            ("f".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("n".to_string(), ValueType::Numeric),
            ("i".to_string(), ValueType::Integer(IntWidth::Int2)),
            ("b".to_string(), ValueType::Integer(IntWidth::Int8)),
        ]);
        for expr in [
            "f > 0.5",
            "f > 1",
            "0.5 > f",
            "1 > d",
            "d > 9223372036854775808",
            "d > 3000000000",
            "COALESCE(f, 0) > 0.5",
            "COALESCE(f, n) > 1",
            "f > f",
            "f > REAL '0.5'",
            "DOUBLE PRECISION '1' > d",
            "f > d",
            "i > 0.5",
            "b > i",
            "n > 1",
            "(i + b) > n",
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {expr} AS out"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{expr}");
        }
        for expr in [
            "COALESCE(f, n) > n",
            "(f + i) > b",
            "f > (1 + 1)",
            "f > (i + 1)",
            "(1 + 1) > d",
            "f > n",
            "i > d",
            "(f + 1) > n",
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {expr} AS out"));
            let err = validate(&d, &source_columns).unwrap_err();
            assert!(
                matches!(err, ValidationError::MixedFloatExactComparison { ref field, .. } if field == "out"),
                "{expr}: {err:?}"
            );
        }
        for expr in ["f > NUMERIC '0.5'", "f > INTEGER '1'"] {
            assert!(
                crate::defs::parser::parse(&format!("TRANSFORM t FROM s SELECT {expr} AS out"))
                    .is_err(),
                "{expr}"
            );
        }
    }

    /// Define never evaluates a constant. Each of these fails in Postgres
    /// while it plans the statement (`22003`), so every row fails and is
    /// quarantined at run time; define admits them all, as it does any
    /// expression whose rows may fail.
    #[test]
    fn a_constant_postgres_fails_while_planning_defines() {
        let source_columns = HashMap::from([
            ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("n".to_string(), ValueType::Numeric),
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        let beyond_real = format!("1{}", "0".repeat(39));
        let beyond_double = "9".repeat(310);
        let tiny = format!("0.{}1", "0".repeat(400));
        let max8 = "9223372036854775807";
        for fields in [
            "2147483647 + 1 AS out".to_string(),
            format!("{max8} + 1 AS out"),
            "2147483647 AS big, big + 1 AS out".to_string(),
            "i + (2147483647 + 1) AS out".to_string(),
            "COALESCE(i, 2147483647 + 1) AS out".to_string(),
            "-(-2147483647 - 1) AS out".to_string(),
            "ABS(-2147483647 - 1) AS out".to_string(),
            "REAL '3e+38' + REAL '3e+38' AS out".to_string(),
            format!("r + {beyond_double} AS out"),
            format!("d + {tiny} AS out"),
            format!("COALESCE(r, {beyond_real}) AS out"),
            format!("COALESCE(d, {beyond_double}) AS out"),
            format!("CASE WHEN i > 0 THEN r ELSE {beyond_real} END AS out"),
            format!("r > {beyond_double} AS out"),
            "DATE '2024-01-01' + 2147483647 AS out".to_string(),
            "CAST(DATE '9999-12-31' + 2000000000 AS timestamp) AS out".to_string(),
            "n / (1 - 1) AS out".to_string(),
            "n / COALESCE(0, 1) AS out".to_string(),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{fields}");
        }
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, SUM(2147483647 + 1) AS out",
        );
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    /// A `/` or `%` whose divisor is written as a literal zero is refused
    /// wherever it sits, guards and `NULL` operands included; a divisor
    /// that only computes to zero is not.
    #[test]
    fn a_literal_zero_divisor_is_refused_wherever_it_is_written() {
        let source_columns = HashMap::from([
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("n".to_string(), ValueType::Numeric),
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        for (expr, text) in [
            ("n / 0", "(n / 0)"),
            ("n / 0.0", "(n / 0.0)"),
            ("n / -0", "(n / -0)"),
            ("n / 00.000", "(n / 00.000)"),
            ("n % 0", "(n % 0)"),
            ("i % -0", "(i % -0)"),
            (
                "d / DOUBLE PRECISION '0'",
                "(d / CAST('0' AS DOUBLE PRECISION))",
            ),
            (
                "d / DOUBLE PRECISION '-0'",
                "(d / CAST('-0' AS DOUBLE PRECISION))",
            ),
            ("d / -REAL '0'", "(d / (-CAST('0' AS REAL)))"),
            ("n / CAST(0 AS numeric)", "(n / CAST(0 AS numeric))"),
            ("1.5 / 0", "(1.5 / 0)"),
            ("CASE WHEN false THEN n / 0 ELSE n END", "(n / 0)"),
            ("(n / 0) + NULL", "(n / 0)"),
            ("COALESCE(5, n / 0)", "(n / 0)"),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {expr} AS out"));
            assert_eq!(
                validate(&d, &source_columns),
                Err(ValidationError::ConstantDivisorZero {
                    field: "out".to_string(),
                    text: text.to_string(),
                }),
                "{expr}"
            );
        }
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, SUM(n) / 0 AS out",
        );
        assert!(
            matches!(
                validate(&d, &source_columns),
                Err(ValidationError::ConstantDivisorZero { .. })
            ),
            "SUM(n) / 0"
        );
        for expr in [
            "n / (1 - 1)",
            "n / COALESCE(0, 1)",
            "n / 0.5",
            "n / 10",
            "d / DOUBLE PRECISION '1e-300'",
            "0 / n",
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {expr} AS out"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{expr}");
        }
        // A calculated field is not the literal: `z` computes to zero.
        let d = parsed("TRANSFORM t FROM s SELECT 0 AS z, n / z AS out");
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn a_doubling_chain_of_constant_fields_validates_quickly() {
        // Each field reads the one before twice. Typing must not walk every
        // path through the chain (2^40 here): define refuses the chain for
        // its inlined size, after typing it.
        let mut fields = vec!["1.5 AS f0".to_string()];
        for k in 1..=40 {
            fields.push(format!("f{p} + f{p} AS f{k}", p = k - 1));
        }
        let d = parsed(&format!(
            "TRANSFORM t FROM s SELECT {}, COALESCE(DOUBLE PRECISION '1', f40) AS out",
            fields.join(", ")
        ));
        assert_eq!(
            validate(&d, &HashMap::new()),
            Err(ValidationError::FieldExpansionTooLarge)
        );
    }

    #[test]
    fn function_call_composed_with_greater_than_type_checks_as_boolean() {
        let d = def(vec![FieldDef {
            name: "has_foo".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::FunctionCall {
                    name: "STRPOS".to_string(),
                    args: vec![col("name"), Expr::StringLiteral("foo".to_string())],
                }),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }]);
        let source_columns = HashMap::from([("name".to_string(), ValueType::Text)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
        let types = infer_field_types(&d, &source_columns).unwrap();
        assert_eq!(types["has_foo"], ValueType::Boolean);
    }

    #[test]
    fn boolean_target_field_from_greater_than_is_accepted() {
        // A calculated field whose final expression type is Boolean must be
        // storable, not just intermediate (a "boolean target
        // column").
        let d = def(vec![FieldDef {
            name: "is_positive".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(col("number_col")),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }]);
        let source_columns = numeric_columns(&["number_col"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn regexp_count_with_a_literal_pattern_is_accepted() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "REGEXP_COUNT".to_string(),
                args: vec![col("text_col"), Expr::StringLiteral("a.c".to_string())],
            },
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn regexp_count_with_a_column_sourced_pattern_is_rejected() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "REGEXP_COUNT".to_string(),
                args: vec![col("text_col"), col("pattern_col")],
            },
        }]);
        let source_columns = HashMap::from([
            ("text_col".to_string(), ValueType::Text),
            ("pattern_col".to_string(), ValueType::Text),
        ]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::NonLiteralRegexPattern {
                field: "out".to_string(),
            }
        );
    }

    fn aggregate_def(group_by: &[&str], fields: Vec<FieldDef>) -> TransformDef {
        aggregate_def_with_keys(group_by.iter().map(|s| s.to_string()).collect(), fields)
    }

    fn aggregate_def_with_keys(group_by: Vec<String>, fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::Aggregate { group_by },
            fields,
            predicate: Predicate::True,
            without_source_index: false,
            adopt_target: false,
            source_key: None,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn a_valid_aggregate_definition_passes() {
        let d = aggregate_def(
            &["order_id"],
            vec![
                FieldDef {
                    name: "order_id".to_string(),
                    expr: col("order_id"),
                },
                FieldDef {
                    name: "total".to_string(),
                    expr: Expr::FunctionCall {
                        name: "SUM".to_string(),
                        args: vec![col("amount")],
                    },
                },
                FieldDef {
                    name: "avg_amount".to_string(),
                    expr: Expr::FunctionCall {
                        name: "AVG".to_string(),
                        args: vec![col("amount")],
                    },
                },
            ],
        );
        let source_columns = numeric_columns(&["order_id", "amount"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    /// `COUNT(<column>)` in a `GROUP BY` definition — counting
    /// non-null occurrences of a specific column, unlike `COUNT(*)` — parses
    /// and validates, typed `bigint` (`Integer(Int8)`), matching Postgres's
    /// own `count(x)`.
    #[test]
    fn count_of_a_column_in_a_group_by_is_accepted_and_typed_bigint() {
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "n".to_string(),
                    expr: Expr::FunctionCall {
                        name: "COUNT".to_string(),
                        args: vec![col("amount")],
                    },
                },
            ],
        );
        let source_columns = HashMap::from([
            ("tag".to_string(), ValueType::Numeric),
            ("amount".to_string(), ValueType::Numeric),
        ]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
        let types = infer_field_types(&d, &source_columns).unwrap();
        assert_eq!(
            types["n"],
            ValueType::Integer(crate::integer::IntWidth::Int8)
        );
    }

    #[test]
    fn group_by_on_a_nonexistent_column_is_rejected() {
        let d = aggregate_def(
            &["missing"],
            vec![FieldDef {
                name: "missing".to_string(),
                expr: col("missing"),
            }],
        );
        let err = validate(&d, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UnresolvedGroupByColumn {
                column: "missing".to_string()
            }
        );
    }

    #[test]
    fn rejects_a_bare_non_grouping_column_reference() {
        let d = aggregate_def(
            &["order_id"],
            vec![FieldDef {
                name: "amount".to_string(),
                expr: col("amount"),
            }],
        );
        let source_columns = numeric_columns(&["order_id", "amount"]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UngroupedColumnReference {
                field: "amount".to_string(),
                column: "amount".to_string(),
            }
        );
    }

    #[test]
    fn aggregating_over_the_grouping_key_itself_is_allowed() {
        let d = aggregate_def(
            &["order_id"],
            vec![FieldDef {
                name: "order_id_sum".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![col("order_id")],
                },
            }],
        );
        let source_columns = numeric_columns(&["order_id"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn a_field_named_after_the_grouping_column_must_be_a_bare_passthrough() {
        let d = aggregate_def(
            &["order_id"],
            vec![FieldDef {
                name: "order_id".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![col("order_id")],
                },
            }],
        );
        let source_columns = numeric_columns(&["order_id"]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::GroupingColumnFieldMustBePassthrough {
                field: "order_id".to_string(),
            }
        );
    }

    #[test]
    fn a_field_named_after_the_grouping_column_as_a_bare_passthrough_is_allowed() {
        let d = aggregate_def(
            &["order_id"],
            vec![FieldDef {
                name: "order_id".to_string(),
                expr: col("order_id"),
            }],
        );
        let source_columns = numeric_columns(&["order_id"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    fn count_star_field() -> FieldDef {
        FieldDef {
            name: "c".to_string(),
            expr: Expr::FunctionCall {
                name: "COUNT".to_string(),
                args: Vec::new(),
            },
        }
    }

    #[test]
    fn a_group_by_key_listed_twice_is_rejected() {
        let d = aggregate_def_with_keys(
            vec!["region".to_string(), "region".to_string()],
            vec![count_star_field()],
        );
        let source_columns = numeric_columns(&["region"]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::GroupByKeysShareTargetColumn {
                first: "region".to_string(),
                second: "region".to_string(),
                column: "region".to_string(),
            }
        );
        let message = err.to_string();
        assert!(message.contains("lists 'region' twice"), "{message}");
    }

    #[test]
    fn aggregate_function_argument_must_be_numeric() {
        let d = aggregate_def(
            &["id"],
            vec![FieldDef {
                name: "total".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![col("label")],
                },
            }],
        );
        let source_columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("label".to_string(), ValueType::Text),
        ]);
        let err = validate(&d, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::FunctionArgTypeMismatch {
                field: "total".to_string(),
                function: "SUM".to_string(),
                arg_index: 0,
                expected: ValueType::Numeric,
                found: ValueType::Text,
            }
        );
    }

    fn parsed(text: &str) -> TransformDef {
        crate::defs::parser::parse(text).expect("parse")
    }

    #[test]
    fn a_field_named_like_an_aggregate_targets_hidden_columns_is_rejected() {
        // Each of these named one of the hidden columns the
        // aggregate target DDL adds (`__{field}_count`, `__{field}_sum`,
        // then `__trellis_members`), so validation passed and install then
        // failed with a raw `42701 column specified more than once`.
        let source_columns = numeric_columns(&["g", "x"]);
        for (text, field) in [
            (
                "TRANSFORM t FROM s GROUP BY g SELECT SUM(x) AS total, COUNT(*) AS __total_count",
                "__total_count",
            ),
            (
                "TRANSFORM t FROM s GROUP BY g SELECT AVG(x) AS a, COUNT(*) AS __a_sum",
                "__a_sum",
            ),
            (
                "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS __trellis_members",
                "__trellis_members",
            ),
        ] {
            let err = validate(&parsed(text), &source_columns).unwrap_err();
            assert_eq!(
                err,
                ValidationError::ReservedFieldName {
                    field: field.to_string()
                },
                "{text}"
            );
            assert!(err.to_string().contains("reserves"), "{err}");
        }
    }

    #[test]
    fn a_target_named_like_a_ledger_or_too_long_for_one_is_rejected() {
        // `t__ledger` is `t`'s ledger, and a name past 55 bytes gives
        // a ledger name Postgres would truncate.
        let columns = numeric_columns(&["g", "x"]);
        let err = validate(
            &parsed("TRANSFORM t__ledger FROM s GROUP BY g SELECT COUNT(*) AS n"),
            &columns,
        )
        .unwrap_err();
        assert_eq!(
            err,
            ValidationError::ReservedTargetSuffix {
                target: "t__ledger".to_string(),
                suffix: "__ledger",
            }
        );
        assert!(err.to_string().contains("__ledger"), "{err}");
        let longest = "t".repeat(crate::defs::ledger::MAX_TARGET_NAME_LEN);
        assert_eq!(
            validate(
                &parsed(&format!("TRANSFORM {longest} FROM s SELECT x AS y")),
                &columns,
            ),
            Ok(())
        );
        let too_long = format!("{longest}t");
        assert_eq!(
            validate(
                &parsed(&format!("TRANSFORM {too_long} FROM s SELECT x AS y")),
                &columns,
            ),
            Err(ValidationError::TargetNameTooLong {
                target: too_long,
                max: 55
            })
        );
    }

    #[test]
    fn a_double_underscore_field_name_is_reserved_on_a_one_to_one_transform_too() {
        // The prefix is reserved uniformly rather than only where a hidden
        // column exists today, so the rule is one a user can learn once.
        let err = validate(
            &parsed("TRANSFORM t FROM s SELECT x AS __x"),
            &numeric_columns(&["x"]),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ValidationError::ReservedFieldName {
                field: "__x".to_string()
            }
        );
    }

    #[test]
    fn a_single_leading_underscore_field_name_is_still_allowed() {
        assert_eq!(
            validate(
                &parsed("TRANSFORM t FROM s GROUP BY g SELECT SUM(x) AS _total"),
                &numeric_columns(&["g", "x"]),
            ),
            Ok(())
        );
    }

    #[test]
    fn a_group_by_key_whose_target_column_is_reserved_is_rejected() {
        // `GROUP BY __a_sum ... AVG(x) AS a` would declare `__a_sum` twice:
        // once as the key's column, once as `a`'s hidden running sum.
        let err = validate(
            &parsed("TRANSFORM t FROM s GROUP BY __a_sum SELECT AVG(x) AS a"),
            &numeric_columns(&["__a_sum", "x"]),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ValidationError::ReservedGroupByColumnName {
                key: "__a_sum".to_string(),
                column: "__a_sum".to_string(),
            }
        );
    }

    #[test]
    fn a_field_named_after_a_primary_key_column_is_rejected() {
        // The target already carries the key column, so a field of
        // the same name, a bare passthrough or not, would declare it twice.
        for text in [
            "TRANSFORM t FROM s SELECT id AS id, amount AS amount",
            "TRANSFORM t FROM s SELECT amount AS amount, amount AS id",
            "TRANSFORM t FROM s SELECT id + 1 AS id",
        ] {
            let err = reject_primary_key_named_fields(&parsed(text).fields, &["id"]).unwrap_err();
            assert_eq!(
                err,
                ValidationError::FieldNamedAfterPrimaryKey {
                    field: "id".to_string()
                },
                "{text}"
            );
            assert!(err.to_string().contains("already carries"), "{err}");
        }
    }

    #[test]
    fn a_field_named_after_any_column_of_a_composite_key_is_rejected() {
        let d = parsed("TRANSFORM t FROM s SELECT x AS x, b AS b");
        assert_eq!(
            reject_primary_key_named_fields(&d.fields, &["a", "b"]),
            Err(ValidationError::FieldNamedAfterPrimaryKey {
                field: "b".to_string()
            })
        );
    }

    #[test]
    fn a_primary_key_column_under_another_name_is_allowed() {
        // `id AS order_id` is an ordinary column that happens to copy the key.
        let d = parsed("TRANSFORM t FROM s SELECT id AS order_id, amount AS amount");
        assert_eq!(reject_primary_key_named_fields(&d.fields, &["id"]), Ok(()));
    }

    fn source(column: &str) -> String {
        column.to_string()
    }

    fn read(field: &str, function: &str, column: String) -> CollationRefusingRead {
        CollationRefusingRead {
            field: field.to_string(),
            function: function.to_string(),
            qualifier: None,
            column,
        }
    }

    fn qualified_read(
        field: &str,
        function: &str,
        table: &str,
        column: &str,
    ) -> CollationRefusingRead {
        CollationRefusingRead {
            qualifier: Some(QualifiedColumn {
                schema: None,
                table: table.to_string(),
                column: column.to_string(),
            }),
            ..read(field, function, column.to_string())
        }
    }

    /// Every column under a `STRPOS`/`REGEXP_COUNT`
    /// argument is reported, directly, through `COALESCE` and through another
    /// field, each once and attributed to the field that makes the call.
    /// `CHAR_LENGTH`/`OCTET_LENGTH` don't depend on collation and report
    /// nothing.
    #[test]
    fn collation_refusing_reads_follow_coalesce_and_fields() {
        let d = parsed(
            "TRANSFORM t FROM s SELECT \
             strpos(title, 'x') AS direct, \
             coalesce(title, body) AS merged, \
             regexp_count(merged, 'a') AS via_field, \
             strpos(s.author, lower_title) AS via_qualified, \
             title AS lower_title, \
             char_length(body) + octet_length(body) AS lengths, \
             strpos(title, title) AS twice",
        );
        assert_eq!(
            collation_refusing_function_reads(&d),
            vec![
                read("direct", "STRPOS", source("title")),
                read("via_field", "REGEXP_COUNT", source("title")),
                read("via_field", "REGEXP_COUNT", source("body")),
                qualified_read("via_qualified", "STRPOS", "s", "author"),
                read("via_qualified", "STRPOS", source("title")),
                read("twice", "STRPOS", source("title")),
            ]
        );
    }

    /// A field named after the source column it passes through reads that
    /// column, not itself, and a call nested in an aggregate is still found.
    #[test]
    fn collation_refusing_reads_resolve_a_self_passthrough_and_nested_calls() {
        let d = parsed("TRANSFORM t FROM s SELECT title AS title, strpos(title, 'x') AS pos");
        assert_eq!(
            collation_refusing_function_reads(&d),
            vec![read("pos", "STRPOS", source("title"))]
        );
        let d =
            parsed("TRANSFORM t FROM s GROUP BY region SELECT sum(regexp_count(note, 'a')) AS n");
        assert_eq!(
            collation_refusing_function_reads(&d),
            vec![read("n", "REGEXP_COUNT", source("note"))]
        );
        let d = parsed("TRANSFORM t FROM s SELECT char_length(title) AS n");
        assert_eq!(collation_refusing_function_reads(&d), vec![]);
    }

    /// Inside an aggregate's argument, an aggregate field's name is
    /// the source column, so its collation is the one checked.
    #[test]
    fn collation_refusing_reads_in_an_aggregate_argument_name_the_source_column() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(n) AS title, \
             SUM(strpos(title, 'x')) AS m",
        );
        assert_eq!(
            collation_refusing_function_reads(&d),
            vec![read("m", "STRPOS", source("title"))]
        );
    }

    /// Inside an aggregate's argument, a field's name that matches
    /// a source column reads the source column, so the definition validates
    /// and `lo` has the source column's type, not the sum's.
    #[test]
    fn an_aggregate_argument_reads_the_source_column_not_a_field_of_that_name() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS val, \
             COUNT(*) AS row_count, SUM(val) AS lo, SUM(val) AS hi",
        );
        let int4 = ValueType::Integer(crate::integer::IntWidth::Int4);
        let source_columns = HashMap::from([
            ("grp".to_string(), ValueType::Text),
            ("val".to_string(), int4),
        ]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
        let types = infer_field_types(&d, &source_columns).unwrap();
        assert_eq!(
            types["lo"],
            ValueType::Integer(crate::integer::IntWidth::Int8),
            "SUM of the int4 source column"
        );
        assert_eq!(
            types["val"],
            ValueType::Integer(crate::integer::IntWidth::Int8),
            "SUM of an int4 is an int8"
        );
    }

    /// A name that is no source column and names another aggregate field
    /// would nest an aggregate: refused, not left to fail every build.
    #[test]
    fn an_aggregate_of_another_aggregate_field_is_rejected() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS total, \
             SUM(total) AS biggest",
        );
        let source_columns = numeric_columns(&["grp", "val"]);
        assert_eq!(
            validate(&d, &source_columns),
            Err(ValidationError::AggregateOfAggregateField {
                field: "biggest".to_string(),
                reference: "total".to_string(),
            })
        );
        // Through a field that reads one: `double` is an aggregate too.
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS total, \
             total + total AS double, SUM(double) AS biggest",
        );
        assert!(matches!(
            validate(&d, &source_columns),
            Err(ValidationError::AggregateOfAggregateField { .. })
        ));
    }

    /// An aggregate argument still reads a field that isn't an aggregate.
    #[test]
    fn an_aggregate_argument_still_reads_a_non_aggregate_field() {
        let d = def(vec![
            FieldDef {
                name: "adj".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(col("id")),
                    rhs: Box::new(Expr::NumberLiteral("1".to_string())),
                },
            },
            FieldDef {
                name: "t".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![col("adj")],
                },
            },
        ]);
        let source_columns = numeric_columns(&["id"]);
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    // A bare name that is both a source column and a field whose
    // expression is something else is refused at define; a qualified
    // `<table>.<column>` reads the source column.

    fn ambiguity(field: &str, name: &str, shadowing_expr: &str) -> ValidationError {
        ValidationError::AmbiguousColumnReference(Box::new(AmbiguousColumnReference {
            field: field.to_string(),
            name: name.to_string(),
            shadowing_expr: shadowing_expr.to_string(),
            source_table: "s".to_string(),
            source_schema: None,
        }))
    }

    /// The decision's example: a non-aggregate field shadowing a source
    /// column, read in an aggregate's argument. It used to be accepted as
    /// `SUM(grp + 1)`.
    #[test]
    fn a_shadowed_name_in_an_aggregate_argument_is_refused() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, (grp + 1) AS val, \
             SUM(val) AS total",
        );
        let source_columns = numeric_columns(&["grp", "val"]);
        assert_eq!(
            validate(&d, &source_columns),
            Err(ambiguity("total", "val", "(grp + 1)"))
        );
    }

    /// The message names both meanings and both qualified spellings, and the
    /// way to read the field.
    #[test]
    fn the_ambiguity_message_names_both_meanings_and_both_spellings() {
        let message = ambiguity("total", "val", "(grp + 1)").to_string();
        assert_eq!(
            message,
            "calculated field 'total' reads 'val', which is ambiguous: it names both the \
             source column 'val' of 's' and the calculated field 'val' (`(grp + 1)`). Write \
             `s.val` or `<schema>.s.val` to read the source column, or rename the field 'val' \
             and read it by its new name"
        );
        // Once the catalog has resolved the source's schema, it is spelled.
        let message = ambiguity("total", "val", "(grp + 1)")
            .with_source_schema("public")
            .to_string();
        assert!(message.contains("`s.val` or `public.s.val`"), "{message}");

        // A FROM clause that names its schema spells it directly.
        let d = parsed(
            "TRANSFORM t FROM app.s GROUP BY grp SELECT grp AS grp, (grp + 1) AS val, \
             SUM(val) AS total",
        );
        let err = validate(&d, &numeric_columns(&["grp", "val"])).unwrap_err();
        assert!(err.to_string().contains("`s.val` or `app.s.val`"), "{err}");
        // ... and the catalog's resolved schema doesn't override it.
        assert_eq!(err.clone().with_source_schema("public"), err);
    }

    /// Outside an aggregate's argument, an aggregate field shadowing a source
    /// column is ambiguous too (it used to be refused as an ungrouped column).
    #[test]
    fn a_shadowed_name_outside_an_aggregate_argument_is_refused() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS val, \
             val + 1 AS bumped",
        );
        assert_eq!(
            validate(&d, &numeric_columns(&["grp", "val"])),
            Err(ambiguity("bumped", "val", "SUM(val)"))
        );
    }

    /// `grp AS grp` and `val AS val` pass their column through, so reading
    /// the name elsewhere means the one thing it can.
    #[test]
    fn a_self_named_passthrough_is_not_ambiguous() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, grp + 1 AS next_grp, \
             SUM(grp) AS grp_sum",
        );
        assert_eq!(validate(&d, &numeric_columns(&["grp", "val"])), Ok(()));
        let d = parsed("TRANSFORM t FROM s SELECT val AS val, val + 1 AS bumped");
        assert_eq!(validate(&d, &numeric_columns(&["id", "val"])), Ok(()));
    }

    /// `s.val AS val` and `app.s.val AS val` are the passthrough of `val`
    /// as `val AS val` is: a 1-1 field, an aggregate's `GROUP BY` key and the
    /// reserved-word column that can only be written this way. The stored
    /// text a rendered `ALTER` re-parses is the same definition.
    #[test]
    fn a_qualified_self_named_read_is_the_passthrough() {
        let columns = numeric_columns(&["id", "val", "grp", "order"]);
        for text in [
            "TRANSFORM t FROM s SELECT s.val AS val, val + 1 AS bumped",
            "TRANSFORM t FROM app.s SELECT app.s.val AS val, s.order AS order",
            "TRANSFORM t FROM s GROUP BY grp SELECT s.grp AS grp, SUM(val) AS total",
            "TRANSFORM t FROM s GROUP BY s.order SELECT s.order AS order, SUM(val) AS total",
        ] {
            let d = parsed(text);
            assert_eq!(validate(&d, &columns), Ok(()), "{text}");
            let again = parsed(&crate::defs::ast::render_definition_text(&d));
            assert_eq!(again, d, "{text}");
        }
    }

    /// Only the field's own column, read through the `FROM` table, is its
    /// passthrough: another column, an expression, an aggregate or another
    /// table's column under the name is still the shadow the guard refuses.
    #[test]
    fn a_qualified_read_that_is_not_the_passthrough_is_refused() {
        let columns = numeric_columns(&["id", "val", "grp"]);
        for text in [
            "TRANSFORM t FROM s SELECT s.grp AS val",
            "TRANSFORM t FROM s SELECT (s.val + 1) AS val",
        ] {
            assert_eq!(
                validate(&parsed(text), &columns),
                Err(ValidationError::CalculatedFieldShadowsSourceColumn {
                    field: "val".to_string()
                }),
                "{text}"
            );
        }
        for text in [
            "TRANSFORM t FROM s GROUP BY grp SELECT s.val AS grp",
            "TRANSFORM t FROM s GROUP BY grp SELECT SUM(s.val) AS grp",
        ] {
            assert_eq!(
                validate(&parsed(text), &columns),
                Err(ValidationError::GroupingColumnFieldMustBePassthrough {
                    field: "grp".to_string()
                }),
                "{text}"
            );
        }
        // Another table's column under the field's name is refused as such,
        // not taken for the passthrough of the `FROM` table's.
        assert!(matches!(
            validate(
                &parsed("TRANSFORM t FROM s SELECT other.val AS val"),
                &columns
            ),
            Err(ValidationError::QualifiedColumnNotFromSource { .. })
        ));
    }

    /// An aggregate field's name inside another aggregate's argument is
    /// the source column, so it isn't ambiguous; nor is a field reading its
    /// own name (`SUM(val) AS val`).
    #[test]
    fn an_aggregate_field_inside_another_aggregate_is_not_ambiguous() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS val, \
             SUM(val) AS lo, SUM(val) AS hi",
        );
        assert_eq!(validate(&d, &numeric_columns(&["grp", "val"])), Ok(()));
    }

    /// A bare name still reads the field when no source column has that name.
    #[test]
    fn a_bare_name_no_source_column_has_reads_the_field() {
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, (grp + 1) AS adj, \
             SUM(adj) AS total",
        );
        assert_eq!(validate(&d, &numeric_columns(&["grp", "val"])), Ok(()));
    }

    /// `s.val` and `app.s.val` read the source column the field `val`
    /// shadows: the argument is typed as the source column, not the field.
    #[test]
    fn a_qualified_name_reads_the_shadowed_source_column() {
        let int4 = ValueType::Integer(crate::integer::IntWidth::Int4);
        let source_columns = HashMap::from([
            ("grp".to_string(), ValueType::Text),
            ("val".to_string(), int4),
        ]);
        for text in [
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, grp AS val, \
             SUM(s.val) AS lo",
            "TRANSFORM t FROM app.s GROUP BY grp SELECT grp AS grp, grp AS val, \
             SUM(app.s.val) AS lo",
        ] {
            let d = parsed(text);
            assert_eq!(validate(&d, &source_columns), Ok(()), "{text}");
            let types = infer_field_types(&d, &source_columns).unwrap();
            assert_eq!(types["val"], ValueType::Text, "{text}");
            assert_eq!(
                types["lo"],
                ValueType::Integer(crate::integer::IntWidth::Int8),
                "SUM of the int4 source column: {text}"
            );
        }
    }

    /// A two-part name whose head isn't the FROM table is refused, naming the
    /// reference and the FROM table: in a field, inside an aggregate's
    /// argument, and in an `ALTER TRANSFORM` clause, which parses to the same
    /// qualified column.
    #[test]
    fn a_two_part_name_of_another_table_is_refused() {
        let source_columns = numeric_columns(&["grp", "val"]);
        let refused = |field: &str| ValidationError::QualifiedColumnNotFromSource {
            field: field.to_string(),
            reference: "product.cost".to_string(),
            source: "s".to_string(),
        };
        let err = validate(
            &parsed("TRANSFORM t FROM s SELECT val + product.cost AS c"),
            &source_columns,
        )
        .unwrap_err();
        assert_eq!(err, refused("c"));
        assert_eq!(
            err.to_string(),
            "calculated field 'c' reads 'product.cost', but a qualified column must name this \
             transform's FROM table ('s'); a second table is read with a JOIN"
        );
        assert_eq!(
            validate(
                &parsed(
                    "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, \
                     SUM(product.cost) AS total"
                ),
                &source_columns,
            ),
            Err(refused("total"))
        );
        let crate::defs::ast::Statement::AlterTransform(alter) =
            crate::defs::parser::parse_statement("ALTER TRANSFORM t ADD product.cost AS c")
                .expect("parse")
        else {
            panic!("not an ALTER");
        };
        let crate::defs::ast::AlterClause::Add(field) = &alter.clauses[0] else {
            panic!("not an ADD");
        };
        let mut def = parsed("TRANSFORM t FROM s SELECT val AS v");
        def.fields.push(field.clone());
        assert_eq!(validate(&def, &source_columns), Err(refused("c")));
    }

    /// A qualified name must name the FROM table and, when known, its schema.
    #[test]
    fn a_qualified_name_of_another_table_is_refused() {
        let source_columns = numeric_columns(&["grp", "val"]);
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(app.other.val) AS total",
        );
        assert_eq!(
            validate(&d, &source_columns),
            Err(ValidationError::QualifiedColumnNotFromSource {
                field: "total".to_string(),
                reference: "app.other.val".to_string(),
                source: "s".to_string(),
            })
        );
        // The FROM clause names schema `app`; `other.s.val` names another.
        let d = parsed(
            "TRANSFORM t FROM app.s GROUP BY grp SELECT grp AS grp, SUM(other.s.val) AS total",
        );
        assert_eq!(
            validate(&d, &source_columns),
            Err(ValidationError::QualifiedColumnNotFromSource {
                field: "total".to_string(),
                reference: "other.s.val".to_string(),
                source: "app.s".to_string(),
            })
        );
        // A bare FROM: the catalog checks the schema once it has resolved it.
        let d =
            parsed("TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(other.s.val) AS total");
        assert_eq!(validate(&d, &source_columns), Ok(()));
        assert_eq!(
            validate_qualified_columns(&d, &source_columns, Some("public")),
            Err(ValidationError::QualifiedColumnNotFromSource {
                field: "total".to_string(),
                reference: "other.s.val".to_string(),
                source: "public.s".to_string(),
            })
        );
        assert_eq!(
            validate_qualified_columns(&d, &source_columns, Some("other")),
            Ok(())
        );
    }

    #[test]
    fn a_qualified_name_of_a_missing_column_is_unresolved() {
        let d = parsed("TRANSFORM t FROM s SELECT s.nope + 1 AS x");
        assert_eq!(
            validate(&d, &numeric_columns(&["id", "val"])),
            Err(ValidationError::UnresolvedColumn {
                field: "x".to_string(),
                column: "s.nope".to_string(),
            })
        );
    }

    /// A qualified name is still a source column for the grouping rules.
    #[test]
    fn a_qualified_ungrouped_column_outside_an_aggregate_is_refused() {
        let source_columns = numeric_columns(&["grp", "val"]);
        let d = parsed("TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, s.val AS v");
        assert_eq!(
            validate(&d, &source_columns),
            Err(ValidationError::UngroupedColumnReference {
                field: "v".to_string(),
                column: "s.val".to_string(),
            })
        );
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY s.grp SELECT grp AS grp, s.grp + 1 AS next_grp, \
             COUNT(*) AS n",
        );
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    /// A 1-1 field can't shadow a source column at all, so a 1-1
    /// field reading a shadowing alias is refused at the alias; the qualified
    /// name reads the column in a 1-1 definition too.
    #[test]
    fn a_one_to_one_field_reading_a_shadowing_alias_is_refused_at_the_alias() {
        let source_columns = numeric_columns(&["id", "val"]);
        let d = parsed("TRANSFORM t FROM s SELECT (val + 1) AS val, val + 2 AS other");
        assert_eq!(
            validate(&d, &source_columns),
            Err(ValidationError::CalculatedFieldShadowsSourceColumn {
                field: "val".to_string(),
            })
        );
        let d = parsed("TRANSFORM t FROM s SELECT s.val + 2 AS other");
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    // ---- JOIN validation ----

    use crate::defs::parser::parse as parse_text;

    fn jcol(value_type: ValueType, pg_type: &str, type_oid: u32, typmod: i32) -> JoinColumnInfo {
        JoinColumnInfo {
            value_type,
            pg_type: pg_type.to_string(),
            type_oid,
            typmod,
            collation_oid: 0,
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            copy_type: pg_type.split('(').next().unwrap_or(pg_type).to_string(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    fn int4() -> JoinColumnInfo {
        jcol(ValueType::Integer(IntWidth::Int4), "integer", 23, -1)
    }

    fn side(cols: &[(&str, JoinColumnInfo)]) -> HashMap<String, JoinColumnInfo> {
        cols.iter()
            .map(|(name, info)| (name.to_string(), info.clone()))
            .collect()
    }

    /// A `JOIN` read resolves against the side its qualifier names, also when
    /// it reaches the call through another field: `items.note` is checked
    /// against `items`, though `orders` has no `note`, and `orders.region`
    /// against `orders`.
    #[test]
    fn join_collation_reads_resolve_against_the_qualified_side_through_fields() {
        let (mut left, mut right) = tables();
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             SELECT items.note AS n, orders.region AS r, \
                    STRPOS(n, 'a') AS pos, r = 'x' AS same",
        )
        .unwrap();
        let KeySpace::JoinOneToOne(join) = &def.key_space else {
            panic!("a join without GROUP BY");
        };
        assert_eq!(
            check_join_collation_reads(&def, join, &left, &[&right]),
            Ok(())
        );
        let ci = |info: &mut JoinColumnInfo| {
            info.collation_deterministic = false;
            info.collation = Some("ci".to_string());
        };
        ci(right.get_mut("note").unwrap());
        let refused = |err: Result<(), ValidationError>| match err {
            Err(ValidationError::NondeterministicCollationTextFunction(refused)) => (
                refused.field,
                refused.function,
                refused.table,
                refused.column,
            ),
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert_eq!(
            refused(check_join_collation_reads(&def, join, &left, &[&right])),
            ("pos".into(), "STRPOS".into(), "items".into(), "note".into())
        );
        ci(right.get_mut("sku").unwrap());
        ci(left.get_mut("region").unwrap());
        right.remove("note");
        assert_eq!(
            refused(check_join_collation_reads(&def, join, &left, &[&right])),
            ("same".into(), "=".into(), "orders".into(), "region".into())
        );
    }

    /// orders(id, region text, total numeric(10,2)) and
    /// items(order_id, sku text, qty int4, price numeric, weight float8, note text).
    fn tables() -> (
        HashMap<String, JoinColumnInfo>,
        HashMap<String, JoinColumnInfo>,
    ) {
        let text = || jcol(ValueType::Text, "text", 25, -1);
        (
            side(&[
                ("id", int4()),
                ("region", text()),
                (
                    "total",
                    jcol(ValueType::Numeric, "numeric(10,2)", 1700, 655366),
                ),
            ]),
            side(&[
                ("order_id", int4()),
                ("sku", text()),
                ("qty", int4()),
                ("price", jcol(ValueType::Numeric, "numeric", 1700, -1)),
                (
                    "weight",
                    jcol(
                        ValueType::Float(FloatWidth::Float8),
                        "double precision",
                        701,
                        -1,
                    ),
                ),
                ("note", text()),
            ]),
        )
    }

    fn check(select: &str) -> Result<(), ValidationError> {
        check_text(&format!(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT {select}"
        ))
    }

    fn check_text(text: &str) -> Result<(), ValidationError> {
        let (left, right) = tables();
        validate_join(&parse_text(text).unwrap(), &left, &[&right])
    }

    #[test]
    fn join_admits_every_aggregate_shape_from_either_side() {
        check("orders.region AS region, COUNT(*) AS n").unwrap();
        check("orders.region AS region, COUNT(items.note) AS n").unwrap();
        check("orders.region AS region, COUNT(items.price) AS n").unwrap();
        check("orders.region AS region, SUM(items.qty) AS s, AVG(items.qty) AS a").unwrap();
        check("orders.region AS region, SUM(orders.total) AS s, AVG(orders.total) AS a").unwrap();
        // Keys from either side, and a left join.
        check_text(
            "TRANSFORM t FROM orders LEFT JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region, items.sku \
             SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n",
        )
        .unwrap();
        // Both ON orders and a conjunction.
        check_text(
            "TRANSFORM t FROM orders JOIN items ON items.order_id = orders.id AND \
             items.sku = orders.region GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
    }

    #[test]
    fn join_defines_a_constant_postgres_fails_while_planning() {
        // A JOIN's fields are typed by the same rule as a 1-1's: define
        // evaluates no constant, so the rows fail at run time instead.
        for select in [
            "orders.region AS region, SUM(items.qty + (2147483647 + 1)) AS s",
            "orders.region AS region, SUM(COALESCE(items.qty, 2147483647 + 1)) AS s",
            "orders.region AS region, SUM(items.qty + 2147483647) AS s",
        ] {
            check(select).unwrap();
        }
        // A literal zero divisor is refused there as anywhere, naming the
        // field and quoting the expression as written, qualified columns and
        // all: inside an aggregate's argument and in a composed field alike.
        for (select, quoted) in [
            (
                "orders.region AS region, SUM(CAST(items.qty AS numeric) / 0) AS s",
                "(CAST(items.qty AS numeric) / 0)",
            ),
            (
                "orders.region AS region, AVG(items.qty) / 0 AS s",
                "(AVG(items.qty) / 0)",
            ),
        ] {
            let err = check(select).unwrap_err();
            assert!(
                matches!(
                    err,
                    ValidationError::ConstantDivisorZero { ref field, ref text }
                        if field == "s" && text == quoted
                ),
                "{select}: {err:?}"
            );
        }
    }

    #[test]
    fn join_greater_than_refuses_a_float_against_an_exact_aggregate() {
        // A JOIN's fields are typed by the same rule as a 1-1's. A
        // float aggregate is refused there, even against a typed float literal.
        let err = check("orders.region AS region, SUM(items.qty) > DOUBLE PRECISION '1' AS x")
            .unwrap_err();
        assert!(
            matches!(err, ValidationError::MixedFloatExactComparison { ref field, .. } if field == "x"),
            "{err:?}"
        );
        check("orders.region AS region, SUM(items.qty) > AVG(orders.total) AS x").unwrap();
    }

    #[test]
    fn join_refuses_each_deferred_aggregate_family_by_name() {
        for (call, family) in [
            ("MIN(items.qty)", "MIN/MAX"),
            ("MAX(orders.total)", "MIN/MAX"),
            ("BOOL_AND(items.qty)", "bool_and/bool_or/bit_and/bit_or"),
            ("BOOL_OR(items.qty)", "bool_and/bool_or/bit_and/bit_or"),
            ("BIT_AND(items.qty)", "bool_and/bool_or/bit_and/bit_or"),
            ("BIT_OR(items.qty)", "bool_and/bool_or/bit_and/bit_or"),
            ("SUM(items.weight)", "float and interval SUM/AVG"),
            ("AVG(items.weight)", "float and interval SUM/AVG"),
            ("ARRAY_AGG(items.sku)", "array_agg/string_agg/jsonb_agg"),
            (
                "STRING_AGG(items.sku, ',')",
                "array_agg/string_agg/jsonb_agg",
            ),
            ("JSONB_AGG(items.sku)", "array_agg/string_agg/jsonb_agg"),
        ] {
            let err = check(&format!("orders.region AS region, {call} AS v")).unwrap_err();
            match &err {
                ValidationError::DeferredAggregate {
                    family: got_family, ..
                } => {
                    assert_eq!(*got_family, family, "{call}");
                }
                other => panic!("{call}: expected DeferredAggregate, got {other:?}"),
            }
            assert!(
                err.to_string().contains("docs/transforms.md") && !err.to_string().contains('#'),
                "{call}: {err}"
            );
        }
    }

    /// `SUM`/`AVG` over a `numeric` with no declared scale (a source's
    /// unconstrained column, a Trellis target's column) is admitted (#1120):
    /// its partials keep a count per scale, recorded as the layout they
    /// need, and by its own admission rule, apart from a declared scale's.
    #[test]
    fn a_sum_over_a_numeric_with_no_declared_scale_is_admitted() {
        use super::super::floor::{Feature, Layout, collect};
        use super::super::registry::{AggregateAdmissionRule, TypeRule};
        let arg = |trellis_target, typmod| AggregateArg {
            column: "p",
            value_type: ValueType::Numeric,
            pg_type: "numeric",
            typmod,
            trellis_target,
            expression: false,
        };
        let rule = |r| Feature::TypeRule(TypeRule::AggregateAdmission(r));
        for function in ["SUM", "AVG"] {
            for (target, typmod, admitted) in [
                (true, -1, AggregateAdmissionRule::SumAvgNumeric),
                (false, -1, AggregateAdmissionRule::SumAvgNumeric),
                (false, 655366, AggregateAdmissionRule::SumAvgScaledNumeric),
            ] {
                let (result, used) = collect(|| {
                    check_aggregate_admitted(
                        function,
                        AggregateScope::Plain,
                        Some(&arg(target, typmod)),
                    )
                });
                assert_eq!(result, Ok(()), "{function} {target} {typmod}");
                assert!(used.contains(&rule(admitted)), "{used:?}");
                assert!(
                    used.contains(&Feature::Layout(Layout::NumericSums)),
                    "{used:?}"
                );
            }
        }
    }

    /// `MIN`/`MAX` in a plain aggregate (#1117 rule 3, #1681): typed as its
    /// argument, as Postgres types `min(x)`, over every type with a btree
    /// order and no collation, equal-but-distinguishable ones (floats,
    /// `interval`, unconstrained `numeric`) included; any other type is
    /// refused by name.
    #[test]
    fn min_max_is_typed_as_its_argument_over_the_types_without_a_collation() {
        let admitted = [
            ValueType::Integer(IntWidth::Int2),
            ValueType::Integer(IntWidth::Int4),
            ValueType::Integer(IntWidth::Int8),
            ValueType::Numeric,
            ValueType::Float(FloatWidth::Float4),
            ValueType::Float(FloatWidth::Float8),
            ValueType::Other(PgType::Date),
            ValueType::Other(PgType::Time),
            ValueType::Other(PgType::Timestamp),
            ValueType::Other(PgType::TimestampTz),
            ValueType::Other(PgType::Interval),
            ValueType::Other(PgType::Oid),
        ];
        let def = parsed(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, MIN(v) AS lo, MAX(v) AS hi, \
             COALESCE(MAX(v), MIN(v)) AS either",
        );
        for value_type in admitted {
            let columns: HashMap<String, ValueType> = [
                ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
                ("v".to_string(), value_type),
            ]
            .into();
            assert_eq!(validate(&def, &columns), Ok(()), "{value_type:?}");
            let types = infer_field_types(&def, &columns).expect("typed");
            for field in ["lo", "hi", "either"] {
                assert_eq!(types[field], value_type, "{field} over {value_type:?}");
            }
        }
        for value_type in [
            ValueType::Text,
            ValueType::Boolean,
            ValueType::Uuid,
            ValueType::Other(PgType::TimeTz),
            ValueType::Other(PgType::Bytea),
        ] {
            let columns: HashMap<String, ValueType> = [
                ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
                ("v".to_string(), value_type),
            ]
            .into();
            let err = validate(&def, &columns).unwrap_err();
            assert!(
                matches!(&err, ValidationError::UnsupportedExtremeArgument { function, argument, .. }
                    if function == "MIN" && argument == "v"),
                "{value_type:?}: {err:?}"
            );
            assert!(
                err.to_string().contains("MIN and MAX are supported over"),
                "{err}"
            );
        }
    }

    /// `MIN`/`MAX` take a plain source column (#1117 rule 5) of a table that
    /// isn't another transform's target (rule 6), each refused by name; the
    /// plain column passes and records its admission.
    #[test]
    fn min_max_of_an_expression_or_of_a_targets_column_is_refused() {
        let (_, mut items) = tables();
        let check = |items: &HashMap<String, JoinColumnInfo>, fields: &str| {
            let def = parse_text(&format!(
                "TRANSFORM t FROM items GROUP BY sku SELECT sku AS sku, {fields}"
            ))
            .unwrap_or_else(|e| panic!("{fields}: {e}"));
            validate_aggregate_admitted(&def, items)
        };
        for fields in [
            "MIN(qty) AS lo, MAX(qty) AS hi",
            "MIN(items.qty) AS lo, COUNT(*) + MAX(order_id) AS hi",
            "MIN(price) AS lo, MAX(weight) AS hi",
        ] {
            assert_eq!(check(&items, fields), Ok(()), "{fields}");
        }
        for (fields, argument) in [
            ("MIN(qty + 1) AS lo", "(qty + 1)"),
            ("MAX(CAST(qty AS bigint)) AS hi", "CAST(qty AS bigint)"),
            ("MAX(ABS(qty)) AS hi", "ABS(qty)"),
        ] {
            match check(&items, fields) {
                Err(ValidationError::ExtremeOfExpression { argument: got, .. }) => {
                    assert_eq!(got, argument, "{fields}");
                }
                other => panic!("{fields}: expected ExtremeOfExpression, got {other:?}"),
            }
        }
        // A grouped calculated field is its expression.
        let def = parse_text(
            "TRANSFORM t FROM items GROUP BY sku, qty \
             SELECT sku AS sku, qty AS qty, (qty + 1) AS q, MIN(q) AS lo",
        )
        .expect("parse");
        assert!(matches!(
            validate_aggregate_admitted(&def, &items),
            Err(ValidationError::ExtremeOfExpression { .. })
        ));
        items.get_mut("qty").expect("qty").trellis_target = true;
        let err = check(&items, "MAX(qty) AS hi").unwrap_err();
        assert!(
            matches!(&err, ValidationError::ChainedExtreme { function, column }
                if function == "MAX" && column == "qty"),
            "{err:?}"
        );
        assert!(err.to_string().contains("table Trellis creates"), "{err}");
    }

    /// `WITHOUT SOURCE INDEX` doesn't apply to `MIN`/`MAX` (#1681 Q1): the
    /// validator refuses the pair, naming the probe index to create, so
    /// define, an edit and a resume (each validates through here) all do. An
    /// `ALTER TRANSFORM ... ADD` can't add an aggregate at all (the clause
    /// parses as a 1-1 field), and a merged definition carrying one is
    /// refused the same way.
    #[test]
    fn without_source_index_refuses_min_max_naming_the_probe_index() {
        let columns: HashMap<String, ValueType> = [
            ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("h".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("v".to_string(), ValueType::Other(PgType::Date)),
            ("w".to_string(), ValueType::Integer(IntWidth::Int8)),
        ]
        .into();
        let def = parsed(
            "TRANSFORM t FROM s GROUP BY g, h WITHOUT SOURCE INDEX \
             SELECT g AS g, h AS h, MIN(v) AS lo, MAX(v) AS hi, MAX(w) AS top",
        );
        let err = validate(&def, &columns).unwrap_err();
        let ValidationError::ExtremesWithoutSourceIndex { target, indexes } = &err else {
            panic!("expected ExtremesWithoutSourceIndex, got {err:?}");
        };
        assert_eq!(target, "t");
        let wanted: Vec<(&str, Vec<&str>)> =
            vec![("s", vec!["g", "h", "v"]), ("s", vec!["g", "h", "w"])];
        let got: Vec<(&str, Vec<&str>)> = indexes
            .iter()
            .map(|i| {
                (
                    i.table.as_str(),
                    i.columns.iter().map(String::as_str).collect(),
                )
            })
            .collect();
        assert_eq!(got, wanted);
        // Define spells the resolved schema.
        let message = err.with_source_schema("public").to_string();
        assert!(
            message.contains("WITHOUT SOURCE INDEX, which doesn't apply to MIN/MAX")
                && message.contains(r#"`create index on "public"."s" ("g", "h", "v")`"#)
                && message.contains(r#"`create index on "public"."s" ("g", "h", "w")`"#)
                && message.contains("serves as the GROUP BY columns' index"),
            "{message}"
        );
        // Without the opt-out, or without MIN/MAX, it validates.
        let mut indexed = def.clone();
        indexed.without_source_index = false;
        assert_eq!(validate(&indexed, &columns), Ok(()));
        let counted = parsed(
            "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, COUNT(*) AS n",
        );
        assert_eq!(validate(&counted, &columns), Ok(()));

        // `ALTER TRANSFORM ... ADD` parses its field as a 1-1 one.
        assert!(
            crate::defs::parser::parse_statement("ALTER TRANSFORM t ADD MIN(v) AS lo").is_err()
        );
        // A merged definition that gains a MIN/MAX field is refused as define
        // refuses it.
        let mut merged = counted.clone();
        merged.fields.push(def.fields[2].clone());
        assert!(matches!(
            validate(&merged, &columns),
            Err(ValidationError::ExtremesWithoutSourceIndex { .. })
        ));
    }

    /// A plain aggregate: every deferred family is
    /// refused by name and issue, `SUM`/`AVG` over an unconstrained `numeric`
    /// (a column, or any `numeric` expression) is refused with the "declare a
    /// scale" remedy, and the admitted set, expression arguments and
    /// composed fields included, passes.
    #[test]
    fn a_plain_aggregate_refuses_each_deferred_family_by_name() {
        let (_, mut items) = tables();
        items.insert(
            "wait".into(),
            jcol(ValueType::Other(PgType::Interval), "interval", 1186, -1),
        );
        items.insert(
            "cost".into(),
            jcol(ValueType::Numeric, "numeric(10,2)", 1700, 655366),
        );
        let check = |fields: &str| {
            let def = parse_text(&format!(
                "TRANSFORM t FROM items GROUP BY sku SELECT sku AS sku, {fields}"
            ))
            .unwrap_or_else(|e| panic!("{fields}: {e}"));
            validate_aggregate_admitted(&def, &items)
        };
        for (call, family) in [
            ("BOOL_AND(qty > 1)", "bool_and/bool_or/bit_and/bit_or"),
            ("BIT_OR(qty)", "bool_and/bool_or/bit_and/bit_or"),
            ("SUM(weight)", "float and interval SUM/AVG"),
            ("AVG(wait)", "float and interval SUM/AVG"),
            ("ARRAY_AGG(note)", "array_agg/string_agg/jsonb_agg"),
            ("STRING_AGG(note, ',')", "array_agg/string_agg/jsonb_agg"),
            // Inside a composed field too.
            (
                "COUNT(*) + BOOL_OR(qty > 1)",
                "bool_and/bool_or/bit_and/bit_or",
            ),
        ] {
            match check(&format!("{call} AS v")) {
                Err(ValidationError::DeferredAggregate {
                    family: got_family, ..
                }) => assert_eq!(got_family, family, "{call}"),
                other => panic!("{call}: expected DeferredAggregate, got {other:?}"),
            }
        }
        // A `numeric` with no declared scale is admitted (#1120): an
        // unconstrained column, and any expression whose result is
        // `numeric`.
        for call in [
            "SUM(price)",
            "AVG(price)",
            "SUM(cost + 1)",
            "AVG(cost + qty)",
            "SUM(cost * 2)",
            "AVG(cost - 1)",
            "SUM(-cost)",
            "SUM(price * qty)",
            "SUM(cost / qty)",
            "SUM(cost % 3)",
            "SUM(ABS(cost))",
            "SUM(ROUND(cost, 2))",
            "SUM(CAST(qty AS numeric))",
            "SUM(CAST(cost AS numeric))",
            "AVG(CAST(cost AS decimal))",
            "SUM(CASE WHEN qty > 1 THEN cost ELSE 0 END)",
        ] {
            if let Err(err) = check(&format!("{call} AS v")) {
                panic!("{call}: {err:?}");
            }
        }
        for fields in [
            "COUNT(*) AS n, COUNT(note) AS notes, COUNT(price) AS priced",
            "SUM(qty) AS s, AVG(qty) AS a, SUM(cost) AS c, AVG(cost) AS ac",
            "SUM(qty + order_id) AS s, COALESCE(SUM(cost), 0) + COUNT(*) AS c",
            // Integer arithmetic keeps its type, so a sum of it is admitted.
            "SUM(qty * order_id) AS s, AVG(qty - order_id) AS a",
            "SUM(-qty) AS s, SUM(ABS(qty)) AS a, SUM(CAST(qty AS bigint)) AS b",
            "SUM(CAST(qty AS integer)) AS s, AVG(CAST(order_id AS int4)) AS a",
            "SUM(qty % 7) AS s, SUM(qty * 2 - 1) AS t",
            "-SUM(qty) AS s, COALESCE(SUM(qty), 0) * 2 AS t, ROUND(AVG(qty), 2) AS r",
        ] {
            if let Err(err) = check(fields) {
                panic!("{fields}: {err:?}");
            }
        }
        // An argument that names a calculated field is typed as that field's
        // expression: an integer one is admitted, a float one is refused by
        // family. (This check doesn't look at `GROUP BY`; `validate` refuses
        // a calculated field over an ungrouped column.)
        let grouped = |fields: &str| {
            let def = parse_text(&format!(
                "TRANSFORM t FROM items GROUP BY sku, qty \
                 SELECT sku AS sku, qty AS qty, {fields}"
            ))
            .unwrap_or_else(|e| panic!("{fields}: {e}"));
            validate_aggregate_admitted(&def, &items)
        };
        assert!(
            matches!(
                grouped("(weight + weight) AS w2, SUM(w2) AS s"),
                Err(ValidationError::DeferredAggregate {
                    family: "float and interval SUM/AVG",
                    ..
                })
            ),
            "a float calculated field's SUM"
        );
        for fields in [
            "(qty + 1) AS q, SUM(q) AS s",
            "(qty + qty) AS q, COUNT(*) AS n, AVG(q) AS a",
        ] {
            if let Err(err) = grouped(fields) {
                panic!("{fields}: {err:?}");
            }
        }
    }

    #[test]
    fn join_admits_sum_and_avg_over_unconstrained_numeric_on_either_side() {
        for call in ["SUM(items.price)", "AVG(items.price)"] {
            check(&format!("orders.region AS region, {call} AS v"))
                .unwrap_or_else(|err| panic!("{call}: {err:?}"));
        }
        // The same on the FROM side.
        let (mut left, right) = tables();
        left.insert(
            "loose".into(),
            jcol(ValueType::Numeric, "numeric", 1700, -1),
        );
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT SUM(orders.loose) AS v",
        )
        .unwrap();
        assert_eq!(validate_join(&def, &left, &[&right]), Ok(()));
    }

    #[test]
    fn join_accepts_numeric_with_a_declared_scale_including_zero() {
        let (mut left, right) = tables();
        // numeric(10): scale 0, typmod (10 << 16) + 4.
        left.insert(
            "whole".into(),
            jcol(ValueType::Numeric, "numeric(10,0)", 1700, 655364),
        );
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT SUM(orders.whole) AS v, AVG(orders.total) AS w",
        )
        .unwrap();
        validate_join(&def, &left, &[&right]).unwrap();
    }

    #[test]
    fn join_refuses_sum_over_other_types_and_expression_shapes() {
        assert!(matches!(
            check("orders.region AS region, SUM(items.sku) AS v"),
            Err(ValidationError::UnsupportedAggregateArgument { .. })
        ));
        assert!(matches!(
            check("orders.region AS region, SUM(qty) AS v"),
            Err(ValidationError::JoinUnqualifiedColumn { .. })
        ));
        assert!(matches!(
            check("orders.region AS region, items.qty + 1 AS v"),
            Err(ValidationError::JoinFieldShape { .. })
        ));
        assert!(matches!(
            check("orders.region AS region, items.sku AS sku"),
            Err(ValidationError::JoinFieldShape { .. })
        ));
        assert!(matches!(
            check("orders.region AS other"),
            Err(ValidationError::JoinFieldShape { .. })
        ));
    }

    #[test]
    fn join_on_terms_are_checked_against_both_tables() {
        let on = |on: &str| {
            check_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON {on} GROUP BY orders.region \
                 SELECT COUNT(*) AS n"
            ))
        };
        assert!(matches!(
            on("orders.id = orders.id"),
            Err(ValidationError::JoinOnSameSide { .. })
        ));
        assert!(matches!(
            on("orders.id = other.order_id"),
            Err(ValidationError::JoinUnknownTable { .. })
        ));
        assert!(matches!(
            on("orders.id = items.nope"),
            Err(ValidationError::JoinUnresolvedColumn { .. })
        ));
        // integer against text: a type mismatch, never a cast.
        assert!(matches!(
            on("orders.id = items.sku"),
            Err(ValidationError::JoinKeyTypeMismatch { .. })
        ));
        // numeric isn't a text-stable join key.
        assert!(matches!(
            on("orders.total = items.price"),
            Err(ValidationError::JoinUnsupportedKeyType { .. })
        ));
    }

    #[test]
    fn join_key_pairing_ignores_modifiers_but_not_types_or_collations() {
        // ADR-0020 rule 6: modifiers never count, `text` and `varchar` are
        // one family, `integer` against `bigint` and another collation are
        // refused.
        let pairs = |lhs: (&str, u32, i32), rhs: (&str, u32, i32), rhs_collation: u32| {
            let (mut left, mut right) = tables();
            left.insert("k".into(), jcol(ValueType::Text, lhs.0, lhs.1, lhs.2));
            let mut info = jcol(ValueType::Text, rhs.0, rhs.1, rhs.2);
            info.collation_oid = rhs_collation;
            right.insert("k".into(), info);
            let def = parse_text(
                "TRANSFORM t FROM orders JOIN items ON orders.k = items.k \
                 GROUP BY orders.region SELECT COUNT(*) AS n",
            )
            .unwrap();
            validate_join(&def, &left, &[&right])
        };
        let v50 = ("character varying(50)", 1043, 54);
        let v255 = ("character varying(255)", 1043, 259);
        let varchar = ("character varying", 1043, -1);
        let text = ("text", 25, -1);
        let default = 0;
        for (lhs, rhs) in [
            (v50, v255),
            (v50, varchar),
            (text, v50),
            (varchar, text),
            (
                ("time(0) without time zone", 1083, 0),
                ("time without time zone", 1083, -1),
            ),
            (("bit varying(5)", 1562, 5), ("bit varying(8)", 1562, 8)),
            // Two lengths of `bit` never hold equal values, so they pair and
            // match nothing (ADR-0020 rule 6).
            (("bit(5)", 1560, 5), ("bit(8)", 1560, 8)),
            (("bit(5)", 1560, 5), ("\"bit\"", 1560, -1)),
        ] {
            pairs(lhs, rhs, default).unwrap_or_else(|err| panic!("{lhs:?} against {rhs:?}: {err}"));
        }
        // Another type, even of one width family, or another collation.
        assert!(matches!(
            pairs(("integer", 23, -1), ("bigint", 20, -1), default),
            Err(ValidationError::JoinKeyTypeMismatch { .. })
        ));
        assert!(matches!(
            pairs(("bit(5)", 1560, 5), ("bit varying(5)", 1562, 5), default),
            Err(ValidationError::JoinKeyTypeMismatch { .. })
        ));
        assert!(matches!(
            pairs(text, text, default + 1),
            Err(ValidationError::JoinKeyTypeMismatch { .. })
        ));

        let (mut left, mut right) = tables();
        for (table, name) in [(&mut left, "c"), (&mut right, "c")] {
            let mut info = jcol(ValueType::Text, "text", 25, -1);
            info.collation_oid = 99;
            info.collation = Some("ci".into());
            info.collation_deterministic = false;
            table.insert(name.into(), info);
        }
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.c = items.c \
             GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
        assert!(matches!(
            validate_join(&def, &left, &[&right]),
            Err(ValidationError::JoinNondeterministicCollation { .. })
        ));
    }

    #[test]
    fn join_group_by_keys_are_checked_from_either_side() {
        let group = |keys: &str| {
            check_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
                 GROUP BY {keys} SELECT COUNT(*) AS n"
            ))
        };
        group("orders.region, items.sku").unwrap();
        assert!(matches!(
            group("items.nope"),
            Err(ValidationError::JoinUnresolvedColumn { .. })
        ));
        assert!(matches!(
            group("items.weight"),
            Err(ValidationError::UnsupportedGroupByKeyType { .. })
        ));
        // Two keys, one target column name.
        let (mut left, right) = tables();
        left.insert("sku".into(), jcol(ValueType::Text, "text", 25, -1));
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.sku, items.sku SELECT COUNT(*) AS n",
        )
        .unwrap();
        assert_eq!(
            validate_join(&def, &left, &[&right]),
            Err(ValidationError::GroupByKeysShareTargetColumn {
                first: "orders.sku".into(),
                second: "items.sku".into(),
                column: "sku".into(),
            })
        );
        // A key's collation is checked with it.
        let (mut left, right) = tables();
        let mut region = jcol(ValueType::Text, "text", 25, -1);
        region.collation = Some("ci".into());
        region.collation_deterministic = false;
        left.insert("region".into(), region);
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
        assert_eq!(
            validate_join(&def, &left, &[&right]),
            Err(ValidationError::NondeterministicKeyCollation(Box::new(
                NondeterministicKeyCollation {
                    key: KeyColumnRole::GroupBy,
                    table: "orders".into(),
                    column: "region".into(),
                    collation: "ci".into(),
                }
            )))
        );
        // Only the key itself takes its column's name.
        assert!(matches!(
            check_text(
                "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
                 GROUP BY orders.region SELECT COUNT(*) AS region"
            ),
            Err(ValidationError::JoinFieldShape { detail, .. })
                if detail.starts_with("only the GROUP BY key itself")
        ));
    }

    /// An aggregate's lone column argument is checked with its live
    /// declared type and modifier, in a plain aggregate and a `JOIN` alike.
    #[test]
    fn an_aggregate_argument_carries_its_columns_declared_type_and_modifier() {
        use super::super::floor::{Feature, collect};
        use super::super::registry::{AggregateAdmissionRule, TypeRule};
        let scaled = Feature::TypeRule(TypeRule::AggregateAdmission(
            AggregateAdmissionRule::SumAvgScaledNumeric,
        ));
        let varchar = || jcol(ValueType::Text, "character varying(5)", 1043, 9);
        let refused = |column: &str| {
            Err(ValidationError::UnsupportedAggregateArgument {
                function: "SUM".into(),
                column: column.into(),
                pg_type: "character varying(5)".into(),
            })
        };

        let plain = side(&[
            ("g", int4()),
            ("v", jcol(ValueType::Numeric, "numeric(10,2)", 1700, 655366)),
        ]);
        let def = parsed("TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(v) AS s");
        let (result, used) = collect(|| validate_live(&def, &plain));
        assert_eq!(result, Ok(()));
        assert!(used.contains(&scaled), "{used:?}");

        let (mut left, right) = tables();
        left.insert("code".into(), varchar());
        let join = |select: &str| {
            parse_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
                 GROUP BY orders.region SELECT {select}"
            ))
            .unwrap()
        };
        let (result, used) =
            collect(|| validate_join(&join("SUM(orders.total) AS s"), &left, &[&right]));
        assert_eq!(result, Ok(()));
        assert!(used.contains(&scaled), "{used:?}");
        assert_eq!(
            validate_join(&join("SUM(orders.code) AS s"), &left, &[&right]),
            refused("orders.code")
        );
    }

    /// Both shapes refuse `GROUP BY` keys one key at a time, in one order: a
    /// key's refusal comes before any later key's, its type before its
    /// collation, its collation before a clash with an earlier key, and every
    /// key's refusal before the index width.
    #[test]
    fn group_by_key_refusals_come_key_by_key_in_one_order() {
        let float = ValueType::Float(crate::float::FloatWidth::Float8);
        let wide = |last: &str| {
            let mut keys: Vec<String> = (0..INDEX_MAX_KEYS).map(|i| format!("k{i}")).collect();
            keys.push(last.to_string());
            keys
        };
        let type_refused = |column: &str| {
            Err(ValidationError::UnsupportedGroupByKeyType {
                column: column.into(),
                value_type: float,
            })
        };

        let mut plain: HashMap<String, ValueType> = (0..INDEX_MAX_KEYS)
            .map(|i| (format!("k{i}"), ValueType::Integer(IntWidth::Int4)))
            .collect();
        plain.insert("f".into(), float);
        let plain_check = |keys: &str| {
            validate(
                &parsed(&format!(
                    "TRANSFORM t FROM s GROUP BY {keys} SELECT COUNT(*) AS n"
                )),
                &plain,
            )
        };
        assert_eq!(plain_check("f, missing"), type_refused("f"));
        assert_eq!(
            plain_check("missing, f"),
            Err(ValidationError::UnresolvedGroupByColumn {
                column: "missing".into()
            })
        );
        assert_eq!(plain_check(&wide("f").join(", ")), type_refused("f"));

        let (mut left, mut right) = tables();
        for i in 0..INDEX_MAX_KEYS {
            left.insert(format!("k{i}"), int4());
        }
        let mut odd = jcol(float, "double precision", 701, -1);
        odd.collation = Some("ci".into());
        odd.collation_deterministic = false;
        left.insert("odd".into(), odd);
        left.insert("sku".into(), jcol(ValueType::Text, "text", 25, -1));
        let sku = right.get_mut("sku").expect("sku");
        sku.collation = Some("ci".into());
        sku.collation_deterministic = false;
        let join_check = |keys: &str| {
            validate_join(
                &parse_text(&format!(
                    "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
                     GROUP BY {keys} SELECT COUNT(*) AS n"
                ))
                .unwrap(),
                &left,
                &[&right],
            )
        };
        assert_eq!(
            join_check("items.weight, items.nope"),
            type_refused("weight")
        );
        assert_eq!(
            join_check("items.nope, items.weight"),
            Err(ValidationError::JoinUnresolvedColumn {
                reference: "items.nope".into()
            })
        );
        assert_eq!(join_check("orders.odd"), type_refused("odd"));
        assert!(matches!(
            join_check("orders.sku, items.sku"),
            Err(ValidationError::NondeterministicKeyCollation(refusal))
                if refusal.table == "items"
        ));
        let qualified: Vec<String> = wide("weight")
            .iter()
            .map(|key| match key.as_str() {
                "weight" => "items.weight".to_string(),
                key => format!("orders.{key}"),
            })
            .collect();
        assert_eq!(join_check(&qualified.join(", ")), type_refused("weight"));
    }

    #[test]
    fn join_refuses_a_self_join_built_by_hand() {
        let (left, right) = tables();
        let mut def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
        if let KeySpace::Join(spec) = &mut def.key_space {
            spec.joins[0].right = "orders".to_string();
        }
        assert!(matches!(
            validate_join(&def, &left, &[&right]),
            Err(ValidationError::JoinSelf { .. })
        ));
    }

    /// An aggregate `JOIN` of a second joined table parses, and is refused
    /// first, even when another check would also refuse it (#1457 step 7
    /// admits more).
    #[test]
    fn an_aggregate_join_refuses_a_second_joined_table() {
        let (left, right) = tables();
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             JOIN shipments ON items.id = shipments.item_id \
             GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
        assert_eq!(
            validate_join(&def, &left, &[&right, &right]),
            Err(ValidationError::JoinTooManyTables)
        );
        assert_eq!(
            ValidationError::JoinTooManyTables.to_string(),
            "a JOIN transform joins exactly two tables; a second JOIN is not supported"
        );
    }

    #[test]
    fn join_admits_a_composite_key_in_either_conjunct_order_but_not_a_repeated_pair() {
        let on = |on: &str| {
            check_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON {on} GROUP BY orders.region \
                 SELECT COUNT(*) AS n"
            ))
        };
        on("orders.id = items.order_id AND orders.region = items.sku").unwrap();
        on("items.sku = orders.region AND items.order_id = orders.id").unwrap();
        assert_eq!(
            on("orders.id = items.order_id AND items.order_id = orders.id"),
            Err(ValidationError::JoinOnRepeatedPair {
                term: "items.order_id = orders.id".to_string()
            })
        );
    }

    #[test]
    fn join_refuses_a_column_named_by_two_on_terms_on_either_side() {
        let on = |on: &str| {
            check_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON {on} GROUP BY orders.region \
                 SELECT COUNT(*) AS n"
            ))
        };
        let repeated = |term: &str, column: &str| {
            Err(ValidationError::JoinOnRepeatedColumn {
                term: term.to_string(),
                column: column.to_string(),
            })
        };
        // The joined table's column repeated, then the FROM table's.
        assert_eq!(
            on("orders.id = items.order_id AND orders.region = items.order_id"),
            repeated("orders.region = items.order_id", "items.order_id")
        );
        assert_eq!(
            on("orders.id = items.order_id AND orders.id = items.qty"),
            repeated("orders.id = items.qty", "orders.id")
        );
        // Either written order of the repeating term.
        assert_eq!(
            on("orders.id = items.order_id AND items.qty = orders.id"),
            repeated("items.qty = orders.id", "orders.id")
        );
        // A schema-qualified spelling of the same column is the same column.
        assert_eq!(
            on("orders.id = items.order_id AND public.orders.id = items.qty"),
            repeated("public.orders.id = items.qty", "public.orders.id")
        );
        // A repeated pair keeps its own refusal.
        assert!(matches!(
            on("orders.id = items.order_id AND items.order_id = orders.id"),
            Err(ValidationError::JoinOnRepeatedPair { .. })
        ));
        // A column reused across the sides is not a repeat: `qty` is on the
        // joined side and `id` on the FROM side.
        on("orders.id = items.qty AND orders.region = items.sku").unwrap();
    }

    #[test]
    fn an_index_key_is_admitted_at_the_postgres_cap_and_refused_past_it() {
        let wide = |n: usize| {
            let keys: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
            let fields: Vec<String> = keys.iter().map(|k| format!("{k} AS {k}")).collect();
            let columns: HashMap<String, ValueType> = keys
                .iter()
                .map(|k| (k.clone(), ValueType::Numeric))
                .collect();
            let text = format!(
                "TRANSFORM t FROM s GROUP BY {} SELECT {}, COUNT(*) AS n",
                keys.join(", "),
                fields.join(", ")
            );
            validate(&crate::defs::parse(&text).unwrap(), &columns)
        };
        assert_eq!(wide(INDEX_MAX_KEYS), Ok(()));
        let err = wide(INDEX_MAX_KEYS + 1).unwrap_err();
        assert_eq!(
            err,
            ValidationError::IndexKeyTooWide {
                clause: "GROUP BY".to_string(),
                columns: 33,
                limit: 32,
            }
        );
        let message = err.to_string();
        assert!(
            message.contains("GROUP BY") && message.contains("33"),
            "{message}"
        );
        assert!(message.contains("32"), "{message}");
    }

    /// An aggregate of `groups` `GROUP BY` columns (of `g0, g1, ...`) with
    /// `fields` fields, each `select(i)`.
    fn wide_aggregate(
        groups: usize,
        fields: usize,
        select: impl Fn(usize) -> String,
    ) -> Result<(), ValidationError> {
        let keys: Vec<String> = (0..groups).map(|i| format!("g{i}")).collect();
        let mut selected: Vec<String> = keys.iter().map(|k| format!("{k} AS {k}")).collect();
        selected.extend((0..fields).map(select));
        let mut columns: HashMap<String, ValueType> = keys
            .iter()
            .map(|k| (k.clone(), ValueType::Integer(IntWidth::Int4)))
            .collect();
        columns.insert("v".to_string(), ValueType::Integer(IntWidth::Int4));
        let text = format!(
            "TRANSFORM t FROM s GROUP BY {} SELECT {}",
            keys.join(", "),
            selected.join(", ")
        );
        validate(&crate::defs::parse(&text).unwrap(), &columns)
    }

    fn too_wide(table: &str, columns: usize) -> ValidationError {
        ValidationError::TableTooWide {
            table: table.to_string(),
            columns,
            limit: TABLE_MAX_COLUMNS,
            dropped: 0,
        }
    }

    #[test]
    fn an_aggregates_hidden_table_is_admitted_at_the_postgres_column_cap_and_refused_past_it() {
        // Two group columns, the row count, two columns per summed argument,
        // then basis, stamp and zero flag: 797 arguments are 1600 columns,
        // and a third group column makes them 1601. (How `COUNT` and a
        // shared argument count is pinned against the DDL in
        // `staging::group_basis`.)
        let sums = |i: usize| format!("SUM(v + {i}) AS s{i}");
        assert_eq!(wide_aggregate(2, 797, sums), Ok(()));
        let err = wide_aggregate(3, 797, sums).unwrap_err();
        assert_eq!(
            err,
            too_wide("the hidden table of the aggregate's partial sums", 1601)
        );
        let message = err.to_string();
        assert!(
            message.contains("1601") && message.contains("1600") && message.contains("fewer"),
            "{message}"
        );
        assert!(!message.contains("dropped"), "{message}");
    }

    #[test]
    fn an_aggregates_target_is_admitted_at_the_postgres_column_cap_and_refused_past_it() {
        // `COUNT(*)` fields add target columns and no hidden ones.
        let counts = |i: usize| format!("COUNT(*) + {i} AS n{i}");
        assert_eq!(wide_aggregate(1, 1599, counts), Ok(()));
        assert_eq!(
            wide_aggregate(1, 1600, counts),
            Err(too_wide("the target table", 1601))
        );
    }

    #[test]
    fn an_aggregate_join_is_admitted_at_the_postgres_column_cap_and_refused_past_it() {
        // Each side's tables, not the definition, are measured. `P` is the
        // `ON` column (`id`, also the group column), the row count, a pair
        // per argument and three more columns.
        let join = |from: usize, joined: usize, group: &str| {
            let mut selected = vec![format!("{group} AS {}", group.split('.').nth(1).unwrap())];
            selected.extend((0..from).map(|i| format!("SUM(orders.id + {i}) AS f{i}")));
            selected.extend((0..joined).map(|i| format!("SUM(items.qty + {i}) AS j{i}")));
            check_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
                 GROUP BY {group} SELECT {}",
                selected.join(", ")
            ))
        };
        let from_side = "orders's hidden partials table";
        let joined_side = "items's hidden partials table";
        let groups = "the hidden table of the JOIN's group partials";
        // 1 key + 1 + 2n + 3 = 2n + 5.
        assert_eq!(join(797, 0, "orders.id"), Ok(()));
        assert_eq!(join(798, 0, "orders.id"), Err(too_wide(from_side, 1601)));
        assert_eq!(join(0, 797, "orders.id"), Ok(()));
        assert_eq!(join(0, 798, "orders.id"), Err(too_wide(joined_side, 1601)));
        // `T` holds both sides' arguments: 1 group column + 1 + 2n + 1.
        assert_eq!(join(399, 399, "orders.id"), Ok(()));
        assert_eq!(join(400, 399, "orders.id"), Err(too_wide(groups, 1601)));
        // The target has a column per field.
        let wide_target = |n: usize| {
            let counts: Vec<String> = (0..n).map(|i| format!("COUNT(*) + {i} AS n{i}")).collect();
            check_text(&format!(
                "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
                 GROUP BY orders.region SELECT orders.region AS region, {}",
                counts.join(", ")
            ))
        };
        assert_eq!(wide_target(1599), Ok(()));
        assert_eq!(wide_target(1600), Err(too_wide("the target table", 1601)));
    }

    #[test]
    fn join_admits_one_side_expression_arguments_and_composed_fields() {
        for select in [
            "SUM(items.qty + 1) AS v",
            "SUM(COALESCE(items.qty, 0)) AS v",
            "COUNT(items.qty > 3) AS v",
            "AVG(orders.id + orders.id) AS v",
            "SUM(items.qty) + COUNT(orders.id) AS v",
            "COALESCE(SUM(items.qty), 0) AS v",
            "SUM(orders.total) + 1 AS v",
            "(COUNT(*) + SUM(items.qty)) > 10 AS v",
        ] {
            check(&format!("orders.region AS region, {select}"))
                .unwrap_or_else(|err| panic!("{select}: {err:?}"));
        }
    }

    #[test]
    fn join_admits_arithmetic_in_one_side_arguments_and_composed_fields() {
        for select in [
            "SUM(items.qty * 2) AS v",
            "SUM(items.qty - items.order_id) AS v",
            "AVG(-items.qty) AS v",
            "SUM(ABS(items.qty)) AS v",
            "SUM(CAST(items.qty AS bigint)) AS v",
            "SUM(orders.id % 7) AS v",
            "-SUM(items.qty) AS v",
            "SUM(items.qty) * 2 - COUNT(*) AS v",
            "ROUND(AVG(items.qty), 2) AS v",
            "ABS(SUM(items.qty)) AS v",
            "CAST(SUM(items.qty) AS numeric) / COUNT(*) AS v",
        ] {
            check(&format!("orders.region AS region, {select}"))
                .unwrap_or_else(|err| panic!("{select}: {err:?}"));
        }
        for select in [
            "SUM(items.qty * orders.id) AS v",
            "SUM(items.qty - orders.id) AS v",
            "SUM(-COALESCE(items.qty, orders.id)) AS v",
        ] {
            assert!(
                matches!(
                    check(&format!("orders.region AS region, {select}")),
                    Err(ValidationError::JoinMixedSideArgument { .. })
                ),
                "{select}"
            );
        }
        // An expression over a numeric is admitted (#1120).
        for select in ["SUM(orders.total * 2) AS v", "SUM(-orders.total) AS v"] {
            check(&format!("orders.region AS region, {select}"))
                .unwrap_or_else(|err| panic!("{select}: {err:?}"));
        }
    }

    #[test]
    fn join_refuses_a_mixed_side_argument_by_name() {
        assert_eq!(
            check("orders.region AS region, SUM(orders.id + items.qty) AS v"),
            Err(ValidationError::JoinMixedSideArgument {
                field: "v".to_string(),
                argument: "SUM((orders.id + items.qty))".to_string(),
            })
        );
        // Inside a composition, and through a scalar function.
        assert!(matches!(
            check("COALESCE(COUNT(COALESCE(orders.id, items.qty)), 0) AS v"),
            Err(ValidationError::JoinMixedSideArgument { .. })
        ));
    }

    #[test]
    fn join_refuses_bad_expression_arguments_and_compositions() {
        let refused = |select: &str| check(&format!("orders.region AS region, {select}"));
        // An expression over numeric is admitted (#1120).
        assert_eq!(refused("SUM(orders.total + 1) AS v"), Ok(()));
        // A deferred aggregate is refused inside a composition too.
        assert!(matches!(
            refused("COALESCE(MIN(items.qty), 0) AS v"),
            Err(ValidationError::DeferredAggregate {
                family: "MIN/MAX",
                ..
            })
        ));
        assert!(matches!(
            refused("SUM(items.sku) + 1 AS v"),
            Err(ValidationError::UnsupportedAggregateArgument { .. })
        ));
        assert!(matches!(
            refused("SUM(items.qty) + 'a' AS v"),
            Err(ValidationError::TypeMismatch { .. })
        ));
        for select in [
            "SUM(COUNT(items.qty)) AS v",
            "1 AS v",
            "COUNT(1) AS v",
            "SUM(items.qty) + items.qty AS v",
            "orders.region + 1 AS v",
        ] {
            assert!(
                matches!(refused(select), Err(ValidationError::JoinFieldShape { .. })),
                "{select}: {:?}",
                refused(select)
            );
        }
        assert!(matches!(
            refused("SUM(qty + 1) AS v"),
            Err(ValidationError::JoinUnqualifiedColumn { .. })
        ));
    }

    #[test]
    fn join_refuses_a_target_named_after_the_joined_table() {
        assert_eq!(
            check_text(
                "TRANSFORM items FROM orders JOIN items ON orders.id = items.order_id \
                 GROUP BY orders.region SELECT COUNT(*) AS n",
            ),
            Err(ValidationError::TargetEqualsSource {
                table: "items".to_string()
            })
        );
    }

    #[test]
    fn join_reads_a_dotted_name_of_neither_table_as_an_unknown_table() {
        for select in [
            "orders.region AS region, author.name AS name",
            "orders.region AS region, SUM(author.age) AS s",
        ] {
            assert_eq!(
                check(select),
                Err(ValidationError::JoinUnknownTable {
                    reference: if select.contains("SUM") {
                        "author.age".to_string()
                    } else {
                        "author.name".to_string()
                    }
                }),
                "{select}"
            );
        }
    }

    #[test]
    fn the_single_table_validator_leaves_a_join_to_validate_join() {
        let def = parse_text(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
        assert_eq!(
            validate(&def, &HashMap::new()),
            Err(ValidationError::JoinNeedsBothTables)
        );
    }
}

/// The pure key-collation check, over hand-built
/// columns carrying the collations Postgres reports.
#[cfg(test)]
mod key_collation_tests {
    use super::*;

    fn column() -> JoinColumnInfo {
        JoinColumnInfo {
            value_type: ValueType::Text,
            pg_type: "text".to_string(),
            type_oid: 25,
            typmod: -1,
            collation_oid: 0,
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            copy_type: "text".to_string(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    fn collated(mut column: JoinColumnInfo, name: &str) -> JoinColumnInfo {
        column.collation = Some(name.to_string());
        column
    }

    /// A key column with a nondeterministic collation is refused
    /// in every key role, and the error names the column, its collation and
    /// the key it belongs to.
    #[test]
    fn a_nondeterministic_key_column_is_rejected_naming_column_collation_and_role() {
        let mut ci = collated(column(), "case_insensitive");
        ci.collation_deterministic = false;
        for (key, role) in [
            (
                KeyColumnRole::SourceKey,
                "part of the source's row-identity key",
            ),
            (KeyColumnRole::GroupBy, "a GROUP BY key"),
        ] {
            let err = check_key_collation(key.clone(), "shop.orders", "code", &ci).unwrap_err();
            assert_eq!(
                err,
                ValidationError::NondeterministicKeyCollation(Box::new(
                    NondeterministicKeyCollation {
                        key,
                        table: "shop.orders".to_string(),
                        column: "code".to_string(),
                        collation: "case_insensitive".to_string(),
                    }
                ))
            );
            let message = err.to_string();
            assert!(
                message.starts_with(&format!("shop.orders.code is {role}, and its collation"))
                    && message.contains(r#""case_insensitive" is nondeterministic"#),
                "{message}"
            );
        }
    }

    /// A deterministic collation passes whether it's the default,
    /// a non-default one (`"C"`), or absent (a type that isn't collatable).
    #[test]
    fn a_deterministic_or_absent_key_collation_passes() {
        for resolved in [
            collated(column(), "default"),
            collated(column(), "C"),
            column(),
        ] {
            assert!(
                check_key_collation(KeyColumnRole::GroupBy, "public.t", "k", &resolved).is_ok()
            );
        }
    }

    /// `ALTER TRANSFORM ... ADD` checks only the fields it adds: a column
    /// re-collated since define doesn't refuse an edit that adds no read of it.
    #[test]
    fn only_the_listed_fields_text_reads_are_checked() {
        let def = crate::defs::parser::parse(
            "TRANSFORM t FROM s SELECT strpos(title, 'x') AS old, char_length(title) AS new",
        )
        .expect("parse");
        let mut ci = collated(column(), "case_insensitive");
        ci.collation_deterministic = false;
        let resolve = |_: &CollationRefusingRead| Some(("public.s".to_string(), &ci));
        let new: Vec<FieldDef> = def.fields[1..].to_vec();
        assert_eq!(check_collation_reads(&def, Some(&new), resolve), Ok(()));
        let old: Vec<FieldDef> = def.fields[..1].to_vec();
        assert!(check_collation_reads(&def, Some(&old), resolve).is_err());
        assert!(check_collation_reads(&def, None, resolve).is_err());
    }

    /// The catalog checks a table's row-identity key before a plain
    /// aggregate's `GROUP BY` keys, so a definition failing both is refused
    /// for its source key.
    #[test]
    fn the_source_key_is_checked_before_the_group_by_keys() {
        let def = crate::defs::parser::parse("TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n")
            .expect("parse");
        let mut ci = collated(column(), "case_insensitive");
        ci.collation_deterministic = false;
        let deterministic = column();
        let key_of = |nondeterministic: &[&str]| {
            check_key_collations(&def, "public.s", &["id"], |name| {
                Some(if nondeterministic.contains(&name) {
                    &ci
                } else {
                    &deterministic
                })
            })
            .map_err(|err| match err {
                ValidationError::NondeterministicKeyCollation(refusal) => {
                    (refusal.key, refusal.column)
                }
                other => panic!("{other:?}"),
            })
        };
        assert_eq!(
            key_of(&["id", "g"]),
            Err((KeyColumnRole::SourceKey, "id".to_string()))
        );
        assert_eq!(
            key_of(&["g"]),
            Err((KeyColumnRole::GroupBy, "g".to_string()))
        );
        assert_eq!(key_of(&[]), Ok(()));
    }
}
