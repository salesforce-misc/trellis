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

use regex::Regex;

use super::ast::{
    Expr, FieldDef, KeySpace, Predicate, QualifiedColumn, TransformDef, ValueType,
    render_field_expr,
};
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
    /// The joined table is the `FROM` table. A self-join needs aliases, which
    /// the grammar doesn't have.
    JoinSelf { table: String },
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
    /// An aggregate in a deferred family (`MIN`/`MAX`, `bool_*`/`bit_*`,
    /// float or `interval` `SUM`/`AVG`, `array_agg`/`string_agg`/`jsonb_agg`).
    /// Only the aggregates the group basis keeps exact from signed deltas are
    /// admitted.
    DeferredAggregate {
        function: String,
        family: &'static str,
    },
    /// `SUM`/`AVG` over an unconstrained `numeric` column, whose display
    /// scale a running sum can't track.
    UnconstrainedNumericAggregate { function: String, column: String },
    /// `SUM`/`AVG` over a `numeric` column of a table Trellis creates (a
    /// target): Trellis creates its columns with no type modifier, so the
    /// column has no declared scale whatever the scale of the column it
    /// copies (ADR-0020, #1120).
    ChainedNumericAggregate { function: String, column: String },
    /// `SUM`/`AVG` over a column whose type is not an integer or a
    /// `numeric(p,s)`.
    UnsupportedAggregateArgument {
        function: String,
        column: String,
        pg_type: String,
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
    /// no implicit string concatenation).
    TypeMismatch {
        field: String,
        expected: ValueType,
        found: ValueType,
    },
    /// `>` was given a float on one side and an exact numeric (an integer of
    /// any width, or `numeric`) on the other. Postgres casts the exact side
    /// to `double precision`, so the comparison inherits float rounding;
    /// Trellis refuses the pair rather than admit a comparison that doesn't
    /// compose.
    MixedFloatExactComparison {
        field: String,
        lhs: ValueType,
        rhs: ValueType,
    },
    /// A bare number literal compared with a float lies outside
    /// `double precision`'s range (overflow, or a nonzero value that
    /// underflows to zero). Postgres raises `22003` for it at plan time.
    FloatComparisonLiteralOutOfRange { field: String, text: String },
    /// A constant argument of a `COALESCE` whose result is a float (a number
    /// literal, or a sum, `COALESCE` or calculated field made only of
    /// literals) lies outside that float type's range (overflow, or a nonzero
    /// value that underflows to zero). Postgres folds the constant, casts it
    /// to the float type and raises `22003` at plan time, for every row.
    /// `text` is the argument as written.
    CoalesceLiteralOutOfRange {
        field: String,
        text: String,
        target: ValueType,
    },
    /// A `+` whose operands are constants Postgres folds at plan time and
    /// whose value is out of range for the type it is computed or cast in:
    /// the sum of two integer constants past `integer`/`bigint`
    /// (`2147483647 + 1`), a float sum past the float's range, or an exact
    /// constant past `double precision` beside a float. Postgres raises
    /// `22003` at plan time, for every row. `text` is the offending
    /// expression as written and `target` the type it does not fit.
    ConstantOutOfRange {
        field: String,
        text: String,
        target: ValueType,
    },
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
    /// known at validate time so it can be compiled and checked here, once, rather than
    /// re-parsed on every row at eval time — a column-sourced pattern is out of scope.
    NonLiteralRegexPattern { field: String },
    /// `regexp_count`'s pattern literal does not compile as a regular
    /// expression, per the `regex` crate.
    InvalidRegexPattern {
        field: String,
        pattern: String,
        error: String,
    },
    /// A typed literal's text isn't in its family's **canonical**
    /// Postgres output spelling — `DATE '2024-1-5'`, `DATE 'today'`,
    /// `BYTEA '\xAB'`. See `super::typed_literal`'s module doc comment for
    /// why the bar is canonical form rather than "Postgres would parse it":
    /// in short, immutability (`date_in` is `STABLE` precisely because of
    /// `'today'`) and text round-trip identity between the literal and
    /// Postgres's rendering of the value.
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
    /// The typed key index
    /// (`docs/type-support.md`'s "Cross-cutting concerns") is what lifts it
    /// per family.
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
    /// expression isn't a bare passthrough of that same column (e.g.
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
    /// bare passthrough of that same column. Without this guard, a *different*
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
}

/// Every column an argument of a `STRPOS`/`REGEXP_COUNT` call in `def`'s
/// fields reads, for the catalog to check each
/// one's collation. In Postgres an argument's collation comes from the
/// columns under it: directly, through `COALESCE`, or through another
/// calculated field it references (whose expression Postgres would inline).
/// So this follows all three, resolving a bare name the way [`super::eval`]
/// does: another field of that name first, unless the name is the enclosing
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
    match expr {
        Expr::FunctionCall { name, args } => {
            // An aggregate's argument reads source columns, not aggregate
            // fields (see `aggregate_field_names`).
            let aggregate_scope;
            let fields_by_name = if super::registry::lookup_aggregate_function(name).is_some() {
                let aggregates =
                    aggregate_field_names(fields_by_name.iter().map(|(n, f)| (*n, &f.expr)));
                aggregate_scope = without_aggregate_fields(fields_by_name, &aggregates);
                &aggregate_scope
            } else {
                fields_by_name
            };
            if NONDETERMINISTIC_COLLATION_REFUSING_FUNCTIONS.contains(&name.as_str()) {
                let mut columns = Vec::new();
                let mut visited = HashSet::new();
                for arg in args {
                    collect_text_columns(arg, field, fields_by_name, &mut visited, &mut columns);
                }
                out.extend(columns.into_iter().map(|column| CollationRefusingRead {
                    field: field.to_string(),
                    function: name.clone(),
                    column,
                }));
            }
            for arg in args {
                collect_refusing_calls(arg, field, fields_by_name, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_refusing_calls(lhs, field, fields_by_name, out);
            collect_refusing_calls(rhs, field, fields_by_name, out);
        }
        Expr::Column(_)
        | Expr::SourceColumn { .. }
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => {}
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
    out: &mut Vec<String>,
) {
    match expr {
        Expr::Column(name) => match fields_by_name.get(name.as_str()) {
            Some(other) if name != field => {
                if visited.insert(other.name.as_str()) {
                    collect_text_columns(&other.expr, &other.name, fields_by_name, visited, out);
                }
            }
            _ => out.push(name.clone()),
        },
        Expr::SourceColumn { column, .. } => out.push(column.clone()),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_text_columns(arg, field, fields_by_name, visited, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_text_columns(lhs, field, fields_by_name, visited, out);
            collect_text_columns(rhs, field, fields_by_name, visited, out);
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {}
    }
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
                "the calculated fields read each other so deeply or so many times that inlining \
                 them into one expression yields more than a build can render; make the chain \
                 shorter"
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
                "'{table}' is joined to itself; a self-join needs table aliases, which this \
                 grammar doesn't have"
            ),
            ValidationError::JoinUnknownTable { reference } => write!(
                f,
                "'{reference}' names a table that is neither the FROM table nor the joined \
                 table"
            ),
            ValidationError::JoinUnqualifiedColumn { field, column } => write!(
                f,
                "field '{field}' reads column '{column}' without a table; in a JOIN every \
                 column is written `<table>.<column>`"
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
            ValidationError::DeferredAggregate { function, family } => write!(
                f,
                "{function} is not supported: {family} is not an aggregate Trellis maintains; \
                 the supported aggregates are COUNT(*), COUNT(col), and SUM/AVG over \
                 smallint, integer, bigint or numeric(p,s) (see \"Supported sources and \
                 targets\" in docs/transforms.md)"
            ),
            ValidationError::UnconstrainedNumericAggregate { function, column } => write!(
                f,
                "{function}({column}) is over a numeric with no declared scale, which is not \
                 supported: Postgres shows a sum with as many decimal places as the most \
                 precise value in the group, which a running sum can't track (see \"Supported \
                 sources and targets\" in docs/transforms.md). Sum a numeric(p,s) column \
                 directly (ALTER TABLE ... ALTER COLUMN ... TYPE numeric(p,s)); an expression \
                 over numeric has no declared scale"
            ),
            ValidationError::ChainedNumericAggregate { function, column } => write!(
                f,
                "{function}({column}) is over a numeric column of a table Trellis creates, \
                 which has no declared scale: Trellis creates its columns with no type \
                 modifier, so a chained SUM/AVG over one is not supported (see \"Supported \
                 sources and targets\" in docs/transforms.md). Sum the source's numeric(p,s) \
                 column in the transform that reads the source"
            ),
            ValidationError::UnsupportedAggregateArgument {
                function,
                column,
                pg_type,
            } => write!(
                f,
                "{function}({column}) is over {pg_type}; SUM and AVG are supported over \
                 smallint, integer, bigint and numeric(p,s) only"
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
                expected,
                found,
            } => write!(
                f,
                "calculated field '{field}' expects a {expected} value here, found {found}"
            ),
            ValidationError::FloatComparisonLiteralOutOfRange { field, text } => write!(
                f,
                "calculated field '{field}' compares a float with the literal {text}, which is \
                 out of range for double precision"
            ),
            ValidationError::CoalesceLiteralOutOfRange {
                field,
                text,
                target,
            } => write!(
                f,
                "calculated field '{field}' has a COALESCE argument {text}, a constant out of \
                 range for {target}, the type the COALESCE resolves to; Postgres cannot cast \
                 it. Use a constant within {target}'s range, or a column (see \"Supported \
                 sources and targets\" in docs/transforms.md)"
            ),
            ValidationError::ConstantOutOfRange {
                field,
                text,
                target,
            } => write!(
                f,
                "calculated field '{field}' has the constant {text}, which is out of range for \
                 {target}; Postgres folds it at plan time and raises an out-of-range error for \
                 every row. Use values that fit, or a column (see \"Supported sources and \
                 targets\" in docs/transforms.md)"
            ),
            ValidationError::MixedFloatExactComparison { field, lhs, rhs } => write!(
                f,
                "calculated field '{field}' compares a {lhs} value with a {rhs} value using \
                 '>'; a float and an exact numeric (an integer or numeric) cannot be \
                 compared, because Postgres would round the exact side to a float. Compare \
                 values of one family: rewrite so both sides are floats or both are exact \
                 (only a bare number literal such as 0.5 is admitted against a float)"
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
            ValidationError::InvalidRegexPattern {
                field,
                pattern,
                error,
            } => write!(
                f,
                "calculated field '{field}': regexp_count's pattern '{pattern}' is not a valid \
                 regular expression: {error}"
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
                 be a bare passthrough of that column (e.g. `{field}`), not another expression"
            ),
            ValidationError::CalculatedFieldShadowsSourceColumn { field } => write!(
                f,
                "calculated field '{field}' shares its name with a source column, so it must \
                 be a bare passthrough of that column (e.g. `{field}`), not another expression; \
                 a different name avoids shadowing the real column for other fields that \
                 reference it"
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
                write!(
                    f,
                    "calculated field '{field}': {function} reads {table}.{column}, whose \
                     collation \"{collation}\" is nondeterministic; Postgres refuses {function} \
                     under a nondeterministic collation, while Trellis would compute it from \
                     the exact text, so the column needs a deterministic collation"
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
    if let KeySpace::Join(_) = def.key_space {
        return Err(ValidationError::JoinNeedsBothTables);
    }

    validate_qualified_columns(def, source_columns, def.explicit_source_schema.as_deref())?;

    // Exhaustive matches: these are the "re-verify at this layer" guard for
    // the already-enforced 1-1/TRUE-only constructs (see module docs).
    // `Aggregate` has real runtime checks (below), since — unlike the other
    // arm here — the AST can encode an invalid one.
    match &def.key_space {
        KeySpace::Join(_) => return Err(ValidationError::JoinNeedsBothTables),
        KeySpace::OneToOne => {
            for field in &def.fields {
                // A calculated field aliased to a real source column's name
                // must be a bare passthrough of it — the same
                // shape the `Aggregate` arm below enforces for grouping
                // columns. `is_self_passthrough`'s exemption in `infer_expr`
                // only covers a field referencing its *own* name;
                // without this check here, a different, non-passthrough
                // field could still claim the column's name, and any *third*
                // field referencing that name would then silently resolve to
                // the calculated value instead of the real column.
                if source_columns.contains_key(&field.name)
                    && !matches!(&field.expr, Expr::Column(name) if name == &field.name)
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
            let mut seen: HashSet<&str> = HashSet::with_capacity(group_by.len());
            for key in group_by {
                let Some(value_type) = source_columns.get(key) else {
                    return Err(ValidationError::UnresolvedGroupByColumn {
                        column: key.clone(),
                    });
                };
                reject_unsupported_group_by_key_type(key, *value_type)?;
                // Each key becomes a target column named after
                // it, so a repeated key would reach the target DDL as a
                // duplicate column. Checked after the key itself resolves,
                // so an unknown column is reported as such rather than as a
                // clash.
                if !seen.insert(key) {
                    return Err(ValidationError::GroupByKeysShareTargetColumn {
                        first: key.clone(),
                        second: key.clone(),
                        column: key.clone(),
                    });
                }
                // A key's column sits beside the target's hidden
                // `__`-prefixed columns, so it may not use their prefix.
                if key.starts_with(RESERVED_COLUMN_PREFIX) {
                    return Err(ValidationError::ReservedGroupByColumnName {
                        key: key.clone(),
                        column: key.clone(),
                    });
                }
            }
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

    Ok(())
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

/// Which table of a `JOIN` a column belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
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
}

/// The per-family refusal of aggregates outside the admitted set: whether `function` (an
/// uppercased aggregate name) over `arg` is in the admitted set, `COUNT(*)`,
/// `COUNT(col)`, and `SUM`/`AVG` over `smallint`/`integer`/`bigint`/
/// `numeric(p,s)`, or else a named refusal. `arg` is `None` for `COUNT(*)`
/// and for a call whose argument isn't a resolved column.
///
/// Shared so every key-space refuses the same way: a `JOIN` and a plain
/// aggregate both use it.
pub(crate) fn check_aggregate_admitted(
    function: &str,
    arg: Option<&AggregateArg<'_>>,
) -> Result<(), ValidationError> {
    let function = function.to_ascii_uppercase();
    if let Some((_, family)) = DEFERRED_AGGREGATES
        .iter()
        .find(|(names, _)| names.contains(&function.as_str()))
    {
        return Err(ValidationError::DeferredAggregate { function, family });
    }
    match function.as_str() {
        "COUNT" => Ok(()),
        "SUM" | "AVG" => {
            let Some(arg) = arg else { return Ok(()) };
            match arg.value_type {
                ValueType::Integer(_) => Ok(()),
                ValueType::Numeric if arg.typmod != -1 => Ok(()),
                ValueType::Numeric if arg.trellis_target => {
                    Err(ValidationError::ChainedNumericAggregate {
                        function,
                        column: arg.column.to_string(),
                    })
                }
                ValueType::Numeric => Err(ValidationError::UnconstrainedNumericAggregate {
                    function,
                    column: arg.column.to_string(),
                }),
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
/// unconstrained one; an expression's `numeric` result has no declared scale.
///
/// Any other key space passes.
pub(crate) fn validate_aggregate_admitted(
    def: &TransformDef,
    columns: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let KeySpace::Aggregate { .. } = &def.key_space else {
        return Ok(());
    };
    let value_types: HashMap<String, ValueType> = columns
        .iter()
        .map(|(name, info)| (name.clone(), info.value_type))
        .collect();
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
        check_aggregate_calls(expr, columns, &value_types)?;
    }
    Ok(())
}

/// [`validate_aggregate_admitted`]'s walk of one field's expression.
fn check_aggregate_calls(
    expr: &Expr,
    columns: &HashMap<String, JoinColumnInfo>,
    value_types: &HashMap<String, ValueType>,
) -> Result<(), ValidationError> {
    match expr {
        Expr::BinaryOp { lhs, rhs, .. } => {
            check_aggregate_calls(lhs, columns, value_types)?;
            check_aggregate_calls(rhs, columns, value_types)
        }
        Expr::FunctionCall { name, args } => {
            let upper = name.to_ascii_uppercase();
            if !(is_deferred_aggregate_name(&upper)
                || super::registry::AGGREGATE_FUNCTIONS.contains(&upper.as_str()))
            {
                for arg in args {
                    check_aggregate_calls(arg, columns, value_types)?;
                }
                return Ok(());
            }
            check_aggregate_admitted(&upper, None)?;
            let [arg] = args.as_slice() else {
                return Ok(());
            };
            // `validate` has typed the definition already; an argument it
            // can't type here is left to it.
            let Ok(value_type) = infer_expr_type(arg, value_types) else {
                return Ok(());
            };
            let column = match arg {
                Expr::Column(column) | Expr::SourceColumn { column, .. } => columns.get(column),
                _ => None,
            };
            let (pg_type, typmod, trellis_target) = match column {
                Some(info) => (info.pg_type.clone(), info.typmod, info.trellis_target),
                None => (super::ddl::pg_type_name(value_type).into_owned(), -1, false),
            };
            check_aggregate_admitted(
                &upper,
                Some(&AggregateArg {
                    column: &render_field_expr(arg),
                    value_type,
                    pg_type: &pg_type,
                    typmod,
                    trellis_target,
                }),
            )
        }
        _ => Ok(()),
    }
}

fn written(column: &QualifiedColumn) -> String {
    match &column.schema {
        Some(schema) => format!("{schema}.{}.{}", column.table, column.column),
        None => format!("{}.{}", column.table, column.column),
    }
}

/// Validates a [`KeySpace::Join`] definition against the live columns of its
/// two tables (`left` is the `FROM` table, `right` the joined one).
///
/// The join is admitted when: the tables differ; every column is qualified
/// and exists; each `ON` term is an equality between one column of each
/// table, of one type, modifier and collation (the join-key rules); every `GROUP BY` key, from either table, is an admitted key type;
/// and each field is a key passthrough or composes admitted aggregates
/// (`COUNT(*)`, and `COUNT`/`SUM`/`AVG` of a column or an expression over
/// one table's columns, per [`check_aggregate_admitted`]) with literals,
/// operators and scalar functions. An argument reading
/// both tables is refused as [`ValidationError::JoinMixedSideArgument`].
pub fn validate_join(
    def: &TransformDef,
    left: &HashMap<String, JoinColumnInfo>,
    right: &HashMap<String, JoinColumnInfo>,
) -> Result<(), ValidationError> {
    let KeySpace::Join(join) = &def.key_space else {
        return Ok(());
    };
    validate_target_names(def)?;
    if join.right == def.source {
        return Err(ValidationError::JoinSelf {
            table: def.source.clone(),
        });
    }
    // The joined table is a source too: writing the target into it would
    // feed Trellis's own writes back into capture.
    if def.target == join.right {
        return Err(ValidationError::TargetEqualsSource {
            table: def.target.clone(),
        });
    }

    let resolve =
        |column: &QualifiedColumn| -> Result<(JoinSide, &JoinColumnInfo), ValidationError> {
            let side = if column.table == def.source {
                JoinSide::Left
            } else if column.table == join.right {
                JoinSide::Right
            } else {
                return Err(ValidationError::JoinUnknownTable {
                    reference: written(column),
                });
            };
            let schema_written = match side {
                JoinSide::Left => def.explicit_source_schema.as_deref(),
                JoinSide::Right => join.explicit_right_schema.as_deref(),
            };
            if let (Some(used), Some(declared)) = (column.schema.as_deref(), schema_written)
                && used != declared
            {
                return Err(ValidationError::JoinUnknownTable {
                    reference: written(column),
                });
            }
            let columns = match side {
                JoinSide::Left => left,
                JoinSide::Right => right,
            };
            columns
                .get(&column.column)
                .map(|info| (side, info))
                .ok_or_else(|| ValidationError::JoinUnresolvedColumn {
                    reference: written(column),
                })
        };

    let check_key_type = |column: &QualifiedColumn, info: &JoinColumnInfo| {
        if !super::join_key_types::is_text_stable_join_key_type(&info.pg_type) && !info.is_enum {
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

    let mut pairs: HashSet<(&str, &str)> = HashSet::with_capacity(join.on.len());
    for term in &join.on {
        let (lhs_side, lhs) = resolve(&term.lhs)?;
        let (rhs_side, rhs) = resolve(&term.rhs)?;
        if lhs_side == rhs_side {
            return Err(ValidationError::JoinOnSameSide {
                term: format!("{} = {}", written(&term.lhs), written(&term.rhs)),
            });
        }
        let pair = match lhs_side {
            JoinSide::Left => (term.lhs.column.as_str(), term.rhs.column.as_str()),
            JoinSide::Right => (term.rhs.column.as_str(), term.lhs.column.as_str()),
        };
        if !pairs.insert(pair) {
            return Err(ValidationError::JoinOnRepeatedPair {
                term: format!("{} = {}", written(&term.lhs), written(&term.rhs)),
            });
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

    let mut key_columns: HashMap<&str, &QualifiedColumn> =
        HashMap::with_capacity(join.group_by.len());
    for key in &join.group_by {
        let (_, info) = resolve(key)?;
        reject_unsupported_group_by_key_type(&key.column, info.value_type)?;
        if !info.collation_deterministic {
            return Err(ValidationError::NondeterministicKeyCollation(Box::new(
                NondeterministicKeyCollation {
                    key: KeyColumnRole::GroupBy,
                    table: key.table.clone(),
                    column: key.column.clone(),
                    collation: info.collation.clone().unwrap_or_default(),
                },
            )));
        }
        if let Some(earlier) = key_columns.insert(key.column.as_str(), key) {
            return Err(ValidationError::GroupByKeysShareTargetColumn {
                first: written(earlier),
                second: written(key),
                column: key.column.clone(),
            });
        }
        if key.column.starts_with(RESERVED_COLUMN_PREFIX) {
            return Err(ValidationError::ReservedGroupByColumnName {
                key: written(key),
                column: key.column.clone(),
            });
        }
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
        if key_columns.contains_key(field.name.as_str()) {
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
            right,
            &mut aggregates,
        )?;
        if aggregates == 0 {
            return Err(shape(
                "a field is a GROUP BY key or composes at least one aggregate",
            ));
        }
        // The composed field's type, over both tables' columns: each column
        // is renamed apart by side, since the two tables may share names.
        join_field_value_type(def, field, left, right)?;
    }
    Ok(())
}

/// One column-read side of an aggregate argument, for the mixed-side check.
type ResolveJoinColumn<'a> =
    dyn Fn(&QualifiedColumn) -> Result<(JoinSide, &'a JoinColumnInfo), ValidationError> + 'a;

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
    mut in_aggregate: Option<&mut Vec<JoinSide>>,
    left: &HashMap<String, JoinColumnInfo>,
    right: &HashMap<String, JoinColumnInfo>,
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
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => Ok(()),
        Expr::BinaryOp { lhs, rhs, .. } => {
            check_join_field_expr(
                lhs,
                field,
                resolve,
                in_aggregate.as_deref_mut(),
                left,
                right,
                aggregates,
            )?;
            check_join_field_expr(rhs, field, resolve, in_aggregate, left, right, aggregates)
        }
        Expr::FunctionCall { name, args } => {
            let upper = name.to_ascii_uppercase();
            let is_aggregate = is_deferred_aggregate_name(&upper)
                || super::registry::AGGREGATE_FUNCTIONS.contains(&upper.as_str());
            if !is_aggregate {
                for arg in args {
                    check_join_field_expr(
                        arg,
                        field,
                        resolve,
                        in_aggregate.as_deref_mut(),
                        left,
                        right,
                        aggregates,
                    )?;
                }
                return Ok(());
            }
            if in_aggregate.is_some() {
                return Err(shape("an aggregate can't be nested in another aggregate"));
            }
            check_aggregate_admitted(&upper, None)?;
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
                left,
                right,
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
            let columns = match side {
                JoinSide::Left => left,
                JoinSide::Right => right,
            };
            // The argument's type over its one side, and for a lone column
            // its declared modifier (an expression's `numeric` result has no
            // declared scale).
            let value_types: HashMap<String, ValueType> = columns
                .iter()
                .map(|(name, info)| (name.clone(), info.value_type))
                .collect();
            let value_type = infer_expr_type(arg, &value_types)?;
            let (pg_type, typmod, trellis_target) = match arg {
                Expr::SourceColumn { column, .. } => columns
                    .get(column)
                    .map(|info| (info.pg_type.clone(), info.typmod, info.trellis_target))
                    .unwrap_or_default(),
                _ => (super::ddl::pg_type_name(value_type).into_owned(), -1, false),
            };
            check_aggregate_admitted(
                &upper,
                Some(&AggregateArg {
                    column: &render_field_expr(arg),
                    value_type,
                    pg_type: &pg_type,
                    typmod,
                    trellis_target,
                }),
            )
        }
    }
}

/// Rewrites every side-qualified column in `expr` to a bare name unique to
/// its side, recording its type in `types`, so a composed field can be typed
/// by the single-table inference.
fn rename_join_columns(
    expr: &mut Expr,
    source: &str,
    left: &HashMap<String, JoinColumnInfo>,
    right: &HashMap<String, JoinColumnInfo>,
    types: &mut HashMap<String, ValueType>,
) {
    match expr {
        Expr::SourceColumn { table, column, .. } => {
            let (prefix, columns) = if table == source {
                ("l", left)
            } else {
                ("r", right)
            };
            let renamed = format!("{prefix}\u{1}{column}");
            if let Some(info) = columns.get(column.as_str()) {
                types.insert(renamed.clone(), info.value_type);
            }
            *expr = Expr::Column(renamed);
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            rename_join_columns(lhs, source, left, right, types);
            rename_join_columns(rhs, source, left, right, types);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                rename_join_columns(arg, source, left, right, types);
            }
        }
        _ => {}
    }
}

/// The type of a `JOIN` definition's field that isn't a `GROUP BY` key, as
/// [`validate_join`] infers it: each column typed by its own side, the
/// single-table inference over the rest. Only meaningful for a definition
/// [`validate_join`] accepts.
pub(crate) fn join_field_value_type(
    def: &TransformDef,
    field: &super::ast::FieldDef,
    left: &HashMap<String, JoinColumnInfo>,
    right: &HashMap<String, JoinColumnInfo>,
) -> Result<ValueType, ValidationError> {
    let mut renamed = field.expr.clone();
    let mut types = HashMap::new();
    rename_join_columns(&mut renamed, &def.source, left, right, &mut types);
    let types_by_field = infer_field_types(
        &TransformDef {
            fields: vec![super::ast::FieldDef {
                name: field.name.clone(),
                expr: renamed,
            }],
            key_space: KeySpace::Aggregate {
                group_by: Vec::new(),
            },
            ..def.clone()
        },
        &types,
    )?;
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
            Expr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, field, def, source_columns, source_schema)?;
                walk(rhs, field, def, source_columns, source_schema)
            }
            Expr::FunctionCall { args, .. } => args
                .iter()
                .try_for_each(|arg| walk(arg, field, def, source_columns, source_schema)),
            Expr::Column(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::TypedLiteral { .. } => Ok(()),
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
        .filter(|f| {
            source_columns.contains_key(&f.name)
                && !matches!(&f.expr, Expr::Column(name) if name == &f.name)
        })
        .map(|f| (f.name.as_str(), f))
        .collect();
    if shadowing.is_empty() {
        return Ok(());
    }
    let aggregates = aggregate_field_names(def.fields.iter().map(|f| (f.name.as_str(), &f.expr)));

    fn first_ambiguous<'a>(
        expr: &'a Expr,
        reader: &str,
        in_aggregate_arg: bool,
        shadowing: &HashMap<&str, &FieldDef>,
        aggregates: &HashSet<&str>,
    ) -> Option<&'a str> {
        match expr {
            Expr::Column(name) => {
                let is_shadowed = name != reader && shadowing.contains_key(name.as_str());
                let reads_source_by_695 = in_aggregate_arg && aggregates.contains(name.as_str());
                (is_shadowed && !reads_source_by_695).then_some(name.as_str())
            }
            Expr::BinaryOp { lhs, rhs, .. } => {
                first_ambiguous(lhs, reader, in_aggregate_arg, shadowing, aggregates).or_else(
                    || first_ambiguous(rhs, reader, in_aggregate_arg, shadowing, aggregates),
                )
            }
            Expr::FunctionCall { name, args } => {
                let in_arg =
                    in_aggregate_arg || super::registry::lookup_aggregate_function(name).is_some();
                args.iter()
                    .find_map(|arg| first_ambiguous(arg, reader, in_arg, shadowing, aggregates))
            }
            Expr::SourceColumn { .. }
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::TypedLiteral { .. } => None,
        }
    }

    for reader in &def.fields {
        if let Some(name) =
            first_ambiguous(&reader.expr, &reader.name, false, &shadowing, &aggregates)
        {
            return Err(ValidationError::AmbiguousColumnReference(Box::new(
                AmbiguousColumnReference {
                    field: reader.name.clone(),
                    name: name.to_string(),
                    shadowing_expr: render_field_expr(&shadowing[name].expr),
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
/// a bare passthrough) would be wrong for a key column.
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
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => Ok(()),
        Expr::BinaryOp { lhs, rhs, .. } => {
            validate_aggregate_field_expr(
                lhs,
                field_name,
                group_by,
                source_columns,
                in_aggregate_call,
            )?;
            validate_aggregate_field_expr(
                rhs,
                field_name,
                group_by,
                source_columns,
                in_aggregate_call,
            )
        }
        Expr::FunctionCall { name, args } => {
            let is_aggregate_call = super::registry::lookup_aggregate_function(name).is_some();
            for arg in args {
                validate_aggregate_field_expr(
                    arg,
                    field_name,
                    group_by,
                    source_columns,
                    in_aggregate_call || is_aggregate_call,
                )?;
            }
            Ok(())
        }
    }
}

/// Whether `expr` is exactly the passthrough shape `GROUP BY` key `key`
/// requires of a field named after it (see
/// [`ValidationError::GroupingColumnFieldMustBePassthrough`]): a bare
/// [`Expr::Column`] of the same name — there is no other expression shape
/// DDL generation gives a target column of its own for that name, so
/// anything else is rejected rather than silently dropped.
fn expr_is_group_by_key_passthrough(expr: &Expr, key: &str) -> bool {
    matches!(expr, Expr::Column(n) if n == key)
}

pub(super) fn collect_columns(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(name) => out.push(name.clone()),
        // Always a source column, never a read of a field.
        Expr::SourceColumn { .. } => {}
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_columns(lhs, out);
            collect_columns(rhs, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
    }
}

/// Whether `expr` contains an aggregate call (`SUM`, `COUNT`, ...).
fn has_aggregate_call(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args } => {
            super::registry::lookup_aggregate_function(name).is_some()
                || args.iter().any(has_aggregate_call)
        }
        Expr::BinaryOp { lhs, rhs, .. } => has_aggregate_call(lhs) || has_aggregate_call(rhs),
        Expr::Column(_)
        | Expr::SourceColumn { .. }
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => false,
    }
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
            let mut reads = Vec::new();
            collect_columns(expr, &mut reads);
            if reads
                .iter()
                .any(|read| read != name && aggregates.contains(read.as_str()))
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

/// [`collect_columns`] split by what each column name resolves to. `fields`
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
        // Always a source column, never a field; `validate`
        // checks it names one through `validate_qualified_columns`.
        Expr::SourceColumn { .. }
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_reads(lhs, aggregates, in_aggregate_arg, fields, aggregate_args);
            collect_reads(rhs, aggregates, in_aggregate_arg, fields, aggregate_args);
        }
        Expr::FunctionCall { name, args } => {
            let in_arg =
                in_aggregate_arg || super::registry::lookup_aggregate_function(name).is_some();
            for arg in args {
                collect_reads(arg, aggregates, in_arg, fields, aggregate_args);
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
/// gets the same `Cycle`/defense-in-depth treatment [`super::eval`] gives
/// its own recursion, rather than overflowing the stack.
/// Rejects an [`super::ast::KeySpace::Aggregate`] `GROUP BY` key whose
/// column is typed [`ValueType::Other`] — see
/// [`ValidationError::UnsupportedGroupByKeyType`] for why a key role needs
/// more than the passthrough classification.
pub(crate) fn reject_unsupported_group_by_key_type(
    column: &str,
    value_type: ValueType,
) -> Result<(), ValidationError> {
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
        ValueType::Integer(_) | ValueType::Numeric | ValueType::Text | ValueType::Uuid => Ok(()),
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
        ValueType::Boolean => Ok(()),
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
        // comparing decoded values through `crate::float::compare`; until
        // then, rejecting is the honest answer and `numeric` is the
        // steer-to. See `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`.
        ValueType::Float(_) => Err(ValidationError::UnsupportedGroupByKeyType {
            column: column.to_string(),
            value_type,
        }),
        // `oid` is one `Other` family admitted as a key:
        // Postgres renders it as canonical unsigned decimal, so it is
        // text-stable in exactly the way `interval` is not. It stays an
        // `Other` rather than joining `ValueType::Integer` because Postgres
        // gives it no arithmetic at all — see `pg_type::PgType::Oid`.
        ValueType::Other(PgType::Oid) => Ok(()),
        // `bytea` is the other, on the same "text-stability is
        // a property of the rendering, not the operator set" reasoning:
        // `byteaout` under the pinned `bytea_output = 'hex'` is a bijection
        // on every byte string, with no session-GUC dependence at all (see
        // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment for the live
        // evidence). Unlike the temporal families it needs no per-value
        // predicate here — every `bytea` value clears the bar, not just a
        // subset — so it is a plain admit rather than a call into a sibling
        // module.
        ValueType::Other(PgType::Bytea) => Ok(()),
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
        ValueType::Other(pg_type) if crate::temporal::is_text_stable(pg_type) => Ok(()),
        // `cidr`/`macaddr`/`macaddr8` join `oid`/`bytea` above —
        // each is on `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` (see that
        // constant's doc comment for the live `pg_cast`/renderer evidence),
        // and unlike the temporal families every value of each clears the
        // bar, not just a subset, so a plain admit is enough here too.
        ValueType::Other(PgType::Cidr | PgType::MacAddr | PgType::MacAddr8) => Ok(()),
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
        ValueType::Other(PgType::Inet) => Ok(()),
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
        ValueType::Other(PgType::VarBit) => Ok(()),
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
        // distinct values to collide under — see `join_key_types::is_enum_type_name`'s
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
        ValueType::Other(PgType::Enum(_)) => Ok(()),
        ValueType::Other(_) => Err(ValidationError::UnsupportedGroupByKeyType {
            column: column.to_string(),
            value_type,
        }),
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

pub(crate) fn infer_field_types(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<HashMap<String, ValueType>, ValidationError> {
    let fields_by_name: HashMap<&str, &super::ast::FieldDef> =
        def.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    let mut types: HashMap<String, ValueType> = HashMap::with_capacity(def.fields.len());
    let mut in_progress: HashSet<String> = HashSet::new();
    for field in &def.fields {
        if !types.contains_key(&field.name) {
            let t = infer_field(
                field,
                source_columns,
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
    infer_expr(
        expr,
        "",
        source_columns,
        &HashMap::new(),
        &mut HashMap::new(),
        &mut HashSet::new(),
        true,
    )
}

fn infer_field(
    field: &super::ast::FieldDef,
    source_columns: &HashMap<String, ValueType>,
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
    // it.
    let t = infer_expr(
        &field.expr,
        &field.name,
        source_columns,
        fields_by_name,
        types,
        in_progress,
        true,
    );
    in_progress.remove(&field.name);
    let t = t?;
    types.insert(field.name.clone(), t);
    Ok(t)
}

/// A constant Postgres folds at plan time: an exact value, or a float with
/// the width it is computed in.
#[derive(Clone)]
enum Constant {
    Exact(crate::numeric::Numeric),
    Float(f64, crate::float::FloatWidth),
}

impl Constant {
    /// The value as the float an operand beside a float is cast to (`double
    /// precision` for an exact one), `None` where Postgres's cast raises.
    fn as_float(&self) -> Option<(f64, crate::float::FloatWidth)> {
        match self {
            Constant::Exact(n) => crate::float::literal_to_f64(&n.to_string())
                .ok()
                .map(|v| (v, crate::float::FloatWidth::Float8)),
            Constant::Float(v, w) => Some((*v, *w)),
        }
    }
}

/// The value of `expr` when it is a constant Postgres folds at plan time: a
/// number literal, a float typed literal, a `+` of two constants, a
/// `COALESCE` whose first argument is one (the grammar has no `NULL`, so it
/// is the result), or a calculated field whose expression is one, resolved
/// as [`infer_expr`] resolves a name. `None` for anything that reads a
/// column, and for a sum whose value Postgres could not compute (which
/// [`check_constant_sum`] has refused). `memo` holds each calculated field
/// already folded, so a chain of fields that each read the one before twice
/// folds in linear time.
fn fold_constant<'f>(
    expr: &Expr,
    field_name: &str,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&'f str, &'f super::ast::FieldDef>,
    memo: &mut HashMap<&'f str, Option<Constant>>,
    depth: usize,
) -> Option<Constant> {
    // Inference has already refused a cycle; this bounds a hand-built one.
    if depth > fields_by_name.len() + 64 {
        return None;
    }
    let mut recurse = |expr: &Expr, field_name: &str| {
        fold_constant(
            expr,
            field_name,
            source_columns,
            fields_by_name,
            memo,
            depth + 1,
        )
    };
    match expr {
        Expr::NumberLiteral(text) => crate::numeric::Numeric::parse(text)
            .ok()
            .map(Constant::Exact),
        Expr::TypedLiteral {
            value_type: ValueType::Float(width),
            text,
        } => crate::float::parse(text, *width)
            .ok()
            .map(|v| Constant::Float(v, *width)),
        Expr::BinaryOp {
            op: super::ast::Operator::Add,
            lhs,
            rhs,
        } => match (recurse(lhs, field_name)?, recurse(rhs, field_name)?) {
            (Constant::Exact(a), Constant::Exact(b)) => Some(Constant::Exact(a.add(&b))),
            (a, b) => {
                let (a, a_width) = a.as_float()?;
                let (b, b_width) = b.as_float()?;
                crate::float::checked_add(a, a_width, b, b_width)
                    .ok()
                    .map(|(sum, width)| Constant::Float(sum, width))
            }
        },
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            recurse(args.first()?, field_name)
        }
        Expr::Column(name) => {
            let is_self_passthrough = name == field_name && source_columns.contains_key(name);
            let calc_field = *fields_by_name
                .get(name.as_str())
                .filter(|_| !is_self_passthrough)?;
            if let Some(folded) = memo.get(calc_field.name.as_str()) {
                return folded.clone();
            }
            let folded = fold_constant(
                &calc_field.expr,
                &calc_field.name,
                source_columns,
                fields_by_name,
                memo,
                depth + 1,
            );
            memo.insert(calc_field.name.as_str(), folded.clone());
            folded
        }
        _ => None,
    }
}

/// Whether `expr` reads no column (directly or through a calculated field)
/// and no aggregate: an expression the planner folds to a constant.
/// `memo` holds each calculated field already resolved.
fn reads_no_column<'f>(
    expr: &Expr,
    field_name: &str,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&'f str, &'f super::ast::FieldDef>,
    memo: &mut HashMap<&'f str, bool>,
    depth: usize,
) -> bool {
    // Inference has already refused a cycle; this bounds a hand-built one.
    if depth > fields_by_name.len() + 64 {
        return false;
    }
    match expr {
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => true,
        Expr::SourceColumn { .. } => false,
        Expr::BinaryOp { lhs, rhs, .. } => {
            reads_no_column(
                lhs,
                field_name,
                source_columns,
                fields_by_name,
                memo,
                depth + 1,
            ) && reads_no_column(
                rhs,
                field_name,
                source_columns,
                fields_by_name,
                memo,
                depth + 1,
            )
        }
        Expr::FunctionCall { name, args } => {
            super::registry::lookup_aggregate_function(name).is_none()
                && args.iter().all(|arg| {
                    reads_no_column(
                        arg,
                        field_name,
                        source_columns,
                        fields_by_name,
                        memo,
                        depth + 1,
                    )
                })
        }
        Expr::Column(name) => {
            let is_self_passthrough = name == field_name && source_columns.contains_key(name);
            let Some(calc_field) = fields_by_name
                .get(name.as_str())
                .filter(|_| !is_self_passthrough)
                .copied()
            else {
                return false;
            };
            if let Some(&constant) = memo.get(calc_field.name.as_str()) {
                return constant;
            }
            let constant = reads_no_column(
                &calc_field.expr,
                &calc_field.name,
                source_columns,
                fields_by_name,
                memo,
                depth + 1,
            );
            memo.insert(calc_field.name.as_str(), constant);
            constant
        }
    }
}

/// [`fold_constant`] restricted to an exact value.
fn exact_constant<'f>(
    expr: &Expr,
    field_name: &str,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&'f str, &'f super::ast::FieldDef>,
    memo: &mut HashMap<&'f str, Option<Constant>>,
) -> Option<crate::numeric::Numeric> {
    match fold_constant(expr, field_name, source_columns, fields_by_name, memo, 0)? {
        Constant::Exact(n) => Some(n),
        Constant::Float(..) => None,
    }
}

/// Refuses a `+` Postgres cannot compute at plan time, whatever the row:
/// two integer constants whose sum does not fit the result width, two float
/// constants whose sum overflows the float, or an exact constant beside a
/// float that does not fit `double precision` (Postgres casts it to the
/// float's type when it plans the `+`). An operand that reads a column is
/// not a constant, so `col + 2147483647` still defines and fails only for
/// the rows it overflows. Each nested `+` is checked as it is inferred, so
/// the innermost offender is the one reported.
fn check_constant_sum<'f>(
    expr: &Expr,
    (lhs, rhs): (&Expr, &Expr),
    result: ValueType,
    field_name: &str,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&'f str, &'f super::ast::FieldDef>,
) -> Result<(), ValidationError> {
    let out_of_range = |arg: &Expr, target: ValueType| ValidationError::ConstantOutOfRange {
        field: field_name.to_string(),
        text: super::ast::render_field_expr(arg),
        target,
    };
    let mut memo = HashMap::new();
    let mut fold =
        |e: &Expr| fold_constant(e, field_name, source_columns, fields_by_name, &mut memo, 0);
    let (l, r) = (fold(lhs), fold(rhs));
    match result {
        ValueType::Integer(width) => {
            if let (Some(Constant::Exact(a)), Some(Constant::Exact(b))) = (&l, &r) {
                let fits = a
                    .add(b)
                    .to_string()
                    .parse::<i64>()
                    .is_ok_and(|v| width.fits(v));
                if !fits {
                    return Err(out_of_range(expr, result));
                }
            }
        }
        ValueType::Float(_) => {
            for (arg, constant) in [(lhs, &l), (rhs, &r)] {
                if let Some(c @ Constant::Exact(_)) = constant
                    && c.as_float().is_none()
                {
                    return Err(out_of_range(
                        arg,
                        ValueType::Float(crate::float::FloatWidth::Float8),
                    ));
                }
            }
            if let (Some(a), Some(b)) = (&l, &r)
                && let (Some((a, a_width)), Some((b, b_width))) = (a.as_float(), b.as_float())
                && crate::float::checked_add(a, a_width, b, b_width).is_err()
            {
                return Err(out_of_range(expr, result));
            }
        }
        _ => {}
    }
    Ok(())
}

/// `folded` is whether Postgres's planner simplifies `expr`, so that a
/// constant in it out of range for its type raises at plan time: false only
/// under a `COALESCE` argument after a constant one, which the planner drops
/// unevaluated (`COALESCE(5, 2147483647 + 1)` is `5`). The type rules hold
/// either way; only the plan-time range refusals are skipped.
#[allow(clippy::too_many_arguments)]
fn infer_expr(
    expr: &Expr,
    field_name: &str,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
    folded: bool,
) -> Result<ValueType, ValidationError> {
    match expr {
        Expr::Column(name) => {
            // See the matching comment in `validate`: a field referencing a
            // source column of its own name is a passthrough, not a
            // self-dependency, so it's resolved via `source_columns` below
            // rather than recursing into its own (in-progress) inference.
            let is_self_passthrough = name == field_name && source_columns.contains_key(name);
            if !is_self_passthrough && let Some(calc_field) = fields_by_name.get(name.as_str()) {
                return infer_field(
                    calc_field,
                    source_columns,
                    fields_by_name,
                    types,
                    in_progress,
                );
            }
            // Column resolution against `source_columns` already happened
            // in `validate`; a name absent here (standalone use, e.g. tests
            // calling `infer_field_types` directly) defaults to Numeric,
            // matching `eval`'s same fallback.
            Ok(source_columns
                .get(name)
                .copied()
                .unwrap_or(ValueType::Numeric))
        }
        // A qualified name is always the source column.
        Expr::SourceColumn { column, .. } => Ok(source_columns
            .get(column)
            .copied()
            .unwrap_or(ValueType::Numeric)),
        // An unadorned literal is typed exactly as Postgres's
        // own lexer types it — `integer` if it fits, else `bigint`, else
        // (or if fractional) `numeric`.
        Expr::NumberLiteral(text) => Ok(number_literal_type(text)),
        Expr::StringLiteral(_) => Ok(ValueType::Text),
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
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs_t = infer_expr(
                lhs,
                field_name,
                source_columns,
                fields_by_name,
                types,
                in_progress,
                folded,
            )?;
            let rhs_t = infer_expr(
                rhs,
                field_name,
                source_columns,
                fields_by_name,
                types,
                in_progress,
                folded,
            )?;
            // Operand admissibility *and* the result type both
            // come from `registry::operator_result_type`, which models
            // Postgres's family of `+`/`>` operators (`int4pl`,
            // `numeric_add`, and the implicit `int -> numeric` coercion
            // between them) rather than the single exact signature this used
            // to compare against. `OperatorSpec::arg_types` survives only as
            // the canonical shape a `TypeMismatch` names.
            if let Some(result) = super::registry::operator_result_type(*op, lhs_t, rhs_t) {
                if folded && matches!(op, super::ast::Operator::Add) {
                    check_constant_sum(
                        expr,
                        (lhs, rhs),
                        result,
                        field_name,
                        source_columns,
                        fields_by_name,
                    )?;
                }
                return Ok(result);
            }
            if matches!(op, super::ast::Operator::GreaterThan)
                && super::registry::is_mixed_float_exact(lhs_t, rhs_t)
            {
                // A *bare* number literal against a float is Postgres's
                // `float_col > 0.5`: the untyped literal is cast to
                // `double precision` (a `real` operand widens to it) and
                // the comparison runs as `float8`. Only the literal's own
                // syntax is exempt: the same value in a column, a calculated
                // field, an expression or a typed `NUMERIC '0.5'` literal
                // still hits the refusal below.
                let literal = match (&**lhs, &**rhs) {
                    (Expr::NumberLiteral(text), _) if matches!(rhs_t, ValueType::Float(_)) => {
                        Some(text)
                    }
                    (_, Expr::NumberLiteral(text)) if matches!(lhs_t, ValueType::Float(_)) => {
                        Some(text)
                    }
                    _ => None,
                };
                if let Some(text) = literal {
                    // Postgres raises `22003` at plan time for a literal
                    // outside `double precision`'s range; refuse it at
                    // define instead of failing every row.
                    if folded {
                        crate::float::literal_to_f64(text).map_err(|_| {
                            ValidationError::FloatComparisonLiteralOutOfRange {
                                field: field_name.to_string(),
                                text: text.clone(),
                            }
                        })?;
                    }
                    return Ok(ValueType::Boolean);
                }
                return Err(ValidationError::MixedFloatExactComparison {
                    field: field_name.to_string(),
                    lhs: lhs_t,
                    rhs: rhs_t,
                });
            }
            // Blame the operand that actually isn't admissible. At least one
            // of them isn't (or resolution above would have succeeded), and
            // the left one is reported first so the error reads in source
            // order.
            let spec = super::registry::operator_spec(*op);
            let (found, expected) = if lhs_t.is_exact_numeric_family() {
                (rhs_t, spec.arg_types.1)
            } else {
                (lhs_t, spec.arg_types.0)
            };
            Err(ValidationError::TypeMismatch {
                field: field_name.to_string(),
                expected,
                found,
            })
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            // The planner simplifies the arguments in order and stops at the
            // first constant one, which is the result (the grammar has no
            // `NULL`): it drops the rest unevaluated, so no constant in
            // them raises.
            let planned = if folded {
                let mut memo = HashMap::new();
                args.iter()
                    .position(|arg| {
                        reads_no_column(
                            arg,
                            field_name,
                            source_columns,
                            fields_by_name,
                            &mut memo,
                            0,
                        )
                    })
                    .map_or(args.len(), |first_constant| first_constant + 1)
            } else {
                0
            };
            let mut common_type = None;
            for (i, arg) in args.iter().enumerate() {
                let arg_t = infer_expr(
                    arg,
                    field_name,
                    source_columns,
                    fields_by_name,
                    types,
                    in_progress,
                    i < planned,
                )?;
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
            let common_type = common_type.unwrap();
            // Postgres casts each argument to the COALESCE's resolved type,
            // and folds a constant one (a number literal, or a sum, COALESCE
            // or calculated field made only of them) at plan time. For an
            // exact result the constant's own type already unifies upward
            // (an integer literal too wide for `integer` is `bigint`, then
            // `numeric`), so only a float result can leave it out of range.
            if let ValueType::Float(width) = common_type {
                let mut memo = HashMap::new();
                for arg in &args[..planned] {
                    if let Some(value) =
                        exact_constant(arg, field_name, source_columns, fields_by_name, &mut memo)
                        && crate::float::literal_to_width(&value.to_string(), width).is_err()
                    {
                        return Err(ValidationError::CoalesceLiteralOutOfRange {
                            field: field_name.to_string(),
                            text: super::ast::render_field_expr(arg),
                            target: common_type,
                        });
                    }
                }
            }
            Ok(common_type)
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
            // An aggregate outside the admitted set is refused
            // by family, before its argument is typed: the grammar parses
            // these names only to refuse them.
            if is_deferred_aggregate_name(name) {
                check_aggregate_admitted(&name.to_ascii_uppercase(), None)?;
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
                    infer_expr(
                        arg,
                        field_name,
                        source_columns,
                        fields_by_name,
                        types,
                        in_progress,
                        folded,
                    )?;
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
                    infer_expr(
                        arg,
                        field_name,
                        source_columns,
                        fields_by_name,
                        types,
                        in_progress,
                        folded,
                    )?;
                }
                return Ok(spec.return_type);
            }
            let mut arg_types: Vec<ValueType> = Vec::with_capacity(args.len());
            for (i, (arg, expected)) in args.iter().zip(spec.arg_types).enumerate() {
                let arg_t = infer_expr(
                    arg,
                    field_name,
                    source_columns,
                    fields_by_name,
                    types,
                    in_progress,
                    folded,
                )?;
                // A `Numeric`-declared *aggregate* argument now
                // means "any exact-numeric type" — `SUM`/`AVG`/`MIN`/`MAX`
                // accept every exact-integer width as well as `numeric`, and
                // the result type depends on which one
                // (`registry::aggregate_result_type`). A scalar function's
                // argument list stays an exact match: all four of those take
                // `Text`, and Postgres would not coerce an integer into one.
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
fn common_numeric_type(a: ValueType, b: ValueType) -> Option<ValueType> {
    if a == b {
        return Some(a);
    }
    if !a.is_numeric_family() || !b.is_numeric_family() {
        return None;
    }
    Some(match (a, b) {
        // `COALESCE` resolves through Postgres's
        // `select_common_type`, which is *not* the same algorithm operator
        // overload resolution uses, and the difference is visible: a live
        // server types `coalesce(1::real, 1::numeric)` (and
        // `coalesce(1::numeric, 1::real)`, and both orders of
        // `real`/`integer`) as **`real`**, while `1::real + 1::numeric` is
        // `double precision`. So a float wins over every exact type here
        // without being widened, and only a genuine `double precision`
        // operand produces `double precision`.
        (ValueType::Float(x), ValueType::Float(y)) => ValueType::Float(x.wider(y)),
        (ValueType::Float(x), _) => ValueType::Float(x),
        (_, ValueType::Float(y)) => ValueType::Float(y),
        (ValueType::Integer(x), ValueType::Integer(y)) => ValueType::Integer(x.wider(y)),
        _ => ValueType::Numeric,
    })
}

/// Checks `regexp_count`'s pattern argument is a string literal that
/// compiles as a regular expression. Validating once here, rather than at
/// eval time on every row, is why the pattern must be a literal at all —
/// see [`ValidationError::NonLiteralRegexPattern`].
fn validate_regexp_pattern(field_name: &str, pattern_arg: &Expr) -> Result<(), ValidationError> {
    let Expr::StringLiteral(pattern) = pattern_arg else {
        return Err(ValidationError::NonLiteralRegexPattern {
            field: field_name.to_string(),
        });
    };
    Regex::new(pattern).map_err(|error| ValidationError::InvalidRegexPattern {
        field: field_name.to_string(),
        pattern: pattern.clone(),
        error: error.to_string(),
    })?;
    Ok(())
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
        Expr::SourceColumn { .. }
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => depth,
        Expr::BinaryOp { lhs, rhs, .. } => {
            column_depths_in(lhs, depth + 1, out).max(column_depths_in(rhs, depth + 1, out))
        }
        Expr::FunctionCall { args, .. } => args
            .iter()
            .map(|arg| column_depths_in(arg, depth + 1, out))
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
            match reject_unsupported_group_by_key_type("k", value_type) {
                Ok(()) => {
                    if !admitted_names.contains(&name) {
                        admitted_names.push(name);
                    }
                }
                Err(ValidationError::UnsupportedGroupByKeyType { .. }) => {}
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
    fn greater_than_over_a_text_operand_is_a_type_mismatch() {
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
            ValidationError::TypeMismatch {
                field: "out".to_string(),
                expected: ValueType::Numeric,
                found: ValueType::Text,
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

    #[test]
    fn a_bare_literal_outside_double_precision_is_refused_at_define() {
        // Postgres raises 22003 for these at plan time (`select
        // 1<309 digits>::float8`, and a nonzero value that underflows).
        let source_columns =
            HashMap::from([("f".to_string(), ValueType::Float(FloatWidth::Float4))]);
        let big = "9".repeat(310);
        let tiny = format!("0.{}1", "0".repeat(400));
        let subnormal = format!("0.{}5", "0".repeat(323)); // 5e-324, finite
        for text in [&big, &tiny] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT f > {text} AS out"));
            assert_eq!(
                validate(&d, &source_columns),
                Err(ValidationError::FloatComparisonLiteralOutOfRange {
                    field: "out".to_string(),
                    text: text.clone(),
                })
            );
        }
        let d = parsed(&format!("TRANSFORM t FROM s SELECT f > {subnormal} AS out"));
        assert_eq!(validate(&d, &source_columns), Ok(()));
        let d = parsed("TRANSFORM t FROM s SELECT f > 0.0000 AS out");
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn a_coalesce_literal_outside_the_floats_range_is_refused_at_define() {
        let source_columns = HashMap::from([
            ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("n".to_string(), ValueType::Numeric),
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        let beyond_real = format!("1{}", "0".repeat(39));
        let beyond_double = "9".repeat(310);
        let underflow_real = format!("0.{}1", "0".repeat(50));
        let fractional = format!("{beyond_real}.0");
        for (expr, text, target) in [
            (
                format!("COALESCE(r, {beyond_real})"),
                &beyond_real,
                FloatWidth::Float4,
            ),
            (
                format!("COALESCE({beyond_real}, r)"),
                &beyond_real,
                FloatWidth::Float4,
            ),
            (
                format!("COALESCE(r, n, {beyond_real})"),
                &beyond_real,
                FloatWidth::Float4,
            ),
            (
                format!("COALESCE(i, r, {fractional})"),
                &fractional,
                FloatWidth::Float4,
            ),
            (
                format!("COALESCE(r, {underflow_real})"),
                &underflow_real,
                FloatWidth::Float4,
            ),
            (
                format!("COALESCE(d, {beyond_double})"),
                &beyond_double,
                FloatWidth::Float8,
            ),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {expr} AS out"));
            assert_eq!(
                validate(&d, &source_columns),
                Err(ValidationError::CoalesceLiteralOutOfRange {
                    field: "out".to_string(),
                    text: text.clone(),
                    target: ValueType::Float(target),
                }),
                "{expr}"
            );
        }
        // In range for the float, an exact result (the literal unifies
        // upward instead of being cast), or a column fallback.
        for expr in [
            format!("COALESCE(d, {beyond_real})"),
            format!("COALESCE(r, 34{})", "0".repeat(37)),
            "COALESCE(r, 0)".to_string(),
            "COALESCE(r, n)".to_string(),
            format!("COALESCE(i, {beyond_real})"),
            format!("COALESCE(n, {beyond_real})"),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {expr} AS out"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{expr}");
        }
        // Postgres folds a constant argument made of literals too, so the
        // cast fails for every row the same way: a sum, a COALESCE whose
        // first argument is the literal, and a calculated field holding it.
        for (fields, text) in [
            (
                format!("COALESCE(r, {beyond_real} + 1) AS out"),
                format!("({beyond_real} + 1)"),
            ),
            (
                format!("COALESCE(r, COALESCE({beyond_real}, i)) AS out"),
                format!("COALESCE({beyond_real}, i)"),
            ),
            (
                format!("{beyond_real} AS big, COALESCE(r, big) AS out"),
                "big".to_string(),
            ),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert_eq!(
                validate(&d, &source_columns),
                Err(ValidationError::CoalesceLiteralOutOfRange {
                    field: "out".to_string(),
                    text,
                    target: ValueType::Float(FloatWidth::Float4),
                }),
                "{fields}"
            );
        }
        // An argument that reads a column is not a constant: the cast fails
        // only for the rows whose value is out of range.
        for fields in [
            format!("COALESCE(r, i + {beyond_real}) AS out"),
            format!("COALESCE(r, COALESCE(i, {beyond_real})) AS out"),
            format!("{beyond_real} + i AS big, COALESCE(r, big) AS out"),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{fields}");
        }
    }

    #[test]
    fn a_constant_sum_postgres_cannot_compute_is_refused_at_define() {
        let source_columns = HashMap::from([
            ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("n".to_string(), ValueType::Numeric),
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        let int4 = ValueType::Integer(IntWidth::Int4);
        let int8 = ValueType::Integer(IntWidth::Int8);
        let double = ValueType::Float(FloatWidth::Float8);
        let real = ValueType::Float(FloatWidth::Float4);
        let beyond_double = "9".repeat(310);
        let tiny = format!("0.{}1", "0".repeat(400));
        let max8 = "9223372036854775807";
        for (fields, text, target) in [
            // Integer overflow, at either width.
            (
                "2147483647 + 1 AS out".to_string(),
                "(2147483647 + 1)".to_string(),
                int4,
            ),
            (format!("{max8} + 1 AS out"), format!("({max8} + 1)"), int8),
            (
                format!("{max8} + 2147483647 AS out"),
                format!("({max8} + 2147483647)"),
                int8,
            ),
            // Nested: the innermost sum that overflows is the one named.
            (
                "(2147483647 + 1) + 5000000000 AS out".to_string(),
                "(2147483647 + 1)".to_string(),
                int4,
            ),
            // Folded constants: a COALESCE whose first argument is one, and
            // a calculated field holding one.
            (
                "COALESCE(2147483647, 5) + 1 AS out".to_string(),
                "(COALESCE(2147483647, 5) + 1)".to_string(),
                int4,
            ),
            (
                "2147483647 AS big, big + 1 AS out".to_string(),
                "(big + 1)".to_string(),
                int4,
            ),
            // A constant sub-expression beside a column, in a COALESCE
            // argument.
            (
                "COALESCE(i, 2147483647 + 1) AS out".to_string(),
                "(2147483647 + 1)".to_string(),
                int4,
            ),
            (
                "i + (2147483647 + 1) AS out".to_string(),
                "(2147483647 + 1)".to_string(),
                int4,
            ),
            // Float constants past the float's range.
            (
                "REAL '3e+38' + REAL '3e+38' AS out".to_string(),
                "(CAST('3e+38' AS REAL) + CAST('3e+38' AS REAL))".to_string(),
                real,
            ),
            (
                "DOUBLE PRECISION '1e+308' + DOUBLE PRECISION '1e+308' AS out".to_string(),
                "(CAST('1e+308' AS DOUBLE PRECISION) + CAST('1e+308' AS DOUBLE PRECISION))"
                    .to_string(),
                double,
            ),
            // An exact constant beside a float is cast to double precision,
            // whether or not the other operand reads a column.
            (
                format!("r + {beyond_double} AS out"),
                beyond_double.clone(),
                double,
            ),
            (format!("d + {tiny} AS out"), tiny.clone(), double),
            (
                format!("{beyond_double} AS big, r + big AS out"),
                "big".to_string(),
                double,
            ),
            (
                format!("r + ({beyond_double} + 1) AS out"),
                format!("({beyond_double} + 1)"),
                double,
            ),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert_eq!(
                validate(&d, &source_columns),
                Err(ValidationError::ConstantOutOfRange {
                    field: "out".to_string(),
                    text,
                    target,
                }),
                "{fields}"
            );
        }
        // What Postgres computes: sums that fit, the wider type absorbing an
        // integer, numeric (which never overflows here), a float constant
        // sum inside its range, and any sum that reads a column.
        for fields in [
            "2147483646 + 1 AS out".to_string(),
            "2147483647 + 3000000000 AS out".to_string(),
            "(2147483647 + 5000000000) + 1 AS out".to_string(),
            "COALESCE(2147483647, 5000000000) + 1 AS out".to_string(),
            format!("{max8} + 0 AS out"),
            format!("{max8}0 + 1 AS out"),
            "2147483647 + 1.5 AS out".to_string(),
            "2147483647 + n AS out".to_string(),
            "i + 2147483647 AS out".to_string(),
            "i + 2147483647 + 2147483647 AS out".to_string(),
            "COALESCE(i, 2147483646 + 1) AS out".to_string(),
            "REAL '2e+38' + REAL '1e+38' AS out".to_string(),
            "REAL '3e+38' + 1 AS out".to_string(),
            "r + 3000000000 AS out".to_string(),
            format!("r + {} AS out", "9".repeat(308)),
            "d + 0.5 AS out".to_string(),
            "REAL '3e+38' + DOUBLE PRECISION '3e+38' AS out".to_string(),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{fields}");
        }
        // The same SQL runs for an aggregate's argument.
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, SUM(2147483647 + 1) AS out",
        );
        assert_eq!(
            validate(&d, &source_columns),
            Err(ValidationError::ConstantOutOfRange {
                field: "out".to_string(),
                text: "(2147483647 + 1)".to_string(),
                target: int4,
            })
        );
        let d = parsed(
            "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, SUM(i + 2147483647) AS out",
        );
        assert_eq!(validate(&d, &source_columns), Ok(()));
    }

    #[test]
    fn a_coalesce_argument_after_a_constant_one_is_not_range_checked() {
        // The planner simplifies a COALESCE's arguments in order and stops
        // at the first constant one, dropping the rest unevaluated, so
        // Postgres 17 returns 5 for `COALESCE(5, 2147483647 + 1)` and never
        // raises. A constant is anything that reads no column: a literal, a
        // sum of them, a calculated field holding one, a scalar function of
        // constants.
        let source_columns = HashMap::from([
            ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("d".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        let beyond_real = format!("1{}", "0".repeat(39));
        let beyond_double = "9".repeat(310);
        for fields in [
            "COALESCE(5, 2147483647 + 1) AS out".to_string(),
            "COALESCE(i, 5, 2147483647 + 1) AS out".to_string(),
            "COALESCE(i, 1 + 1, 2147483647 + 1) AS out".to_string(),
            "COALESCE(i, CHAR_LENGTH('ab'), 2147483647 + 1) AS out".to_string(),
            "5 AS five, COALESCE(i, five, 2147483647 + 1) AS out".to_string(),
            "COALESCE(1, i + (2147483647 + 1)) AS out".to_string(),
            "COALESCE(i, 5, COALESCE(i, 2147483647 + 1)) AS out".to_string(),
            format!("COALESCE(r, 1, {beyond_real}) AS out"),
            format!("COALESCE(REAL '1', {beyond_real}) AS out"),
            format!("COALESCE(i > 0, 1 > 0, d > {beyond_double}) AS out"),
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert_eq!(validate(&d, &source_columns), Ok(()), "{fields}");
        }
        let d = parsed(&format!(
            "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, COALESCE(SUM(i), 0, {beyond_real} + 1) AS out"
        ));
        assert_eq!(validate(&d, &source_columns), Ok(()));
        // Up to and including the first constant argument, the planner
        // folds each, so its range is checked; a field is its own target
        // column, so it is checked wherever it is read.
        for fields in [
            "COALESCE(i, 2147483647 + 1, 5) AS out",
            "COALESCE(i + (2147483647 + 1), 5) AS out",
            "2147483647 + 1 AS big, COALESCE(5, big) AS out",
        ] {
            let d = parsed(&format!("TRANSFORM t FROM s SELECT {fields}"));
            assert!(
                matches!(
                    validate(&d, &source_columns),
                    Err(ValidationError::ConstantOutOfRange { .. })
                ),
                "{fields}"
            );
        }
        let d = parsed(&format!(
            "TRANSFORM t FROM s SELECT COALESCE(r, i, {beyond_real}, 1) AS out"
        ));
        assert!(matches!(
            validate(&d, &source_columns),
            Err(ValidationError::CoalesceLiteralOutOfRange { .. })
        ));
    }

    #[test]
    fn a_doubling_chain_of_constant_fields_validates_quickly() {
        // Each field reads the one before twice. Folding a constant must not
        // walk every path through the chain (2^40 here): define refuses the
        // chain for its inlined size, after typing it.
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

    #[test]
    fn regexp_count_with_an_uncompilable_pattern_literal_is_rejected() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::FunctionCall {
                name: "REGEXP_COUNT".to_string(),
                args: vec![col("text_col"), Expr::StringLiteral("(".to_string())],
            },
        }]);
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        let err = validate(&d, &source_columns).unwrap_err();
        match err {
            ValidationError::InvalidRegexPattern { field, pattern, .. } => {
                assert_eq!(field, "out");
                assert_eq!(pattern, "(");
            }
            other => panic!("expected InvalidRegexPattern, got {other:?}"),
        }
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
            column,
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
                read("via_qualified", "STRPOS", source("author")),
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
        validate_join(&parse_text(text).unwrap(), &left, &right)
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
             items.qty = orders.id GROUP BY orders.region SELECT COUNT(*) AS n",
        )
        .unwrap();
    }

    #[test]
    fn join_refuses_a_constant_sum_postgres_cannot_compute() {
        // A JOIN's fields are typed by the same rule as a 1-1's, so a
        // constant overflow inside an aggregate argument is refused too, and
        // a sum that reads a column is not.
        for select in [
            "orders.region AS region, SUM(items.qty + (2147483647 + 1)) AS s",
            "orders.region AS region, SUM(COALESCE(items.qty, 2147483647 + 1)) AS s",
        ] {
            assert!(
                matches!(
                    check(select),
                    Err(ValidationError::ConstantOutOfRange { .. })
                ),
                "{:?}: {select}",
                check(select)
            );
        }
        check("orders.region AS region, SUM(items.qty + 2147483647) AS s").unwrap();
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

    /// ADR-0020, rule 7: a `numeric` column of a table Trellis creates has no
    /// declared scale whatever the column it copies had, so a chained `SUM` or
    /// `AVG` over it gets its own named refusal; the same column of a source
    /// keeps the plain one.
    #[test]
    fn a_chained_sum_over_a_trellis_targets_numeric_is_a_named_refusal() {
        let arg = |trellis_target| AggregateArg {
            column: "p",
            value_type: ValueType::Numeric,
            pg_type: "numeric",
            typmod: -1,
            trellis_target,
        };
        for function in ["SUM", "AVG"] {
            assert!(matches!(
                check_aggregate_admitted(function, Some(&arg(true))),
                Err(ValidationError::ChainedNumericAggregate { .. })
            ));
            assert!(matches!(
                check_aggregate_admitted(function, Some(&arg(false))),
                Err(ValidationError::UnconstrainedNumericAggregate { .. })
            ));
        }
        let message = check_aggregate_admitted("SUM", Some(&arg(true)))
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("docs/transforms.md") && message.contains("no declared scale"),
            "{message}"
        );
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
            ("MIN(qty)", "MIN/MAX"),
            ("MAX(note)", "MIN/MAX"),
            ("BOOL_AND(qty > 1)", "bool_and/bool_or/bit_and/bit_or"),
            ("BIT_OR(qty)", "bool_and/bool_or/bit_and/bit_or"),
            ("SUM(weight)", "float and interval SUM/AVG"),
            ("AVG(wait)", "float and interval SUM/AVG"),
            ("ARRAY_AGG(note)", "array_agg/string_agg/jsonb_agg"),
            ("STRING_AGG(note, ',')", "array_agg/string_agg/jsonb_agg"),
            // Inside a composed field too.
            ("COUNT(*) + MAX(qty)", "MIN/MAX"),
        ] {
            match check(&format!("{call} AS v")) {
                Err(ValidationError::DeferredAggregate {
                    family: got_family, ..
                }) => assert_eq!(got_family, family, "{call}"),
                other => panic!("{call}: expected DeferredAggregate, got {other:?}"),
            }
        }
        for call in [
            "SUM(price)",
            "AVG(price)",
            "SUM(cost + 1)",
            "AVG(cost + qty)",
        ] {
            let err = check(&format!("{call} AS v")).unwrap_err();
            assert!(
                matches!(err, ValidationError::UnconstrainedNumericAggregate { .. }),
                "{call}: {err:?}"
            );
            assert!(err.to_string().contains("numeric(p,s)"), "{call}: {err}");
        }
        for fields in [
            "COUNT(*) AS n, COUNT(note) AS notes, COUNT(price) AS priced",
            "SUM(qty) AS s, AVG(qty) AS a, SUM(cost) AS c, AVG(cost) AS ac",
            "SUM(qty + order_id) AS s, COALESCE(SUM(cost), 0) + COUNT(*) AS c",
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
    fn join_refuses_sum_and_avg_over_unconstrained_numeric_on_either_side() {
        for call in ["SUM(items.price)", "AVG(items.price)"] {
            assert!(
                matches!(
                    check(&format!("orders.region AS region, {call} AS v")),
                    Err(ValidationError::UnconstrainedNumericAggregate { .. })
                ),
                "{call}"
            );
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
        assert!(matches!(
            validate_join(&def, &left, &right),
            Err(ValidationError::UnconstrainedNumericAggregate { .. })
        ));
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
        validate_join(&def, &left, &right).unwrap();
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
            validate_join(&def, &left, &right)
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
            validate_join(&def, &left, &right),
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
        assert!(matches!(
            validate_join(&def, &left, &right),
            Err(ValidationError::GroupByKeysShareTargetColumn { .. })
        ));
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
            spec.right = "orders".to_string();
        }
        assert!(matches!(
            validate_join(&def, &left, &right),
            Err(ValidationError::JoinSelf { .. })
        ));
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
        // An expression over numeric has no declared scale.
        assert!(matches!(
            refused("SUM(orders.total + 1) AS v"),
            Err(ValidationError::UnconstrainedNumericAggregate { .. })
        ));
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
