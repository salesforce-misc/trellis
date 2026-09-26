//! Validator for the 1-1 transform-definition subset (issue #23).
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
//! it, is column resolution, cycle detection, and (issue #63) type-checking.

use std::collections::{HashMap, HashSet};
use std::fmt;

use regex::Regex;

use super::ast::{Expr, FieldDef, GroupByKey, KeySpace, Predicate, TransformDef, ValueType};
use super::model::RelationshipCardinality;
use super::pg_type::PgType;
use crate::error_code::ErrorCode;
use crate::integer::IntWidth;

/// A relationship referenced by a definition, resolved by the caller
/// ([`super::catalog`]) against the persisted relationship catalog and live
/// `pg_catalog` so this (sync, DB-less) validator can enforce ADR-0006's
/// reference-time rules. Mirrors how `source_columns` is resolved by the
/// caller and passed in: the validator itself never touches the database.
///
/// `column_types` maps each to-side column this relationship's paths
/// reference to its [`ValueType`], so a `<rel>.<column>` enrichment field's
/// type can be inferred from the *to-side* column it reads (the from-side
/// `source_columns` map has no entry for it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRelationship {
    pub cardinality: RelationshipCardinality,
    pub to_table: String,
    pub to_col: String,
    pub column_types: HashMap<String, ValueType>,
}

/// Why a [`TransformDef`] was rejected by the validator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// Two calculated fields on the same target share a name.
    DuplicateFieldName { name: String },
    /// A field's name starts with [`RESERVED_COLUMN_PREFIX`], which is kept
    /// for the hidden columns Trellis adds to a target
    /// ([`super::ddl::RECOMPUTE_LSN_COLUMN`], an `AVG`'s `__{field}_sum`, a
    /// `SUM`'s `__{field}_count`). Without the reservation such a name could
    /// equal one of them, and install failed with a raw Postgres
    /// duplicate-column error (issue #566).
    ReservedFieldName { field: String },
    /// A `GROUP BY` key's target column name starts with
    /// [`RESERVED_COLUMN_PREFIX`] (see [`ValidationError::ReservedFieldName`]).
    /// `key` is the key as written, `column` the target column it would
    /// become.
    ReservedGroupByColumnName { key: String, column: String },
    /// A 1-1 field is named after one of the source's primary key columns
    /// (issue #566), whether it passes that column through (`id AS id`) or
    /// computes something else under its name. The target already carries
    /// every key column as its own key, so the field would declare the column
    /// a second time. Raised by [`reject_primary_key_named_fields`], which the
    /// catalog runs once it knows the source's key.
    FieldNamedAfterPrimaryKey { field: String },
    /// A field's expression references a name that is neither a known
    /// source column nor another calculated field on the same target.
    UnresolvedColumn { field: String, column: String },
    /// The calculated-column dependency graph contains a cycle.
    Cycle { cycle: Vec<String> },
    /// Adding a table-to-table dependency edge from a definition's source to
    /// its target would create a cycle in the two-level dependency graph
    /// (issue #22 follow-up: cycle detection generalized from column-only,
    /// within one [`TransformDef`], to the whole graph of tables connected
    /// by [`super::model::SchemaEdge`]s — currently just `Source` edges, but
    /// checked generically over whatever edge kinds exist so `Join`/
    /// `Relationship` edges need no rework here once persisted). Distinct
    /// from [`ValidationError::Cycle`], which names calculated *columns*
    /// within a single definition, since this names *tables* across
    /// definitions and is only ever detected once the persisted graph is
    /// consulted — see [`super::catalog`].
    TableCycle { cycle: Vec<String> },
    /// The target table is the same as the source table. Calculated columns
    /// must live on a separate neighbor table — writing them back onto the
    /// source would feed our own WAL into ingestion (see `docs/data-flow.md`).
    TargetEqualsSource { table: String },
    /// An operator was applied to an operand of the wrong [`ValueType`] —
    /// e.g. `+` given a `Text` operand (issue #63: `+` stays Numeric-only,
    /// no implicit string concatenation).
    TypeMismatch {
        field: String,
        expected: ValueType,
        found: ValueType,
    },
    /// A function call's argument had the wrong [`ValueType`] for its
    /// position (issue #64) — e.g. `octet_length(1)`. Distinct from
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
    /// `regexp_count`'s pattern argument was not a string literal (issue
    /// #64/#65 follow-up). The pattern must be known at validate time so it
    /// can be compiled and checked here, once, rather than re-parsed on
    /// every row at eval time — a column-sourced pattern is out of scope.
    NonLiteralRegexPattern { field: String },
    /// `regexp_count`'s pattern literal does not compile as a regular
    /// expression, per the `regex` crate.
    InvalidRegexPattern {
        field: String,
        pattern: String,
        error: String,
    },
    /// A typed literal's text (issue #109) isn't in its family's **canonical**
    /// Postgres output spelling — `DATE '2024-1-5'`, `DATE 'today'`,
    /// `BYTEA '\xAB'`. See [`super::typed_literal`]'s module doc comment for
    /// why the bar is canonical form rather than "Postgres would parse it":
    /// in short, immutability (`date_in` is `STABLE` precisely because of
    /// `'today'`) and text round-trip identity between the Rust evaluator
    /// and the SQL oracle.
    InvalidTypedLiteral {
        field: String,
        value_type: ValueType,
        text: String,
        detail: String,
    },
    /// A hand-built [`super::ast::Expr::TypedLiteral`] names a [`ValueType`]
    /// outside [`super::typed_literal::TYPED_LITERALS`]'s allowlist. The
    /// parser can't produce one — it only builds the node from that same
    /// table — so this is the validator's defense-in-depth against an AST
    /// assembled directly, in the same spirit as
    /// [`super::eval::EvalError::Cycle`].
    UnsupportedLiteralType {
        field: String,
        value_type: ValueType,
    },
    /// An [`super::ast::KeySpace::Aggregate`] definition's `GROUP BY` names a
    /// column that isn't a real source column.
    UnresolvedGroupByColumn { column: String },
    /// An [`super::ast::KeySpace::Aggregate`] definition's `GROUP BY` names a
    /// column typed [`ValueType::Other`] (issue #108 review) — a Postgres
    /// family the OID registry recognizes but that `docs/type-support.md`
    /// still lists as passthrough-only, with no key role.
    ///
    /// This is the `GROUP BY` twin of the check the two *other* key roles
    /// already make on the raw pg type name
    /// (`super::catalog::is_text_stable_join_key_type`, gating relationship
    /// join keys in #28 and 1-1 primary keys in #107). A `GROUP BY` key is
    /// matched by its `::text` rendering exactly like those are, so the same
    /// hazards apply and then some: `interval`'s native `=` holds
    /// `'1 day' = '24 hours'` while their renderings differ (two groups
    /// where Postgres's own `GROUP BY` has one), `money` renders under
    /// `lc_monetary`, and `json` has no `=` at all, so its key column can't
    /// even take the unique index the aggregate target needs. `bytea`
    /// (issue #114) used to be another name on this list; it turned out not
    /// to belong there — `byteaout` under the pinned `bytea_output = 'hex'`
    /// is a bijection with no session-GUC dependence at all, so it is
    /// admitted below alongside `oid`, and `timestamptz` (issue #246) is a
    /// second former name here now admitted the same way, once `TimeZone`
    /// was pinned identically on every connection Trellis opens rather than
    /// varying by session.
    ///
    /// Nothing could reach this before #108 — an `Other`-typed column was
    /// dropped from the validator's view entirely — so this gate only
    /// narrows the surface that issue newly opened. The typed key index
    /// (`docs/type-support.md`'s "Cross-cutting concerns") is what lifts it
    /// per family.
    UnsupportedGroupByKeyType {
        column: String,
        value_type: ValueType,
    },
    /// An [`super::ast::KeySpace::Aggregate`] definition's `GROUP BY` names a
    /// `<rel>.<column>` path whose `<rel>` is not a relationship declared on
    /// this definition's source table (ADR-0006: relationship names are
    /// scoped per from-table) — the `GROUP BY` twin of
    /// [`ValidationError::UnknownRelationship`], which blames a specific
    /// field; a `GROUP BY` key has no field to blame, so this variant names
    /// the relationship alone (issue #137).
    UnknownGroupByRelationship { rel: String },
    /// An [`super::ast::KeySpace::Aggregate`] definition's `GROUP BY` names a
    /// **to-many** `<rel>.<column>` relationship path (issue #137).
    /// Grouping by "the many child rows on the other end of a to-many
    /// relationship" has no defined semantics — a `GROUP BY` key must
    /// resolve to exactly one value per source row, which only a to-one
    /// relationship path guarantees.
    GroupByRelationshipMustBeToOne { rel: String, column: String },
    /// Two of an [`super::ast::KeySpace::Aggregate`] definition's `GROUP BY`
    /// keys map to the same target column name (issue #519) — e.g.
    /// `GROUP BY buyer.name, seller.name`, a plain `name` alongside
    /// `buyer.name`, or one key listed twice. A key's target column is named
    /// after its bare column ([`GroupByKey::target_column_name`]), and the
    /// grammar has no `GROUP BY ... AS` alias yet, so the target DDL would
    /// declare that column twice. `first`/`second` are the clashing keys as
    /// written (`rel.column` or `column`), in `GROUP BY` order.
    GroupByKeysShareTargetColumn {
        first: String,
        second: String,
        column: String,
    },
    /// An [`super::ast::KeySpace::Aggregate`] definition's field references a
    /// source column that is neither a grouping key nor wrapped in exactly
    /// one of `SUM`/`MIN`/`MAX`/`AVG` — every row in a group must be folded
    /// down to one value before it can appear in the target, and a bare
    /// reference to a non-grouping-key source column doesn't do that.
    UngroupedColumnReference { field: String, column: String },
    /// An [`super::ast::KeySpace::Aggregate`] definition has a calculated
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
    /// A [`KeySpace::OneToOne`] definition has a calculated field whose alias
    /// matches the name of a real source column, but whose expression isn't a
    /// bare passthrough of that same column. Without this guard, a *different*
    /// field referencing that name would resolve it via `fields_by_name`
    /// (issue #80's inter-field composition) and get the calculated value
    /// instead of the real column, with the alias unconditionally — and
    /// silently — shadowing the column. Mirrors
    /// [`ValidationError::GroupingColumnFieldMustBePassthrough`]'s guard for
    /// the `Aggregate` arm.
    CalculatedFieldShadowsSourceColumn { field: String },
    /// A field's expression references a `<rel>.<column>` path whose `<rel>`
    /// is not a relationship declared on this definition's source table
    /// (ADR-0006: relationship names are scoped per from-table). Resolved by
    /// the caller against the relationship catalog and reported here rather
    /// than reaching [`super::eval`] as an `UnknownRelationship` at apply
    /// time.
    UnknownRelationship { field: String, rel: String },
    /// A field wraps a *to-one* relationship path in an aggregate
    /// (`SUM(<rel>.<column>)`), which ADR-0006 forbids: a to-one path already
    /// resolves to a single related value, so aggregating it is meaningless.
    /// Use the bare `<rel>.<column>` enrichment instead.
    RelationshipToOneWrappedInAggregate {
        field: String,
        rel: String,
        column: String,
    },
    /// A field references a *to-many* relationship path bare
    /// (`<rel>.<column>` with no aggregate), which ADR-0006 forbids: a
    /// to-many path denotes a *set* of related values, so a bare reference is
    /// ambiguous. Wrap it in an aggregate (`SUM`/`COUNT`/`MIN`/`MAX`/`AVG`)
    /// that folds the set to a single value.
    RelationshipToManyRequiresAggregate {
        field: String,
        rel: String,
        column: String,
    },
    /// An [`super::ast::KeySpace::Aggregate`] (GROUP BY) definition's field
    /// references a **to-many** `<rel>.<column>` relationship path inside an
    /// aggregate call. That is a nested aggregation — folding an aggregate
    /// over *related* rows into an aggregate over *source* rows — and has no
    /// settled semantics (which fold runs first? does a source row with no
    /// related rows contribute `NULL` or the inner aggregate's empty result?),
    /// so it stays rejected until one is chosen. A **to-one** path inside an
    /// aggregate is *not* this case: it resolves to exactly one related value
    /// per source row, so `SUM(post.word_count)` is simply a `LEFT JOIN`
    /// followed by `GROUP BY`, and is accepted (issue #94).
    RelationshipPathInAggregate {
        field: String,
        rel: String,
        column: String,
    },
    /// An [`super::ast::KeySpace::Aggregate`] (GROUP BY) definition's field
    /// references a to-one `<rel>.<column>` relationship path **bare**, not
    /// wrapped in an aggregate call. A to-one path resolves per *source* row,
    /// exactly like a source column does, so within a GROUP BY it names one
    /// value per row of the group rather than one value for the group —
    /// there's nothing to write into the group's single target row. Wrap it in
    /// `SUM`/`MIN`/`MAX`/`AVG`/`COUNT` to fold it down. The relationship-path
    /// twin of [`ValidationError::UngroupedColumnReference`], kept separate so
    /// the message can name `<rel>.<column>` rather than pretend the path is a
    /// source column name (issue #94).
    UngroupedRelationshipReference {
        field: String,
        rel: String,
        column: String,
    },
    /// A relationship endpoint (issue #27, ADR-0006) names a `table.column`
    /// pair that doesn't exist, as introspected live against `pg_catalog` —
    /// ADR-0005 forbids Trellis from assuming a column exists rather than
    /// checking, even though it never issues DDL against the table itself.
    UnknownRelationshipColumn { table: String, column: String },
    /// A relationship's `from_col`/`to_col` (issue #27, ADR-0006's "type-check
    /// the join") resolved to Postgres types that aren't comparable — e.g.
    /// joining a `text` column to a `uuid` column.
    RelationshipTypeMismatch(Box<RelationshipTypeMismatch>),
    /// A relationship name was already declared on the same `from_table`
    /// (issue #27, surfacing ADR-0006's "Naming and scope": unique
    /// per-from-table, not global) with an actionable message, ahead of the
    /// `relationship_definitions` unique constraint that backstops this
    /// check against a same-name race between concurrent callers.
    /// `from_table` is the qualified `"schema.table"` it collided on — the
    /// name is unique per qualified from-table (issue #288), so a same-named
    /// table in another schema is no collision.
    DuplicateRelationshipName { from_table: String, name: String },
    /// A relationship's join key resolved to a Postgres type that isn't
    /// text-stable — one where `a::text = b::text` disagrees with the type's
    /// native typed `=` (see
    /// [`super::catalog::TEXT_STABLE_JOIN_KEY_TYPES`]). Trellis's evaluator,
    /// staging reverse-lookup, and oracle all join by raw `::text` equality,
    /// but the oracle SELECT joins by native `=`, so a non-text-stable key
    /// (`numeric`/`real`/`double precision` — `1.0` vs `1.00`; `character(n)`
    /// — blank-padding; `citext` — case; `interval` — `'1 day'` vs
    /// `'24 hours'`) would render a real LEFT JOIN match as a false-miss NULL
    /// in the engine. Rejected at definition time rather than silently
    /// diverging from the Postgres oracle.
    RelationshipUnsupportedJoinKeyType {
        name: String,
        table: String,
        column: String,
        pg_type: String,
    },
    /// A *to-many* relationship's to-side (issue #41) lacks a replica identity
    /// that carries the join column in row pre-images. For to-many, the join
    /// key (`to_col`) is a *non-PK* column on the to-side, and the staging
    /// reverse-recompute resolver reads it from the DELETE/UPDATE pre-image to
    /// find which from-side rows to re-derive. Under the default replica
    /// identity (primary key), that non-PK column is absent from the
    /// pre-image, so a delete or a re-parent (UPDATE of the join column) would
    /// silently under-recompute and diverge from the Postgres oracle with no
    /// error. Accepted only when the to-side has `REPLICA IDENTITY FULL` or a
    /// replica-identity index covering `to_col`; rejected at definition time
    /// (ADR-0006, a correctness prerequisite → hard reject per ADR-0005).
    RelationshipToManyRequiresReplicaIdentity {
        name: String,
        to_table: String,
        to_col: String,
    },
    /// An explicitly-qualified `FROM <schema>.<table>` source reference
    /// (issue #76, ADR-0007 grammar clause 4) named a schema that does not
    /// actually contain a table by that name, as introspected live against
    /// `information_schema.tables`
    /// ([`super::catalog::confirm_qualified_table_exists_in_txn`]). Distinct
    /// from [`super::catalog::CatalogError::SourceTableNotFound`]: that
    /// variant means "no schema on `search_path` has this bare name",
    /// whereas this one means "the exact schema the definition named is
    /// real, or isn't, but either way doesn't have this table" — an explicit
    /// spelling resolves to that one relation, never a `search_path` walk.
    QualifiedSourceTableNotFound { schema: String, table: String },
    /// The `TRANSFORM <target>` twin of
    /// [`ValidationError::QualifiedSourceTableNotFound`]: an explicitly-qualified
    /// `TRANSFORM <schema>.<target>` reference (issue #76) named a schema
    /// that doesn't actually contain a table by that name. For the common
    /// [`super::catalog::install_definition`] path this schema is checked
    /// *after* the physical `CREATE TABLE` DDL already ran under the same
    /// explicit schema, so a miss here would mean that DDL itself failed
    /// silently rather than a genuine spelling error; for the ring-path entry
    /// points ([`super::catalog::create_definition`]/
    /// [`super::catalog::create_definition_without_backfill`]), which assume
    /// their caller already created the physical target table, this is the
    /// only check that a bogus explicit schema ever gets.
    QualifiedTargetTableNotFound { schema: String, table: String },
    /// A `KeySpace::OneToOne` field's `MIN`/`MAX` wraps a **to-many**
    /// relationship path over an enum-typed column (issue #117,
    /// `super::eval`'s to-many relationship enrichment, issue #29). Refused,
    /// unlike every other admitted `MIN`/`MAX(enum)` shape: this specific
    /// combination folds through the pure, synchronous, DB-less evaluator
    /// (`eval::eval_to_many_aggregate` -> `eval::reduce_numeric_aggregate`)
    /// with no live-Postgres fallback the way a `KeySpace::Aggregate`
    /// field's own `MIN`/`MAX` always has (it is
    /// `Invertibility::RecomputeOnly`, so `staging::apply_aggregate` always
    /// resolves its written value with a real server-side `min()`/`max()`
    /// push-down, never by folding a multi-row group through this
    /// evaluator — see `defs::invertibility`'s own doc comment). An enum's
    /// creation-order comparison is a live, per-type, schema-defined fact
    /// (`pg_enum.enumsortorder`), not a universal property of the family
    /// the way `date`/`inet` ordering is, so the evaluator has no way to
    /// answer it correctly without a connection it structurally never has
    /// here — refusing at validation time is what keeps a naive
    /// lexicographic fallback from ever silently computing the wrong
    /// answer (see `eval::reduce_enum_aggregate`'s own doc comment for the
    /// runtime-side twin of this guard). Every other `MIN`/`MAX(enum)`
    /// shape — a `KeySpace::Aggregate` field, or a to-*one* relationship
    /// path (which resolves to one value per row, never a multi-row fold)
    /// — is unaffected.
    EnumAggregateOverToManyRelationshipUnsupported {
        field: String,
        function: String,
        rel: String,
    },
    /// A `KeySpace::OneToOne` field's `MIN`/`MAX` wraps a **to-many**
    /// relationship path over a `text`/`varchar`-typed column (issue #120) —
    /// `EnumAggregateOverToManyRelationshipUnsupported`'s exact structural
    /// twin, for the same reason: this one shape folds a real multi-row
    /// group through the pure, synchronous, DB-less evaluator
    /// (`eval::eval_to_many_aggregate` -> `eval::reduce_numeric_aggregate` ->
    /// `eval::reduce_text_aggregate`) with no live-Postgres fallback, unlike
    /// a `KeySpace::Aggregate` field's own `MIN`/`MAX(text)` (always
    /// `Invertibility::RecomputeOnly`, always resolved by a real server-side
    /// `min()`/`max()` push-down instead). Postgres's `text` ordering is a
    /// per-*column* **collation**, not a fixed, universal property of the
    /// `text` family the way `date`/`inet` ordering is — and this crate
    /// tracks no column collation metadata at all — so the evaluator has no
    /// way to answer "which of these two distinct strings sorts first"
    /// correctly without a live lookup it structurally never has here.
    /// Refusing at validation time is what keeps an assumed byte-order
    /// (`C`/`POSIX`) fallback from silently computing the wrong answer under
    /// any other collation — see `eval::reduce_text_aggregate`'s own doc
    /// comment for the runtime-side twin of this guard. (`uuid` — this
    /// issue's other candidate `MIN`/`MAX` family — turned out, checked live
    /// rather than assumed, not to need an equivalent guard: Postgres has no
    /// `min(uuid)`/`max(uuid)` aggregate *at all*, the same "opclass but no
    /// aggregate" finding #114/#116 made for `bytea`/`macaddr`, so `uuid`
    /// never reaches this arm in the first place — `registry::aggregate_result_type`
    /// refuses it outright as an ordinary `FunctionArgTypeMismatch`,
    /// independent of relationship shape.) Every other `MIN`/`MAX(text)`
    /// shape — a `KeySpace::Aggregate` field, or a to-*one* relationship
    /// path — is unaffected.
    TextAggregateOverToManyRelationshipUnsupported {
        field: String,
        function: String,
        rel: String,
    },
}

/// Payload of [`ValidationError::RelationshipTypeMismatch`], boxed out of the
/// enum so this variant's seven `String` fields don't inflate every error
/// type that wraps [`ValidationError`] (clippy::result_large_err).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipTypeMismatch {
    pub name: String,
    pub from_table: String,
    pub from_col: String,
    pub from_type: String,
    pub to_table: String,
    pub to_col: String,
    pub to_type: String,
}

impl ValidationError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Almost every variant here is a rejected definition —
    /// [`ErrorCode::Validation`] — with one exception:
    /// [`ValidationError::DuplicateRelationshipName`] is a naming collision
    /// with an already-declared relationship, so it reports
    /// [`ErrorCode::Conflict`] instead, the same category a uniqueness
    /// violation from Postgres itself would report.
    pub fn code(&self) -> ErrorCode {
        match self {
            ValidationError::DuplicateRelationshipName { .. } => ErrorCode::Conflict,
            _ => ErrorCode::Validation,
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
            ValidationError::TableCycle { cycle } => {
                write!(f, "cycle among tables: {}", cycle.join(" -> "))
            }
            ValidationError::TargetEqualsSource { table } => write!(
                f,
                "target table '{table}' is the same as the source table; calculated \
                 columns must live on a separate neighbor table"
            ),
            ValidationError::TypeMismatch {
                field,
                expected,
                found,
            } => write!(
                f,
                "calculated field '{field}' expects a {expected} value here, found {found}"
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
            ValidationError::UnknownGroupByRelationship { rel } => write!(
                f,
                "GROUP BY references relationship '{rel}', which is not declared on this \
                 definition's source table (relationship names are scoped per from-table, \
                 ADR-0006)"
            ),
            ValidationError::GroupByRelationshipMustBeToOne { rel, column } => write!(
                f,
                "GROUP BY references to-many relationship path '{rel}.{column}'; grouping by \
                 the many related rows on the other end of a to-many relationship has no \
                 defined semantics, so a GROUP BY relationship path must be to-one"
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
                 GROUP BY keys can't be aliased yet, so group by only one of them (or by the \
                 relationship's from-side column instead), or group over a 1-1 transform \
                 that selects them under distinct names (e.g. `buyer.name AS buyer_name`)"
            ),
            ValidationError::UngroupedColumnReference { field, column } => write!(
                f,
                "calculated field '{field}' references source column '{column}' outside of \
                 SUM/MIN/MAX/AVG; a non-grouping-key column must be aggregated, not referenced \
                 bare"
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
            ValidationError::UnknownRelationship { field, rel } => write!(
                f,
                "calculated field '{field}' references relationship '{rel}', which is not \
                 declared on this definition's source table (relationship names are scoped \
                 per from-table, ADR-0006)"
            ),
            ValidationError::RelationshipToOneWrappedInAggregate { field, rel, column } => write!(
                f,
                "calculated field '{field}' wraps to-one relationship path '{rel}.{column}' in \
                 an aggregate; a to-one relationship already resolves to a single related value, \
                 so drop the aggregate and reference '{rel}.{column}' directly (ADR-0006)"
            ),
            ValidationError::RelationshipToManyRequiresAggregate { field, rel, column } => write!(
                f,
                "calculated field '{field}' references to-many relationship path '{rel}.{column}' \
                 bare; a to-many relationship denotes a set of related values, so wrap it in an \
                 aggregate such as SUM({rel}.{column}) or COUNT({rel}.{column}) (ADR-0006)"
            ),
            ValidationError::RelationshipPathInAggregate { field, rel, column } => write!(
                f,
                "calculated field '{field}' aggregates to-many relationship path \
                 '{rel}.{column}' inside a GROUP BY (aggregate) definition; aggregating an \
                 aggregate is not supported — a to-one relationship path may be aggregated \
                 here, but a to-many one may not"
            ),
            ValidationError::UngroupedRelationshipReference { field, rel, column } => write!(
                f,
                "calculated field '{field}' references relationship path '{rel}.{column}' \
                 outside of SUM/COUNT/MIN/MAX/AVG; in a GROUP BY definition a to-one \
                 relationship path resolves once per source row, so it must be aggregated \
                 (e.g. SUM({rel}.{column})), not referenced bare"
            ),
            ValidationError::UnknownRelationshipColumn { table, column } => write!(
                f,
                "relationship references '{table}.{column}', which does not exist"
            ),
            ValidationError::RelationshipTypeMismatch(mismatch) => {
                let RelationshipTypeMismatch {
                    name,
                    from_table,
                    from_col,
                    from_type,
                    to_table,
                    to_col,
                    to_type,
                } = &**mismatch;
                write!(
                    f,
                    "relationship '{name}' joins {from_table}.{from_col} ({from_type}) to \
                     {to_table}.{to_col} ({to_type}), which are not comparable types"
                )
            }
            ValidationError::DuplicateRelationshipName { from_table, name } => write!(
                f,
                "relationship '{name}' is already declared on '{from_table}'; relationship \
                 names must be unique per from-table (ADR-0006), so pick a different name"
            ),
            ValidationError::RelationshipUnsupportedJoinKeyType {
                name,
                table,
                column,
                pg_type,
            } => write!(
                f,
                "relationship '{name}' joins on {table}.{column} ({pg_type}), a type whose \
                 equality isn't text-stable, so the engine (which compares join keys as text) \
                 would silently diverge from the Postgres oracle's typed join; supported join \
                 key types are {}",
                super::catalog::supported_join_key_types()
            ),
            ValidationError::RelationshipToManyRequiresReplicaIdentity {
                name,
                to_table,
                to_col,
            } => write!(
                f,
                "relationship '{name}' is to-many (its join key {to_table}.{to_col} is not \
                 unique), so the to-side needs a replica identity that carries {to_col} in \
                 delete/re-parent pre-images — otherwise reverse recompute can't find the \
                 from-side rows to re-derive and silently diverges from the Postgres oracle; \
                 run `ALTER TABLE {to_table} REPLICA IDENTITY FULL;` (or use a replica-identity \
                 index that covers {to_col})"
            ),
            ValidationError::QualifiedSourceTableNotFound { schema, table } => write!(
                f,
                "FROM names '{schema}.{table}' explicitly, but schema '{schema}' has no table \
                 named '{table}'; an explicit schema.table spelling resolves to that exact \
                 relation, not a search_path walk (ADR-0007), so this is checked as written"
            ),
            ValidationError::QualifiedTargetTableNotFound { schema, table } => write!(
                f,
                "TRANSFORM names '{schema}.{table}' explicitly, but schema '{schema}' has no \
                 table named '{table}'; an explicit schema.table spelling resolves to that exact \
                 relation, not the configured target schema (ADR-0007), so this is checked as \
                 written"
            ),
            ValidationError::EnumAggregateOverToManyRelationshipUnsupported {
                field,
                function,
                rel,
            } => write!(
                f,
                "calculated field '{field}': {function}('{rel}.<column>') over a to-many \
                 relationship is not supported for an enum-typed column — its creation-order \
                 comparison cannot be resolved without a live connection this fold has no way \
                 to reach (issue #117); a GROUP BY definition's own {function}(<enum column>) \
                 is unaffected"
            ),
            ValidationError::TextAggregateOverToManyRelationshipUnsupported {
                field,
                function,
                rel,
            } => write!(
                f,
                "calculated field '{field}': {function}('{rel}.<column>') over a to-many \
                 relationship is not supported for a text-typed column — its ordering depends on \
                 a column collation this crate does not track, which cannot be resolved without \
                 a live connection this fold has no way to reach (issue #120); a GROUP BY \
                 definition's own {function}(<text column>) is unaffected"
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

/// A non-fatal caveat surfaced alongside an otherwise-successful definition —
/// distinct from [`ValidationError`], which rejects the definition outright.
/// Per ADR-0005's "correctness vs. performance" split: a missing correctness
/// prerequisite is a hard rejection, but a missing *performance* prerequisite
/// (this type's only variant so far) still leaves the definition correct, so
/// it's surfaced as guidance instead of an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipWarning {
    /// The from-side join column (issue #31) has no usable btree index —
    /// reverse propagation (ADR-0006: "finding rows to re-derive when a
    /// related row changes") is still correct, since it's a plain
    /// `from_col = $1` lookup, but a full scan of `from_table` on every
    /// update to `to_table` is slow. Per ADR-0005, Trellis never creates the
    /// index itself; it only names the exact DDL the user may run.
    MissingFkIndex {
        from_table: String,
        from_col: String,
    },
}

impl fmt::Display for RelationshipWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelationshipWarning::MissingFkIndex {
                from_table,
                from_col,
            } => write!(
                f,
                "no index on '{from_table}.{from_col}'; finding rows to re-derive when a \
                 related row changes will require a full scan of '{from_table}' — consider \
                 `CREATE INDEX ON {from_table} ({from_col});`"
            ),
        }
    }
}

impl std::error::Error for RelationshipWarning {}

/// Validates `def` against the 1-1 subset. `source_columns` maps each
/// column name known to exist on `def.source` to its [`ValueType`];
/// resolving it against a real Postgres schema is intake's job (out of
/// scope here — see issue #23's report), so callers supply it explicitly.
pub fn validate(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    relationships: &HashMap<String, ResolvedRelationship>,
) -> Result<(), ValidationError> {
    if def.target == def.source {
        return Err(ValidationError::TargetEqualsSource {
            table: def.target.clone(),
        });
    }

    // Issue #566: checked ahead of every per-key-space rule, so a reserved
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

    // Exhaustive matches: these are the "re-verify at this layer" guard for
    // #22's already-enforced 1-1/TRUE-only constructs (see module docs).
    // `Aggregate` has real runtime checks (below), since — unlike the other
    // arm here — the AST can encode an invalid one.
    match &def.key_space {
        KeySpace::OneToOne => {
            for field in &def.fields {
                // A calculated field aliased to a real source column's name
                // must be a bare passthrough of it (issue #80) — the same
                // shape the `Aggregate` arm below enforces for grouping
                // columns. `is_self_passthrough`'s exemption in `infer_expr`/
                // `eval_expr` only covers a field referencing its *own* name;
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
        }
        KeySpace::Aggregate { group_by } => {
            // Issue #137: a `GROUP BY` key is either a plain source column
            // (checked exactly as before) or a to-one relationship path,
            // resolved against the same catalog-supplied `relationships` map
            // a field's own `<rel>.<column>` reference uses. An unknown
            // relationship name or a to-many cardinality is rejected here,
            // ahead of the per-field checks below, so a bad `GROUP BY`
            // clause is never masked by a field error instead.
            let mut keys_by_target_column: HashMap<&str, &GroupByKey> =
                HashMap::with_capacity(group_by.len());
            for key in group_by {
                match key {
                    GroupByKey::Column(column) => {
                        let Some(value_type) = source_columns.get(column) else {
                            return Err(ValidationError::UnresolvedGroupByColumn {
                                column: column.clone(),
                            });
                        };
                        reject_unsupported_group_by_key_type(column, *value_type)?;
                    }
                    GroupByKey::RelationshipPath { rel, column } => {
                        let resolved = relationships.get(rel).ok_or_else(|| {
                            ValidationError::UnknownGroupByRelationship { rel: rel.clone() }
                        })?;
                        if resolved.cardinality != RelationshipCardinality::ToOne {
                            return Err(ValidationError::GroupByRelationshipMustBeToOne {
                                rel: rel.clone(),
                                column: column.clone(),
                            });
                        }
                        let Some(value_type) = resolved.column_types.get(column) else {
                            return Err(ValidationError::UnknownRelationshipColumn {
                                table: resolved.to_table.clone(),
                                column: column.clone(),
                            });
                        };
                        reject_unsupported_group_by_key_type(column, *value_type)?;
                    }
                }
                // Issue #519: each key becomes a target column named after
                // its bare column, so two keys sharing that name (two
                // relationships onto the same table, a relationship column
                // named like a from-side one, or a repeated key) would reach
                // the target DDL as a duplicate column. Checked after the key
                // itself resolves, so an unknown relationship or column is
                // reported as such rather than as a clash.
                if let Some(earlier) = keys_by_target_column.insert(key.target_column_name(), key) {
                    return Err(ValidationError::GroupByKeysShareTargetColumn {
                        first: group_by_key_as_written(earlier),
                        second: group_by_key_as_written(key),
                        column: key.target_column_name().to_string(),
                    });
                }
                // Issue #566: a key's column sits beside the target's hidden
                // `__`-prefixed columns, so it may not use their prefix.
                if key.target_column_name().starts_with(RESERVED_COLUMN_PREFIX) {
                    return Err(ValidationError::ReservedGroupByColumnName {
                        key: group_by_key_as_written(key),
                        column: key.target_column_name().to_string(),
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
                // drop it. For a relationship-path key the passthrough shape
                // is the same bare path, not the column's own name (there is
                // no source column named e.g. `author` on this definition's
                // source at all).
                if let Some(key) = group_by
                    .iter()
                    .find(|k| k.target_column_name() == field.name)
                    && !expr_is_group_by_key_passthrough(&field.expr, key)
                {
                    return Err(ValidationError::GroupingColumnFieldMustBePassthrough {
                        field: field.name.clone(),
                    });
                }
                validate_aggregate_field_expr(
                    &field.expr,
                    &field.name,
                    group_by,
                    source_columns,
                    relationships,
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

    let mut deps: HashMap<&str, Vec<String>> = HashMap::with_capacity(def.fields.len());
    for field in &def.fields {
        let mut refs = Vec::new();
        collect_columns(&field.expr, &mut refs);

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
            // `UnresolvedColumn` (issue #78).
            let is_a_different_calc_field = column != field.name && field_names.contains(&column);
            let is_calc_field = !is_self_passthrough && is_a_different_calc_field;
            if !is_source_column && !is_calc_field {
                return Err(ValidationError::UnresolvedColumn {
                    field: field.name.clone(),
                    column,
                });
            }
            if is_calc_field {
                calc_deps.push(column);
            }
        }
        deps.insert(field.name.as_str(), calc_deps);
    }

    detect_cycle(&deps)?;

    // ADR-0006 reference-time cardinality rules: a to-one relationship is
    // enriched by a bare `<rel>.<column>`; a to-many by an aggregate over the
    // path. Checked here (with catalog-resolved cardinalities) before type
    // inference, which relies on the relationship being resolvable.
    let is_aggregate = matches!(def.key_space, KeySpace::Aggregate { .. });
    for field in &def.fields {
        validate_relationship_refs(&field.expr, &field.name, relationships, is_aggregate, false)?;
    }

    infer_field_types(def, source_columns, relationships)?;

    Ok(())
}

/// The name prefix reserved for the hidden columns Trellis keeps on a target
/// table (issue #566). A user field or `GROUP BY` column may not start with it.
pub const RESERVED_COLUMN_PREFIX: &str = "__";

/// Rejects any of a 1-1 definition's `fields` named after one of the
/// source's `primary_key` columns (issue #566). The target already carries
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

/// A `GROUP BY` key spelled the way the definition wrote it: `column` or
/// `rel.column`.
fn group_by_key_as_written(key: &GroupByKey) -> String {
    match key {
        GroupByKey::Column(column) => column.clone(),
        GroupByKey::RelationshipPath { rel, column } => format!("{rel}.{column}"),
    }
}

/// Walks a field's expression enforcing ADR-0006's cardinality rules on every
/// `<rel>.<column>` relationship path it contains. `in_aggregate` tracks
/// whether the current position is the sole argument of an aggregate function
/// (the only shape the parser admits for a path under an aggregate — see the
/// parser's OneToOne to-many handling and [`super::eval`]'s to-many arm).
///
/// - unknown relationship name → [`ValidationError::UnknownRelationship`];
/// - to-one path under an aggregate →
///   [`ValidationError::RelationshipToOneWrappedInAggregate`];
/// - bare to-many path (not under an aggregate) →
///   [`ValidationError::RelationshipToManyRequiresAggregate`].
///
/// `in_group_by_def` is whether this definition is a
/// [`KeySpace::Aggregate`] one. In that key-space the first rule above is
/// *inverted*, so it is skipped here: a to-one path must be aggregate-wrapped
/// (folded over the group's source rows — see
/// [`ValidationError::UngroupedRelationshipReference`]) rather than referenced
/// bare, and [`validate_aggregate_field_expr`] enforces that instead. The
/// to-many rule is unchanged either way, though in an aggregate definition
/// [`validate_aggregate_field_expr`] rejects a to-many path first, wrapped or
/// not.
///
/// The referenced column's existence and type are checked separately by
/// [`infer_expr`], which resolves the path against `column_types`.
fn validate_relationship_refs(
    expr: &Expr,
    field_name: &str,
    relationships: &HashMap<String, ResolvedRelationship>,
    in_group_by_def: bool,
    in_aggregate: bool,
) -> Result<(), ValidationError> {
    match expr {
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => Ok(()),
        Expr::RelationshipPath { rel, column } => {
            let Some(resolved) = relationships.get(rel) else {
                return Err(ValidationError::UnknownRelationship {
                    field: field_name.to_string(),
                    rel: rel.clone(),
                });
            };
            match resolved.cardinality {
                RelationshipCardinality::ToOne if in_aggregate && !in_group_by_def => {
                    Err(ValidationError::RelationshipToOneWrappedInAggregate {
                        field: field_name.to_string(),
                        rel: rel.clone(),
                        column: column.clone(),
                    })
                }
                RelationshipCardinality::ToMany if !in_aggregate => {
                    Err(ValidationError::RelationshipToManyRequiresAggregate {
                        field: field_name.to_string(),
                        rel: rel.clone(),
                        column: column.clone(),
                    })
                }
                _ => Ok(()),
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            // A relationship path never appears directly under a binary
            // operator as an aggregate argument, so the aggregate context does
            // not propagate across an operator.
            validate_relationship_refs(lhs, field_name, relationships, in_group_by_def, false)?;
            validate_relationship_refs(rhs, field_name, relationships, in_group_by_def, false)
        }
        Expr::FunctionCall { name, args } => {
            let is_aggregate = super::registry::lookup_aggregate_function(name).is_some();
            for arg in args {
                validate_relationship_refs(
                    arg,
                    field_name,
                    relationships,
                    in_group_by_def,
                    is_aggregate,
                )?;
            }
            Ok(())
        }
    }
}

/// Walks a [`KeySpace::Aggregate`] field's expression, rejecting a bare
/// (not wrapped in `SUM`/`MIN`/`MAX`/`AVG`) reference to a source column
/// that isn't a grouping key. `in_aggregate_call` tracks whether the current
/// position is already inside such a call; only a real source column
/// reference is checked — a reference to another calculated field (already
/// a single value per group by the time it's used) is unaffected, the same
/// as inter-field composition in a 1-1 definition.
///
/// A `<rel>.<column>` relationship path (issue #94) is held to the same rule,
/// for the same reason: a **to-one** path resolves to exactly one related
/// value per *source* row (a `LEFT JOIN`, evaluated before the group folds),
/// so it is accepted when — and only when — it is wrapped in an aggregate
/// call, *or* (issue #137) it names exactly the same relationship path as one
/// of this definition's own `GROUP BY` keys (the relationship-path twin of a
/// bare grouping-column reference, e.g. `GROUP BY tag, post.author SELECT
/// post.author AS author_alias, COUNT(*) AS c` — `post.author` resolves once
/// per group, exactly like a bare `tag` reference does, so it needs no
/// aggregate wrapper either) — and otherwise rejected as
/// [`ValidationError::UngroupedRelationshipReference`]. A **to-many** path is
/// rejected outright ([`ValidationError::RelationshipPathInAggregate`]):
/// aggregating it inside a GROUP BY aggregate is a nested aggregation with no
/// settled semantics. An *unresolvable* relationship name is passed over here
/// so [`validate_relationship_refs`] can report the more specific
/// [`ValidationError::UnknownRelationship`] against the same field.
fn validate_aggregate_field_expr(
    expr: &Expr,
    field_name: &str,
    group_by: &[GroupByKey],
    source_columns: &HashMap<String, ValueType>,
    relationships: &HashMap<String, ResolvedRelationship>,
    in_aggregate_call: bool,
) -> Result<(), ValidationError> {
    match expr {
        Expr::Column(name) => {
            let is_group_key = group_by
                .iter()
                .any(|k| matches!(k, GroupByKey::Column(c) if c == name));
            if source_columns.contains_key(name) && !is_group_key && !in_aggregate_call {
                return Err(ValidationError::UngroupedColumnReference {
                    field: field_name.to_string(),
                    column: name.clone(),
                });
            }
            Ok(())
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => Ok(()),
        Expr::RelationshipPath { rel, column } => {
            let Some(resolved) = relationships.get(rel) else {
                // Unknown name: `validate_relationship_refs` reports it.
                return Ok(());
            };
            let is_group_key = group_by.iter().any(|k| {
                matches!(k, GroupByKey::RelationshipPath { rel: r, column: c } if r == rel && c == column)
            });
            match resolved.cardinality {
                RelationshipCardinality::ToMany => {
                    Err(ValidationError::RelationshipPathInAggregate {
                        field: field_name.to_string(),
                        rel: rel.clone(),
                        column: column.clone(),
                    })
                }
                RelationshipCardinality::ToOne if !in_aggregate_call && !is_group_key => {
                    Err(ValidationError::UngroupedRelationshipReference {
                        field: field_name.to_string(),
                        rel: rel.clone(),
                        column: column.clone(),
                    })
                }
                RelationshipCardinality::ToOne => Ok(()),
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            validate_aggregate_field_expr(
                lhs,
                field_name,
                group_by,
                source_columns,
                relationships,
                in_aggregate_call,
            )?;
            validate_aggregate_field_expr(
                rhs,
                field_name,
                group_by,
                source_columns,
                relationships,
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
                    relationships,
                    in_aggregate_call || is_aggregate_call,
                )?;
            }
            Ok(())
        }
    }
}

/// Whether `expr` is exactly the passthrough shape [`GroupByKey`] `key`
/// requires of a field named after its target column name (see
/// [`ValidationError::GroupingColumnFieldMustBePassthrough`]): a bare
/// [`Expr::Column`] of the same name for a plain grouping column, or a bare
/// [`Expr::RelationshipPath`] naming the exact same relationship and column
/// for a relationship-path grouping key — there is no other expression shape
/// DDL generation gives a target column of its own for that name, so
/// anything else is rejected rather than silently dropped.
fn expr_is_group_by_key_passthrough(expr: &Expr, key: &GroupByKey) -> bool {
    match key {
        GroupByKey::Column(name) => matches!(expr, Expr::Column(n) if n == name),
        GroupByKey::RelationshipPath { rel, column } => {
            matches!(expr, Expr::RelationshipPath { rel: r, column: c } if r == rel && c == column)
        }
    }
}

fn collect_columns(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(name) => out.push(name.clone()),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {}
        // Not a source-column reference by name — its type is resolved from
        // relationship metadata by `infer_expr`, and its cardinality rules by
        // `validate_relationship_refs`, when they walk the same expression.
        Expr::RelationshipPath { .. } => {}
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

/// Infers every calculated field's [`ValueType`], type-checking each
/// operator's operands along the way. Reused by [`super::ddl`] to pick each
/// target column's Postgres type, since a field's inferred type *is* its
/// target column's declared type — this grammar has no separate "declare a
/// target column's type" syntax (see issue #63's report).
///
/// Assumes `def`'s column references already resolved (this module's own
/// [`validate`] checks that first) and that `def` is acyclic; a definition
/// that reaches this function without either guarantee having been checked
/// gets the same `Cycle`/defense-in-depth treatment [`super::eval`] gives
/// its own recursion, rather than overflowing the stack.
/// Rejects an [`super::ast::KeySpace::Aggregate`] `GROUP BY` key whose
/// column is typed [`ValueType::Other`] — see
/// [`ValidationError::UnsupportedGroupByKeyType`] for why a key role needs
/// more than #108's passthrough classification.
fn reject_unsupported_group_by_key_type(
    column: &str,
    value_type: ValueType,
) -> Result<(), ValidationError> {
    match value_type {
        // Issue #111: an exact integer's text rendering is canonical (no
        // leading zeros, no `+`, no whitespace), so it round-trips through
        // the still-text-keyed group-key path byte-for-byte — the same
        // property that already puts `smallint`/`integer`/`bigint` on
        // `catalog::TEXT_STABLE_JOIN_KEY_TYPES`.
        //
        // `Numeric` is admitted here as it always has been, and that is
        // knowingly weaker: `1.0` and `1.00` are equal to Postgres but
        // distinct under `::text` matching, which is exactly why `numeric`
        // is *rejected* as a relationship join key and as a primary key
        // (#107). Tightening the `GROUP BY` gate to match belongs with the
        // typed key index (#110), not here — see the note on issue #111.
        ValueType::Integer(_) | ValueType::Numeric | ValueType::Text | ValueType::Uuid => Ok(()),
        // Issue #119: `boolean` was already admitted here (this arm
        // predates the issue), and it is genuinely safe, but not for the
        // reason every other admission on this function is — this is the
        // one type where the *raw-text* premise the function's own error
        // message states ("GROUP BY keys are matched by their text
        // rendering") is not actually what happens underneath.
        //
        // `boolean` is the only type with a `pg_cast`-registered `::text`
        // function distinct from its output function (`pg_catalog.text
        // (boolean)` renders `'true'`/`'false'`; `boolout` — what CDC
        // decodes and what `intake::extract_key` stores verbatim — renders
        // `'t'`/`'f'`), so a `GROUP BY` key genuinely does have two
        // possible spellings depending on which code path produced it. See
        // `catalog::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment for the live
        // `pg_cast` evidence and why that divergence is exactly why
        // `boolean` is *not* on that allowlist (the join/primary-key role
        // really does raw-text `{col}::text = $1` matching in places).
        //
        // What saves the *final SQL* half of the `GROUP BY` role is that
        // `staging::apply_aggregate` never does raw-text matching against
        // the database: every keyset match (`keyset_unnest`) binds the
        // group key as a `$1::text[]::{ty}[]` array, so whichever spelling
        // a key arrived in gets parsed back through `boolin` — a permissive
        // *input* function that accepts both `'t'`/`'f'` and `'true'`/
        // `'false'` — before ever being compared, natively, against the
        // live column. Two renderings of one value always resolve to the
        // same physical target row.
        //
        // That alone was not the whole story, and issue #119's review
        // found the second half live: before this arm's write, resolving
        // to the same *row* was not the same as resolving to the same
        // *value* for it. `staging::apply_aggregate::accumulate_changes`
        // buckets a batch's touched rows into in-memory `GroupPlan`s keyed
        // by `derive_group_key`'s own `text` — a bare Rust `HashMap` key,
        // compared byte-for-byte with no database (and so no `boolin`) in
        // the loop at all. An image-bearing CDC row keyed `'t'` and a bare
        // recompute's live-read row keyed `'true'` used to land in *two*
        // separate `GroupPlan`s that both then independently wrote to the
        // one row the SQL layer correctly resolved them to — silently
        // corrupting its value (a delta add stacked on an unrelated forced
        // recompute) rather than visibly splitting it into two rows the way
        // #248's pre-fix `timestamp` did. `derive_group_key` now runs each
        // part through `canonicalize_group_key_part` before folding it into
        // that dedup key (a no-op for every type but `Boolean`), closing
        // the gap at its actual source. `trellis/tests/defs_boolean.rs`'s
        // `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
        // reproduces the corruption end-to-end and pins the fix.
        ValueType::Boolean => Ok(()),
        // Issue #112: `real`/`double precision` are rejected outright, and
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
        // Admitting floats here is #110's typed key index to grant, by
        // comparing decoded values through `crate::float::compare`; until
        // then, rejecting is the honest answer and `numeric` is the
        // steer-to. See `catalog::TEXT_STABLE_JOIN_KEY_TYPES`.
        ValueType::Float(_) => Err(ValidationError::UnsupportedGroupByKeyType {
            column: column.to_string(),
            value_type,
        }),
        // `oid` is one `Other` family admitted as a key (issue #111):
        // Postgres renders it as canonical unsigned decimal, so it is
        // text-stable in exactly the way `interval` is not. It stays an
        // `Other` rather than joining `ValueType::Integer` because Postgres
        // gives it no arithmetic at all — see `pg_type::PgType::Oid`.
        ValueType::Other(PgType::Oid) => Ok(()),
        // `bytea` is the other (issue #114), on the same "text-stability is
        // a property of the rendering, not the operator set" reasoning:
        // `byteaout` under the pinned `bytea_output = 'hex'` is a bijection
        // on every byte string, with no session-GUC dependence at all (see
        // `catalog::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment for the live
        // evidence). Unlike the temporal families it needs no per-value
        // predicate here — every `bytea` value clears the bar, not just a
        // subset — so it is a plain admit rather than a call into a sibling
        // module.
        ValueType::Other(PgType::Bytea) => Ok(()),
        // Issue #113: `date`, `timestamp`, `time`, `timetz` and (issue #246)
        // `timestamptz` join `oid` on the same grounds, gated by
        // `crate::temporal`'s own per-family verdict rather than by a list
        // repeated here — see that module's doc comment for the live
        // evidence, and `catalog::TEXT_STABLE_JOIN_KEY_TYPES` for the
        // relationship/PK half of the same decision. `timestamp` joined this
        // arm for real once issue #248 fixed its render-consistency defect
        // (it used to fall through to the reject arm below despite being
        // named here); `timestamptz` needed both #248's fix *and* #246's —
        // its rendering is `TimeZone`-dependent, and half of it used to be
        // produced by a walsender Trellis could not pin
        // (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) until issue #246
        // wired `pgwire_replication::ReplicationConfig::with_options`.
        //
        // `interval` is the one temporal family still rejected here, for a
        // reason no GUC or renderer reconciliation touches: it has no
        // canonical rendering at all — `'24 hours'` and `'1 day'` are `=`
        // and render differently, so a text-matched `GROUP BY` would split
        // one Postgres group in two, exactly as a float `-0`/`0` key would.
        ValueType::Other(pg_type) if crate::temporal::is_text_stable(pg_type) => Ok(()),
        // Issue #116: `cidr`/`macaddr`/`macaddr8` join `oid`/`bytea` above —
        // each is on `catalog::TEXT_STABLE_JOIN_KEY_TYPES` (see that
        // constant's doc comment for the live `pg_cast`/renderer evidence),
        // and unlike the temporal families every value of each clears the
        // bar, not just a subset, so a plain admit is enough here too.
        ValueType::Other(PgType::Cidr | PgType::MacAddr | PgType::MacAddr8) => Ok(()),
        // `inet` is admitted here despite being deliberately *absent* from
        // `catalog::TEXT_STABLE_JOIN_KEY_TYPES` — the same split issue #119
        // gave `boolean`, for the same mechanical reason.
        //
        // `inet_out` (what CDC decodes, what `intake::extract_key` stores
        // verbatim) and `network_show` (`<col>::text` — what this engine's
        // own live reads use, post-#248) disagree on a bare-host `inet`
        // value's spelling (`192.168.1.5` vs `192.168.1.5/32` — see
        // `crate::netaddr`'s module doc for the live grid), the identical
        // "one value, two renderers" shape that keeps `inet` off the
        // join/primary-key allowlist. That divergence does not reach the
        // `GROUP BY` role's *final SQL*, because `staging::apply_aggregate`'s
        // keyset match never does raw-text comparison — it casts the *bound
        // array* to the column's native type (`$1::text[]::inet[]`), and
        // `inet_in` is permissive enough to parse both spellings back to the
        // identical stored value (`'192.168.1.5'::inet =
        // '192.168.1.5/32'::inet` is `true`, verified live). What it does
        // reach is `staging::apply_aggregate::accumulate_changes`'s
        // in-memory `GroupPlan` bucketing, which compares `derive_group_key`'s
        // text byte-for-byte with no database (and so no `inet_in`) anywhere
        // in the loop — `boolean`'s exact live bug shape (#119's review).
        // `apply_aggregate::canonicalize_group_key_part`'s `Inet` arm (via
        // `crate::netaddr::canonicalize_group_key_text`) closes that gap the
        // same way it does for `Boolean`, which is what makes this an honest
        // `Ok(())` rather than a still-open hazard.
        ValueType::Other(PgType::Inet) => Ok(()),
        // Issue #118: `bit varying` is safe here for a *different* reason
        // than every arm above it — it isn't about `::text`-vs-native-`=`
        // rendering agreement at all. `bit`/`varbit` have no `pg_cast`
        // override the way `boolean` does (verified live; see
        // `catalog::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment), so raw-text
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
        // reason — it is perfectly safe as a relationship join key or a 1-1
        // primary key (`catalog::TEXT_STABLE_JOIN_KEY_TYPES`), neither of
        // which ever asks a bare `ValueType` to conjure a new column: a
        // join key only ever compares against an already-existing column,
        // and a 1-1 primary key copies the source's own *concrete*
        // introspected type (`bit(5)`, not bare `bit`) verbatim
        // (`ddl::source_primary_key`). It is refused *only* here, and only
        // because this one role's DDL has no concrete-type-introspection
        // escape hatch to reach for. `trellis/tests/defs_bit.rs` pins both
        // halves of this split live.
        ValueType::Other(PgType::VarBit) => Ok(()),
        // Issue #115: `jsonb` falls through to the reject arm below like
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
        // unlock is #110's typed key index, same as `numeric`/`real`/
        // `double precision`/`timestamptz` above — not a jsonb-specific
        // mechanism.
        // Issue #117: an enum type is safe here on *both* of this gate's two
        // independent hazard axes, unlike `bit`'s split above.
        //
        // Text-stability: `enumout` (what `<col>::text` calls — no `pg_cast`
        // override the way `boolean` has, verified live in
        // `defs_enum.rs`) renders each value as its own label text
        // verbatim; an enum value's identity *is* that label (a `pg_enum`
        // row keyed by OID), so there is no deeper representation for two
        // distinct values to collide under — see `catalog::is_enum_type_name`'s
        // own doc comment for the fuller version of this same reasoning
        // (the relationship/PK key roles' dynamic counterpart of this
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
/// out of step with the allowlist it describes the way it did before issue
/// #264 — the exact hand-maintained-second-copy failure mode issue #111
/// already fixed for [`ValidationError::RelationshipUnsupportedJoinKeyType`]
/// via [`super::catalog::supported_join_key_types`].
///
/// It had drifted: the message was missing `timestamp` (admitted since
/// #248) and, after this week's #246/#254/#256/#258/#260/#261/#262 merges,
/// `timestamptz`, `bytea`, `bit varying`, and the network-address types too.
pub(crate) fn supported_group_by_key_types() -> String {
    GROUP_BY_KEY_TYPE_NAMES.join(", ")
}

/// The names [`reject_unsupported_group_by_key_type`] actually admits, in
/// the same order as that function's `Ok` arms, kept here as the single
/// source [`supported_group_by_key_types`] renders instead of a
/// hand-written string.
///
/// This is **not** the same set as
/// [`super::catalog::TEXT_STABLE_JOIN_KEY_TYPES`] — the two gates are
/// checked against each other, deliberately, not assumed identical:
///
/// * `numeric` and `boolean` are admitted *here* but refused as a
///   relationship/primary-key join key (`numeric`: #107, its own text
///   rendering isn't a `1.0`/`1.00` bijection; `boolean`: #119, a second
///   `pg_cast`-registered `::text` renderer disagrees with `boolout`). Both
///   are safe for this role for reasons specific to it — `boolean`'s
///   because `staging::apply_aggregate` never does the raw-text comparison
///   that trips it up elsewhere, once `canonicalize_group_key_part` closes
///   the in-memory `GroupPlan`-bucketing half of the gap (issue #119).
/// * `inet` is admitted here for the identical reason `boolean` is (its own
///   `network_show`-vs-`inet_out` split, closed the same way), but stays off
///   the join-key list.
/// * Fixed-length `bit` is refused *here only*, for a DDL-only reason with
///   nothing to do with text-rendering: `ddl::create_aggregate_target_table`
///   would declare a brand-new `bit(1)` column from a bare `ValueType`,
///   truncating any wider source column (issue #118's `VarBit` arm has the
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
    relationships: &HashMap<String, ResolvedRelationship>,
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
                relationships,
                &fields_by_name,
                &mut types,
                &mut in_progress,
            )?;
            types.insert(field.name.clone(), t);
        }
    }
    Ok(types)
}

fn infer_field(
    field: &super::ast::FieldDef,
    source_columns: &HashMap<String, ValueType>,
    relationships: &HashMap<String, ResolvedRelationship>,
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
    let t = infer_expr(
        &field.expr,
        &field.name,
        source_columns,
        relationships,
        fields_by_name,
        types,
        in_progress,
    );
    in_progress.remove(&field.name);
    let t = t?;
    types.insert(field.name.clone(), t);
    Ok(t)
}

fn infer_expr(
    expr: &Expr,
    field_name: &str,
    source_columns: &HashMap<String, ValueType>,
    relationships: &HashMap<String, ResolvedRelationship>,
    fields_by_name: &HashMap<&str, &super::ast::FieldDef>,
    types: &mut HashMap<String, ValueType>,
    in_progress: &mut HashSet<String>,
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
                    relationships,
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
        // Issue #111: an unadorned literal is typed exactly as Postgres's
        // own lexer types it — `integer` if it fits, else `bigint`, else
        // (or if fractional) `numeric`. `eval::number_literal` is the
        // value-level twin of this rule and must agree with it.
        Expr::NumberLiteral(text) => Ok(number_literal_type(text)),
        Expr::StringLiteral(_) => Ok(ValueType::Text),
        Expr::TypedLiteral { value_type, text } => {
            // Checked here, not in the parser, for the same reason
            // `regexp_count`'s pattern is (see `validate_regexp_pattern`):
            // this runs on every path into the catalog, including a
            // hand-built AST that never went through `parse`, so a literal
            // can't reach the evaluator un-checked. See
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
        Expr::RelationshipPath { rel, column } => {
            // A `<rel>.<column>` enrichment's type is the *to-side* column's
            // type, resolved by the caller into `relationships`. The
            // relationship's existence and this path's cardinality were
            // already checked by `validate_relationship_refs`; here only the
            // referenced column's existence (hence its type) remains.
            let resolved =
                relationships
                    .get(rel)
                    .ok_or_else(|| ValidationError::UnknownRelationship {
                        field: field_name.to_string(),
                        rel: rel.clone(),
                    })?;
            resolved.column_types.get(column).copied().ok_or_else(|| {
                ValidationError::UnknownRelationshipColumn {
                    table: resolved.to_table.clone(),
                    column: column.clone(),
                }
            })
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs_t = infer_expr(
                lhs,
                field_name,
                source_columns,
                relationships,
                fields_by_name,
                types,
                in_progress,
            )?;
            let rhs_t = infer_expr(
                rhs,
                field_name,
                source_columns,
                relationships,
                fields_by_name,
                types,
                in_progress,
            )?;
            // Issue #111: operand admissibility *and* the result type both
            // come from `registry::operator_result_type`, which models
            // Postgres's family of `+`/`>` operators (`int4pl`,
            // `numeric_add`, and the implicit `int -> numeric` coercion
            // between them) rather than the single exact signature this used
            // to compare against. `OperatorSpec::arg_types` survives only as
            // the canonical shape a `TypeMismatch` names.
            if let Some(result) = super::registry::operator_result_type(*op, lhs_t, rhs_t) {
                return Ok(result);
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
            let mut common_type = None;
            for (i, arg) in args.iter().enumerate() {
                let arg_t = infer_expr(
                    arg,
                    field_name,
                    source_columns,
                    relationships,
                    fields_by_name,
                    types,
                    in_progress,
                )?;
                if let Some(ct) = common_type {
                    // Issue #111: `COALESCE`'s arguments used to have to be
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
            Ok(common_type.unwrap())
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
            let spec = super::registry::lookup_function(name).or(aggregate_spec);
            let Some(spec) = spec else {
                for arg in args {
                    infer_expr(
                        arg,
                        field_name,
                        source_columns,
                        relationships,
                        fields_by_name,
                        types,
                        in_progress,
                    )?;
                }
                return Ok(ValueType::Numeric);
            };
            // Issue #120: `COUNT` is special-cased ahead of the generic
            // per-argument loop below, for both its accepted shapes —
            // `COUNT(*)` (`args` empty) and `COUNT(<expr>)` (`args` one
            // element). `AGGREGATE_FUNCTION_SPECS`'s `COUNT` row declares
            // `arg_types: &[]` (the parser, not this table, enforces
            // `COUNT`'s arity — see `super::parser`'s own `COUNT` branch), so
            // the generic `args.iter().zip(spec.arg_types)` loop just below
            // would `zip` a real one-element `args` against an empty
            // `arg_types` and silently produce *zero* iterations — meaning
            // `COUNT(<expr>)`'s argument would never be recursively
            // validated at all (an unresolved column, unknown relationship,
            // etc. would sail through unchecked). Postgres's own `count(x)`
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
                        relationships,
                        fields_by_name,
                        types,
                        in_progress,
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
                    relationships,
                    fields_by_name,
                    types,
                    in_progress,
                )?;
                // Issue #111: a `Numeric`-declared *aggregate* argument now
                // means "any exact-numeric type" — `SUM`/`AVG`/`MIN`/`MAX`
                // accept every exact-integer width as well as `numeric`, and
                // the result type depends on which one
                // (`registry::aggregate_result_type`). A scalar function's
                // argument list stays an exact match: all four of those take
                // `Text`, and Postgres would not coerce an integer into one.
                let admissible = if is_aggregate && *expected == ValueType::Numeric {
                    // Issues #111/#112: a `Numeric`-declared *aggregate*
                    // argument means "any numeric type", floats included —
                    // Postgres has a `sum`/`avg`/`min`/`max` for `real` and
                    // `double precision` just as it does for the exact
                    // types. Issue #113 widened it past the numeric family
                    // entirely (`MIN`/`MAX` over the temporal types,
                    // `SUM(interval)`), at which point "what does this
                    // aggregate accept?" and "what does it return?" are one
                    // question with one answer, so the admissibility test
                    // *is* `registry::aggregate_result_type` resolving —
                    // rather than a predicate here that a future family
                    // could teach one of the two and not the other.
                    super::registry::aggregate_result_type(name, arg_t).is_some()
                } else if is_aggregate && matches!(name.as_str(), "BIT_AND" | "BIT_OR") {
                    // Issue #118: `bit_and`/`bit_or` accept *two* distinct
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
            if name == "REGEXP_COUNT"
                && let Some(pattern_arg) = args.get(1)
            {
                validate_regexp_pattern(field_name, pattern_arg)?;
            }
            if is_aggregate
                && let Some(&arg_t) = arg_types.first()
                && let Some(result) = super::registry::aggregate_result_type(name, arg_t)
            {
                // Issue #117: `MIN`/`MAX(enum)` is refused specifically when
                // it wraps a *to-many* relationship path — see
                // `ValidationError::EnumAggregateOverToManyRelationshipUnsupported`'s
                // own doc comment for the full reasoning (no live-SQL
                // fallback exists for this one shape, unlike a
                // `KeySpace::Aggregate` field's own `MIN`/`MAX`). A to-*one*
                // path resolves to a single value per row — never a
                // multi-row fold — so it is unaffected, matched by
                // `RelationshipCardinality::ToMany` specifically rather than
                // "any `RelationshipPath` argument".
                //
                // Issue #120: `MIN`/`MAX(text)` wrapping a to-many path hits
                // the identical structural gap, for the identical reason —
                // see `ValidationError::TextAggregateOverToManyRelationshipUnsupported`'s
                // own doc comment. `uuid` never reaches this match arm at
                // all (checked live, not assumed): Postgres has no
                // `min(uuid)`/`max(uuid)` aggregate whatsoever, so
                // `registry::aggregate_result_type` already returned `None`
                // for it above, well before this block — the same "opclass
                // but no aggregate" shape #114/#116 found for
                // `bytea`/`macaddr`.
                if matches!(name.as_str(), "MIN" | "MAX")
                    && matches!(result, ValueType::Other(PgType::Enum(_)) | ValueType::Text)
                    && let Expr::RelationshipPath { rel, .. } = &args[0]
                    && relationships
                        .get(rel)
                        .is_some_and(|r| r.cardinality == RelationshipCardinality::ToMany)
                {
                    return Err(match result {
                        ValueType::Text => {
                            ValidationError::TextAggregateOverToManyRelationshipUnsupported {
                                field: field_name.to_string(),
                                function: name.clone(),
                                rel: rel.clone(),
                            }
                        }
                        _ => ValidationError::EnumAggregateOverToManyRelationshipUnsupported {
                            field: field_name.to_string(),
                            function: name.clone(),
                            rel: rel.clone(),
                        },
                    });
                }
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
/// The type-level twin of [`super::eval`]'s `number_literal`; the two must
/// agree, or the validator and the evaluator would disagree about a field's
/// target column type.
fn number_literal_type(text: &str) -> ValueType {
    if !text.contains('.')
        && let Ok(value) = text.parse::<i64>()
    {
        return ValueType::Integer(IntWidth::narrowest_for(value));
    }
    ValueType::Numeric
}

/// The type two operands unify to, mirroring Postgres's own implicit
/// coercions *within the exact-numeric family* (issue #111): two integers
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
        // Issue #112. `COALESCE` resolves through Postgres's
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

#[derive(PartialEq, Eq)]
enum Mark {
    Visiting,
    Done,
}

/// DFS with a visiting/done mark set, hand-rolled per the repo's convention
/// of avoiding a graph crate for a problem this small.
fn detect_cycle(deps: &HashMap<&str, Vec<String>>) -> Result<(), ValidationError> {
    let mut marks: HashMap<&str, Mark> = HashMap::with_capacity(deps.len());
    let names: Vec<&str> = deps.keys().copied().collect();
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
    deps: &'a HashMap<&'a str, Vec<String>>,
    marks: &mut HashMap<&'a str, Mark>,
    stack: &mut Vec<String>,
) -> Result<(), ValidationError> {
    match marks.get(name) {
        Some(Mark::Done) => return Ok(()),
        Some(Mark::Visiting) => {
            stack.push(name.to_string());
            return Err(ValidationError::Cycle {
                cycle: stack.clone(),
            });
        }
        None => {}
    }

    marks.insert(name, Mark::Visiting);
    stack.push(name.to_string());

    if let Some(children) = deps.get(name) {
        for child in children {
            visit(child.as_str(), deps, marks, stack)?;
        }
    }

    stack.pop();
    marks.insert(name, Mark::Done);
    Ok(())
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn unresolved_column_is_rejected() {
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: col("mystery"),
        }]);
        let err = validate(&d, &HashMap::new(), &HashMap::new()).unwrap_err();
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
        let err = validate(&d, &HashMap::new(), &HashMap::new()).unwrap_err();
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
        // is "another calculated field" to depend on (issue #78): a field
        // is never "another" calculated field relative to itself, so this
        // must resolve as an unknown column rather than a self-cycle.
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: col("x"),
        }]);
        let err = validate(&d, &HashMap::new(), &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn a_calc_field_named_after_a_real_column_must_be_a_bare_passthrough() {
        // Issue #80's exact shape: `total_posted_words` is a calculated-field
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::CalculatedFieldShadowsSourceColumn {
                field: "total_posted_words".to_string(),
            }
        );
    }

    #[test]
    fn a_calc_field_bare_passthrough_of_a_same_named_column_is_still_allowed() {
        // The self-passthrough idiom (issue #36) must remain legal: a field
        // named after a real column, whose expression is exactly that
        // column, is unambiguous and carries no shadowing risk.
        let d = def(vec![FieldDef {
            name: "price".to_string(),
            expr: col("price"),
        }]);
        let source_columns = numeric_columns(&["price"]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn target_equals_source_is_rejected() {
        let mut d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: Expr::NumberLiteral("1".to_string()),
        }]);
        d.target = "s".to_string();
        d.source = "s".to_string();
        let err = validate(&d, &HashMap::new(), &HashMap::new()).unwrap_err();
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
        let err = validate(&d, &HashMap::new(), &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn uuid_column_passthrough_is_accepted() {
        let d = def(vec![FieldDef {
            name: "author".to_string(),
            expr: col("author"),
        }]);
        let source_columns = HashMap::from([("author".to_string(), ValueType::Uuid)]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    /// Issue #108 review: a `GROUP BY` key is matched by its `::text`
    /// rendering, so a `ValueType::Other` column — passthrough-only per
    /// `docs/type-support.md`, with no key role yet — must be rejected here
    /// rather than silently become a text-matched key. `interval` is the
    /// sharpest case: Postgres holds `'1 day' = '24 hours'`, but those
    /// render differently, so a text-matched key would split one real group
    /// into two and diverge from the Postgres oracle.
    ///
    /// Nothing could reach this before #108 (an `Other`-typed column never
    /// made it into `source_columns` at all), so this only narrows that
    /// issue's newly opened surface — the two other key roles already gate
    /// on `catalog::is_text_stable_join_key_type`.
    #[test]
    fn an_other_typed_column_is_rejected_as_an_aggregate_group_by_key() {
        // Issue #118: `PgType::Bit` (fixed-length) is deliberately in this
        // refusal list even though `PgType::VarBit` right next to it is
        // admitted — see this function's own `VarBit` arm for the DDL-only
        // reason (not a text-rendering one) the two split. `PgType::TimestampTz`
        // used to be here too, until issue #246 pinned `TimeZone` on the
        // walsender the way `DateStyle` was already pinned on the pool —
        // see `defs_temporal.rs`'s live coverage for its new admitted status.
        for pg_type in [
            PgType::Interval,
            PgType::Json,
            // Issue #115: `jsonb` genuinely has canonical, sorted key
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
                validate(&d, &source_columns, &HashMap::new()),
                Err(ValidationError::UnsupportedGroupByKeyType {
                    column: "k".to_string(),
                    value_type: ValueType::Other(pg_type),
                }),
                "{pg_type} must not be accepted as a GROUP BY key"
            );
        }
    }

    /// Issue #264: `UnsupportedGroupByKeyType`'s `Display` impl used to
    /// name a second, hand-maintained copy of this gate's admission list
    /// ("numeric, text, boolean, uuid, oid, date, time and timetz") that had
    /// drifted stale — missing `timestamp` (admitted since #248) and, after
    /// #246/#254/#256/#258/#260/#261/#262, `timestamptz`, `bytea`, `bit
    /// varying`, and the network-address types too.
    ///
    /// This test drives [`reject_unsupported_group_by_key_type`] directly
    /// with one representative value per type family it actually
    /// classifies — every admitted family alongside every still-refused one
    /// (`interval`, `real`/`double precision`, `jsonb`, `json`, fixed-length
    /// `bit`, `money`, `xml`, `tsvector`/`tsquery`, `unrecognized`) — and
    /// checks that [`supported_group_by_key_types`]'s rendering names
    /// exactly the families that came back `Ok`. A future admission change to that
    /// function that isn't mirrored in [`GROUP_BY_KEY_TYPE_NAMES`] fails
    /// here, the same drift guard #111 gave the sibling join-key message.
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
                 no longer name every type it should — the exact drift issue #264 fixed"
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
    /// perfectly good bare *passthrough* field (#108's actual scope) — only
    /// the key role is gated.
    #[test]
    fn an_other_typed_column_passthrough_is_still_accepted() {
        let d = def(vec![FieldDef {
            name: "payload".to_string(),
            expr: col("payload"),
        }]);
        let source_columns =
            HashMap::from([("payload".to_string(), ValueType::Other(PgType::Jsonb))]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn boolean_column_passthrough_is_accepted() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: col("flag"),
        }]);
        let source_columns = HashMap::from([("flag".to_string(), ValueType::Boolean)]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &HashMap::new(), &HashMap::new()), Ok(()));
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
        let types = infer_field_types(&d, &source_columns, &HashMap::new()).unwrap();
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
        let types = infer_field_types(&d, &source_columns, &HashMap::new()).unwrap();
        assert_eq!(types["has_foo"], ValueType::Boolean);
    }

    #[test]
    fn boolean_target_field_from_greater_than_is_accepted() {
        // A calculated field whose final expression type is Boolean must be
        // storable, not just intermediate — issue #65's "boolean target
        // column" requirement.
        let d = def(vec![FieldDef {
            name: "is_positive".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(col("number_col")),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }]);
        let source_columns = numeric_columns(&["number_col"]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::NonLiteralRegexPattern {
                field: "out".to_string(),
            }
        );
    }

    fn aggregate_def(group_by: &[&str], fields: Vec<FieldDef>) -> TransformDef {
        aggregate_def_with_keys(
            group_by
                .iter()
                .map(|s| GroupByKey::Column(s.to_string()))
                .collect(),
            fields,
        )
    }

    fn aggregate_def_with_keys(group_by: Vec<GroupByKey>, fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::Aggregate { group_by },
            fields,
            predicate: Predicate::True,
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn a_to_many_relationship_path_in_an_aggregate_definition_is_rejected() {
        // A *to-many* path inside a GROUP BY (aggregate) definition is a
        // nested aggregation — an aggregate over related rows, itself folded
        // over the group's source rows — with no settled semantics, so it is
        // rejected even though the path is aggregate-wrapped (which is what
        // ADR-0006's per-field cardinality rule asks of a to-many path). Only
        // the *to-one* case was opened up by issue #94; see
        // `a_to_one_relationship_path_aggregated_in_a_group_by_is_accepted`.
        let d = aggregate_def(
            &["order_id"],
            vec![
                FieldDef {
                    name: "order_id".to_string(),
                    expr: col("order_id"),
                },
                FieldDef {
                    name: "total_words".to_string(),
                    expr: Expr::FunctionCall {
                        name: "SUM".to_string(),
                        args: vec![Expr::RelationshipPath {
                            rel: "comments".to_string(),
                            column: "word_count".to_string(),
                        }],
                    },
                },
            ],
        );
        let source_columns = numeric_columns(&["order_id"]);
        let relationships = HashMap::from([(
            "comments".to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToMany,
                to_table: "comments".to_string(),
                to_col: "order_id".to_string(),
                column_types: HashMap::from([("word_count".to_string(), ValueType::Numeric)]),
            },
        )]);
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::RelationshipPathInAggregate {
                field: "total_words".to_string(),
                rel: "comments".to_string(),
                column: "word_count".to_string(),
            }
        );
    }

    /// Issue #117: a `KeySpace::OneToOne` field's `MIN`/`MAX` over an
    /// enum-typed column reached through a **to-many** relationship path is
    /// refused, even though the identical shape over a `Numeric` column
    /// (`a_to_many_relationship_path_in_an_aggregate_definition_is_rejected`'s
    /// sibling case, minus the `KeySpace::Aggregate`-only
    /// `RelationshipPathInAggregate` rule that doesn't apply to a plain 1-1
    /// definition at all) is perfectly valid issue #29 grammar. See
    /// `ValidationError::EnumAggregateOverToManyRelationshipUnsupported`'s
    /// own doc comment for why: this specific combination folds through the
    /// pure, DB-less evaluator with no live-Postgres fallback, and an
    /// enum's creation-order comparison cannot be answered without one.
    #[test]
    fn min_max_enum_over_a_to_many_relationship_is_refused() {
        let priority = ValueType::Other(PgType::Enum("enum:public.priority_enum"));
        let d = def(vec![FieldDef {
            name: "worst".to_string(),
            expr: Expr::FunctionCall {
                name: "MAX".to_string(),
                args: vec![Expr::RelationshipPath {
                    rel: "items".to_string(),
                    column: "priority".to_string(),
                }],
            },
        }]);
        let source_columns = HashMap::new();
        let relationships = HashMap::from([(
            "items".to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToMany,
                to_table: "items".to_string(),
                to_col: "order_id".to_string(),
                column_types: HashMap::from([("priority".to_string(), priority)]),
            },
        )]);
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::EnumAggregateOverToManyRelationshipUnsupported {
                field: "worst".to_string(),
                function: "MAX".to_string(),
                rel: "items".to_string(),
            }
        );
    }

    /// The identical shape over a to-*one* relationship, inside a
    /// `KeySpace::Aggregate` (GROUP BY) definition — issue #94's headline
    /// shape (`a_to_one_relationship_path_aggregated_in_a_group_by_is_accepted`),
    /// now with an enum-typed column — is unaffected by this issue's new
    /// guard: it resolves to exactly one related value per source row,
    /// never a multi-row fold, so there is no ordering question the
    /// evaluator needs to answer at all. (A `KeySpace::OneToOne` definition
    /// has no equivalent shape to test here: ADR-0006 never allows a to-one
    /// path to be aggregate-wrapped there at all —
    /// `ValidationError::RelationshipToOneWrappedInAggregate` — regardless
    /// of this issue.)
    #[test]
    fn min_max_enum_over_a_to_one_relationship_in_a_group_by_is_accepted() {
        let priority = ValueType::Other(PgType::Enum("enum:public.priority_enum"));
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "worst".to_string(),
                    expr: Expr::FunctionCall {
                        name: "MAX".to_string(),
                        args: vec![Expr::RelationshipPath {
                            rel: "assignee".to_string(),
                            column: "priority".to_string(),
                        }],
                    },
                },
            ],
        );
        let source_columns = numeric_columns(&["tag", "assignee"]);
        let relationships = HashMap::from([(
            "assignee".to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToOne,
                to_table: "assignees".to_string(),
                to_col: "id".to_string(),
                column_types: HashMap::from([("priority".to_string(), priority)]),
            },
        )]);
        assert_eq!(validate(&d, &source_columns, &relationships), Ok(()));
    }

    /// A plain `KeySpace::Aggregate` field's own `MIN`/`MAX(<enum column>)`
    /// — no relationship at all — is unaffected by this issue's new guard,
    /// which only ever fires for a *to-many relationship path* argument.
    /// This is the shape that is always
    /// [`super::invertibility::Invertibility::RecomputeOnly`] and therefore
    /// always resolved by a live server-side `min()`/`max()` push-down, per
    /// `ValidationError::EnumAggregateOverToManyRelationshipUnsupported`'s
    /// own doc comment.
    #[test]
    fn min_max_enum_over_a_plain_group_by_column_is_accepted() {
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "worst".to_string(),
                    expr: Expr::FunctionCall {
                        name: "MAX".to_string(),
                        args: vec![col("priority")],
                    },
                },
            ],
        );
        let source_columns = HashMap::from([
            ("tag".to_string(), ValueType::Numeric),
            (
                "priority".to_string(),
                ValueType::Other(PgType::Enum("enum:public.priority_enum")),
            ),
        ]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    /// Issue #120's `text` twin of `min_max_enum_over_a_to_many_relationship_is_refused`
    /// — see `ValidationError::TextAggregateOverToManyRelationshipUnsupported`'s
    /// own doc comment for why `text`'s collation-dependent ordering hits the
    /// identical structural gap enum's schema-dependent ordering does.
    #[test]
    fn min_max_text_over_a_to_many_relationship_is_refused() {
        let d = def(vec![FieldDef {
            name: "worst".to_string(),
            expr: Expr::FunctionCall {
                name: "MAX".to_string(),
                args: vec![Expr::RelationshipPath {
                    rel: "items".to_string(),
                    column: "label".to_string(),
                }],
            },
        }]);
        let source_columns = HashMap::new();
        let relationships = HashMap::from([(
            "items".to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToMany,
                to_table: "items".to_string(),
                to_col: "order_id".to_string(),
                column_types: HashMap::from([("label".to_string(), ValueType::Text)]),
            },
        )]);
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::TextAggregateOverToManyRelationshipUnsupported {
                field: "worst".to_string(),
                function: "MAX".to_string(),
                rel: "items".to_string(),
            }
        );
    }

    /// A plain `KeySpace::Aggregate` field's own `MIN`/`MAX(<text column>)` —
    /// no relationship at all — is unaffected: always
    /// [`super::invertibility::Invertibility::RecomputeOnly`], always
    /// resolved by a live server-side `min()`/`max()` push-down.
    #[test]
    fn min_max_text_over_a_plain_group_by_column_is_accepted() {
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "worst".to_string(),
                    expr: Expr::FunctionCall {
                        name: "MAX".to_string(),
                        args: vec![col("label")],
                    },
                },
            ],
        );
        let source_columns = HashMap::from([
            ("tag".to_string(), ValueType::Numeric),
            ("label".to_string(), ValueType::Text),
        ]);
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    /// Issue #120: `MIN`/`MAX(<uuid column>)` is refused *unconditionally* —
    /// checked live rather than assumed, Postgres has no `min(uuid)`/
    /// `max(uuid)` aggregate at all (the same "opclass but no aggregate"
    /// shape #114/#116 found for `bytea`/`macaddr` — `trellis/tests/
    /// defs_min_max_text_and_uuid.rs` pins this against a live server), so
    /// `registry::aggregate_result_type("MIN"/"MAX", ValueType::Uuid)` is
    /// `None` and this is an ordinary `FunctionArgTypeMismatch`, independent
    /// of relationship shape — a plain `GROUP BY` field's own
    /// `MIN`/`MAX(<uuid column>)` is refused identically (no special
    /// to-many-only guard needed the way `text`/`enum` need, since there is
    /// no shape of this call this crate could ever admit).
    #[test]
    fn min_max_uuid_is_refused_unconditionally() {
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "worst".to_string(),
                    expr: Expr::FunctionCall {
                        name: "MAX".to_string(),
                        args: vec![col("token")],
                    },
                },
            ],
        );
        let source_columns = HashMap::from([
            ("tag".to_string(), ValueType::Numeric),
            ("token".to_string(), ValueType::Uuid),
        ]);
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::FunctionArgTypeMismatch {
                field: "worst".to_string(),
                function: "MAX".to_string(),
                arg_index: 0,
                expected: ValueType::Numeric,
                found: ValueType::Uuid,
            }
        );
    }

    /// Issue #120: `COUNT(<column>)` in a `GROUP BY` definition — counting
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
        let types = infer_field_types(&d, &source_columns, &HashMap::new()).unwrap();
        assert_eq!(
            types["n"],
            ValueType::Integer(crate::integer::IntWidth::Int8)
        );
    }

    /// A resolved *to-one* relationship whose column is read by `rel.column`.
    fn to_one_rel(
        name: &str,
        to_table: &str,
        column: &str,
    ) -> HashMap<String, ResolvedRelationship> {
        HashMap::from([(
            name.to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToOne,
                to_table: to_table.to_string(),
                to_col: "id".to_string(),
                column_types: HashMap::from([(column.to_string(), ValueType::Numeric)]),
            },
        )])
    }

    #[test]
    fn a_to_one_relationship_path_aggregated_in_a_group_by_is_accepted() {
        // Issue #94's headline shape: `SUM(post.word_count)` over a to-one
        // relationship is a LEFT JOIN followed by a GROUP BY — one related
        // value per source row, folded over the group — so it validates,
        // exactly like `SUM(<source column>)` does.
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "total_words".to_string(),
                    expr: Expr::FunctionCall {
                        name: "SUM".to_string(),
                        args: vec![Expr::RelationshipPath {
                            rel: "post".to_string(),
                            column: "word_count".to_string(),
                        }],
                    },
                },
            ],
        );
        let source_columns = numeric_columns(&["tag", "post"]);
        let relationships = to_one_rel("post", "posts", "word_count");
        assert_eq!(validate(&d, &source_columns, &relationships), Ok(()));
    }

    #[test]
    fn a_bare_to_one_relationship_path_in_a_group_by_is_rejected() {
        // Unwrapped, the path names one value per *row* of the group, with
        // nowhere to go in the group's single target row — the same objection
        // `UngroupedColumnReference` raises for a bare non-grouping-key source
        // column, reported against the path rather than a column name.
        let d = aggregate_def(
            &["tag"],
            vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: col("tag"),
                },
                FieldDef {
                    name: "wc".to_string(),
                    expr: Expr::RelationshipPath {
                        rel: "post".to_string(),
                        column: "word_count".to_string(),
                    },
                },
            ],
        );
        let source_columns = numeric_columns(&["tag", "post"]);
        let relationships = to_one_rel("post", "posts", "word_count");
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UngroupedRelationshipReference {
                field: "wc".to_string(),
                rel: "post".to_string(),
                column: "word_count".to_string(),
            }
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
        let err = validate(&d, &HashMap::new(), &HashMap::new()).unwrap_err();
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        assert_eq!(validate(&d, &source_columns, &HashMap::new()), Ok(()));
    }

    #[test]
    fn a_to_one_relationship_path_group_by_key_is_accepted() {
        // Issue #137's headline shape: `GROUP BY tag, post.author` groups
        // `post_tags` rows partly by their linked post's `author` — no field
        // needs to mention `post.author` at all, exactly like a plain
        // grouping column needs no corresponding field.
        let d = aggregate_def_with_keys(
            vec![
                GroupByKey::Column("tag".to_string()),
                GroupByKey::RelationshipPath {
                    rel: "post".to_string(),
                    column: "author".to_string(),
                },
            ],
            vec![FieldDef {
                name: "post_count".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            }],
        );
        let source_columns = numeric_columns(&["tag", "post"]);
        let mut relationships = to_one_rel("post", "posts", "author");
        relationships
            .get_mut("post")
            .unwrap()
            .column_types
            .insert("author".to_string(), ValueType::Text);
        assert_eq!(validate(&d, &source_columns, &relationships), Ok(()));
    }

    #[test]
    fn a_group_by_relationship_path_bare_passthrough_field_is_allowed() {
        // Mirrors `a_field_named_after_the_grouping_column_as_a_bare_passthrough_is_allowed`
        // for a relationship-path grouping key: a field explicitly named
        // after the key's target column, whose expression is the exact same
        // bare path, is a legal passthrough — DDL gives it no separate
        // target column of its own.
        let d = aggregate_def_with_keys(
            vec![GroupByKey::RelationshipPath {
                rel: "post".to_string(),
                column: "author".to_string(),
            }],
            vec![FieldDef {
                name: "author".to_string(),
                expr: Expr::RelationshipPath {
                    rel: "post".to_string(),
                    column: "author".to_string(),
                },
            }],
        );
        let source_columns = numeric_columns(&["post"]);
        let mut relationships = to_one_rel("post", "posts", "author");
        relationships
            .get_mut("post")
            .unwrap()
            .column_types
            .insert("author".to_string(), ValueType::Text);
        assert_eq!(validate(&d, &source_columns, &relationships), Ok(()));
    }

    #[test]
    fn a_group_by_relationship_path_field_must_be_a_bare_passthrough() {
        let d = aggregate_def_with_keys(
            vec![GroupByKey::RelationshipPath {
                rel: "post".to_string(),
                column: "author".to_string(),
            }],
            vec![FieldDef {
                name: "author".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            }],
        );
        let source_columns = numeric_columns(&["post"]);
        let mut relationships = to_one_rel("post", "posts", "author");
        relationships
            .get_mut("post")
            .unwrap()
            .column_types
            .insert("author".to_string(), ValueType::Text);
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::GroupingColumnFieldMustBePassthrough {
                field: "author".to_string(),
            }
        );
    }

    #[test]
    fn an_unknown_relationship_in_group_by_is_rejected() {
        let d = aggregate_def_with_keys(
            vec![GroupByKey::RelationshipPath {
                rel: "nope".to_string(),
                column: "author".to_string(),
            }],
            vec![FieldDef {
                name: "c".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            }],
        );
        let source_columns = numeric_columns(&["post"]);
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UnknownGroupByRelationship {
                rel: "nope".to_string(),
            }
        );
    }

    #[test]
    fn a_to_many_relationship_path_in_group_by_is_rejected() {
        // Grouping by "the many child rows on the other end of a to-many
        // relationship" has no defined semantics (issue #137's own scoping)
        // — rejected with a clear, GROUP-BY-specific error rather than the
        // field-position `RelationshipPathInAggregate`.
        let d = aggregate_def_with_keys(
            vec![GroupByKey::RelationshipPath {
                rel: "comments".to_string(),
                column: "author".to_string(),
            }],
            vec![FieldDef {
                name: "c".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            }],
        );
        let source_columns = numeric_columns(&["id"]);
        let relationships = HashMap::from([(
            "comments".to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToMany,
                to_table: "comments".to_string(),
                to_col: "post_id".to_string(),
                column_types: HashMap::from([("author".to_string(), ValueType::Text)]),
            },
        )]);
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::GroupByRelationshipMustBeToOne {
                rel: "comments".to_string(),
                column: "author".to_string(),
            }
        );
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

    fn rel_key(rel: &str, column: &str) -> GroupByKey {
        GroupByKey::RelationshipPath {
            rel: rel.to_string(),
            column: column.to_string(),
        }
    }

    /// `buyer` and `seller` both to-one onto `users`, each exposing `name`.
    fn buyer_and_seller_rels() -> HashMap<String, ResolvedRelationship> {
        let mut rels = to_one_rel("buyer", "users", "name");
        rels.extend(to_one_rel("seller", "users", "name"));
        rels
    }

    #[test]
    fn two_group_by_relationship_paths_sharing_a_column_name_are_rejected() {
        // Issue #519: `GROUP BY buyer.name, seller.name` maps both keys to a
        // target column named `name`, which used to reach Postgres as a raw
        // "column \"name\" appears twice in unique constraint" from the target DDL.
        let d = aggregate_def_with_keys(
            vec![rel_key("buyer", "name"), rel_key("seller", "name")],
            vec![count_star_field()],
        );
        let source_columns = numeric_columns(&["buyer_id", "seller_id"]);
        let err = validate(&d, &source_columns, &buyer_and_seller_rels()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::GroupByKeysShareTargetColumn {
                first: "buyer.name".to_string(),
                second: "seller.name".to_string(),
                column: "name".to_string(),
            }
        );
        let message = err.to_string();
        assert!(message.contains("'buyer.name'"), "{message}");
        assert!(message.contains("'seller.name'"), "{message}");
        assert!(message.contains("buyer.name AS buyer_name"), "{message}");
        assert_eq!(err.code(), ErrorCode::Validation);
    }

    #[test]
    fn a_group_by_relationship_path_sharing_a_plain_group_by_columns_name_is_rejected() {
        // The from-side twin the issue calls out: a plain `name` source
        // column and `buyer.name` would both become target column `name`.
        let d = aggregate_def_with_keys(
            vec![
                GroupByKey::Column("name".to_string()),
                rel_key("buyer", "name"),
            ],
            vec![count_star_field()],
        );
        let source_columns = numeric_columns(&["name", "buyer_id"]);
        let err = validate(&d, &source_columns, &buyer_and_seller_rels()).unwrap_err();
        assert_eq!(
            err,
            ValidationError::GroupByKeysShareTargetColumn {
                first: "name".to_string(),
                second: "buyer.name".to_string(),
                column: "name".to_string(),
            }
        );
    }

    #[test]
    fn a_group_by_key_listed_twice_is_rejected() {
        let d = aggregate_def_with_keys(
            vec![
                GroupByKey::Column("region".to_string()),
                GroupByKey::Column("region".to_string()),
            ],
            vec![count_star_field()],
        );
        let source_columns = numeric_columns(&["region"]);
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
    fn group_by_relationship_paths_with_distinct_column_names_still_pass() {
        let mut rels = buyer_and_seller_rels();
        rels.get_mut("seller")
            .unwrap()
            .column_types
            .insert("email".to_string(), ValueType::Numeric);
        let d = aggregate_def_with_keys(
            vec![rel_key("buyer", "name"), rel_key("seller", "email")],
            vec![count_star_field()],
        );
        let source_columns = numeric_columns(&["buyer_id", "seller_id"]);
        validate(&d, &source_columns, &rels).unwrap();
    }

    #[test]
    fn a_group_by_relationship_path_referencing_an_unknown_to_side_column_is_rejected() {
        let d = aggregate_def_with_keys(
            vec![GroupByKey::RelationshipPath {
                rel: "post".to_string(),
                column: "missing".to_string(),
            }],
            vec![FieldDef {
                name: "c".to_string(),
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: Vec::new(),
                },
            }],
        );
        let source_columns = numeric_columns(&["post"]);
        let relationships = to_one_rel("post", "posts", "word_count");
        let err = validate(&d, &source_columns, &relationships).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UnknownRelationshipColumn {
                table: "posts".to_string(),
                column: "missing".to_string(),
            }
        );
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        let err = validate(&d, &source_columns, &HashMap::new()).unwrap_err();
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
        // Issue #566: each of these named one of the hidden columns the
        // aggregate target DDL adds (`__{field}_count`, `__{field}_sum`,
        // `RECOMPUTE_LSN_COLUMN`), so validation passed and install then
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
                "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS __trellis_recompute_lsn",
                "__trellis_recompute_lsn",
            ),
        ] {
            let err = validate(&parsed(text), &source_columns, &HashMap::new()).unwrap_err();
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
    fn a_double_underscore_field_name_is_reserved_on_a_one_to_one_transform_too() {
        // The prefix is reserved uniformly rather than only where a hidden
        // column exists today, so the rule is one a user can learn once.
        let err = validate(
            &parsed("TRANSFORM t FROM s SELECT x AS __x"),
            &numeric_columns(&["x"]),
            &HashMap::new(),
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
                &HashMap::new(),
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
            &HashMap::new(),
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
        // Issue #566: the target already carries the key column, so a field of
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
}
