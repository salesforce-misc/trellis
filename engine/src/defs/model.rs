//! The definition model (issue #23): a validated [`TransformDef`] (issue
//! #22's AST) plus the identity the catalog assigns once it's persisted.

use std::collections::HashMap;

use super::ast::{RelationshipDef, TransformDef, ValueType};

/// A transform definition as stored in the catalog: the parsed AST — source
/// table, target table name, and calculated fields are all on
/// [`TransformDef`] already — plus the catalog-assigned id, the source
/// table's version at the moment this definition was created (the value
/// stage 05's version fence will read), and the source-column type map `def`
/// was validated against (issue #63's write-path gap: persisted so the
/// physical apply path — [`crate::staging::apply::compute`] — can evaluate
/// non-Numeric fields the same way [`super::catalog::create_definition`]
/// validated them, rather than re-deriving or defaulting to Numeric).
#[derive(Debug, Clone, PartialEq)]
pub struct Definition {
    pub id: i64,
    /// The physical source relation selected at definition time. Its OID is
    /// authoritative; schema/name are refreshable presentation metadata.
    pub source: SourceRelation,
    /// The physical target relation once target DDL has materialized and bound
    /// it. It is absent between definition creation and target creation.
    pub target_relation_oid: Option<u32>,
    pub source_version: i64,
    pub def: TransformDef,
    pub source_columns: HashMap<String, ValueType>,
    /// Physical bindings for source columns this definition actually reads.
    /// Empty for definitions written before ADR-0007's column binding
    /// migration, which retain legacy name-based evaluation.
    pub source_column_bindings: Vec<SourceColumnBinding>,
}

/// A transform source column's immutable PostgreSQL attribute identity and
/// its original logical DSL spelling. `attnum` survives a column rename;
/// `type_oid` and `type_modifier` fence incompatible type changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceColumnBinding {
    pub logical_name: String,
    pub source_relation_oid: u32,
    pub attnum: i16,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub value_type: ValueType,
}

/// The PostgreSQL relation a transform source is bound to. `oid` is database
/// local and stable across relation renames and schema moves; `schema` and
/// `name` describe its current catalog metadata when it was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRelation {
    /// PostgreSQL's unsigned `pg_class.oid`.
    pub oid: u32,
    pub schema: String,
    pub name: String,
}

impl SourceRelation {
    /// Current schema-qualified presentation name.
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// A relationship declaration as stored in the catalog (issue #26): the
/// parsed [`RelationshipDef`] plus the catalog-assigned id. Unlike
/// [`Definition`], there's no `source_version`/`source_columns` to carry —
/// a relationship's endpoints aren't validated against a live column-type
/// map the way a transform's source is (no source-schema DDL, ADR-0005), so
/// nothing here depends on the from-side table's version. `cardinality` is
/// [`super::catalog::create_relationship`]'s one piece of derived state
/// (issue #27): computed once, live against `pg_catalog`, at creation time
/// and persisted rather than re-introspected on every read, so a later
/// reference-time check (validating a bare-path vs. aggregate-wrapped use of
/// the relationship — deferred past issue #27, since no such reference
/// syntax resolves yet) has a stable answer even if the underlying
/// PK/`UNIQUE` index is later dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDefinition {
    pub id: i64,
    pub def: RelationshipDef,
    pub cardinality: RelationshipCardinality,
}

/// Whether a relationship's to-side is guaranteed at most one row per
/// from-row (issue #27, ADR-0006): [`RelationshipCardinality::ToOne`] iff
/// `to_col` is the sole column of a `PRIMARY KEY` or `UNIQUE` index on
/// `to_table` at the moment the relationship is declared, `ToMany`
/// otherwise. ADR-0006 requires a `ToMany` relationship's bare-path
/// references to be rejected in favor of an aggregate-wrapped form — that
/// rejection is reference-time (checked where a relationship is *used* in a
/// calculated field), and deferred past issue #27 since no such reference
/// resolves yet (see [`super::ast::Expr::RelationshipPath`]'s current
/// unconditional rejection in [`super::validate`]); this type exists now so
/// that check has something to read once it lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipCardinality {
    ToOne,
    ToMany,
}

impl RelationshipCardinality {
    /// The text this variant is persisted/queried as in
    /// `relationship_definitions.cardinality`.
    pub fn as_str(self) -> &'static str {
        match self {
            RelationshipCardinality::ToOne => "one",
            RelationshipCardinality::ToMany => "many",
        }
    }

    /// Parses [`Self::as_str`]'s persisted form back, or `None` for any
    /// other text — meaning the row was written by something other than
    /// [`super::catalog::create_relationship`]. Named `from_persisted`
    /// rather than `from_str` so it isn't confused with (and doesn't trip
    /// clippy's `should_implement_trait` lint against) `std::str::FromStr`,
    /// which this isn't — there's no matching `Err` type worth inventing
    /// for a value that should only ever come from this table's own
    /// `check` constraint.
    pub fn from_persisted(text: &str) -> Option<Self> {
        match text {
            "one" => Some(RelationshipCardinality::ToOne),
            "many" => Some(RelationshipCardinality::ToMany),
            _ => None,
        }
    }
}

/// Which role [`super::catalog::resolve_node`] is being asked to establish
/// for a table: a **source** table is one Trellis reads over logical
/// replication and never issues DDL against (ADR-0005); a **target** table
/// is one a transform declares and creates. These aren't mutually
/// exclusive on a table over its lifetime — a transform's target is a
/// completely ordinary table a *later* transform can subscribe to as its
/// source (chained/multi-hop transforms, exercised by
/// `engine/tests/apply.rs`'s two-hop propagation tests), so the same
/// physical table ends up resolved under both roles. [`SchemaNode`] tracks
/// that as two independent flags rather than one exclusive kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Source,
    Target,
}

/// A first-class identity for a table Trellis knows about — as a source, a
/// target, or (via chained transforms) both — that transforms (and, later,
/// relationships) resolve their endpoints against instead of a bare
/// table-name string. Nodes gain a physical relation OID when their relation
/// is materialized. `is_source`/`is_target` each start
/// `false` and are only ever set to `true` by [`super::catalog::resolve_node`],
/// never back to `false`. A bound node's schema/name metadata is refreshed
/// from `pg_catalog`; source columns/types remain live metadata per ADR-0005.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    pub id: i64,
    /// The bound physical relation, if it has been materialized as a source or
    /// target.
    pub relation_oid: Option<u32>,
    /// Current namespace metadata for [`Self::relation_oid`].
    pub schema_name: Option<String>,
    pub table_name: String,
    pub is_source: bool,
    pub is_target: bool,
}

/// Which kind of dependency a [`super::catalog`] edge represents (issue
/// #21). [`EdgeKind::Relationship`] is persisted by `create_relationship`
/// (issue #26); [`EdgeKind::Source`] remains the only kind a transform
/// itself persists — a transform's `FROM` is its only join input the AST
/// can produce (see `engine/src/defs/ast.rs`'s `TransformDef::source`, a
/// single `String`, no multi-source join yet). `Join` exists so the column
/// this enum backs doesn't need a migration when join-edge persistence
/// lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Source,
    Join,
    Relationship,
}

impl EdgeKind {
    /// The text this variant is persisted/queried as in `schema_edges.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeKind::Source => "source",
            EdgeKind::Join => "join",
            EdgeKind::Relationship => "relationship",
        }
    }
}

/// A directed dependency edge between two [`SchemaNode`]s: `to_node`
/// depends on `from_node` via `kind` (e.g. a `Source` edge from `orders` to
/// `order_totals` means `order_totals` is a transform target reading from
/// `orders`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaEdge {
    pub id: i64,
    pub from_node_id: i64,
    pub to_node_id: i64,
    pub kind: EdgeKind,
}
