//! Postgres-backed catalog for transform definitions (issue #23).
//!
//! **Schema choice**: a definition is stored as its original source text
//! (`transform_definitions.definition_text`) rather than a serialized AST.
//! Reading it back re-parses via [`super::parse`], reusing the grammar's
//! own parser instead of standing up a second, independently-drifting
//! serialization format for the same information. The tradeoff is a
//! re-parse on every read; given definitions are small and read
//! infrequently relative to the source-row volume they govern, that's the
//! right side to take the cost on. Flagged in the issue #23 report as the
//! open question to confirm.
//!
//! Every source table has exactly one monotonically increasing version, in
//! `source_table_versions`, that definition creation bumps in the same
//! transaction as the insert. That version lives in a real, lockable row
//! (not a computed value) so stage 05's version fence can `FOR SHARE`/
//! `FOR UPDATE` it directly.
//!
//! v1 definitions are immutable: this module only exposes creation and
//! read, no update/delete.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::pool::Pool;

use super::ast::{RelationshipDef, TransformDef, ValueType};
use super::error::ParseError;
use super::model::{
    Definition, EdgeKind, NodeKind, RelationshipCardinality, RelationshipDefinition, SchemaEdge,
    SchemaNode, SourceColumnBinding, SourceRelation,
};
use super::parser::{parse, parse_relationship};
use super::validate::{ValidationError, validate};

/// Why creating or reading a definition failed.
#[derive(Debug)]
pub enum CatalogError {
    /// The source text failed to parse (issue #22's grammar).
    Parse(ParseError),
    /// The parsed definition failed validation (issue #23).
    Validate(ValidationError),
    /// Acquiring a connection or running a query against Postgres failed.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// A definition's persisted `source_columns` jsonb held a value other
    /// than `"numeric"`/`"text"`/`"boolean"`/`"uuid"` for some column —
    /// meaning the row was written by something other than
    /// [`create_definition`], since that's the only writer and it only ever
    /// encodes [`ValueType`]'s variants.
    UnknownValueType { column: String, text: String },
    /// The definition's initial backfill (issue #23) failed to enumerate its
    /// source table.
    Backfill(crate::intake::IntakeError),
    /// PostgreSQL could not resolve the source spelling through the definition
    /// connection's normal `search_path`.
    SourceRelationNotFound { source: String },
    /// PostgreSQL could not resolve the target table in its configured schema.
    TargetRelationNotFound { target: String },
    /// A definition already points at a different physical target relation.
    TargetRelationMismatch {
        target: String,
        expected_oid: u32,
        actual_oid: u32,
    },
    /// A definition remains in the catalog, but its OID-bound source relation
    /// has been dropped and must not be silently omitted from lookups.
    BoundSourceRelationMissing { oid: u32 },
    /// A source column declared by a caller is absent from the actual bound
    /// relation, so a caller-provided type map cannot invent source columns.
    SourceColumnNotFound { relation_oid: u32, column: String },
    /// PostgreSQL's physical type cannot be represented by the DSL's current
    /// four-value type model.
    UnsupportedSourceColumnType {
        relation_oid: u32,
        column: String,
        type_oid: u32,
    },
    /// The caller's logical source type disagrees with the live physical
    /// source column type.
    SourceColumnTypeMismatch {
        relation_oid: u32,
        column: String,
        expected: ValueType,
        actual: ValueType,
    },
    /// A persisted binding no longer identifies a live, non-dropped source
    /// attribute.
    BoundSourceColumnMissing {
        relation_oid: u32,
        attnum: i16,
        logical_name: String,
    },
    /// A persisted attribute's type changed after the definition was stored.
    BoundSourceColumnIncompatible {
        relation_oid: u32,
        attnum: i16,
        logical_name: String,
        expected_type_oid: u32,
        expected_type_modifier: i32,
        actual_type_oid: u32,
        actual_type_modifier: i32,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::Parse(err) => write!(f, "failed to parse transform definition: {err}"),
            CatalogError::Validate(err) => {
                write!(f, "definition failed validation: {err}")
            }
            CatalogError::Db(err) => {
                write!(f, "transform catalog database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            CatalogError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            CatalogError::UnknownValueType { column, text } => write!(
                f,
                "column '{column}' has an unrecognized persisted value type '{text}'"
            ),
            CatalogError::Backfill(err) => {
                write!(f, "failed to backfill the definition's source table: {err}")
            }
            CatalogError::SourceRelationNotFound { source } => {
                write!(
                    f,
                    "source relation '{source}' does not exist or is not visible on search_path"
                )
            }
            CatalogError::TargetRelationNotFound { target } => {
                write!(f, "target relation '{target}' does not exist")
            }
            CatalogError::TargetRelationMismatch {
                target,
                expected_oid,
                actual_oid,
            } => {
                write!(
                    f,
                    "target relation '{target}' changed from OID {expected_oid} to {actual_oid}"
                )
            }
            CatalogError::BoundSourceRelationMissing { oid } => {
                write!(f, "bound source relation OID {oid} no longer exists")
            }
            CatalogError::SourceColumnNotFound {
                relation_oid,
                column,
            } => write!(
                f,
                "source relation OID {relation_oid} has no column '{column}'"
            ),
            CatalogError::UnsupportedSourceColumnType {
                relation_oid,
                column,
                type_oid,
            } => write!(
                f,
                "source column '{column}' on relation OID {relation_oid} has unsupported PostgreSQL type OID {type_oid}"
            ),
            CatalogError::SourceColumnTypeMismatch {
                relation_oid,
                column,
                expected,
                actual,
            } => write!(
                f,
                "source column '{column}' on relation OID {relation_oid} is {actual}, not the declared {expected}"
            ),
            CatalogError::BoundSourceColumnMissing {
                relation_oid,
                attnum,
                logical_name,
            } => write!(
                f,
                "bound source column '{logical_name}' at relation OID {relation_oid} attnum {attnum} is missing or dropped"
            ),
            CatalogError::BoundSourceColumnIncompatible {
                relation_oid,
                attnum,
                logical_name,
                expected_type_oid,
                expected_type_modifier,
                actual_type_oid,
                actual_type_modifier,
            } => write!(
                f,
                "bound source column '{logical_name}' at relation OID {relation_oid} attnum {attnum} changed type from OID {expected_type_oid} modifier {expected_type_modifier} to OID {actual_type_oid} modifier {actual_type_modifier}"
            ),
        }
    }
}

impl std::error::Error for CatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CatalogError::Parse(err) => Some(err),
            CatalogError::Validate(err) => Some(err),
            CatalogError::Db(err) => Some(err),
            CatalogError::Pool(err) => Some(err),
            CatalogError::UnknownValueType { .. } => None,
            CatalogError::Backfill(err) => Some(err),
            CatalogError::SourceRelationNotFound { .. } => None,
            CatalogError::TargetRelationNotFound { .. } => None,
            CatalogError::TargetRelationMismatch { .. } => None,
            CatalogError::BoundSourceRelationMissing { .. } => None,
            CatalogError::SourceColumnNotFound { .. }
            | CatalogError::UnsupportedSourceColumnType { .. }
            | CatalogError::SourceColumnTypeMismatch { .. }
            | CatalogError::BoundSourceColumnMissing { .. }
            | CatalogError::BoundSourceColumnIncompatible { .. } => None,
        }
    }
}

impl From<ParseError> for CatalogError {
    fn from(err: ParseError) -> Self {
        CatalogError::Parse(err)
    }
}

impl From<ValidationError> for CatalogError {
    fn from(err: ValidationError) -> Self {
        CatalogError::Validate(err)
    }
}

impl From<tokio_postgres::Error> for CatalogError {
    fn from(err: tokio_postgres::Error) -> Self {
        CatalogError::Db(err)
    }
}

impl From<crate::error::Error> for CatalogError {
    fn from(err: crate::error::Error) -> Self {
        CatalogError::Pool(err)
    }
}

impl From<crate::intake::IntakeError> for CatalogError {
    fn from(err: crate::intake::IntakeError) -> Self {
        CatalogError::Backfill(err)
    }
}

/// Parses, validates, and stores a new transform definition, bumping its
/// source table's version in the same transaction. `source_columns` maps the
/// definition's source table's known columns to their [`ValueType`] (see
/// [`super::validate::validate`] — introspecting a live Postgres schema for
/// this is intake's concern, out of scope here).
pub async fn create_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;
    validate(&def, source_columns)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Resolve the user spelling once through this transaction's normal
    // search_path. The resulting OID is the definition's stable source
    // identity; schema/name are only current presentation metadata.
    let source = resolve_source_relation_in_txn(&txn, &def.source).await?;
    let source_column_bindings =
        bind_source_columns_in_txn(&txn, &source, &def, source_columns).await?;

    // Issue #20: every definition's source and target resolve to a
    // first-class `SchemaNode`, created on first reference (a source node
    // the moment something first transforms it; a target node the moment
    // its owning definition is created) — a side effect alongside the
    // catalog writes below rather than a change to `TransformDef`'s shape,
    // per the issue's "prefer the smaller change" guidance.
    let source_node = resolve_source_node_in_txn(&txn, &source, NodeKind::Source).await?;
    let target_node = resolve_node_in_txn(&txn, &def.target, NodeKind::Target).await?;

    // Issue #22 (generalized): reject this definition if the `Source` edge
    // it's about to add — def.source -> def.target — would close a cycle in
    // the table-level dependency graph, transitively through any edges
    // already persisted. Checked against the transaction's own view of
    // `schema_edges` so it sees the graph exactly as it will look right up
    // to (but not including) the edge this definition is about to add.
    // Known v1 limitation: under Postgres's default read-committed
    // isolation, two concurrent `create_definition` calls (e.g. one adding
    // a->b, another adding b->a) can each pass this check before either
    // commits — nothing here serializes them (no advisory lock/
    // SERIALIZABLE) — so a cycle could theoretically still persist. Same
    // class of race as any check-then-insert pattern; accepted for now,
    // out of scope for this issue.
    reject_if_table_cycle(&txn, source_node.id, target_node.id).await?;

    // Issue #21: a transform's `FROM` is a `Source` dependency edge from its
    // source node to its target node — persisted alongside the node
    // resolutions above so `dependents_of` can walk the graph instead of
    // matching on `transform_definitions.source_table` string equality.
    persist_edge_in_txn(&txn, source_node.id, target_node.id, EdgeKind::Source).await?;

    // Issue #23: a definition's initial backfill is one enumeration of its
    // source table, staged as `Recompute` triggers into the active ring
    // segment via the same append path CDC/reverse-propagation use — one
    // call here regardless of how many calculated fields the definition
    // declares, not one per field, preserving the "N columns, one backfill"
    // property as the definition model becomes first-class. This uses the
    // binding resolved above rather than resolving `def.source` again.
    crate::intake::publication::enumerate_and_append(&txn, &source).await?;

    let version: i64 = txn
        .query_one(
            "insert into source_table_versions (source_table, source_relation_oid, version)
             values ($1, $2::oid, 1)
             on conflict (source_relation_oid) where source_relation_oid is not null
             do update set version = source_table_versions.version + 1
             returning version",
            &[&def.source, &source.oid],
        )
        .await?
        .get(0);

    let (type_keys, type_vals) = encode_type_map(source_columns);

    let id: i64 = txn
        .query_one(
            "insert into transform_definitions
                (target_table, source_table, source_relation_oid, source_version, definition_text, source_columns)
             values ($1, $2, $3::oid, $4, $5, jsonb_object($6::text[], $7::text[]))
             returning id",
            &[
                &def.target,
                &def.source,
                &source.oid,
                &version,
                &source_text,
                &type_keys,
                &type_vals,
            ],
        )
        .await?
        .get(0);

    for binding in &source_column_bindings {
        txn.execute(
            "insert into transform_source_columns
                 (definition_id, logical_name, source_relation_oid, attnum, type_oid, type_modifier, value_type)
             values ($1, $2, $3::oid, $4, $5::oid, $6, $7)",
            &[
                &id,
                &binding.logical_name,
                &binding.source_relation_oid,
                &binding.attnum,
                &binding.type_oid,
                &binding.type_modifier,
                &value_type_text(binding.value_type),
            ],
        )
        .await?;
    }

    txn.commit().await?;

    Ok(Definition {
        id,
        source,
        target_relation_oid: None,
        source_version: version,
        def,
        source_columns: source_columns.clone(),
        source_column_bindings,
    })
}

/// Binds a definition's target to the table just materialized by target DDL.
/// `target_schema` is intentionally a separate argument from the definition's
/// bare target name: never let a same-named table earlier on `search_path`
/// become this transform's target.
pub async fn bind_target_relation(
    pool: &Pool,
    target_table: &str,
    target_schema: &str,
) -> Result<SourceRelation, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let target = resolve_qualified_relation_in_txn(&txn, target_schema, target_table)
        .await?
        .ok_or_else(|| CatalogError::TargetRelationNotFound {
            target: format!("{target_schema}.{target_table}"),
        })?;
    let existing_oid = txn
        .query_opt(
            "select target_relation_oid from transform_definitions where target_table = $1",
            &[&target_table],
        )
        .await?;
    let Some(existing_oid) = existing_oid else {
        txn.commit().await?;
        return Ok(target);
    };
    let existing_oid: Option<u32> = existing_oid.get(0);
    if let Some(expected_oid) = existing_oid.filter(|oid| *oid != target.oid) {
        return Err(CatalogError::TargetRelationMismatch {
            target: target_table.to_string(),
            expected_oid,
            actual_oid: target.oid,
        });
    }
    txn.execute(
        "update transform_definitions set target_relation_oid = $1::oid where target_table = $2",
        &[&target.oid, &target_table],
    )
    .await?;
    resolve_relation_node_in_txn(&txn, &target, NodeKind::Target).await?;
    txn.commit().await?;
    Ok(target)
}

/// Returns a target's bound physical identity. Unlike [`resolve_source_relation`],
/// this consults the transform catalog rather than the caller's `search_path`.
pub async fn target_relation_oid(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<u32>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select target_relation_oid from transform_definitions where target_table = $1",
            &[&target_table],
        )
        .await?;
    Ok(row.and_then(|row| row.get(0)))
}

/// Parses, validates, and stores a new relationship declaration (issue #26
/// storage, issue #27 validation, ADR-0006): resolves/creates `schema_nodes`
/// for both endpoints, persists a `schema_edges` row from `from_table` to
/// `to_table` tagged [`EdgeKind::Relationship`], and inserts the immutable
/// `relationship_definitions` row — all in one transaction, mirroring
/// [`create_definition`]'s pattern.
///
/// Validates, in order: both endpoints' `table.column` exist and resolve to
/// comparable Postgres types ([`column_type_in_txn`] /
/// [`assert_comparable_types`], ADR-0006's "type-check the join"); the
/// relationship's name is not already declared on `from_table`
/// ([`ValidationError::DuplicateRelationshipName`], a friendlier
/// definition-time surfacing of the same rule
/// `relationship_definitions_from_table_name_key` backstops at the DB
/// level); and the new `Relationship` edge would not close a cycle
/// ([`reject_if_table_cycle`], generalized unchanged from
/// [`create_definition`]'s `Source`-edge use). Cardinality
/// ([`RelationshipCardinality`]) is determined via
/// [`to_col_cardinality_in_txn`] and persisted, not rejected on — ADR-0006's
/// "a to-many reference must be aggregate-wrapped" rule is a *reference*-time
/// check (validating how a relationship is *used* in a calculated field),
/// deferred past this issue since no such reference resolves yet (see
/// [`super::ast::Expr::RelationshipPath`]).
///
/// Node-kind resolution: a relationship's endpoints may each be "a source
/// table or a transform target, in any combination" (ADR-0006), and nothing
/// here can tell which without cross-referencing `transform_definitions`.
/// Both endpoints resolve as [`NodeKind::Source`] — directionally accurate
/// either way, since computing the join reads both tables' columns
/// regardless of whether one side later turns out to also be a transform
/// target — and [`resolve_node_in_txn`]'s flags are additive (OR'd in, never
/// cleared), so a later `create_definition` call that resolves the same
/// table as [`NodeKind::Target`] merges into the same node rather than
/// conflicting with this choice.
pub async fn create_relationship(
    pool: &Pool,
    source_text: &str,
) -> Result<RelationshipDefinition, CatalogError> {
    let def: RelationshipDef = parse_relationship(source_text)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let from_type = column_type_in_txn(&txn, &def.from_table, &def.from_col).await?;
    let to_type = column_type_in_txn(&txn, &def.to_table, &def.to_col).await?;
    assert_comparable_types(&def, &from_type, &to_type)?;

    let already_declared: bool = txn
        .query_one(
            "select exists (
                select 1 from relationship_definitions where from_table = $1 and name = $2
             )",
            &[&def.from_table, &def.name],
        )
        .await?
        .get(0);
    if already_declared {
        return Err(ValidationError::DuplicateRelationshipName {
            from_table: def.from_table.clone(),
            name: def.name.clone(),
        }
        .into());
    }

    let from_node = resolve_node_in_txn(&txn, &def.from_table, NodeKind::Source).await?;
    // `to_table` is marked `is_source` here too, even though a relationship's
    // to-side is often really a transform target: the flag is additive/OR'd
    // (a later `create_definition` call can still set `is_target` on the
    // same node), and today's only `is_source` reader — `all_source_tables`,
    // the publication feeder — reads `transform_definitions`, not this flag,
    // so no consumer is misled. If a future `is_source` consumer reads
    // `schema_nodes` directly, re-check this call.
    let to_node = resolve_node_in_txn(&txn, &def.to_table, NodeKind::Source).await?;

    // The `Relationship` edge is persisted `to_table -> from_table` (parent
    // -> child), matching `Source`'s "to_node depends on from_node"
    // convention (see `SchemaEdge`'s doc comment): the FK-holding
    // `from_table` is the dependent side — a bare-path reference like
    // `product.x` in a calculated field over `from_table` pulls from
    // `to_table`, so `from_table` depends on `to_table`, not the reverse.
    // Persisting it `from_table -> to_table` instead (the naive reading of
    // "FROM ... TO ...") would invert that: it'd wrongly reject a
    // target-table-references-its-own-source relationship as a false
    // 2-cycle (both edges actually mean "target depends on source"), and it
    // would make future dependents-of-a-changed-table traversals (#28+)
    // miss relationship dependents, since `edges_from(to_table)` wouldn't
    // reach `from_table` at all.
    reject_if_table_cycle(&txn, to_node.id, from_node.id).await?;

    persist_edge_in_txn(&txn, to_node.id, from_node.id, EdgeKind::Relationship).await?;

    let cardinality = to_col_cardinality_in_txn(&txn, &def.to_table, &def.to_col).await?;

    let id: i64 = txn
        .query_one(
            "insert into relationship_definitions
                (name, from_table, from_col, to_table, to_col, definition_text, cardinality)
             values ($1, $2, $3, $4, $5, $6, $7)
             returning id",
            &[
                &def.name,
                &def.from_table,
                &def.from_col,
                &def.to_table,
                &def.to_col,
                &source_text,
                &cardinality.as_str(),
            ],
        )
        .await?
        .get(0);

    txn.commit().await?;

    Ok(RelationshipDefinition {
        id,
        def,
        cardinality,
    })
}

/// Reads back the relationship named `name` declared on `from_table` — the
/// pair a [`super::ast::Expr::RelationshipPath`]'s `rel` head resolves
/// against (ADR-0006: a relationship name is unique per from-table, not
/// global, so both are needed to identify one row). Re-parses the persisted
/// `definition_text` rather than reconstructing [`RelationshipDef`] from the
/// denormalized columns, matching [`dependents_of`]'s "reuse the grammar's
/// own parser" convention; `cardinality` is read back from its own column
/// instead, since it isn't part of the source text (issue #27: it's derived,
/// not declared).
pub async fn relationship_by_name(
    pool: &Pool,
    from_table: &str,
    name: &str,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, definition_text, cardinality
             from relationship_definitions
             where from_table = $1 and name = $2",
            &[&from_table, &name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };

    let id: i64 = row.get(0);
    let text: String = row.get(1);
    let cardinality_text: String = row.get(2);
    let def = parse_relationship(&text)?;
    let cardinality =
        RelationshipCardinality::from_persisted(&cardinality_text).unwrap_or_else(|| {
            panic!(
                "relationship_definitions.cardinality held unrecognized value '{cardinality_text}'"
            )
        });
    Ok(Some(RelationshipDefinition {
        id,
        def,
        cardinality,
    }))
}

/// The Postgres type of `table.column`, as rendered by `format_type`, via a
/// bound `::regclass` cast (matching [`super::ddl::source_primary_key`]'s
/// convention) rather than string-interpolating either name into the query.
/// Distinguishes "the table itself doesn't resolve" from "the table exists
/// but has no such column" only in that both are reported the same way
/// (issue #27 doesn't need the distinction: either one means the endpoint
/// isn't real) — see [`ValidationError::UnknownRelationshipColumn`].
async fn column_type_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
    column: &str,
) -> Result<String, CatalogError> {
    let row = txn
        .query_opt(
            "select pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&table, &column],
        )
        .await?;
    match row {
        Some(row) => Ok(row.get(0)),
        None => Err(ValidationError::UnknownRelationshipColumn {
            table: table.to_string(),
            column: column.to_string(),
        }
        .into()),
    }
}

/// Postgres type names that are freely joinable despite not being textually
/// identical — the common case of an identity primary key (`bigint`) and a
/// foreign key column declared as a plain `integer`, or a `text`/`character
/// varying` split between two independently-authored tables. Anything not
/// named here must match `from_type`/`to_type` exactly to be considered
/// comparable; see [`assert_comparable_types`].
///
/// `pg_type` is [`column_type_in_txn`]'s `format_type(atttypid, atttypmod)`
/// rendering, which includes any length/precision modifier (`character
/// varying(255)`, `numeric(10,2)`). The modifier is stripped before bucket
/// matching — otherwise `varchar(255)` and `varchar(100)`, or `text` and
/// `varchar(n)`, would fall into the `other` catch-all as two distinct
/// strings and be wrongly rejected as a type mismatch, even though they're
/// exactly the kind of join this function exists to allow.
fn type_family(pg_type: &str) -> &str {
    let base = pg_type.split('(').next().unwrap_or(pg_type).trim();
    match base {
        "smallint" | "integer" | "bigint" => "integer",
        "numeric" | "real" | "double precision" => "numeric",
        "text" | "character varying" | "character" => "text",
        other => other,
    }
}

/// Rejects `def` if `from_type`/`to_type` (both already resolved by
/// [`column_type_in_txn`]) aren't in the same [`type_family`] — ADR-0006's
/// "type-check the join" requirement.
fn assert_comparable_types(
    def: &RelationshipDef,
    from_type: &str,
    to_type: &str,
) -> Result<(), CatalogError> {
    if type_family(from_type) == type_family(to_type) {
        return Ok(());
    }
    Err(ValidationError::RelationshipTypeMismatch {
        name: def.name.clone(),
        from_table: def.from_table.clone(),
        from_col: def.from_col.clone(),
        from_type: from_type.to_string(),
        to_table: def.to_table.clone(),
        to_col: def.to_col.clone(),
        to_type: to_type.to_string(),
    }
    .into())
}

/// [`RelationshipCardinality::ToOne`] iff `to_col` is the sole column of a
/// `PRIMARY KEY` or `UNIQUE` index on `to_table`, introspected live against
/// `pg_catalog` (`pg_index.indisunique` covers both index kinds; `indkey`'s
/// length excludes any multi-column index `to_col` merely participates in,
/// since that doesn't make `to_col` alone unique) — ADR-0006's cardinality
/// rule. `indisvalid`/`indpred is null` exclude indexes that don't actually
/// guarantee global uniqueness of `to_col`: a not-yet-validated index (e.g.
/// left behind by a failed `CREATE UNIQUE INDEX CONCURRENTLY`) or a partial
/// unique index (`... where active`), which only constrains the rows it
/// covers. Assumes `to_table`/`to_col` already resolved (callers run this
/// after [`column_type_in_txn`] has confirmed both exist).
async fn to_col_cardinality_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    to_table: &str,
    to_col: &str,
) -> Result<RelationshipCardinality, CatalogError> {
    let is_unique: bool = txn
        .query_one(
            "select exists (
                select 1
                from pg_index i
                join pg_attribute a
                  on a.attrelid = i.indrelid and a.attname = $2
                where i.indrelid = pg_catalog.to_regclass($1)
                  and i.indisunique
                  and i.indisvalid
                  and i.indpred is null
                  and array_length(i.indkey::int2[], 1) = 1
                  and i.indkey[0] = a.attnum
             )",
            &[&to_table, &to_col],
        )
        .await?
        .get(0);

    Ok(if is_unique {
        RelationshipCardinality::ToOne
    } else {
        RelationshipCardinality::ToMany
    })
}

/// Resolves `table_name` to its [`SchemaNode`], creating one if this is the
/// first time Trellis has seen the table and setting its `kind` role flag
/// (`is_source`/`is_target`) to `true`. Idempotent, and additive across
/// roles: resolving the same table under both [`NodeKind::Source`] and
/// [`NodeKind::Target`] over separate calls (chained/multi-hop transforms —
/// see [`super::model::NodeKind`]'s doc comment) merges into one node with
/// both flags set, rather than erroring.
///
/// Runs in its own transaction; [`create_definition`] instead calls
/// [`resolve_node_in_txn`] directly so both of a definition's node
/// resolutions land in the same transaction as the definition write.
pub async fn resolve_node(
    pool: &Pool,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let node = resolve_node_in_txn(&txn, table_name, kind).await?;
    txn.commit().await?;
    Ok(node)
}

/// Resolves a source spelling through PostgreSQL's normal `search_path` and
/// returns the physical relation plus its current presentation metadata.
pub async fn resolve_source_relation(
    pool: &Pool,
    source: &str,
) -> Result<SourceRelation, CatalogError> {
    let client = pool.get().await?;
    if let Some(oid) = bound_target_oid_in_client(&*client, source).await? {
        return source_relation_by_oid_from_client(&client, oid)
            .await?
            .ok_or(CatalogError::BoundSourceRelationMissing { oid });
    }
    let row = client
        .query_opt(
            "select c.oid, n.nspname::text, c.relname::text
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where c.oid = pg_catalog.to_regclass($1)",
            &[&source],
        )
        .await?;
    row.map(source_relation_from_row)
        .ok_or_else(|| CatalogError::SourceRelationNotFound {
            source: source.to_string(),
        })
}

/// Resolves an already-bound source OID to its current schema and relation
/// name. This refreshes presentation metadata after a rename or schema move
/// without changing the catalog identity.
pub async fn source_relation_by_oid(
    pool: &Pool,
    oid: u32,
) -> Result<Option<SourceRelation>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select c.oid, n.nspname::text, c.relname::text
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where c.oid = $1::oid",
            &[&oid],
        )
        .await?;
    Ok(row.map(source_relation_from_row))
}

fn source_relation_from_row(row: tokio_postgres::Row) -> SourceRelation {
    SourceRelation {
        oid: row.get(0),
        schema: row.get(1),
        name: row.get(2),
    }
}

async fn resolve_source_relation_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    source: &str,
) -> Result<SourceRelation, CatalogError> {
    if let Some(oid) = bound_target_oid_in_txn(txn, source).await? {
        return source_relation_by_oid_in_txn(txn, oid)
            .await?
            .ok_or(CatalogError::BoundSourceRelationMissing { oid });
    }
    let row = txn
        .query_opt(
            "select c.oid, n.nspname::text, c.relname::text
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where c.oid = pg_catalog.to_regclass($1)",
            &[&source],
        )
        .await?;
    row.map(source_relation_from_row)
        .ok_or_else(|| CatalogError::SourceRelationNotFound {
            source: source.to_string(),
        })
}

/// A bound transform target wins over a same-named relation on `search_path`.
/// This keeps a configured target usable as a chained source even when the
/// Trellis schema itself contains a colliding relation name.
async fn bound_target_oid_in_client(
    client: &tokio_postgres::Client,
    table_name: &str,
) -> Result<Option<u32>, CatalogError> {
    let row = client
        .query_opt(
            "select target_relation_oid from transform_definitions
             where target_table = $1 and target_relation_oid is not null",
            &[&table_name],
        )
        .await?;
    Ok(row.and_then(|row| row.get(0)))
}

async fn bound_target_oid_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table_name: &str,
) -> Result<Option<u32>, CatalogError> {
    let row = txn
        .query_opt(
            "select target_relation_oid from transform_definitions
             where target_table = $1 and target_relation_oid is not null",
            &[&table_name],
        )
        .await?;
    Ok(row.and_then(|row| row.get(0)))
}

async fn source_relation_by_oid_from_client(
    client: &tokio_postgres::Client,
    oid: u32,
) -> Result<Option<SourceRelation>, CatalogError> {
    let row = client
        .query_opt(
            "select c.oid, n.nspname::text, c.relname::text
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where c.oid = $1::oid",
            &[&oid],
        )
        .await?;
    Ok(row.map(source_relation_from_row))
}

async fn source_relation_by_oid_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    oid: u32,
) -> Result<Option<SourceRelation>, CatalogError> {
    let row = txn
        .query_opt(
            "select c.oid, n.nspname::text, c.relname::text
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where c.oid = $1::oid",
            &[&oid],
        )
        .await?;
    Ok(row.map(source_relation_from_row))
}

/// Resolves a name in an explicitly selected namespace. `to_regclass` cannot
/// bind a schema name separately, so use the catalog relation lookup instead.
async fn resolve_qualified_relation_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    name: &str,
) -> Result<Option<SourceRelation>, CatalogError> {
    let row = txn
        .query_opt(
            "select c.oid, n.nspname::text, c.relname::text
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&schema, &name],
        )
        .await?;
    Ok(row.map(source_relation_from_row))
}

/// Validates every caller-declared source column against the live relation,
/// then records bindings only for columns the transform actually reads. The
/// public map remains the validator's compatibility API; it cannot invent a
/// source column that PostgreSQL does not have.
async fn bind_source_columns_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    source: &SourceRelation,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Vec<SourceColumnBinding>, CatalogError> {
    let mut physical_columns: HashMap<String, SourceColumnBinding> = HashMap::new();
    for (name, expected) in source_columns {
        let binding = source_column_in_txn(txn, source.oid, name).await?;
        if binding.value_type != *expected {
            return Err(CatalogError::SourceColumnTypeMismatch {
                relation_oid: source.oid,
                column: name.clone(),
                expected: *expected,
                actual: binding.value_type,
            });
        }
        physical_columns.insert(name.clone(), binding);
    }

    let mut needed = HashSet::new();
    if let super::ast::KeySpace::Aggregate { group_by } = &def.key_space {
        needed.extend(group_by.iter().cloned());
    }
    let field_names: HashSet<&str> = def.fields.iter().map(|field| field.name.as_str()).collect();
    for field in &def.fields {
        let mut references = Vec::new();
        collect_source_column_references(&field.expr, &mut references);
        needed.extend(references.into_iter().filter(|name| {
            source_columns.contains_key(name)
                && (name == &field.name || !field_names.contains(name.as_str()))
        }));
    }

    let mut bindings: Vec<SourceColumnBinding> = needed
        .into_iter()
        .map(|name| {
            physical_columns
                .remove(&name)
                .expect("validated source column reference must be declared")
        })
        .collect();
    bindings.sort_by(|a, b| a.logical_name.cmp(&b.logical_name));
    Ok(bindings)
}

fn collect_source_column_references(expr: &super::ast::Expr, out: &mut Vec<String>) {
    match expr {
        super::ast::Expr::Column(name) => out.push(name.clone()),
        super::ast::Expr::NumberLiteral(_)
        | super::ast::Expr::StringLiteral(_)
        | super::ast::Expr::RelationshipPath { .. } => {}
        super::ast::Expr::BinaryOp { lhs, rhs, .. } => {
            collect_source_column_references(lhs, out);
            collect_source_column_references(rhs, out);
        }
        super::ast::Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_source_column_references(arg, out);
            }
        }
    }
}

async fn source_column_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    relation_oid: u32,
    logical_name: &str,
) -> Result<SourceColumnBinding, CatalogError> {
    let row = txn
        .query_opt(
            "select a.attnum, a.atttypid, a.atttypmod, t.typname::text, t.typcategory::text
             from pg_catalog.pg_attribute a
             join pg_catalog.pg_type t on t.oid = a.atttypid
             where a.attrelid = $1::oid
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&relation_oid, &logical_name],
        )
        .await?;
    let Some(row) = row else {
        return Err(CatalogError::SourceColumnNotFound {
            relation_oid,
            column: logical_name.to_string(),
        });
    };
    let type_oid: u32 = row.get(1);
    let type_name: String = row.get(3);
    let type_category: String = row.get(4);
    let Some(value_type) = physical_value_type(&type_name, &type_category) else {
        return Err(CatalogError::UnsupportedSourceColumnType {
            relation_oid,
            column: logical_name.to_string(),
            type_oid,
        });
    };
    Ok(SourceColumnBinding {
        logical_name: logical_name.to_string(),
        source_relation_oid: relation_oid,
        attnum: row.get(0),
        type_oid,
        type_modifier: row.get(2),
        value_type,
    })
}

fn physical_value_type(type_name: &str, type_category: &str) -> Option<ValueType> {
    match (type_name, type_category) {
        ("bool", _) => Some(ValueType::Boolean),
        ("uuid", _) => Some(ValueType::Uuid),
        ("text" | "varchar" | "bpchar", _) => Some(ValueType::Text),
        (_, "N") => Some(ValueType::Numeric),
        _ => None,
    }
}

fn value_type_text(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
    }
}

/// Resolves a definition's bound source attributes to their current names.
/// This is deliberately a definition-scoped lookup: a single staged source
/// image may feed definitions created before and after different renames.
pub async fn source_column_current_names(
    pool: &Pool,
    definition: &Definition,
) -> Result<HashMap<String, String>, CatalogError> {
    let client = pool.get().await?;
    let mut names = HashMap::with_capacity(definition.source_column_bindings.len());
    for binding in &definition.source_column_bindings {
        let row = client
            .query_opt(
                "select attname::text, atttypid, atttypmod
                 from pg_catalog.pg_attribute
                 where attrelid = $1::oid
                   and attnum = $2
                   and attnum > 0
                   and not attisdropped",
                &[&binding.source_relation_oid, &binding.attnum],
            )
            .await?;
        let Some(row) = row else {
            return Err(CatalogError::BoundSourceColumnMissing {
                relation_oid: binding.source_relation_oid,
                attnum: binding.attnum,
                logical_name: binding.logical_name.clone(),
            });
        };
        let actual_type_oid: u32 = row.get(1);
        let actual_type_modifier: i32 = row.get(2);
        if actual_type_oid != binding.type_oid || actual_type_modifier != binding.type_modifier {
            return Err(CatalogError::BoundSourceColumnIncompatible {
                relation_oid: binding.source_relation_oid,
                attnum: binding.attnum,
                logical_name: binding.logical_name.clone(),
                expected_type_oid: binding.type_oid,
                expected_type_modifier: binding.type_modifier,
                actual_type_oid,
                actual_type_modifier,
            });
        }
        names.insert(binding.logical_name.clone(), row.get(0));
    }
    Ok(names)
}

/// The transactional core of [`resolve_node`] — see its doc comment.
/// Upserts `table_name`, OR-ing `kind`'s role flag into whatever the row
/// already has (or defaulting the other flag `false` if the row is new).
async fn resolve_node_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let (is_source, is_target) = match kind {
        NodeKind::Source => (true, false),
        NodeKind::Target => (false, true),
    };

    // A target name may already identify a bound source node (notably when a
    // cycle points back to an existing source). Reuse that graph identity
    // rather than split the graph into a second name-only node. Do not
    // resolve arbitrary target names through `search_path`: target DDL later
    // supplies the authoritative configured-schema OID.
    if kind == NodeKind::Target {
        if let Some(row) = txn
            .query_opt(
                "select sn.id, sn.relation_oid, sn.schema_name, sn.table_name, sn.is_source, sn.is_target
                 from schema_nodes sn
                 where sn.table_name = $1
                   and sn.relation_oid is not null
                   and sn.is_source
                 limit 1",
                &[&table_name],
            )
            .await?
        {
            let id: i64 = row.get(0);
            let row = txn
                .query_one(
                    "update schema_nodes set is_target = true
                     where id = $1
                     returning id, relation_oid, schema_name, table_name, is_source, is_target",
                    &[&id],
                )
                .await?;
            return Ok(schema_node_from_row(row));
        }
    }

    let row = txn
        .query_one(
            "insert into schema_nodes (table_name, is_source, is_target)
             values ($1, $2, $3)
             on conflict (table_name) where relation_oid is null do update
                set is_source = schema_nodes.is_source or excluded.is_source,
                    is_target = schema_nodes.is_target or excluded.is_target
             returning id, relation_oid, schema_name, table_name, is_source, is_target",
            &[&table_name, &is_source, &is_target],
        )
        .await?;

    Ok(schema_node_from_row(row))
}

/// Inserts or refreshes the OID-backed graph node for a transform source.
async fn resolve_source_node_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    source: &SourceRelation,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    resolve_relation_node_in_txn(txn, source, kind).await
}

/// Resolves one physical relation to its graph node. Target binding promotes
/// the definition-time name node into this OID node; if the same table was
/// already independently bound as a source, all edges are redirected before
/// removing the pending node.
async fn resolve_relation_node_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    relation: &SourceRelation,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let (is_source, is_target) = match kind {
        NodeKind::Source => (true, false),
        NodeKind::Target => (false, true),
    };

    let bound = txn
        .query_opt(
            "select id, relation_oid, schema_name, table_name, is_source, is_target
             from schema_nodes where relation_oid = $1::oid",
            &[&relation.oid],
        )
        .await?;
    let pending_target = if kind == NodeKind::Target {
        txn.query_opt(
            "select id from schema_nodes
             where relation_oid is null and table_name = $1 and is_target",
            &[&relation.name],
        )
        .await?
    } else {
        None
    };

    if let (Some(bound), Some(pending)) = (&bound, &pending_target) {
        let bound_id: i64 = bound.get(0);
        let pending_id: i64 = pending.get(0);
        if bound_id != pending_id {
            merge_schema_nodes_in_txn(txn, bound_id, pending_id).await?;
        }
    }

    let row = match bound {
        Some(row) => {
            let id: i64 = row.get(0);
            txn.query_one(
                "update schema_nodes
                 set schema_name = $2, table_name = $3,
                     is_source = is_source or $4,
                     is_target = is_target or $5
                 where id = $1
                 returning id, relation_oid, schema_name, table_name, is_source, is_target",
                &[
                    &id,
                    &relation.schema,
                    &relation.name,
                    &is_source,
                    &is_target,
                ],
            )
            .await?
        }
        None if let Some(pending) = pending_target => {
            let id: i64 = pending.get(0);
            txn.query_one(
                "update schema_nodes
                 set relation_oid = $2::oid, schema_name = $3, table_name = $4,
                     is_source = is_source or $5,
                     is_target = is_target or $6
                 where id = $1
                 returning id, relation_oid, schema_name, table_name, is_source, is_target",
                &[
                    &id,
                    &relation.oid,
                    &relation.schema,
                    &relation.name,
                    &is_source,
                    &is_target,
                ],
            )
            .await?
        }
        None => {
            txn.query_one(
                "insert into schema_nodes
                     (relation_oid, schema_name, table_name, is_source, is_target)
                 values ($1::oid, $2, $3, $4, $5)
                 returning id, relation_oid, schema_name, table_name, is_source, is_target",
                &[
                    &relation.oid,
                    &relation.schema,
                    &relation.name,
                    &is_source,
                    &is_target,
                ],
            )
            .await?
        }
    };
    Ok(schema_node_from_row(row))
}

/// Moves every edge attached to `redundant_id` to `canonical_id`, suppressing
/// duplicates before the redundant node is deleted.
async fn merge_schema_nodes_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    canonical_id: i64,
    redundant_id: i64,
) -> Result<(), CatalogError> {
    txn.execute(
        "insert into schema_edges (from_node_id, to_node_id, kind, created_at)
         select case when from_node_id = $2 then $1 else from_node_id end,
                case when to_node_id = $2 then $1 else to_node_id end,
                kind, created_at
         from schema_edges
         where from_node_id = $2 or to_node_id = $2
         on conflict (from_node_id, to_node_id, kind) do nothing",
        &[&canonical_id, &redundant_id],
    )
    .await?;
    txn.execute(
        "delete from schema_edges where from_node_id = $1 or to_node_id = $1",
        &[&redundant_id],
    )
    .await?;
    txn.execute("delete from schema_nodes where id = $1", &[&redundant_id])
        .await?;
    Ok(())
}

fn schema_node_from_row(row: tokio_postgres::Row) -> SchemaNode {
    SchemaNode {
        id: row.get(0),
        relation_oid: row.get(1),
        schema_name: row.get(2),
        table_name: row.get(3),
        is_source: row.get(4),
        is_target: row.get(5),
    }
}

/// Pool-level wrapper over [`persist_edge_in_txn`], mirroring
/// [`resolve_node`]'s relationship to [`resolve_node_in_txn`]. Exists so the
/// `on conflict do nothing` dedup path on `schema_edges`'s
/// `(from_node_id, to_node_id, kind)` uniqueness constraint has direct test
/// coverage — [`create_definition`] can never hit it itself, since
/// `transform_definitions.target_table` is unique and so no two definitions
/// can ever resolve to the same `(from_node_id, to_node_id)` pair.
pub async fn persist_edge(
    pool: &Pool,
    from_node_id: i64,
    to_node_id: i64,
    kind: EdgeKind,
) -> Result<(), CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    persist_edge_in_txn(&txn, from_node_id, to_node_id, kind).await?;
    txn.commit().await?;
    Ok(())
}

/// Records that `to_node_id` depends on `from_node_id` via `kind` — the
/// transactional core [`create_definition`] calls for a transform's `FROM`
/// edge. `on conflict do nothing` on `schema_edges`'s
/// `(from_node_id, to_node_id, kind)` uniqueness constraint makes
/// re-declaring the same transform's edge idempotent (definitions are
/// immutable, but nothing stops the same source/target pair from being
/// resolved through this path more than once as the graph grows).
async fn persist_edge_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    from_node_id: i64,
    to_node_id: i64,
    kind: EdgeKind,
) -> Result<(), CatalogError> {
    txn.execute(
        "insert into schema_edges (from_node_id, to_node_id, kind)
         values ($1, $2, $3)
         on conflict (from_node_id, to_node_id, kind) do nothing",
        &[&from_node_id, &to_node_id, &kind.as_str()],
    )
    .await?;
    Ok(())
}

/// Rejects an edge from `source_node_id` to `target_node_id` when it would
/// close a dependency-graph cycle: this holds iff `target_node_id` can already
/// reach `source_node_id` through some path of
/// existing [`super::model::SchemaEdge`]s (of any kind — see
/// [`create_definition`]'s call site for why this stays kind-generic).
/// Walks the transaction's current `schema_edges`/`schema_nodes` state as a
/// small in-memory adjacency map, hand-rolled DFS, matching
/// [`super::validate::detect_cycle`]'s column-level convention rather than
/// pulling in a graph crate for a problem this small.
async fn reject_if_table_cycle(
    txn: &tokio_postgres::Transaction<'_>,
    source_node_id: i64,
    target_node_id: i64,
) -> Result<(), CatalogError> {
    let rows = txn
        .query(
            "select se.from_node_id, se.to_node_id
             from schema_edges se",
            &[],
        )
        .await?;

    let mut adjacency: HashMap<i64, Vec<i64>> = HashMap::new();
    for row in rows {
        let from: i64 = row.get(0);
        let to: i64 = row.get(1);
        adjacency.entry(from).or_default().push(to);
    }

    if let Some(mut path) = find_node_path(&adjacency, target_node_id, source_node_id) {
        path.push(target_node_id);
        let names = txn
            .query(
                "select id, table_name
                 from schema_nodes where id = any($1)",
                &[&path],
            )
            .await?;
        let names_by_id: HashMap<i64, String> = names
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let cycle = path
            .into_iter()
            .map(|id| {
                names_by_id
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| id.to_string())
            })
            .collect();
        return Err(ValidationError::TableCycle { cycle }.into());
    }
    Ok(())
}

/// DFS from `from` to `to` over `adjacency`, returning the path (inclusive
/// of both ends) if one exists.
fn find_node_path(adjacency: &HashMap<i64, Vec<i64>>, from: i64, to: i64) -> Option<Vec<i64>> {
    let mut visited: HashSet<i64> = HashSet::new();
    let mut path: Vec<i64> = Vec::new();
    if find_node_path_from(adjacency, from, to, &mut visited, &mut path) {
        Some(path)
    } else {
        None
    }
}

fn find_node_path_from(
    adjacency: &HashMap<i64, Vec<i64>>,
    node: i64,
    to: i64,
    visited: &mut HashSet<i64>,
    path: &mut Vec<i64>,
) -> bool {
    path.push(node);
    if node == to {
        return true;
    }
    visited.insert(node);
    if let Some(neighbors) = adjacency.get(&node) {
        for neighbor in neighbors {
            if !visited.contains(neighbor)
                && find_node_path_from(adjacency, *neighbor, to, visited, path)
            {
                return true;
            }
        }
    }
    path.pop();
    false
}

/// The [`SchemaNode`] already resolved for `table_name`, or `None` if
/// nothing has ever referenced it as a source or a target.
pub async fn node_for_table(
    pool: &Pool,
    table_name: &str,
) -> Result<Option<SchemaNode>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, relation_oid, schema_name, table_name, is_source, is_target
             from schema_nodes where table_name = $1 order by relation_oid nulls last limit 1",
            &[&table_name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };

    Ok(Some(schema_node_from_row(row)))
}

/// The OID-backed graph node for a source relation, if it has been referenced
/// by a transform definition.
pub async fn node_for_source_oid(
    pool: &Pool,
    source_relation_oid: u32,
) -> Result<Option<SchemaNode>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, relation_oid, schema_name, table_name, is_source, is_target
             from schema_nodes where relation_oid = $1::oid",
            &[&source_relation_oid],
        )
        .await?;
    Ok(row.map(schema_node_from_row))
}

/// The [`SchemaEdge`]s of `kind` directed away from `node_table` — a
/// generalized, `transform_definitions`-agnostic sibling of
/// [`dependents_of`] for edge kinds whose dependents don't join back into
/// `transform_definitions` (e.g. [`EdgeKind::Relationship`], whose
/// dependents are `relationship_definitions` rows, read separately via
/// [`relationship_by_name`]). Returns raw edges rather than joining onto any
/// definition table, so it works for any [`EdgeKind`] without needing a
/// kind-specific query.
pub async fn edges_from(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<SchemaEdge>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select se.id, se.from_node_id, se.to_node_id
             from schema_edges se
             join schema_nodes from_node on from_node.id = se.from_node_id
             where from_node.table_name = $1 and se.kind = $2
             order by se.id",
            &[&node_table, &kind.as_str()],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| SchemaEdge {
            id: row.get(0),
            from_node_id: row.get(1),
            to_node_id: row.get(2),
            kind,
        })
        .collect())
}

/// Splits `source_columns` into the parallel key/value text arrays
/// `jsonb_object`'s two-array form wants (see `create_definition`'s insert
/// and [`transforms_for_source`]'s matching read side).
fn encode_type_map(source_columns: &HashMap<String, ValueType>) -> (Vec<&str>, Vec<&'static str>) {
    let mut keys = Vec::with_capacity(source_columns.len());
    let mut vals = Vec::with_capacity(source_columns.len());
    for (name, value_type) in source_columns {
        keys.push(name.as_str());
        vals.push(match value_type {
            ValueType::Numeric => "numeric",
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
        });
    }
    (keys, vals)
}

/// One definition row's non-`source_columns` fields, accumulated while
/// [`transforms_for_source`] walks its single, lateral-joined query — see
/// that function's doc comment.
struct PendingDefinition {
    source: SourceRelation,
    target_relation_oid: Option<u32>,
    source_version: i64,
    text: String,
    source_columns: HashMap<String, ValueType>,
}

async fn hydrate_source_column_bindings(
    client: &tokio_postgres::Client,
    definitions: &mut [Definition],
) -> Result<(), CatalogError> {
    if definitions.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = definitions.iter().map(|definition| definition.id).collect();
    let rows = client
        .query(
            "select definition_id, logical_name, source_relation_oid, attnum, type_oid, type_modifier, value_type
             from transform_source_columns
             where definition_id = any($1)
             order by definition_id, logical_name",
            &[&ids],
        )
        .await?;
    let by_id: HashMap<i64, usize> = definitions
        .iter()
        .enumerate()
        .map(|(index, definition)| (definition.id, index))
        .collect();
    for row in rows {
        let definition_id: i64 = row.get(0);
        let value_text: String = row.get(6);
        let binding = SourceColumnBinding {
            logical_name: row.get(1),
            source_relation_oid: row.get(2),
            attnum: row.get(3),
            type_oid: row.get(4),
            type_modifier: row.get(5),
            value_type: decode_value_type("bound source column", &value_text)?,
        };
        definitions[by_id[&definition_id]]
            .source_column_bindings
            .push(binding);
    }
    Ok(())
}

/// The transform definitions that depend on `node_table` via a `kind` edge
/// in the persisted dependency graph (issue #21) — e.g. `EdgeKind::Source`
/// answers "what reads from `node_table` as its `FROM`". Walking
/// `schema_edges` (rather than matching on
/// `transform_definitions.source_table` string equality) is what makes this
/// a real graph lookup: multi-hop chains resolve by calling this again with
/// a dependent's target table, not by any special-casing here.
///
/// One query, not one-per-definition-row (issue #69): a `left join lateral
/// jsonb_each_text(...)` unnests every dependent definition's persisted
/// `source_columns` map inline, so this is still "decode JSON via SQL, no
/// serde_json dependency" — matching `staging::apply::decode_image`'s
/// convention — just decoded for every row in one round trip instead of one
/// per definition. The `left join` (rather than an inner join/`cross join
/// lateral`) matters: a definition whose `source_columns` is `{}` must still
/// come back with zero entries, not disappear from the result entirely.
pub async fn dependents_of(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_table, t.source_relation_oid, c.oid, n.nspname::text, c.relname::text,
                    t.target_relation_oid, t.source_version, t.definition_text, e.key, e.value
             from schema_nodes from_node
             join schema_edges se on se.from_node_id = from_node.id and se.kind = $2
             join schema_nodes to_node on to_node.id = se.to_node_id
             join transform_definitions t on t.target_table = to_node.table_name
              left join pg_catalog.pg_class c on c.oid = t.source_relation_oid
              left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             left join lateral jsonb_each_text(t.source_columns) e on true
             where from_node.table_name = $1
             order by t.id",
            &[&node_table, &kind.as_str()],
        )
        .await?;

    // `order` preserves the query's `order by t.id` across the group-by
    // done in Rust below (a plain `HashMap` has no ordering of its own).
    let mut order: Vec<i64> = Vec::new();
    let mut by_id: HashMap<i64, PendingDefinition> = HashMap::new();

    for row in rows {
        let id: i64 = row.get(0);
        let legacy_source: String = row.get(1);
        let source_oid: Option<u32> = row.get(2);
        let resolved_oid: Option<u32> = row.get(3);
        if resolved_oid.is_none() {
            return match source_oid {
                Some(oid) => Err(CatalogError::BoundSourceRelationMissing { oid }),
                None => Err(CatalogError::SourceRelationNotFound {
                    source: legacy_source,
                }),
            };
        }
        let key: Option<String> = row.get(9);
        let value: Option<String> = row.get(10);

        let pending = by_id.entry(id).or_insert_with(|| {
            order.push(id);
            PendingDefinition {
                source: SourceRelation {
                    oid: resolved_oid.expect("checked source relation exists"),
                    schema: row.get(4),
                    name: row.get(5),
                },
                target_relation_oid: row.get(6),
                source_version: row.get(7),
                text: row.get(8),
                source_columns: HashMap::new(),
            }
        });

        if let (Some(key), Some(value)) = (key, value) {
            let value_type = match value.as_str() {
                "numeric" => ValueType::Numeric,
                "text" => ValueType::Text,
                "boolean" => ValueType::Boolean,
                "uuid" => ValueType::Uuid,
                other => {
                    return Err(CatalogError::UnknownValueType {
                        column: key,
                        text: other.to_string(),
                    });
                }
            };
            pending.source_columns.insert(key, value_type);
        }
    }

    let mut result = Vec::with_capacity(order.len());
    for id in order {
        let pending = by_id.remove(&id).expect("id was just pushed to order");
        let def = parse(&pending.text)?;
        result.push(Definition {
            id,
            source: pending.source,
            target_relation_oid: pending.target_relation_oid,
            source_version: pending.source_version,
            def,
            source_columns: pending.source_columns,
            source_column_bindings: Vec::new(),
        });
    }
    hydrate_source_column_bindings(&client, &mut result).await?;
    Ok(result)
}

/// The transform definitions currently subscribed to `source_table` — the
/// mapping intake (#7/#8) will use to decide what to subscribe to. A thin
/// wrapper over [`dependents_of`] filtered to [`EdgeKind::Source`], the only
/// edge kind persisted today.
pub async fn transforms_for_source(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<Definition>, CatalogError> {
    match resolve_source_relation(pool, source_table).await {
        Ok(source) => transforms_for_source_oid(pool, source.oid).await,
        Err(CatalogError::SourceRelationNotFound { .. }) => {
            let client = pool.get().await?;
            let missing_oid = client
                .query_opt(
                    "select t.source_relation_oid
                     from transform_definitions t
                     left join pg_catalog.pg_class c on c.oid = t.source_relation_oid
                     where t.source_table = $1
                       and t.source_relation_oid is not null
                       and c.oid is null
                     limit 1",
                    &[&source_table],
                )
                .await?;
            match missing_oid {
                Some(row) => Err(CatalogError::BoundSourceRelationMissing { oid: row.get(0) }),
                None => Ok(Vec::new()),
            }
        }
        Err(err) => Err(err),
    }
}

/// The definitions subscribed to one physical source relation.
pub async fn transforms_for_source_oid(
    pool: &Pool,
    source_relation_oid: u32,
) -> Result<Vec<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_table, t.source_relation_oid, c.oid, n.nspname::text, c.relname::text,
                     t.target_relation_oid, t.source_version, t.definition_text, e.key, e.value
             from schema_nodes from_node
             join schema_edges se on se.from_node_id = from_node.id and se.kind = 'source'
             join schema_nodes to_node on to_node.id = se.to_node_id
             join transform_definitions t on t.target_table = to_node.table_name
             left join pg_catalog.pg_class c on c.oid = t.source_relation_oid
             left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             left join lateral jsonb_each_text(t.source_columns) e on true
             where from_node.relation_oid = $1::oid
             order by t.id",
            &[&source_relation_oid],
        )
        .await?;
    let mut definitions = definitions_from_rows(rows)?;
    hydrate_source_column_bindings(&client, &mut definitions).await?;
    Ok(definitions)
}

fn definitions_from_rows(rows: Vec<tokio_postgres::Row>) -> Result<Vec<Definition>, CatalogError> {
    let mut order: Vec<i64> = Vec::new();
    let mut by_id: HashMap<i64, PendingDefinition> = HashMap::new();
    for row in rows {
        let id: i64 = row.get(0);
        let legacy_source: String = row.get(1);
        let source_oid: Option<u32> = row.get(2);
        let resolved_oid: Option<u32> = row.get(3);
        if resolved_oid.is_none() {
            return match source_oid {
                Some(oid) => Err(CatalogError::BoundSourceRelationMissing { oid }),
                None => Err(CatalogError::SourceRelationNotFound {
                    source: legacy_source,
                }),
            };
        }
        let key: Option<String> = row.get(9);
        let value: Option<String> = row.get(10);
        let pending = by_id.entry(id).or_insert_with(|| {
            order.push(id);
            PendingDefinition {
                source: SourceRelation {
                    oid: resolved_oid.expect("checked source relation exists"),
                    schema: row.get(4),
                    name: row.get(5),
                },
                target_relation_oid: row.get(6),
                source_version: row.get(7),
                text: row.get(8),
                source_columns: HashMap::new(),
            }
        });
        if let (Some(key), Some(value)) = (key, value) {
            let value_type = decode_value_type(&key, &value)?;
            pending.source_columns.insert(key, value_type);
        }
    }

    let mut result = Vec::with_capacity(order.len());
    for id in order {
        let pending = by_id.remove(&id).expect("id was just pushed to order");
        result.push(Definition {
            id,
            source: pending.source,
            target_relation_oid: pending.target_relation_oid,
            source_version: pending.source_version,
            def: parse(&pending.text)?,
            source_columns: pending.source_columns,
            source_column_bindings: Vec::new(),
        });
    }
    Ok(result)
}

fn decode_value_type(column: &str, value: &str) -> Result<ValueType, CatalogError> {
    match value {
        "numeric" => Ok(ValueType::Numeric),
        "text" => Ok(ValueType::Text),
        "boolean" => Ok(ValueType::Boolean),
        "uuid" => Ok(ValueType::Uuid),
        other => Err(CatalogError::UnknownValueType {
            column: column.to_string(),
            text: other.to_string(),
        }),
    }
}

/// Every distinct source table with at least one registered transform
/// definition, unqualified (as stored — see [`create_definition`]'s
/// `def.source`). Issue #14: a running [`crate::Client`]'s maintenance loop
/// polls this to notice a transform registered against a source table it
/// hasn't seen before, so it can add that table to the publication and
/// discharge its backfill without waiting for a restart.
pub async fn all_source_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select distinct source_table from transform_definitions",
            &[],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// Every currently resolvable physical source relation with a registered
/// definition. This reads catalog OIDs directly, so a dropped binding is not
/// accidentally rebound through its legacy source spelling.
pub async fn all_source_relations(pool: &Pool) -> Result<Vec<SourceRelation>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select distinct c.oid, n.nspname::text, c.relname::text
             from transform_definitions t
             join pg_catalog.pg_class c on c.oid = t.source_relation_oid
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             order by c.oid",
            &[],
        )
        .await?;
    Ok(rows.into_iter().map(source_relation_from_row).collect())
}

/// The original source spelling stored for an OID-bound definition. This is
/// retained solely to recognize an explicit client source entry that named the
/// relation before a rename; physical relation metadata remains authoritative.
pub async fn source_table_names_for_oid(
    pool: &Pool,
    source_relation_oid: u32,
) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query(
            "select source_table from transform_definitions \
             where source_relation_oid = $1::oid order by id",
            &[&source_relation_oid],
        )
        .await?;
    Ok(row.into_iter().map(|row| row.get(0)).collect())
}

/// The current version of `source_table`, or `None` if no definition has
/// ever been created against it.
pub async fn source_table_version(
    pool: &Pool,
    source_table: &str,
) -> Result<Option<i64>, CatalogError> {
    match resolve_source_relation(pool, source_table).await {
        Ok(source) => source_table_version_by_oid(pool, source.oid).await,
        Err(CatalogError::SourceRelationNotFound { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

/// The version fence for one physical source relation, or `None` when no
/// definition has ever been registered against that exact OID.
pub async fn source_table_version_by_oid(
    pool: &Pool,
    source_relation_oid: u32,
) -> Result<Option<i64>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select version from source_table_versions where source_relation_oid = $1::oid",
            &[&source_relation_oid],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}
