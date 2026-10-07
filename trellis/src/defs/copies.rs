//! The columns Trellis creates with a type that comes from the source.
//!
//! Some tables Trellis creates hold a source column's values in a column of
//! that source column's type, as define found it:
//!
//! - a 1-1 target's key columns, typed as the source key's
//!   ([`super::ddl::target_table_ddl`]), and its bare passthrough fields,
//!   typed as the source column they copy (#45);
//! - an aggregate target's `GROUP BY` columns, and the same columns of its
//!   ledger and its group-delta table, typed as the key's value family
//!   ([`super::ddl::pg_type_name`]: `integer`, `bigint`, `text`, `numeric`);
//! - a to-one relationship's settled projection's key, typed as the
//!   to-side's join column, and the projection's column for each to-side
//!   column a definition reads through the relationship (`author.name`,
//!   `GROUP BY author.country`), typed as that to-side column.
//!
//! Others are typed by an expression over source columns, as define's type
//! inference ([`super::validate::infer_field_types`]) gives it from the
//! source's column types:
//!
//! - a 1-1 target's calculated field (`qty + 1`);
//! - an aggregate target's field column (`SUM(qty)`, `MIN(qty)`);
//! - an aggregate ledger's contribution column, typed as the aggregate's
//!   argument ([`super::ledger::contribution_pg_type`]).
//!
//! None of them changes type when the source column does. After the source
//! column widens (`integer` to `bigint`, `varchar(50)` to `text`), the
//! first value the column can't hold fails its write (`22003`, `22001`). So
//! the staging worker's capture pass compares each column with the type
//! define would give it from the live schema ([`inspect`]), and acts on
//! each table holding a column the source widened
//! (`staging::schema_change::pause_readers_of_retyped`):
//!
//! - When every widened column of the table widened by changing only the
//!   catalog ([`CopyState::catalog_only`]: a longer `varchar`, `text`, a
//!   `varchar` without its length, or a `numeric` with more precision at the
//!   same scale), the pass re-types those columns itself, in place. No value
//!   changes, nothing is rewritten, and no definition pauses or rebuilds.
//!   Then the keys its definitions held for a value the old type couldn't
//!   hold are released (`staging::quarantine::release_retyped_keys`).
//! - Otherwise it pauses every definition that owns a column the source has
//!   outgrown ([`super::key_types::widens`]). A resume brings every column
//!   whose type differs from define's to it ([`retype_statements`]) before
//!   it rebuilds.
//!
//! Trellis re-types a column on its own only in the first case. Either
//! re-type keeps the column's collation. A narrowing, or a change to another
//! type family, pauses nothing here and is re-typed only by a resume: every
//! value still fits, or define's own checks own it.
//!
//! The ledger's `__from_key` is always `text`, and an aggregate's hidden
//! partials (`numeric` sums, `bigint` counts) have fixed types, so none of
//! them is checked.

use std::collections::{BTreeMap, HashMap, HashSet};

use tokio_postgres::GenericClient;

use super::ast::{Expr, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains};
use super::key_types::ColumnType;
use super::model::{RelationshipCardinality, RelationshipDefinition};
use crate::pool::quote_ident;

/// What a [`TypedCopy`] is a copy of, and where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyKind {
    /// A 1-1 target's key column.
    TargetKey,
    /// A 1-1 target's bare passthrough field.
    Passthrough,
    /// An aggregate target's `GROUP BY` column.
    GroupBy,
    /// The same column of the aggregate's ledger.
    LedgerGroup,
    /// The same column of the aggregate's group-delta table.
    DeltasGroup,
    /// A to-one relationship projection's key.
    ProjectionKey,
    /// A to-one relationship projection's column for a `GROUP BY` key read
    /// through the relationship: the aggregate's groups are read from it.
    ProjectionGroupBy,
    /// A to-one relationship projection's column for any other to-side
    /// column read through the relationship (`author.name`).
    ProjectionData,
    /// A 1-1 target's calculated field, typed by its expression.
    Field,
    /// An aggregate target's field column, typed by its expression.
    AggregateField,
    /// An aggregate ledger's contribution column, typed as its argument.
    Contribution,
}

impl CopyKind {
    /// Whether the copy is typed as its source column's value family
    /// ([`super::ddl::pg_type_name`]) rather than as the column itself.
    fn by_family(self) -> bool {
        matches!(
            self,
            CopyKind::GroupBy | CopyKind::LedgerGroup | CopyKind::DeltasGroup
        )
    }
}

/// One column Trellis creates with a type that comes from the source: a
/// copy of one source column, or a column typed by an expression over some.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypedCopy {
    pub kind: CopyKind,
    /// The table holding the column, quoted and qualified: ready for SQL
    /// text and for `to_regclass`.
    pub table: String,
    /// The same table as messages name it, `schema.table`.
    pub table_name: String,
    pub column: String,
    /// The source columns its type comes from, each as a qualified,
    /// unquoted table and a column: the one column it copies, or every
    /// column its expression reads.
    pub reads: Vec<(String, String)>,
    /// For a column typed by an expression, the type define's inference
    /// gives it from the live schema, as [`super::ddl::pg_type_name`]
    /// renders it. `None` for a copy of one column, typed from that column.
    pub inferred: Option<String>,
}

impl TypedCopy {
    /// `schema.table.column`, as messages name it.
    pub(crate) fn label(&self) -> String {
        format!("{}.{}", self.table_name, self.column)
    }

    /// The columns of `table` its type comes from.
    pub(crate) fn columns_of<'a>(&'a self, table: &'a str) -> impl Iterator<Item = &'a str> {
        self.reads
            .iter()
            .filter(move |(t, _)| t == table)
            .map(|(_, c)| c.as_str())
    }
}

/// Every column definition `def` created with a type that comes from the
/// source, reading `source` (qualified) into `target` (qualified): its
/// typed copies, and the columns typed by an expression
/// ([`inferred_copies`]). `rels` are the relationships declared on
/// `source`; the to-one ones `def` reads through have a projection, in
/// `schema`, the catalog schema. A copy of a source column that no longer
/// exists is left out (the missing-column check owns that case).
pub(crate) async fn typed_copies(
    client: &impl GenericClient,
    schema: &str,
    def: &TransformDef,
    source: &str,
    target: &str,
    rels: &[&RelationshipDefinition],
) -> Result<Vec<TypedCopy>, tokio_postgres::Error> {
    let (target_schema, target_bare) = target.split_once('.').unwrap_or(("", target));
    let quoted_target = super::ddl::qualified_target_table_ident(target);
    let mut copies = Vec::new();
    let copy =
        |kind, table: &str, table_name: &str, column: &str, from: &str, from_col: &str| TypedCopy {
            kind,
            table: table.to_string(),
            table_name: table_name.to_string(),
            column: column.to_string(),
            reads: vec![(from.to_string(), from_col.to_string())],
            inferred: None,
        };
    match &def.key_space {
        KeySpace::OneToOne => {
            let source_columns: HashSet<String> = client
                .query(
                    "select attname::text from pg_catalog.pg_attribute \
                     where attrelid = pg_catalog.to_regclass($1) \
                       and attnum > 0 and not attisdropped",
                    &[&super::ddl::regclass_arg(source)],
                )
                .await?
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            for key in super::ddl::identity_key_columns(client, target).await? {
                if source_columns.contains(&key.name) {
                    copies.push(copy(
                        CopyKind::TargetKey,
                        &quoted_target,
                        target,
                        &key.name,
                        source,
                        &key.name,
                    ));
                }
            }
            for field in &def.fields {
                // A bare source-column reference, as `ddl::target_table_ddl`
                // finds one: not a reference to another field by its name.
                let Expr::Column(name) = &field.expr else {
                    continue;
                };
                if !source_columns.contains(name)
                    || (name != &field.name && def.fields.iter().any(|f| &f.name == name))
                {
                    continue;
                }
                copies.push(copy(
                    CopyKind::Passthrough,
                    &quoted_target,
                    target,
                    &field.name,
                    source,
                    name,
                ));
            }
        }
        KeySpace::Aggregate { group_by } => {
            let ledger_bare = super::ledger::ledger_table_name(target_bare);
            let deltas_bare = super::ledger::deltas_table_name(target_bare);
            let tables = [
                (CopyKind::GroupBy, quoted_target.clone(), target.to_string()),
                (
                    CopyKind::LedgerGroup,
                    super::ledger::qualified_ledger_table(target_schema, target_bare),
                    format!("{target_schema}.{ledger_bare}"),
                ),
                (
                    CopyKind::DeltasGroup,
                    super::ledger::qualified_deltas_table(target_schema, target_bare),
                    format!("{target_schema}.{deltas_bare}"),
                ),
            ];
            for key in group_by {
                let (from, from_col) = match key {
                    GroupByKey::Column(column) => (source.to_string(), column.as_str()),
                    GroupByKey::RelationshipPath { rel, column } => {
                        match rels.iter().find(|r| &r.def.name == rel) {
                            Some(r) => (r.qualified_to_table(), column.as_str()),
                            None => continue,
                        }
                    }
                };
                for (kind, table, table_name) in &tables {
                    copies.push(copy(
                        *kind,
                        table,
                        table_name,
                        key.target_column_name(),
                        &from,
                        from_col,
                    ));
                }
            }
        }
    }
    let references = super::eval::relationship_references(def);
    let read: HashSet<&str> = references.iter().map(|(rel, _)| rel.as_str()).collect();
    for rel in rels.iter().filter(|r| {
        r.cardinality == RelationshipCardinality::ToOne && read.contains(r.def.name.as_str())
    }) {
        let Some(row) = client
            .query_opt(
                "select projection_table from relationship_projections \
                 where relationship_id = $1",
                &[&rel.id],
            )
            .await?
        else {
            continue;
        };
        let projection: String = row.get(0);
        let table = super::ddl::qualified_relationship_projection_table(schema, &projection);
        let table_name = format!("{schema}.{projection}");
        let to_table = rel.qualified_to_table();
        copies.push(copy(
            CopyKind::ProjectionKey,
            &table,
            &table_name,
            &rel.def.to_col,
            &to_table,
            &rel.def.to_col,
        ));
        if let KeySpace::Aggregate { group_by } = &def.key_space {
            for key in group_by {
                if let GroupByKey::RelationshipPath { rel: name, column } = key
                    && name == &rel.def.name
                    && column != &rel.def.to_col
                {
                    copies.push(copy(
                        CopyKind::ProjectionGroupBy,
                        &table,
                        &table_name,
                        column,
                        &to_table,
                        column,
                    ));
                }
            }
        }
        // Every other to-side column it reads through the relationship.
        for (name, column) in &references {
            if name != &rel.def.name
                || column == &rel.def.to_col
                || copies
                    .iter()
                    .any(|c| c.table == table && &c.column == column)
            {
                continue;
            }
            copies.push(copy(
                CopyKind::ProjectionData,
                &table,
                &table_name,
                column,
                &to_table,
                column,
            ));
        }
    }
    copies.extend(inferred_copies(client, def, source, target, rels).await?);
    Ok(copies)
}

/// The columns of `def`'s target and ledger typed by an expression
/// ([`CopyKind::Field`], [`CopyKind::AggregateField`],
/// [`CopyKind::Contribution`]), each with the type define's inference gives
/// it from the live schema: the same inference, over the same source column
/// and relationship types, that define created it with
/// ([`super::ddl::target_table_ddl`], [`super::ddl::aggregate_target_table_ddl`]).
/// A column whose expression reads no source column (`COUNT(*)`) has no
/// source to drift from and is left out. Empty while the definition doesn't
/// validate against the live schema: a resume's re-validation owns that.
async fn inferred_copies(
    client: &impl GenericClient,
    def: &TransformDef,
    source: &str,
    target: &str,
    rels: &[&RelationshipDefinition],
) -> Result<Vec<TypedCopy>, tokio_postgres::Error> {
    use super::catalog::CatalogError;
    let source_columns = match super::catalog::live_source_columns(client, source).await {
        Ok(columns) => columns,
        Err(CatalogError::Db(err)) => return Err(err),
        Err(_) => return Ok(Vec::new()),
    };
    let relationships = match super::catalog::resolve_relationships_in(client, def, source).await {
        Ok(relationships) => relationships,
        Err(CatalogError::Db(err)) => return Err(err),
        Err(_) => return Ok(Vec::new()),
    };
    let Ok(field_types) = super::validate::infer_field_types(def, &source_columns, &relationships)
    else {
        return Ok(Vec::new());
    };
    let Ok(substituted) = super::backfill::substituted_field_exprs(def) else {
        return Ok(Vec::new());
    };
    let (target_schema, target_bare) = target.split_once('.').unwrap_or(("", target));
    let quoted_target = super::ddl::qualified_target_table_ident(target);
    let reads = |expr: &Expr| {
        let mut out = Vec::new();
        expr_reads(expr, source, &source_columns, rels, &mut out);
        out
    };
    let field_type = |name: &str| {
        super::ddl::pg_type_name(field_types.get(name).copied().unwrap_or(ValueType::Numeric))
            .into_owned()
    };
    let mut copies = Vec::new();
    let mut push = |kind, table: &str, table_name: &str, column: &str, reads, inferred| {
        let reads: Vec<(String, String)> = reads;
        if !reads.is_empty() {
            copies.push(TypedCopy {
                kind,
                table: table.to_string(),
                table_name: table_name.to_string(),
                column: column.to_string(),
                reads,
                inferred: Some(inferred),
            });
        }
    };
    match &def.key_space {
        KeySpace::OneToOne => {
            for field in &def.fields {
                // A bare passthrough is a copy of its column, as
                // `ddl::target_table_ddl` decides one is.
                let passthrough = matches!(&field.expr, Expr::Column(name)
                    if source_columns.contains_key(name)
                        && (name == &field.name || !def.fields.iter().any(|f| &f.name == name)));
                if passthrough {
                    continue;
                }
                let Some(expr) = substituted.get(&field.name) else {
                    continue;
                };
                push(
                    CopyKind::Field,
                    &quoted_target,
                    target,
                    &field.name,
                    reads(expr),
                    field_type(&field.name),
                );
            }
        }
        KeySpace::Aggregate { group_by } => {
            for field in &def.fields {
                if group_by_contains(group_by, &field.name) {
                    continue;
                }
                let Some(expr) = substituted.get(&field.name) else {
                    continue;
                };
                push(
                    CopyKind::AggregateField,
                    &quoted_target,
                    target,
                    &field.name,
                    reads(expr),
                    field_type(&field.name),
                );
            }
            let ledger = super::ledger::qualified_ledger_table(target_schema, target_bare);
            let ledger_name = format!(
                "{target_schema}.{}",
                super::ledger::ledger_table_name(target_bare)
            );
            for contribution in super::ledger::contributions(&def.fields, group_by, &substituted) {
                let Ok(value_type) = super::validate::infer_expr_type(
                    &contribution.arg,
                    &source_columns,
                    &relationships,
                ) else {
                    continue;
                };
                push(
                    CopyKind::Contribution,
                    &ledger,
                    &ledger_name,
                    &contribution.column,
                    reads(&contribution.arg),
                    super::ledger::contribution_pg_type(value_type),
                );
            }
        }
    }
    Ok(copies)
}

/// The source columns `expr` (substituted: it names no other field) reads,
/// appended to `out` once each: a column of `source` (qualified), or a
/// to-side column read through one of `rels`.
fn expr_reads(
    expr: &Expr,
    source: &str,
    source_columns: &HashMap<String, ValueType>,
    rels: &[&RelationshipDefinition],
    out: &mut Vec<(String, String)>,
) {
    let read = match expr {
        Expr::Column(name) if source_columns.contains_key(name) => {
            Some((source.to_string(), name.clone()))
        }
        Expr::RelationshipPath { rel, column } => rels
            .iter()
            .find(|r| &r.def.name == rel)
            .map(|r| (r.qualified_to_table(), column.clone())),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                expr_reads(arg, source, source_columns, rels, out);
            }
            None
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_reads(lhs, source, source_columns, rels, out);
            expr_reads(rhs, source, source_columns, rels, out);
            None
        }
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => None,
    };
    if let Some(read) = read
        && !out.contains(&read)
    {
        out.push(read);
    }
}

/// A column type: its OID and modifier, its `schema.typname` and modifier
/// ([`ColumnType`]), and `format_type`'s rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyType {
    pub oid: u32,
    pub typmod: i32,
    pub ty: ColumnType,
    pub display: String,
    /// Whether the type takes a collation.
    pub collatable: bool,
}

/// A [`TypedCopy`] with its own type now and the type define would give it
/// from the live schema.
#[derive(Debug, Clone)]
pub(crate) struct CopyState {
    pub copy: TypedCopy,
    pub copy_type: CopyType,
    pub live_type: CopyType,
    /// The copy's collation, quoted and qualified, when it isn't its type's
    /// default: a 1-1 target's key keeps its source key's (#769). A re-type
    /// keeps it.
    pub collation: Option<String>,
}

impl CopyState {
    /// Whether the copy's type is no longer the one define would create.
    pub(crate) fn drifted(&self) -> bool {
        (self.copy_type.oid, self.copy_type.typmod) != (self.live_type.oid, self.live_type.typmod)
    }

    /// Whether the source widened past what the column holds: the type
    /// define would give it now holds values its type can't
    /// ([`super::key_types::widens`]).
    pub(crate) fn outgrown(&self) -> bool {
        super::key_types::widens(&self.copy_type.ty, &self.live_type.ty)
    }

    /// Whether the copy drifted by a widening its table takes by changing
    /// only the catalog ([`super::key_types::catalog_only`]): no rewrite, no
    /// value changed, every index kept. A group-delta table's column never
    /// is one: its re-type generates the partition column again.
    pub(crate) fn catalog_only(&self) -> bool {
        self.copy.kind != CopyKind::DeltasGroup
            && super::key_types::catalog_only(&self.copy_type.ty, &self.live_type.ty)
    }
}

/// The types of each of `copies` now, and the types define would give them
/// from the live schema, as [`CopyState`]s: a copy of one column is typed
/// as that column (or as its value family), and a column typed by an
/// expression as [`TypedCopy::inferred`]. A copy whose table or column, or
/// whose source column, no longer exists is left out.
pub(crate) async fn inspect(
    client: &impl GenericClient,
    copies: Vec<TypedCopy>,
) -> Result<Vec<CopyState>, tokio_postgres::Error> {
    if copies.is_empty() {
        return Ok(Vec::new());
    }
    // The type of each `(i, table, column)`, by `i`, and its collation
    // when it isn't the type's default.
    let column_types = |columns: Vec<(i64, String, String)>| async move {
        let (indexes, (tables, names)): (Vec<i64>, (Vec<String>, Vec<String>)) =
            columns.into_iter().map(|(i, t, c)| (i, (t, c))).unzip();
        let rows = client
            .query(
                "select k.i, a.atttypid, a.atttypmod, \
                        pg_catalog.quote_ident(cn.nspname) || '.' \
                            || pg_catalog.quote_ident(co.collname) \
                 from unnest($1::text[], $2::text[], $3::int8[]) as k(t, c, i) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = pg_catalog.to_regclass(k.t) and a.attname = k.c \
                  and a.attnum > 0 and not a.attisdropped \
                 join pg_catalog.pg_type t on t.oid = a.atttypid \
                 left join pg_catalog.pg_collation co \
                   on co.oid = a.attcollation and a.attcollation <> t.typcollation \
                 left join pg_catalog.pg_namespace cn on cn.oid = co.collnamespace",
                &[&tables, &names, &indexes],
            )
            .await?;
        Ok::<_, tokio_postgres::Error>(
            rows.into_iter()
                .map(|row| {
                    let i: i64 = row.get(0);
                    (
                        i as usize,
                        (
                            (row.get::<_, u32>(1), row.get::<_, i32>(2)),
                            row.get::<_, Option<String>>(3),
                        ),
                    )
                })
                .collect::<HashMap<usize, ((u32, i32), Option<String>)>>(),
        )
    };
    let copied = column_types(
        copies
            .iter()
            .enumerate()
            .map(|(i, c)| (i as i64, c.table.clone(), c.column.clone()))
            .collect(),
    )
    .await?;
    let sources = column_types(
        copies
            .iter()
            .enumerate()
            .filter(|(_, c)| c.inferred.is_none())
            .filter_map(|(i, c)| {
                c.reads.first().map(|(table, column)| {
                    (i as i64, super::ddl::regclass_arg(table), column.clone())
                })
            })
            .collect(),
    )
    .await?;

    let mut described: BTreeMap<(u32, i32), CopyType> = BTreeMap::new();
    let mut named: HashMap<String, Option<u32>> = HashMap::new();
    let mut states = Vec::new();
    for (i, copy) in copies.into_iter().enumerate() {
        let Some((copy_type, collation)) = copied.get(&i).cloned() else {
            continue;
        };
        let live_type = match &copy.inferred {
            Some(name) => match regtype(client, &mut named, name).await? {
                Some(oid) => (oid, -1),
                None => continue,
            },
            None => {
                let Some(&((source_oid, source_typmod), _)) = sources.get(&i) else {
                    continue;
                };
                if copy.kind.by_family() {
                    let value_type = super::pg_type::value_type_for_oid(client, source_oid).await?;
                    let name = super::ddl::pg_type_name(value_type);
                    match regtype(client, &mut named, &name).await? {
                        Some(oid) => (oid, -1),
                        None => continue,
                    }
                } else {
                    (source_oid, source_typmod)
                }
            }
        };
        let copy_type = describe(client, &mut described, copy_type).await?;
        let live_type = describe(client, &mut described, live_type).await?;
        states.push(CopyState {
            copy,
            copy_type,
            live_type,
            collation,
        });
    }
    Ok(states)
}

/// The OID of the type `name` names, memoized in `named`; `None` for none.
async fn regtype(
    client: &impl GenericClient,
    named: &mut HashMap<String, Option<u32>>,
    name: &str,
) -> Result<Option<u32>, tokio_postgres::Error> {
    if let Some(known) = named.get(name) {
        return Ok(*known);
    }
    let oid: Option<u32> = client
        .query_one("select pg_catalog.to_regtype($1)::oid", &[&name])
        .await?
        .get(0);
    named.insert(name.to_string(), oid);
    Ok(oid)
}

/// `(oid, typmod)` as a [`CopyType`], memoized in `described`.
async fn describe(
    client: &impl GenericClient,
    described: &mut BTreeMap<(u32, i32), CopyType>,
    (oid, typmod): (u32, i32),
) -> Result<CopyType, tokio_postgres::Error> {
    if let Some(known) = described.get(&(oid, typmod)) {
        return Ok(known.clone());
    }
    let row = client
        .query_one(
            "select n.nspname::text || '.' || t.typname::text, \
                    pg_catalog.format_type(t.oid, $2), t.typcollation <> 0 \
             from pg_catalog.pg_type t \
             join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
             where t.oid = $1",
            &[&oid, &typmod],
        )
        .await?;
    let ty = CopyType {
        oid,
        typmod,
        ty: ColumnType {
            type_name: row.get(0),
            typmod,
        },
        display: row.get(1),
        collatable: row.get(2),
    };
    described.insert((oid, typmod), ty.clone());
    Ok(ty)
}

/// The statements that bring every drifted copy in `states` to its live
/// type, one batch per table, each to run in its own transaction: an
/// `alter table … alter column … type … using …`, which takes `ACCESS
/// EXCLUSIVE` on the table and rewrites it when the change isn't
/// binary-compatible (`integer` to `bigint`; a `varchar` widening only
/// changes the catalog). A group-delta table's partition column is
/// generated from its `GROUP BY` columns, so it is generated again
/// ([`super::ledger::deltas_retype_sql`]). Each comes with the labels of the
/// copies it re-types, for messages. Empty when nothing drifted.
pub(crate) fn retype_statements(states: &[CopyState]) -> Vec<(String, Vec<String>)> {
    let mut by_table: BTreeMap<&str, Vec<&CopyState>> = BTreeMap::new();
    for state in states.iter().filter(|s| s.drifted()) {
        by_table.entry(&state.copy.table).or_default().push(state);
    }
    by_table
        .into_iter()
        .map(|(table, drifted)| {
            let alter = drifted
                .iter()
                .map(|s| {
                    let column = quote_ident(&s.copy.column);
                    let collate = match &s.collation {
                        Some(collation) if s.live_type.collatable => {
                            format!(" collate {collation}")
                        }
                        _ => String::new(),
                    };
                    format!(
                        "alter column {column} type {ty}{collate} using {column}::{ty}",
                        ty = s.live_type.display
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = if drifted[0].copy.kind == CopyKind::DeltasGroup {
                let group_columns: Vec<super::ledger::LedgerColumn> = states
                    .iter()
                    .filter(|s| s.copy.table == table)
                    .map(|s| super::ledger::LedgerColumn {
                        name: s.copy.column.clone(),
                        pg_type: s.live_type.display.clone(),
                        collation: None,
                    })
                    .collect();
                super::ledger::deltas_retype_sql(table, &group_columns, &alter)
            } else {
                format!("alter table {table} {alter}")
            };
            (sql, drifted.iter().map(|s| s.copy.label()).collect())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(kind: CopyKind, table: &str, column: &str, from: &str, to: &str) -> CopyState {
        let ty = |display: &str, oid| CopyType {
            oid,
            typmod: -1,
            ty: ColumnType {
                type_name: format!("pg_catalog.{display}"),
                typmod: -1,
            },
            display: display.to_string(),
            collatable: false,
        };
        CopyState {
            copy: TypedCopy {
                kind,
                table: table.to_string(),
                table_name: table.replace('"', ""),
                column: column.to_string(),
                reads: vec![("public.s".to_string(), column.to_string())],
                inferred: None,
            },
            copy_type: ty(from, if from == "integer" { 23 } else { 20 }),
            live_type: ty(to, if to == "integer" { 23 } else { 20 }),
            collation: None,
        }
    }

    #[test]
    fn only_drifted_copies_are_re_typed_one_statement_per_table() {
        let states = [
            state(
                CopyKind::TargetKey,
                "\"public\".\"t\"",
                "id",
                "integer",
                "bigint",
            ),
            state(
                CopyKind::Passthrough,
                "\"public\".\"t\"",
                "n",
                "bigint",
                "bigint",
            ),
            state(
                CopyKind::Passthrough,
                "\"public\".\"t\"",
                "m",
                "integer",
                "bigint",
            ),
        ];
        let statements = retype_statements(&states);
        assert_eq!(
            statements,
            vec![(
                "alter table \"public\".\"t\" alter column \"id\" type bigint using \"id\"::bigint, \
                 alter column \"m\" type bigint using \"m\"::bigint"
                    .to_string(),
                vec!["public.t.id".to_string(), "public.t.m".to_string()],
            )]
        );
    }

    #[test]
    fn a_delta_table_generates_its_partition_again() {
        let states = [
            state(
                CopyKind::DeltasGroup,
                "\"public\".\"t__deltas\"",
                "g",
                "integer",
                "bigint",
            ),
            state(
                CopyKind::DeltasGroup,
                "\"public\".\"t__deltas\"",
                "h",
                "bigint",
                "bigint",
            ),
        ];
        let statements = retype_statements(&states);
        assert_eq!(statements.len(), 1);
        let sql = &statements[0].0;
        assert!(
            sql.starts_with("alter table \"public\".\"t__deltas\" drop column \"__part\";"),
            "{sql}"
        );
        assert!(
            sql.contains("alter column \"g\" type bigint using \"g\"::bigint;"),
            "{sql}"
        );
        assert!(
            sql.contains("hash_record_extended(row(\"g\", \"h\"), 0)"),
            "{sql}"
        );
        assert!(sql.ends_with("(\"__part\", \"__seq\")"), "{sql}");
    }

    /// The source columns an expression-typed column reads: each once,
    /// through the substitution of another field's name, and none for
    /// `COUNT(*)` or a literal.
    #[test]
    fn an_expression_reads_each_source_column_once() {
        let def = crate::defs::parser::parse(
            "TRANSFORM t FROM s GROUP BY k \
             SELECT k AS k, SUM(a) AS total, total + MIN(a) + MAX(b) AS mixed, COUNT(*) AS n",
        )
        .expect("parse");
        let substituted = crate::defs::backfill::substituted_field_exprs(&def).expect("subst");
        let columns: HashMap<String, ValueType> = ["k", "a", "b", "c"]
            .into_iter()
            .map(|c| (c.to_string(), ValueType::Numeric))
            .collect();
        let reads = |field: &str| {
            let mut out = Vec::new();
            expr_reads(&substituted[field], "public.s", &columns, &[], &mut out);
            out
        };
        let read = |c: &str| ("public.s".to_string(), c.to_string());
        assert_eq!(reads("total"), vec![read("a")]);
        assert_eq!(reads("mixed"), vec![read("a"), read("b")]);
        assert!(reads("n").is_empty());
    }

    #[test]
    fn expression_typed_columns_are_re_typed_with_their_table() {
        let states = [
            state(
                CopyKind::AggregateField,
                "\"public\".\"t\"",
                "least",
                "integer",
                "bigint",
            ),
            state(
                CopyKind::Contribution,
                "\"public\".\"t__ledger\"",
                "__arg0",
                "integer",
                "bigint",
            ),
            state(
                CopyKind::Contribution,
                "\"public\".\"t__ledger\"",
                "__arg1",
                "bigint",
                "bigint",
            ),
        ];
        assert_eq!(
            retype_statements(&states),
            vec![
                (
                    "alter table \"public\".\"t\" alter column \"least\" type bigint \
                     using \"least\"::bigint"
                        .to_string(),
                    vec!["public.t.least".to_string()],
                ),
                (
                    "alter table \"public\".\"t__ledger\" alter column \"__arg0\" type bigint \
                     using \"__arg0\"::bigint"
                        .to_string(),
                    vec!["public.t__ledger.__arg0".to_string()],
                ),
            ]
        );
    }

    /// A re-type keeps the copy's own collation (a 1-1 key keeps its
    /// source key's, #769): `alter column … type` would otherwise give the
    /// column its new type's default, and rebuild its index. A type that
    /// takes none gets none.
    #[test]
    fn a_re_type_keeps_the_copy_s_collation() {
        let mut key = state(
            CopyKind::TargetKey,
            "\"public\".\"t\"",
            "code",
            "varchar(10)",
            "varchar(40)",
        );
        key.copy_type.typmod = 14;
        key.live_type.typmod = 44;
        key.collation = Some("pg_catalog.\"C\"".to_string());
        key.live_type.collatable = true;
        assert_eq!(
            retype_statements(std::slice::from_ref(&key))[0].0,
            "alter table \"public\".\"t\" alter column \"code\" type varchar(40) \
             collate pg_catalog.\"C\" using \"code\"::varchar(40)"
        );
        key.live_type.collatable = false;
        assert_eq!(
            retype_statements(std::slice::from_ref(&key))[0].0,
            "alter table \"public\".\"t\" alter column \"code\" type varchar(40) \
             using \"code\"::varchar(40)"
        );
    }

    #[test]
    fn nothing_drifted_means_nothing_to_run() {
        let states = [state(
            CopyKind::GroupBy,
            "\"public\".\"t\"",
            "g",
            "bigint",
            "bigint",
        )];
        assert!(retype_statements(&states).is_empty());
    }
}
