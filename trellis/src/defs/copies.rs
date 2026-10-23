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
//! - a `JOIN` target's `GROUP BY` columns, and the same columns of its
//!   hidden `T` table, typed as the column of the side each comes from, and
//!   each side's hidden partials table's `GROUP BY` and join-key columns and
//!   the hidden `J` table's join key, typed as that side's columns
//!   ([`super::join::hidden_tables_ddl`]), each with its collation.
//!
//! Others are typed by an expression over source columns, as define's type
//! inference ([`super::validate::infer_field_types`]) gives it from the
//! source's column types:
//!
//! - a 1-1 target's calculated field (`qty + 1`);
//! - an aggregate or `JOIN` target's field column (`SUM(qty)`, `MIN(qty)`);
//! - an aggregate ledger's contribution column, typed as the aggregate's
//!   argument ([`super::ledger::contribution_pg_type`]).
//!
//! A copy has its source column's type with no type modifier, and the
//! column's collation (ADR-0020, [`super::copy_type`]): a `numeric(10,2)` is
//! copied as `numeric`, a `varchar(n)` as `varchar`, a `char(n)` as
//! `bpchar`. A modifier constrains what the source can hold and plays no
//! part in a comparison, so the copy holds whatever the source column holds
//! after any change of its modifier, and a `text` and `varchar(n)` source
//! column are one family ([`CopyState::drifted`]).
//!
//! None of them changes type when the source column does. After the source
//! column widens to a wider type (`integer` to `bigint`, `real` to `double
//! precision`, `bigint` to `numeric`), the first value the column can't hold
//! fails its write (`22003`). So the staging worker's capture pass compares
//! each column with the type define would give it from the live schema
//! ([`inspect`]), and pauses every definition that owns a column the source
//! has outgrown ([`super::key_types::widens`],
//! `staging::schema_change::pause_readers_of_retyped`). A resume brings
//! every column whose type differs from define's to it
//! ([`retype_statements`]), keeping its collation, before it rebuilds,
//! except a `JOIN`'s hidden tables, which it drops and creates again at the
//! live types ([`CopyKind::recreated_by_resume`]).
//!
//! Trellis never re-types a column on its own. A narrowing, or a change to
//! another type family, pauses nothing here and is re-typed only by a
//! resume: every value still fits, or define's own checks own it.
//!
//! The comparison costs a few queries per definition, so the capture pass
//! runs it for a definition only when a table it reads has changed since a
//! comparison found every column current (`staging::drift_memo`, [`inputs`]).
//!
//! The ledger's `__from_key` is always `text`, and an aggregate's or a
//! `JOIN`'s hidden partials (`numeric` sums, `bigint` counts) have fixed
//! types, so none of them is checked.

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
    /// A `JOIN` target's `GROUP BY` column, typed as its side's column.
    JoinGroupBy,
    /// A `JOIN`'s hidden-table column (`P`, `M`, `J` or `T`), typed as the
    /// side's column it holds.
    JoinHidden,
}

impl CopyKind {
    /// Whether a resume drops and recreates the copy's table at the live
    /// types (a `JOIN`'s hidden tables), so it re-types nothing there:
    /// `J` keeps every key it has seen, so a narrowed key column's
    /// `ALTER` could fail on a key whose rows are gone.
    pub(crate) fn recreated_by_resume(self) -> bool {
        self == CopyKind::JoinHidden
    }

    /// Whether the copy is a key of its table's rows: a target's, a
    /// ledger's or a group-delta table's `GROUP BY` column, a 1-1 target's
    /// key, a projection's key or group column. Such a column can't be
    /// nulled in place ([`retype_statements`]).
    fn is_key(self) -> bool {
        matches!(
            self,
            CopyKind::TargetKey
                | CopyKind::GroupBy
                | CopyKind::LedgerGroup
                | CopyKind::DeltasGroup
                | CopyKind::ProjectionKey
                | CopyKind::ProjectionGroupBy
                | CopyKind::JoinGroupBy
        )
    }

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
        KeySpace::Join(_) => return join_copies(client, schema, def, source, target).await,
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
                let Some(name) = field.passthrough_column(&def.fields) else {
                    continue;
                };
                if !source_columns.contains(name) {
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

/// [`typed_copies`] for a `JOIN` definition, `def`, reading `source` (its
/// `FROM` table, qualified) into `target` (qualified): its target's `GROUP
/// BY` columns, its hidden tables' columns typed from a side's
/// ([`super::join::hidden_tables_ddl`], in `schema`, the catalog schema),
/// and its target's other fields, typed by
/// [`super::join::field_pg_type`] from both sides' live columns. A field
/// is left out while the definition doesn't validate against them, as
/// [`inferred_copies`] leaves one out.
async fn join_copies(
    client: &impl GenericClient,
    schema: &str,
    def: &TransformDef,
    source: &str,
    target: &str,
) -> Result<Vec<TypedCopy>, tokio_postgres::Error> {
    use super::join::{HiddenTable, Side};
    let Some(row) = client
        .query_opt(
            "select id, join_table from transform_definitions \
             where target_table = $1 and join_table is not null",
            &[&target],
        )
        .await?
    else {
        return Ok(Vec::new());
    };
    let (id, joined): (i64, String) = (row.get(0), row.get(1));
    let side_table = |side: Side| match side {
        Side::From => source.to_string(),
        Side::Joined => joined.clone(),
    };
    let hidden = |table: HiddenTable| {
        let name = table.name(id);
        (
            format!("{}.{}", quote_ident(schema), quote_ident(&name)),
            format!("{schema}.{name}"),
        )
    };
    let target_table = (
        super::ddl::qualified_target_table_ident(target),
        target.to_string(),
    );
    // One copy per table and column: a side's `GROUP BY` column may be its
    // join key too.
    let mut copies: Vec<TypedCopy> = Vec::new();
    let mut push = |kind, (table, table_name): (String, String), side: Side, column: &str| {
        if copies
            .iter()
            .any(|c| c.table == table && c.column == column)
        {
            return;
        }
        copies.push(TypedCopy {
            kind,
            table,
            table_name,
            column: column.to_string(),
            reads: vec![(side_table(side), column.to_string())],
            inferred: None,
        });
    };
    for (side, column) in super::join::group_by_sides(def) {
        push(CopyKind::JoinGroupBy, target_table.clone(), side, &column);
        push(
            CopyKind::JoinHidden,
            hidden(HiddenTable::TargetPartials),
            side,
            &column,
        );
    }
    for (side, table) in [
        (Side::From, HiddenTable::FromPartials),
        (Side::Joined, HiddenTable::JoinedPartials),
    ] {
        for column in super::join::side_key_columns(def, side) {
            push(CopyKind::JoinHidden, hidden(table), side, &column);
        }
    }
    // `J` is typed from the `FROM` side's join key: define holds each `ON`
    // pair to one type.
    for column in super::join::join_key(def, Side::From) {
        push(
            CopyKind::JoinHidden,
            hidden(HiddenTable::Matches),
            Side::From,
            &column,
        );
    }
    let sides =
        match super::catalog::JoinSides::read(client, source.to_string(), joined.clone()).await {
            Ok(sides) => sides,
            Err(super::catalog::CatalogError::Db(err)) => return Err(err),
            Err(_) => return Ok(copies),
        };
    let group_by: Vec<String> = super::join::group_by_sides(def)
        .into_iter()
        .map(|(_, column)| column)
        .collect();
    for field in &def.fields {
        if group_by.contains(&field.name) {
            continue;
        }
        let reads: Vec<(String, String)> = super::join::expr_columns(def, &field.expr)
            .into_iter()
            .map(|(side, column)| (side_table(side), column))
            .collect();
        if reads.is_empty() {
            continue;
        }
        let Ok(inferred) =
            super::join::field_pg_type(def, field, &sides.from_columns, &sides.joined_columns)
        else {
            continue;
        };
        copies.push(TypedCopy {
            kind: CopyKind::AggregateField,
            table: target_table.0.clone(),
            table_name: target_table.1.clone(),
            column: field.name.clone(),
            reads,
            inferred: Some(inferred),
        });
    }
    Ok(copies)
}

/// Every relation [`typed_copies`] and [`inspect`] read for `def`, which
/// reads `source` (qualified) into `target` (qualified), as SQL resolves
/// them (`to_regclass` text): the source, the to-side of each of `rels`, the
/// target and, for an aggregate, its ledger and group-delta table, and
/// `projections`, the projection tables (bare names, in `schema`) of `rels`.
/// For a `JOIN`, `join` is its definition id and its joined table
/// (qualified), which adds that table and the hidden tables.
/// A superset of the tables the copies are in and read from: the capture
/// pass fingerprints these to skip the comparison while none changed
/// (`staging::drift_memo`, #858). A table `typed_copies` starts reading
/// belongs here.
pub(crate) fn inputs(
    schema: &str,
    def: &TransformDef,
    source: &str,
    target: &str,
    rels: &[&RelationshipDefinition],
    projections: &[&str],
    join: Option<(i64, &str)>,
) -> Vec<String> {
    let mut names = vec![
        super::ddl::regclass_arg(source),
        super::ddl::qualified_target_table_ident(target),
    ];
    if let Some((id, joined)) = join {
        names.push(super::ddl::regclass_arg(joined));
        names.extend(
            super::join::HiddenTable::ALL
                .iter()
                .map(|table| format!("{}.{}", quote_ident(schema), quote_ident(&table.name(id)))),
        );
    }
    names.extend(
        rels.iter()
            .map(|r| super::ddl::regclass_arg(&r.qualified_to_table())),
    );
    if matches!(def.key_space, KeySpace::Aggregate { .. }) {
        let (target_schema, target_bare) = target.split_once('.').unwrap_or(("", target));
        names.push(super::ledger::qualified_ledger_table(
            target_schema,
            target_bare,
        ));
        names.push(super::ledger::qualified_deltas_table(
            target_schema,
            target_bare,
        ));
    }
    names.extend(
        projections
            .iter()
            .map(|p| super::ddl::qualified_relationship_projection_table(schema, p)),
    );
    names.sort();
    names.dedup();
    names
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
        // Never reached: `typed_copies` returns before it for a JOIN.
        KeySpace::Join(_) => {}
        KeySpace::OneToOne => {
            for field in &def.fields {
                // A bare passthrough is a copy of its column, as
                // `ddl::target_table_ddl` decides one is.
                let passthrough = field
                    .passthrough_column(&def.fields)
                    .is_some_and(|name| source_columns.contains_key(name));
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
        Expr::Column(name) | Expr::SourceColumn { column: name, .. }
            if source_columns.contains_key(name) =>
        {
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
        | Expr::SourceColumn { .. }
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
    /// `format_type`'s rendering, with the modifier: for messages only.
    pub display: String,
    /// The type as a copy declares and casts to it: no modifier, qualified
    /// ([`super::copy_type::type_sql`]).
    pub sql: String,
    /// Whether the type takes a collation.
    pub collatable: bool,
    /// Whether it is an enum type, whose values are its own labels
    /// (ADR-0020): no value of one enum is a value of another.
    pub is_enum: bool,
}

/// A [`TypedCopy`] with its own type now and the type define would give it
/// from the live schema.
#[derive(Debug, Clone)]
pub(crate) struct CopyState {
    pub copy: TypedCopy,
    pub copy_type: CopyType,
    pub live_type: CopyType,
    /// The modifier of the source column the copy is of, when it is a copy of
    /// one: what `live_type` leaves out (a copy has none, ADR-0020), and what
    /// tells an integer outgrown by a bounded `numeric` from one that isn't.
    pub source_typmod: i32,
    /// The copy's collation, quoted and qualified, when it isn't its type's
    /// default: a 1-1 target's key keeps its source key's (#769). A re-type
    /// keeps it.
    pub collation: Option<String>,
}

impl CopyState {
    /// Whether the copy's type is no longer the one define would create.
    ///
    /// `text` and `varchar` are one family (#1207): Trellis's copy of a
    /// `varchar(n)` column is an unbounded `varchar`, so a source column
    /// that changes between `text` and `varchar(n)`, or between bounds,
    /// leaves every copy as it should be.
    pub(crate) fn drifted(&self) -> bool {
        if self.copy_type.ty.is_string() && self.live_type.ty.is_string() {
            return false;
        }
        (self.copy_type.oid, self.copy_type.typmod) != (self.live_type.oid, self.live_type.typmod)
    }

    /// Whether a re-type to its live type discards the values the copy holds
    /// rather than casting them: its type is one enum and its live type
    /// another (#1327). No value of the old type is one of the new, and a
    /// stored label is trusted only under its own type's value identity
    /// (ADR-0020), so the resume's rebuild refills the copy from the source.
    pub(crate) fn discards_values(&self) -> bool {
        self.copy_type.is_enum && self.live_type.is_enum && self.drifted()
    }

    /// Whether the source widened past what the column holds: the type
    /// define would give it now holds values its type can't
    /// ([`super::key_types::widens`]).
    pub(crate) fn outgrown(&self) -> bool {
        let source = ColumnType {
            type_oid: self.live_type.oid,
            type_name: self.live_type.ty.type_name.clone(),
            typmod: self.source_typmod,
        };
        super::key_types::widens(&self.copy_type.ty, &source)
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
        let mut source_typmod = -1;
        let live_type = match &copy.inferred {
            Some(name) => match regtype(client, &mut named, name).await? {
                Some(oid) => (oid, -1),
                None => continue,
            },
            None => {
                let Some(&((source_oid, typmod), _)) = sources.get(&i) else {
                    continue;
                };
                source_typmod = typmod;
                if copy.kind.by_family() {
                    let value_type = super::pg_type::value_type_for_oid(client, source_oid).await?;
                    let name = super::ddl::pg_type_name(value_type);
                    match regtype(client, &mut named, &name).await? {
                        Some(oid) => (oid, -1),
                        None => continue,
                    }
                } else {
                    // A copy has the source column's type and no modifier
                    // (ADR-0020), so a source that only changes its modifier
                    // leaves every copy as it should be.
                    (source_oid, -1)
                }
            }
        };
        let copy_type = describe(client, &mut described, copy_type).await?;
        let live_type = describe(client, &mut described, live_type).await?;
        states.push(CopyState {
            copy,
            copy_type,
            live_type,
            source_typmod,
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
pub(crate) async fn describe(
    client: &impl GenericClient,
    described: &mut BTreeMap<(u32, i32), CopyType>,
    (oid, typmod): (u32, i32),
) -> Result<CopyType, tokio_postgres::Error> {
    if let Some(known) = described.get(&(oid, typmod)) {
        return Ok(known.clone());
    }
    let row = client
        .query_one(
            &format!(
                "select n.nspname::text || '.' || t.typname::text, \
                        pg_catalog.format_type(t.oid, $2), t.typcollation <> 0, {}, \
                        t.typtype = 'e' \
                 from pg_catalog.pg_type t \
                 join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
                 where t.oid = $1",
                super::copy_type::type_sql("t.oid")
            ),
            &[&oid, &typmod],
        )
        .await?;
    let ty = CopyType {
        oid,
        typmod,
        ty: ColumnType {
            type_oid: oid,
            type_name: row.get(0),
            typmod,
        },
        display: row.get(1),
        collatable: row.get(2),
        sql: row.get(3),
        is_enum: row.get(4),
    };
    described.insert((oid, typmod), ty.clone());
    Ok(ty)
}

/// The statements that bring every drifted copy in `states` to its live
/// type, one batch per table, each to run in its own transaction: an
/// `alter table … alter column … type … using …`, which takes `ACCESS
/// EXCLUSIVE` on the table and rewrites it when the change isn't
/// binary-compatible (`integer` to `bigint`; a `numeric` precision widening
/// at the same scale only changes the catalog). A group-delta table's partition column is
/// generated from its `GROUP BY` columns, so it is generated again
/// ([`super::ledger::deltas_retype_sql`]). Each comes with the labels of the
/// copies it re-types, for messages. Empty when nothing drifted.
///
/// A copy re-typed from one enum to another casts no stored value
/// ([`CopyState::discards_values`], #1327): it is re-typed `using null`,
/// and the resume's rebuild refills it from the source. A key copy
/// ([`CopyKind::is_key`]) can't be nulled in place, which would merge every
/// group onto one key, so the table's rows with a key of the old type are
/// deleted first, in the same transaction, and the rebuild writes each
/// back.
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
                    let using = if s.discards_values() {
                        "null".to_string()
                    } else {
                        column.clone()
                    };
                    format!(
                        "alter column {column} type {ty}{collate} using {using}::{ty}",
                        ty = s.live_type.sql
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let discarded_keys: Vec<String> = drifted
                .iter()
                .filter(|s| s.discards_values() && s.copy.kind.is_key())
                .map(|s| format!("{} is not null", quote_ident(&s.copy.column)))
                .collect();
            let delete = if discarded_keys.is_empty() {
                String::new()
            } else {
                format!(
                    "delete from {table} where {}; ",
                    discarded_keys.join(" or ")
                )
            };
            let sql = if drifted[0].copy.kind == CopyKind::DeltasGroup {
                let group_columns: Vec<super::ledger::LedgerColumn> = states
                    .iter()
                    .filter(|s| s.copy.table == table)
                    .map(|s| super::ledger::LedgerColumn {
                        name: s.copy.column.clone(),
                        pg_type: s.live_type.sql.clone(),
                        collation: None,
                    })
                    .collect();
                super::ledger::deltas_retype_sql(table, &group_columns, &alter)
            } else {
                format!("alter table {table} {alter}")
            };
            let sql = format!("{delete}{sql}");
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
                type_oid: oid,
                type_name: format!("pg_catalog.{display}"),
                typmod: -1,
            },
            display: display.to_string(),
            sql: display.to_string(),
            collatable: false,
            is_enum: false,
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
            source_typmod: -1,
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

    /// #1327: a copy re-typed from one enum to another casts no stored
    /// label. A passthrough is re-typed `using null`; a key's rows with a
    /// value are deleted first, in the same batch, and a `NULL` group keeps
    /// its row. A non-enum copy beside them is still cast.
    #[test]
    fn a_re_type_between_enums_discards_the_values_and_a_key_s_rows() {
        let as_enum = |mut s: CopyState, from: (&str, u32), to: (&str, u32)| {
            for (ty, (name, oid)) in [(&mut s.copy_type, from), (&mut s.live_type, to)] {
                ty.oid = oid;
                ty.ty.type_oid = oid;
                ty.ty.type_name = name.to_string();
                ty.sql = name.to_string();
                ty.display = name.to_string();
                ty.is_enum = true;
            }
            s
        };
        let tone = ("public.tone", 70_001);
        let mood = ("public.mood", 70_002);
        let passthrough = as_enum(
            state(CopyKind::Passthrough, "\"public\".\"c\"", "v", "x", "x"),
            tone,
            mood,
        );
        let widened = state(
            CopyKind::Passthrough,
            "\"public\".\"c\"",
            "n",
            "integer",
            "bigint",
        );
        assert_eq!(
            retype_statements(&[passthrough, widened])[0].0,
            "alter table \"public\".\"c\" alter column \"v\" type public.mood \
             using null::public.mood, alter column \"n\" type bigint using \"n\"::bigint"
        );
        let group = as_enum(
            state(CopyKind::GroupBy, "\"public\".\"g\"", "k", "x", "x"),
            tone,
            mood,
        );
        assert_eq!(
            retype_statements(&[group])[0].0,
            "delete from \"public\".\"g\" where \"k\" is not null; \
             alter table \"public\".\"g\" alter column \"k\" type public.mood \
             using null::public.mood"
        );
        // An enum re-recorded under the same type casts nothing either way:
        // nothing drifted.
        let same = as_enum(
            state(CopyKind::GroupBy, "\"public\".\"g\"", "k", "x", "x"),
            tone,
            tone,
        );
        assert!(retype_statements(&[same]).is_empty());
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
            "integer",
            "bigint",
        );
        key.collation = Some("pg_catalog.\"C\"".to_string());
        key.live_type.collatable = true;
        assert_eq!(
            retype_statements(std::slice::from_ref(&key))[0].0,
            "alter table \"public\".\"t\" alter column \"code\" type bigint \
             collate pg_catalog.\"C\" using \"code\"::bigint"
        );
        key.live_type.collatable = false;
        assert_eq!(
            retype_statements(std::slice::from_ref(&key))[0].0,
            "alter table \"public\".\"t\" alter column \"code\" type bigint \
             using \"code\"::bigint"
        );
    }

    /// `text` and `varchar(n)` are one family (#1207): a copy of either is
    /// unbounded, so a source column changing between them, or between
    /// bounds, drifts nothing and re-types nothing.
    #[test]
    fn a_string_column_never_drifts_between_text_and_varchar() {
        let string = |name: &str, oid, typmod| CopyType {
            oid,
            typmod,
            ty: ColumnType {
                type_oid: oid,
                type_name: format!("pg_catalog.{name}"),
                typmod,
            },
            display: name.to_string(),
            sql: name.to_string(),
            collatable: true,
            is_enum: false,
        };
        let mut s = state(CopyKind::Passthrough, "\"public\".\"t\"", "c", "x", "y");
        for (copy, live) in [
            (string("varchar", 1043, -1), string("text", 25, -1)),
            (string("text", 25, -1), string("varchar", 1043, 12)),
            (string("varchar", 1043, -1), string("varchar", 1043, 12)),
            (string("varchar", 1043, 9), string("varchar", 1043, 12)),
        ] {
            s.copy_type = copy;
            s.live_type = live;
            assert!(!s.drifted() && !s.outgrown());
            assert!(retype_statements(std::slice::from_ref(&s)).is_empty());
        }
        s.live_type = CopyType {
            oid: 23,
            typmod: -1,
            ty: ColumnType {
                type_oid: 23,
                type_name: "pg_catalog.int4".to_string(),
                typmod: -1,
            },
            display: "integer".to_string(),
            sql: "integer".to_string(),
            collatable: false,
            is_enum: false,
        };
        assert!(s.drifted(), "another family still drifts");
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
