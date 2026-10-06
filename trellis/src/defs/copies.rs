//! The columns Trellis keeps a typed copy of (issue #767).
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
//!   to-side's join column.
//!
//! None of them changes type when the source column does. After the source
//! column widens (`integer` to `bigint`, `varchar(50)` to `text`), the
//! first value the copy can't hold fails its write (`22003`, `22001`). So
//! the staging worker's capture pass pauses every definition that owns a
//! copy its source column has outgrown ([`super::key_types::widens`],
//! `staging::schema_change::pause_readers_of_retyped`), and a resume brings
//! each copy to the type define would create from the live schema
//! ([`retype`]) before it rebuilds. Trellis never re-types a copy on its own.
//!
//! The ledger's `__from_key` is always `text`, so it has no copy to check,
//! and a calculated field's column is typed by its expression, not copied.

use std::collections::{BTreeMap, HashMap, HashSet};

use tokio_postgres::GenericClient;

use super::ast::{Expr, GroupByKey, KeySpace, TransformDef};
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

/// One column Trellis keeps a typed copy of a source column in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypedCopy {
    pub kind: CopyKind,
    /// The table holding the copy, quoted and qualified: ready for SQL text
    /// and for `to_regclass`.
    pub table: String,
    /// The same table as messages name it, `schema.table`.
    pub table_name: String,
    pub column: String,
    /// The qualified, unquoted table whose column it copies.
    pub source_table: String,
    pub source_column: String,
}

impl TypedCopy {
    /// `schema.table.column`, as messages name it.
    pub(crate) fn label(&self) -> String {
        format!("{}.{}", self.table_name, self.column)
    }
}

/// Every typed copy definition `def` keeps, read from `source` (qualified)
/// into `target` (qualified). `rels` are the relationships declared on
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
            source_table: from.to_string(),
            source_column: from_col.to_string(),
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
    let read: HashSet<String> = super::eval::relationship_references(def)
        .into_iter()
        .map(|(rel, _)| rel)
        .collect();
    for rel in rels
        .iter()
        .filter(|r| r.cardinality == RelationshipCardinality::ToOne && read.contains(&r.def.name))
    {
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
        copies.push(copy(
            CopyKind::ProjectionKey,
            &super::ddl::qualified_relationship_projection_table(schema, &projection),
            &format!("{schema}.{projection}"),
            &rel.def.to_col,
            &rel.qualified_to_table(),
            &rel.def.to_col,
        ));
    }
    Ok(copies)
}

/// A column type: its OID and modifier, its `schema.typname` and modifier
/// ([`ColumnType`]), and `format_type`'s rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyType {
    pub oid: u32,
    pub typmod: i32,
    pub ty: ColumnType,
    pub display: String,
}

/// A [`TypedCopy`] with its own type now and the type define would give it
/// from the live schema.
#[derive(Debug, Clone)]
pub(crate) struct CopyState {
    pub copy: TypedCopy,
    pub copy_type: CopyType,
    pub live_type: CopyType,
}

impl CopyState {
    /// Whether the copy's type is no longer the one define would create.
    pub(crate) fn drifted(&self) -> bool {
        (self.copy_type.oid, self.copy_type.typmod) != (self.live_type.oid, self.live_type.typmod)
    }

    /// Whether the source column widened past what the copy holds
    /// ([`super::key_types::widens`]).
    pub(crate) fn outgrown(&self) -> bool {
        super::key_types::widens(&self.copy_type.ty, &self.live_type.ty)
    }
}

/// The types of each of `copies`, and of the column each copies, as
/// [`CopyState`]s. A copy whose table or column, or whose source column, no
/// longer exists is left out.
pub(crate) async fn inspect(
    client: &impl GenericClient,
    copies: Vec<TypedCopy>,
) -> Result<Vec<CopyState>, tokio_postgres::Error> {
    if copies.is_empty() {
        return Ok(Vec::new());
    }
    let column_types = |tables: Vec<String>, columns: Vec<String>| async move {
        let rows = client
            .query(
                "select k.i, a.atttypid, a.atttypmod \
                 from unnest($1::text[], $2::text[]) with ordinality as k(t, c, i) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = pg_catalog.to_regclass(k.t) and a.attname = k.c \
                  and a.attnum > 0 and not a.attisdropped",
                &[&tables, &columns],
            )
            .await?;
        Ok::<_, tokio_postgres::Error>(
            rows.into_iter()
                .map(|row| {
                    let i: i64 = row.get(0);
                    (i as usize - 1, (row.get::<_, u32>(1), row.get::<_, i32>(2)))
                })
                .collect::<HashMap<usize, (u32, i32)>>(),
        )
    };
    let copied = column_types(
        copies.iter().map(|c| c.table.clone()).collect(),
        copies.iter().map(|c| c.column.clone()).collect(),
    )
    .await?;
    let sources = column_types(
        copies
            .iter()
            .map(|c| super::ddl::regclass_arg(&c.source_table))
            .collect(),
        copies.iter().map(|c| c.source_column.clone()).collect(),
    )
    .await?;

    let mut described: BTreeMap<(u32, i32), CopyType> = BTreeMap::new();
    let mut states = Vec::new();
    for (i, copy) in copies.into_iter().enumerate() {
        let (Some(&copy_type), Some(&(source_oid, source_typmod))) =
            (copied.get(&i), sources.get(&i))
        else {
            continue;
        };
        let live_type = if copy.kind.by_family() {
            let value_type = super::pg_type::value_type_for_oid(client, source_oid).await?;
            let name = super::ddl::pg_type_name(value_type);
            let oid: Option<u32> = client
                .query_one("select pg_catalog.to_regtype($1)::oid", &[&name.as_ref()])
                .await?
                .get(0);
            match oid {
                Some(oid) => (oid, -1),
                None => continue,
            }
        } else {
            (source_oid, source_typmod)
        };
        let copy_type = describe(client, &mut described, copy_type).await?;
        let live_type = describe(client, &mut described, live_type).await?;
        states.push(CopyState {
            copy,
            copy_type,
            live_type,
        });
    }
    Ok(states)
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
                    pg_catalog.format_type(t.oid, $2) \
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
                    format!(
                        "alter column {column} type {ty} using {column}::{ty}",
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
        };
        CopyState {
            copy: TypedCopy {
                kind,
                table: table.to_string(),
                table_name: table.replace('"', ""),
                column: column.to_string(),
                source_table: "public.s".to_string(),
                source_column: column.to_string(),
            },
            copy_type: ty(from, if from == "integer" { 23 } else { 20 }),
            live_type: ty(to, if to == "integer" { 23 } else { 20 }),
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
