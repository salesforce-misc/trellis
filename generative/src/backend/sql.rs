//! Shared DDL/DML/read-back plumbing (issue #166): every piece of
//! [`ManualBackend`](super::ManualBackend)'s rendering and raw-SQL logic
//! that has nothing to do with *how* the engine itself is run — only with
//! turning a [`Program`]/[`Op`]/[`Snapshot`] into concrete SQL text and back
//! — lives here instead, so [`SubprocessBackend`](super::SubprocessBackend)
//! (issue #166's subprocess-supervised backend) can reuse it verbatim rather
//! than re-deriving the same DDL/DML shapes a second time. Split out of
//! `manual.rs` when `SubprocessBackend` was added; every function here was
//! previously a private `ManualBackend` method or free function, unchanged
//! in behavior, just parameterized over `&HashMap<String, Table>`/
//! `&tokio_postgres::Client` instead of closing over `&self`.
//!
//! Deliberately does **not** know anything about starting/stopping/
//! restarting an engine — that's each backend's own, genuinely different
//! concern (an in-process [`trellis::Client`] vs. a real OS subprocess
//! `Command`), and is the only reason two `Backend` impls exist at all. See
//! the `backend` module doc comment's "backend seam" note: this module is
//! part of that seam, reachable only from within `generative::backend`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use trellis::dev::defs::TransformStatus;
use trellis::dev::defs::ast::{
    Expr, FieldDef, GroupByKey, JoinKind, KeySpace, Operator, Predicate, QualifiedColumn,
    TransformDef, ValueType,
};
use trellis::dev::staging::{StagingError, await_converged, converged_through, watermark_token};

use crate::model::{Column, Op, PartitionAction, PartitionStrategy, Partitioning, Table};

/// Quotes a Postgres identifier for safe interpolation into SQL text,
/// mirroring `trellis::pool`'s own (crate-private) helper of the same name.
pub(super) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The Postgres type name a [`ValueType`] casts to.
///
/// Returns [`std::borrow::Cow`] rather than a bare `&'static str` since
/// issue #117, mirroring `defs::ddl::pg_type_name`'s own signature change:
/// every family but [`trellis::dev::defs::PgType::Enum`] still
/// renders a fixed keyword and stays `Cow::Borrowed` — the generator itself
/// never emits an `Other`-typed column at all (see `generate::Column`'s doc
/// comment), so the owned-`Cow` branch is unreachable here in practice, but
/// this stays parity-complete with the real `defs::ddl::pg_type_name` rather
/// than silently drifting out of sync with it.
pub(super) fn pg_type_name(value_type: ValueType) -> std::borrow::Cow<'static, str> {
    match value_type {
        ValueType::Numeric => "numeric".into(),
        // Issue #111: an exact integer renders as its own Postgres width,
        // mirroring `defs::ddl::pg_type_name`.
        ValueType::Integer(width) => width.pg_name().into(),
        // Issue #112: likewise `real`/`double precision`.
        ValueType::Float(width) => width.pg_name().into(),
        ValueType::Text => "text".into(),
        ValueType::Boolean => "boolean".into(),
        ValueType::Uuid => "uuid".into(),
        // Issue #108: the generator itself never emits an `Other`-typed
        // column (see `generate::Column`'s doc comment), but this mirrors
        // `defs::ddl::pg_type_name` for exhaustiveness/parity.
        ValueType::Other(pg_type) => pg_type.sql_type_name(),
    }
}

/// The declared scale of every generated [`ValueType::Numeric`] source column.
pub(super) const NUMERIC_COLUMN_TYPE: &str = "numeric(14,2)";

/// The type a generated source column is declared with: [`pg_type_name`],
/// except that a [`ValueType::Numeric`] column is declared
/// [`NUMERIC_COLUMN_TYPE`]. The group basis admits `SUM`/`AVG` only over a
/// `numeric` with a declared scale (issue #1100), and every aggregate this
/// generator draws reads such a column. The drawn values are small integers
/// (`generate::VALUE_MAX`, `GRAIN_MAX`), which `numeric(14,2)` stores
/// exactly; casts and typed literals keep the bare `numeric` of
/// [`pg_type_name`], which the column's assignment coerces.
pub(super) fn column_ddl_type(value_type: ValueType) -> std::borrow::Cow<'static, str> {
    match value_type {
        ValueType::Numeric => NUMERIC_COLUMN_TYPE.into(),
        other => pg_type_name(other),
    }
}

/// Renders `def` back to the concrete `TRANSFORM ... FROM ... SELECT ...`
/// syntax `create_definition`/`install_definition` parses — see
/// `ManualBackend`'s former doc comment on this function (preserved in git
/// history) for the full rationale; unchanged here.
pub(super) fn render_definition(def: &TransformDef) -> String {
    let fields: Vec<String> = def
        .fields
        .iter()
        .map(|field| format!("{} AS {}", render_expr(&field.expr), field.name))
        .collect();
    debug_assert_eq!(def.predicate, Predicate::True);
    let key_space_clause = match &def.key_space {
        // The JOIN tier (#1113): `FROM a [LEFT|INNER] JOIN b ON … GROUP BY …`.
        KeySpace::Join(join) => {
            let qualified = |c: &QualifiedColumn| format!("{}.{}", c.table, c.column);
            format!(
                " {} {} ON {} GROUP BY {}",
                match join.kind {
                    JoinKind::Inner => "INNER JOIN",
                    JoinKind::Left => "LEFT JOIN",
                },
                join.right,
                join.on
                    .iter()
                    .map(|on| format!("{} = {}", qualified(&on.lhs), qualified(&on.rhs)))
                    .collect::<Vec<_>>()
                    .join(" AND "),
                join.group_by
                    .iter()
                    .map(qualified)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        KeySpace::OneToOne => String::new(),
        KeySpace::Aggregate { group_by } => format!(
            " GROUP BY {}",
            group_by
                .iter()
                .map(|k| k.target_column_name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let opt_out = if def.without_source_index {
        " WITHOUT SOURCE INDEX"
    } else {
        ""
    };
    format!(
        "TRANSFORM {} FROM {}{key_space_clause}{opt_out} SELECT {}",
        def.target,
        def.source,
        fields.join(", ")
    )
}

/// The source indexes `def` needs (#1167), as `(table, leading columns)`: an
/// aggregate's `GROUP BY` columns on its source, and a `JOIN` side's `ON`
/// columns, in `ON` order, on each side. Asked of every definition whatever
/// the group-basis knob says, which only decides whether the engine requires
/// them: the index serves a `self_check` walk either way.
pub(super) fn required_source_indexes(def: &TransformDef) -> Vec<(String, Vec<String>)> {
    match &def.key_space {
        KeySpace::OneToOne => Vec::new(),
        KeySpace::Aggregate { group_by } => {
            let columns: Vec<String> = group_by
                .iter()
                .filter_map(|key| match key {
                    GroupByKey::Column(column) => Some(column.clone()),
                    GroupByKey::RelationshipPath { .. } => None,
                })
                .collect();
            if columns.len() == group_by.len() {
                vec![(def.source.clone(), columns)]
            } else {
                Vec::new()
            }
        }
        KeySpace::Join(join) => [def.source.as_str(), join.right.as_str()]
            .into_iter()
            .map(|table| {
                let columns = join
                    .on
                    .iter()
                    .filter_map(|term| {
                        [&term.lhs, &term.rhs]
                            .into_iter()
                            .find(|c| c.table == table)
                            .map(|c| c.column.clone())
                    })
                    .collect();
                (table.to_string(), columns)
            })
            .collect(),
    }
}

/// Creates the source indexes `def` needs ([`required_source_indexes`]) on
/// tables in `schema` (`None`: the session's `search_path`), if they don't
/// exist: the engine refuses a `GROUP BY` or `JOIN` definition without them.
pub(super) async fn create_source_indexes(
    raw: &tokio_postgres::Client,
    schema: Option<&str>,
    def: &TransformDef,
) -> Result<(), tokio_postgres::Error> {
    for (table, columns) in required_source_indexes(def) {
        let qualified = match schema {
            Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table)),
            None => quote_ident(&table),
        };
        let mut name = format!("gen_{table}_{}_idx", columns.join("_"));
        name.truncate(60);
        let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        raw.batch_execute(&format!(
            "create index if not exists {} on {qualified} ({})",
            quote_ident(&name),
            cols.join(", ")
        ))
        .await?;
    }
    Ok(())
}

/// Renders a generator-built [`Expr`] back to source text. Every operand of
/// a `BinaryOp` is unconditionally parenthesized (issue #67's reviewer
/// follow-up) so a rendered expression always re-parses to the exact same
/// tree regardless of operator precedence — see
/// `tests::render_expr_parenthesizes_nested_binary_ops_so_they_round_trip`
/// below.
pub(super) fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => match schema {
            Some(schema) => format!("{schema}.{table}.{column}"),
            None => format!("{table}.{column}"),
        },
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
        // Issue #109's typed literal, rendered in the `<type> '<text>'`
        // spelling rather than `CAST(... AS ...)`. Both re-parse to this
        // same node, so either would round-trip; the typed-literal form is
        // the shorter one and keeps the generated definition text closer to
        // how the docs spell it.
        Expr::TypedLiteral { value_type, text } => format!(
            "{} '{}'",
            pg_type_name(*value_type).to_ascii_uppercase(),
            text.replace('\'', "''")
        ),
        Expr::BinaryOp { op, lhs, rhs } => {
            format!(
                "({}) {} ({})",
                render_expr(lhs),
                render_operator(*op),
                render_expr(rhs)
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "COUNT(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let args: Vec<String> = args.iter().map(render_expr).collect();
            format!("{name}({})", args.join(", "))
        }
        Expr::RelationshipPath { rel, column } => format!("{rel}.{column}"),
    }
}

pub(super) fn render_operator(op: Operator) -> &'static str {
    match op {
        Operator::Add => "+",
        Operator::GreaterThan => ">",
    }
}

/// `source_columns` for `table`, as `trellis::dev::defs::install_definition` wants
/// it.
pub(super) fn source_columns(table: &Table) -> HashMap<String, ValueType> {
    table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type))
        .collect()
}

/// One row's placeholder assignment for an `INSERT`/`UPDATE` statement:
/// `column = $n::type` (or `column` for the column list), plus the bound
/// text value at that position.
pub(super) struct Assignment {
    pub(super) fragment: String,
    pub(super) value: Option<String>,
}

/// The Postgres type `column` on `table` was actually declared with in
/// [`create_source_table`] — the single source of truth every
/// `$n::text::<type>` cast renders from.
///
/// Since issue #111 this is just [`pg_type_name`] of the column's own
/// [`ValueType`], including for the primary key: `ValueType` can now name
/// `bigint`, so the PK no longer needs the override it carried while its
/// recorded type was a `Numeric` placeholder (see
/// `crate::model::PRIMARY_KEY_VALUE_TYPE`).
pub(super) fn column_pg_type(
    tables: &HashMap<String, Table>,
    table: &str,
    column: &str,
) -> std::borrow::Cow<'static, str> {
    pg_type_name(
        tables
            .get(table)
            .and_then(|t| t.columns.iter().find(|c| c.name == column))
            .map(|c| c.value_type)
            .unwrap_or(ValueType::Numeric),
    )
}

pub(super) fn assignment(
    tables: &HashMap<String, Table>,
    table: &str,
    column: &str,
    index: usize,
    value: &Option<String>,
) -> Assignment {
    Assignment {
        fragment: format!(
            "{}=${}::text::{}",
            quote_ident(column),
            index,
            column_pg_type(tables, table, column)
        ),
        value: value.clone(),
    }
}

/// Creates `table` as a source table: one column per [`Column`], the primary
/// key declared from its own [`crate::model::PRIMARY_KEY_VALUE_TYPE`]
/// (`bigint`), a single-column `UNIQUE` constraint
/// on every column named in `table.unique_cols`.
///
/// `schema` qualifies the table's name (`None` leaves it to the session's
/// `search_path`, which is the instance schema): a source table that more than
/// one Trellis instance reads is created in `public`, outside every
/// instance's catalog schema.
pub(super) async fn create_source_table(
    raw: &tokio_postgres::Client,
    schema: Option<&str>,
    table: &Table,
) -> Result<(), tokio_postgres::Error> {
    let name = match schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
        None => quote_ident(&table.name),
    };
    if let Some(partitioning) = &table.partitioning {
        raw.batch_execute(&partitioned_source_ddl(&name, table, partitioning))
            .await?;
        return Ok(());
    }
    let mut sql = format!("create table {name} (");
    for (i, column) in table.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_ident(&column.name));
        sql.push(' ');
        if column.name == table.pk_col {
            sql.push_str(&column_ddl_type(column.value_type));
            sql.push_str(" primary key");
        } else {
            sql.push_str(&column_ddl_type(column.value_type));
            if table.unique_cols.iter().any(|c| c == &column.name) {
                sql.push_str(" unique");
            }
        }
    }
    sql.push(')');
    raw.batch_execute(&sql).await?;
    Ok(())
}

/// The DDL of a partitioned source table (ADR-0017): the root, keyed by
/// `(pk_col, partition column)` with the partition column `NOT NULL`, and
/// its leaves, each named after the root with a `_p<n>` suffix (`_pd` for a
/// `DEFAULT` leaf).
fn partitioned_source_ddl(name: &str, table: &Table, partitioning: &Partitioning) -> String {
    assert!(
        table.unique_cols.is_empty(),
        "a partitioned table can't carry a single-column UNIQUE constraint — a generator bug"
    );
    let key = quote_ident(&partitioning.column);
    let mut columns: Vec<String> = table
        .columns
        .iter()
        .map(|column| {
            let mut sql = format!(
                "{} {}",
                quote_ident(&column.name),
                column_ddl_type(column.value_type)
            );
            if column.name == table.pk_col || column.name == partitioning.column {
                sql.push_str(" not null");
            }
            sql
        })
        .collect();
    columns.push(format!(
        "primary key ({}, {key})",
        quote_ident(&table.pk_col)
    ));
    let (method, leaves): (&str, Vec<String>) = match partitioning.strategy {
        PartitionStrategy::Hash { modulus } => (
            "hash",
            (0..modulus)
                .map(|r| format!("for values with (modulus {modulus}, remainder {r})"))
                .collect(),
        ),
        PartitionStrategy::List { values, default } => (
            "list",
            (0..values)
                .map(|v| format!("for values in ({v})"))
                .chain(default.then(|| "default".to_string()))
                .collect(),
        ),
        PartitionStrategy::Range { bounds } => (
            "range",
            (0..bounds)
                .map(|b| {
                    let from = if b == 0 {
                        "minvalue".to_string()
                    } else {
                        b.to_string()
                    };
                    let to = if b + 1 == bounds {
                        "maxvalue".to_string()
                    } else {
                        (b + 1).to_string()
                    };
                    format!("for values from ({from}) to ({to})")
                })
                .collect(),
        ),
    };
    let mut sql = format!(
        "create table {name} ({}) partition by {method} ({key});",
        columns.join(", ")
    );
    for (i, bound) in leaves.iter().enumerate() {
        let suffix = if bound == "default" {
            "pd".to_string()
        } else {
            format!("p{i}")
        };
        // The leaf sits beside the root: `name` is the root's (maybe
        // schema-qualified, quoted) name, so the leaf's is built from the
        // table's own bare name and the same qualifier.
        let leaf = match name.rsplit_once('.') {
            Some((schema, _)) => format!(
                "{schema}.{}",
                quote_ident(&format!("{}_{suffix}", table.name))
            ),
            None => quote_ident(&format!("{}_{suffix}", table.name)),
        };
        sql.push_str(&format!(
            " create table {leaf} partition of {name} {bound};"
        ));
    }
    sql
}

/// [`apply_op`]'s failure modes: either an unknown table — `apply_op` was
/// given an op naming a table `install` never created, same "generator bug,
/// not a condition to paper over" contract each backend's own
/// `UnknownTable` error variant documented before this was shared — or a
/// real database error. Each backend's own error enum wraps both via
/// `From`.
#[derive(Debug)]
pub(super) enum ApplyOpError {
    UnknownTable(String),
    Db(tokio_postgres::Error),
}

impl From<tokio_postgres::Error> for ApplyOpError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApplyOpError::Db(err)
    }
}

/// Applies one [`Op`] as raw source DML against `raw`, given the currently
/// tracked `tables` (for column/PK type resolution) — the shared body behind
/// both `ManualBackend::apply` and `SubprocessBackend::apply`. Returns the
/// statement's affected-row count, exactly like [`super::Backend::apply`]'s
/// own contract.
pub(super) async fn apply_op(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    op: &Op,
) -> Result<u64, ApplyOpError> {
    match op {
        Op::Leaf { leaf, op } => {
            assert!(
                !matches!(**op, Op::Leaf { .. } | Op::Partition { .. }),
                "apply_op: an Op::Leaf wraps a write — a generator bug"
            );
            apply_write(raw, tables, op, Some(leaf)).await
        }
        Op::Partition { table, action, .. } => {
            if !tables.contains_key(table) {
                return Err(ApplyOpError::UnknownTable(table.clone()));
            }
            raw.batch_execute(&partition_action_sql(tables, table, action))
                .await?;
            Ok(1)
        }
        _ => apply_write(raw, tables, op, None).await,
    }
}

/// The SQL of one [`PartitionAction`] on the root `table`, sent as one
/// simple-query string. A `Create`'s statements run as one implicit
/// transaction, so a refused bound leaves nothing behind (an explicit
/// `BEGIN` would leave the session in an aborted transaction instead).
fn partition_action_sql(
    tables: &HashMap<String, Table>,
    table: &str,
    action: &PartitionAction,
) -> String {
    let root = quote_ident(table);
    match action {
        PartitionAction::Detach { leaf, concurrently } => format!(
            "alter table {root} detach partition {}{}",
            quote_ident(leaf),
            if *concurrently { " concurrently" } else { "" }
        ),
        PartitionAction::Attach { leaf, bound } => {
            format!(
                "alter table {root} attach partition {} {bound}",
                quote_ident(leaf)
            )
        }
        PartitionAction::Drop { leaf } => format!("drop table {}", quote_ident(leaf)),
        PartitionAction::Create {
            leaf,
            bound,
            rows,
            standalone,
        } => {
            let quoted = quote_ident(leaf);
            let mut sql = String::new();
            if *standalone {
                sql.push_str(&format!("create table {quoted} (like {root}); "));
            } else {
                sql.push_str(&format!(
                    "create table {quoted} partition of {root} {bound}; "
                ));
            }
            for row in rows {
                let columns: Vec<String> = row.iter().map(|(c, _)| quote_ident(c)).collect();
                let values: Vec<String> = row
                    .iter()
                    .map(|(c, v)| match v {
                        Some(v) => {
                            format!("{}::{}", quote_literal(v), column_pg_type(tables, table, c))
                        }
                        None => "null".to_string(),
                    })
                    .collect();
                sql.push_str(&format!(
                    "insert into {quoted} ({}) values ({}); ",
                    columns.join(", "),
                    values.join(", ")
                ));
            }
            if *standalone {
                sql.push_str(&format!(
                    "alter table {root} attach partition {quoted} {bound}; "
                ));
            }
            sql
        }
    }
}

/// `value` as a single-quoted SQL string literal.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// [`apply_op`] for a single write: issued against `relation` when given (a
/// leaf of the op's partitioned table), else against the op's own table,
/// whose [`Table`] types every value either way.
async fn apply_write(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    op: &Op,
    relation: Option<&str>,
) -> Result<u64, ApplyOpError> {
    let relation = |table: &str| quote_ident(relation.unwrap_or(table));
    let affected = match op {
        Op::Insert { table, row, .. } => {
            let columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
            let assignments: Vec<Assignment> = row
                .iter()
                .enumerate()
                .map(|(i, (col, val))| Assignment {
                    fragment: format!("${}::text::{}", i + 1, column_pg_type(tables, table, col)),
                    value: val.clone(),
                })
                .collect();
            let column_list = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = assignments
                .iter()
                .map(|a| a.fragment.clone())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "insert into {} ({column_list}) values ({placeholders})",
                relation(table)
            );
            let params: Vec<Option<String>> = assignments.into_iter().map(|a| a.value).collect();
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
        }
        Op::Update {
            table, pk, changes, ..
        } => {
            let pk_col = tables
                .get(table)
                .map(|t| t.pk_col.clone())
                .ok_or_else(|| ApplyOpError::UnknownTable(table.clone()))?;
            let mut assignments = Vec::with_capacity(changes.len());
            for (i, (col, val)) in changes.iter().enumerate() {
                assignments.push(assignment(tables, table, col, i + 1, val));
            }
            let set_clause = assignments
                .iter()
                .map(|a| a.fragment.clone())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "update {} set {set_clause} where {}=${}::text::{}",
                relation(table),
                quote_ident(&pk_col),
                changes.len() + 1,
                column_pg_type(tables, table, &pk_col),
            );
            let mut params: Vec<Option<String>> =
                assignments.into_iter().map(|a| a.value).collect();
            params.push(Some(pk.clone()));
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
        }
        Op::Delete { table, pk, .. } => {
            let pk_col = tables
                .get(table)
                .map(|t| t.pk_col.clone())
                .ok_or_else(|| ApplyOpError::UnknownTable(table.clone()))?;
            let sql = format!(
                "delete from {} where {}=$1::text::{}",
                relation(table),
                quote_ident(&pk_col),
                column_pg_type(tables, table, &pk_col),
            );
            raw.execute(&sql, &[pk]).await?
        }
        Op::Truncate { table, .. } => {
            // Improvement-plan task E6's load-bearing gotcha: Postgres's
            // `TRUNCATE` command tag always reports `0` rows affected,
            // regardless of how many rows actually existed — trusting that
            // raw count into `run_convergence`'s `Ok(0) => AffectsNoRows`
            // classifier would misclassify every non-empty truncate as a
            // no-op. So the real row count is synthesized instead: a
            // `SELECT count(*)` in the *same transaction* as the `TRUNCATE`,
            // taken before it runs, so nothing can slip a concurrent write in
            // between the count and the clear.
            let quoted = relation(table);
            let txn = raw.transaction().await?;
            let count_row = txn
                .query_one(&format!("select count(*) from {quoted}"), &[])
                .await?;
            let count: i64 = count_row.get(0);
            txn.batch_execute(&format!("truncate table {quoted}"))
                .await?;
            txn.commit().await?;
            count as u64
        }
        Op::BulkInsert { table, rows, .. } => {
            let Some(first_row) = rows.first() else {
                panic!(
                    "apply_op: Op::BulkInsert against table {table:?} carries no rows — a \
                     generator bug"
                );
            };
            let columns: Vec<&str> = first_row.iter().map(|(c, _)| c.as_str()).collect();
            let column_list = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");

            let mut placeholder_groups = Vec::with_capacity(rows.len());
            let mut params: Vec<Option<String>> = Vec::with_capacity(rows.len() * columns.len());
            for row in rows {
                let row_columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
                assert_eq!(
                    row_columns, columns,
                    "apply_op: every Op::BulkInsert row must carry the same columns in the same \
                     order as the first row — a generator bug (table {table:?})"
                );
                let placeholders: Vec<String> = row
                    .iter()
                    .map(|(col, val)| {
                        params.push(val.clone());
                        format!(
                            "${}::text::{}",
                            params.len(),
                            column_pg_type(tables, table, col)
                        )
                    })
                    .collect();
                placeholder_groups.push(format!("({})", placeholders.join(", ")));
            }

            let sql = format!(
                "insert into {} ({column_list}) values {}",
                relation(table),
                placeholder_groups.join(", ")
            );
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
        }
        Op::Leaf { .. } | Op::Partition { .. } => {
            unreachable!("apply_op handles these before apply_write")
        }
    };
    Ok(affected)
}

/// Reads `qualified_table` back as text, ordered by `pk_col`, into `pk ->
/// column -> value`.
pub(super) async fn read_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    pk_col: &str,
    columns: &[Column],
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>,
    tokio_postgres::Error,
> {
    let select_list = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select {select_list} from {qualified_table} order by {}",
        quote_ident(pk_col)
    );
    let rows = client.query(&sql, &[]).await?;

    let mut result = std::collections::BTreeMap::new();
    for row in rows {
        let mut by_column = std::collections::BTreeMap::new();
        let pk_value: Option<String> = row.get(0);
        let pk_value = pk_value.expect("primary key column is never NULL");
        for (i, column) in columns.iter().enumerate() {
            by_column.insert(column.name.clone(), row.get::<_, Option<String>>(i));
        }
        result.insert(pk_value, by_column);
    }
    Ok(result)
}

/// Reads an `Aggregate` key-space target table back as text, keyed by the
/// same composite [`crate::model::group_key`] convention the SQL/evaluator
/// oracles use — see `ManualBackend`'s former doc comment (git history) for
/// the full rationale; unchanged here.
pub(super) async fn read_aggregate_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    group_by: &[String],
    fields: &[FieldDef],
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>,
    tokio_postgres::Error,
> {
    let value_fields: Vec<&str> = fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(|name| !group_by.iter().any(|g| g == name))
        .collect();

    let mut select_list: Vec<String> = group_by
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect();
    select_list.extend(
        value_fields
            .iter()
            .map(|c| format!("{}::text", quote_ident(c))),
    );
    let sql = format!("select {} from {qualified_table}", select_list.join(", "));
    let rows = client.query(&sql, &[]).await?;

    let mut result = std::collections::BTreeMap::new();
    for row in rows {
        let group_values: Vec<Option<String>> = (0..group_by.len())
            .map(|i| row.get::<_, Option<String>>(i))
            .collect();
        let key = crate::model::group_key(&group_values);
        let mut by_column = std::collections::BTreeMap::new();
        for (i, name) in value_fields.iter().enumerate() {
            by_column.insert(
                (*name).to_string(),
                row.get::<_, Option<String>>(group_by.len() + i),
            );
        }
        result.insert(key, by_column);
    }
    Ok(result)
}

/// Why [`quiesce`] gave up.
#[derive(Debug)]
pub(super) enum QuiesceError {
    /// A status or marker query itself failed — a dropped connection, a
    /// catalog table that isn't there. Propagated, deliberately not panicked
    /// on: the proptest-driven harness above `Backend::quiesce`
    /// (`run::check_program`'s callers) depends on a structured, shrinkable
    /// failure rather than an unwind from inside a wait loop.
    Db(tokio_postgres::Error),
    /// The ring's own wait failed: taking the watermark token, or
    /// [`await_converged`] (a query failure or its named
    /// `ConvergenceTimeout`).
    Staging(StagingError),
    /// `unsettled` names every target table (`def.target`) whose
    /// `transform_definitions.status` hadn't reached a terminal backfill
    /// outcome when the budget ran out after `waited`.
    DefinitionsUnsettled {
        unsettled: Vec<String>,
        waited: Duration,
    },
    /// `unsettled` names every partitioned source (ADR-0017) whose tree the
    /// reconcile pass hadn't recorded yet, or that still had a correction
    /// queued, when the budget ran out after `waited`.
    PartitionsUnsettled {
        unsettled: Vec<String>,
        waited: Duration,
    },
}

impl From<tokio_postgres::Error> for QuiesceError {
    fn from(err: tokio_postgres::Error) -> Self {
        QuiesceError::Db(err)
    }
}

impl From<StagingError> for QuiesceError {
    fn from(err: StagingError) -> Self {
        QuiesceError::Staging(err)
    }
}

/// The target tables of every `defs` entry whose current
/// `transform_definitions.status` is neither `live` nor `quarantined` — see
/// `ManualBackend::unsettled_definitions`'s former doc comment (git history)
/// for the full rationale behind this query shape.
async fn unsettled_definitions(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
) -> Result<Vec<String>, tokio_postgres::Error> {
    let mut unsettled = Vec::new();
    for def in defs {
        let Some(row) = raw
            .query_opt(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&def.target],
            )
            .await?
        else {
            unsettled.push(def.target.clone());
            continue;
        };
        let status_text: String = row.get(0);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        // `Paused` joins the terminal set for the same reason `Quarantined`
        // is in it (issue #142): a frozen definition never progresses on its
        // own, so waiting for it to settle any further is waiting forever.
        // The suite doesn't pause anything today — this is here so that if it
        // ever does, the wait fails its assertion rather than hanging.
        if !matches!(
            status,
            TransformStatus::Live | TransformStatus::Quarantined | TransformStatus::Paused
        ) {
            unsettled.push(def.target.clone());
        }
    }
    Ok(unsettled)
}

/// Every partitioned source (ADR-0017) whose partition maintenance a target
/// may not reflect yet: its tree as the catalog has it now (each node's oid
/// and `pg_inherits.xmin`, which an attach, detach, drop or new leaf
/// changes) isn't the tree the reconcile pass last recorded, or a correction
/// is still queued for it. The pass records the tree and queues the
/// corrections in one transaction, and a correction leaves the queue once
/// every definition reading the source has applied it, so a source that
/// passes this has no maintenance left to correct. A definition reads
/// `live` throughout the first part (until the pass sees the DDL) and the
/// queued part of the second (until the pass schedules the correction), so
/// definition status alone can't show either.
///
/// It reads the engine's recorded tree as `capture::partitions` writes it
/// (`nodes.nodes[]`, each `oid` and `inherits_xmin` as `xmin::text`). A
/// change to that format leaves every source unsettled, so a quiesce times
/// out naming it (`partition_maintenance_converges_end_to_end` fails); it
/// can't make one look settled early.
async fn unsettled_partitions(
    raw: &tokio_postgres::Client,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = raw
        .query(
            "select s.table_name from partition_sources s \
             where (select array_agg((n->>'oid') || ':' || coalesce(n->>'inherits_xmin', '') \
                                     order by (n->>'oid')::bigint) \
                    from jsonb_array_elements(s.nodes->'nodes') n) \
                   is distinct from \
                   (select array_agg(t.relid::oid::bigint::text || ':' \
                                     || coalesce(i.xmin::text, '') \
                                     order by t.relid::oid::bigint) \
                    from pg_catalog.pg_partition_tree(s.root_oid::oid) t \
                    left join pg_catalog.pg_inherits i on i.inhrelid = t.relid) \
             union \
             select table_name from partition_corrections \
             order by 1",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Polls `probe` every [`SETTLE_POLL`] until it returns an empty list or
/// `started + timeout` passes, returning the last non-empty list and the time
/// waited on timeout.
async fn poll_until_empty<F, Fut>(
    started: Instant,
    timeout: Duration,
    mut probe: F,
) -> Result<Result<(), (Vec<String>, Duration)>, tokio_postgres::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<String>, tokio_postgres::Error>>,
{
    loop {
        let found = probe().await?;
        if found.is_empty() {
            return Ok(Ok(()));
        }
        let waited = started.elapsed();
        if waited >= timeout {
            return Ok(Err((found, waited)));
        }
        tokio::time::sleep(SETTLE_POLL.min(timeout - waited)).await;
    }
}

/// How often [`quiesce`] re-reads definition status.
/// Each read is one cheap catalog query on the test's own isolated database,
/// so a short fixed interval costs nothing and keeps a settle from
/// overshooting the moment it happens.
const SETTLE_POLL: Duration = Duration::from_millis(20);

/// Waits, within one `timeout` budget, until the engine has no work left
/// that could still change a target (issue #432). Shared by both backends'
/// `Backend::quiesce`.
///
/// A two-way check, the same one an operator makes (ADR-0002, "What `live`
/// promises"): every definition reports `live` (or a terminal `paused` /
/// `quarantined`), then the convergence wait on a fresh token returns. Each
/// covers work the other can't see:
///
/// - **Live status.** A direct-build definition's backfill runs through
///   `defs::chunk_queue`, entirely outside the ring (ADR-0002),
///   and a marker-driven one sits in
///   `waiting_to_backfill`/`backfilling` until its enumeration commits. A
///   finished build, or a `live` definition given a catch-up of its own,
///   reports `catching_up` until that catch-up's discharge has run (#476).
///   Only `transform_definitions.status` shows any of these. It used to take
///   a third check, that no `pending_backfill` marker was left, while a
///   chunked or direct build still flipped `live` with its catch-up only
///   parked; #476 made `live` wait for the discharge, and the check went.
/// - **Caught up to the token.** [`await_converged`] against a token taken
///   after the mutations of interest: the public read-your-writes path, so
///   every quiesce in the suite exercises it (issue #452).
///
/// - **Partition maintenance corrected** (ADR-0017). An attach, detach,
///   drop or new leaf reaches a target only through the correction the next
///   reconcile pass queues, and a leaf `TRUNCATE` through the one its seal
///   queues; until the pass schedules it, the definition still reads `live`.
///   [`unsettled_partitions`] waits for the pass to record the tree and for
///   the queue to empty, ahead of the status check, and is checked again at
///   the end, since the ring wait is what seals a truncate's marker.
///
/// The order is what makes this sound. Status first, then the ring: a
/// definition reports `live` only once the enumeration that took it there
/// has committed its `Recompute` rows to the ring, in the same transaction
/// as the flip, and those rows carry a NULL `origin_lsn`, which gates *any*
/// token. So the convergence wait, started only then, covers them. Run the
/// other way around (as it was before #432), the ring wait can pass on an
/// empty ring before an enumeration appends to it, and the status wait then
/// returns the moment the definition flips `live` with its rows still
/// undrained.
///
/// After the ring drains, the status check runs once more, and then the ring
/// once more against the same token (a definition can drop to `catching_up`
/// and be flipped back in between, leaving only its enumeration behind).
/// The whole sequence repeats if either finds work again rather than
/// returning on stale evidence. A definition can drop to `catching_up` after
/// it is `live`: a stale chunk given up after a rebuild went live parks a
/// catch-up for it (`defs::chunk_queue`).
pub(super) async fn quiesce(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    timeout: Duration,
) -> Result<(), QuiesceError> {
    let started = Instant::now();
    loop {
        poll_until_empty(started, timeout, || unsettled_partitions(raw))
            .await?
            .map_err(|(unsettled, waited)| QuiesceError::PartitionsUnsettled {
                unsettled,
                waited,
            })?;
        poll_until_empty(started, timeout, || unsettled_definitions(raw, defs))
            .await?
            .map_err(|(unsettled, waited)| QuiesceError::DefinitionsUnsettled {
                unsettled,
                waited,
            })?;

        let token = watermark_token(raw).await?;
        await_converged(raw, token, timeout.saturating_sub(started.elapsed())).await?;

        let unsettled = unsettled_definitions(raw, defs).await?;
        if unsettled.is_empty() && unsettled_partitions(raw).await?.is_empty() {
            // A catch-up parked and discharged between the ring wait and the
            // read above leaves its definition `live` again, with only its
            // enumeration in the ring. Those rows have no origin, so they
            // gate every token, `token` included, so this one check fails
            // only if such rows appeared.
            if converged_through(raw, token).await? {
                return Ok(());
            }
            continue;
        }
        // Every wait above checks at least once even with the budget spent,
        // so without this a state that keeps reappearing would loop forever.
        let waited = started.elapsed();
        if waited >= timeout {
            return Err(QuiesceError::DefinitionsUnsettled { unsettled, waited });
        }
    }
}

/// One pass of [`quiesce`]'s two checks, in its order, without waiting on
/// either (issue #453): every definition settled, then the ring converged
/// through a token taken after that. A caller that has to act between
/// checks (seal by hand, say) polls this and then confirms with [`quiesce`].
pub(super) async fn settled(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
) -> Result<bool, QuiesceError> {
    if !unsettled_partitions(raw).await?.is_empty()
        || !unsettled_definitions(raw, defs).await?.is_empty()
    {
        return Ok(false);
    }
    let token = watermark_token(raw).await?;
    Ok(converged_through(raw, token).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::dev::defs::parse;

    /// A partitioned source keys on `(pk, partition column)`, both `NOT
    /// NULL`, and its leaves cover every value each strategy routes.
    #[test]
    fn a_partitioned_source_keys_on_its_partition_column_and_routes_every_value() {
        let table = Table {
            name: "t0".to_string(),
            pk_col: "c0".to_string(),
            columns: vec![
                Column {
                    name: "c0".to_string(),
                    value_type: crate::model::PRIMARY_KEY_VALUE_TYPE,
                },
                Column {
                    name: "g".to_string(),
                    value_type: crate::model::PRIMARY_KEY_VALUE_TYPE,
                },
            ],
            unique_cols: Vec::new(),
            partitioning: None,
        };
        let ddl = |strategy| {
            partitioned_source_ddl(
                "\"public\".\"t0\"",
                &table,
                &Partitioning {
                    column: "g".to_string(),
                    strategy,
                },
            )
        };
        assert_eq!(
            ddl(PartitionStrategy::Range { bounds: 2 }),
            "create table \"public\".\"t0\" (\"c0\" bigint not null, \"g\" bigint not null, \
             primary key (\"c0\", \"g\")) partition by range (\"g\"); \
             create table \"public\".\"t0_p0\" partition of \"public\".\"t0\" \
             for values from (minvalue) to (1); \
             create table \"public\".\"t0_p1\" partition of \"public\".\"t0\" \
             for values from (1) to (maxvalue);"
        );
        assert!(
            ddl(PartitionStrategy::List {
                values: 1,
                default: true
            })
            .ends_with(
                "\"t0_p0\" partition of \"public\".\"t0\" for values in (0); \
                 create table \"public\".\"t0_pd\" partition of \"public\".\"t0\" default;"
            )
        );
        assert!(
            ddl(PartitionStrategy::Hash { modulus: 2 })
                .ends_with("for values with (modulus 2, remainder 1);")
        );
    }

    /// The reviewer-flagged follow-up to issue #67 (real operator
    /// precedence): a nested, mixed-operator `Expr` must round-trip through
    /// `render_expr` and back through the real parser to the exact same
    /// tree, not a reflowed one. See the pre-#166 `manual.rs` history for
    /// the original, more detailed version of this doc comment.
    #[test]
    fn render_expr_parenthesizes_nested_binary_ops_so_they_round_trip() {
        let expr = Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column("b".to_string())),
                rhs: Box::new(Expr::Column("c".to_string())),
            }),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(
            def.fields[0].expr, expr,
            "round-tripping through render_expr -> parse must reproduce the exact original tree"
        );
    }

    /// Issue #830: a qualified source column inside an aggregate renders
    /// back to the same qualified spelling, so a generated definition that
    /// carries one re-parses to the same tree.
    #[test]
    fn render_expr_round_trips_a_qualified_source_column_in_an_aggregate() {
        for (schema, prefix) in [(None, "s"), (Some("app"), "app.s")] {
            let expr = Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![Expr::SourceColumn {
                    schema: schema.map(str::to_string),
                    table: "s".to_string(),
                    column: "v".to_string(),
                }],
            };
            let rendered = render_expr(&expr);
            assert_eq!(rendered, format!("SUM({prefix}.v)"));
            let text = format!("TRANSFORM t FROM s GROUP BY g SELECT g AS g, {rendered} AS total");
            let def = parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            assert_eq!(def.fields[1].expr, expr);
        }
    }

    /// #1113: every `JOIN` shape the JOIN tier draws renders back to text
    /// that parses to the same definition.
    #[test]
    fn render_definition_round_trips_a_join() {
        use crate::generate::join::{JoinDefSpec, JoinField, JoinGroups, JoinKey, join_definition};
        for left in [true, false] {
            for key in [JoinKey::Single, JoinKey::Pair] {
                for groups in [JoinGroups::Joined, JoinGroups::Both, JoinGroups::From] {
                    let def = join_definition(
                        "j",
                        &JoinDefSpec {
                            left,
                            key,
                            groups,
                            fields: JoinField::ALL.to_vec(),
                        },
                    );
                    let text = render_definition(&def);
                    let reparsed = parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
                    assert_eq!(reparsed, def, "{text}");
                }
            }
        }
    }

    /// The mirror shape, pinned so a future change to `render_expr` can't
    /// quietly regress this direction while only testing the other one.
    #[test]
    fn render_expr_round_trips_a_greater_than_wrapping_an_add() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            }),
            rhs: Box::new(Expr::Column("c".to_string())),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(def.fields[0].expr, expr);
    }

    // --- `quiesce` (issue #432) ---
    //
    // No engine runs in these tests. Each stands the engine's side up by hand
    // on a second session, in the order the engine itself commits it, so the
    // interleaving that used to let `quiesce` return early is exact rather
    // than left to timing. Replication progress is faked far ahead, so the
    // ring's convergence predicate turns only on what the ring holds.

    const SOURCE: &str = "public.s";

    async fn session(dsn: &str) -> tokio_postgres::Client {
        let config = trellis::Config::from_dsn(dsn.to_string()).expect("config");
        let (client, connection) = tokio_postgres::connect(config.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("set search_path to {}, public", config.schema()))
            .await
            .expect("set search_path");
        client
    }

    /// One definition `t` on `SOURCE`, catalogued with `status`, and the
    /// harness's view of it.
    async fn catalog_definition(client: &tokio_postgres::Client, status: &str) -> TransformDef {
        client
            .batch_execute(&format!(
                "insert into source_table_versions (source_table, version) values ('{SOURCE}', 1); \
                 insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ('public.t', '{SOURCE}', 1, 'TRANSFORM t FROM s SELECT a AS a', '{status}')"
            ))
            .await
            .expect("catalog the definition");
        parse("TRANSFORM t FROM s SELECT a AS a").expect("parse")
    }

    /// Stages what an enumeration stages: an image-less `recompute` row,
    /// NULL `origin_lsn`, in the active slot (`seg_0` on a fresh ring).
    const STAGE_ENUMERATION: &str =
        "insert into seg_0 (src_table, key, op) values ('public.s', '1', 'recompute')";

    /// Stands in for the drain: empties the active slot. Returns the instant
    /// just before it started, so a `quiesce` that waited for it can't have
    /// returned earlier.
    async fn drain_ring(client: &tokio_postgres::Client) -> Instant {
        let drained_at = Instant::now();
        client
            .execute("delete from seg_0", &[])
            .await
            .expect("drain");
        drained_at
    }

    /// A marker-driven build commits its enumeration and only then flips the
    /// definition `live`, in a later transaction. The ring wait must start
    /// after the flip, or it passes on an empty ring and the status wait
    /// returns at the flip with the enumeration still undrained.
    #[tokio::test]
    async fn quiesce_drains_an_enumeration_committed_before_its_definition_went_live() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "backfilling").await;

        let engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            engine.execute(STAGE_ENUMERATION, &[]).await.expect("stage");
            engine
                .execute("update transform_definitions set status = 'live'", &[])
                .await
                .expect("go live");
            tokio::time::sleep(Duration::from_millis(300)).await;
            drain_ring(&engine).await
        });

        quiesce(&raw, &[def], Duration::from_secs(30))
            .await
            .expect("quiesce");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(
            returned_at >= drained_at,
            "quiesce returned before the enumeration staged ahead of the go-live drained"
        );
    }

    /// A finished build reports `catching_up` until its go-live catch-up
    /// has run (#476); the discharge commits the catch-up's enumeration and
    /// the flip to `live` together. `quiesce` waits for the flip and then for
    /// that enumeration to drain.
    #[tokio::test]
    async fn quiesce_waits_for_a_catching_up_definition_and_its_enumeration() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "catching_up").await;

        let mut engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            discharge_catch_up(&mut engine).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            drain_ring(&engine).await
        });

        quiesce(&raw, &[def], Duration::from_secs(30))
            .await
            .expect("quiesce");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(
            returned_at >= drained_at,
            "quiesce returned before the catch-up discharged and its enumeration drained"
        );
    }

    /// A `live` definition drops to `catching_up` while `quiesce` is already
    /// waiting on the ring (a stale chunk parks a catch-up after a rebuild
    /// went live). The ring draining must not end the wait: the re-check has
    /// to find the definition catching up and wait out its discharge and
    /// that enumeration's drain too.
    #[tokio::test]
    async fn quiesce_waits_for_a_catch_up_parked_while_it_waited_on_the_ring() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "live").await;
        // An earlier enumeration, still undrained, so `quiesce` passes the
        // status check at once and settles into the ring wait.
        raw.execute(STAGE_ENUMERATION, &[])
            .await
            .expect("stage the earlier enumeration");

        let mut engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            engine
                .execute(
                    "update transform_definitions set status = 'catching_up'",
                    &[],
                )
                .await
                .expect("park a catch-up");
            tokio::time::sleep(Duration::from_millis(200)).await;
            drain_ring(&engine).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            discharge_catch_up(&mut engine).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            drain_ring(&engine).await
        });

        quiesce(&raw, &[def], Duration::from_secs(30))
            .await
            .expect("quiesce");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(
            returned_at >= drained_at,
            "quiesce returned on the first drain, with a catch-up parked during its ring wait \
             still undischarged"
        );
    }

    /// A catch-up that never discharges is a named timeout, not a hang and
    /// not a silent success.
    #[tokio::test]
    async fn quiesce_names_a_definition_still_catching_up_on_timeout() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "catching_up").await;

        match quiesce(&raw, &[def], Duration::from_millis(300)).await {
            Err(QuiesceError::DefinitionsUnsettled { unsettled, .. }) => {
                assert_eq!(unsettled, vec!["t".to_string()]);
            }
            other => panic!("expected DefinitionsUnsettled, got {other:?}"),
        }
    }

    /// A catch-up's discharge: its enumeration and the definition's flip to
    /// `live` commit together.
    async fn discharge_catch_up(engine: &mut tokio_postgres::Client) {
        let txn = engine.transaction().await.expect("begin");
        txn.execute(STAGE_ENUMERATION, &[]).await.expect("stage");
        txn.execute("update transform_definitions set status = 'live'", &[])
            .await
            .expect("go live");
        txn.commit().await.expect("commit");
    }

    /// Issue #1320: the partition model's `HASH` routing table is the one
    /// Postgres computes, for every modulus and grain it covers.
    #[tokio::test]
    async fn the_hash_remainders_are_postgres_own() {
        use crate::generate::partition::{HASH_REMAINDERS, PARTITION_GRAIN_MAX};
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        raw.batch_execute("create table h (g bigint not null) partition by hash (g)")
            .await
            .expect("create");
        for (m, row) in HASH_REMAINDERS.iter().enumerate() {
            let modulus = m as i32 + 1;
            for (grain, &remainder) in row.iter().enumerate() {
                let found: i32 = raw
                    .query_one(
                        "select r from generate_series(0, $1 - 1) r \
                         where satisfies_hash_partition('h'::regclass, $1, r, $2::bigint)",
                        &[&modulus, &(grain as i64)],
                    )
                    .await
                    .expect("remainder")
                    .get(0);
                assert_eq!(
                    found,
                    i32::from(remainder),
                    "modulus {modulus}, grain {grain} (of 0..={PARTITION_GRAIN_MAX})"
                );
            }
        }
    }

    /// Issue #1320: a partitioned source is unsettled while its tree differs
    /// from the one recorded (here: a leaf attached since), and while a
    /// correction is queued for it.
    #[tokio::test]
    async fn a_partitioned_source_is_unsettled_until_recorded_and_corrected() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        raw.batch_execute(
            "create table public.r (g bigint not null) partition by list (g); \
             create table public.r0 partition of public.r for values in (0);",
        )
        .await
        .expect("create");
        let record = "insert into partition_sources \
                          (table_name, root_oid, lifecycle_fp, compliance_fp, nodes, \
                           catalog_oid) \
                      select 'public.r', 'public.r'::regclass::oid::bigint, '', '', \
                             jsonb_build_object('nodes', jsonb_agg(jsonb_build_object( \
                                 'oid', t.relid::oid::bigint, \
                                 'inherits_xmin', i.xmin::text))), \
                             pg_catalog.to_regclass('partition_sources') \
                      from pg_partition_tree('public.r') t \
                      left join pg_inherits i on i.inhrelid = t.relid \
                      on conflict (table_name) do update set nodes = excluded.nodes";
        raw.batch_execute(record).await.expect("record");
        assert!(unsettled_partitions(&raw).await.expect("probe").is_empty());

        raw.batch_execute("create table public.r1 partition of public.r for values in (1)")
            .await
            .expect("attach");
        assert_eq!(
            unsettled_partitions(&raw).await.expect("probe"),
            vec!["public.r".to_string()]
        );
        raw.batch_execute(record).await.expect("record again");
        assert!(unsettled_partitions(&raw).await.expect("probe").is_empty());

        raw.batch_execute(
            "insert into partition_corrections (table_name, kind, node_oid, node_name, cause) \
             values ('public.r', 'new_leaf', 'public.r1'::regclass::oid::bigint, \
                     'public.r1', 'attached')",
        )
        .await
        .expect("queue");
        assert_eq!(
            unsettled_partitions(&raw).await.expect("probe"),
            vec!["public.r".to_string()]
        );
    }
}
