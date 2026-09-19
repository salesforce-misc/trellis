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

use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::TransformStatus;

use crate::model::{Column, Op, PRIMARY_KEY_PG_TYPE, Relationship, Table};

/// Quotes a Postgres identifier for safe interpolation into SQL text,
/// mirroring `trellis::pool`'s own (crate-private) helper of the same name.
pub(super) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The Postgres type name a [`ValueType`] casts to.
pub(super) fn pg_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
        // Issue #108: the generator itself never emits an `Other`-typed
        // column (see `generate::Column`'s doc comment), but this mirrors
        // `trellis::defs::ddl::pg_type_name` for exhaustiveness/parity.
        ValueType::Other(pg_type) => pg_type.sql_type_name(),
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
    format!(
        "TRANSFORM {} FROM {}{key_space_clause} SELECT {}",
        def.target,
        def.source,
        fields.join(", ")
    )
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
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
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

/// Renders a [`Relationship`] back to the concrete
/// `RELATIONSHIP <name> FROM <table>.<col> TO <table>.<col>` syntax
/// `create_relationship` parses (ADR-0006). Cardinality is deliberately not
/// rendered — see `ManualBackend`'s former doc comment (git history) for why.
pub(super) fn render_relationship(rel: &Relationship) -> String {
    format!(
        "RELATIONSHIP {} FROM {}.{} TO {}.{}",
        rel.name, rel.from_table, rel.from_col, rel.to_table, rel.to_col
    )
}

pub(super) fn render_operator(op: Operator) -> &'static str {
    match op {
        Operator::Add => "+",
        Operator::GreaterThan => ">",
    }
}

/// `source_columns` for `table`, as `trellis::defs::install_definition` wants
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
/// [`create_source_table`] — the single source of truth every `$n::text::<type>`
/// cast renders from. A primary-key column resolves to [`PRIMARY_KEY_PG_TYPE`]
/// rather than to [`pg_type_name`] of its [`ValueType`] — see
/// `ManualBackend`'s former doc comment (git history) for why.
pub(super) fn column_pg_type(tables: &HashMap<String, Table>, table: &str, column: &str) -> &'static str {
    let is_pk = tables.get(table).is_some_and(|t| t.pk_col == column);
    if is_pk {
        return PRIMARY_KEY_PG_TYPE;
    }
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
/// key declared [`PRIMARY_KEY_PG_TYPE`], a single-column `UNIQUE` constraint
/// on every column named in `table.unique_cols`, and (unconditionally, issue
/// #34/task B4) `REPLICA IDENTITY FULL` — see `ManualBackend`'s former doc
/// comment (git history) for why every table gets full replica identity
/// rather than only ones an `Aggregate` def happens to source from.
pub(super) async fn create_source_table(
    raw: &tokio_postgres::Client,
    table: &Table,
) -> Result<(), tokio_postgres::Error> {
    let mut sql = format!("create table {} (", quote_ident(&table.name));
    for (i, column) in table.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_ident(&column.name));
        sql.push(' ');
        if column.name == table.pk_col {
            sql.push_str(PRIMARY_KEY_PG_TYPE);
            sql.push_str(" primary key");
        } else {
            sql.push_str(pg_type_name(column.value_type));
            if table.unique_cols.iter().any(|c| c == &column.name) {
                sql.push_str(" unique");
            }
        }
    }
    sql.push(')');
    raw.batch_execute(&sql).await?;

    raw.batch_execute(&format!(
        "alter table {} replica identity full",
        quote_ident(&table.name)
    ))
    .await?;
    Ok(())
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
                quote_ident(table)
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
                quote_ident(table),
                quote_ident(&pk_col),
                changes.len() + 1,
                PRIMARY_KEY_PG_TYPE,
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
                quote_ident(table),
                quote_ident(&pk_col),
                PRIMARY_KEY_PG_TYPE,
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
            let quoted = quote_ident(table);
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
                        format!("${}::text::{}", params.len(), column_pg_type(tables, table, col))
                    })
                    .collect();
                placeholder_groups.push(format!("({})", placeholders.join(", ")));
            }

            let sql = format!(
                "insert into {} ({column_list}) values {}",
                quote_ident(table),
                placeholder_groups.join(", ")
            );
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
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
) -> Result<std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>, tokio_postgres::Error>
{
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
) -> Result<std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>, tokio_postgres::Error>
{
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

/// [`await_definitions_settled`] timed out: `unsettled` names every target
/// table (`def.target`) whose `transform_definitions.status` never reached a
/// terminal backfill outcome within `waited`.
#[derive(Debug)]
pub(super) struct DefinitionSettleTimeout {
    pub(super) unsettled: Vec<String>,
    pub(super) waited: Duration,
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
        if !matches!(status, TransformStatus::Live | TransformStatus::Quarantined) {
            unsettled.push(def.target.clone());
        }
    }
    Ok(unsettled)
}

/// Polls every entry in `defs` until each has reached a terminal backfill
/// outcome (`live`/`quarantined`) or `timeout` elapses — see
/// `ManualBackend::await_definitions_settled`'s former doc comment (git
/// history) for why this closes a real gap in ring-only convergence
/// waiting (a direct-build 1-1 definition's backfill runs entirely outside
/// the ring).
pub(super) async fn await_definitions_settled(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    timeout: Duration,
) -> Result<(), DefinitionSettleTimeout> {
    const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
    const MAX_BACKOFF: Duration = Duration::from_millis(250);

    let started = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let unsettled = unsettled_definitions(raw, defs)
            .await
            .expect("transform_definitions is queryable");
        if unsettled.is_empty() {
            return Ok(());
        }
        let waited = started.elapsed();
        if waited >= timeout {
            return Err(DefinitionSettleTimeout { unsettled, waited });
        }
        tokio::time::sleep(backoff.min(timeout - waited)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::defs::parse;

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
}
