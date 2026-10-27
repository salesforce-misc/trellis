//! A 1-1 definition's fields rendered to SQL over a source row
//! (ADR-0002 "Apply" and "Re-derive"): Postgres evaluates them in the
//! statement that writes the target row, and Rust computes no values.
//!
//! The row is a text-valued jsonb object, a column's value its `::text`
//! rendering: a staged image (`staging::append`), or the live row the
//! Re-derive read renders under the entry lock (`one_to_one_ledger::read_rows`).
//! [`OneToOneRender::row_sql`] casts each column the fields read back to the
//! source column's own type, with its own collation, so an expression over the
//! row means what it means over the source table. The cast reads back what
//! Postgres itself wrote, under the same pinned output settings
//! (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`), so the value is the
//! source's value by construction.
//!
//! A field reads another by alias through its substituted expression
//! (`defs::backfill::substitute_all_fields`), as the Re-derive build's
//! chunks render it (`build::one_to_one`). A paused field, and every field
//! reading one by alias, is left out (ADR-0003).
//!
//! A data exception in a field (SQLSTATE class 22: an overflow, an invalid
//! regex, an out-of-range cast) fails the whole statement. The drain's
//! isolation bisects the page down to the record that fails alone
//! (`quarantine::isolate_and_evict`), and [`failing_fields`] names its
//! columns for the column fuse.

use std::collections::{BTreeSet, HashSet};

use tokio_postgres::GenericClient;

use crate::defs::ast::{Expr, TransformDef};
use crate::defs::ddl;
use crate::pool::{quote_ident, quote_literal};

use super::apply::ApplyError;

/// One source column as a row reads it back: its name, its type as
/// `format_type` spells it (with its modifier), and its collation when it
/// differs from its type's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImageColumn {
    pub name: String,
    pub type_sql: String,
    pub collation: Option<String>,
}

/// Every column of `table`, in column order, as [`ImageColumn`]s.
pub(crate) async fn image_columns(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<ImageColumn>, ApplyError> {
    let rows = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    case when a.attcollation <> 0 and a.attcollation <> t.typcollation \
                         then pg_catalog.quote_ident(n.nspname) || '.' \
                              || pg_catalog.quote_ident(c.collname) end \
             from pg_catalog.pg_attribute a \
             join pg_catalog.pg_type t on t.oid = a.atttypid \
             left join pg_catalog.pg_collation c on c.oid = a.attcollation \
             left join pg_catalog.pg_namespace n on n.oid = c.collnamespace \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped \
             order by a.attnum",
            &[&ddl::regclass_arg(table)],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ImageColumn {
            name: row.get(0),
            type_sql: row.get(1),
            collation: row.get(2),
        })
        .collect())
}

/// A 1-1 definition's written fields as SQL over the source row `s`, and
/// the source columns they read.
#[derive(Debug, Clone)]
pub(crate) struct OneToOneRender {
    /// Each written field's name and its SQL over `s`, in definition order.
    pub fields: Vec<(String, String)>,
    /// The source columns the fields read, in column order.
    columns: Vec<ImageColumn>,
}

impl OneToOneRender {
    /// `def`'s fields but the `paused` ones (already closed over their alias
    /// readers, `quarantine::paused_columns_for`), over the source whose
    /// columns are `source_columns` ([`image_columns`]).
    pub(crate) fn new(
        def: &TransformDef,
        source_columns: &[ImageColumn],
        paused: &HashSet<String>,
    ) -> Result<Self, ApplyError> {
        let substituted = crate::defs::backfill::substitute_all_fields(def)?;
        let mut read: BTreeSet<String> = BTreeSet::new();
        let mut fields = Vec::with_capacity(def.fields.len());
        for (field, expr) in def.fields.iter().zip(&substituted) {
            if paused.contains(&field.name) {
                continue;
            }
            collect_columns(expr, &mut read);
            fields.push((
                field.name.clone(),
                crate::defs::oracle::render_qualified_expr_sql(expr, "s"),
            ));
        }
        let columns = source_columns
            .iter()
            .filter(|c| read.contains(&c.name))
            .cloned()
            .collect();
        Ok(Self { fields, columns })
    }

    /// A lateral subquery `s` with the columns the fields read, cast from
    /// `image`, SQL for a jsonb row of text values in scope (`v.__img`).
    /// A column the row lacks reads `NULL`.
    pub(crate) fn row_sql(&self, image: &str) -> String {
        if self.columns.is_empty() {
            return "(select 1 as __none) s".to_string();
        }
        let casts: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                let collate = c
                    .collation
                    .as_ref()
                    .map(|name| format!(" collate {name}"))
                    .unwrap_or_default();
                format!(
                    "({image} ->> {})::{}{collate} as {}",
                    quote_literal(&c.name),
                    c.type_sql,
                    quote_ident(&c.name)
                )
            })
            .collect();
        format!("lateral (select {}) s", casts.join(", "))
    }
}

/// Every source column a substituted expression reads.
fn collect_columns(expr: &Expr, out: &mut BTreeSet<String>) {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => {
            out.insert(name.clone());
        }
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

/// Whether `err` is a data exception, SQLSTATE class 22: a value a field
/// can't be computed from (an overflow, an invalid regex, an out-of-range
/// cast), as opposed to a lock timeout, a serialization failure or a lost
/// connection.
pub(crate) fn is_data_exception(err: &(dyn std::error::Error + 'static)) -> bool {
    super::quarantine::sqlstate_of(err).is_some_and(|code| code.starts_with("22"))
}

/// The per-field probe: evaluates each of `render`'s fields alone over
/// `image` (a text-valued jsonb row), and returns the ones that fail with a
/// data exception, each with its error, in definition order. Each probe is its own
/// statement on `client` outside any transaction, and takes no lock. Any other
/// failure propagates.
pub(crate) async fn failing_fields(
    client: &impl GenericClient,
    render: &OneToOneRender,
    image: &str,
) -> Result<Vec<(String, String)>, ApplyError> {
    let row = render.row_sql("v.__img");
    let mut failing = Vec::new();
    for (name, sql) in &render.fields {
        let probe = format!(
            "select ({sql})::text from (select $1::text::jsonb as __img) v cross join {row}"
        );
        match client.query(&probe, &[&image]).await {
            Ok(_) => {}
            Err(err) if is_data_exception(&err) => {
                let message = err
                    .as_db_error()
                    .map_or_else(|| err.to_string(), |db| db.message().to_string());
                failing.push((name.clone(), message));
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(failing)
}
