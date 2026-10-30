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

/// One source column as a row reads it back: its name, its type as a copy
/// casts to it (`defs::copy_type::type_sql`: qualified, no modifier, so a
/// value never rounds or truncates on the cast, ADR-0020), and its
/// collation unless it is the database default.
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
    let sql = format!(
        "select a.attname::text, {}, {} \
         from pg_catalog.pg_attribute a \
         where a.attrelid = pg_catalog.to_regclass($1) \
           and a.attnum > 0 and not a.attisdropped \
         order by a.attnum",
        crate::defs::copy_type::type_sql("a.atttypid"),
        crate::defs::copy_type::collation_sql("a.attcollation"),
    );
    let rows = client.query(&sql, &[&ddl::regclass_arg(table)]).await?;
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
    /// A render of no fields over `columns`, for the rows of a hidden ledger
    /// ([`Self::row_sql`] casts each from a text-valued jsonb image).
    pub(crate) fn of_columns(columns: Vec<ImageColumn>) -> Self {
        Self {
            fields: Vec::new(),
            columns,
        }
    }

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
    ///
    /// A column the row lacks fails the statement with a data exception
    /// (`22P02`, naming the column), so the record is isolated and its
    /// fields charged like any other value a field can't be computed from.
    /// Reading it as `NULL` would write a wrong value silently. The capture
    /// gate (`crate::capture::install`) keeps a row imaged before a widen
    /// away from a reader of the new column; this is the defense behind it.
    /// The failing arm reads the row, so the planner can't fold it into an
    /// error for every row.
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
                let name = quote_literal(&c.name);
                let missing = quote_literal(&format!("the row has no column {}", c.name));
                format!(
                    "(case when {image} operator(pg_catalog.?) {name} then {image} ->> {name} \
                     else ({missing} || pg_catalog.left({image}::text, 0))::integer::text end)\
                     ::{}{collate} as {}",
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
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BooleanLiteral(_)
        | Expr::Null
        | Expr::TypedLiteral { .. } => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_columns(lhs, out);
            collect_columns(rhs, out);
        }
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => collect_columns(operand, out),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            for child in expr.logic_children() {
                collect_columns(child, out);
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
/// `image` (a text-valued jsonb row), cast to its target column's type in
/// `target_columns` as the upsert's assignment casts it (a no-op while
/// define types a field as Postgres does), and returns the ones that fail
/// with a data exception, each with its error, in definition order. Each probe is its own
/// statement on `client` outside any transaction, and takes no lock. Any other
/// failure propagates. The row comes from a set-returning function, as the
/// upsert's does, so the planner can't fold the fields over a constant row:
/// a fallback arm that a non-`NULL` column hides must fail here as it does
/// there.
pub(crate) async fn failing_fields(
    client: &impl GenericClient,
    render: &OneToOneRender,
    target_columns: &[ImageColumn],
    image: &str,
) -> Result<Vec<(String, String)>, ApplyError> {
    let row = render.row_sql("v.__img");
    let mut failing = Vec::new();
    for (name, sql) in &render.fields {
        let cast = target_columns
            .iter()
            .find(|c| &c.name == name)
            .map_or_else(String::new, |c| format!("::{}", c.type_sql));
        let probe = format!(
            "select (({sql}){cast})::text from unnest(array[$1::text]::text[]::jsonb[]) as v(__img) cross join {row}"
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
