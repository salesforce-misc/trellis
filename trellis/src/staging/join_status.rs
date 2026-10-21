//! What `Trellis::status` reports about a `JOIN` transform (#1112): how big
//! its hidden tables are, and whether `P` has grown close to the fact table.
//! The source indexes it needs are every target kind's
//! (`DefinitionStatus::source_index_gaps`, #1167). Detection only
//! ([#1096 decision 3](https://github.com/salesforce-misc/trellis/issues/1096)):
//! nothing here changes the definition's status or repairs anything.
//!
//! Sizes and row counts are the planner's estimates (`pg_class.reltuples`,
//! `pg_total_relation_size`), so a status call costs the same on a billion
//! rows as on ten; they are as fresh as the last `ANALYZE` or autovacuum of
//! the table.

use tokio_postgres::GenericClient;

use crate::defs::ast::TransformDef;
use crate::defs::ddl::regclass_arg;
use crate::defs::join::{self as spec, HiddenTable};

/// `P` is reported once its estimated rows reach this share of the fact
/// table's, as a percentage: past it the partials no longer compress the
/// fact table, and the `JOIN` costs a fact-table-sized hidden table.
pub(crate) const PARTIALS_WARNING_PERCENT: i64 = 50;

/// Below this many estimated fact rows `P` is never reported: a small table
/// whose groups are mostly distinct is not a cost worth a warning.
pub(crate) const PARTIALS_WARNING_MIN_FACT_ROWS: i64 = 1000;

/// One hidden table of a `JOIN` definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenTableSize {
    /// Which table: `P` (the `FROM` side's partials), `M` (the joined
    /// side's), `J` (the matches per join key) or `T` (the target's
    /// partials).
    pub table: &'static str,
    /// The table's name in the catalog schema.
    pub name: String,
    /// The estimated live rows (`reltuples`; 0 before the first analyze).
    pub estimated_rows: i64,
    /// The table's total size on disk with its indexes, in bytes.
    pub bytes: i64,
}

/// `P` has grown close to the fact table's size; see [`JoinStatus::partials_warning`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialsWarning {
    /// `P`'s estimated rows.
    pub partials_rows: i64,
    /// The `FROM` (fact) table's estimated rows.
    pub fact_rows: i64,
}

/// A `JOIN` definition's hidden tables; see
/// `DefinitionStatus::join`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinStatus {
    /// `P`, `M`, `J` and `T`, in that order. A table that doesn't exist is
    /// left out.
    pub hidden_tables: Vec<HiddenTableSize>,
    /// Set when `P`'s estimated rows reach
    /// [`PARTIALS_WARNING_PERCENT`] percent of the fact table's.
    pub partials_warning: Option<PartialsWarning>,
}

/// The status of `def`, definition `id` in `catalog_schema`, if it is a
/// `JOIN`.
pub(crate) async fn for_definition(
    client: &impl GenericClient,
    catalog_schema: &str,
    id: i64,
    source_table: &str,
    def: &TransformDef,
) -> Result<Option<JoinStatus>, tokio_postgres::Error> {
    if spec::spec(def).is_none() {
        return Ok(None);
    }
    let mut hidden_tables = Vec::new();
    for (label, table) in [
        ("P", HiddenTable::FromPartials),
        ("M", HiddenTable::JoinedPartials),
        ("J", HiddenTable::Matches),
        ("T", HiddenTable::TargetPartials),
    ] {
        let name = table.name(id);
        let qualified = format!("{catalog_schema}.{name}");
        let row = client
            .query_opt(
                "select c.reltuples::bigint, pg_catalog.pg_total_relation_size(c.oid) \
                 from pg_catalog.pg_class c where c.oid = pg_catalog.to_regclass($1)",
                &[&regclass_arg(&qualified)],
            )
            .await?;
        if let Some(row) = row {
            hidden_tables.push(HiddenTableSize {
                table: label,
                name,
                estimated_rows: row.get::<_, i64>(0).max(0),
                bytes: row.get(1),
            });
        }
    }
    let partials_warning = partials_warning(client, &hidden_tables, source_table).await?;
    Ok(Some(JoinStatus {
        hidden_tables,
        partials_warning,
    }))
}

async fn partials_warning(
    client: &impl GenericClient,
    hidden_tables: &[HiddenTableSize],
    source_table: &str,
) -> Result<Option<PartialsWarning>, tokio_postgres::Error> {
    let Some(p) = hidden_tables.iter().find(|t| t.table == "P") else {
        return Ok(None);
    };
    let fact_rows: i64 = client
        .query_opt(
            "select c.reltuples::bigint from pg_catalog.pg_class c \
             where c.oid = pg_catalog.to_regclass($1)",
            &[&regclass_arg(source_table)],
        )
        .await?
        .map_or(0, |row| row.get::<_, i64>(0).max(0));
    Ok(
        warns(p.estimated_rows, fact_rows).then_some(PartialsWarning {
            partials_rows: p.estimated_rows,
            fact_rows,
        }),
    )
}

/// Whether `partials` estimated `P` rows warrant a warning against
/// `fact_rows` estimated fact rows.
pub(crate) fn warns(partials: i64, fact_rows: i64) -> bool {
    fact_rows >= PARTIALS_WARNING_MIN_FACT_ROWS
        && partials.saturating_mul(100) >= fact_rows.saturating_mul(PARTIALS_WARNING_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::warns;

    #[test]
    fn partials_warn_at_half_the_fact_table_and_not_for_a_small_one() {
        assert!(!warns(499, 1000));
        assert!(warns(500, 1000));
        assert!(warns(5000, 1000));
        assert!(!warns(10, 10), "a tiny fact table never warns");
        assert!(!warns(0, 0));
    }
}
