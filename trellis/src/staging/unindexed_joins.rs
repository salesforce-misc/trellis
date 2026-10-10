//! Unindexed relationship join columns (#973): the observability side of
//! #972's planner setting.
//!
//! A relationship's reads look rows up by a join column: `from_col` on the
//! from-side table, and, for a to-many relationship, `to_col` on the to-side
//! table (a to-one's `to_col` is the projection's primary key, so it always
//! has an index). Without an index each read scans the table. Trellis never
//! creates indexes on a source table (ADR-0005), so `Trellis::status` reports
//! the columns a definition reads that lack one
//! (`DefinitionStatus::unindexed_joins`) and `self_check` reports them for
//! the audited definition (`SelfCheckReport::unindexed_joins`). Neither
//! changes the definition's status or the audit's outcome: it is a warning.
//!
//! "Indexed" is [`ddl::key_column_in`]'s definition, the one the batch reads
//! plan against, so a column reported here is exactly one whose reads run
//! unindexed. It is read live, one catalog query per column, so an index
//! created or dropped shows on the next call.

use tokio_postgres::GenericClient;

use crate::defs::ast::TransformDef;
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl;
use crate::defs::model::RelationshipCardinality;

/// A join column of a relationship a definition reads that has no usable
/// index (#973); see `DefinitionStatus::unindexed_joins` and
/// `SelfCheckReport::unindexed_joins`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnindexedJoin {
    /// The relationship's name.
    pub relationship: String,
    /// The qualified table that holds the column: the relationship's
    /// from-table for its `from_col`, its to-table for a to-many's `to_col`.
    pub table: String,
    /// The column.
    pub column: String,
}

impl UnindexedJoin {
    /// What to do about it, as a sentence naming the table and column.
    pub fn fix(&self) -> String {
        format!("create an index on {} ({})", self.table, self.column)
    }
}

/// The join columns of the relationships `def` reads through, from
/// `source_table`, that have no usable index, in relationship-name order
/// with a relationship's `from_col` before its `to_col`. A relationship no
/// longer declared is left out.
pub(crate) async fn for_definition(
    client: &impl GenericClient,
    source_table: &str,
    def: &TransformDef,
) -> Result<Vec<UnindexedJoin>, CatalogError> {
    let mut unindexed = Vec::new();
    for rel in catalog::relationships_read_by(client, def, source_table).await? {
        let from_table = rel.qualified_from_table();
        let mut columns = vec![(from_table, &rel.def.from_col)];
        // A to-one's `to_col` is unique, so it is indexed already.
        if rel.cardinality == RelationshipCardinality::ToMany {
            columns.push((rel.qualified_to_table(), &rel.def.to_col));
        }
        for (table, column) in columns {
            if !ddl::column_is_indexed_in(client, &table, column).await? {
                unindexed.push(UnindexedJoin {
                    relationship: rel.def.name.clone(),
                    table,
                    column: column.clone(),
                });
            }
        }
    }
    Ok(unindexed)
}
