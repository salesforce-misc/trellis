//! The hidden ordering ledgers of a 1-1 `JOIN` transform (ADR-0021): `R`,
//! one entry per `FROM`-side row, and `M`, one per joined-side row. They
//! live in the catalog schema, named `join1_{id}_{r|m}` by definition id, and
//! carry the ordering-state columns of a ledger ([`super::ledger`]) so the
//! tombstone GC, the ordering primitives and `RESUME`'s reset treat them as
//! any other ledger. Their `ledger_truncate_floor` rows are keyed by the
//! quoted table ident, as a `JOIN`'s `P` and `M` floors are
//! ([`super::join::HiddenTable`]).

use crate::pool::quote_ident;

/// A 1-1 `JOIN`'s hidden ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LedgerTable {
    /// `R`: the `FROM` side's entries.
    From,
    /// `M`: the joined side's entries.
    Joined,
}

impl LedgerTable {
    pub(crate) const ALL: [LedgerTable; 2] = [LedgerTable::From, LedgerTable::Joined];

    fn part(self) -> &'static str {
        match self {
            LedgerTable::From => "r",
            LedgerTable::Joined => "m",
        }
    }

    /// The table's bare name in the catalog schema, for definition `id`.
    pub(crate) fn name(self, id: i64) -> String {
        format!("join1_{id}_{}", self.part())
    }

    /// The table, qualified and quoted: its SQL text and its
    /// `ledger_truncate_floor` key.
    pub(crate) fn ident(self, catalog_schema: &str, id: i64) -> String {
        format!(
            "{}.{}",
            quote_ident(catalog_schema),
            quote_ident(&self.name(id))
        )
    }
}

/// The statement dropping both hidden ledgers of definition `id` from
/// `catalog_schema`, any that is already gone included.
pub(crate) fn drop_sql(catalog_schema: &str, id: i64) -> String {
    let tables: Vec<String> = LedgerTable::ALL
        .iter()
        .map(|table| table.ident(catalog_schema, id))
        .collect();
    format!("drop table if exists {}", tables.join(", "))
}

/// The `ledger_truncate_floor` keys of both hidden ledgers of definition
/// `id`.
pub(crate) fn floor_keys(catalog_schema: &str, id: i64) -> Vec<String> {
    LedgerTable::ALL
        .iter()
        .map(|table| table.ident(catalog_schema, id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ledgers_are_named_by_definition_id() {
        assert_eq!(LedgerTable::From.name(7), "join1_7_r");
        assert_eq!(
            LedgerTable::Joined.ident("trellis", 7),
            "\"trellis\".\"join1_7_m\""
        );
        assert_eq!(
            drop_sql("trellis", 7),
            "drop table if exists \"trellis\".\"join1_7_r\", \"trellis\".\"join1_7_m\""
        );
    }
}
