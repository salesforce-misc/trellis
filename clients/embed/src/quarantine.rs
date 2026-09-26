//! [`QuarantineTarget`] crosses as its address string, `transform` or
//! `transform.column` — the idiom ADR-0008 decision 5 settled — not as a
//! two-variant struct (ADR-0010 decision 4). Every `trellis` method that takes
//! a target already takes that string, so a host hands it straight back.

use trellis::QuarantineTarget;

/// `target`'s address: `transform` for a whole transform, `transform.column`
/// for one column.
///
/// [`QuarantineTarget::parse`] reads back the address of every target
/// `trellis` reports, whose names are grammar identifiers and so hold no dot.
/// A hand-built target with a dot in its transform name does not round-trip
/// (see `parse`'s doc), and nothing here pretends otherwise.
pub fn quarantine_address(target: &QuarantineTarget) -> String {
    target.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_whole_transform_crosses_as_its_name() {
        let target = QuarantineTarget::Transform("order_totals".to_string());
        assert_eq!(quarantine_address(&target), "order_totals");
    }

    #[test]
    fn a_column_crosses_as_transform_dot_column() {
        let target = QuarantineTarget::Column("order_totals".to_string(), "total".to_string());
        assert_eq!(quarantine_address(&target), "order_totals.total");
    }

    /// The targets `trellis` reports have identifier names, and those
    /// round-trip.
    #[test]
    fn addresses_round_trip_through_parse() {
        for target in [
            QuarantineTarget::Transform("order_totals".to_string()),
            QuarantineTarget::Column("order_totals".to_string(), "total".to_string()),
            // The address splits on its first dot, so a dot in the column
            // name survives.
            QuarantineTarget::Column("order_totals".to_string(), "a.b".to_string()),
        ] {
            assert_eq!(
                QuarantineTarget::parse(&quarantine_address(&target)),
                target
            );
        }
    }
}
