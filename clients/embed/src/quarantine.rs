//! [`QuarantineTarget`] crosses as its address string, `transform` or
//! `transform.column` — the idiom ADR-0008 decision 5 settled — not as a
//! two-variant struct (ADR-0010 decision 4). Every `trellis` method that takes
//! a target already takes that string, so a host hands it straight back.
//!
//! The quarantine reads' results flatten here too: a [`QuarantineEntry`]
//! (from `quarantined` and `quarantine_status`) with its target as an address
//! and its state as a word, a [`PoisonEntry`] (from `poisoned_since`) and a
//! page of [`PoisonSample`]s (from `sample_quarantined`) with the opaque
//! cursor for the next page. Times cross as [`crate::epoch_micros`].

use trellis::{PoisonEntry, PoisonSample, QuarantineEntry, QuarantineTarget};

use crate::{epoch_micros, next_cursor, quarantine_state};

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

/// A [`QuarantineEntry`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainQuarantineEntry {
    /// The target's address; see [`quarantine_address`].
    pub target: String,
    /// The state's `as_str()` word; see [`crate::quarantine_state`].
    pub state: &'static str,
    /// When a paused column's pause tripped, as [`crate::epoch_micros`];
    /// `None` for anything else.
    pub paused_at_micros: Option<i64>,
    /// A paused column's most recent failure, if its pause has one of its
    /// own (a cascaded pause has none).
    pub last_error: Option<String>,
}

impl From<&QuarantineEntry> for PlainQuarantineEntry {
    fn from(entry: &QuarantineEntry) -> Self {
        PlainQuarantineEntry {
            target: quarantine_address(&entry.target),
            state: quarantine_state(entry.state),
            paused_at_micros: entry.paused_at.map(epoch_micros),
            last_error: entry.last_error.clone(),
        }
    }
}

/// A [`PoisonEntry`] flattened to plain data: one key the apply path gave up
/// on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainPoisonEntry {
    /// The fully-qualified `schema.table` the key is from.
    pub src_table: String,
    /// The key, as `trellis` renders it.
    pub key: String,
    pub last_error: String,
    /// When the key was poisoned, as [`crate::epoch_micros`].
    pub poisoned_at_micros: i64,
}

impl From<&PoisonEntry> for PlainPoisonEntry {
    fn from(entry: &PoisonEntry) -> Self {
        PlainPoisonEntry {
            src_table: entry.src_table.clone(),
            key: entry.key.clone(),
            last_error: entry.last_error.clone(),
            poisoned_at_micros: epoch_micros(entry.poisoned_at),
        }
    }
}

/// One sampled quarantined row. [`PoisonSample`] is already plain; this is
/// its owned mirror, so a page is plain data end to end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainPoisonSample {
    /// The fully-qualified `schema.table` the row is from.
    pub src_table: String,
    /// The row's key, as `trellis` renders it.
    pub key: String,
    pub error_message: String,
}

/// One page of `sample_quarantined`, with the cursor for the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainSamplePage {
    pub samples: Vec<PlainPoisonSample>,
    /// The opaque cursor to pass back for the page after this one (see
    /// [`crate::decode_cursor`]). An empty page hands back the cursor it was
    /// asked for, so a caller that keeps polling from where it got to never
    /// falls back to the first page; `None` only when that was `None` too.
    pub next_cursor: Option<String>,
}

impl PlainSamplePage {
    /// The page `samples` makes, asked for after `after` (the cursor the
    /// caller passed, `None` for the first page).
    pub fn new(samples: &[PoisonSample], after: Option<&str>) -> Self {
        PlainSamplePage {
            next_cursor: next_cursor(samples).or_else(|| after.map(str::to_string)),
            samples: samples
                .iter()
                .map(|sample| PlainPoisonSample {
                    src_table: sample.src_table.clone(),
                    key: sample.key.clone(),
                    error_message: sample.error_message.clone(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use trellis::QuarantineState;

    use super::*;
    use crate::decode_cursor;

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

    #[test]
    fn a_paused_column_entry_crosses_with_its_address_and_pause_time() {
        let entry = QuarantineEntry {
            target: QuarantineTarget::Column("order_totals".to_string(), "total".to_string()),
            state: QuarantineState::Paused,
            paused_at: Some(UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456)),
            last_error: Some("integer out of range".to_string()),
        };

        assert_eq!(
            PlainQuarantineEntry::from(&entry),
            PlainQuarantineEntry {
                target: "order_totals.total".to_string(),
                state: "paused",
                paused_at_micros: Some(1_727_222_400_123_456),
                last_error: Some("integer out of range".to_string()),
            }
        );
    }

    #[test]
    fn a_quarantined_transform_entry_has_no_pause_detail() {
        let entry = QuarantineEntry {
            target: QuarantineTarget::Transform("order_totals".to_string()),
            state: QuarantineState::Quarantined,
            paused_at: None,
            last_error: None,
        };

        assert_eq!(
            PlainQuarantineEntry::from(&entry),
            PlainQuarantineEntry {
                target: "order_totals".to_string(),
                state: "quarantined",
                paused_at_micros: None,
                last_error: None,
            }
        );
    }

    #[test]
    fn a_poison_entry_crosses_with_its_time_in_microseconds() {
        let entry = PoisonEntry {
            src_table: "public.orders".to_string(),
            key: "42".to_string(),
            last_error: "new row violates check constraint".to_string(),
            poisoned_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
        };

        assert_eq!(
            PlainPoisonEntry::from(&entry),
            PlainPoisonEntry {
                src_table: "public.orders".to_string(),
                key: "42".to_string(),
                last_error: "new row violates check constraint".to_string(),
                poisoned_at_micros: 1_727_222_400_654_321,
            }
        );
    }

    fn sample(key: &str) -> PoisonSample {
        PoisonSample {
            src_table: "public.orders".to_string(),
            key: key.to_string(),
            error_message: "integer out of range".to_string(),
        }
    }

    #[test]
    fn a_page_carries_its_rows_and_the_cursor_after_the_last() {
        let page = PlainSamplePage::new(&[sample("1"), sample("2")], None);

        assert_eq!(
            page.samples,
            [
                PlainPoisonSample {
                    src_table: "public.orders".to_string(),
                    key: "1".to_string(),
                    error_message: "integer out of range".to_string(),
                },
                PlainPoisonSample {
                    src_table: "public.orders".to_string(),
                    key: "2".to_string(),
                    error_message: "integer out of range".to_string(),
                },
            ]
        );
        assert_eq!(
            decode_cursor(page.next_cursor.as_deref()).unwrap(),
            Some(("public.orders".to_string(), "2".to_string())),
        );
    }

    /// Polling past the end must not wrap back to the first page.
    #[test]
    fn an_empty_page_hands_back_the_cursor_it_was_asked_for() {
        let cursor = crate::encode_cursor("public.orders", "5");

        assert_eq!(
            PlainSamplePage::new(&[], Some(&cursor)),
            PlainSamplePage {
                samples: Vec::new(),
                next_cursor: Some(cursor),
            }
        );
        assert_eq!(
            PlainSamplePage::new(&[], None),
            PlainSamplePage {
                samples: Vec::new(),
                next_cursor: None,
            }
        );
    }
}
