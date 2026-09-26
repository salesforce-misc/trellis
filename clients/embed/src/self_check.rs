//! `self_check`'s report crosses as a [`PlainSelfCheckReport`]: its outcome
//! as a word and its divergences as a flat list, each tagged with its kind's
//! word (ADR-0010 decision 4). A host makes atoms or symbols of those words
//! from [`SELF_CHECK_OUTCOMES`] and [`DIVERGENCE_KINDS`], allocated at load.
//!
//! The report's `checked_through` position crosses as a watermark token
//! ([`crate::encode_watermark`]), so a host can hand it straight to
//! `await_converged`. Its `next_after` is already a plain string: the keyset
//! cursor the next page's `after` takes back.
//!
//! The mode goes the other way, as a word [`self_check_mode`] reads.

use trellis::{Divergence, ErrorCode, SelfCheckMode, SelfCheckOutcome, SelfCheckReport};

use crate::{PlainError, encode_watermark};

/// Every word [`PlainSelfCheckReport::outcome`] can be.
pub const SELF_CHECK_OUTCOMES: [&str; 3] = ["converged", "not_caught_up", "diverged"];

/// Every word [`PlainDivergence::kind`] can be.
pub const DIVERGENCE_KINDS: [&str; 5] = [
    "cell",
    "missing_row",
    "extra_row",
    "missing_column",
    "extra_column",
];

/// Every word [`self_check_mode`] accepts.
pub const SELF_CHECK_MODES: [&str; 2] = ["standard", "strict"];

/// The [`SelfCheckMode`] named `word`, one of [`SELF_CHECK_MODES`]. Anything
/// else is a `validation` error.
pub fn self_check_mode(word: &str) -> Result<SelfCheckMode, PlainError> {
    match word {
        "standard" => Ok(SelfCheckMode::Standard),
        "strict" => Ok(SelfCheckMode::Strict),
        _ => Err(PlainError::new(
            ErrorCode::Validation,
            format!("{word:?} is not a self-check mode; expected one of {SELF_CHECK_MODES:?}"),
        )),
    }
}

/// A [`SelfCheckReport`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainSelfCheckReport {
    /// The audited target's bare table name.
    pub target: String,
    /// The position the outcome was checked through, as a watermark token.
    pub checked_through: String,
    /// How many distinct keys the call compared; zero when the target never
    /// caught up.
    pub rows_compared: i64,
    /// The cursor to pass as the next call's `after`, or `None` when this
    /// page reached the end of the target's keys.
    pub next_after: Option<String>,
    /// One of [`SELF_CHECK_OUTCOMES`].
    pub outcome: &'static str,
    /// What diverged; empty unless `outcome` is `diverged`.
    pub divergences: Vec<PlainDivergence>,
}

/// One [`Divergence`] flattened to plain data. `kind` is one of
/// [`DIVERGENCE_KINDS`]; each other field is set only for the kinds that
/// carry it (`key` for `cell`, `missing_row` and `extra_row`; `column` for
/// `cell`, `missing_column` and `extra_column`; `persisted` and `recomputed`
/// for `cell`, and even there `None` stands for SQL `NULL`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlainDivergence {
    pub kind: &'static str,
    pub key: Option<String>,
    pub column: Option<String>,
    /// The value in the target table, as text.
    pub persisted: Option<String>,
    /// The value the recompute produced, as text.
    pub recomputed: Option<String>,
}

impl From<&SelfCheckReport> for PlainSelfCheckReport {
    fn from(report: &SelfCheckReport) -> Self {
        let (outcome, divergences) = match &report.outcome {
            SelfCheckOutcome::Converged => ("converged", Vec::new()),
            SelfCheckOutcome::NotCaughtUp => ("not_caught_up", Vec::new()),
            SelfCheckOutcome::Diverged(divergences) => (
                "diverged",
                divergences.iter().map(PlainDivergence::from).collect(),
            ),
        };
        PlainSelfCheckReport {
            target: report.target.clone(),
            checked_through: encode_watermark(report.checked_through),
            rows_compared: report.rows_compared,
            next_after: report.next_after.clone(),
            outcome,
            divergences,
        }
    }
}

impl From<&Divergence> for PlainDivergence {
    fn from(divergence: &Divergence) -> Self {
        match divergence {
            Divergence::Cell {
                key,
                column,
                persisted,
                recomputed,
            } => PlainDivergence {
                kind: "cell",
                key: Some(key.clone()),
                column: Some(column.clone()),
                persisted: persisted.clone(),
                recomputed: recomputed.clone(),
            },
            Divergence::MissingRow { key } => PlainDivergence {
                kind: "missing_row",
                key: Some(key.clone()),
                ..PlainDivergence::default()
            },
            Divergence::ExtraRow { key } => PlainDivergence {
                kind: "extra_row",
                key: Some(key.clone()),
                ..PlainDivergence::default()
            },
            Divergence::MissingColumn { column } => PlainDivergence {
                kind: "missing_column",
                column: Some(column.clone()),
                ..PlainDivergence::default()
            },
            Divergence::ExtraColumn { column } => PlainDivergence {
                kind: "extra_column",
                column: Some(column.clone()),
                ..PlainDivergence::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use trellis::PgLsn;

    use super::*;
    use crate::decode_watermark;

    fn report(outcome: SelfCheckOutcome) -> SelfCheckReport {
        SelfCheckReport {
            target: "order_totals".to_string(),
            checked_through: PgLsn::from(0x1_016B_3748),
            rows_compared: 42,
            next_after: Some("42".to_string()),
            outcome,
        }
    }

    #[test]
    fn a_converged_report_crosses_with_a_watermark_token_and_no_divergences() {
        let plain = PlainSelfCheckReport::from(&report(SelfCheckOutcome::Converged));

        assert_eq!(
            plain,
            PlainSelfCheckReport {
                target: "order_totals".to_string(),
                checked_through: "1/16B3748".to_string(),
                rows_compared: 42,
                next_after: Some("42".to_string()),
                outcome: "converged",
                divergences: Vec::new(),
            }
        );
        // The position is a real watermark token, so a host can wait on it.
        assert_eq!(
            decode_watermark(&plain.checked_through).unwrap(),
            PgLsn::from(0x1_016B_3748)
        );
    }

    #[test]
    fn not_caught_up_is_its_own_outcome() {
        let plain = PlainSelfCheckReport::from(&report(SelfCheckOutcome::NotCaughtUp));
        assert_eq!(plain.outcome, "not_caught_up");
        assert!(plain.divergences.is_empty());
    }

    #[test]
    fn each_divergence_kind_carries_only_its_own_fields() {
        let plain = PlainSelfCheckReport::from(&report(SelfCheckOutcome::Diverged(vec![
            Divergence::Cell {
                key: "1".to_string(),
                column: "total".to_string(),
                persisted: Some("3".to_string()),
                recomputed: None,
            },
            Divergence::MissingRow {
                key: "2".to_string(),
            },
            Divergence::ExtraRow {
                key: "3".to_string(),
            },
            Divergence::MissingColumn {
                column: "tax".to_string(),
            },
            Divergence::ExtraColumn {
                column: "legacy".to_string(),
            },
        ])));

        let key = |key: &str| Some(key.to_string());
        assert_eq!(plain.outcome, "diverged");
        assert_eq!(
            plain.divergences,
            [
                PlainDivergence {
                    kind: "cell",
                    key: key("1"),
                    column: key("total"),
                    persisted: key("3"),
                    recomputed: None,
                },
                PlainDivergence {
                    kind: "missing_row",
                    key: key("2"),
                    ..PlainDivergence::default()
                },
                PlainDivergence {
                    kind: "extra_row",
                    key: key("3"),
                    ..PlainDivergence::default()
                },
                PlainDivergence {
                    kind: "missing_column",
                    column: key("tax"),
                    ..PlainDivergence::default()
                },
                PlainDivergence {
                    kind: "extra_column",
                    column: key("legacy"),
                    ..PlainDivergence::default()
                },
            ]
        );
        // Every kind produced is one a host allocated at load.
        for divergence in &plain.divergences {
            assert!(DIVERGENCE_KINDS.contains(&divergence.kind));
        }
    }

    #[test]
    fn every_mode_word_reads_back_and_nothing_else_does() {
        assert_eq!(
            self_check_mode("standard").unwrap(),
            SelfCheckMode::Standard
        );
        assert_eq!(self_check_mode("strict").unwrap(), SelfCheckMode::Strict);
        for word in SELF_CHECK_MODES {
            assert!(self_check_mode(word).is_ok(), "{word}");
        }
        for word in ["", "Standard", "STRICT", "lenient"] {
            assert_eq!(
                self_check_mode(word).unwrap_err().code,
                "validation",
                "{word:?}"
            );
        }
    }
}
