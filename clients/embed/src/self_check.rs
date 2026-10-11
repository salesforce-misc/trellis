//! `self_check` starts a background job (#1023), and the job crosses as a
//! [`PlainSelfCheckJob`]: its id, its state as a word, how many keys it has
//! compared, and, once it is done, its report. The report is a
//! [`PlainSelfCheckReport`]: its outcome as a word and its divergences as a
//! flat list, each tagged with its kind's word (ADR-0010 decision 4). A host
//! makes atoms or symbols of those words from [`SELF_CHECK_STATES`],
//! [`SELF_CHECK_OUTCOMES`] and [`DIVERGENCE_KINDS`], allocated at load.
//!
//! The report's `checked_through` position crosses as a watermark token
//! ([`crate::encode_watermark`]), so a host can hand it straight to
//! `await_converged`.
//!
//! The mode goes the other way, as a word [`self_check_mode`] reads.

use trellis::{
    Divergence, ErrorCode, SelfCheckJob, SelfCheckJobState, SelfCheckMode, SelfCheckOutcome,
    SelfCheckReport,
};

use crate::{PlainDrainFailure, PlainError, PlainHeldKeys, encode_watermark, transform_status};

/// Every word [`PlainSelfCheckJob::state`] can be.
pub const SELF_CHECK_STATES: [&str; 5] = ["queued", "running", "done", "failed", "cancelled"];

/// Every word [`PlainSelfCheckReport::outcome`] can be.
pub const SELF_CHECK_OUTCOMES: [&str; 4] = ["converged", "not_caught_up", "not_live", "diverged"];

/// Every word [`PlainDivergence::kind`] can be.
pub const DIVERGENCE_KINDS: [&str; 6] = [
    "cell",
    "missing_row",
    "extra_row",
    "missing_column",
    "extra_column",
    "capture",
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

/// A [`SelfCheckJob`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainSelfCheckJob {
    /// The id to poll the job by.
    pub id: i64,
    /// The audited target's bare table name.
    pub target: String,
    /// One of [`SELF_CHECK_MODES`].
    pub mode: &'static str,
    /// One of [`SELF_CHECK_STATES`]: `queued` (no worker has taken it),
    /// `running`, then `done`, `failed` or `cancelled`.
    pub state: &'static str,
    /// How many distinct keys the pages so far compared.
    pub rows_compared: i64,
    /// The result, when `state` is `done`.
    pub report: Option<PlainSelfCheckReport>,
    /// Why the job is `failed` or `cancelled`.
    pub error: Option<String>,
}

impl From<&SelfCheckJob> for PlainSelfCheckJob {
    fn from(job: &SelfCheckJob) -> Self {
        PlainSelfCheckJob {
            id: job.id,
            target: job.target.clone(),
            mode: match job.mode {
                SelfCheckMode::Standard => "standard",
                SelfCheckMode::Strict => "strict",
            },
            state: match job.state {
                SelfCheckJobState::Queued => "queued",
                SelfCheckJobState::Running => "running",
                SelfCheckJobState::Done => "done",
                SelfCheckJobState::Failed => "failed",
                SelfCheckJobState::Cancelled => "cancelled",
            },
            rows_compared: job.rows_compared,
            report: job.report.as_ref().map(PlainSelfCheckReport::from),
            error: job.error.clone(),
        }
    }
}

/// A [`SelfCheckReport`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainSelfCheckReport {
    /// The audited target's bare table name.
    pub target: String,
    /// The position the outcome was checked through, as a watermark token.
    pub checked_through: String,
    /// How many distinct keys the job compared; zero when the target never
    /// caught up.
    pub rows_compared: i64,
    /// Whether the job stopped before the end of the target's keys; see
    /// [`SelfCheckReport::truncated`].
    pub truncated: bool,
    /// One of [`SELF_CHECK_OUTCOMES`].
    pub outcome: &'static str,
    /// The audited definition's status word (one of
    /// [`crate::transform_status_names`]) when `outcome` is `not_live`:
    /// nothing was awaited or compared, because it isn't `live`. `None`
    /// for every other outcome.
    pub status: Option<&'static str>,
    /// What diverged; empty unless `outcome` is `diverged`.
    pub divergences: Vec<PlainDivergence>,
    /// The keys the audited definition holds in quarantine, whatever the
    /// outcome; see [`SelfCheckReport::held_keys`].
    pub held_keys: Option<PlainHeldKeys>,
    /// Every page the drain keeps failing on with nothing charged or paused,
    /// oldest first, whatever the outcome; see
    /// [`SelfCheckReport::drain_failures`].
    pub drain_failures: Vec<PlainDrainFailure>,
}

/// One [`Divergence`] flattened to plain data. `kind` is one of
/// [`DIVERGENCE_KINDS`]; each other field is set only for the kinds that
/// carry it (`key` for `cell`, `missing_row` and `extra_row`; `column` for
/// `cell`, `missing_column` and `extra_column`; `persisted` and `recomputed`
/// for `cell`, and even there `None` stands for SQL `NULL`; `detail` for
/// `capture`, and `table` for a `capture` fault on one table).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlainDivergence {
    pub kind: &'static str,
    pub key: Option<String>,
    pub column: Option<String>,
    /// The value in the target table, as text.
    pub persisted: Option<String>,
    /// The value the recompute produced, as text.
    pub recomputed: Option<String>,
    /// The captured table (`schema.table`) a `capture` fault is on. `None`
    /// for a missing privilege, which is about the Trellis role, not one
    /// table.
    pub table: Option<String>,
    /// What is wrong with the capture, as one sentence.
    pub detail: Option<String>,
}

impl From<&SelfCheckReport> for PlainSelfCheckReport {
    fn from(report: &SelfCheckReport) -> Self {
        let (outcome, divergences) = match &report.outcome {
            SelfCheckOutcome::Converged => ("converged", Vec::new()),
            SelfCheckOutcome::NotCaughtUp => ("not_caught_up", Vec::new()),
            SelfCheckOutcome::NotLive(_) => ("not_live", Vec::new()),
            SelfCheckOutcome::Diverged(divergences) => (
                "diverged",
                divergences.iter().map(PlainDivergence::from).collect(),
            ),
        };
        let status = match &report.outcome {
            SelfCheckOutcome::NotLive(status) => Some(transform_status(*status)),
            _ => None,
        };
        PlainSelfCheckReport {
            target: report.target.clone(),
            checked_through: encode_watermark(report.checked_through),
            rows_compared: report.rows_compared,
            truncated: report.truncated,
            outcome,
            status,
            divergences,
            held_keys: report.held_keys.as_ref().map(PlainHeldKeys::from),
            drain_failures: report
                .drain_failures
                .iter()
                .map(PlainDrainFailure::from)
                .collect(),
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
                ..PlainDivergence::default()
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
            Divergence::Capture(fault) => PlainDivergence {
                kind: "capture",
                table: fault.table().map(str::to_string),
                detail: Some(fault.to_string()),
                ..PlainDivergence::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use trellis::{CaptureFault, PgLsn};

    use super::*;
    use crate::decode_watermark;

    fn report(outcome: SelfCheckOutcome) -> SelfCheckReport {
        SelfCheckReport {
            target: "order_totals".to_string(),
            checked_through: PgLsn::from(0x1_016B_3748),
            rows_compared: 42,
            truncated: false,
            outcome,
            held_keys: None,
            drain_failures: Vec::new(),
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
                truncated: false,
                outcome: "converged",
                status: None,
                divergences: Vec::new(),
                held_keys: None,
                drain_failures: Vec::new(),
            }
        );
        // The position is a real watermark token, so a host can wait on it.
        assert_eq!(
            decode_watermark(&plain.checked_through).unwrap(),
            PgLsn::from(0x1_016B_3748)
        );
    }

    #[test]
    fn held_keys_cross_with_any_outcome() {
        let mut held = report(SelfCheckOutcome::NotCaughtUp);
        held.held_keys = Some(trellis::HeldKeys {
            count: 2,
            oldest_poisoned_at: std::time::UNIX_EPOCH
                + std::time::Duration::from_micros(1_727_222_400_000_001),
        });
        assert_eq!(
            PlainSelfCheckReport::from(&held).held_keys,
            Some(PlainHeldKeys {
                count: 2,
                oldest_poisoned_at_micros: 1_727_222_400_000_001,
            })
        );
    }

    #[test]
    fn drain_failures_cross_with_any_outcome_oldest_first() {
        let at = |micros| std::time::UNIX_EPOCH + std::time::Duration::from_micros(micros);
        let mut held_up = report(SelfCheckOutcome::Converged);
        held_up.drain_failures = vec![
            trellis::DrainFailure {
                seg_seq: 3,
                tables: vec!["public.orders".to_string()],
                error: "permission denied for column note".to_string(),
                sqlstate: Some("42501".to_string()),
                since: at(1_727_222_400_000_001),
                last_seen: at(1_727_222_400_000_002),
                attempts: 4,
            },
            trellis::DrainFailure {
                seg_seq: 9,
                tables: vec!["public.lines".to_string()],
                error: "records fail only together".to_string(),
                sqlstate: None,
                since: at(1_727_222_400_000_003),
                last_seen: at(1_727_222_400_000_004),
                attempts: 1,
            },
        ];

        assert_eq!(
            PlainSelfCheckReport::from(&held_up).drain_failures,
            vec![
                PlainDrainFailure {
                    seg_seq: 3,
                    tables: vec!["public.orders".to_string()],
                    error: "permission denied for column note".to_string(),
                    sqlstate: Some("42501".to_string()),
                    since_micros: 1_727_222_400_000_001,
                    last_seen_micros: 1_727_222_400_000_002,
                    attempts: 4,
                },
                PlainDrainFailure {
                    seg_seq: 9,
                    tables: vec!["public.lines".to_string()],
                    error: "records fail only together".to_string(),
                    sqlstate: None,
                    since_micros: 1_727_222_400_000_003,
                    last_seen_micros: 1_727_222_400_000_004,
                    attempts: 1,
                },
            ]
        );
    }

    #[test]
    fn not_caught_up_is_its_own_outcome() {
        let plain = PlainSelfCheckReport::from(&report(SelfCheckOutcome::NotCaughtUp));
        assert_eq!(plain.outcome, "not_caught_up");
        assert!(plain.divergences.is_empty());
    }

    #[test]
    fn not_live_is_its_own_outcome_and_names_the_status() {
        for status in trellis::TransformStatus::ALL {
            let plain = PlainSelfCheckReport::from(&report(SelfCheckOutcome::NotLive(status)));
            assert_eq!(plain.outcome, "not_live");
            assert_eq!(plain.status, Some(status.as_str()));
            assert!(plain.divergences.is_empty());
        }
        let converged = PlainSelfCheckReport::from(&report(SelfCheckOutcome::Converged));
        assert_eq!(converged.status, None, "only not_live names a status");
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
            Divergence::Capture(CaptureFault::MissingTrigger {
                table: "public.orders".to_string(),
                trigger: "trellis_capture_insert".to_string(),
            }),
            Divergence::Capture(CaptureFault::MissingPrivilege {
                role: "trellis".to_string(),
                privilege: "INSERT".to_string(),
                object: "trellis.seg_0".to_string(),
            }),
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
                    ..PlainDivergence::default()
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
                PlainDivergence {
                    kind: "capture",
                    table: key("public.orders"),
                    detail: key(
                        "the capture trigger trellis_capture_insert on public.orders is missing"
                    ),
                    ..PlainDivergence::default()
                },
                PlainDivergence {
                    kind: "capture",
                    detail: key(
                        "role trellis, which the capture functions run as, lacks INSERT on \
                         trellis.seg_0"
                    ),
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
    fn a_job_crosses_with_its_state_word_and_its_report_only_when_done() {
        let job = |state, report: Option<SelfCheckReport>, error: Option<&str>| SelfCheckJob {
            id: 7,
            target: "order_totals".to_string(),
            mode: SelfCheckMode::Strict,
            state,
            rows_compared: 42,
            report,
            error: error.map(str::to_string),
        };

        let queued = PlainSelfCheckJob::from(&job(SelfCheckJobState::Queued, None, None));
        assert_eq!(
            queued,
            PlainSelfCheckJob {
                id: 7,
                target: "order_totals".to_string(),
                mode: "strict",
                state: "queued",
                rows_compared: 42,
                report: None,
                error: None,
            }
        );

        let done = PlainSelfCheckJob::from(&job(
            SelfCheckJobState::Done,
            Some(report(SelfCheckOutcome::Converged)),
            None,
        ));
        assert_eq!(done.state, "done");
        assert_eq!(done.report.expect("report").outcome, "converged");

        let cancelled = PlainSelfCheckJob::from(&job(
            SelfCheckJobState::Cancelled,
            None,
            Some("cancelled: the worker running it shut down"),
        ));
        assert_eq!(cancelled.state, "cancelled");
        assert!(cancelled.error.expect("why").contains("shut down"));

        for state in [
            SelfCheckJobState::Queued,
            SelfCheckJobState::Running,
            SelfCheckJobState::Done,
            SelfCheckJobState::Failed,
            SelfCheckJobState::Cancelled,
        ] {
            let word = PlainSelfCheckJob::from(&job(state, None, None)).state;
            assert!(SELF_CHECK_STATES.contains(&word), "{word}");
            assert_eq!(word, state.as_str());
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
