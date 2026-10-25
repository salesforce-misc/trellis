//! A registered transform crosses as its [`DefinitionSummary`] (what
//! `definitions()` lists): the target table, status, creation time and the
//! failures it carries. Nothing about its sources crosses outside a failure.
//! [`DefinitionStatus`] (what `status()` polls) flattens here too, with its
//! backfill failure's retry time, its capture wait's times, its capture
//! failure's detection time and its drain failure's times in epoch
//! microseconds. A capture failure's kind
//! crosses as its word, one of [`capture_failure_kind_names`], which a host
//! turns into an atom or symbol from that set, allocated at load.

use trellis::{
    BackfillFailure, CaptureFailure, CaptureFailureKind, CaptureWait, CorrectingNode,
    DefinitionStatus, DefinitionSummary, DrainFailure, SourceIndexGap,
};

use crate::{PlainHeldKeys, epoch_micros, transform_status};

/// A [`DefinitionSummary`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinitionSummary {
    /// The fully-qualified `schema.table` the definition writes.
    pub target_table: String,
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// When the definition was registered, as [`crate::epoch_micros`].
    pub created_at_micros: i64,
    /// Set while the definition's build keeps failing; see
    /// [`DefinitionSummary::backfill_failure`].
    pub backfill_failure: Option<PlainBackfillFailure>,
    /// Set while the drain, or its build's merge, has halted on the
    /// definition; see [`DefinitionSummary::halt`].
    pub halt: Option<PlainCaptureFailure>,
}

impl From<&DefinitionSummary> for PlainDefinitionSummary {
    fn from(summary: &DefinitionSummary) -> Self {
        PlainDefinitionSummary {
            target_table: summary.target_table.clone(),
            status: transform_status(summary.status),
            created_at_micros: epoch_micros(summary.created_at),
            backfill_failure: summary
                .backfill_failure
                .as_ref()
                .map(PlainBackfillFailure::from),
            halt: summary.halt.as_ref().map(PlainCaptureFailure::from),
        }
    }
}

/// A [`DefinitionStatus`] flattened to plain data: what a host polls until a
/// definition reaches `live`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinitionStatus {
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// Set while one of the definition's build chunks keeps failing; see
    /// [`DefinitionStatus::backfill_failure`].
    pub backfill_failure: Option<PlainBackfillFailure>,
    /// Set while the definition's capture waits on a table lock; see
    /// [`DefinitionStatus::capture_wait`].
    pub capture_wait: Option<PlainCaptureWait>,
    /// Set while capture of a table the definition reads is broken; see
    /// [`DefinitionStatus::capture_failure`].
    pub capture_failure: Option<PlainCaptureFailure>,
    /// Set while the definition holds keys in quarantine; see
    /// [`DefinitionStatus::held_keys`].
    pub held_keys: Option<PlainHeldKeys>,
    /// Set while the drain keeps failing on a page holding changes to a table
    /// the definition reads; see [`DefinitionStatus::drain_failure`].
    pub drain_failure: Option<PlainDrainFailure>,
    /// The source indexes a `GROUP BY` or `JOIN` target needs and lacks; see
    /// [`DefinitionStatus::source_index_gaps`].
    pub source_index_gaps: Vec<PlainSourceIndexGap>,
    /// The nodes of its partitioned source the definition is being corrected
    /// for, oldest first; see [`DefinitionStatus::correcting`].
    pub correcting: Vec<PlainCorrectingNode>,
    /// The paused upstream columns (`target.column`) that make a `live`
    /// definition report `catching_up`; see
    /// [`DefinitionStatus::paused_upstream_columns`].
    pub paused_upstream_columns: Vec<String>,
}

/// A [`CorrectingNode`] flattened to plain data: a node of a partitioned
/// source a definition is being corrected for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainCorrectingNode {
    /// The schema-qualified node.
    pub node: String,
    /// When the correction was queued, as [`crate::epoch_micros`].
    pub since_micros: i64,
    /// The keys still to re-derive, `None` until the correction has counted
    /// them.
    pub keys_remaining: Option<u64>,
}

impl From<&CorrectingNode> for PlainCorrectingNode {
    fn from(node: &CorrectingNode) -> Self {
        PlainCorrectingNode {
            node: node.node.clone(),
            since_micros: epoch_micros(node.since),
            keys_remaining: node.keys_remaining,
        }
    }
}

/// A [`SourceIndexGap`] flattened to plain data: a source index a target
/// needs and lacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainSourceIndexGap {
    /// The qualified table the index is missing on.
    pub table: String,
    /// The columns the index should lead with.
    pub columns: Vec<String>,
    /// The part of the definition that needs the index, as
    /// [`trellis::IndexRole::as_str`]: `group_by`, `join_from` or
    /// `join_joined`.
    pub role: &'static str,
    /// Whether the definition opted out (`WITHOUT SOURCE INDEX`).
    pub opted_out: bool,
    /// What to do about it: a `create index` statement.
    pub fix: String,
}

/// Every [`PlainSourceIndexGap::role`] word, for a binding to intern up
/// front.
pub fn index_role_names() -> [&'static str; 3] {
    trellis::IndexRole::ALL.map(trellis::IndexRole::as_str)
}

/// A [`CaptureWait`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainCaptureWait {
    /// The fully-qualified `schema.table` whose lock the capture waits for.
    pub table: String,
    /// `install`, `widen` or `uninstall`.
    pub operation: String,
    /// The lock mode it asks for, as `pg_locks` spells it.
    pub lock_mode: String,
    /// When the staging worker first found the table locked, as
    /// [`crate::epoch_micros`].
    pub waiting_since_micros: i64,
    /// When it last read who holds the lock, as [`crate::epoch_micros`].
    pub observed_at_micros: i64,
    /// One line per session holding or queued for a conflicting lock.
    pub blockers: Vec<String>,
}

/// A [`CaptureFailure`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainCaptureFailure {
    /// `capture` or `halt`, as [`CaptureFailureKind::as_str`] spells it.
    pub kind: &'static str,
    /// The fully-qualified `schema.table` whose capture is broken.
    pub source_table: String,
    /// The columns the failure is about; empty when it isn't about one.
    pub columns: Vec<String>,
    /// A sentence naming the cause.
    pub error: String,
    /// When it was first found, as [`crate::epoch_micros`].
    pub detected_at_micros: i64,
}

/// A [`DrainFailure`] flattened to plain data: a drain page that keeps
/// failing with nothing charged or paused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDrainFailure {
    /// The segment whose page fails.
    pub seg_seq: i64,
    /// The qualified source tables the failing page holds changes to.
    pub tables: Vec<String>,
    /// The latest failure's error, as the drain surfaced it.
    pub error: String,
    /// The latest failure's SQLSTATE, `None` when it didn't come from
    /// Postgres.
    pub sqlstate: Option<String>,
    /// When a drain first failed on the page, as [`crate::epoch_micros`].
    pub since_micros: i64,
    /// When a drain last failed on it, as [`crate::epoch_micros`].
    pub last_seen_micros: i64,
    /// How many drain passes have failed on it.
    pub attempts: u32,
}

/// A [`BackfillFailure`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainBackfillFailure {
    /// The fully-qualified `schema.table` the failing build reads.
    pub source_table: String,
    /// How many times the failing chunk has failed.
    pub attempts: u32,
    /// The latest failure's error message.
    pub last_error: String,
    /// The earliest next attempt, as [`crate::epoch_micros`].
    pub next_attempt_at_micros: i64,
}

impl From<&DefinitionStatus> for PlainDefinitionStatus {
    fn from(status: &DefinitionStatus) -> Self {
        PlainDefinitionStatus {
            status: transform_status(status.status),
            backfill_failure: status
                .backfill_failure
                .as_ref()
                .map(PlainBackfillFailure::from),
            capture_wait: status.capture_wait.as_ref().map(PlainCaptureWait::from),
            capture_failure: status
                .capture_failure
                .as_ref()
                .map(PlainCaptureFailure::from),
            held_keys: status.held_keys.as_ref().map(PlainHeldKeys::from),
            drain_failure: status.drain_failure.as_ref().map(PlainDrainFailure::from),
            source_index_gaps: status
                .source_index_gaps
                .iter()
                .map(PlainSourceIndexGap::from)
                .collect(),
            correcting: status
                .correcting
                .iter()
                .map(PlainCorrectingNode::from)
                .collect(),
            paused_upstream_columns: status.paused_upstream_columns.clone(),
        }
    }
}

impl From<&SourceIndexGap> for PlainSourceIndexGap {
    fn from(gap: &SourceIndexGap) -> Self {
        PlainSourceIndexGap {
            table: gap.table.clone(),
            columns: gap.columns.clone(),
            role: gap.role.as_str(),
            opted_out: gap.opted_out,
            fix: gap.fix(),
        }
    }
}

impl From<&CaptureWait> for PlainCaptureWait {
    fn from(wait: &CaptureWait) -> Self {
        PlainCaptureWait {
            table: wait.table.clone(),
            operation: wait.operation.clone(),
            lock_mode: wait.lock_mode.clone(),
            waiting_since_micros: epoch_micros(wait.waiting_since),
            observed_at_micros: epoch_micros(wait.observed_at),
            blockers: wait.blockers.clone(),
        }
    }
}

impl From<&CaptureFailure> for PlainCaptureFailure {
    fn from(failure: &CaptureFailure) -> Self {
        PlainCaptureFailure {
            kind: failure.kind.as_str(),
            source_table: failure.source_table.clone(),
            columns: failure.columns.clone(),
            error: failure.error.clone(),
            detected_at_micros: epoch_micros(failure.detected_at),
        }
    }
}

impl From<&DrainFailure> for PlainDrainFailure {
    fn from(failure: &DrainFailure) -> Self {
        PlainDrainFailure {
            seg_seq: failure.seg_seq,
            tables: failure.tables.clone(),
            error: failure.error.clone(),
            sqlstate: failure.sqlstate.clone(),
            since_micros: epoch_micros(failure.since),
            last_seen_micros: epoch_micros(failure.last_seen),
            attempts: failure.attempts,
        }
    }
}

/// Every word [`PlainCaptureFailure::kind`] can be: the set a host allocates
/// its kind names from at load time.
pub fn capture_failure_kind_names() -> Vec<&'static str> {
    CaptureFailureKind::ALL.iter().map(|k| k.as_str()).collect()
}

impl From<&BackfillFailure> for PlainBackfillFailure {
    fn from(failure: &BackfillFailure) -> Self {
        PlainBackfillFailure {
            source_table: failure.source_table.clone(),
            attempts: failure.attempts,
            last_error: failure.last_error.clone(),
            next_attempt_at_micros: epoch_micros(failure.next_attempt_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use trellis::TransformStatus;

    use super::*;

    #[test]
    fn a_summary_crosses_with_its_creation_time_in_microseconds() {
        let summary = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: TransformStatus::Live,
            created_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
            backfill_failure: None,
            halt: None,
        };

        let plain = PlainDefinitionSummary::from(&summary);

        assert_eq!(
            plain,
            PlainDefinitionSummary {
                target_table: "public.order_totals".to_string(),
                status: "live",
                created_at_micros: 1_727_222_400_123_456,
                backfill_failure: None,
                halt: None,
            }
        );
    }

    #[test]
    fn a_summary_carries_its_backfill_failure() {
        let summary = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: TransformStatus::WaitingToBackfill,
            created_at: UNIX_EPOCH,
            backfill_failure: Some(BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 2,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
            }),
            halt: None,
        };

        assert_eq!(
            PlainDefinitionSummary::from(&summary).backfill_failure,
            Some(PlainBackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 2,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at_micros: 1_727_222_400_654_321,
            })
        );
    }

    #[test]
    fn a_summary_carries_its_halt_with_its_kind_word() {
        let summary = DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: TransformStatus::Paused,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: Some(CaptureFailure {
                kind: CaptureFailureKind::Halt,
                source_table: "public.orders".to_string(),
                columns: Vec::new(),
                error: "the drain halted".to_string(),
                detected_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_000_004),
            }),
        };

        assert_eq!(
            PlainDefinitionSummary::from(&summary).halt,
            Some(PlainCaptureFailure {
                kind: "halt",
                source_table: "public.orders".to_string(),
                columns: Vec::new(),
                error: "the drain halted".to_string(),
                detected_at_micros: 1_727_222_400_000_004,
            })
        );
    }

    #[test]
    fn every_capture_failure_kind_word_is_in_the_load_time_set_once() {
        let names = capture_failure_kind_names();
        assert_eq!(names, vec!["capture", "halt", "disruption"]);
        for kind in CaptureFailureKind::ALL {
            assert!(names.contains(&kind.as_str()));
        }
    }

    #[test]
    fn a_healthy_status_crosses_as_its_word_with_no_failure() {
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        };

        assert_eq!(
            PlainDefinitionStatus::from(&status),
            PlainDefinitionStatus {
                status: "live",
                backfill_failure: None,
                capture_wait: None,
                capture_failure: None,
                held_keys: None,
                drain_failure: None,
                source_index_gaps: Vec::new(),
                paused_upstream_columns: Vec::new(),
                correcting: Vec::new(),
            }
        );
    }

    #[test]
    fn held_keys_cross_with_their_oldest_poison_time_in_microseconds() {
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: Some(trellis::HeldKeys {
                count: 3,
                oldest_poisoned_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
                max_retry_attempts: 0,
                next_retry_at: None,
            }),
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        };

        assert_eq!(
            PlainDefinitionStatus::from(&status).held_keys,
            Some(PlainHeldKeys {
                count: 3,
                oldest_poisoned_at_micros: 1_727_222_400_654_321,
            })
        );
    }

    #[test]
    fn paused_upstream_columns_cross_as_plain_strings() {
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::CatchingUp,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            correcting: Vec::new(),
            paused_upstream_columns: vec!["t.total".to_string()],
            build_wait: None,
        };
        let plain = PlainDefinitionStatus::from(&status);
        assert_eq!(plain.status, "catching_up");
        assert_eq!(plain.paused_upstream_columns, ["t.total"]);
    }

    #[test]
    fn a_source_index_gap_crosses_with_its_role_and_fix() {
        let gap = SourceIndexGap {
            table: "public.regions".to_string(),
            columns: vec!["id".to_string()],
            role: trellis::IndexRole::JoinJoined,
            opted_out: false,
        };
        assert_eq!(
            PlainSourceIndexGap::from(&gap),
            PlainSourceIndexGap {
                table: "public.regions".to_string(),
                columns: vec!["id".to_string()],
                role: "join_joined",
                opted_out: false,
                fix: "create index on \"public\".\"regions\" (\"id\")".to_string(),
            }
        );
        assert_eq!(trellis::IndexRole::GroupBy.as_str(), "group_by");
        assert_eq!(trellis::IndexRole::JoinFrom.as_str(), "join_from");
    }

    #[test]
    fn a_correcting_node_crosses_with_its_queue_time_in_microseconds() {
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::Backfilling,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: vec![
                CorrectingNode {
                    node: "public.items_p1".to_string(),
                    since: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
                    keys_remaining: Some(5),
                },
                CorrectingNode {
                    node: "public.items_p2".to_string(),
                    since: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
                    keys_remaining: None,
                },
            ],
            build_wait: None,
        };

        let plain = PlainDefinitionStatus::from(&status);

        assert_eq!(plain.status, "backfilling");
        assert_eq!(
            plain.correcting,
            vec![
                PlainCorrectingNode {
                    node: "public.items_p1".to_string(),
                    since_micros: 1_727_222_400_123_456,
                    keys_remaining: Some(5),
                },
                PlainCorrectingNode {
                    node: "public.items_p2".to_string(),
                    since_micros: 1_727_222_400_654_321,
                    keys_remaining: None,
                },
            ]
        );
    }

    #[test]
    fn a_drain_failure_crosses_with_its_times_in_microseconds() {
        let at = |micros| UNIX_EPOCH + Duration::from_micros(micros);
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: Some(DrainFailure {
                seg_seq: 17,
                tables: vec!["public.lines".to_string(), "public.orders".to_string()],
                error: "permission denied for function audit_hook".to_string(),
                sqlstate: Some("42501".to_string()),
                since: at(1_727_222_400_000_001),
                last_seen: at(1_727_222_400_654_321),
                attempts: 5,
            }),
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        };

        assert_eq!(
            PlainDefinitionStatus::from(&status).drain_failure,
            Some(PlainDrainFailure {
                seg_seq: 17,
                tables: vec!["public.lines".to_string(), "public.orders".to_string()],
                error: "permission denied for function audit_hook".to_string(),
                sqlstate: Some("42501".to_string()),
                since_micros: 1_727_222_400_000_001,
                last_seen_micros: 1_727_222_400_654_321,
                attempts: 5,
            })
        );
    }

    #[test]
    fn a_backfill_failure_crosses_with_its_retry_time_in_microseconds() {
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::WaitingToBackfill,
            backfill_failure: Some(BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 4,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
            }),
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        };

        assert_eq!(
            PlainDefinitionStatus::from(&status),
            PlainDefinitionStatus {
                status: "waiting_to_backfill",
                backfill_failure: Some(PlainBackfillFailure {
                    source_table: "public.orders".to_string(),
                    attempts: 4,
                    last_error: "permission denied for table orders".to_string(),
                    next_attempt_at_micros: 1_727_222_400_654_321,
                }),
                capture_wait: None,
                capture_failure: None,
                held_keys: None,
                drain_failure: None,
                source_index_gaps: Vec::new(),
                paused_upstream_columns: Vec::new(),
                correcting: Vec::new(),
            }
        );
    }

    #[test]
    fn a_capture_wait_and_failure_cross_with_their_times_in_microseconds() {
        let at = |micros| UNIX_EPOCH + Duration::from_micros(micros);
        let status = DefinitionStatus {
            join: None,
            status: TransformStatus::CatchingUp,
            backfill_failure: None,
            capture_wait: Some(CaptureWait {
                table: "public.orders".to_string(),
                operation: "widen".to_string(),
                lock_mode: "ShareRowExclusiveLock".to_string(),
                waiting_since: at(1_727_222_400_000_001),
                observed_at: at(1_727_222_400_000_002),
                blockers: vec!["pid 42 (client backend) holds RowExclusiveLock".to_string()],
            }),
            capture_failure: Some(CaptureFailure {
                kind: CaptureFailureKind::Capture,
                source_table: "public.lines".to_string(),
                columns: vec!["qty".to_string()],
                error: "capture of public.lines needs column \"qty\"".to_string(),
                detected_at: at(1_727_222_400_000_003),
            }),
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        };

        let plain = PlainDefinitionStatus::from(&status);

        assert_eq!(
            plain.capture_wait,
            Some(PlainCaptureWait {
                table: "public.orders".to_string(),
                operation: "widen".to_string(),
                lock_mode: "ShareRowExclusiveLock".to_string(),
                waiting_since_micros: 1_727_222_400_000_001,
                observed_at_micros: 1_727_222_400_000_002,
                blockers: vec!["pid 42 (client backend) holds RowExclusiveLock".to_string()],
            })
        );
        assert_eq!(
            plain.capture_failure,
            Some(PlainCaptureFailure {
                kind: "capture",
                source_table: "public.lines".to_string(),
                columns: vec!["qty".to_string()],
                error: "capture of public.lines needs column \"qty\"".to_string(),
                detected_at_micros: 1_727_222_400_000_003,
            })
        );
    }
}
