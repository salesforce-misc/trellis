//! A registered transform definition crosses as its summary fields, never as
//! [`Definition`] itself: that carries the parsed `TransformDef` AST and a
//! `HashMap<String, ValueType>`, neither of which a host can represent
//! without inventing semantics (ADR-0010 decision 4). An embedder who wants
//! the AST wants the Rust crate.
//!
//! `trellis` reports a definition in two shapes, and each flattens here on its
//! own: [`Definition`] (what `apply` returns for a `TRANSFORM` statement) has
//! the source columns but no creation time, and [`DefinitionSummary`] (what
//! `definitions()` lists) has the creation time but no source columns.
//! [`DefinitionStatus`] (what `status()` polls) flattens here too, with its
//! backfill failure's retry time, its capture wait's times and its capture
//! failure's detection time in epoch microseconds.

use std::collections::BTreeMap;

use trellis::{
    BackfillFailure, CaptureFailure, CaptureWait, Definition, DefinitionStatus, DefinitionSummary,
};

use crate::{epoch_micros, transform_status};

/// A [`Definition`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinition {
    pub id: i64,
    /// The fully-qualified `schema.table` the definition writes.
    pub target_table: String,
    /// The fully-qualified `schema.table` the definition reads.
    pub source_table: String,
    pub source_version: i64,
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// Each source column the definition was validated against, by name, with
    /// its type's name (`numeric`, `bigint`, `text`, `inet`, ...). Ordered by
    /// column name, so a host sees the same order on every call.
    pub source_columns: BTreeMap<String, String>,
}

impl From<&Definition> for PlainDefinition {
    fn from(definition: &Definition) -> Self {
        PlainDefinition {
            id: definition.id,
            target_table: definition.target_table.clone(),
            source_table: definition.source_table.clone(),
            source_version: definition.source_version,
            status: transform_status(definition.status),
            source_columns: definition
                .source_columns
                .iter()
                .map(|(name, value_type)| (name.clone(), value_type.to_string()))
                .collect(),
        }
    }
}

/// A [`DefinitionSummary`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinitionSummary {
    pub id: i64,
    /// The fully-qualified `schema.table` the definition writes.
    pub target_table: String,
    /// The fully-qualified `schema.table` the definition reads.
    pub source_table: String,
    pub source_version: i64,
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// When the definition was registered, as [`crate::epoch_micros`].
    pub created_at_micros: i64,
    /// Set while the definition's source table's backfill keeps failing to
    /// discharge; see [`DefinitionSummary::backfill_failure`].
    pub backfill_failure: Option<PlainBackfillFailure>,
}

impl From<&DefinitionSummary> for PlainDefinitionSummary {
    fn from(summary: &DefinitionSummary) -> Self {
        PlainDefinitionSummary {
            id: summary.id,
            target_table: summary.target_table.clone(),
            source_table: summary.source_table.clone(),
            source_version: summary.source_version,
            status: transform_status(summary.status),
            created_at_micros: epoch_micros(summary.created_at),
            backfill_failure: summary
                .backfill_failure
                .as_ref()
                .map(PlainBackfillFailure::from),
        }
    }
}

/// A [`DefinitionStatus`] flattened to plain data: what a host polls until a
/// definition reaches `live`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainDefinitionStatus {
    /// The status's `as_str()` word; see [`crate::transform_status`].
    pub status: &'static str,
    /// Set while the definition's source table's backfill keeps failing to
    /// discharge; see [`DefinitionStatus::backfill_failure`].
    pub backfill_failure: Option<PlainBackfillFailure>,
    /// Set while the definition's capture waits on a table lock; see
    /// [`DefinitionStatus::capture_wait`].
    pub capture_wait: Option<PlainCaptureWait>,
    /// Set while capture of a table the definition reads is broken; see
    /// [`DefinitionStatus::capture_failure`].
    pub capture_failure: Option<PlainCaptureFailure>,
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
    /// The fully-qualified `schema.table` whose capture is broken.
    pub source_table: String,
    /// The columns the failure is about; empty when it isn't about one.
    pub columns: Vec<String>,
    /// A sentence naming the cause.
    pub error: String,
    /// When it was first found, as [`crate::epoch_micros`].
    pub detected_at_micros: i64,
}

/// A [`BackfillFailure`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainBackfillFailure {
    /// The fully-qualified `schema.table` the backfill marker is parked on.
    pub source_table: String,
    /// How many discharges have failed since the marker was parked.
    pub attempts: u32,
    /// The latest failure's error message.
    pub last_error: String,
    /// The earliest next discharge attempt, as [`crate::epoch_micros`].
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
            source_table: failure.source_table.clone(),
            columns: failure.columns.clone(),
            error: failure.error.clone(),
            detected_at_micros: epoch_micros(failure.detected_at),
        }
    }
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
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    use trellis::dev::defs::{PgType, ValueType, parse};
    use trellis::{FloatWidth, IntWidth, TransformStatus};

    use super::*;

    #[test]
    fn a_definition_crosses_as_its_summary_and_column_type_names() {
        let definition = Definition {
            id: 7,
            source_version: 3,
            def: parse("TRANSFORM order_totals FROM orders SELECT amount + tax AS total").unwrap(),
            source_columns: HashMap::from([
                ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
                ("amount".to_string(), ValueType::Numeric),
                ("tax".to_string(), ValueType::Float(FloatWidth::Float8)),
                ("note".to_string(), ValueType::Text),
                ("shipped".to_string(), ValueType::Boolean),
                ("buyer".to_string(), ValueType::Uuid),
                ("origin".to_string(), ValueType::Other(PgType::Inet)),
            ]),
            status: TransformStatus::Backfilling,
            source_table: "public.orders".to_string(),
            target_table: "public.order_totals".to_string(),
        };

        let plain = PlainDefinition::from(&definition);

        assert_eq!(
            plain,
            PlainDefinition {
                id: 7,
                target_table: "public.order_totals".to_string(),
                source_table: "public.orders".to_string(),
                source_version: 3,
                status: "backfilling",
                source_columns: BTreeMap::from([
                    ("amount".to_string(), "numeric".to_string()),
                    ("buyer".to_string(), "uuid".to_string()),
                    ("id".to_string(), "bigint".to_string()),
                    ("note".to_string(), "text".to_string()),
                    ("origin".to_string(), "inet".to_string()),
                    ("shipped".to_string(), "boolean".to_string()),
                    ("tax".to_string(), "double precision".to_string()),
                ]),
            }
        );
    }

    #[test]
    fn a_summary_crosses_with_its_creation_time_in_microseconds() {
        let summary = DefinitionSummary {
            id: 7,
            target_table: "public.order_totals".to_string(),
            source_table: "public.orders".to_string(),
            source_version: 3,
            status: TransformStatus::Live,
            created_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
            backfill_failure: None,
        };

        let plain = PlainDefinitionSummary::from(&summary);

        assert_eq!(
            plain,
            PlainDefinitionSummary {
                id: 7,
                target_table: "public.order_totals".to_string(),
                source_table: "public.orders".to_string(),
                source_version: 3,
                status: "live",
                created_at_micros: 1_727_222_400_123_456,
                backfill_failure: None,
            }
        );
    }

    #[test]
    fn a_summary_carries_its_backfill_failure() {
        let summary = DefinitionSummary {
            id: 7,
            target_table: "public.order_totals".to_string(),
            source_table: "public.orders".to_string(),
            source_version: 3,
            status: TransformStatus::WaitingToBackfill,
            created_at: UNIX_EPOCH,
            backfill_failure: Some(BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 2,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
            }),
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
    fn a_healthy_status_crosses_as_its_word_with_no_failure() {
        let status = DefinitionStatus {
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
        };

        assert_eq!(
            PlainDefinitionStatus::from(&status),
            PlainDefinitionStatus {
                status: "live",
                backfill_failure: None,
                capture_wait: None,
                capture_failure: None,
            }
        );
    }

    #[test]
    fn a_backfill_failure_crosses_with_its_retry_time_in_microseconds() {
        let status = DefinitionStatus {
            status: TransformStatus::WaitingToBackfill,
            backfill_failure: Some(BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 4,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
            }),
            capture_wait: None,
            capture_failure: None,
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
            }
        );
    }

    #[test]
    fn a_capture_wait_and_failure_cross_with_their_times_in_microseconds() {
        let at = |micros| UNIX_EPOCH + Duration::from_micros(micros);
        let status = DefinitionStatus {
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
                source_table: "public.lines".to_string(),
                columns: vec!["qty".to_string()],
                error: "capture of public.lines needs column \"qty\"".to_string(),
                detected_at: at(1_727_222_400_000_003),
            }),
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
                source_table: "public.lines".to_string(),
                columns: vec!["qty".to_string()],
                error: "capture of public.lines needs column \"qty\"".to_string(),
                detected_at_micros: 1_727_222_400_000_003,
            })
        );
    }
}
