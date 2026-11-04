//! A registered transform crosses as its [`DefinitionSummary`] (what
//! `definitions()` lists): the target table, status, creation time and the
//! failures it carries. Nothing about its sources crosses outside a failure.
//! [`DefinitionStatus`] (what `status()` polls) flattens here too, with its
//! backfill failure's retry time, its capture wait's times, its capture
//! failure's detection time and its drain failure's times in epoch
//! microseconds. Its build wait crosses as the fence's transaction id, its
//! version handover as this binary's version and the other processes by
//! worker id and version, and a catalog floor this binary is below as the
//! floor and this binary's version. Its `JOIN` status crosses as the hidden tables'
//! sizes, the two warnings and the build step under way, the step as words
//! from [`JOIN_BUILD_STEPS`], [`JOIN_SIDES`] and [`JOIN_SIDE_PARTS`]. A
//! capture failure's kind
//! crosses as its word, one of [`capture_failure_kind_names`], which a host
//! turns into an atom or symbol from that set, allocated at load.

use trellis::{
    BackfillFailure, BelowCatalogFloor, BuildWait, CaptureFailure, CaptureFailureKind, CaptureWait,
    CorrectingNode, DefinitionStatus, DefinitionSummary, DrainFailure, FanOutWarning,
    HiddenTableSize, JoinBuildStep, JoinSide, JoinSidePart, JoinStatus, OneToOneBuildStep,
    OtherProcess, OtherVersionLive, PartialsWarning, SourceIndexGap,
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
    /// The transaction id of the fence a Re-derive build waits on instead of
    /// doing its own work; see [`DefinitionStatus::build_wait`] and
    /// [`BuildWait::Fence`]. A kind of wait that carries no id gets its own
    /// field beside this one.
    pub build_wait_xid: Option<i64>,
    /// Set while a process of another Trellis version is live, so this
    /// binary stands by; see [`DefinitionStatus::version_handover`].
    pub version_handover: Option<PlainVersionHandover>,
    /// Set while this binary is below the catalog floor, so it stands by and
    /// its state-changing calls refuse; see
    /// [`DefinitionStatus::below_catalog_floor`].
    pub below_catalog_floor: Option<PlainBelowCatalogFloor>,
    /// A `JOIN` transform's hidden tables, warnings and build step; see
    /// [`DefinitionStatus::join`]. `None` for any other transform.
    pub join: Option<PlainJoinStatus>,
    /// The oldest Trellis release that can run the definition,
    /// `major.minor.patch`; see [`DefinitionStatus::floor`].
    pub floor: String,
}

/// A [`JoinStatus`] flattened to plain data: how big a `JOIN` transform's
/// hidden tables are, the warnings that point at its cost, and the step its
/// build runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainJoinStatus {
    /// `P`, `M`, `J` and `T` (`R`, `M` and `J` for a 1-1 `JOIN`), in that
    /// order; a table that doesn't exist is left out.
    pub hidden_tables: Vec<PlainHiddenTable>,
    /// Set when `P` has grown close to the fact table's size; see
    /// [`JoinStatus::partials_warning`].
    pub partials_warning: Option<PlainPartialsWarning>,
    /// Set when a 1-1 `JOIN` has a join key with a very large group of `a`
    /// rows; see [`JoinStatus::fan_out_warning`].
    pub fan_out_warning: Option<PlainFanOutWarning>,
    /// The step the build runs now, or runs next while no worker holds its
    /// job; `None` when no build is under way.
    pub building: Option<PlainJoinBuildStep>,
}

/// A [`HiddenTableSize`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainHiddenTable {
    /// Which table: `P`, `M`, `J`, `T` or (a 1-1 `JOIN`'s) `R`.
    pub table: String,
    /// The table's name in the catalog schema.
    pub name: String,
    /// The estimated live rows (0 before the first analyze).
    pub estimated_rows: i64,
    /// The table's total size on disk with its indexes, in bytes.
    pub bytes: i64,
}

/// A [`PartialsWarning`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainPartialsWarning {
    /// `P`'s estimated rows.
    pub partials_rows: i64,
    /// The `FROM` (fact) table's estimated rows.
    pub fact_rows: i64,
}

/// A [`FanOutWarning`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainFanOutWarning {
    /// The join key's text: its columns' values, joined by `,`.
    pub key: String,
    /// The live `R` entries on the key.
    pub rows: i64,
    /// The threshold the group passed.
    pub threshold: i64,
}

/// A [`JoinBuildStep`] flattened to plain data: the step's kind as a word,
/// and the fields only some kinds carry, which are `None` for the others.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlainJoinBuildStep {
    /// One of [`JOIN_BUILD_STEPS`]: `whole`, `side`, `final`, a 1-1 `JOIN`'s
    /// `joined_rows`, `joined_null_keys`, `joined_gone`, `planning_chunks`,
    /// `chunks`, `sweep_stale` or `sweep_orphans`, or `unknown` for a step
    /// added since this binding was built.
    pub step: &'static str,
    /// For a `side` step, the side it builds: one of [`JOIN_SIDES`].
    pub side: Option<&'static str>,
    /// For a `side` step, what it builds of its side: one of
    /// [`JOIN_SIDE_PARTS`].
    pub part: Option<&'static str>,
    /// For a `side` step whose `part` is `leaf`, the leaf's recorded
    /// `schema.table` name.
    pub leaf: Option<String>,
    /// For a `chunks` step, how many chunks are still to run.
    pub remaining: Option<i64>,
}

/// Every [`PlainJoinBuildStep::step`] word, for a binding to intern up
/// front.
pub const JOIN_BUILD_STEPS: [&str; 11] = [
    "whole",
    "side",
    "final",
    "joined_rows",
    "joined_null_keys",
    "joined_gone",
    "planning_chunks",
    "chunks",
    "sweep_stale",
    "sweep_orphans",
    "unknown",
];

/// Every [`PlainJoinBuildStep::side`] word.
pub const JOIN_SIDES: [&str; 3] = ["from", "joined", "unknown"];

/// Every [`PlainJoinBuildStep::part`] word.
pub const JOIN_SIDE_PARTS: [&str; 5] = ["whole", "leaf", "remainder", "unroutable", "unknown"];

/// An [`OtherVersionLive`] flattened to plain data: another Trellis version
/// is live on the instance, so this binary changes nothing until it is gone
/// (ADR-0023).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainVersionHandover {
    /// This binary's version.
    pub this_version: String,
    /// The live processes holding this binary back.
    pub processes: Vec<PlainOtherProcess>,
    /// The staging-worker lock is held by a session no registered process
    /// names: a staging worker of a release before 0.2.0.
    pub unregistered_staging_worker: bool,
    /// Migrations this binary carries are pending, so every live process
    /// holds it back, whatever its version.
    pub migrations_pending: bool,
}

/// A [`BelowCatalogFloor`] flattened to plain data: this binary's version is
/// below the catalog floor, so it changes nothing (ADR-0024).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainBelowCatalogFloor {
    /// The floor the catalog records.
    pub floor: String,
    /// This binary's version.
    pub this_version: String,
}

impl From<&BelowCatalogFloor> for PlainBelowCatalogFloor {
    fn from(seen: &BelowCatalogFloor) -> Self {
        PlainBelowCatalogFloor {
            floor: seen.floor.clone(),
            this_version: seen.this_version.clone(),
        }
    }
}

/// An [`OtherProcess`] flattened to plain data: one live process of another
/// version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainOtherProcess {
    /// Its `worker_registry` id.
    pub worker_id: String,
    /// Its binary's version, `None` for a release before 0.2.0.
    pub version: Option<String>,
}

impl From<&OtherVersionLive> for PlainVersionHandover {
    fn from(seen: &OtherVersionLive) -> Self {
        PlainVersionHandover {
            this_version: seen.this_version.clone(),
            processes: seen.processes.iter().map(PlainOtherProcess::from).collect(),
            unregistered_staging_worker: seen.unregistered_staging_worker,
            migrations_pending: seen.migrations_pending,
        }
    }
}

impl From<&OtherProcess> for PlainOtherProcess {
    fn from(process: &OtherProcess) -> Self {
        PlainOtherProcess {
            worker_id: process.worker_id.clone(),
            version: process.version.clone(),
        }
    }
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
    /// `capture`, `halt`, `disruption` or `unknown`, as
    /// [`CaptureFailureKind::as_str`] spells it.
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
            build_wait_xid: status.build_wait.as_ref().and_then(|wait| match wait {
                BuildWait::Fence { xid } => Some(*xid),
                // `BuildWait` is `#[non_exhaustive]`: a kind of wait added
                // later gets its own plain field.
                _ => None,
            }),
            version_handover: status
                .version_handover
                .as_ref()
                .map(PlainVersionHandover::from),
            below_catalog_floor: status
                .below_catalog_floor
                .as_ref()
                .map(PlainBelowCatalogFloor::from),
            join: status.join.as_ref().map(PlainJoinStatus::from),
            floor: status.floor.clone(),
        }
    }
}

impl From<&JoinStatus> for PlainJoinStatus {
    fn from(join: &JoinStatus) -> Self {
        PlainJoinStatus {
            hidden_tables: join
                .hidden_tables
                .iter()
                .map(PlainHiddenTable::from)
                .collect(),
            partials_warning: join
                .partials_warning
                .as_ref()
                .map(PlainPartialsWarning::from),
            fan_out_warning: join.fan_out_warning.as_ref().map(PlainFanOutWarning::from),
            building: join.building.as_ref().map(PlainJoinBuildStep::from),
        }
    }
}

impl From<&HiddenTableSize> for PlainHiddenTable {
    fn from(table: &HiddenTableSize) -> Self {
        PlainHiddenTable {
            table: table.table.to_string(),
            name: table.name.clone(),
            estimated_rows: table.estimated_rows,
            bytes: table.bytes,
        }
    }
}

impl From<&PartialsWarning> for PlainPartialsWarning {
    fn from(warning: &PartialsWarning) -> Self {
        PlainPartialsWarning {
            partials_rows: warning.partials_rows,
            fact_rows: warning.fact_rows,
        }
    }
}

impl From<&FanOutWarning> for PlainFanOutWarning {
    fn from(warning: &FanOutWarning) -> Self {
        PlainFanOutWarning {
            key: warning.key.clone(),
            rows: warning.rows,
            threshold: warning.threshold,
        }
    }
}

impl From<&JoinBuildStep> for PlainJoinBuildStep {
    fn from(step: &JoinBuildStep) -> Self {
        let kind = |step| PlainJoinBuildStep {
            step,
            ..PlainJoinBuildStep::default()
        };
        // The enums are `#[non_exhaustive]`: a variant added later crosses
        // as `unknown` until the binding learns its word.
        match step {
            JoinBuildStep::Whole => kind("whole"),
            JoinBuildStep::Final => kind("final"),
            JoinBuildStep::Side { side, part } => {
                let (part, leaf) = match part {
                    JoinSidePart::Whole => ("whole", None),
                    JoinSidePart::Leaf(name) => ("leaf", Some(name.clone())),
                    JoinSidePart::Remainder => ("remainder", None),
                    JoinSidePart::Unroutable => ("unroutable", None),
                    _ => ("unknown", None),
                };
                PlainJoinBuildStep {
                    step: "side",
                    side: Some(match side {
                        JoinSide::From => "from",
                        JoinSide::Joined => "joined",
                        _ => "unknown",
                    }),
                    part: Some(part),
                    leaf,
                    remaining: None,
                }
            }
            JoinBuildStep::OneToOne(step) => match step {
                OneToOneBuildStep::JoinedRows => kind("joined_rows"),
                OneToOneBuildStep::JoinedNullKeys => kind("joined_null_keys"),
                OneToOneBuildStep::JoinedGone => kind("joined_gone"),
                OneToOneBuildStep::PlanningChunks => kind("planning_chunks"),
                OneToOneBuildStep::Chunks { remaining } => PlainJoinBuildStep {
                    remaining: Some(*remaining),
                    ..kind("chunks")
                },
                OneToOneBuildStep::SweepStale => kind("sweep_stale"),
                OneToOneBuildStep::SweepOrphans => kind("sweep_orphans"),
                _ => kind("unknown"),
            },
            _ => kind("unknown"),
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

    use trellis::{BuildWait, TransformStatus};

    use super::*;

    #[test]
    fn a_summary_crosses_with_its_creation_time_in_microseconds() {
        let summary = trellis::fixture!(DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: TransformStatus::Live,
            created_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
            backfill_failure: None,
            halt: None,
        });

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
        let summary = trellis::fixture!(DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: TransformStatus::WaitingToBackfill,
            created_at: UNIX_EPOCH,
            backfill_failure: Some(trellis::fixture!(BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 2,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
            })),
            halt: None,
        });

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
        let summary = trellis::fixture!(DefinitionSummary {
            target_table: "public.order_totals".to_string(),
            status: TransformStatus::Paused,
            created_at: UNIX_EPOCH,
            backfill_failure: None,
            halt: Some(trellis::fixture!(CaptureFailure {
                kind: CaptureFailureKind::Halt,
                source_table: "public.orders".to_string(),
                columns: Vec::new(),
                error: "the drain halted".to_string(),
                detected_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_000_004),
            })),
        });

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
        assert_eq!(names, vec!["capture", "halt", "disruption", "unknown"]);
        for kind in CaptureFailureKind::ALL {
            assert!(names.contains(&kind.as_str()));
        }
    }

    #[test]
    fn a_healthy_status_crosses_as_its_word_with_no_failure() {
        let status = trellis::fixture!(DefinitionStatus {
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
            floor: "0.3.1".to_string(),
        });

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
                build_wait_xid: None,
                version_handover: None,
                below_catalog_floor: None,
                join: None,
                floor: "0.3.1".to_string(),
            }
        );
    }

    #[test]
    fn held_keys_cross_with_their_oldest_poison_time_in_microseconds() {
        let status = trellis::fixture!(DefinitionStatus {
            join: None,
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: Some(trellis::fixture!(HeldKeys {
                count: 3,
                oldest_poisoned_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
                max_retry_attempts: 0,
                next_retry_at: None,
            })),
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        });

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
        let status = trellis::fixture!(DefinitionStatus {
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
        });
        let plain = PlainDefinitionStatus::from(&status);
        assert_eq!(plain.status, "catching_up");
        assert_eq!(plain.paused_upstream_columns, ["t.total"]);
    }

    #[test]
    fn a_source_index_gap_crosses_with_its_role_and_fix() {
        let gap = trellis::fixture!(SourceIndexGap {
            table: "public.regions".to_string(),
            columns: vec!["id".to_string()],
            role: trellis::IndexRole::JoinJoined,
            opted_out: false,
        });
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
        let status = trellis::fixture!(DefinitionStatus {
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
                trellis::fixture!(CorrectingNode {
                    node: "public.items_p1".to_string(),
                    since: UNIX_EPOCH + Duration::from_micros(1_727_222_400_123_456),
                    keys_remaining: Some(5),
                }),
                trellis::fixture!(CorrectingNode {
                    node: "public.items_p2".to_string(),
                    since: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
                    keys_remaining: None,
                }),
            ],
            build_wait: None,
        });

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
        let status = trellis::fixture!(DefinitionStatus {
            join: None,
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: Some(trellis::fixture!(DrainFailure {
                seg_seq: 17,
                tables: vec!["public.lines".to_string(), "public.orders".to_string()],
                error: "permission denied for function audit_hook".to_string(),
                sqlstate: Some("42501".to_string()),
                since: at(1_727_222_400_000_001),
                last_seen: at(1_727_222_400_654_321),
                attempts: 5,
            })),
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        });

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
        let status = trellis::fixture!(DefinitionStatus {
            join: None,
            status: TransformStatus::WaitingToBackfill,
            backfill_failure: Some(trellis::fixture!(BackfillFailure {
                source_table: "public.orders".to_string(),
                attempts: 4,
                last_error: "permission denied for table orders".to_string(),
                next_attempt_at: UNIX_EPOCH + Duration::from_micros(1_727_222_400_654_321),
            })),
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        });

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
                build_wait_xid: None,
                version_handover: None,
                below_catalog_floor: None,
                join: None,
                floor: "0.2.0".to_string(),
            }
        );
    }

    #[test]
    fn a_capture_wait_and_failure_cross_with_their_times_in_microseconds() {
        let at = |micros| UNIX_EPOCH + Duration::from_micros(micros);
        let status = trellis::fixture!(DefinitionStatus {
            join: None,
            status: TransformStatus::CatchingUp,
            backfill_failure: None,
            capture_wait: Some(trellis::fixture!(CaptureWait {
                table: "public.orders".to_string(),
                operation: "widen".to_string(),
                lock_mode: "ShareRowExclusiveLock".to_string(),
                waiting_since: at(1_727_222_400_000_001),
                observed_at: at(1_727_222_400_000_002),
                blockers: vec!["pid 42 (client backend) holds RowExclusiveLock".to_string()],
            })),
            capture_failure: Some(trellis::fixture!(CaptureFailure {
                kind: CaptureFailureKind::Capture,
                source_table: "public.lines".to_string(),
                columns: vec!["qty".to_string()],
                error: "capture of public.lines needs column \"qty\"".to_string(),
                detected_at: at(1_727_222_400_000_003),
            })),
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            paused_upstream_columns: Vec::new(),
            correcting: Vec::new(),
            build_wait: None,
        });

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

    #[test]
    fn a_build_waiting_on_a_fence_crosses_as_its_transaction_id() {
        let status = trellis::fixture!(DefinitionStatus {
            status: TransformStatus::Backfilling,
            build_wait: Some(BuildWait::Fence { xid: 4_294_967_301 }),
        });

        let plain = PlainDefinitionStatus::from(&status);

        assert_eq!(plain.build_wait_xid, Some(4_294_967_301));
        assert_eq!(plain.version_handover, None);
        assert_eq!(plain.below_catalog_floor, None);
    }

    #[test]
    fn a_catalog_floor_crosses_with_this_binarys_version() {
        let status = trellis::fixture!(DefinitionStatus {
            below_catalog_floor: Some(trellis::fixture!(BelowCatalogFloor {
                floor: "0.3.0".to_string(),
                this_version: "0.2.0".to_string(),
            })),
        });

        let plain = PlainDefinitionStatus::from(&status);

        assert_eq!(
            plain.below_catalog_floor,
            Some(PlainBelowCatalogFloor {
                floor: "0.3.0".to_string(),
                this_version: "0.2.0".to_string(),
            })
        );
    }

    #[test]
    fn a_version_handover_crosses_with_its_processes_by_worker_id_and_version() {
        let status = trellis::fixture!(DefinitionStatus {
            version_handover: Some(trellis::fixture!(OtherVersionLive {
                this_version: "0.2.0".to_string(),
                processes: vec![
                    trellis::fixture!(OtherProcess {
                        worker_id: "trellis-host-17-ab12".to_string(),
                        version: Some("0.1.0".to_string()),
                    }),
                    trellis::fixture!(OtherProcess {
                        worker_id: "trellis-host-18-cd34".to_string(),
                        version: None,
                    }),
                ],
                unregistered_staging_worker: true,
                migrations_pending: false,
            })),
        });

        let plain = PlainDefinitionStatus::from(&status);

        assert_eq!(plain.build_wait_xid, None);
        assert_eq!(
            plain.version_handover,
            Some(PlainVersionHandover {
                this_version: "0.2.0".to_string(),
                processes: vec![
                    PlainOtherProcess {
                        worker_id: "trellis-host-17-ab12".to_string(),
                        version: Some("0.1.0".to_string()),
                    },
                    PlainOtherProcess {
                        worker_id: "trellis-host-18-cd34".to_string(),
                        version: None,
                    },
                ],
                unregistered_staging_worker: true,
                migrations_pending: false,
            })
        );
    }

    #[test]
    fn a_join_status_crosses_with_its_tables_warnings_and_build_step() {
        // Every sibling number is distinct, and the sizes pass 32 bits, so a
        // swapped pair of fields or a narrowed integer fails.
        let status = trellis::fixture!(DefinitionStatus {
            join: Some(trellis::fixture!(JoinStatus {
                hidden_tables: vec![
                    trellis::fixture!(HiddenTableSize {
                        table: "P",
                        name: "join_7_p".to_string(),
                        estimated_rows: 5_000_000_011,
                        bytes: 9_000_000_017,
                    }),
                    trellis::fixture!(HiddenTableSize {
                        table: "M",
                        name: "join_7_m".to_string(),
                        estimated_rows: 21,
                        bytes: 8192,
                    }),
                ],
                partials_warning: Some(trellis::fixture!(PartialsWarning {
                    partials_rows: 6_000_000_013,
                    fact_rows: 7_000_000_019,
                })),
                fan_out_warning: Some(trellis::fixture!(FanOutWarning {
                    key: "42,eu".to_string(),
                    rows: 3_000_000_023,
                    threshold: 250_000,
                })),
                building: Some(JoinBuildStep::Final),
            })),
        });

        let plain = PlainDefinitionStatus::from(&status);

        assert_eq!(
            plain.join,
            Some(PlainJoinStatus {
                hidden_tables: vec![
                    PlainHiddenTable {
                        table: "P".to_string(),
                        name: "join_7_p".to_string(),
                        estimated_rows: 5_000_000_011,
                        bytes: 9_000_000_017,
                    },
                    PlainHiddenTable {
                        table: "M".to_string(),
                        name: "join_7_m".to_string(),
                        estimated_rows: 21,
                        bytes: 8192,
                    },
                ],
                partials_warning: Some(PlainPartialsWarning {
                    partials_rows: 6_000_000_013,
                    fact_rows: 7_000_000_019,
                }),
                fan_out_warning: Some(PlainFanOutWarning {
                    key: "42,eu".to_string(),
                    rows: 3_000_000_023,
                    threshold: 250_000,
                }),
                building: Some(PlainJoinBuildStep {
                    step: "final",
                    ..PlainJoinBuildStep::default()
                }),
            })
        );
    }

    #[test]
    fn a_definition_that_is_not_a_join_has_no_join_status() {
        let status = trellis::fixture!(DefinitionStatus {});

        assert_eq!(PlainDefinitionStatus::from(&status).join, None);
    }

    #[test]
    fn every_join_build_step_crosses_as_its_word_and_data() {
        let step = |step: JoinBuildStep| PlainJoinBuildStep::from(&step);
        let side = |side, part| step(JoinBuildStep::Side { side, part });
        let words =
            |step: PlainJoinBuildStep| (step.step, step.side, step.part, step.leaf, step.remaining);

        assert_eq!(
            words(step(JoinBuildStep::Whole)),
            ("whole", None, None, None, None)
        );
        assert_eq!(
            words(step(JoinBuildStep::Final)),
            ("final", None, None, None, None)
        );
        assert_eq!(
            words(side(JoinSide::From, JoinSidePart::Whole)),
            ("side", Some("from"), Some("whole"), None, None)
        );
        assert_eq!(
            words(side(
                JoinSide::Joined,
                JoinSidePart::Leaf("public.items_p2".to_string())
            )),
            (
                "side",
                Some("joined"),
                Some("leaf"),
                Some("public.items_p2".to_string()),
                None
            )
        );
        assert_eq!(
            words(side(JoinSide::From, JoinSidePart::Remainder)),
            ("side", Some("from"), Some("remainder"), None, None)
        );
        assert_eq!(
            words(side(JoinSide::Joined, JoinSidePart::Unroutable)),
            ("side", Some("joined"), Some("unroutable"), None, None)
        );
        for (one_to_one, word) in [
            (OneToOneBuildStep::JoinedRows, "joined_rows"),
            (OneToOneBuildStep::JoinedNullKeys, "joined_null_keys"),
            (OneToOneBuildStep::JoinedGone, "joined_gone"),
            (OneToOneBuildStep::PlanningChunks, "planning_chunks"),
            (OneToOneBuildStep::SweepStale, "sweep_stale"),
            (OneToOneBuildStep::SweepOrphans, "sweep_orphans"),
        ] {
            assert_eq!(
                words(step(JoinBuildStep::OneToOne(one_to_one))),
                (word, None, None, None, None)
            );
        }
        assert_eq!(
            words(step(JoinBuildStep::OneToOne(OneToOneBuildStep::Chunks {
                remaining: 4_294_967_301,
            }))),
            ("chunks", None, None, None, Some(4_294_967_301))
        );
    }

    #[test]
    fn the_join_word_sets_hold_every_word_a_step_can_carry() {
        let steps = [
            JoinBuildStep::Whole,
            JoinBuildStep::Final,
            JoinBuildStep::Side {
                side: JoinSide::From,
                part: JoinSidePart::Whole,
            },
            JoinBuildStep::Side {
                side: JoinSide::Joined,
                part: JoinSidePart::Leaf(String::new()),
            },
            JoinBuildStep::Side {
                side: JoinSide::From,
                part: JoinSidePart::Remainder,
            },
            JoinBuildStep::Side {
                side: JoinSide::Joined,
                part: JoinSidePart::Unroutable,
            },
            JoinBuildStep::OneToOne(OneToOneBuildStep::JoinedRows),
            JoinBuildStep::OneToOne(OneToOneBuildStep::JoinedNullKeys),
            JoinBuildStep::OneToOne(OneToOneBuildStep::JoinedGone),
            JoinBuildStep::OneToOne(OneToOneBuildStep::PlanningChunks),
            JoinBuildStep::OneToOne(OneToOneBuildStep::Chunks { remaining: 1 }),
            JoinBuildStep::OneToOne(OneToOneBuildStep::SweepStale),
            JoinBuildStep::OneToOne(OneToOneBuildStep::SweepOrphans),
        ];
        let mut seen_steps = Vec::new();
        let mut seen_parts = Vec::new();
        for step in &steps {
            let plain = PlainJoinBuildStep::from(step);
            assert!(JOIN_BUILD_STEPS.contains(&plain.step), "{}", plain.step);
            seen_steps.push(plain.step);
            if let Some(side) = plain.side {
                assert!(JOIN_SIDES.contains(&side), "{side}");
            }
            if let Some(part) = plain.part {
                assert!(JOIN_SIDE_PARTS.contains(&part), "{part}");
                seen_parts.push(part);
            }
        }
        // Each word but `unknown`, which no variant known now maps to, is
        // reached.
        for word in JOIN_BUILD_STEPS.iter().filter(|word| **word != "unknown") {
            assert!(seen_steps.contains(word), "{word}");
        }
        for word in JOIN_SIDE_PARTS.iter().filter(|word| **word != "unknown") {
            assert!(seen_parts.contains(word), "{word}");
        }
    }
}
