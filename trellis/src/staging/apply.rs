//! Apply ∪ mark-drained (stage 05). See
//! docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md.
//!
//! A 1-1 definition's records are applied here; an aggregate's and a
//! `JOIN`'s are handed to [`super::group_basis`] and [`super::join`] in
//! Phase 3.
//!
//! The design's three phases map onto three functions:
//!
//! - Phase 1 (the claim, committed on its own) is `drain_segments`' opening
//!   block, reusing [`super::claim::claim`] directly. The fold runs after it
//!   commits, in its own read transaction: [`super::fold::fold_limited`] for
//!   a share that fits `drain_batch_cap`, pages from [`super::page`]
//!   otherwise.
//! - Phase 2 (compute: read the live rows and build each change's target
//!   write, no transaction, no locks; Postgres evaluates the fields in
//!   Phase 3's upsert) is [`compute`].
//! - Phase 3 (apply ∪ mark-drained, one transaction) is
//!   [`apply_and_mark_drained`].
//!
//! [`drain_once`] is the orchestrator tying the three together, including
//! the version-fence/serialization retry loop the design calls for.
//! [`next_claimable_segment`] is the "which batch should a free worker pick
//! up next" query a real drain loop (not assembled here)
//! would call before it.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::types::{PgLsn, ToSql};
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::KeySpace;
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::validate::ValidationError;
use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident, quote_literal};

use super::append;
use super::claim;
use super::claim::{HeldShare, held_share};
use super::error::StagingError;
use super::fold::{self, FoldedChange, earliest_origin};
use super::liveness::FenceMissBackoff;
use super::one_to_one_ledger;
use super::quarantine;
use super::target_mutations::TargetMutations;

/// The absolute ceiling on [`FoldedChange::hop_gen`] propagation, a backstop
/// over and above the schema-derived hop bound doc 05 describes ("one past
/// its deepest trigger"): even if the catalog's own graph analysis is wrong
/// or a definition cycle somehow reaches this stage, propagation cannot
/// wind up more than this many hops before [`ApplyError::HopBoundExceeded`]
/// stops it. `defs::validate` already rejects definition cycles at
/// creation time (`detect_cycle`), so this is defense-in-depth, not the
/// primary guard.
pub const MAX_HOP_GEN: i32 = 32;

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Why a drain attempt (fold, compute, or apply ∪ mark-drained) failed. A
/// module-local enum, plain `Display` + `std::error::Error`, composing with
/// the crate's other error types via `From` — matching
/// [`StagingError`]/[`CatalogError`]/[`DdlError`]'s own
/// convention.
#[derive(Debug)]
#[non_exhaustive]
pub enum ApplyError {
    /// A failure from the staging ring (claim, fold, append).
    Staging(StagingError),
    /// A failure reading the transform catalog.
    Catalog(CatalogError),
    /// A failure introspecting a source table's primary key.
    Ddl(DdlError),
    /// A definition's calculated fields failed to type-infer against its own
    /// persisted `source_columns` — meaning a definition that already passed
    /// `crate::defs::validate::validate` at creation time no longer
    /// type-checks against the map it was created with, which should not be
    /// reachable; kept as a typed error rather than a panic per this
    /// module's own convention of not trusting invariants it cannot enforce
    /// itself.
    Validate(ValidationError),
    /// Substituting a `GROUP BY` definition's cross-field-alias references
    /// (`defs::backfill::substituted_field_exprs`) failed — a cyclic alias
    /// chain or a pathologically large expansion. Both are rejected by
    /// `crate::defs::validate::validate` (a real cycle) or bounded at
    /// definition-creation time (the direct backfill's node budget) before a
    /// definition can ever reach live apply, so this should not be reachable
    /// for a definition that already passed backfill at creation time — kept
    /// as a typed error rather than a panic per this module's convention of
    /// not trusting invariants it cannot enforce itself.
    Backfill(crate::defs::backfill::BackfillError),
    /// A direct Postgres protocol/query error, for statements this module
    /// runs itself (the version fence, the per-target apply statement, the
    /// completion statement) rather than through another module's helper.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// The completion statement's `DELETE FROM seg_claims ... RETURNING
    /// bucket` matched no rows: this worker's claim was gone by the time
    /// Phase 3 tried to complete it (reclaimed on TTL, or raced by another
    /// worker). Nothing was applied twice — Phase 3 is one transaction and
    /// this is checked before it commits — but the caller must not treat
    /// its work as done; the buckets it thought it owned need reclaiming by
    /// whoever holds them now.
    ClaimLost,
    /// A non-final page found its buckets' `drain_cursor` already at its own
    /// last key once it held their claims: the same page committed before,
    /// and only the reply to its `COMMIT` was lost. Its transaction
    /// rolls back, so nothing applies twice, and the drain carries on from
    /// the next page (`drain_batch`). The group basis needs this: a page
    /// adds its deltas to the groups' partials, with nothing per key that
    /// would make a second application move nothing.
    PageAlreadyCommitted,
    /// Phase 3's version fence found `source_table_versions.version` had
    /// moved since Phase 2 loaded it: a definition change landed on
    /// `src_table` mid-drain. Routine and immediately retryable — see
    /// `super::liveness::release`'s doc comment on why a fence miss is
    /// not parked behind the reclaim TTL.
    VersionFenceMiss { src_table: String },
    /// The entry lock of a 1-1 target (`super::one_to_one_ledger::lock_entries`)
    /// found a key with no entry: the tombstone GC
    /// (`super::retire::collect_tombstones`) collected it between
    /// the placeholder insert and the sorted `for update`. Transient,
    /// like a deadlock: the transaction rolls back, giving up every lock it
    /// took, and its retry inserts the key's placeholder afresh. Retaking
    /// the lock inside the transaction instead would insert that placeholder
    /// while holding the other keys' locks, out of I5's one order.
    LedgerEntryCollected { target: String },
    /// Downstream propagation would have staged a `Recompute` row past
    /// `MAX_HOP_GEN`. Named rather than silently truncated: an operator
    /// needs to know a wave ran away, and which target tables it ran away
    /// through, rather than have the tail of it quietly disappear.
    HopBoundExceeded { hop_gen: i32, tables: Vec<String> },
    /// A group-basis aggregate target (`super::group_basis`) met
    /// what it doesn't support yet: an aggregate it doesn't route, a ring
    /// row missing the images its change needs, a fresh build or
    /// resume over a partitioned source that has rows before its tree is
    /// recorded, or a build or rebuild over one whose tree is not recorded or
    /// has a leaf with no usable constraint text; or, on a `JOIN`
    /// (`super::join`), a field build, or a partitioned side whose tree
    /// is not recorded or has a leaf with no usable constraint text.
    /// Every page of the target would reproduce it, so it halts the target
    /// rather than charging each key a death.
    GroupBasisUnsupported { target: String, what: &'static str },
    /// A `JOIN`'s `T` write (`super::group_basis`, ADR-0019's strict form)
    /// staged `groups` groups that have no hidden row: the writer didn't lock
    /// them (`GroupTargetPlan::lock_sql` inserts the missing rows and locks
    /// the rest), so applying would have dropped their deltas. A gap in the
    /// lock set, never a state of the data. The transaction rolls back.
    GroupLockGap { target: String, groups: i64 },
    /// A folded record names a source table Postgres no longer has
    /// (`42P01` from a live query against it) — the one sanctioned
    /// exception to immutability: no retry or per-key quarantine can
    /// resolve this, since the table itself is gone, not any one row.
    /// `drain_once` routes this to `quarantine::purge_dropped_table`
    /// rather than the ordinary isolate/evict path.
    SourceTableDropped { source_table: String },
    /// `super::quarantine::resume_column` (or, indirectly,
    /// a `RESUME TRANSFORM <target>.<column>` statement,
    /// [`crate::app::Trellis::apply`]) was asked to resume a
    /// `(transform, column)` pair with no currently-paused `column_status`
    /// row — resuming a column that isn't paused is caller error, not a
    /// silent no-op. Also reused for "no such column on this definition at
    /// all," so an address naming a real transform but the wrong field name
    /// gets a specific error rather than silently doing nothing.
    ColumnNotPaused { transform: String, column: String },
    /// `super::quarantine::resume_column` was asked to resume a column
    /// whose owning definition is not currently applying
    /// (`live` or
    /// `catching_up`) — most concretely, a definition still `Backfilling`
    /// behind an in-flight `backfill_chunks` queue nothing is draining. `resume_column`
    /// takes one snapshot of the *source* table and only clears
    /// `column_status` after writing it back, so any row a still-running
    /// backfill chunk inserts into the target *during* that window is never
    /// in the snapshot and never revisited once the column is unpaused —
    /// permanently stranding that row's column at NULL/default while
    /// `resume_column` reports success. This branch's cascade pause
    /// (`defs::catalog::column_dependents`, unlike the applying-status
    /// filtered paths CDC apply uses) can reach a downstream definition in
    /// exactly this state, so the gate is not just theoretical. Refusing to
    /// resume until the definition's build has finished closes the window
    /// instead of racing it.
    DefinitionNotLive { transform: String },
    /// `super::quarantine::resume_column` was asked to resume an `ALTER
    /// TRANSFORM` field that is paused until its source's capture images the
    /// column it reads (`column_status.awaiting_capture`). Unpausing it
    /// before then would let a row the narrower capture function staged,
    /// which lacks that column, reach it and fail for want of it. The
    /// field build's start unpauses it once the widened capture lands
    /// (`staging::build`'s "Field builds"); the definition's `capture_wait`
    /// or `capture_failure` status says what holds that up.
    ColumnAwaitingCapture { transform: String, column: String },
    /// A failure from `crate::intake`'s helpers (rebuilding the readers
    /// of a table, or qualifying a table name).
    Intake(crate::intake::IntakeError),
    /// `super::quarantine::resume_transform` was asked to resume a target
    /// with no corresponding `transform_definitions` row at all.
    TransformNotFound { transform: String },
    /// `super::quarantine::resume_transform` was asked to resume a target
    /// whose current status is neither of ADR-0014's two frozen states —
    /// [`crate::defs::model::TransformStatus::Quarantined`] (the poison fuse
    /// tripped it) nor [`crate::defs::model::TransformStatus::Paused`] (an
    /// operator froze it deliberately). Resuming a transform that isn't
    /// frozen at all is caller error, not a silent no-op, mirroring
    /// [`ApplyError::ColumnNotPaused`]'s same discipline for the
    /// column-level tier.
    TransformNotPaused { transform: String },
    /// `super::quarantine::release_key` was asked to release a key the
    /// transform doesn't hold: no `poison` or `poison_held` row of the
    /// transform names that source table and key, in either spelling of the
    /// table. Releasing a key that isn't held is caller error, not a silent
    /// no-op, as resuming a column that isn't paused is
    /// ([`ApplyError::ColumnNotPaused`]). It covers a table the transform
    /// doesn't read and a key it never held alike.
    KeyNotHeld {
        transform: String,
        src_table: String,
        key: String,
    },
    /// `super::quarantine::release_key`'s first lock, the bump of the key's
    /// table's version fence, waited out the session's `lock_timeout`
    /// (`crate::locks::LOCK_TIMEOUT`, 30 s, unless the session set a
    /// shorter one). The release waits for every drain page holding
    /// that fence, the pages in flight on the table, so a page that parked a
    /// change for the key has committed before the release reads the key's
    /// rows; a page that holds it longer than the timeout makes the release
    /// give up. A define, an alter or a column build on the table holds the
    /// fence to its commit too, so it can be one of those the release waits
    /// out. The bump never lands and its transaction rolls back, so
    /// nothing changed and the key is still held: retry the release. Only
    /// this wait is named: a lock timeout anywhere else in the release is the
    /// raw [`ApplyError::Db`], as it is in every other operator call.
    ReleaseLockTimeout {
        transform: String,
        src_table: String,
        key: String,
    },
    /// A column pause, resume, fuse trip or cascade pair, or a build's
    /// release of capture-widen pauses, waited for the column-pause lock
    /// (`crate::locks::lock_column_pauses`) past its transaction's
    /// `lock_timeout`. Its transaction rolled back with nothing
    /// written: retry the call. A fuse trip and the capture pass's
    /// callers retry on their next pass.
    ColumnPauseLockTimeout(crate::locks::ColumnPauseLockTimeout),
    /// `super::quarantine::resume_transform` or
    /// `super::quarantine::resume_column` re-ran define-time validation
    /// against the live schema and it failed: define would
    /// refuse the definition as the schema stands now, so a rebuild would
    /// build from what it no longer matches. `reason` is define's own error,
    /// naming the column and what to change. Nothing changed: the
    /// definition (or field) stays paused.
    ResumeRefused {
        transform: String,
        reason: Box<crate::defs::catalog::CatalogError>,
    },
    /// A folded truncate of `src_table` carries no ring `lsn`. Not
    /// reachable: the capture trigger is the only writer of a truncate's
    /// ring row and stamps it with `pg_current_wal_insert_lsn()`, and
    /// `StagedChange::Truncate` requires one. Without it the truncate could
    /// raise no truncate floor, and a change from before it that still
    /// reached a page would apply over the truncate (I2). It halts the
    /// definitions reading `src_table` rather than clearing their targets
    /// without the floor.
    TruncateWithoutLsn { src_table: String },
}

impl ApplyError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever
    /// one nests here, so the mapping composes rather than re-deriving a
    /// category this crate already has one for.
    /// [`ApplyError::SourceTableDropped`] names a source table that no
    /// longer exists -> [`ErrorCode::NotFound`]; [`ApplyError::ClaimLost`],
    /// [`ApplyError::VersionFenceMiss`], and [`ApplyError::HopBoundExceeded`]
    /// are all internal drain-mechanics conditions the caller can't act on
    /// beyond "retry" -> [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        match self {
            ApplyError::Staging(err) => err.code(),
            ApplyError::Catalog(err) => err.code(),
            ApplyError::Ddl(err) => err.code(),
            ApplyError::Validate(err) => err.code(),
            ApplyError::Backfill(err) => err.code(),
            ApplyError::Db(err) => error_code::classify_pg_error(err),
            ApplyError::Pool(err) => err.code(),
            ApplyError::ClaimLost
            | ApplyError::PageAlreadyCommitted
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::LedgerEntryCollected { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::GroupBasisUnsupported { .. }
            | ApplyError::GroupLockGap { .. }
            | ApplyError::TruncateWithoutLsn { .. } => ErrorCode::Internal,
            ApplyError::SourceTableDropped { .. } => ErrorCode::NotFound,
            ApplyError::ColumnNotPaused { .. } => ErrorCode::NotFound,
            // The definition's persisted status conflicts with what
            // `resume_column` was asked to do, the same category
            // `StagingError::ProducerAlreadyRunning` uses for "existing state
            // blocks this request" rather than "the request itself is
            // malformed" (-> Validation) or "nothing by that name exists"
            // (-> NotFound).
            ApplyError::DefinitionNotLive { .. } | ApplyError::ColumnAwaitingCapture { .. } => {
                ErrorCode::Conflict
            }
            ApplyError::Intake(err) => err.code(),
            ApplyError::TransformNotFound { .. } => ErrorCode::NotFound,
            ApplyError::TransformNotPaused { .. } => ErrorCode::Conflict,
            ApplyError::KeyNotHeld { .. } => ErrorCode::NotFound,
            // A wait that ran out, not a fault: the release changed nothing
            // and the same call succeeds once the table's pages commit.
            ApplyError::ReleaseLockTimeout { .. } => ErrorCode::Timeout,
            // The same kind of wait, on the column-pause lock.
            ApplyError::ColumnPauseLockTimeout(_) => ErrorCode::Timeout,
            // The schema blocks the request, as define's own refusal does.
            ApplyError::ResumeRefused { reason, .. } => reason.code(),
        }
    }
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyError::Staging(err) => write!(f, "staging ring error: {err}"),
            ApplyError::Catalog(err) => write!(f, "transform catalog error: {err}"),
            ApplyError::Ddl(err) => write!(f, "target-table DDL error: {err}"),
            ApplyError::Validate(err) => {
                write!(f, "calculated-field type inference error: {err}")
            }
            ApplyError::Backfill(err) => {
                write!(f, "calculated-field alias substitution error: {err}")
            }
            ApplyError::Db(err) => {
                write!(f, "apply database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            ApplyError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            ApplyError::ClaimLost => write!(
                f,
                "this worker's claim was gone by completion time; nothing was applied twice, \
                 but the buckets it thought it owned must be reclaimed by whoever holds them now"
            ),
            ApplyError::PageAlreadyCommitted => write!(
                f,
                "this page committed before and only its commit's reply was lost; nothing was \
                 applied twice"
            ),
            ApplyError::VersionFenceMiss { src_table } => write!(
                f,
                "source table '{src_table}' changed definitions mid-drain; retry against the \
                 current catalog"
            ),
            ApplyError::LedgerEntryCollected { target } => write!(
                f,
                "a ledger entry of '{target}' was collected while its lock was being taken; \
                 retry the transaction"
            ),
            ApplyError::GroupBasisUnsupported { target, what } => write!(
                f,
                "group-basis aggregate target '{target}' can't apply {what}"
            ),
            ApplyError::GroupLockGap { target, groups } => write!(
                f,
                "a write of '{target}''s group table staged {groups} group(s) that no lock \
                 covered (no hidden row to apply their change to)"
            ),
            ApplyError::HopBoundExceeded { hop_gen, tables } => write!(
                f,
                "downstream propagation exceeded the hop bound (hop_gen {hop_gen} > \
                 {MAX_HOP_GEN}) through: {tables:?}"
            ),
            ApplyError::SourceTableDropped { source_table } => write!(
                f,
                "source table '{source_table}' no longer exists; purging its staged rows"
            ),
            ApplyError::ColumnNotPaused { transform, column } => write!(
                f,
                "'{transform}.{column}' is not currently paused (or is not a column of that \
                 definition)"
            ),
            ApplyError::DefinitionNotLive { transform } => write!(
                f,
                "'{transform}' is not currently live (it may still be backfilling); resuming a \
                 paused column requires its definition to be live first"
            ),
            ApplyError::ColumnAwaitingCapture { transform, column } => write!(
                f,
                "'{transform}.{column}' awaits the capture of its source's new column and \
                 unpauses on its own once the capture images it (the definition's status says \
                 what holds that up); RESUME it then only if it was also paused for another \
                 reason"
            ),
            ApplyError::Intake(err) => write!(f, "intake error: {err}"),
            ApplyError::TransformNotFound { transform } => {
                write!(f, "no transform named '{transform}' is registered")
            }
            ApplyError::TransformNotPaused { transform } => write!(
                f,
                "'{transform}' is not currently paused; resuming it re-runs its full \
                 backfill, which is only valid from `paused` or `quarantined`"
            ),
            ApplyError::KeyNotHeld {
                transform,
                src_table,
                key,
            } => write!(
                f,
                "'{transform}' holds no key {key:?} of '{src_table}' in quarantine"
            ),
            ApplyError::ReleaseLockTimeout {
                transform,
                src_table,
                key,
            } => write!(
                f,
                "releasing key {key:?} of '{src_table}' for '{transform}' timed out waiting for \
                 the work in flight on '{src_table}' to commit (its drain pages, and any define, \
                 alter or column build on it, which hold its version fence); nothing changed \
                 and the key is still held: retry the release"
            ),
            ApplyError::ColumnPauseLockTimeout(err) => write!(f, "{err}"),
            ApplyError::ResumeRefused { transform, reason } if reason.is_define_refusal() => {
                write!(
                    f,
                    "'{transform}' can't resume: define would refuse it as the schema stands now: \
                 {reason}. It stays paused"
                )
            }
            ApplyError::ResumeRefused { transform, reason } => {
                write!(f, "'{transform}' can't resume: {reason}. It stays paused")
            }
            ApplyError::TruncateWithoutLsn { src_table } => write!(
                f,
                "a truncate of '{src_table}' reached the drain with no ring lsn, so it can't \
                 raise its targets' truncate floor; the capture trigger always stamps one, so \
                 the ring row was written by something else"
            ),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApplyError::Staging(err) => Some(err),
            ApplyError::Catalog(err) => Some(err),
            ApplyError::Ddl(err) => Some(err),
            ApplyError::Validate(err) => Some(err),
            ApplyError::Backfill(err) => Some(err),
            ApplyError::Db(err) => Some(err),
            ApplyError::Pool(err) => Some(err),
            ApplyError::Intake(err) => Some(err),
            ApplyError::ColumnPauseLockTimeout(err) => Some(err),
            ApplyError::ResumeRefused { reason, .. } => Some(reason.as_ref()),
            ApplyError::ClaimLost
            | ApplyError::PageAlreadyCommitted
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::LedgerEntryCollected { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::GroupBasisUnsupported { .. }
            | ApplyError::GroupLockGap { .. }
            | ApplyError::SourceTableDropped { .. }
            | ApplyError::ColumnNotPaused { .. }
            | ApplyError::DefinitionNotLive { .. }
            | ApplyError::ColumnAwaitingCapture { .. }
            | ApplyError::TransformNotFound { .. }
            | ApplyError::TransformNotPaused { .. }
            | ApplyError::KeyNotHeld { .. }
            | ApplyError::ReleaseLockTimeout { .. }
            | ApplyError::TruncateWithoutLsn { .. } => None,
        }
    }
}

impl From<StagingError> for ApplyError {
    fn from(err: StagingError) -> Self {
        ApplyError::Staging(err)
    }
}

impl From<CatalogError> for ApplyError {
    fn from(err: CatalogError) -> Self {
        ApplyError::Catalog(err)
    }
}

impl From<DdlError> for ApplyError {
    fn from(err: DdlError) -> Self {
        ApplyError::Ddl(err)
    }
}

impl From<ValidationError> for ApplyError {
    fn from(err: ValidationError) -> Self {
        ApplyError::Validate(err)
    }
}

impl From<crate::defs::backfill::BackfillError> for ApplyError {
    fn from(err: crate::defs::backfill::BackfillError) -> Self {
        ApplyError::Backfill(err)
    }
}

impl From<crate::intake::IntakeError> for ApplyError {
    fn from(err: crate::intake::IntakeError) -> Self {
        ApplyError::Intake(err)
    }
}

impl From<crate::locks::ColumnPauseLockError> for ApplyError {
    fn from(err: crate::locks::ColumnPauseLockError) -> Self {
        match err {
            crate::locks::ColumnPauseLockError::Timeout(err) => {
                ApplyError::ColumnPauseLockTimeout(err)
            }
            crate::locks::ColumnPauseLockError::Db(err) => ApplyError::Db(err),
        }
    }
}

impl From<tokio_postgres::Error> for ApplyError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApplyError::Db(err)
    }
}

impl From<crate::error::Error> for ApplyError {
    fn from(err: crate::error::Error) -> Self {
        ApplyError::Pool(err)
    }
}

// ---------------------------------------------------------------------
// src_table qualification
// ---------------------------------------------------------------------

/// Resolves `src_table` to the fully-qualified identity
/// [`catalog::transforms_for_source`]/[`catalog::dependents_of`] now require
/// (ADR-0011: `schema_nodes` keys on qualified identity, so a
/// bare lookup there silently finds nothing rather than erroring).
///
/// A no-op for the common case — `src_table` already contains a `.` — which
/// covers every real CDC-staged or backfill-enumerated change (the producer
/// qualifies `change.src_table` unconditionally at the point it's staged).
/// The one bare shape that still reaches it is a chained definition's own
/// target, bare as [`crate::defs::ddl::neighbor_table_name`] always returns
/// it — the bookkeeping key `compute`'s downstream-reader lookup
/// (terminal-target latency metrics) asks about. That resolves through
/// [`catalog::resolve_graph_identity`]'s bare-target-suffix fallback (for a
/// target that isn't on the `search_path`): the name can only be some other
/// live definition's own target.
///
/// This is the canonicalization applied *before* staging: its output is what
/// those rows carry into the ring in the first place, so a bare `src_table`
/// reaching `compute` is not something this module itself produces. The
/// reading side still tolerates a bare name anyway: ring rows are durable,
/// and this crate's integration-test fixtures stage bare names by hand.
async fn qualified_schema_node_key(pool: &Pool, src_table: &str) -> Result<String, ApplyError> {
    if src_table.contains('.') {
        return Ok(src_table.to_string());
    }
    Ok(catalog::resolve_graph_identity(pool, src_table).await?)
}

/// The row `change` is written from, as a [`TargetWrite::image`]: an
/// Apply's new image, or a Re-derive's current source row, read live.
/// `None` for a delete, or a key whose row is gone. For the per-field probe
/// of a record that fails alone (`quarantine`'s column attribution).
pub(super) async fn record_row_image(
    pool: &Pool,
    change: &FoldedChange,
) -> Result<Option<String>, ApplyError> {
    if change.last_change.is_some() && !change.has_recompute {
        return Ok(change.new_image.clone());
    }
    let pk = ddl::source_primary_key(pool, &change.src_table).await?;
    let client = pool.get().await?;
    let columns = live_row_columns(&**client, &change.src_table).await?;
    let query = live_rows_query(&change.src_table, &pk, &columns, &[change.key.as_str()])?;
    let rows = client
        .query(
            &format!("select m.doc::text from ({}) m", query.docs),
            &query.params(),
        )
        .await?;
    Ok(rows.first().map(|row| row.get(0)))
}

/// The live read of a batch of keys' source rows and the arrays it binds:
/// one [`LiveRowsArm`] per pattern of `NULL` key columns among the batch's
/// keys.
pub(super) struct LiveRowsQuery<'a> {
    /// The read split by `jsonb_each_text` into `(k, column, value)` rows,
    /// for the tests of the read's plan.
    #[cfg(test)]
    sql: String,
    /// One `(k, doc)` row per matched source row, `doc` its `row_columns`
    /// as a text-valued `jsonb` (the Re-derive read,
    /// `super::one_to_one_ledger::read_rows`).
    pub(super) docs: String,
    arms: Vec<LiveRowsArm<'a>>,
}

/// The keys of one [`live_rows_query`] whose `pk` is `NULL` in
/// exactly the same columns, bound as one `union all` arm.
struct LiveRowsArm<'a> {
    /// One array per `pk` column that is not `NULL` in this pattern, in
    /// column order. A `NULL` column binds nothing: the arm matches it with
    /// `is null`.
    parts: Vec<Vec<Cow<'a, str>>>,
}

impl LiveRowsQuery<'_> {
    /// The bind parameters, in the order `sql` (and `docs`) number them.
    pub(super) fn params(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.arms
            .iter()
            .flat_map(|arm| arm.parts.iter().map(|part| part as &(dyn ToSql + Sync)))
            .collect()
    }
}

/// Builds the live read of a batch of keys: one `union all` arm per
/// pattern of `NULL` key columns present among `keys`, usually
/// just one. Each arm joins its keys' non-`NULL` parts as a keyset relation
/// (one array per such column, so the bind count doesn't grow with the
/// batch) and matches with [`live_rows_join_cond`], so every arm probes an
/// index on `pk`. Each matched row is returned as its own recomputed key
/// text plus one `(column, value)` pair per column, via a `cross join
/// lateral jsonb_each_text` (so decoding costs no extra round trip).
///
/// Matching a whole batch with `is not distinct from` on any column one of
/// its keys binds a `NULL` for (the naive shape) is correct but not
/// indexable: one `NULL`-keyed group in a batch turned the refetch into a
/// nested loop over a sequential scan of the source. This is the same split
/// `target_mutations::read_new_images` makes.
///
/// Every caller runs the statement under `super::ledger::ENTRY_PLAN_SETTINGS`
/// (no sequential scan), and a single-column key's non-`NULL` arm also
/// restricts the column to its array (`t.<col> = any(<array>)`), which the
/// join already implies, so the source's side of whatever join the planner
/// picks is read through the key's index and bounded by the batch.
/// It takes both while the source's statistics lag its size (analyzed while
/// small, grown since), for a page's 5,000 keys:
///
/// - Neither: a hash join over a sequential scan of the source, 100 ms at
///   1.15M rows on PostgreSQL 16 and 17.
/// - The bound alone: still a sequential scan on PostgreSQL 16, which prices
///   an index scan for 5,000 values far above 17's estimate: 28 ms at 1.15M rows,
///   and 41 ms at 2M, where the unbounded join had gone back to probing the
///   index (23 ms). PostgreSQL 17 reads the index: 10–15 ms.
/// - No sequential scan alone: a scan of the key's whole index, 31 ms.
/// - Both: 15–19 ms on PostgreSQL 16, 10–15 ms on 17.
///
/// A composite key is never restricted this way ([`bounds_keyset_by_array`]):
/// one `= any` per column took 1,474 ms against 13 ms for 5,000 keys of a
/// four-column key at 1M rows with fresh statistics, and 946 ms on a
/// two-column key at 20M rows. Without a sequential scan to hash, it probes
/// the key's index once per key whether or not its statistics lag (18 ms on
/// PostgreSQL 16 for 5,000 keys at 400k rows analyzed at 100, against 70 ms
/// for the hash join over a sequential scan).
pub(super) fn live_rows_query<'a>(
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    row_columns: &[String],
    keys: &[&'a str],
) -> Result<LiveRowsQuery<'a>, ApplyError> {
    // `pattern[i]`: key column `i` is NULL.
    let mut arms: BTreeMap<Vec<bool>, LiveRowsArm<'a>> = BTreeMap::new();
    for &key in keys {
        let decoded = ddl::split_pk_key(pk, source_table, key)?;
        let pattern: Vec<bool> = decoded.iter().map(Option::is_none).collect();
        let arm = arms
            .entry(pattern)
            .or_insert_with_key(|pattern| LiveRowsArm {
                parts: vec![Vec::new(); pattern.iter().filter(|null| !**null).count()],
            });
        for (column, part) in arm.parts.iter_mut().zip(decoded.into_iter().flatten()) {
            column.push(part);
        }
    }

    let k_expr = ddl::pk_key_sql_expr(pk, Some("t"));
    // An explicit per-column `jsonb_build_object`, not
    // `to_jsonb(t.*)` — see `row_as_text_jsonb_sql`'s doc comment for why.
    let doc_expr = row_as_text_jsonb_sql("t", row_columns);
    let source_ident = ddl::qualified_source_table(source_table);
    let mut next_param = 0;
    let selects: Vec<String> = arms
        .keys()
        .map(|pattern| {
            let mut arrays = Vec::new();
            let mut u_cols = Vec::new();
            let mut bounds = Vec::new();
            for (i, (column, &null)) in pk.iter().zip(pattern).enumerate() {
                if !null {
                    next_param += 1;
                    let array = format!("${next_param}::text[]::{}[]", column.data_type);
                    if bounds_keyset_by_array(pk) {
                        bounds.push(format!("t.{} = any({array})", quote_ident(&column.name)));
                    }
                    arrays.push(array);
                    u_cols.push(pk_keyset_col(i));
                }
            }
            // An all-`NULL` pattern binds no array at all: its one possible
            // row is found by `is null` alone.
            let keyset = if arrays.is_empty() {
                String::new()
            } else {
                format!(
                    " join unnest({}) as u({}) on true",
                    arrays.join(", "),
                    u_cols.join(", ")
                )
            };
            let mut cond = vec![live_rows_join_cond(pk, pattern)];
            cond.extend(bounds);
            format!(
                "select {k_expr} as k, {doc_expr} as doc from {source_ident} t{keyset} \
                 where {}",
                cond.join(" and "),
            )
        })
        .collect();
    let docs = selects.join(" union all ");
    Ok(LiveRowsQuery {
        #[cfg(test)]
        sql: format!(
            "select m.k, e.key, e.value from ({docs}) m \
             cross join lateral jsonb_each_text(m.doc) e"
        ),
        docs,
        arms: arms.into_values().collect(),
    })
}

/// The match for one [`live_rows_query`] arm: a
/// conjunction over `pk` where column `i` is `t.<col> is null` if
/// `pattern[i]` (every key in this arm is `NULL` there), else `t.<col> =
/// u.c<i>`. Never `is not distinct from`: that is not indexable,
/// and within one arm it is never needed, since a column is either
/// `NULL` for every key or for none.
fn live_rows_join_cond(pk: &[PrimaryKeyColumn], pattern: &[bool]) -> String {
    pk.iter()
        .zip(pattern)
        .enumerate()
        .map(|(i, (column, &null))| {
            let col = quote_ident(&column.name);
            if null {
                format!("t.{col} is null")
            } else {
                format!("t.{col} = u.{}", pk_keyset_col(i))
            }
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// The live, `attnum`-ordered column names of `table`, from a
/// `to_regclass`-bound `pg_attribute` introspection. `table` is the unquoted
/// `schema.table` identity, quoted for the lookup by [`ddl::regclass_arg`].
/// An already-quoted name is quoted twice, finds nothing, and
/// yields an empty column list.
///
/// Every `to_jsonb(t.*)`-based row decode in this crate needs
/// this to build an explicit per-column `jsonb_build_object` (see
/// [`row_as_text_jsonb_sql`]) instead — `to_jsonb` renders a `timestamp`/
/// `timestamptz` column with its own ISO-8601 writer rather than calling the
/// column's real output function, so it disagrees with every other `<col>::
/// text` cast in Trellis specifically for those two types. That divergence
/// is invisible until the two renderings of one value are compared as raw
/// text — a join/`GROUP BY`/primary-key key, or a `MIN`/`MAX` fold that
/// returns one of its inputs verbatim — at which point it reads as *two*
/// distinct keys/values for one underlying row.
pub async fn live_row_columns(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<String>, ApplyError> {
    let rows = client
        .query(
            "select a.attname::text \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 \
               and not a.attisdropped \
             order by a.attnum",
            &[&ddl::regclass_arg(table)],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// `to_jsonb(<alias>.*)`'s replacement: an explicit
/// `jsonb_build_object('<col>', <alias>."<col>"::text, ...)` over `columns`,
/// so every value lands the same way an ordinary `<col>::text` cast would —
/// including `timestamp`/`timestamptz`, where `to_jsonb`'s own writer
/// disagrees with the type's real output function (a space where `::text`
/// renders one, `to_jsonb` renders a `T`). Downstream, every caller of this
/// SQL fragment still decodes the result via `jsonb_each_text`, whose key for
/// each pair is exactly the quoted literal given here — the *raw* column
/// name, matching the key `to_jsonb(t.*)` itself would have produced, so no
/// downstream field lookup needs to change.
///
/// `columns` is expected non-empty in practice (every table this crate reads
/// has a primary key, so [`live_row_columns`] never returns an empty list for
/// a real relation) — an empty slice still renders valid SQL
/// (`jsonb_build_object()`), just an empty object, rather than panicking.
pub fn row_as_text_jsonb_sql(alias: &str, columns: &[String]) -> String {
    let pairs: Vec<String> = columns
        .iter()
        .map(|col| format!("{}, {alias}.{}::text", quote_literal(col), quote_ident(col)))
        .collect();
    format!("jsonb_build_object({})", pairs.join(", "))
}

/// Where a propagated change traces back to: its source commit's
/// `src_changed` and `origin_lsn`, merged with
/// [`earliest_src_changed`] and [`earliest_origin`] respectively.
type Provenance = (Option<std::time::SystemTime>, Option<PgLsn>);

// ---------------------------------------------------------------------
// Phase 2: compute
// ---------------------------------------------------------------------

/// ADR-0009 decision 5: buffers one applied change's per-transform
/// hop latency/throughput observation, from data [`compute`]'s by-source
/// grouping already has in hand — no new I/O, no new join. `transform` is
/// the consuming definition's target table (this crate's one "transform
/// name," per `ApplyError::ColumnNotPaused`/`DefinitionNotLive`'s own
/// `transform` fields). `change`'s [`FoldedChange::src_changed`] is
/// `Some` for a change that traces back to a real source commit (the
/// histogram's eventual `.observe()` value is `now - src_changed`, `now`
/// sampled fresh at flush time — see [`flush_apply_metrics`]), `None` for a
/// bare recompute trigger with no origin timestamp to measure against —
/// such a change still counts toward throughput, just not latency.
/// Its [`FoldedChange::row_count`] is the number of staged ring rows the
/// change folded: `trellis_changes_applied_total` counts staged
/// rows, while both latency histograms still observe once per
/// folded change.
///
/// **Buffers, does not record**: `compute` (Phase 2) has no transaction
/// and no locks, and [`drain_once`]/[`drain_many`]'s "reload, recompute,
/// retry" loop calls it again, from scratch, on the very same `folded`
/// input, for a version-fence miss or a rolled-back Phase 3 failure —
/// [`classify_and_retry`]'s `VersionFenceMiss`/`Transient` classes both
/// return "retry unchanged." Recording straight into the global registry
/// here, as this function used to, meant a change that took N attempts to
/// actually land got counted into `trellis_changes_applied_total` and both
/// latency histograms N times instead of once, worst exactly under the
/// lock-contention/version-fence-race conditions where accurate throughput
/// numbers matter most. Buffering into `transform_observations` (one entry
/// per call, mirroring what used to be recorded immediately) and flushing
/// only once, after the winning attempt's transaction actually commits
/// (`flush_apply_metrics`, called from `drain_once`/`drain_many` right after
/// their own `txn.commit().await?`), fixes both the double-counting and the
/// secondary issue of latency being measured against a pre-commit
/// timestamp: `flush_apply_metrics` samples `SystemTime::now()` itself, at
/// commit time, rather than reusing whatever this function would have
/// sampled during planning.
///
/// Called once per applied change per consuming definition — both the 1-1
/// write/delete dispatch and the aggregate accumulate path below call this
/// at the point each of their per-change loops already visits every folded
/// change, so this reuses grouping/iteration `compute` performs regardless
/// of whether metrics are recorded, per the ADR's "effectively free"
/// framing.
///
/// Every `Some(src_changed)` this function sees is also buffered
/// into `end_to_end_origins`, keyed by `transform` — one origin timestamp
/// per applied change, same as the per-transform histogram observes. This
/// is *not* itself gated on terminal-ness: at the point every call site
/// below runs, `compute` hasn't yet determined which targets in this batch
/// are terminal (that's `compute`'s downstream-reader lookup, computed once,
/// after every source's changes have been evaluated — see the end of
/// [`compute`]). Buffering here and filtering to only the terminal targets'
/// entries there reuses that one dedup'd downstream-reader lookup instead of
/// adding a second one per change; the filtered result is itself stored on
/// [`ApplyPlan`] (not flushed) for the same retry-safety reason as
/// `transform_observations`.
fn buffer_transform_apply_metrics(
    transform: &str,
    change: &FoldedChange,
    end_to_end_origins: &mut HashMap<String, Vec<std::time::SystemTime>>,
    transform_observations: &mut Vec<TransformObservation>,
) {
    if let Some(src_changed) = change.src_changed {
        end_to_end_origins
            .entry(transform.to_string())
            .or_default()
            .push(src_changed);
    }
    transform_observations.push(TransformObservation {
        transform: transform.to_string(),
        src_changed: change.src_changed,
        row_count: change.row_count,
    });
}

/// One applied folded change, as [`buffer_transform_apply_metrics`] buffers
/// it for [`flush_apply_metrics`]: `src_changed` feeds one per-transform
/// latency observation (when `Some`), and `row_count` is how much the change
/// adds to `trellis_changes_applied_total` (one per staged ring
/// row, not one per folded change).
#[derive(Debug, Clone)]
struct TransformObservation {
    transform: String,
    src_changed: Option<std::time::SystemTime>,
    row_count: u64,
}

/// One key's write into a target table: the source row it is written from,
/// as a text-valued jsonb object (a staged image, or the Re-derive read's
/// row), which [`apply_target`]'s upsert evaluates the fields over
/// ([`TargetPlan::fields`]), plus the `hop_gen` it carries forward if this
/// write propagates downstream.
///
/// `src_changed` is the triggering
/// [`FoldedChange::src_changed`], carried forward the same way `hop_gen` is
/// — so a downstream `Recompute` row this write's own propagation stages
/// (see [`apply_and_mark_drained_many`]'s step 4) keeps a real origin
/// instead of losing it at this hop.
#[derive(Debug, Clone)]
struct TargetWrite {
    pk_text: String,
    image: String,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    /// The triggering [`FoldedChange::origin_lsn`], carried
    /// forward exactly as `src_changed` is.
    origin_lsn: Option<PgLsn>,
}

/// One key's deletion from a target table. `src_changed` plays the same
/// forward-carrying role as [`TargetWrite::src_changed`].
#[derive(Debug, Clone)]
struct TargetDelete {
    pk_text: String,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
}

/// One folded change to a 1-1 target: an Apply of its last change, written
/// from its image, or a Re-derive (`apply` `None`), written from the row
/// Phase 3 reads under the entry lock (see `super::one_to_one_ledger`).
#[derive(Debug, Clone)]
struct OneToOneRecord {
    pk_text: String,
    apply: Option<OneToOneApply>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
}

/// A 1-1 Apply's `(lsn, txid)` and its new image, `None` for a delete.
type OneToOneApply = (PgLsn, String, Option<String>);

/// Everything Phase 3 needs to settle one 1-1 target: its primary key shape
/// (for the pre-lock/upsert/delete SQL), its fields rendered to SQL over the
/// source row, this page's records for it, and how to read a Re-derive's
/// row.
#[derive(Debug, Clone)]
struct TargetPlan {
    pk: Vec<PrimaryKeyColumn>,
    /// The fields Apply writes (every one but the paused), as their names
    /// and their SQL over the source row `s` ([`OneToOneRender`]).
    render: Arc<super::one_to_one_render::OneToOneRender>,
    records: Vec<OneToOneRecord>,
    /// The persisted, fully-qualified `"schema.table"` identity of this
    /// target (`Definition::target_table`, ADR-0011) —
    /// carried alongside the bare `def.def.target` this plan is keyed by
    /// (see [`ApplyPlan::targets`]'s doc comment on why the map key itself
    /// stays bare) so [`apply_target`] can bind the *right* physical table
    /// into its `INSERT`/`UPDATE`/`DELETE` SQL, rather than leaving a
    /// target explicitly qualified into a non-default schema to
    /// resolve against whatever `search_path` the executing session
    /// happens to carry.
    qualified_target: String,
    /// The ring's spelling of the source table, which the Re-derive read
    /// reads.
    qualified_source: String,
    /// Every column of the source, which the Re-derive read renders.
    row_columns: Vec<String>,
}

/// One target table this batch must clear in full before its own keyed
/// writes/deletes apply (a truncate on `src_table` clears every
/// row a 1-1 transform ever derived from it). `pk` is the target's primary
/// key shape — reused, per [`compute`]'s existing convention, from the
/// truncated source's own PK introspection (a 1-1 target's key column
/// mirrors its source's) — needed so Phase 3's `DELETE ... RETURNING` can
/// name the right column. `hop_gen` is the triggering truncate sentinel's
/// own `hop_gen`, carried forward so keys the clear physically removes
/// propagate downstream at `hop_gen + 1`, exactly like any other
/// physically-changed key.
///
/// `src_changed` is the triggering truncate sentinel's own `src_changed`,
/// fan-in tie-broken by `min` across
/// however many truncated sources resolve to this same target — see
/// [`earliest_src_changed`]'s doc comment for why `min`, not `max`, is the
/// right merge here.
#[derive(Debug, Clone)]
struct ClearPlan {
    pk: Vec<PrimaryKeyColumn>,
    hop_gen: i32,
    /// Same role as [`TargetPlan::qualified_target`]: the persisted,
    /// fully-qualified target identity this clear's `DELETE FROM` must bind,
    /// rather than the bare map key it's stored under.
    qualified_target: String,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
    /// The latest truncating commit's ring `lsn` this batch
    /// carries, which the clear raises the target's truncate floor to as it
    /// empties the ledger.
    truncate_lsn: PgLsn,
}

/// The aggregate-target counterpart to [`ClearPlan`] — see
/// [`ApplyPlan::aggregate_clears`]'s doc comment. Unlike [`ClearPlan`], its
/// `pk` is the target's own `GROUP BY` identity, not the truncated source's
/// key. `qualified_target` plays the
/// same role [`ClearPlan::qualified_target`]/[`TargetPlan::qualified_target`]
/// do: the persisted, fully-qualified identity the `DELETE FROM` must bind,
/// not the bare map key this is stored under.
#[derive(Debug, Clone)]
struct AggregateClearPlan {
    hop_gen: i32,
    qualified_target: String,
    /// The aggregate target's own row identity — its `GROUP BY` columns, as
    /// `ddl::identity_key_columns` reports them (ungated by key type)
    /// — so the clear can report each cleared group's key to the seam.
    pk: Vec<PrimaryKeyColumn>,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
    /// Where the target keeps its ordering state, whose clear also empties
    /// it and raises the truncate floor to `truncate_lsn`.
    state: AggregateState,
    /// The latest truncating commit's ring `lsn` this batch carries.
    truncate_lsn: PgLsn,
}

/// Where an aggregate target keeps its ordering state ([`AggregateClearPlan`]).
#[derive(Debug, Clone)]
enum AggregateState {
    /// The group basis's hidden table (`super::group_basis`), quoted
    /// and qualified.
    GroupBasis { hidden: String },
    /// Neither: nothing to empty.
    Neither,
}

/// The fan-in tie-break for [`StagedChange::Recompute::src_changed`]:
/// when more than one change in a batch
/// feeds the same propagated key (forward propagation's `changed` map), the
/// **earliest** (`min`) of their origins wins — the oldest/earliest
/// source-commit timestamp captures the slowest straggler in the group,
/// matching the p99/stall-visibility intent these latency histograms exist
/// for. This is deliberately the opposite of `hop_gen`'s own fan-in
/// tie-break (`max`, propagation depth: the deepest contributor sets the
/// bound) — same shape of merge, different direction, because the two
/// numbers answer different questions ("how stale is the staler input" vs.
/// "how deep is the deepest input").
///
/// `None` never wins over a real `Some`: an origin-less contributor (a
/// backfill-enumerated recompute, or any other change with no traceable
/// source commit) doesn't get to blank out a known origin its fan-in sibling
/// carried — it simply contributes nothing to the merge. Only when *every*
/// contributor is origin-less does the result stay `None`.
///
/// `pub(super)` so `quarantine` and `target_mutations` fan in the same way.
pub(super) fn earliest_src_changed(
    a: Option<std::time::SystemTime>,
    b: Option<std::time::SystemTime>,
) -> Option<std::time::SystemTime> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(t), None) | (None, Some(t)) => Some(t),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::ValueType;
    use std::time::{Duration, SystemTime};
    use tokio_postgres::NoTls;
    use tokio_postgres::types::PgLsn;

    #[test]
    fn earliest_src_changed_picks_the_lesser_of_two_known_origins() {
        let earlier = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let later = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        assert_eq!(
            earliest_src_changed(Some(later), Some(earlier)),
            Some(earlier),
            "the earlier of two known origins must win, regardless of argument order"
        );
        assert_eq!(
            earliest_src_changed(Some(earlier), Some(later)),
            Some(earlier)
        );
    }

    #[test]
    fn earliest_src_changed_never_lets_a_none_beat_a_known_origin() {
        let known = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        assert_eq!(
            earliest_src_changed(Some(known), None),
            Some(known),
            "an origin-less fan-in sibling must not blank out a known origin"
        );
        assert_eq!(earliest_src_changed(None, Some(known)), Some(known));
    }

    #[test]
    fn earliest_src_changed_of_two_unknowns_stays_unknown() {
        assert_eq!(earliest_src_changed(None, None), None);
    }

    /// The exact numeric value of a Prometheus exposition line whose metric
    /// name is `metric` (matched with a trailing `{` so `_count`/`_sum`/
    /// `_bucket`/plain-counter variants never collide with one another) and
    /// which carries a `transform="..."` label matching `transform` —
    /// `None` if no such line exists yet. Used below to assert an *exact*
    /// count (not just presence, which `metrics.rs`'s own tests already
    /// cover), since proving this module's fix means proving a count that
    /// could have been inflated by a retry is not.
    fn metric_value(rendered: &str, metric: &str, transform: &str) -> Option<u64> {
        rendered
            .lines()
            .find(|line| {
                line.starts_with(&format!("{metric}{{"))
                    && line.contains(&format!("transform=\"{transform}\""))
            })
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v as u64)
    }

    /// [`compute`] (Phase 2: no transaction, no locks) must not record
    /// straight into the global metrics registry: [`drain_once`]/
    /// [`drain_many`]'s "reload, recompute, retry" loop calls `compute`
    /// again, unchanged, on a version-fence miss or a rolled-back Phase 3
    /// failure, so a change that took more than one attempt to actually land
    /// would be double-(or worse-) counted into
    /// `trellis_changes_applied_total` and both latency histograms. So
    /// `compute` only *buffers* observations onto the
    /// [`ApplyPlan`] it returns (`transform_observations`/`end_to_end_origins`),
    /// and only [`flush_apply_metrics`] — called by `drain_once`/`drain_many`
    /// right after their own `txn.commit().await?` succeeds — actually
    /// records them.
    ///
    /// This proves both halves directly: `compute` run twice against the
    /// exact same folded input (standing in for `drain_once`'s retry loop
    /// without needing to force a real, racy version-fence/serialization
    /// failure) never touches the registry either time, and a single
    /// `flush_apply_metrics` call on the winning attempt's plan records
    /// exactly one observation per metric — not two, even though `compute`
    /// itself ran twice.
    ///
    /// The key is staged as three ring rows (an insert and two
    /// updates) that fold to one change. The counter reports the three
    /// staged rows, while each latency histogram's `_count` reports the one
    /// folded change.
    #[tokio::test]
    async fn compute_only_buffers_metrics_and_a_single_flush_records_them_exactly_once() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "set search_path to {}, public; {}",
                crate::config::DEFAULT_SCHEMA,
                crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS
            ))
            .await
            .expect("set search_path");

        // `testkit::TestDatabase::pool` is `trellis::pool::Pool` from
        // testkit's point of view — a *different* (if structurally
        // identical) type from this crate's own `crate::pool::Pool` when
        // this module is compiled as `trellis`'s own `--lib` test binary
        // (testkit depends on the published `trellis` crate, not on "this"
        // compilation of it). Every function this test calls below
        // (`compute`, `crate::defs::create_definition`, ...) takes this
        // crate's own `Pool`, so a fresh one is built here, straight from
        // the same DSN `testkit` already migrated — Postgres itself doesn't
        // care which Rust type did the connecting.
        let pool_config =
            crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = crate::pool::Pool::new(&pool_config).expect("build a same-crate pool");

        let source = "apply_rs_metrics_buffer_test_orders";
        let target = "apply_rs_metrics_buffer_test_totals";

        // `source` starts empty — its one row arrives below, after the
        // definition exists, purely as this test's own staged CDC event.
        // That keeps the definition's own initial backfill (which
        // enumerates whatever `source` holds at definition time) from
        // separately re-discovering and writing the same row, which would
        // otherwise land a second, backfill-driven observation alongside
        // this test's hand-staged one — mirroring `apply.rs`'s own
        // `drain_matches_the_oracle_across_an_insert_update_and_delete`
        // convention.
        client
            .batch_execute(&format!(
                "create table {source} (id integer primary key, price numeric, tax numeric)"
            ))
            .await
            .expect("seed source table");

        let source_columns: HashMap<String, ValueType> = [
            ("id".to_string(), ValueType::Numeric),
            ("price".to_string(), ValueType::Numeric),
            ("tax".to_string(), ValueType::Numeric),
        ]
        .into_iter()
        .collect();
        let definition = crate::defs::create_definition(
            &pool,
            &format!("TRANSFORM {target} FROM {source} SELECT price + tax AS total"),
            &source_columns,
        )
        .await
        .expect("create definition");
        let pk = crate::defs::source_primary_key(&pool, source)
            .await
            .expect("introspect source primary key");
        crate::defs::create_target_table(
            &pool,
            &definition.def,
            "public",
            &pk,
            &source_columns,
            source,
        )
        .await
        .expect("create target table");

        client
            .execute(
                &format!("insert into {source} (id, price, tax) values (1, 10.00, 1.50)"),
                &[],
            )
            .await
            .expect("seed source rows after the definition exists");

        // Stage three CDC rows for one key, each with a real
        // `src_changed`, so both the per-transform and end-to-end
        // histograms have something to observe, not just the throughput
        // counter — and so the fold has several rows to collapse.
        let image = r#"{"price":"10.00","tax":"1.50"}"#.to_string();
        // Ordered offsets above the real WAL insert position, as
        // three successive captured commits would carry.
        let base = u64::from(testkit::wal_insert_lsn(&client).await);
        for (op, lsn, old_image) in [
            ("insert", 1u64, None),
            ("update", 2, Some(image.clone())),
            ("update", 3, Some(image.clone())),
        ] {
            client
                .execute(
                    "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, hop_gen, \
                     src_changed) \
                     values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, now())",
                    &[
                        &source,
                        &"1",
                        &op,
                        &PgLsn::from(base + lsn),
                        &old_image,
                        &Some(image.clone()),
                    ],
                )
                .await
                .expect("stage cdc row");
        }

        let mut seal_client = client;
        let seal_outcome = crate::staging::seal::seal_phase1(&mut seal_client)
            .await
            .expect("seal phase 1");
        crate::staging::seal::seal_phase2(&seal_client, seal_outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        let seg_seq = seal_outcome.sealed_seg_seq;

        let mut phase1_client = pool.get().await.expect("connection");
        let txn = phase1_client.transaction().await.expect("begin phase 1");
        claim::claim(&txn, seg_seq, "worker", 1)
            .await
            .expect("claim");
        let share = claim::held_share(&*txn, seg_seq, "worker")
            .await
            .expect("held_share");
        let filter = share.filter(share.buckets());
        let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
        txn.commit().await.expect("commit phase 1");
        assert_eq!(folded.len(), 1, "three rows for one key fold to one change");
        assert_eq!(folded[0].row_count, 3);

        // Before any `compute` call: the registry must not already mention
        // this test's distinctively-named transform (guards against a
        // false pass if some later assertion's "still absent" check were
        // vacuously true for an unrelated reason).
        let before = crate::metrics::Metrics::new().render_prometheus();
        assert!(
            metric_value(&before, "trellis_changes_applied_total", target).is_none(),
            "transform must not already appear in the registry: {before}"
        );

        // First "attempt": builds a plan and buffers its observations —
        // must not touch the registry at all.
        let plan1 = compute(&pool, &folded).await.expect("compute (attempt 1)");
        assert_eq!(
            plan1.transform_observations.len(),
            1,
            "compute must buffer exactly one observation per folded change: {:?}",
            plan1.transform_observations
        );
        let observation = &plan1.transform_observations[0];
        assert_eq!(observation.transform, target);
        assert!(
            observation.src_changed.is_some(),
            "the staged change carried a real src_changed, so it must be buffered as Some"
        );
        assert_eq!(observation.row_count, 3);
        assert_eq!(
            plan1.end_to_end_origins.get(target).map(Vec::len),
            Some(1),
            "target has no downstream reader, so it is terminal and must buffer one end-to-end \
             origin"
        );
        let after_compute_1 = crate::metrics::Metrics::new().render_prometheus();
        assert!(
            metric_value(&after_compute_1, "trellis_changes_applied_total", target).is_none(),
            "compute (Phase 2, no transaction) must never record into the registry itself: \
             {after_compute_1}"
        );

        // Second "attempt": `drain_once`/`drain_many`'s retry loop calls
        // `compute` again, from scratch, against the exact same `folded`
        // input, on a version-fence miss or a rolled-back Phase 3 failure.
        // Simulated here directly (rather than forcing a real, racy
        // version-fence/serialization failure) — what matters is that
        // `compute` running twice must not, by itself, double anything.
        let plan2 = compute(&pool, &folded).await.expect("compute (attempt 2)");
        let after_compute_2 = crate::metrics::Metrics::new().render_prometheus();
        assert!(
            metric_value(&after_compute_2, "trellis_changes_applied_total", target).is_none(),
            "a second compute() call over the same input must still not record anything: \
             {after_compute_2}"
        );

        // Only the winning attempt's plan is ever flushed, exactly once —
        // mirroring `drain_once`/`drain_many` calling `flush_apply_metrics`
        // right after their own successful `txn.commit().await?`.
        flush_apply_metrics(&plan2);

        let after_flush = crate::metrics::Metrics::new().render_prometheus();
        assert_eq!(
            metric_value(&after_flush, "trellis_changes_applied_total", target),
            Some(3),
            "the counter must count the three staged rows exactly once, even though compute() \
             ran twice: {after_flush}"
        );
        assert_eq!(
            metric_value(
                &after_flush,
                "trellis_transform_latency_seconds_count",
                target
            ),
            Some(1),
            "exactly one per-transform latency observation must land — one per folded change, \
             not per staged row: {after_flush}"
        );
        assert_eq!(
            metric_value(
                &after_flush,
                "trellis_end_to_end_latency_seconds_count",
                target
            ),
            Some(1),
            "exactly one end-to-end latency observation must land (target is terminal): \
             {after_flush}"
        );
    }

    /// Regression pin: [`live_rows_join_cond`] matches a
    /// column that is `NULL` in the arm's pattern with `is null` (so a
    /// `NULL`-keyed group's live row is found instead of being mistaken for
    /// "already deleted") and every other column with the indexable `=` —
    /// never `is not distinct from`.
    #[test]
    fn live_rows_join_cond_matches_a_null_column_with_is_null_and_the_rest_with_eq() {
        let pk: Vec<PrimaryKeyColumn> = ["warehouse", "sku"]
            .into_iter()
            .map(|name| PrimaryKeyColumn {
                name: name.to_string(),
                data_type: "integer".to_string(),
                nullable: true,
                collation: None,
            })
            .collect();

        assert_eq!(
            live_rows_join_cond(&pk, &[false, false]),
            r#"t."warehouse" = u.c0 and t."sku" = u.c1"#,
            "no NULL in the pattern: both columns keep the indexable `=`"
        );
        assert_eq!(
            live_rows_join_cond(&pk, &[true, false]),
            r#"t."warehouse" is null and t."sku" = u.c1"#,
            "only the NULL column switches to `is null`, and binds no array"
        );
        assert_eq!(
            live_rows_join_cond(&pk, &[true, true]),
            r#"t."warehouse" is null and t."sku" is null"#
        );
    }

    /// A [`live_rows_query`] batch that includes
    /// `NULL`-keyed groups (an aggregate target read as a source) still
    /// probes the table's `UNIQUE NULLS NOT DISTINCT` index, at a
    /// single-column and a composite key, and still finds every key's row.
    /// Before the fix, one `NULL` key switched the whole batch's match on
    /// that column to `is not distinct from`, which can't use the index, so
    /// the plan was a nested loop over a sequential scan.
    #[tokio::test]
    async fn live_rows_query_probes_the_index_when_a_batch_binds_a_null() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table single (g int, total int, unique nulls not distinct (g)); \
                 insert into single select g, g from generate_series(1, 200000) g; \
                 insert into single values (null, 0); \
                 analyze single; \
                 create table composite (g int, h text, total int, \
                                         unique nulls not distinct (g, h)); \
                 insert into composite select g, 'k' || g, g from generate_series(1, 200000) g; \
                 insert into composite values (null, 'k1', 0), (5, null, 0), (null, null, 0); \
                 analyze composite;",
            )
            .await
            .expect("seed large aggregate-style sources");
        // Each table's 500-key batch: its NULL-bearing keys plus plain keys
        // spread across the table.
        let cases = [
            ("public.single", "t.g is null or t.g % 397 = 0", 2),
            (
                "public.composite",
                "t.g is null or t.h is null or t.g % 397 = 0",
                4,
            ),
        ];
        for (table, batch, patterns) in cases {
            let pk = ddl::identity_key_columns(&client, table)
                .await
                .expect("identity");
            let columns = live_row_columns(&client, table).await.expect("columns");
            let key_sql = ddl::pk_key_sql_expr(&pk, Some("t"));
            let owned: Vec<String> = client
                .query(
                    &format!(
                        "select {key_sql} from {table} t where {batch} \
                         order by t.g nulls first limit 500"
                    ),
                    &[],
                )
                .await
                .expect("keys")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            let keys: Vec<&str> = owned.iter().map(String::as_str).collect();
            assert_eq!(keys.len(), 500);
            let query = live_rows_query(table, &pk, &columns, &keys).expect("query");
            assert_eq!(
                query.arms.len(),
                patterns,
                "{table}: one arm per NULL pattern in the batch"
            );
            let plan: String = client
                .query(&format!("explain {}", query.sql), &query.params())
                .await
                .expect("explain")
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("Index") && !plan.contains("Seq Scan"),
                "{table}: every arm should probe the key index, got:\n{plan}"
            );
            let found: std::collections::HashSet<String> = client
                .query(&query.sql, &query.params())
                .await
                .expect("read")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(
                found,
                owned.iter().cloned().collect(),
                "{table}: every key, NULL-bearing or not, finds its own row"
            );
        }

        // The naive shape over a single-column batch with one NULL key can
        // only plan a sequential scan, so the difference above is real.
        let groups: Vec<Option<String>> = std::iter::once(None)
            .chain((1..500).map(|i| Some((i * 397).to_string())))
            .collect();
        let plan: String = client
            .query(
                "explain select t.total from single t \
                 join unnest($1::text[]::int[]) as u(c0) on t.g is not distinct from u.c0",
                &[&groups],
            )
            .await
            .expect("explain")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("Seq Scan"),
            "the naive shape should not be able to probe the index, got:\n{plan}"
        );
    }

    /// A [`live_rows_query`] batch, run as its callers run it
    /// (under `ENTRY_PLAN_SETTINGS`), reads only its keys' rows of a
    /// single-column key's source whose statistics lag its size: analyzed at
    /// 100 rows, then grown to 400k with autovacuum off. Left to the join
    /// alone, the planner hashed a 5,000-key batch against a sequential scan
    /// of the source (or, with sequential scans off, a full scan of its
    /// key's index). With the arm's `= any` restriction but sequential scans
    /// on, PostgreSQL 16 still scanned the source and filtered it (CI); 17
    /// read the index.
    ///
    /// A composite key is left unrestricted, and with fresh statistics it
    /// must not be matched by comparing every source row with every key:
    /// here a four-column key at 1M rows. One `= any` per column made the
    /// planner expect a row from the source and loop over every key for
    /// each bounded row (1,474 ms against 13 ms).
    ///
    /// Either way every key must find its own row.
    #[tokio::test]
    async fn live_rows_query_reads_only_the_batch_while_source_statistics_lag() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table single (id int primary key, total int) \
                     with (autovacuum_enabled = false); \
                 create table composite (g int, h text, i int, j text, total int, \
                                         primary key (g, h, i, j)) \
                     with (autovacuum_enabled = false); \
                 insert into single select i, i from generate_series(1, 100) i; \
                 analyze single; \
                 insert into single select i, i from generate_series(101, 400000) i; \
                 insert into composite select i, 'k' || i, i, 'j' || i, i \
                     from generate_series(1, 1000000) i; \
                 analyze composite; \
                 create table pair (g int, h text, total int, primary key (g, h)) \
                     with (autovacuum_enabled = false); \
                 insert into pair select i, 'k' || i, i from generate_series(1, 100) i; \
                 analyze pair; \
                 insert into pair select i, 'k' || i, i from generate_series(101, 1000000) i;",
            )
            .await
            .expect("seed the sources");
        // (source, whether its key is a single column: bounded, with lagging statistics)
        for (table, single) in [
            ("public.single", true),
            ("public.composite", false),
            ("public.pair", false),
        ] {
            let pk = ddl::identity_key_columns(&client, table)
                .await
                .expect("identity");
            let columns = live_row_columns(&client, table).await.expect("columns");
            let key_sql = ddl::pk_key_sql_expr(&pk, Some("t"));
            let owned: Vec<String> = client
                .query(
                    &format!("select {key_sql} from {table} t where t.total % 79 = 0 limit 5000"),
                    &[],
                )
                .await
                .expect("keys")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            let keys: Vec<&str> = owned.iter().map(String::as_str).collect();
            assert_eq!(keys.len(), 5000);
            let query = live_rows_query(table, &pk, &columns, &keys).expect("query");
            assert_eq!(
                query.sql.contains("= any("),
                single,
                "{table}: only a single-column key is bounded:\n{}",
                query.sql
            );
            let txn = client.transaction().await.expect("begin");
            let plan: String = crate::staging::ledger::query_by_entry_key(
                &txn,
                &format!("explain (analyze, timing off) {}", query.sql),
                &query.params(),
            )
            .await
            .expect("explain")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
            txn.commit().await.expect("commit");
            let source = &table["public.".len()..];
            let scans: Vec<&str> = plan
                .lines()
                .filter(|line| line.contains(&format!(" on {source} ")))
                .collect();
            assert!(
                !scans.is_empty(),
                "{table}: no scan of the source in:\n{plan}"
            );
            for scan in scans {
                let rows: f64 = scan
                    .split("rows=")
                    .nth(1)
                    .and_then(|rest| rest.split(' ').next())
                    .and_then(|rows| rows.parse().ok())
                    .expect("a row estimate");
                assert!(
                    !scan.contains("Seq Scan") && rows <= keys.len() as f64,
                    "{table}: the source must be read through the batch's keys, \
                     got:\n{plan}"
                );
            }
            let filtered: u64 = plan
                .lines()
                .filter_map(|line| line.split("Rows Removed by Join Filter: ").nth(1))
                .map(|n| n.trim().parse::<u64>().expect("a row count"))
                .sum();
            assert!(
                filtered < keys.len() as u64,
                "{table}: the source must be matched to the keys without comparing \
                 every row with every key, got:\n{plan}"
            );
            // What the scans actually read, not what the planner expected
            // them to.
            let read = testkit::plan::rows_read(&plan, source);
            assert!(
                read <= 2 * keys.len() as u64,
                "{table}: the source must be read for the batch's keys alone, \
                 {read} rows read, got:\n{plan}"
            );
            // The plan above is explained through `query_by_entry_key`; a 1-1
            // ledger page's Re-derive read must run the statement that way
            // too. Without the settings, a stale source is scanned.
            let txn = client.transaction().await.expect("begin");
            // It reads the newest segment from Trellis's own schema too.
            txn.batch_execute(&format!(
                "set local search_path to {}, public",
                crate::config::DEFAULT_SCHEMA
            ))
            .await
            .expect("set search_path");
            let seq_scans = "select seq_scan from pg_stat_xact_user_tables \
                             where relid = $1::text::regclass";
            let before: i64 = txn
                .query_one(seq_scans, &[&table])
                .await
                .expect("scans")
                .get(0);
            let rederived = crate::staging::one_to_one_ledger::read_rows(
                &txn, table, table, &pk, &columns, &keys,
            )
            .await
            .expect("re-derive read");
            let after: i64 = txn
                .query_one(seq_scans, &[&table])
                .await
                .expect("scans")
                .get(0);
            txn.rollback().await.expect("rollback");
            assert_eq!(
                after, before,
                "{table}: the Re-derive read must not scan the source, as its plan above doesn't"
            );
            assert_eq!(
                rederived
                    .rows
                    .into_keys()
                    .collect::<std::collections::HashSet<_>>(),
                owned.iter().cloned().collect(),
                "{table}: the Re-derive read finds every key's row"
            );
            let found: std::collections::HashSet<String> = client
                .query(&query.sql, &query.params())
                .await
                .expect("read")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(
                found,
                owned.iter().cloned().collect(),
                "{table}: every key finds its own row"
            );
        }
    }

    /// A composite-key 1-1 target's pre-lock
    /// ([`lock_composite_keys`]) is matched to its keys without comparing
    /// every target row with every key, and without scanning the target.
    ///
    /// - `fresh`: a four-column key at 1M rows with fresh statistics.
    ///   Bounding each key column by its own `= any` array made the planner
    ///   expect one row from the target and loop over all 5,000 keys for
    ///   each of the 5,000 rows it read: 25M comparisons, 2.3 s against
    ///   25 ms.
    /// - `stale`: a two-column key analyzed at 100 rows, then grown to 1M
    ///   with autovacuum off. With sequential scans on, PostgreSQL 16 and 17
    ///   both hashed the keys against a sequential scan of the target.
    ///
    /// The plan is explained under the settings the pre-lock runs with, and
    /// the pre-lock itself must start no sequential scan of the target. It
    /// must still lock every key.
    #[tokio::test]
    async fn the_composite_pre_lock_never_compares_every_row_with_every_key() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table fresh (a int, b text, c int, d text, total int, \
                                     primary key (a, b, c, d)) \
                     with (autovacuum_enabled = false); \
                 insert into fresh select i, 'k' || i, i, 'd' || i, i \
                     from generate_series(1, 1000000) i; \
                 analyze fresh; \
                 create table stale (a int, b text, total int, primary key (a, b)) \
                     with (autovacuum_enabled = false); \
                 insert into stale select i, 'k' || i, i from generate_series(1, 100) i; \
                 analyze stale; \
                 insert into stale select i, 'k' || i, i from generate_series(101, 1000000) i;",
            )
            .await
            .expect("seed the targets");
        for table in ["public.fresh", "public.stale"] {
            let pk = ddl::identity_key_columns(&client, table)
                .await
                .expect("identity");
            let key_sql = ddl::pk_key_sql_expr(&pk, Some("t"));
            let part_sql: Vec<String> = pk
                .iter()
                .map(|c| format!("t.{}::text", quote_ident(&c.name)))
                .collect();
            let rows: Vec<(String, Vec<String>)> = client
                .query(
                    &format!(
                        "select {key_sql}, {} from {table} t \
                         where t.total % 79 = 0 limit 5000",
                        part_sql.join(", ")
                    ),
                    &[],
                )
                .await
                .expect("keys")
                .into_iter()
                .map(|row| (row.get(0), (1..=pk.len()).map(|i| row.get(i)).collect()))
                .collect();
            assert_eq!(rows.len(), 5000);
            let parts: Vec<Vec<&str>> = rows
                .iter()
                .map(|(_, parts)| parts.iter().map(String::as_str).collect())
                .collect();
            let arrays = transpose_pk_parts(pk.len(), &parts);
            let params: Vec<&(dyn ToSql + Sync)> = arrays.iter().map(|a| a as _).collect();
            let sql = composite_lock_statement(table, &pk, &key_sql);
            let txn = client.transaction().await.expect("begin");
            let plan: String = crate::staging::ledger::query_by_entry_key(
                &txn,
                &format!("explain (analyze, timing off) {sql}"),
                &params,
            )
            .await
            .expect("explain")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
            let filtered: u64 = plan
                .lines()
                .filter_map(|line| line.split("Rows Removed by Join Filter: ").nth(1))
                .map(|n| n.trim().parse::<u64>().expect("a row count"))
                .sum();
            assert!(
                !plan.contains("Seq Scan") && filtered < rows.len() as u64,
                "{table}: the target must be matched to the keys without comparing \
                 every row with every key, got:\n{plan}"
            );
            // What the scans actually read, not what the planner expected
            // them to.
            let read = testkit::plan::rows_read(&plan, &table["public.".len()..]);
            assert!(
                read <= 2 * rows.len() as u64,
                "{table}: the target must be read for the keys alone, {read} rows \
                 read, got:\n{plan}"
            );
            let seq_scans = "select seq_scan from pg_stat_xact_user_tables \
                             where relid = $1::text::regclass";
            let before: i64 = txn
                .query_one(seq_scans, &[&table])
                .await
                .expect("scans")
                .get(0);
            let locked: std::collections::HashSet<String> =
                lock_composite_keys(&txn, table, &pk, &key_sql, &arrays)
                    .await
                    .expect("lock")
                    .into_iter()
                    .map(|row| row.get(0))
                    .collect();
            let after: i64 = txn
                .query_one(seq_scans, &[&table])
                .await
                .expect("scans")
                .get(0);
            assert_eq!(
                after, before,
                "{table}: the pre-lock must not scan the target, as its plan above doesn't"
            );
            txn.rollback().await.expect("rollback");
            assert_eq!(
                locked,
                rows.into_iter().map(|(key, _)| key).collect(),
                "{table}: every key is locked"
            );
        }
    }

    /// A single-column-key target's pre-lock
    /// ([`lock_single_keys`]) starts no sequential scan of the target while
    /// its statistics lag its size. PostgreSQL 16 priced the index scan for
    /// thousands of `= any` keys so high that it read a 400k-row target
    /// analyzed at 100 in full and filtered it by the keys.
    ///
    /// The plan is explained under the settings the pre-lock runs with, and
    /// the pre-lock itself must start no sequential scan of the target. It
    /// must still lock every key, in key order.
    #[tokio::test]
    async fn the_single_column_pre_lock_never_scans_a_stale_target() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table stale (id int primary key, total int) \
                     with (autovacuum_enabled = false); \
                 insert into stale select i, i from generate_series(1, 100) i; \
                 analyze stale; \
                 insert into stale select i, i from generate_series(101, 400000) i;",
            )
            .await
            .expect("seed the target");
        let table = "public.stale";
        let pk = ddl::identity_key_columns(&client, table)
            .await
            .expect("identity");
        assert_eq!(pk.len(), 1);
        let pk_ident = quote_ident(&pk[0].name);
        let pk_cast = pk[0].data_type.as_str();
        let key_sql = ddl::pk_key_sql_expr(&pk, Some("t"));
        let keys: Vec<String> = client
            .query(
                "select id::text from stale where total % 79 = 0 order by stale.id limit 5000",
                &[],
            )
            .await
            .expect("keys")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(keys.len(), 5000);
        let mut bound: Vec<&str> = keys.iter().map(String::as_str).collect();
        bound.reverse();
        let sql = single_lock_statement(table, &pk_ident, pk_cast, &key_sql);
        let txn = client.transaction().await.expect("begin");
        let plan: String = crate::staging::ledger::query_by_entry_key(
            &txn,
            &format!("explain (analyze, timing off) {sql}"),
            &[&bound],
        )
        .await
        .expect("explain")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            !plan.contains("Seq Scan"),
            "the pre-lock must not scan the target, got:\n{plan}"
        );
        let seq_scans = "select seq_scan from pg_stat_xact_user_tables \
                         where relid = $1::text::regclass";
        let before: i64 = txn
            .query_one(seq_scans, &[&table])
            .await
            .expect("scans")
            .get(0);
        let locked: Vec<String> =
            lock_single_keys(&txn, table, &pk_ident, pk_cast, &key_sql, &bound)
                .await
                .expect("lock")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
        let after: i64 = txn
            .query_one(seq_scans, &[&table])
            .await
            .expect("scans")
            .get(0);
        assert_eq!(
            after, before,
            "the pre-lock must not scan the target, as its plan above doesn't"
        );
        let setting: String = txn
            .query_one("select current_setting('enable_seqscan')", &[])
            .await
            .expect("setting")
            .get(0);
        assert_eq!(setting, "on", "the settings are put back afterwards");
        txn.rollback().await.expect("rollback");
        assert_eq!(locked, keys, "every key is locked, in key order");
    }

    /// The single-column key's delete
    /// ([`delete_single_keys`]) reads the same stale target by the same
    /// keys, so it starts no sequential scan of it either, and still deletes
    /// every key.
    #[tokio::test]
    async fn the_single_column_delete_never_scans_a_stale_target() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table stale (id int primary key, total int) \
                     with (autovacuum_enabled = false); \
                 insert into stale select i, i from generate_series(1, 100) i; \
                 analyze stale; \
                 insert into stale select i, i from generate_series(101, 400000) i;",
            )
            .await
            .expect("seed the target");
        let table = "public.stale";
        let pk = ddl::identity_key_columns(&client, table)
            .await
            .expect("identity");
        let pk_ident = quote_ident(&pk[0].name);
        let pk_cast = pk[0].data_type.as_str();
        let key_sql = ddl::pk_key_sql_expr(&pk, Some("t"));
        let keys: Vec<String> = client
            .query(
                "select id::text from stale where total % 79 = 0 order by stale.id limit 5000",
                &[],
            )
            .await
            .expect("keys")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(keys.len(), 5000);
        let bound: Vec<&str> = keys.iter().map(String::as_str).collect();
        let sql = single_delete_statement(table, &pk_ident, pk_cast, &key_sql);
        let txn = client.transaction().await.expect("begin");
        let plan: String =
            crate::staging::ledger::query_by_entry_key(&txn, &format!("explain {sql}"), &[&bound])
                .await
                .expect("explain")
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>()
                .join("\n");
        assert!(
            !plan.contains("Seq Scan"),
            "the delete must not scan the target, got:\n{plan}"
        );
        let seq_scans = "select seq_scan from pg_stat_xact_user_tables \
                         where relid = $1::text::regclass";
        let before: i64 = txn
            .query_one(seq_scans, &[&table])
            .await
            .expect("scans")
            .get(0);
        let deleted: std::collections::HashSet<String> =
            delete_single_keys(&txn, table, &pk_ident, pk_cast, &key_sql, &bound)
                .await
                .expect("delete")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
        let after: i64 = txn
            .query_one(seq_scans, &[&table])
            .await
            .expect("scans")
            .get(0);
        assert_eq!(
            after, before,
            "the delete must not scan the target, as its plan above doesn't"
        );
        txn.rollback().await.expect("rollback");
        assert_eq!(deleted, keys.into_iter().collect(), "every key is deleted");
    }

    /// `classify` decides by the innermost `ApplyError`,
    /// which for a lost claim is `Isolate`, so `classify_and_retry`'s lost
    /// claim early return has to see through a wrapper too. Otherwise a
    /// wrapped `ClaimLost` would be isolated, and every key in the page
    /// charged a death for a claim nobody's key lost. The pool is never
    /// reached: isolation's first step would be its start log line.
    #[tokio::test]
    async fn classify_and_retry_never_isolates_a_wrapped_lost_claim() {
        let (_guard, captured) = crate::client::log_capture::install_capture();
        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(
                "host=/nonexistent/trellis-issue-670 port=1 user=nobody dbname=nothing".to_string(),
            )
            .expect("valid dsn"),
        )
        .expect("a lazy pool");
        let wrapped = ApplyError::Backfill(crate::defs::backfill::BackfillError::Propagation(
            Box::new(ApplyError::ClaimLost),
        ));

        let result = classify_and_retry(
            &pool,
            &[SegmentStep {
                seg_seq: 7,
                page: None,
            }],
            "worker-a",
            "wake",
            &[],
            &mut 1,
            &mut FenceMissBackoff::new(),
            &mut TransientRetry::new(),
            &mut HaltRetry::default(),
            wrapped,
        )
        .await;
        let err = result.expect_err("a lost claim surfaces");
        assert!(quarantine::is_claim_lost(&err), "{err:?}");

        let events = captured.0.lock().unwrap().clone();
        assert!(
            !events.iter().any(|e| e
                .fields
                .get("message")
                .is_some_and(|m| m.contains("isolat"))),
            "a lost claim is never isolated: {events:?}"
        );
    }

    /// A page takes its version fences in table-name order, the
    /// order a define, drop, resume or freeze bumps several in, so a page and
    /// a bump of the same fences never deadlock. A page of a `JOIN` target
    /// holds both its tables' fences, and a `JOIN` define bumps both: a page
    /// that took the second table's fence first and then queued on the
    /// first, behind a define that held the first and wanted the second,
    /// closed a cycle (`40P01`, returned to the `DEFINE`).
    ///
    /// The interleaving is forced: a blocker holds the first fence, the
    /// define bumps every fence in name order and queues behind it, and the
    /// page queues behind the define. A page that took any fence before the
    /// first one holds it when the blocker lets the define go. The fences
    /// are many, and the scenario runs a few rounds, so a page taking them
    /// in any other order holds one in some round.
    #[tokio::test]
    async fn a_page_takes_its_fences_in_the_order_a_define_bumps_them() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let connect = async || {
            let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(&format!(
                    "set search_path to {}, public",
                    crate::config::DEFAULT_SCHEMA
                ))
                .await
                .expect("set search_path");
            let pid: i32 = client
                .query_one("select pg_backend_pid()", &[])
                .await
                .expect("read pid")
                .get(0);
            (client, pid)
        };
        let (raw, _) = connect().await;
        let tables: Vec<String> = (0..8).map(|i| format!("public.f{i}")).collect();
        let waiting_on_lock = async |pid: i32| {
            for _ in 0..500 {
                let waiting: bool = raw
                    .query_one(
                        "select coalesce((select wait_event_type = 'Lock' \
                         from pg_stat_activity where pid = $1), false)",
                        &[&pid],
                    )
                    .await
                    .expect("read pg_stat_activity")
                    .get(0);
                if waiting {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("session {pid} never queued on a lock");
        };

        for round in 1..=3_i64 {
            for table in &tables {
                raw.execute(
                    "insert into source_table_versions (source_table, version) \
                     values ($1, $2) on conflict (source_table) do update set version = $2",
                    &[table, &round],
                )
                .await
                .expect("seed the fences");
            }

            let (mut blocker, _) = connect().await;
            let blocking = blocker.transaction().await.expect("begin");
            blocking
                .execute(
                    "select 1 from source_table_versions where source_table = $1 for update",
                    &[&tables[0]],
                )
                .await
                .expect("hold the first fence");

            let (mut definer, define_pid) = connect().await;
            let defining = tokio::spawn({
                let tables = tables.clone();
                async move {
                    let txn = definer.transaction().await.expect("begin");
                    for table in &tables {
                        super::super::build::bump_version_fence(&txn, table).await?;
                    }
                    txn.commit().await
                }
            });
            waiting_on_lock(define_pid).await;

            let (mut pager, page_pid) = connect().await;
            let paging = tokio::spawn({
                let versions: BTreeMap<String, Option<i64>> =
                    tables.iter().map(|t| (t.clone(), Some(round))).collect();
                async move {
                    let txn = pager.transaction().await.expect("begin");
                    hold_version_fences(&txn, &versions).await
                }
            });
            waiting_on_lock(page_pid).await;

            blocking.commit().await.expect("let the define go");
            defining
                .await
                .expect("join")
                .unwrap_or_else(|err| panic!("round {round}: the define failed: {err:?}"));
            let paged = paging.await.expect("join");
            assert!(
                matches!(paged, Err(ApplyError::VersionFenceMiss { .. })),
                "round {round}: the page waits out the define and misses its fence, got {paged:?}"
            );
        }
    }
}

/// Phase 2's output: an in-memory plan Phase 3 applies inside one
/// transaction, with no further catalog reads of its own beyond the version
/// fence and the target-mutation seam's reader lookup.
///
/// `versions` is the version-fence's read set: every source table this
/// batch evaluated against, and the `source_table_versions.version` Phase 2
/// loaded for it (`None` for a source with no definitions at all — still
/// fenced, so a definition created against it mid-drain is caught too),
/// ordered by table name, the order Phase 3 takes the fences in.
/// Which targets have downstream readers is *not* decided here: Phase 3's
/// target-mutation seam (`staging::target_mutations`) resolves that inside
/// its own transaction, for every target any step wrote.
#[derive(Debug, Clone, Default)]
pub struct ApplyPlan {
    versions: BTreeMap<String, Option<i64>>,
    targets: BTreeMap<String, TargetPlan>,
    /// The aggregate targets on the group basis (`super::group_basis`),
    /// each with this page's ring rows for it, keyed by the definition's
    /// bare target name, in target order.
    group_targets: BTreeMap<String, super::group_basis::GroupTargetPlan>,
    /// The `JOIN` targets (`super::join`),
    /// each with this page's ring rows of either table, keyed by the
    /// definition's bare target name, in target order.
    join_targets: BTreeMap<String, super::join::JoinTargetPlan>,
    /// Targets to clear in full at Phase 3, keyed by target table name —
    /// the truncate propagation. See [`ClearPlan`].
    clears: HashMap<String, ClearPlan>,
    /// The aggregate-target counterpart to [`ApplyPlan::clears`]: a
    /// truncate on an aggregate definition's source clears every group.
    /// Applied by the same `clear_target` as a 1-1 clear, keyed by the
    /// target's `GROUP BY` columns, so each cleared group reaches the seam
    /// and a transform chained off the aggregate target sees it.
    aggregate_clears: HashMap<String, AggregateClearPlan>,
    /// The (non-truncate) folded records whose `(src_table,
    /// key)` is poisoned for a definition that reads it, each paired with
    /// that definition's id — left out of that definition's plan only
    /// (every other reader applies it), and instead parked into
    /// `poison_held` for it by [`apply_and_mark_drained`], in the same
    /// Phase-3 transaction, before the drained mark. See
    /// `quarantine::park_batch_contribution`'s doc comment for why this must
    /// happen even when this batch didn't cause the key's eviction.
    poisoned_park: Vec<(i64, FoldedChange)>,
    /// Every non-truncate `(src_table, key)` this batch computed
    /// against for at least one definition — cleared from `key_deaths` by
    /// [`apply_and_mark_drained`] on a successful commit, per doc 06's "a
    /// clean drain clears the counters for the keys it just applied", for
    /// every definition the key isn't poisoned for.
    applied_keys: Vec<(String, String)>,
    /// How many of those keys were an automatic retry of a held key,
    /// set when the page clears their death counters
    /// ([`quarantine::clear_key_deaths`]) and counted by
    /// [`flush_apply_metrics`] once the page commits. Shared, so a clone of
    /// the plan sees the same count.
    retries_succeeded: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Every
    /// [`TransformObservation`] [`buffer_transform_apply_metrics`]
    /// buffered during this `compute` call, in place of recording each one
    /// immediately — drained by [`flush_apply_metrics`] into
    /// [`crate::metrics::record_transform_latency`]/
    /// [`crate::metrics::increment_changes_applied`] only once the batch
    /// this plan belongs to actually commits, so a plan a retry discards
    /// (version-fence miss, rolled-back Phase 3 failure) never reaches the
    /// registry at all. See [`buffer_transform_apply_metrics`]'s doc comment
    /// for why eager recording here was the bug.
    transform_observations: Vec<TransformObservation>,
    /// The terminal-transform-only counterpart to `transform_observations`,
    /// above: `end_to_end_origins` (the accumulator `compute` builds while
    /// evaluating every source) filtered down, once `downstream_readers` is
    /// known, to only the targets with no downstream reader of their own —
    /// mirroring exactly what `compute` used to flush directly into
    /// [`crate::metrics::record_end_to_end_latency`] at the end of its
    /// per-target loop. Buffered for the same retry-safety reason.
    end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>>,
}

/// The primary key [`compute`] keys `qualified_source`'s changes by
/// ([`ddl::source_primary_key`]), or `None` when the key can't be used and
/// no definition that isn't frozen reads the table, so `compute` skips them.
///
/// A dropped source is [`ApplyError::SourceTableDropped`], reported under
/// `qualified_source`, the ring's own spelling: its consumers
/// (`quarantine::purge_dropped_table` and `drain_once`/`drain_many`'s
/// `folded.retain`) match it against a ring row's `src_table` as an exact
/// string.
///
/// A key that no longer passes the key gate (a type off the
/// allowlist, or no key at all) halts the page while any definition that
/// isn't frozen reads the table ([`catalog::has_unfrozen_reader`], under the
/// canonical `source_key`): one that applies would key its rows wrongly or
/// not at all. A table whose readers are all frozen (paused, capture-failed
/// or quarantined) is skipped instead. Its changes drain with the page,
/// which marks its claim drained whatever the plan holds, exactly as a
/// paused definition's share is dropped when `catalog::transforms_for_source`
/// leaves it out, and a resume rebuilds from the source. Asked only on the
/// error, so a drain over usable keys reads nothing more. The answer is
/// fenced: the caller holds `source_key`'s version, and a definition, its
/// resume and its edit each bump it, so a page can't commit a skip after a
/// reader it didn't see.
///
/// The halt pauses every such reader and what is downstream of it
/// (`super::halt`, which pauses the reader in the
/// drain), so the page's retry finds none and skips the table.
async fn source_key_for_apply(
    pool: &Pool,
    qualified_source: &str,
    source_key: &str,
) -> Result<Option<Vec<PrimaryKeyColumn>>, ApplyError> {
    let err = match ddl::source_primary_key(pool, qualified_source).await {
        Ok(pk) => return Ok(Some(pk)),
        Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
            return Err(ApplyError::SourceTableDropped {
                source_table: qualified_source.to_string(),
            });
        }
        Err(DdlError::NoPrimaryKey { source_table })
            if quarantine::source_table_missing(pool, &source_table).await? =>
        {
            return Err(ApplyError::SourceTableDropped { source_table });
        }
        Err(err) => err,
    };
    if !is_key_gate(&err) {
        return Err(err.into());
    }
    if has_unfrozen_reader(pool, source_key).await? {
        return Err(err.into());
    }
    crate::instance_log::warn!(
        src_table = %qualified_source,
        error = %err,
        "every definition reading this table is frozen and its key can't be used; \
         skipping its changes"
    );
    Ok(None)
}

/// Whether `err`, from [`ddl::source_primary_key`], is its key gate: the
/// table has no usable key (none, or one of a type off the allowlist).
fn is_key_gate(err: &DdlError) -> bool {
    matches!(
        err,
        DdlError::NoPrimaryKey { .. } | DdlError::UnsupportedPrimaryKeyType { .. }
    )
}

/// Whether some definition that isn't frozen reads `table`
/// ([`catalog::has_unfrozen_reader`]), on a pooled connection. A page skips
/// the table's changes when none does ([`source_key_for_apply`],
/// [`compute_page`]), fenced by the caller's hold of `table`'s version.
async fn has_unfrozen_reader(pool: &Pool, table: &str) -> Result<bool, ApplyError> {
    let client = pool.get().await?;
    Ok(catalog::has_unfrozen_reader(&**client, table).await?)
}

/// Each of `changes`' [`KeyExclusion`], or `None` when every one is
/// empty, the common case, which costs no allocation: `changes` are the folded
/// changes to `source_key` (canonical), `defs` the definitions that apply
/// it, and `poisoned` the batch's poison ([`quarantine::poisoned_keys_among`]).
/// `focus` overrides one key's readers for an isolation probe.
fn key_exclusions(
    source_key: &str,
    changes: &[&FoldedChange],
    defs: &[crate::defs::model::Definition],
    poisoned: &HashMap<(String, String), HashSet<i64>>,
    focus: Option<&ProbeFocus<'_>>,
) -> Option<Vec<KeyExclusion>> {
    let lookup = |key: &str| poisoned.get(&(source_key.to_string(), key.to_string()));
    let any_poisoned = !poisoned.is_empty() && changes.iter().any(|c| lookup(&c.key).is_some());
    if !any_poisoned && focus.is_none() {
        return None;
    }
    let exclusions: Vec<KeyExclusion> = changes
        .iter()
        .map(|change| {
            let poisoned_for = lookup(&change.key);
            let mut exclusion = KeyExclusion::default();
            for def in defs {
                let excluded = focus
                    .and_then(|focus| focus.excludes(source_key, &change.key, def.id))
                    .unwrap_or_else(|| poisoned_for.is_some_and(|ids| ids.contains(&def.id)));
                if excluded {
                    exclusion.defs.insert(def.id);
                }
            }
            exclusion
        })
        .collect();
    (!exclusions.iter().all(KeyExclusion::is_empty)).then_some(exclusions)
}

/// One definition's share of a source's `changes`: every change but those
/// `exclusions` leave out of
/// `transform_id`. Borrowed when it leaves out none, the common case.
fn definition_share<'c, 'f>(
    transform_id: i64,
    changes: &'c [&'f FoldedChange],
    exclusions: &Option<Vec<KeyExclusion>>,
) -> Cow<'c, [&'f FoldedChange]> {
    let Some(exclusions) = exclusions
        .as_ref()
        .filter(|exclusions| exclusions.iter().any(|e| e.defs.contains(&transform_id)))
    else {
        return Cow::Borrowed(changes);
    };
    Cow::Owned(
        (0..changes.len())
            .filter(|&i| !exclusions[i].defs.contains(&transform_id))
            .map(|i| changes[i])
            .collect(),
    )
}

/// Phase 2 (design doc: "no transaction, no locks"): builds the write of
/// every folded change against the transform currently reading its
/// source table (the fields are evaluated by Phase 3's upsert), grouped by (unqualified) `src_table` so each source's
/// catalog version is loaded — and fenced against — exactly once.
///
/// Reloads the catalog fresh on every call, including retries: this is
/// what makes [`drain_once`]'s retry-on-fence-miss loop "reload, recompute"
/// rather than needing any separate invalidation path.
///
/// ADR-0009 decision 3: this span is the "hop" half of the
/// source-commit → hop → hop → apply tree — one span per compute pass over
/// a folded batch, with a per-source-table `tracing::debug!` event inside
/// the loop below (not a nested span: the loop body's accumulators
/// (`targets`, `versions`, `end_to_end_origins`, ...) are threaded through
/// by mutable reference across many `.await` points, and a held span guard
/// across those would make this function's future non-`Send` for no benefit
/// — an event carries the same `src_table`/`changes` information without
/// that cost). [`apply_target`] (Phase 3) is this tree's next, more
/// fine-grained span, one per consuming transform. The span is
/// [`compute_page`]'s, which this calls and [`drain_batch`] calls directly.
#[cfg(any(test, feature = "internals"))]
pub async fn compute(pool: &Pool, folded: &[FoldedChange]) -> Result<ApplyPlan, ApplyError> {
    compute_page(pool, folded, None, false).await
}

/// One key an isolation probe (`quarantine::attribute`) applies with
/// some of its work left out: the probe that tells which definition's apply
/// a key fails in. `direct` picks the readers of its table that apply it.
/// The key's poison stands as it is wherever the focus doesn't override it.
pub(super) struct ProbeFocus<'a> {
    /// Canonical, as `poison` keys it.
    pub src_table: &'a str,
    pub key: &'a str,
    pub direct: DirectFocus,
}

/// Which readers of a [`ProbeFocus`]'s table apply its key.
#[derive(Clone, Copy, Debug)]
pub(super) enum DirectFocus {
    /// That one alone.
    Only(i64),
    /// Every one the key isn't poisoned for, but that one (the
    /// leave-one-out).
    AllBut(i64),
}

impl ProbeFocus<'_> {
    fn names(&self, src_table: &str, key: &str) -> bool {
        self.src_table == src_table && self.key == key
    }

    /// Whether this focus leaves `key` of `src_table` out of `transform_id`.
    /// `None` where the focus leaves it to the key's poison: a key it
    /// doesn't name, or a reader [`DirectFocus::AllBut`] doesn't name.
    fn excludes(&self, src_table: &str, key: &str, transform_id: i64) -> Option<bool> {
        if !self.names(src_table, key) {
            return None;
        }
        match self.direct {
            DirectFocus::Only(id) => Some(id != transform_id),
            DirectFocus::AllBut(id) => (id == transform_id).then_some(true),
        }
    }
}

/// Which readers a folded change's key is left out of: the readers
/// of its table it's poisoned for, in id order, which it is parked for.
#[derive(Default)]
struct KeyExclusion {
    /// Readers of the change's own table (`defs` ids) that skip it.
    defs: BTreeSet<i64>,
}

impl KeyExclusion {
    /// Whether it leaves the change out of nothing.
    fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }
}

/// [`compute`], with `focus` overriding one key's direct readers for an
/// isolation probe, and with `skip_frozen` also skipping every
/// table no definition that isn't frozen reads, as [`source_key_for_apply`]
/// skips one whose key can't be used ([`has_unfrozen_reader`], fenced the
/// same way). [`drain_batch`] asks for it on the retry after Postgres
/// refused the drain a read or write (`42501`, see [`HaltRetry`]), and so do
/// that call's isolation probes: the halt has paused the definitions reading
/// the refused table, so the page must not read it again. Asked only then,
/// so a drain that was refused nothing reads nothing more.
#[tracing::instrument(
    name = "staging.compute",
    skip(pool, folded, focus),
    fields(
        folded = folded.len(),
        poisoned = tracing::field::Empty,
        sources = tracing::field::Empty,
    )
)]
pub(super) async fn compute_page(
    pool: &Pool,
    folded: &[FoldedChange],
    focus: Option<&ProbeFocus<'_>>,
    skip_frozen: bool,
) -> Result<ApplyPlan, ApplyError> {
    // Find the already-poisoned keys before anything else touches
    // them. Whole-key poison is per transform: a poisoned key is left
    // out of the apply of each definition it's poisoned for, decided per
    // source below, and every other reader applies it. Truncate sentinels
    // are never candidates: a truncate is whole-keyspace, not a key
    // quarantine can attribute anything to.
    //
    // Every quarantine table — `poison` included — is keyed on the
    // *canonical* (qualified, where resolvable) identity of a source table, not
    // the raw ring spelling, so each distinct `src_table` in this batch is
    // resolved once here and that canonical value is what this exclusion query,
    // `applied_keys` (`clear_key_deaths`) and `poisoned_park`
    // (`park_batch_contribution`) all use below. Matching raw would miss a key
    // already poisoned under the other spelling of its own table and
    // re-evaluate (then re-poison) it.
    //
    // `by_source` and the version fence (`versions`) key on the
    // same canonical identity, so truncates are resolved here too (their
    // fence entry needs it). Keying either on the bare table-name suffix
    // merged same-named tables in different schemas into one bucket and one
    // fence row.
    let mut canonical_srcs = quarantine::CanonicalSrcTables::default();
    for change in folded {
        canonical_srcs.get(pool, &change.src_table).await?;
    }
    let canonical_of = |src_table: &str| -> String {
        canonical_srcs
            .canonical(src_table)
            .unwrap_or(src_table)
            .to_string()
    };
    let canonical_candidates: Vec<(String, String)> = folded
        .iter()
        .filter(|c| !c.is_truncate)
        .map(|c| (canonical_of(&c.src_table), c.key.clone()))
        .collect();
    let candidates: Vec<(&str, &str)> = canonical_candidates
        .iter()
        .map(|(t, k)| (t.as_str(), k.as_str()))
        .collect();
    let poisoned = quarantine::poisoned_keys_among(pool, &candidates).await?;
    tracing::Span::current().record("poisoned", poisoned.len());

    let mut by_source: HashMap<String, Vec<&FoldedChange>> = HashMap::new();
    // Truncate sentinels never enter the keyed by-source
    // evaluation loop below — they carry no key of their own (see
    // `append::TRUNCATE_SENTINEL_KEY`) and produce no write/delete;
    // they're handled separately, right after that loop.
    let mut truncated: Vec<&FoldedChange> = Vec::new();
    let mut poisoned_park: Vec<(i64, FoldedChange)> = Vec::new();
    let mut applied_keys: Vec<(String, String)> = Vec::new();
    for change in folded {
        if change.is_truncate {
            truncated.push(change);
            continue;
        }
        // Canonical, per this function's `poisoned` comment above: the
        // exclusion match, the parked row's own `src_table`
        // (`poison_held` is keyed canonically, and the
        // qualified spelling is the ring invariant a release would replay it
        // under anyway), and `applied_keys`, whose only consumer is
        // `clear_key_deaths` against the canonically-keyed `key_deaths`.
        by_source
            .entry(canonical_of(&change.src_table))
            .or_default()
            .push(change);
    }
    tracing::Span::current().record("sources", by_source.len());

    let mut versions: BTreeMap<String, Option<i64>> = BTreeMap::new();
    let mut targets: BTreeMap<String, TargetPlan> = BTreeMap::new();
    // Aggregate targets on the group basis, in target order too.
    let mut group_targets: BTreeMap<String, super::group_basis::GroupTargetPlan> = BTreeMap::new();
    // `JOIN` targets, in target order too.
    let mut join_targets: BTreeMap<String, super::join::JoinTargetPlan> = BTreeMap::new();
    // Every `Some(src_changed)` origin timestamp
    // `buffer_transform_apply_metrics` sees below, buffered per consuming
    // target — filtered into `ApplyPlan::end_to_end_origins` only for
    // targets the `downstream_readers` computation at the end of this
    // function finds terminal (see that call site's comment). Not itself
    // part of `ApplyPlan` — only the terminal-filtered subset is.
    let mut end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>> = HashMap::new();
    // Every observation `buffer_transform_apply_metrics` below makes is
    // buffered here and carried out on `ApplyPlan`, flushed post-commit by
    // [`flush_apply_metrics`]. See `buffer_transform_apply_metrics`'s doc
    // comment for why eager recording here would be a bug.
    let mut transform_observations: Vec<TransformObservation> = Vec::new();

    for (source_key, mut changes) in by_source {
        crate::instance_log::debug!(
            src_table = %source_key,
            changes = changes.len(),
            "evaluating a source table's folded changes"
        );
        let version = catalog::source_table_version(pool, &source_key).await?;
        versions.entry(source_key.clone()).or_insert(version);

        // The source identity this batch's own CDC producer staged
        // (ADR-0011) — the ring's own spelling of `change.src_table`,
        // which `SourceTableDropped` below must echo back verbatim. Every
        // change in this bucket resolves to the same canonical `source_key`
        // by construction (`by_source` grouped on it), so any one
        // of them names the right physical table for the reads below.
        let qualified_source = changes[0].src_table.as_str();

        if skip_frozen && !has_unfrozen_reader(pool, &source_key).await? {
            crate::instance_log::warn!(
                src_table = %qualified_source,
                "every definition reading this table is frozen, and the drain was refused a \
                 read or write; skipping its changes"
            );
            continue;
        }

        // `source_key` alone determines the source table's primary key, not
        // the individual definition — introspected once per
        // source here and reused both below (every definition subscribed to
        // this source) and by the row decode below (every change, whichever
        // definition it's evaluated against). A dropped source is
        // [`ApplyError::SourceTableDropped`] (carrying the
        // ring's spelling); `None` is a key that can't be used on a table no
        // definition applies, whose changes are dropped with
        // the page. See [`source_key_for_apply`].
        let Some(pk) = source_key_for_apply(pool, qualified_source, &source_key).await? else {
            continue;
        };

        // The source column list a 1-1 target's Phase 3 check
        // renders the current row with. Loaded at most once per source, and
        // only when some definition on it is 1-1.
        let mut source_row_columns: Option<(
            Vec<String>,
            Vec<super::one_to_one_render::ImageColumn>,
        )> = None;

        // `qualified_source` (via `qualified_schema_node_key`):
        // `schema_nodes`/`schema_edges` key on qualified identity
        // (ADR-0011), so `transforms_for_source` (a thin `dependents_of`
        // wrapper) needs an exact qualified match here. `qualified_source` is
        // usually already fully-qualified (real CDC/backfill), but a
        // downstream-propagation hop's `src_table` is a bare target name
        // this same apply path staged — `qualified_schema_node_key` resolves
        // that case too; see its own doc comment.
        let defs = catalog::transforms_for_source(
            pool,
            &qualified_schema_node_key(pool, qualified_source).await?,
        )
        .await?;

        // Which readers each change is left out of (see
        // `KeyExclusion`). A change poisoned for a reader of this table is
        // left out of that reader's apply only, and parked for it. A change
        // left out of every reader is dropped here, before its image is
        // decoded, so a failure that is really in the source (an image that
        // won't decode) stops once every reader holds the key. A clean commit
        // clears the death counters of every definition that applied the
        // rest (`clear_key_deaths`).
        let exclusions = key_exclusions(&source_key, &changes, &defs, &poisoned, focus);
        let exclusions = match exclusions {
            None => {
                applied_keys.extend(changes.iter().map(|c| (source_key.clone(), c.key.clone())));
                None
            }
            Some(exclusions) => {
                let mut kept: Vec<&FoldedChange> = Vec::with_capacity(changes.len());
                let mut kept_exclusions: Vec<KeyExclusion> = Vec::with_capacity(changes.len());
                for (change, exclusion) in changes.into_iter().zip(exclusions) {
                    for &transform_id in &exclusion.defs {
                        let mut parked = change.clone();
                        parked.src_table = source_key.clone();
                        poisoned_park.push((transform_id, parked));
                    }
                    if !defs.is_empty() && exclusion.defs.len() == defs.len() {
                        continue;
                    }
                    applied_keys.push((source_key.clone(), change.key.clone()));
                    kept.push(change);
                    kept_exclusions.push(exclusion);
                }
                changes = kept;
                Some(kept_exclusions)
            }
        };
        if changes.is_empty() {
            continue;
        }

        // A definition on the group basis reads its changes' ring rows in
        // Phase 3 (`super::group_basis`), so does a `JOIN` (`super::join`),
        // and a 1-1 definition's fields are evaluated over each change's
        // image in Phase 3's upsert (`super::one_to_one_render`): nothing is
        // decoded or re-read here.
        let mut group_shapes: Vec<Option<super::group_basis::GroupShape>> =
            Vec::with_capacity(defs.len());
        for def in &defs {
            group_shapes.push(super::group_basis::route(&def.def, &def.source_columns));
        }

        for (def, group_shape) in defs.iter().zip(group_shapes) {
            // This definition's share, without the changes whose key
            // is poisoned for it (parked above).
            let changes = definition_share(def.id, &changes, &exclusions);
            if changes.is_empty() {
                continue;
            }
            // A `JOIN` takes each change's ring rows as they are,
            // routed to the side this source is; Phase 3 applies both sides'
            // (`super::join`).
            if matches!(def.def.key_space, KeySpace::Join(_)) {
                let side = if canonical_of(qualified_source) == canonical_of(&def.source_table) {
                    crate::defs::join::Side::From
                } else {
                    crate::defs::join::Side::Joined
                };
                let target_plan = match join_targets.entry(def.def.target.clone()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let client = pool.get().await?;
                        entry.insert(super::join::plan_for(&**client, pool.schema(), def).await?)
                    }
                };
                for change in changes.iter() {
                    target_plan.push(side, change)?;
                    buffer_transform_apply_metrics(
                        &def.def.target,
                        change,
                        &mut end_to_end_origins,
                        &mut transform_observations,
                    );
                }
                continue;
            }
            // An aggregate on the group basis takes each change's
            // ring rows as they are; Phase 3 sums their sides
            // (`super::group_basis`).
            if let Some(shape) = group_shape {
                let target_plan = match group_targets.entry(def.def.target.clone()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let identity = {
                            let client = pool.get().await?;
                            ddl::identity_key_columns(&**client, &def.target_table).await?
                        };
                        entry.insert(super::group_basis::GroupTargetPlan::new(
                            pool.schema(),
                            def.id,
                            &def.target_table,
                            qualified_source,
                            pk.clone(),
                            identity,
                            shape,
                        ))
                    }
                };
                for change in changes.iter() {
                    target_plan.push(change)?;
                    buffer_transform_apply_metrics(
                        &def.def.target,
                        change,
                        &mut end_to_end_origins,
                        &mut transform_observations,
                    );
                }
                continue;
            }
            // Every aggregate define accepts is on the group basis.
            if matches!(def.def.key_space, KeySpace::Aggregate { .. }) {
                return Err(ApplyError::GroupBasisUnsupported {
                    target: def.def.target.clone(),
                    what: "an aggregate the group basis doesn't take",
                });
            }
            // ADR-0003 (column-level quarantine): a column
            // the fuse has paused is left out of this plan's rendered fields,
            // so `apply_target`'s generated SQL never mentions it at all —
            // decision: freeze at the last successfully computed value, don't
            // null it out or keep reattempting a formula that's already fused
            // off. Empty for every definition with nothing currently paused —
            // the overwhelmingly common case — so this is a cheap, indexed
            // no-op read then.
            let paused = quarantine::paused_columns_for(pool, &def.def).await?;

            // `pk` is this source's full (possibly
            // composite) primary key — a `KeySpace::OneToOne` target's own primary
            // key now mirrors the source's in full, at whatever arity it
            // has, rather than narrowing to one column.
            let target_pk = pk.clone();
            let (columns, image_columns) = match &source_row_columns {
                Some(columns) => columns.clone(),
                None => {
                    let client = pool.get().await?;
                    let columns = live_row_columns(&**client, qualified_source).await?;
                    let image_columns =
                        super::one_to_one_render::image_columns(&**client, qualified_source)
                            .await?;
                    source_row_columns = Some((columns.clone(), image_columns.clone()));
                    (columns, image_columns)
                }
            };
            let render = Arc::new(super::one_to_one_render::OneToOneRender::new(
                &def.def,
                &image_columns,
                &paused,
            )?);
            let plan = targets
                .entry(def.def.target.clone())
                .or_insert_with(|| TargetPlan {
                    pk: target_pk,
                    render: Arc::clone(&render),
                    records: Vec::new(),
                    // The persisted, fully-qualified identity
                    // — not re-derived, since `def` (this source's own
                    // catalog `Definition`) already carries it. See
                    // `TargetPlan::qualified_target`'s doc comment.
                    qualified_target: def.target_table.clone(),
                    qualified_source: qualified_source.to_string(),
                    row_columns: columns,
                });

            // An Apply is written from its change's new image —
            // a delete when it has none. A Re-derive (a `recompute`, or a
            // change folded with one) is written from the row Phase 3 reads
            // under the entry lock.
            for change in changes.iter() {
                let apply = match &change.last_change {
                    Some(last) if !change.has_recompute => {
                        Some((last.lsn, last.row_txid.clone(), change.new_image.clone()))
                    }
                    _ => None,
                };
                plan.records.push(OneToOneRecord {
                    pk_text: change.key.clone(),
                    apply,
                    hop_gen: change.hop_gen,
                    src_changed: change.src_changed,
                    origin_lsn: change.origin_lsn,
                });
                buffer_transform_apply_metrics(
                    &def.def.target,
                    change,
                    &mut end_to_end_origins,
                    &mut transform_observations,
                );
            }
        }
    }

    if !poisoned_park.is_empty() {
        crate::instance_log::warn!(
            parked = poisoned_park.len(),
            "batch leaves already-poisoned keys out of the definitions they're poisoned for, \
             parking this batch's own contribution for each"
        );
    }

    // Truncate clears: for each truncated src_table, resolve its
    // targets via the catalog and record a full clear for each — the same
    // "resolve targets from the catalog" step the by-source loop above runs
    // per key, just once per truncated source instead of once per key.
    let mut clears: HashMap<String, ClearPlan> = HashMap::new();
    let mut aggregate_clears: HashMap<String, AggregateClearPlan> = HashMap::new();
    for change in &truncated {
        let source_key = canonical_of(&change.src_table);
        // Fence this source too, even though nothing evaluated against it —
        // a definition change against a truncated source, landing mid-drain,
        // must trip Phase 3's version fence exactly like it would for a
        // source this batch actually evaluated `f()` against.
        let version = catalog::source_table_version(pool, &source_key).await?;
        versions.entry(source_key.clone()).or_insert(version);

        // The same key read, and skip, as the by-source loop's above.
        let Some(pk) = source_key_for_apply(pool, &change.src_table, &source_key).await? else {
            continue;
        };
        // `&change.src_table` (qualified), not `source_key` — see
        // the by-source loop above's identical comment on its own
        // `transforms_for_source` call. A `TRUNCATE` is always a real
        // physical CDC event (never a bare, internally-synthesized
        // `Recompute` row), so `qualified_schema_node_key` is a no-op here
        // in practice — routed through it anyway for the same safety the
        // by-source loop gets, at effectively no cost.
        let defs = catalog::transforms_for_source(
            pool,
            &qualified_schema_node_key(pool, &change.src_table).await?,
        )
        .await?;
        for def in &defs {
            // The floor the clear raises (see the error's doc).
            let truncate_lsn = change.lsn.ok_or_else(|| ApplyError::TruncateWithoutLsn {
                src_table: change.src_table.clone(),
            })?;
            match &def.def.key_space {
                // A `JOIN` page clears what the truncate of its side
                // empties, under its lock row (`super::join`).
                KeySpace::Join(_) => {
                    let side = crate::defs::join::side_of_table(&def.def, &change.src_table)
                        .filter(|_| super::join::applies(&def.def))
                        .ok_or_else(|| ApplyError::GroupBasisUnsupported {
                            target: def.def.target.clone(),
                            what: "a TRUNCATE of a JOIN side the JOIN apply doesn't take",
                        })?;
                    let target_plan = match join_targets.entry(def.def.target.clone()) {
                        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            let client = pool.get().await?;
                            entry
                                .insert(super::join::plan_for(&**client, pool.schema(), def).await?)
                        }
                    };
                    target_plan.truncate(side, truncate_lsn, change);
                }
                KeySpace::Aggregate { .. } => {
                    if let Some(existing) = aggregate_clears.get_mut(&def.def.target) {
                        existing.hop_gen = existing.hop_gen.max(change.hop_gen);
                        existing.src_changed =
                            earliest_src_changed(existing.src_changed, change.src_changed);
                        existing.origin_lsn =
                            earliest_origin(existing.origin_lsn, change.origin_lsn);
                        existing.truncate_lsn = existing.truncate_lsn.max(truncate_lsn);
                    } else {
                        // The ungated `identity_key_columns`, not
                        // `source_primary_key`. The clear only renders this
                        // identity through `pk_key_sql_expr` (a `::text` of
                        // each column) to report cleared groups to the seam,
                        // so the key-type gate buys nothing here — and a
                        // `numeric` `GROUP BY` is still admitted, so the gate
                        // halted the instance on every such truncate. A
                        // chained reader can't depend on this key's text
                        // being stable: chaining off an aggregate whose
                        // identity fails the gate is refused.
                        let pk = {
                            let client = pool.get().await?;
                            ddl::identity_key_columns(&**client, &def.target_table).await?
                        };
                        if pk.is_empty() {
                            return Err(DdlError::NoPrimaryKey {
                                source_table: def.target_table.clone(),
                            }
                            .into());
                        }
                        // A group-basis target empties its hidden
                        // table.
                        let state =
                            if super::group_basis::route(&def.def, &def.source_columns).is_some() {
                                AggregateState::GroupBasis {
                                    hidden: super::group_basis::hidden_table_ident(
                                        pool.schema(),
                                        def.id,
                                    ),
                                }
                            } else {
                                AggregateState::Neither
                            };
                        aggregate_clears.insert(
                            def.def.target.clone(),
                            AggregateClearPlan {
                                hop_gen: change.hop_gen,
                                qualified_target: def.target_table.clone(),
                                pk,
                                src_changed: change.src_changed,
                                origin_lsn: change.origin_lsn,
                                state,
                                truncate_lsn,
                            },
                        );
                    }
                }
                KeySpace::OneToOne => {
                    // Same full, un-narrowed primary key as
                    // the by-source loop's identical `TargetPlan` construction
                    // above.
                    let target_pk = pk.clone();
                    clears
                        .entry(def.def.target.clone())
                        .and_modify(|existing| {
                            existing.hop_gen = existing.hop_gen.max(change.hop_gen);
                            existing.src_changed =
                                earliest_src_changed(existing.src_changed, change.src_changed);
                            existing.origin_lsn =
                                earliest_origin(existing.origin_lsn, change.origin_lsn);
                            existing.truncate_lsn = existing.truncate_lsn.max(truncate_lsn);
                        })
                        .or_insert(ClearPlan {
                            pk: target_pk,
                            hop_gen: change.hop_gen,
                            qualified_target: def.target_table.clone(),
                            src_changed: change.src_changed,
                            origin_lsn: change.origin_lsn,
                            truncate_lsn,
                        });
                }
            }
            // A TRUNCATE is a genuine applied change to every direct
            // downstream target, same as a row-driven change — recorded
            // once per def per truncated source, mirroring the row-driven
            // by_source loop above (ADR-0009 decision 5).
            buffer_transform_apply_metrics(
                &def.def.target,
                change,
                &mut end_to_end_origins,
                &mut transform_observations,
            );
        }
    }

    let mut downstream_readers = HashMap::new();
    let mut all_targets: std::collections::HashSet<&String> = targets.keys().collect();
    all_targets.extend(clears.keys());
    all_targets.extend(group_targets.keys());
    all_targets.extend(join_targets.keys());
    all_targets.extend(aggregate_clears.keys());
    // Only the
    // terminal-filtered subset of `end_to_end_origins` survives into
    // `ApplyPlan` — flushed post-commit by `flush_apply_metrics`, not
    // recorded here.
    let mut terminal_end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>> =
        HashMap::new();
    for target in all_targets {
        // `target` is bare (`def.def.target`) — `schema_nodes` keys on
        // qualified identity (ADR-0011), so a bare lookup here
        // would silently find nothing and permanently disable downstream
        // propagation for every chained transform.
        // `qualified_schema_node_key` resolves it the same way it resolves
        // a bare `Recompute`-staged `src_table` above (this *is* exactly
        // that case, one step earlier: `target` is about to become such a
        // row's `src_table` the moment this loop's caller stages it).
        //
        // That resolution is now *kept*, not discarded once the
        // reader question is answered, and Phase 3 stages it verbatim as the
        // propagated row's `src_table`. Reusing the identity this lookup
        // already computed is what makes the emitted spelling agree, by
        // construction, with the one a capture trigger independently stages
        // for the same physical table (its spec's `intake::qualify`
        // spelling) — `resolve_graph_identity`'s first step is a
        // `search_path` lookup of the real relation, which a target table
        // satisfies, and both sides then format the schema it finds through
        // that same `intake::qualify`, so the two strings are equal by
        // construction rather than by coincidence. (Strictly, step 1 returns
        // the *first* `current_schemas(false)` match, so an unrelated relation
        // of the same bare name in an earlier schema on the pinned path would
        // resolve to the wrong one — a pre-existing property of
        // `resolve_graph_identity` shared with every other caller, including
        // the reader lookup on the next line, not something this staging site
        // introduces: the reader lookup would come back empty for that same
        // wrong name and propagation would stop rather than misfold.)
        // Without that agreement, one write to an intermediate hop folds as
        // two unrelated `(src_table, key)` groups and the downstream target's
        // batched upsert is handed the same conflict key twice, which
        // Postgres rejects outright — a permanent live-lock, since every
        // retry re-derives the identical pair.
        let qualified_target = qualified_schema_node_key(pool, target).await?;
        let has_downstream = !catalog::transforms_for_source(pool, &qualified_target)
            .await?
            .is_empty();
        downstream_readers.insert(target.clone(), has_downstream.then_some(qualified_target));
        // ADR-0009 decision 2: end-to-end latency is only ever
        // recorded for a *terminal* transform — one with no downstream
        // reader of its own — reusing this exact "does anything read
        // `target`" lookup rather than a second one. An intermediate hop
        // (`has_downstream` true) still gets its per-transform latency from
        // `buffer_transform_apply_metrics` above; it simply never carries
        // through to `ApplyPlan::end_to_end_origins`, so its origins in the
        // local `end_to_end_origins` accumulator are dropped once this
        // function returns.
        if !has_downstream && let Some(origins) = end_to_end_origins.get(target) {
            terminal_end_to_end_origins.insert(target.clone(), origins.clone());
        }
    }

    Ok(ApplyPlan {
        versions,
        targets,
        group_targets,
        join_targets,
        clears,
        aggregate_clears,
        poisoned_park,
        applied_keys,
        retries_succeeded: Default::default(),
        transform_observations,
        end_to_end_origins: terminal_end_to_end_origins,
    })
}

/// Flushes `plan`'s
/// buffered metrics observations — [`ApplyPlan::transform_observations`]
/// into [`crate::metrics::record_transform_latency`]/
/// [`crate::metrics::increment_changes_applied`], [`ApplyPlan::end_to_end_origins`]
/// into [`crate::metrics::record_end_to_end_latency`] — sampling
/// `SystemTime::now()` fresh, right here, rather than reusing whatever
/// [`compute`] would have sampled during planning.
///
/// Must only be called once a batch's `apply_and_mark_drained`/
/// `apply_and_mark_drained_many` call has actually committed. `compute`
/// (Phase 2: no transaction, no locks) can run more than once for the same
/// folded input — [`drain_once`]/[`drain_many`]'s retry loop calls it again
/// on a version-fence miss or a rolled-back Phase 3 failure
/// (`classify_and_retry`'s `VersionFenceMiss`/`Transient` classes) — so a
/// `plan` built by a losing attempt must never reach this function; only
/// the plan behind the attempt whose transaction actually commits should.
/// Both `drain_once` and `drain_many` share this one helper (called right
/// after their own `txn.commit().await?`) rather than each recording
/// inline, since both hand it the exact same `&ApplyPlan` shape regardless
/// of how many segments that attempt coalesced.
fn flush_apply_metrics(plan: &ApplyPlan) {
    let retried = plan
        .retries_succeeded
        .load(std::sync::atomic::Ordering::Relaxed);
    if retried > 0 {
        crate::metrics::increment_held_key_retries_succeeded(retried);
    }
    for observation in &plan.transform_observations {
        if let Some(src_changed) = observation.src_changed {
            let latency = std::time::SystemTime::now()
                .duration_since(src_changed)
                .unwrap_or(std::time::Duration::ZERO);
            crate::metrics::record_transform_latency(&observation.transform, latency);
        }
        crate::metrics::increment_changes_applied(&observation.transform, observation.row_count);
    }
    for (transform, origins) in &plan.end_to_end_origins {
        for origin in origins {
            let latency = std::time::SystemTime::now()
                .duration_since(*origin)
                .unwrap_or(std::time::Duration::ZERO);
            crate::metrics::record_end_to_end_latency(transform, latency);
        }
    }
}

// ---------------------------------------------------------------------
// Phase 3: apply ∪ mark-drained
// ---------------------------------------------------------------------

/// At most this many rows per [`apply_target`] upsert statement.
const WRITE_CHUNK_ROWS: usize = 4096;

/// At most this much row text per [`apply_target`] upsert statement, unless
/// one row is larger on its own.
const WRITE_CHUNK_BYTES: usize = 4 << 20;

/// Decodes one [`TargetWrite::pk_text`]/[`TargetDelete::pk_text`] (this
/// crate's shared key-contract text, [`ddl::pk_key_sql_expr`]/
/// [`ddl::join_pk_key`]) into the value of each of `pk`'s columns, via
/// [`ddl::split_pk_key`], `None` for a `NULL` part.
///
/// A `NULL` part is reachable only for a source keyed by a nullable unique
/// index (an aggregate or `JOIN` target's `GROUP BY` columns), whose 1-1
/// target is keyed `unique nulls not distinct` in turn
/// (`ddl::target_table_ddl`): [`apply_target`] writes and matches it as a
/// real SQL `NULL`, never as the sentinel text that encodes it.
pub(super) fn decode_target_pk_parts(
    pk: &[PrimaryKeyColumn],
    target: &str,
    key: &str,
) -> Result<Vec<Option<String>>, ApplyError> {
    Ok(ddl::split_pk_key(pk, target, key)?
        .into_iter()
        .map(|part| part.map(Cow::into_owned))
        .collect())
}

/// `c0`, `c1`, … — collision-free column aliases for a keyset `unnest(...)`
/// relation, one per `pk` column.
pub(super) fn pk_keyset_col(i: usize) -> String {
    format!("c{i}")
}

/// `unnest($start::text[]::t0[], $start+1::text[]::t1[], …) as k(c0, c1, …)`
/// — `pk`'s columns as a bound keyset relation, one array bind parameter per
/// column (so the whole keyset costs `pk.len()` bind parameters regardless of
/// how many keys it carries, well under Postgres's bind cap). The multi-
/// column match [`apply_target`] needs once a composite (arity > 1) primary
/// key means a plain `= any($1::text[]::cast[])` no longer identifies a row
/// on its own — arity 1 keeps that simpler, pre-existing form
/// instead (see [`apply_target`]'s own arity branch). A column recompute
/// used it at every arity, since joining on the target's own
/// key columns is what lets its write-back use the index.
pub(super) fn pk_keyset_unnest(pk: &[PrimaryKeyColumn], start: usize) -> String {
    let arrays: Vec<String> = pk
        .iter()
        .enumerate()
        .map(|(i, c)| format!("${}::text[]::{}[]", start + i, c.data_type))
        .collect();
    let cols: Vec<String> = (0..pk.len()).map(pk_keyset_col).collect();
    format!("unnest({}) as k({})", arrays.join(", "), cols.join(", "))
}

/// `<alias>.<col0> = k.c0 and <alias>.<col1> = k.c1 and …` — matches one row
/// of `alias` against [`pk_keyset_unnest`]'s bound relation. Plain `=`, so
/// only for keys with no `NULL` part: a key with one is matched by
/// [`null_pattern_match`] instead.
pub(super) fn pk_keyset_match(pk: &[PrimaryKeyColumn], alias: &str) -> String {
    pk.iter()
        .enumerate()
        .map(|(i, c)| format!("{alias}.{} = k.{}", quote_ident(&c.name), pk_keyset_col(i)))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Whether a join of a keyset to a table keyed by `key` should also restrict
/// the key's column to the keyset's own array (`t.<col> = any(<array>)`),
/// which the join already implies: only when `key` is a single column. Every
/// keyset join that takes such a bound asks here.
///
/// The bound caps the table's side of whatever join the planner picks at the
/// keyset. Without it, a table analyzed while small and grown since is read
/// in full: at 1M rows analyzed at 100, a 5,000-key batch hashed a
/// sequential scan of the table, 3 to 13 times slower than through its key's
/// index.
///
/// A composite key isn't bounded. The planner multiplies one bound per
/// column's selectivities as if they were independent, but a composite key's
/// columns are each nearly as selective as the whole key, so it expects the
/// bounded table to yield a row or two. It then loops over that scan and
/// compares every bounded row with every key, quadratic in the batch,
/// statistics fresh or not: 5,000 keys took 2.3 s to lock on a 1M-row target
/// with a four-column key (25 ms unbounded), and 1.8 s on a 20M-row target
/// with a two-column key (19 ms). A leading-column bound alone avoids that,
/// but reads a whole tenant when the leading column is a low-cardinality one
/// (84 ms against 4 ms for 500 keys over 1,000 tenants). Unbounded, a
/// composite key is probed per key while its statistics are fresh, and can
/// still be hashed against a sequential scan while they lag (139 ms against
/// 27 ms bounded, two columns at 1M rows), unless the statement also runs
/// under `super::ledger::ENTRY_PLAN_SETTINGS` (no sequential scan), as the
/// source read, the endpoint feed's re-read, the sweep delete and the
/// single-column and composite pre-locks ([`lock_single_keys`],
/// [`lock_composite_keys`]) and the single-column delete
/// ([`delete_single_keys`]) do. A composite key is always the full key of
/// a primary key or unique index (`ddl::identity_key_columns`), so the
/// settings always have an index to probe. Each composite statement
/// among them has a plan test on a two-column key analyzed at 100 rows and
/// grown to 400k or 1M, which counts the rows its scans actually read rather
/// than the planner's estimates, and holds them to the batch.
/// [`apply_target`]'s composite delete runs without the settings: its
/// `exists` probed the key's index once per key for 5,000 keys of a
/// two-column key analyzed at 100 rows and grown to 1M or 3M, and of a
/// four-column one grown to 1M, on PostgreSQL 16, 17 and 18.
///
/// The bound alone isn't enough on PostgreSQL 16, which prices an index scan
/// for thousands of `= any` values far above 17's estimate: it scanned a
/// 400k-row table analyzed at 100 and filtered it by the bound, where 17
/// read the index. So each statement that takes the bound also runs under
/// those settings.
pub(crate) fn bounds_keyset_by_array(key: &[PrimaryKeyColumn]) -> bool {
    key.len() == 1
}

/// [`apply_target`]'s pre-lock for a single-column key: the target rows of
/// the bound `text[]` keys at `$1` (cast to the key's type, so the column
/// itself stays uncast and its index usable), `select`ing `columns`, locked
/// `for update` in key order. `pk_ident` is the quoted key column and
/// `pk_cast` its type.
fn single_lock_statement(
    target_ident: &str,
    pk_ident: &str,
    pk_cast: &str,
    columns: &str,
) -> String {
    format!(
        "select {columns} from {target_ident} as t \
         where t.{pk_ident} = any($1::text[]::{pk_cast}[]) \
         order by t.{pk_ident} for update"
    )
}

/// Runs [`single_lock_statement`] over `keys` under
/// `super::ledger::ENTRY_PLAN_SETTINGS` (no sequential scan), and returns
/// the locked rows in key order. While a target's statistics lag its size,
/// PostgreSQL 16 prices an index scan for thousands of `= any` values so
/// high that it read a 400k-row target in full and filtered it by the keys;
/// with the settings it probes the key's index. With fresh
/// statistics the planner already probes the index, and the settings leave
/// that plan alone.
async fn lock_single_keys(
    txn: &Transaction<'_>,
    target_ident: &str,
    pk_ident: &str,
    pk_cast: &str,
    columns: &str,
    keys: &[&str],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    super::ledger::query_by_entry_key(
        txn,
        &single_lock_statement(target_ident, pk_ident, pk_cast, columns),
        &[&keys],
    )
    .await
}

/// [`apply_target`]'s delete for a single-column key: the target rows of the
/// bound `text[]` keys at `$1`, `returning` the key text `returning`.
fn single_delete_statement(
    target_ident: &str,
    pk_ident: &str,
    pk_cast: &str,
    returning: &str,
) -> String {
    format!(
        "delete from {target_ident} as t \
         where t.{pk_ident} = any($1::text[]::{pk_cast}[]) \
         returning {returning} as pk"
    )
}

/// Runs [`single_delete_statement`] over `keys` under
/// `super::ledger::ENTRY_PLAN_SETTINGS` (no sequential scan), as
/// [`lock_single_keys`] runs the pre-lock of the same rows, and returns the
/// deleted keys' rows.
async fn delete_single_keys(
    txn: &Transaction<'_>,
    target_ident: &str,
    pk_ident: &str,
    pk_cast: &str,
    returning: &str,
    keys: &[&str],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    super::ledger::query_by_entry_key(
        txn,
        &single_delete_statement(target_ident, pk_ident, pk_cast, returning),
        &[&keys],
    )
    .await
}

/// [`apply_target`]'s pre-lock for a composite key: the target
/// rows of a bound keyset relation ([`pk_keyset_unnest`] at `$1`), `select`ing
/// `columns`, locked `for update of t` in key order. No one column's `=
/// any(...)` test identifies a row of a composite key, so the pre-lock joins
/// the target to the keyset and locks only the real table's rows
/// (`unnest(...)`'s derived rows aren't real table rows Postgres could lock).
///
/// The join is deliberately not also restricted by one `t.<col> = any(<its
/// array>)` per key column: see [`bounds_keyset_by_array`]. It runs under
/// `ENTRY_PLAN_SETTINGS` instead ([`lock_composite_keys`]).
fn composite_lock_statement(target_ident: &str, pk: &[PrimaryKeyColumn], columns: &str) -> String {
    format!(
        "select {columns} from {target_ident} as t join {} on ({}) \
         order by {} for update of t",
        pk_keyset_unnest(pk, 1),
        pk_keyset_match(pk, "t"),
        pk.iter()
            .map(|c| format!("t.{}", quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// Runs [`composite_lock_statement`] over the keys in `arrays` (column `j`'s
/// parts in `arrays[j]`, as [`transpose_pk_parts`] gives them) under
/// `super::ledger::ENTRY_PLAN_SETTINGS` (no sequential scan), and returns
/// the locked rows. While a target's statistics lag its size, the join
/// alone hashed the keys against a sequential scan of the target: a
/// two-column key at 1M rows analyzed at 100 took 133–167 ms on
/// PostgreSQL 16 and 17, against 12–16 ms probing the key's index per key
/// with the settings. With fresh statistics the planner already
/// probes per key, and the settings leave that plan alone.
async fn lock_composite_keys(
    txn: &Transaction<'_>,
    target_ident: &str,
    pk: &[PrimaryKeyColumn],
    columns: &str,
    arrays: &[Vec<&str>],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    let params: Vec<&(dyn ToSql + Sync)> = arrays.iter().map(|a| a as _).collect();
    super::ledger::query_by_entry_key(
        txn,
        &composite_lock_statement(target_ident, pk, columns),
        &params,
    )
    .await
}

/// The per-column bind arrays [`pk_keyset_unnest`] needs, transposed from
/// `rows` (each an already-[`decode_target_pk_parts`]-decoded key with no
/// `NULL` part, in `pk`'s own declared column order) so column `j`'s array
/// is every row's `j`th part.
pub(super) fn transpose_pk_parts<'a>(arity: usize, rows: &[Vec<&'a str>]) -> Vec<Vec<&'a str>> {
    (0..arity)
        .map(|j| rows.iter().map(|r| r[j]).collect())
        .collect()
}

/// Decoded keys ([`decode_target_pk_parts`]) as [`apply_target`] matches
/// them against its target: `plain`, each key with no `NULL` part (its
/// parts, in key order), matched by the plain-key statements;
/// and `patterns`, the keys with a `NULL` part, by their `NULL` pattern
/// (`pattern[i]`: part `i` is `NULL`), each pattern's non-`NULL` parts
/// transposed into one array per such column, as [`null_pattern_match`]
/// binds them.
///
/// Matched per pattern because `is not distinct from` (or an `or … is
/// null` arm) can't probe the key's index: `col is null` can, so each
/// pattern's statement reaches its rows through the index, as
/// `staging::target_mutations` matches its.
#[derive(Default)]
struct MatchKeys<'a> {
    plain: Vec<Vec<&'a str>>,
    patterns: BTreeMap<Vec<bool>, Vec<Vec<&'a str>>>,
}

impl<'a> MatchKeys<'a> {
    fn of(keys: impl IntoIterator<Item = &'a Vec<Option<String>>>) -> Self {
        let mut out = Self::default();
        for key in keys {
            let pattern: Vec<bool> = key.iter().map(Option::is_none).collect();
            if !pattern.contains(&true) {
                out.plain
                    .push(key.iter().flatten().map(String::as_str).collect());
                continue;
            }
            let columns = out.patterns.entry(pattern).or_insert_with_key(|pattern| {
                vec![Vec::new(); pattern.iter().filter(|null| !**null).count()]
            });
            for (column, part) in columns.iter_mut().zip(key.iter().flatten()) {
                column.push(part.as_str());
            }
        }
        out
    }
}

/// The match of `alias`'s row against the keys of one `NULL` pattern
/// ([`MatchKeys::patterns`]): `<alias>.<col> is null` for each `NULL` part,
/// and `<alias>.<col> = k.c<i>` for the others, against their keyset
/// (`unnest(…) as k(c<i>, …)`, one `text[]` per non-`NULL` column, bound from
/// `$start`). The keyset is `None` when every part is `NULL`: the pattern is
/// then one key, matched by its `is null` tests alone.
fn null_pattern_match(
    pk: &[PrimaryKeyColumn],
    pattern: &[bool],
    alias: &str,
    start: usize,
) -> (Option<String>, String) {
    let mut arrays = Vec::new();
    let mut cols = Vec::new();
    let mut matched = Vec::with_capacity(pk.len());
    for (i, (column, null)) in pk.iter().zip(pattern).enumerate() {
        let ident = quote_ident(&column.name);
        if *null {
            matched.push(format!("{alias}.{ident} is null"));
        } else {
            arrays.push(format!(
                "${}::text[]::{}[]",
                start + arrays.len(),
                column.data_type
            ));
            cols.push(pk_keyset_col(i));
            matched.push(format!("{alias}.{ident} = k.{}", pk_keyset_col(i)));
        }
    }
    let keyset = (!arrays.is_empty())
        .then(|| format!("unnest({}) as k({})", arrays.join(", "), cols.join(", ")));
    (keyset, matched.join(" and "))
}

/// [`apply_target`]'s pre-lock of the target rows of one `NULL` pattern's
/// keys ([`null_pattern_match`] at `$1`), `select`ing `columns`, locked `for
/// update of t` in key order.
fn null_pattern_lock_statement(
    target_ident: &str,
    pk: &[PrimaryKeyColumn],
    pattern: &[bool],
    columns: &str,
) -> String {
    let order: Vec<String> = pk
        .iter()
        .map(|c| format!("t.{}", quote_ident(&c.name)))
        .collect();
    match null_pattern_match(pk, pattern, "t", 1) {
        (Some(keyset), matched) => format!(
            "select {columns} from {target_ident} as t join {keyset} on ({matched}) \
             order by {} for update of t",
            order.join(", ")
        ),
        (None, matched) => {
            format!("select {columns} from {target_ident} as t where {matched} for update of t")
        }
    }
}

/// [`apply_target`]'s delete of the target rows of one `NULL` pattern's keys
/// ([`null_pattern_match`] at `$1`), `returning` the key text `returning`.
fn null_pattern_delete_statement(
    target_ident: &str,
    pk: &[PrimaryKeyColumn],
    pattern: &[bool],
    returning: &str,
) -> String {
    match null_pattern_match(pk, pattern, "t", 1) {
        (Some(keyset), matched) => format!(
            "delete from {target_ident} as t using {keyset} where {matched} \
             returning {returning} as pk"
        ),
        (None, matched) => {
            format!("delete from {target_ident} as t where {matched} returning {returning} as pk")
        }
    }
}

/// `columns` as the bind parameters of [`null_pattern_match`]'s keyset.
fn pattern_params<'a>(columns: &'a [Vec<&'a str>]) -> Vec<&'a (dyn ToSql + Sync)> {
    columns.iter().map(|c| c as &(dyn ToSql + Sync)).collect()
}

/// What [`settle_one_to_one_target`] did to one target: how many keys it
/// physically wrote and deleted (the keys themselves went to its
/// [`TargetMutations`]).
type AppliedTarget = (usize, usize);

/// Settles one 1-1 target's records on its ledger, in the page's
/// transaction: locks the entries, reads the Re-derives' rows, updates the
/// entries, then writes the target rows of only the keys whose entry changed,
/// evaluating their fields in the upsert. See `super::one_to_one_ledger`.
///
/// A key carried by more than one record settles once, from the record that
/// reflects the latest state: a Re-derive, which reads it, or else the
/// latest Apply.
///
/// `first_seg` and `seg_seq` are the lowest and highest segments of the
/// page's batches. A page whose first is at or below the target's
/// `build_seg` re-derives every record instead of applying it, as a ledger
/// target's page does (`super::ledger::page_may_predate_build`): a
/// change committed before the target's Re-derive build started may have
/// had a later change drained before the start, which never reached the
/// target, and a key deleted that way has no source row for a chunk to
/// find. A tombstone takes `seg_seq` as its `applied_seg`, or the
/// Re-derive read's newest segment when that is newer: the read is
/// live, so it can see a later batch's delete, and a tombstone stamped below
/// that batch could be collected while an older change in it is pending.
async fn settle_one_to_one_target(
    txn: &Transaction<'_>,
    target: &str,
    plan: &TargetPlan,
    first_seg: i64,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<AppliedTarget, ApplyError> {
    let ledger = one_to_one_ledger::LedgerRef::of_target(&plan.qualified_target);
    let rederived_records: Vec<OneToOneRecord>;
    let records: &[OneToOneRecord] = if plan.records.iter().any(|r| r.apply.is_some())
        && super::ledger::page_may_predate_build(txn, &ledger, first_seg).await?
    {
        rederived_records = plan
            .records
            .iter()
            .map(|r| OneToOneRecord {
                apply: None,
                ..r.clone()
            })
            .collect();
        &rederived_records
    } else {
        &plan.records
    };
    let mut by_key: BTreeMap<&str, &OneToOneRecord> = BTreeMap::new();
    for record in records {
        by_key
            .entry(&record.pk_text)
            .and_modify(|kept| {
                let supersedes = match (&record.apply, &kept.apply) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some((lsn, ..)), Some((kept_lsn, ..))) => lsn > kept_lsn,
                };
                if supersedes {
                    *kept = record;
                }
            })
            .or_insert(record);
    }
    if by_key.is_empty() {
        return Ok((0, 0));
    }
    // Planted bug: apply every change without ADR-0002's
    // I2. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let predicate = !crate::plant::fires(
        crate::plant::Plant::StaleOneToOneWrite,
        by_key.values().any(|r| r.apply.is_some()),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let predicate = true;
    // A Re-derive's `present` is unknown before its read; the lock ignores
    // it.
    let to_lock: Vec<one_to_one_ledger::EntryChange<'_>> = by_key
        .values()
        .map(|r| one_to_one_ledger::EntryChange {
            key: &r.pk_text,
            apply: r.apply.as_ref().map(|(lsn, txid, _)| (*lsn, txid.as_str())),
            present: r
                .apply
                .as_ref()
                .is_none_or(|(_, _, values)| values.is_some()),
        })
        .collect();
    let inserted =
        one_to_one_ledger::lock_entries(txn, &ledger, &to_lock, seg_seq, predicate, false).await?;

    let rederive_keys: Vec<&str> = by_key
        .values()
        .filter(|r| r.apply.is_none())
        .map(|r| r.pk_text.as_str())
        .collect();
    // The entries' segment stamp: the page's latest segment, or the
    // Re-derive read's when that is newer (see `read_rows`).
    let (mut read, snapshot, entry_seg) = if rederive_keys.is_empty() {
        (HashMap::new(), None, seg_seq)
    } else {
        let read = one_to_one_ledger::read_rows(
            txn,
            &plan.qualified_target,
            &plan.qualified_source,
            &plan.pk,
            &plan.row_columns,
            &rederive_keys,
        )
        .await?;
        (read.rows, Some(read.snapshot), seg_seq.max(read.seg))
    };
    // A Re-derive's row, `None` for a key with no row.
    let rederived: HashMap<&str, Option<String>> = rederive_keys
        .into_iter()
        .map(|key| (key, read.remove(key)))
        .collect();

    let mut changes: Vec<one_to_one_ledger::EntryChange<'_>> = Vec::with_capacity(by_key.len());
    for record in by_key.values() {
        // A new key's Apply was settled by its insert.
        if record.apply.is_some() && inserted.keys.contains(&record.pk_text) {
            continue;
        }
        let present = match &record.apply {
            Some((_, _, values)) => values.is_some(),
            None => match rederived.get(record.pk_text.as_str()) {
                Some(values) => values.is_some(),
                None => continue,
            },
        };
        changes.push(one_to_one_ledger::EntryChange {
            key: &record.pk_text,
            apply: record
                .apply
                .as_ref()
                .map(|(lsn, txid, _)| (*lsn, txid.as_str())),
            present,
        });
    }
    let mut changed = one_to_one_ledger::update_entries(
        txn,
        &ledger,
        &changes,
        snapshot.as_deref(),
        entry_seg,
        predicate,
    )
    .await?;
    changed.extend(inserted.applied);
    // A placeholder nothing wrote doesn't outlive the page.
    let unwritten: Vec<&str> = inserted
        .keys
        .iter()
        .filter(|key| !changed.contains(*key))
        .map(String::as_str)
        .collect();
    one_to_one_ledger::drop_placeholders(txn, &ledger, &unwritten).await?;

    let mut writes = Vec::new();
    let mut deletes = Vec::new();
    for record in by_key.values() {
        if !changed.contains(&record.pk_text) {
            continue;
        }
        let image = match &record.apply {
            Some((_, _, image)) => image.clone(),
            None => rederived.get(record.pk_text.as_str()).cloned().flatten(),
        };
        match image {
            Some(image) => writes.push(TargetWrite {
                pk_text: record.pk_text.clone(),
                image,
                hop_gen: record.hop_gen,
                src_changed: record.src_changed,
                origin_lsn: record.origin_lsn,
            }),
            None => deletes.push(TargetDelete {
                pk_text: record.pk_text.clone(),
                hop_gen: record.hop_gen,
                src_changed: record.src_changed,
                origin_lsn: record.origin_lsn,
            }),
        }
    }
    apply_target(txn, target, plan, &writes, &deletes, mutations).await
}

/// Runs one target table's ordered pre-lock, then its no-op-suppressed
/// upsert and delete, reporting every key Postgres actually wrote or deleted
/// (as opposed to every key this batch merely *proposed* — the
/// no-op-suppression `WHERE ... IS DISTINCT FROM ...` guard can mean a
/// proposed write physically changes nothing) to `mutations`, with the
/// key's prior image (which covers an image-bearing delete). Returns only
/// the counts.
///
/// The pre-lock takes every key this call touches (write or delete) `FOR
/// UPDATE`, ordered ascending, in one round trip — the deadlock-avoidance
/// convention doc 05 calls for between concurrent workers writing
/// overlapping target rows. At arity 1 it binds the whole key set as a
/// single `text[]` parameter; a composite (arity > 1) key instead binds one
/// typed array per column and joins the target to that bound keyset.
/// A key with a `NULL` part (a target over a nullable-keyed source)
/// is locked and deleted by a statement per `NULL` pattern
/// after the others ([`MatchKeys`]) —
/// either way the parameter count is `O(pk.len())`, not `O(batch size)`, so
/// — unlike the upsert below — it never approaches the bind-parameter cap
/// regardless of batch size. Because this transaction already holds every
/// lock it needs before the upsert/delete run, chunking those into multiple
/// statements below doesn't reopen the ordering gap the pre-lock exists to
/// close: two transactions racing on overlapping keys still each take every
/// lock, in the same ascending order, before either writes anything.
///
/// ADR-0009 decision 3: the finest-grained span in the
/// propagation tree — one per consuming transform per batch, downstream of
/// fold (`docs/observability.md`'s "Logs and traces" section), the exact
/// same grouping `buffer_transform_apply_metrics` observes its
/// per-transform latency histogram from. `transform` (this target's own
/// name — this crate's one "transform name," per
/// `ApplyError::ColumnNotPaused`/`DefinitionNotLive`'s own `transform`
/// fields) matches the metrics facade's `transform` label exactly, so a
/// trace and a Prometheus series for the same transform are easy to
/// cross-reference by eye.
#[tracing::instrument(
    name = "staging.apply_target",
    skip(txn, target, plan, plan_writes, plan_deletes),
    fields(
        transform = %target,
        proposed_writes = plan_writes.len(),
        proposed_deletes = plan_deletes.len(),
        written = tracing::field::Empty,
        deleted = tracing::field::Empty,
    )
)]
async fn apply_target(
    txn: &Transaction<'_>,
    target: &str,
    plan: &TargetPlan,
    plan_writes: &[TargetWrite],
    plan_deletes: &[TargetDelete],
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if plan_writes.is_empty() && plan_deletes.is_empty() {
        return Ok((0, 0));
    }

    let arity = plan.pk.len();
    let pk_idents: Vec<String> = plan.pk.iter().map(|c| quote_ident(&c.name)).collect();
    let pk_col_list = pk_idents.join(", ");
    // `plan.qualified_target` (the persisted identity), not a bare
    // `quote_ident(target)` — a target explicitly qualified into a
    // non-default schema isn't necessarily on this connection's
    // pinned `search_path`. See `TargetPlan::qualified_target`'s doc comment
    // and `ddl::qualified_target_table_ident`'s.
    let target_ident = ddl::qualified_target_table_ident(&plan.qualified_target);
    let field_idents: Vec<String> = plan
        .render
        .fields
        .iter()
        .map(|(n, _)| quote_ident(n))
        .collect();

    // The pre-lock below doubles as the prior-image capture every
    // changed key reports to `mutations` — see `staging::target_mutations`.
    // `None` (nothing reads this target) captures nothing.
    let prior_image_expr = mutations
        .image_sql(txn, &plan.qualified_target, "t")
        .await?;
    let prior_select = match &prior_image_expr {
        Some(expr) => format!(", ({expr})::text"),
        None => String::new(),
    };
    let lock_key_expr = ddl::pk_key_sql_expr(&plan.pk, Some("t"));

    // `write.pk_text`/`delete.pk_text` is this target's
    // shared key-contract text, not raw key values: decode each one through
    // `decode_target_pk_parts` before treating it as a key value (or a
    // lock/match key) anywhere below. A `NULL` part is a real
    // SQL `NULL` in the target's `unique nulls not distinct` key, written
    // as one and matched by `is null` (`MatchKeys`), never the sentinel text.
    let decoded_writes: Vec<Vec<Option<String>>> = plan_writes
        .iter()
        .map(|w| decode_target_pk_parts(&plan.pk, target, &w.pk_text))
        .collect::<Result<_, _>>()?;
    let decoded_deletes: Vec<Vec<Option<String>>> = plan_deletes
        .iter()
        .map(|d| decode_target_pk_parts(&plan.pk, target, &d.pk_text))
        .collect::<Result<_, _>>()?;

    // Deduplicated lock keys (in their decoded form, so a batch never
    // locks the same physical row twice under two differently-encoded
    // spellings of the same key) — the actual lock *order* comes from each
    // branch's own `order by` below, not from this collection's order, so a
    // plain dedup is all that's needed here.
    let mut lock_key_parts: Vec<&Vec<Option<String>>> = decoded_writes
        .iter()
        .chain(decoded_deletes.iter())
        .collect();
    lock_key_parts.sort_unstable();
    lock_key_parts.dedup();
    let lock_keys = MatchKeys::of(lock_key_parts);
    let lock_columns = format!("{lock_key_expr}{prior_select}");

    let mut locked = if lock_keys.plain.is_empty() {
        Vec::new()
    } else if arity == 1 {
        // A single bound `text[]` array.
        let pk_ident = &pk_idents[0];
        let pk_cast = plan.pk[0].data_type.as_str();
        let keys: Vec<&str> = lock_keys.plain.iter().map(|p| p[0]).collect();
        lock_single_keys(txn, &target_ident, pk_ident, pk_cast, &lock_columns, &keys).await?
    } else {
        // A composite key is matched against a bound keyset
        // relation instead; see `composite_lock_statement`.
        let arrays = transpose_pk_parts(arity, &lock_keys.plain);
        lock_composite_keys(txn, &target_ident, &plan.pk, &lock_columns, &arrays).await?
    };
    // The keys with a `NULL` part, a statement per pattern, after the
    // others, the patterns in `MatchKeys`' order and each one's keys in key
    // order. That isn't ascending key order over a composite key (`(1,
    // NULL)` locks after `(2, 1)`).
    for (pattern, columns) in &lock_keys.patterns {
        locked.extend(
            super::ledger::query_by_entry_key(
                txn,
                &null_pattern_lock_statement(&target_ident, &plan.pk, pattern, &lock_columns),
                &pattern_params(columns),
            )
            .await?,
        );
    }
    let prior_images: HashMap<String, String> = if prior_image_expr.is_some() {
        locked
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect()
    } else {
        HashMap::new()
    };

    let col_list = pk_idents
        .iter()
        .cloned()
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    let writable: Vec<(&TargetWrite, &Vec<Option<String>>)> =
        plan_writes.iter().zip(decoded_writes.iter()).collect();

    let mut written = Vec::new();
    if !writable.is_empty() {
        // Every one of this target's calculated columns can be paused at
        // once (ADR-0003) — a single-field definition whose lone
        // column's fuse has tripped is the simplest such case. There is then
        // nothing for a conflicting key to update at all: `do update set`
        // with an empty set list is invalid SQL, and an empty-tuple `is
        // distinct from` comparison is too. `do nothing` is also the
        // semantically right behavior, not just the SQL-valid one — an
        // existing row with every column frozen genuinely has no physical
        // change to make; a brand-new key still gets its bare row inserted
        // (frozen at the column defaults) via the same statement's `insert`
        // half.
        let on_conflict = if field_idents.is_empty() {
            format!("on conflict ({pk_col_list}) do nothing")
        } else {
            let set_list = field_idents
                .iter()
                .map(|f| format!("{f} = excluded.{f}"))
                .collect::<Vec<_>>()
                .join(", ");
            let target_cols = field_idents
                .iter()
                .map(|f| format!("{target_ident}.{f}"))
                .collect::<Vec<_>>()
                .join(", ");
            let excluded_cols = field_idents
                .iter()
                .map(|f| format!("excluded.{f}"))
                .collect::<Vec<_>>()
                .join(", ");
            // By text, as the rebuild merges do (ADR-0020): a 1-1 copy holds the
            // source's exact rendering, so `1.25` to `1.2500`, a case change
            // under a nondeterministic collation or an `interval` respelling
            // must write although the type's `=` calls the two equal. A
            // record's text, not a bare `col::text`: a text column cast to
            // text keeps its collation, and the comparison with it.
            format!(
                "on conflict ({pk_col_list}) do update set {set_list} \
                 where row({target_cols})::text is distinct from row({excluded_cols})::text"
            )
        };

        // The composite key's own canonical identity text —
        // what this statement's `RETURNING` reports as each written row's
        // key, matching `ddl::pk_key_sql_expr`'s row-identity encoding at any
        // arity (a bare `{col}::text` at arity 1, byte-identical to before
        // this issue) so a downstream chained definition's live re-fetch
        // parses it back the same way.
        let returning_pk_expr = ddl::pk_key_sql_expr(&plan.pk, None);

        // The fields are evaluated here, over each key's row
        // (`super::one_to_one_render`): one statement per chunk, binding one
        // `text[]` per key column and one of rows, so the parameter count is
        // `O(pk.len())`.
        let mut select_cols: Vec<String> = plan
            .pk
            .iter()
            .enumerate()
            .map(|(j, c)| format!("v.__p{j}::{}", c.data_type))
            .collect();
        select_cols.extend(plan.render.fields.iter().map(|(_, sql)| sql.clone()));
        let unnest_args: Vec<String> = (0..=arity)
            .map(|j| {
                if j == arity {
                    format!("${}::text[]::jsonb[]", j + 1)
                } else {
                    format!("${}::text[]", j + 1)
                }
            })
            .collect();
        let unnest_cols: Vec<String> = (0..arity)
            .map(|j| format!("__p{j}"))
            .chain(std::iter::once("__img".to_string()))
            .collect();
        let sql = format!(
            "insert into {target_ident} ({col_list}) \
             select {} from unnest({}) as v({}) cross join {} \
             {on_conflict} \
             returning {returning_pk_expr} as pk",
            select_cols.join(", "),
            unnest_args.join(", "),
            unnest_cols.join(", "),
            plan.render.row_sql("v.__img"),
        );
        let mut start = 0;
        while start < writable.len() {
            let mut end = start;
            let mut bytes = 0;
            while end < writable.len()
                && (end == start
                    || (end - start < WRITE_CHUNK_ROWS
                        && bytes + writable[end].0.image.len() <= WRITE_CHUNK_BYTES))
            {
                bytes += writable[end].0.image.len();
                end += 1;
            }
            let chunk = &writable[start..end];
            let mut columns: Vec<Vec<Option<&str>>> = vec![Vec::with_capacity(chunk.len()); arity];
            let mut images: Vec<&str> = Vec::with_capacity(chunk.len());
            for (write, pk_parts) in chunk {
                for (j, part) in pk_parts.iter().enumerate() {
                    columns[j].push(part.as_deref());
                }
                images.push(&write.image);
            }
            let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(arity + 1);
            for column in &columns {
                params.push(column);
            }
            params.push(&images);
            let rows = txn.query(&sql, &params).await?;
            written.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
            start = end;
        }
    }

    // A key can appear in both `plan.writes` and `plan.deletes` (e.g. two
    // differently-qualified `src_table` spellings folding to the same
    // catalog source and disagreeing on whether the row is still live —
    // see `compute`'s per-change write/delete dispatch). The old
    // single-CTE-statement form of this function got "the write wins"
    // for free from Postgres's rule that every data-modifying CTE in one
    // WITH sees the same pre-statement snapshot, so a delete could never
    // remove a row its sibling CTE had just inserted. Splitting the write
    // and delete into separate sequential statements (above/below) loses
    // that guarantee — the delete would now run against a snapshot that
    // already includes the write — so it's restored explicitly here
    // instead: never delete a key this same call just wrote. Compared as
    // decoded keys, encoded to one canonical text
    // so a composite key compares as a whole tuple, not accidentally by its
    // first column alone, and a `NULL` part as itself.
    let write_keys: std::collections::HashSet<String> = writable
        .iter()
        .map(|(_, parts)| ddl::encode_pk_key(&plan.pk, parts))
        .collect();

    let mut deleted = Vec::new();
    let delete_keys = MatchKeys::of(
        decoded_deletes
            .iter()
            .filter(|k| !write_keys.contains(&ddl::encode_pk_key(&plan.pk, k))),
    );
    // `t.`-qualified (the composite branch's `exists (...)`
    // subquery below introduces a second relation into scope, so an
    // unqualified column reference would become ambiguous).
    let returning_pk_expr = ddl::pk_key_sql_expr(&plan.pk, Some("t"));
    if !delete_keys.plain.is_empty() {
        let rows = if arity == 1 {
            let pk_ident = &pk_idents[0];
            let pk_cast = plan.pk[0].data_type.as_str();
            let keys: Vec<&str> = delete_keys.plain.iter().map(|p| p[0]).collect();
            delete_single_keys(
                txn,
                &target_ident,
                pk_ident,
                pk_cast,
                &returning_pk_expr,
                &keys,
            )
            .await?
        } else {
            // A composite key has no single column an
            // `= any(...)` array test could name, so the delete instead
            // matches any row whose full key tuple appears in a bound
            // keyset relation (a correlated `exists`, mirroring the
            // pre-lock's join above).
            let arrays = transpose_pk_parts(arity, &delete_keys.plain);
            let params: Vec<&(dyn ToSql + Sync)> = arrays.iter().map(|a| a as _).collect();
            txn.query(
                &format!(
                    "delete from {target_ident} as t \
                     where exists (select 1 from {} where {}) \
                     returning {returning_pk_expr} as pk",
                    pk_keyset_unnest(&plan.pk, 1),
                    pk_keyset_match(&plan.pk, "t"),
                ),
                &params,
            )
            .await?
        };
        deleted.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
    }
    for (pattern, columns) in &delete_keys.patterns {
        let rows = txn
            .query(
                &null_pattern_delete_statement(
                    &target_ident,
                    &plan.pk,
                    pattern,
                    &returning_pk_expr,
                ),
                &pattern_params(columns),
            )
            .await?;
        deleted.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
    }

    // Report every physically-changed key to the seam, with the
    // `hop_gen`/`src_changed` of the change that produced it and the prior
    // image the pre-lock captured (`None` for a key this call created). Keyed
    // by the key text the statements' `RETURNING` renders
    // (`ddl::pk_key_sql_expr`): each record's key is decoded and encoded
    // again (`ddl::encode_pk_key`), so it reaches the lookup in that one
    // canonical form whatever spelling it was staged in. A deleted row's
    // prior image is what lets a downstream aggregate find the group it left
    // (a recompute hint rather than a delta).
    let mut origin_of: HashMap<String, (i32, Provenance)> = HashMap::new();
    for (pk_text, hop_gen, src_changed, origin_lsn) in plan_writes
        .iter()
        .map(|w| (&w.pk_text, w.hop_gen, w.src_changed, w.origin_lsn))
        .chain(
            plan_deletes
                .iter()
                .map(|d| (&d.pk_text, d.hop_gen, d.src_changed, d.origin_lsn)),
        )
    {
        let decoded = decode_target_pk_parts(&plan.pk, target, pk_text)?;
        origin_of.insert(
            ddl::encode_pk_key(&plan.pk, &decoded),
            (hop_gen, (src_changed, origin_lsn)),
        );
    }
    let mut prior_images = prior_images;
    for key in written.iter().chain(deleted.iter()) {
        let (hop_gen, (src_changed, origin_lsn)) =
            origin_of.get(key).copied().unwrap_or((0, (None, None)));
        mutations.record(
            &plan.qualified_target,
            key.clone(),
            prior_images.remove(key),
            hop_gen,
            src_changed,
            origin_lsn,
        );
    }

    let span = tracing::Span::current();
    span.record("written", written.len());
    span.record("deleted", deleted.len());
    Ok((written.len(), deleted.len()))
}

/// A source `TRUNCATE`'s clear of one target (every row of a 1-1 target,
/// every group of an aggregate one): a plain `DELETE FROM <target>`, each
/// removed row's key (in `pk`'s shared key-contract encoding,
/// `ddl::pk_key_sql_expr` — for an aggregate target its `GROUP BY`
/// columns) and, when something reads
/// the target, its image reported to `mutations` as the key's prior image.
/// Returns how many rows it deleted.
pub(super) async fn clear_target(
    txn: &Transaction<'_>,
    qualified_target: &str,
    pk: &[PrimaryKeyColumn],
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
    mutations: &mut TargetMutations,
) -> Result<usize, ApplyError> {
    let pk_key_expr = ddl::pk_key_sql_expr(pk, Some("t"));
    let target_ident = ddl::qualified_target_table_ident(qualified_target);
    let image_expr = mutations.image_sql(txn, qualified_target, "t").await?;
    let image_select = match &image_expr {
        Some(expr) => format!(", ({expr})::text"),
        None => String::new(),
    };
    let rows = txn
        .query(
            &format!("delete from {target_ident} as t returning {pk_key_expr}{image_select}"),
            &[],
        )
        .await?;
    let cleared = rows.len();
    for row in rows {
        let prior = image_expr.as_ref().map(|_| row.get::<_, String>(1));
        mutations.record(
            qualified_target,
            row.get(0),
            prior,
            hop_gen,
            src_changed,
            origin_lsn,
        );
    }
    Ok(cleared)
}

/// Phase 3 (design doc: "apply ∪ mark-drained are one transaction"). Given
/// `plan` (Phase 2's output) and the claim it belongs to, runs, inside
/// `txn`:
///
/// 1. **The version fence**: `FOR SHARE`s every source table `plan`
///    evaluated against and compares its current version to the one loaded
///    at compute time. A mismatch means a definition changed mid-drain;
///    this returns [`ApplyError::VersionFenceMiss`] without writing
///    anything, for [`drain_once`] to retry against the reloaded catalog.
/// 2. **Truncate clears**, one plain `DELETE FROM <target>` per
///    [`ApplyPlan::clears`] entry, run *before* that target's own ordered
///    pre-lock + upsert/delete below: a truncate-bearing batch always seals
///    with `bucket_count = 1` (`seal::seal_phase2`) and is drained under
///    `next_claimable_segment`'s barrier (no predecessor or successor
///    segment concurrently draining the same target), so within this one
///    transaction "clear, then write" is exactly what makes a same-batch
///    post-truncate insert (already computed into `plan.targets` by
///    [`compute`]) survive, while anything the truncate is meant to erase
///    does not.
/// 3. **The 1-1 targets on their ledgers** ([`settle_one_to_one_target`],
///    the sorted entry lock, the Re-derive read, the entries'
///    ADR-0002 I2 test, then the ordered pre-lock + upsert/delete of only
///    the keys it changed ([`apply_target`]).
/// 4. **Downstream propagation**: every key a step above physically changed
///    (write or delete — no-op-suppressed writes don't count) was reported to
///    a [`TargetMutations`]; for every such target some `live`
///    definition reads, it stages an image-less `Recompute` at `hop_gen + 1`
///    carrying the key's prior image, enforcing [`MAX_HOP_GEN`] first. See
///    `staging::target_mutations` for why this seam, not CDC, is how a
///    chained hop hears about its upstream target. A definition created
///    between Phase 2 and this commit that starts reading a target is not
///    missed: its build derives it from current target state.
/// 5. **The completion statement**: deletes this claim's `seg_claims` rows
///    and ORs their buckets into `segments.drained_mask`, flipping
///    `state` to `'drained'` once every bucket has drained — one statement,
///    so "this claim released" and "its buckets marked drained" can never
///    observably happen one without the other. Empty `DELETE ... RETURNING`
///    means the claim was already gone — [`ApplyError::ClaimLost`].
/// 6. A `pg_notify` on `wake_channel`, for anything awaiting convergence.
///
/// A thin single-segment wrapper over [`apply_and_mark_drained_many`] —
/// every step below is shared verbatim with the multi-segment path; this
/// function exists only to keep the single-segment public signature (and
/// the every-`drain_once`-drains-exactly-one-segment contract every
/// existing caller and test relies on).
pub async fn apply_and_mark_drained(
    txn: &Transaction<'_>,
    seg_seq: i64,
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
) -> Result<ApplyOutcome, ApplyError> {
    let outcome =
        apply_and_mark_drained_many(txn, &[seg_seq], claimed_by, plan, wake_channel).await?;
    Ok(ApplyOutcome {
        keys_written: outcome.keys_written,
        keys_deleted: outcome.keys_deleted,
        batch_drained: outcome.segments_drained[0].1,
        pages: outcome.pages,
    })
}

/// The [`apply_and_mark_drained`] steps generalized over `seg_seqs` — the
/// segment-coalescing seam. `plan` (Phase 2's output) was
/// computed once over every coalesced segment's *merged* folded changes
/// ([`super::fold::merge_folded_changes`]), so steps 1-4 below (version
/// fence, truncate clears, ordered writes, downstream propagation) already
/// run exactly once for the whole batch — that sharing *is* the milestone's
/// win, collapsing what used to be one such pass per sealed segment into
/// one pass for however many sealed segments this call coalesces. Only step
/// 5 (completion) is inherently per-segment: each `seg_seq` in `seg_seqs`
/// has its own `seg_claims` rows and its own `drained_mask`, so "this
/// claim's buckets are drained" must still be recorded once per segment,
/// all in this same transaction — the one place this function's cost still
/// scales with segment count, and it is O(1) per segment (no source-table
/// work), unlike the passes above it.
///
/// `seg_seqs` must be the segments this call actually holds at least one
/// claimed bucket on (never a segment this worker claimed nothing from —
/// see [`drain_many`]'s `owned` filtering), and, since [`ApplyPlan::versions`]
/// etc. are shared across all of them, must never mix a truncate-bearing
/// segment with any other (see [`next_claimable_segments`]'s barrier).
///
/// ADR-0009 decision 3: the batch-level span in the propagation
/// tree's apply phase — parent of every [`apply_target`]/ledger apply span
/// this call makes (one per
/// consuming transform), since each of those runs inside this async fn's own
/// `#[tracing::instrument]`-created span.
pub async fn apply_and_mark_drained_many(
    txn: &Transaction<'_>,
    seg_seqs: &[i64],
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
) -> Result<ManyApplyOutcome, ApplyError> {
    let steps: Vec<SegmentStep> = seg_seqs
        .iter()
        .map(|&seg_seq| SegmentStep {
            seg_seq,
            page: None,
        })
        .collect();
    apply_page(txn, &steps, claimed_by, plan, wake_channel).await
}

/// How one segment's claim ends a Phase 3 transaction.
#[derive(Debug, Clone)]
pub(crate) struct SegmentStep {
    pub(crate) seg_seq: i64,
    /// `None`: the pre-paging contract [`apply_and_mark_drained_many`] keeps
    /// for its direct callers (the quarantine probe, tests): complete whatever
    /// buckets `claimed_by` holds, [`ApplyError::ClaimLost`] only if it holds
    /// none. The drain itself always passes `Some`.
    pub(crate) page: Option<PageClaim>,
}

/// The buckets one page covers, and whether it is their last.
#[derive(Debug, Clone)]
pub(crate) struct PageClaim {
    /// The buckets this page's records came from.
    pub(crate) buckets: Vec<i16>,
    /// Every bucket `claimed_by` still holds on the segment: `buckets`, plus
    /// any it holds under a different cursor and pages separately. The claim
    /// check must find all of them, or the page is `ClaimLost`.
    pub(crate) held: Vec<i16>,
    /// `Some(k)`: more pages follow, so the transaction heartbeats the claim
    /// and advances `buckets`' cursors to `k`. `None`: this page ends
    /// `buckets`' share, so the transaction releases them and ORs them into
    /// `drained_mask`, as the unpaged drain always did.
    pub(crate) next: Option<fold::PageKey>,
}

/// Takes the page's version fences (`versions`, the fence's read set) `for
/// share`, its transaction's first locks, and fails with
/// [`ApplyError::VersionFenceMiss`] if any moved since the page read it.
///
/// It takes them in table-name order (`versions` is ordered), the order a
/// define, drop, resume or freeze bumps several fences in. A
/// `JOIN` target's page holds both its tables' fences, and a `JOIN` define
/// bumps both: a page that held the second while it queued on the first,
/// behind a define that held the first, would close a cycle with it.
///
/// `source_key` is the canonical (qualified, where resolvable) source
/// identity `compute` keyed `versions` on, and
/// `source_table_versions.source_table` is qualified, so this
/// matches exactly — see `defs::source_table_version`, which reads the same
/// row the same way.
async fn hold_version_fences(
    txn: &Transaction<'_>,
    versions: &BTreeMap<String, Option<i64>>,
) -> Result<(), ApplyError> {
    for (source_key, loaded_version) in versions {
        let row = txn
            .query_opt(
                "select version from source_table_versions \
                 where source_table = $1 for share",
                &[source_key],
            )
            .await?;
        let current: Option<i64> = row.map(|r| r.get(0));
        if current != *loaded_version {
            crate::instance_log::debug!(
                src_table = %source_key,
                "version fence miss: source table's definitions changed mid-drain"
            );
            return Err(ApplyError::VersionFenceMiss {
                src_table: source_key.clone(),
            });
        }
    }
    Ok(())
}

/// [`apply_and_mark_drained_many`] with an explicit claim ending per segment:
/// a non-final page's transaction applies, checks the claim
/// and advances the cursor; a final page's applies and completes. Everything
/// before step 5 is the same either way.
#[tracing::instrument(
    name = "staging.apply_and_mark_drained",
    skip(txn, steps, plan, wake_channel),
    fields(
        segments = steps.len(),
        targets = plan.targets.len(),
        keys_written = tracing::field::Empty,
        keys_deleted = tracing::field::Empty,
    )
)]
pub(crate) async fn apply_page(
    txn: &Transaction<'_>,
    steps: &[SegmentStep],
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
) -> Result<ManyApplyOutcome, ApplyError> {
    // 1. Version fence.
    hold_version_fences(txn, &plan.versions).await?;

    // 1b. Park this batch's own folded contribution for every
    // already-poisoned key it's excluding, before the drained mark below —
    // "parked work is the source of truth" means every excluding batch must
    // do this itself, in the same transaction, not just the batch that
    // caused the eviction. See `quarantine::park_batch_contribution`'s doc
    // comment for why this runs unconditionally rather than only on the
    // batch that tripped the threshold. Attributed to the lowest (earliest)
    // of the coalesced segments — `poison_held`'s `seg_seq` is audit
    // bookkeeping ("which batch's contribution is this"), not something
    // later correctness depends on picking exactly right among several
    // equally-valid coalesced segments.
    quarantine::park_batch_contribution(txn, steps[0].seg_seq, &plan.poisoned_park).await?;

    let mut keys_written = 0usize;
    let mut keys_deleted = 0usize;
    // Every target write below reports its physically-changed
    // keys here, and step 4 turns them into downstream `Recompute` rows. See
    // `staging::target_mutations` — no writer hands its keys back to this
    // function, so none can skip propagation.
    let mut mutations = TargetMutations::new();

    // 2. Truncate clears, before this target's own upsert/delete below —
    // see this function's doc comment on why "clear, then write" is safe
    // here specifically (single-bucket batch, barrier-drained).
    for clear in plan.clears.values() {
        one_to_one_ledger::truncate(
            txn,
            &one_to_one_ledger::LedgerRef::of_target(&clear.qualified_target),
            clear.truncate_lsn,
        )
        .await?;
        keys_deleted += clear_target(
            txn,
            &clear.qualified_target,
            &clear.pk,
            clear.hop_gen,
            clear.src_changed,
            clear.origin_lsn,
            &mut mutations,
        )
        .await?;
    }

    // 2b. Aggregate truncate clears: every group of an aggregate target
    // whose source was truncated, the same `clear_target` as step 2 over the
    // target's `GROUP BY` key (each cleared group reaches the
    // seam, so it propagates).
    for clear in plan.aggregate_clears.values() {
        // A group-basis target's truncate empties its hidden table and
        // raises its truncate floor (the D split's Q6), then clears its
        // groups.
        match &clear.state {
            AggregateState::GroupBasis { hidden } => {
                super::group_basis::truncate_groups(
                    txn,
                    hidden,
                    &clear.qualified_target,
                    clear.truncate_lsn,
                )
                .await?;
            }
            AggregateState::Neither => {}
        }
        keys_deleted += clear_target(
            txn,
            &clear.qualified_target,
            &clear.pk,
            clear.hop_gen,
            clear.src_changed,
            clear.origin_lsn,
            &mut mutations,
        )
        .await?;
    }

    // 3. The 1-1 targets on their ledgers, one at a time in
    // target order, so every page and every direct writer takes their
    // entry locks in one order: see `settle_one_to_one_target`.
    let page_seg = steps.iter().map(|step| step.seg_seq).max().unwrap_or(0);
    let page_first_seg = steps.iter().map(|step| step.seg_seq).min().unwrap_or(0);
    for (target, target_plan) in &plan.targets {
        let (written, deleted) = settle_one_to_one_target(
            txn,
            target,
            target_plan,
            page_first_seg,
            page_seg,
            &mut mutations,
        )
        .await?;
        keys_written += written;
        keys_deleted += deleted;
    }

    // 3b. Aggregate targets on the group basis, in target order.
    for group_plan in plan.group_targets.values() {
        let (written, deleted) =
            super::group_basis::apply_group_target(txn, group_plan, &mut mutations).await?;
        keys_written += written;
        keys_deleted += deleted;
    }

    // 3c. `JOIN` targets, in target order.
    for join_plan in plan.join_targets.values() {
        let (written, deleted) =
            super::join::apply_join_target(txn, join_plan, &mut mutations).await?;
        keys_written += written;
        keys_deleted += deleted;
    }

    // 4. Downstream propagation, with the hop bound checked before staging
    // anything. Every key a write above physically changed, for
    // every target some `live` definition reads, becomes one image-less
    // `Recompute` at `hop_gen + 1` carrying the key's prior image — see
    // `staging::target_mutations`. `lsn: None`, like every other row this
    // step stages: a propagated hop has no source LSN of its own. (A target
    // the seam feeds images for gets a CDC-shaped row with the write token
    // as its `lsn` instead.)
    let propagation = mutations.into_staged(txn).await?;
    let recompute_changes = propagation.changes;
    let mut hop_bound_tables = propagation.hop_bound_tables;
    let worst_hop_gen = propagation.worst_hop_gen;

    if !hop_bound_tables.is_empty() {
        hop_bound_tables.sort();
        hop_bound_tables.dedup();
        // Debug: the drain's halt logs the closure it pauses for it.
        crate::instance_log::debug!(
            hop_gen = worst_hop_gen,
            tables = ?hop_bound_tables,
            "downstream propagation exceeded the hop bound; a wave may have run away"
        );
        return Err(ApplyError::HopBoundExceeded {
            hop_gen: worst_hop_gen,
            tables: hop_bound_tables,
        });
    }

    if !recompute_changes.is_empty() {
        crate::instance_log::debug!(
            count = recompute_changes.len(),
            "staged downstream recomputes from this batch's physically-changed keys"
        );
    }
    append::append(txn, &recompute_changes).await?;

    // 4b. A clean drain clears the death counters for every key
    // it just applied (not the poisoned ones it parked above) — doc 06's
    // "clean drain clears counters for keys it applied."
    let retried = quarantine::clear_key_deaths(txn, &plan.applied_keys).await?;
    plan.retries_succeeded
        .store(retried, std::sync::atomic::Ordering::Relaxed);

    // 5. Completion: release each coalesced segment's claim and mark its
    // buckets drained, in one statement per segment — inherently per-segment
    // (each has its own `seg_claims` rows and `drained_mask`), unlike steps
    // 1-4 above, which already ran once for the whole coalesced batch.
    //
    // A page that isn't its buckets' last instead checks the
    // claim and advances their cursor. See `end_segment_step`.
    let mut segments_drained = Vec::with_capacity(steps.len());
    for step in steps {
        let batch_drained = end_segment_step(txn, step, claimed_by).await?;
        // The page commits, so its holdup goes with this transaction.
        super::holdup::clear(txn, step).await?;
        segments_drained.push((step.seg_seq, batch_drained));
    }

    // 6. Wake anything awaiting convergence.
    txn.execute("select pg_notify($1, '')", &[&wake_channel])
        .await?;

    // Every DML statement this Phase 3 pass will ever issue
    // against `txn` has now run (including the completion statement above,
    // which is what actually marks this claim's buckets drained, and the
    // `pg_notify` just above, whose payload Postgres won't actually deliver
    // until commit) — nothing below this point writes anything. This is
    // "moves computed, not yet persisted": the caller (`drain_once`/
    // `drain_many`) commits `txn` immediately after this call returns, and
    // not one statement earlier. See `pause_before_commit_for_tests`'s own
    // doc comment for why a test-only hook sits exactly here. Compiled out
    // entirely (call site included) unless this crate is built for tests or
    // with `test-util` — see that function's doc comment.
    #[cfg(any(test, feature = "test-util"))]
    pause_before_commit_for_tests().await;
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    for target in plan
        .targets
        .values()
        .map(|t| &t.qualified_target)
        .chain(plan.group_targets.values().map(|t| &t.target))
        .chain(plan.join_targets.values().map(|t| &t.target))
    {
        super::interleave::pause_at(txn, super::interleave::PausePoint::BeforeCommit, target)
            .await?;
    }

    let span = tracing::Span::current();
    span.record("keys_written", keys_written);
    span.record("keys_deleted", keys_deleted);
    Ok(ManyApplyOutcome {
        keys_written,
        keys_deleted,
        segments_drained,
        pages: 1,
    })
}

/// Step 5 of [`apply_page`] for one segment: returns whether this
/// transaction flipped the segment to `'drained'`.
///
/// **A non-final page** runs the claim check, then advances
/// the cursor:
///
/// - `update seg_claims set claimed_at = clock_timestamp() ... returning
///   bucket` must return every bucket the worker still holds, or the page is
///   [`ApplyError::ClaimLost`] and its whole transaction rolls back. The row
///   lock this takes is what makes `RECLAIM_STALE_SQL`'s `skip locked` pass
///   over an in-flight page, and the update doubles as a heartbeat. It stamps
///   the clock, not `now()`: `now()` is the transaction's start, so a page
///   whose apply ran for 20 s would commit a claim already 20 s old,
///   overwriting the daemon's fresher refresh from mid-page. A
///   reclaim that committed first deleted the row, so the update misses it; a
///   reclaim still in flight holds the row lock, so the update waits and then
///   misses it. Either way a stale claimant's page never commits, so no
///   delta applies twice.
/// - The page's buckets' `drain_cursor` rows move to the page's last key, in
///   this same transaction, so a committed cursor always means "applied
///   through here".
///
/// **A final page** runs the completion statement: delete the page's buckets'
/// claims and OR them into `drained_mask`, flipping the segment to
/// `'drained'` once every bucket is in. The delete must return every one of
/// the page's buckets (and, when the worker still holds other buckets it
/// pages separately, the heartbeat above checks those too). A bucket's
/// `drained_mask` bit is set only here, on its last page.
async fn end_segment_step(
    txn: &Transaction<'_>,
    step: &SegmentStep,
    claimed_by: &str,
) -> Result<bool, ApplyError> {
    let seg_seq = step.seg_seq;
    let claim_lost = |detail: &str| {
        crate::instance_log::warn!(
            seg_seq,
            claimed_by = %claimed_by,
            detail,
            "claim was gone by completion time; nothing applied twice, but its buckets \
             must be reclaimed by whoever holds them now"
        );
        ApplyError::ClaimLost
    };

    if let Some(page) = &step.page
        && (page.next.is_some() || page.held.len() != page.buckets.len())
    {
        let refreshed: std::collections::HashSet<i16> = txn
            .query(
                "update seg_claims set claimed_at = clock_timestamp() \
                 where seg_seq = $1 and claimed_by = $2 returning bucket",
                &[&seg_seq, &claimed_by],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if !page.held.iter().all(|bucket| refreshed.contains(bucket)) {
            return Err(claim_lost("a held bucket's claim is gone"));
        }
        if let Some(after) = &page.next {
            // A retry of a page whose `COMMIT` landed but whose reply
            // was lost. The claim update above waited for that commit, so
            // this statement sees its cursor.
            let at_this_page: i64 = txn
                .query_one(
                    "select count(*) from drain_cursor \
                     where seg_seq = $1 and bucket = any($2::smallint[]) \
                       and after_route = $3 and after_src_table = $4 and after_key = $5",
                    &[
                        &seg_seq,
                        &page.buckets,
                        &after.route,
                        &after.src_table,
                        &after.key,
                    ],
                )
                .await?
                .get(0);
            if at_this_page == page.buckets.len() as i64 {
                return Err(ApplyError::PageAlreadyCommitted);
            }
            txn.execute(
                "insert into drain_cursor \
                     (seg_seq, bucket, after_route, after_src_table, after_key) \
                 select $1, bucket, $3, $4, $5 from unnest($2::smallint[]) as bucket \
                 on conflict (seg_seq, bucket) do update \
                 set after_route = excluded.after_route, \
                     after_src_table = excluded.after_src_table, \
                     after_key = excluded.after_key",
                &[
                    &seg_seq,
                    &page.buckets,
                    &after.route,
                    &after.src_table,
                    &after.key,
                ],
            )
            .await?;
            return Ok(false);
        }
    }

    // Lock the segment row before deleting any of its claims: the lock order
    // `claim::claim` documents. Deleting first, then waiting
    // here on a claim that holds the row, deadlocked with that claim's
    // insert waiting on this delete.
    let bucket_count: i16 = txn
        .query_one(
            "select bucket_count from segments where seg_seq = $1 for no key update",
            &[&seg_seq],
        )
        .await?
        .get(0);

    let claimed_buckets: Vec<i16> = match &step.page {
        None => txn
            .query(
                "delete from seg_claims where seg_seq = $1 and claimed_by = $2 returning bucket",
                &[&seg_seq, &claimed_by],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect(),
        Some(page) => txn
            .query(
                "delete from seg_claims \
                 where seg_seq = $1 and claimed_by = $2 and bucket = any($3::smallint[]) \
                 returning bucket",
                &[&seg_seq, &claimed_by, &page.buckets],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect(),
    };

    if claimed_buckets.is_empty() {
        return Err(claim_lost("no claim left to complete"));
    }
    if let Some(page) = &step.page
        && !page
            .buckets
            .iter()
            .all(|bucket| claimed_buckets.contains(bucket))
    {
        return Err(claim_lost("one of the page's buckets' claim is gone"));
    }

    let mut mask: i64 = 0;
    for bucket in &claimed_buckets {
        mask |= 1i64 << bucket;
    }
    let full_mask: i64 = (1i64 << bucket_count) - 1;

    let completed = txn
        .query_opt(
            "update segments \
             set drained_mask = drained_mask | $2::bigint, \
                 state = case when (drained_mask | $2::bigint) = $3::bigint \
                              then 'drained' else state end \
             where seg_seq = $1 and state = 'draining' \
             returning state",
            &[&seg_seq, &mask, &full_mask],
        )
        .await?;
    Ok(matches!(
        completed.map(|row| row.get::<_, String>(0)),
        Some(state) if state == "drained"
    ))
}

/// The hook for a genuine `SIGKILL`-mid-drain test (not an in-process
/// simulation): a no-op unless the environment names a trigger file, in
/// which case — only once that file actually exists — this blocks forever
/// right after every write [`apply_and_mark_drained_many`]'s Phase 3 pass
/// makes and right before its caller commits `txn`.
///
/// Two env vars, both read fresh on every call (cheap; this only ever runs
/// under `cfg(any(test, feature = "test-util"))` — see this crate's
/// `Cargo.toml` `test-util` feature doc comment):
///
/// - `TRELLIS_TEST_PAUSE_TRIGGER`: a path. If unset, or if the path doesn't
///   exist yet, this returns immediately — an ordinary, unpaused commit.
///   Checking *existence* (rather than gating on the env var alone) is what
///   lets a long-running subprocess engine pause on-demand: the env var is
///   fixed for the process's whole lifetime, but a test can create this file
///   at exactly the moment it wants the *next* Phase 3 commit — and only
///   that one — to pause, letting every earlier commit (schema setup,
///   seeding) proceed normally.
/// - `TRELLIS_TEST_PAUSE_MARKER`: a path this touches right before parking,
///   once the trigger above has fired — so the test, polling for this file
///   (`generative::backend::subprocess::SubprocessBackend::wait_for_pause`,
///   the same existence-polling idea as `testkit::crash::wait_until`, just
///   async), can observe "now paused, transaction open, not yet committed"
///   deterministically instead of guessing with a sleep before sending
///   `SIGKILL`.
///
/// Blocking here is sound specifically because Phase 3 is one transaction
/// (this module's own doc comment, "apply ∪ mark-drained is one
/// transaction"): every statement this pass issued against `txn` is still
/// uncommitted, so a `SIGKILL` landing anywhere inside this pause drops the
/// connection and Postgres rolls the whole batch back — nothing partially
/// applied, nothing "half-drained." A fresh engine's next drain of the same
/// (still-`'draining'`, still-claimed-until-`reclaim_ttl`-or-liveness-catches-it)
/// segment redoes Phase 2 and Phase 3 in full, which is exactly the
/// atomicity `generative::backend::subprocess::SubprocessBackend`'s
/// SIGKILL-mid-Phase-3 regression test exists to prove holds across a real
/// crash, not just a simulated one.
#[cfg(any(test, feature = "test-util"))]
async fn pause_before_commit_for_tests() {
    let Ok(trigger_path) = std::env::var("TRELLIS_TEST_PAUSE_TRIGGER") else {
        return;
    };
    if !std::path::Path::new(&trigger_path).exists() {
        return;
    }
    crate::instance_log::warn!(
        trigger_path,
        "TRELLIS_TEST_PAUSE_TRIGGER fired: pausing Phase 3 indefinitely before commit \
         (test-only hook)"
    );
    if let Ok(marker_path) = std::env::var("TRELLIS_TEST_PAUSE_MARKER")
        && let Err(err) = std::fs::write(&marker_path, b"paused")
    {
        crate::instance_log::warn!(?err, marker_path, "failed to write Phase 3 pause marker");
    }
    // Never resolves on its own: the only way out is an external kill (the
    // intended path) or the process exiting some other way (e.g. the test
    // binary itself tearing down without ever arming the trigger, which
    // never reaches this branch in the first place).
    std::future::pending::<()>().await;
}

/// What one successful [`apply_and_mark_drained`] call did: how many target
/// rows it physically wrote/deleted (no-op-suppressed writes excluded), and
/// whether this call's completion flipped the segment to `'drained'`
/// (`false` if other buckets are still outstanding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub batch_drained: bool,
    /// See [`ManyApplyOutcome::pages`].
    pub pages: usize,
}

/// What one successful [`apply_and_mark_drained_many`] call did — the
/// coalesced-segment counterpart to [`ApplyOutcome`]:
/// the same physically-written/deleted key counts, now totalled across
/// every segment this call drained from, plus each individual segment's own
/// `(seg_seq, batch_drained)` completion result — a coalesced call can flip
/// some of its segments to `'drained'` while leaving others still short a
/// peer's bucket, exactly as any one of them would on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManyApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub segments_drained: Vec<(i64, bool)>,
    /// How many compute-and-apply transactions (pages) the drain
    /// committed. 1 when the share fit `drain_batch_cap`; at most
    /// ⌈share / cap⌉ when it didn't (a page can come out short, never over).
    /// One Phase 3 transaction is one page.
    pub pages: usize,
}

// ---------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------

/// The most drain attempts one page's compute-and-apply retries before giving
/// up and surfacing the last error — a bound on a fence-miss/serialization-
/// failure loop that never resolves (e.g. a source under constant, colliding
/// definition churn), rather than retrying forever.
const MAX_APPLY_ATTEMPTS: u32 = 5;

/// How long one page keeps retrying lock timeouts (ADR-0002 I7)
/// before [`drain_batch`] surfaces the last one and the worker releases the
/// claim. Each retry holds no transaction; the bound is on how long a worker
/// sits on one page (and how long shutdown can wait for it), not on any
/// snapshot. Three [`crate::locks::LOCK_TIMEOUT`]s: a page gets a few
/// retries before it gives up, however long the timeout is.
const LOCK_RETRY_BUDGET: Duration = crate::locks::LOCK_TIMEOUT.saturating_mul(3);

/// The first wait after a page's transaction hits `lock_timeout`.
const LOCK_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(50);

/// The longest wait between two of a page's retries after a transient
/// failure.
const TRANSIENT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

/// [`drain_batch`]'s state for retrying transient failures:
/// a backoff that doubles on each consecutive transient failure, and when
/// the page's first lock timeout happened, for [`LOCK_RETRY_BUDGET`].
#[derive(Debug, Default)]
struct TransientRetry {
    /// The delay the next transient failure waits, once one has happened.
    next: Option<Duration>,
    /// When this page first hit `lock_timeout`.
    first_lock_timeout: Option<std::time::Instant>,
}

impl TransientRetry {
    fn new() -> Self {
        Self::default()
    }

    /// The wait before the next retry: [`LOCK_RETRY_INITIAL_DELAY`], then
    /// doubling to [`TRANSIENT_RETRY_MAX_DELAY`].
    fn next_delay(&mut self) -> Duration {
        let delay = self.next.unwrap_or(LOCK_RETRY_INITIAL_DELAY);
        self.next = Some((delay * 2).min(TRANSIENT_RETRY_MAX_DELAY));
        delay
    }

    /// How long ago this page first hit `lock_timeout`, starting the clock
    /// if this is the first time.
    fn lock_waited(&mut self) -> Duration {
        self.first_lock_timeout
            .get_or_insert_with(std::time::Instant::now)
            .elapsed()
    }
}

/// [`crate::client::ClientOptions::drain_batch_cap`]'s default: the most
/// folded records one drain batch holds at once (ADR-0002). A
/// worker's peak memory is about this many changes' worth, whatever size the
/// segment it drains grew to: ~0.8 GB across 8 workers at ~1 KB per
/// image-less change, ~3.2 GB at ~4 KB per image-bearing one.
pub const DEFAULT_DRAIN_BATCH_CAP: usize = 100_000;

/// Test seams for [`drain_many_with_hooks`]: the
/// exactly-once tests need a drainer stopped between two pages, and a
/// drainer whose claim is taken away mid-page, without timing anything.
/// Both default to off, which is what every production drain passes.
#[derive(Debug, Default)]
pub struct DrainHooks {
    /// Return right after this many pages have committed, leaving the claim
    /// held and the cursor where the last page left it: a drainer that died
    /// between two pages.
    pub stop_after_pages: Option<usize>,
    /// Before page `n` (1-based) computes, send on the first channel and
    /// wait on the second: the test does whatever it wants to the claim in
    /// between (age it, reclaim it, claim it for someone else).
    pub pause_before_page: Option<(
        usize,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
    /// Once page `n` (1-based) commits, end its session before the drain
    /// sees the commit succeed: a connection that drops after its COMMIT
    /// landed. The drain sees only the dropped connection, so it retries
    /// the committed page. Taken when it fires.
    pub lose_commit_reply_on_page: Option<usize>,
}

/// Runs one full drain of `seg_seq`: see [`drain_many`], which this is the
/// single-segment form of. Returns `Ok(None)` if this call's claim won (and
/// already owned) nothing.
///
/// ADR-0009 decision 3: the outermost span in the propagation
/// tree's apply phase — parent, across however many retries and pages this
/// call takes, of every [`compute`]/[`apply_and_mark_drained`] span (and,
/// through those, every per-transform [`apply_target`] span). `attempt` is
/// recorded once per loop iteration, so its final exported value is however
/// many attempts the last page took.
#[tracing::instrument(
    name = "staging.drain_once",
    skip(pool, wake_channel),
    fields(claimed_by = %claimed_by, attempt = tracing::field::Empty)
)]
#[cfg(any(test, feature = "internals"))]
pub async fn drain_once(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
) -> Result<Option<ApplyOutcome>, ApplyError> {
    let outcome = drain_segments(
        pool,
        &[seg_seq],
        claimed_by,
        live_workers,
        wake_channel,
        DEFAULT_DRAIN_BATCH_CAP,
        &mut DrainHooks::default(),
    )
    .await?;
    Ok(outcome.map(|outcome| ApplyOutcome {
        keys_written: outcome.keys_written,
        keys_deleted: outcome.keys_deleted,
        batch_drained: outcome
            .segments_drained
            .first()
            .is_some_and(|&(_, drained)| drained),
        pages: outcome.pages,
    }))
}

/// Drains this worker's share of `seg_seqs` with the default
/// [`DEFAULT_DRAIN_BATCH_CAP`]: see [`drain_many_with_cap`].
#[cfg(any(test, feature = "internals"))]
pub async fn drain_many(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    drain_many_with_cap(
        pool,
        seg_seqs,
        claimed_by,
        live_workers,
        wake_channel,
        DEFAULT_DRAIN_BATCH_CAP,
    )
    .await
}

/// Claims this worker's share of `seg_seqs` and drains it, holding at most
/// `drain_batch_cap` folded records at a time.
///
/// **Phase 1, the claim**, is `CLAIM_SQL` per segment and a commit, nothing
/// else. The fold used to share the claim's transaction, so every peer's
/// `CLAIM_SQL` queued on the uncommitted `sealed -> draining` flip and
/// `seg_claims` rows for the whole fold. The fenced window is immutable
/// once fenced, so the fold needs no shared snapshot with the claim.
///
/// **The direct path.** When no held bucket has a `drain_cursor` row and the
/// share, estimated from `segments.row_count` (rows over buckets, times the
/// buckets held), fits the cap, each segment folds in full, with a `limit cap
/// + 1` guard, and the merged records run one compute-and-apply pass whose
/// transaction completes every held bucket: the pre-paging drain, one page.
/// [`next_claimable_segments`] only coalesces segments whose row counts sum
/// under the cap, so a coalesced batch takes this path.
///
/// **The paged path.** Otherwise (the estimate is over the cap, the guard
/// tripped, or a bucket has a cursor from an earlier claimant), the first
/// segment drains alone and any other is released for a later call. Its held
/// buckets are grouped by cursor (all at the start, normally) and each group
/// is folded once, from its cursor, into a session `TEMP` table on an
/// unpooled connection ([`super::page::MaterializedPages`]), then walked in
/// pages of at most `drain_batch_cap` records, keyset-ordered on
/// [`fold::PageKey`] with the truncate sentinel first. Each page is its own
/// compute-and-apply transaction: a non-final page checks the claim and
/// advances the cursor, the final page completes (see [`end_segment_step`]).
/// Quarantine isolation and the fuse see one page at a time. A page that
/// fails surfaces its error like any drain failure; the caller releases the
/// claim, and the next claimant resumes at that page.
///
/// Returns `Ok(None)` if this call's claims won nothing at all.
///
/// ADR-0009 decision 3: [`drain_once`]'s doc comment describes the
/// span this creates — same role, just parenting a coalesced batch's spans
/// instead of a single segment's.
#[tracing::instrument(
    name = "staging.drain_many",
    skip(pool, wake_channel),
    fields(
        claimed_by = %claimed_by,
        segments = seg_seqs.len(),
        attempt = tracing::field::Empty,
    )
)]
pub async fn drain_many_with_cap(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    drain_batch_cap: usize,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    drain_segments(
        pool,
        seg_seqs,
        claimed_by,
        live_workers,
        wake_channel,
        drain_batch_cap,
        &mut DrainHooks::default(),
    )
    .await
}

/// [`drain_many_with_cap`] with [`DrainHooks`], for the exactly-once tests.
#[cfg(any(test, feature = "internals"))]
pub async fn drain_many_with_hooks(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    drain_batch_cap: usize,
    hooks: &mut DrainHooks,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    drain_segments(
        pool,
        seg_seqs,
        claimed_by,
        live_workers,
        wake_channel,
        drain_batch_cap,
        hooks,
    )
    .await
}

impl ManyApplyOutcome {
    /// Folds one page's outcome into a running total across a paged drain.
    fn absorb(&mut self, page: ManyApplyOutcome) {
        self.keys_written += page.keys_written;
        self.keys_deleted += page.keys_deleted;
        for (seg_seq, drained) in page.segments_drained {
            match self
                .segments_drained
                .iter_mut()
                .find(|(s, _)| *s == seg_seq)
            {
                Some(entry) => entry.1 = drained,
                None => self.segments_drained.push((seg_seq, drained)),
            }
        }
        self.pages += page.pages;
    }
}

/// The body [`drain_once`], [`drain_many_with_cap`] and
/// [`drain_many_with_hooks`] share; see [`drain_many_with_cap`].
async fn drain_segments(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    drain_batch_cap: usize,
    hooks: &mut DrainHooks,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    let cap = drain_batch_cap.max(1);
    if seg_seqs.is_empty() {
        return Ok(None);
    }

    // Phase 1: the claim, committed on its own. Segments are claimed in
    // order until one would take the batch past the cap: a share that must
    // page (over the cap, or resuming a cursor) drains alone, so it either
    // comes first and ends the batch, or its claim is undone in this same
    // transaction and a later call takes it. A segment whose buckets peers
    // already hold costs nothing here, so a later segment still coalesces
    // behind it.
    let held: Vec<HeldShare> = {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let mut held: Vec<HeldShare> = Vec::with_capacity(seg_seqs.len());
        let mut estimated: i64 = 0;
        for &seg_seq in seg_seqs {
            // Never commit a claim on a segment whose fence isn't published
            // yet (seal phase 1 done, phase 2 not): `CLAIM_SQL`'s `sealed ->
            // draining` flip would then commit, and phase 2 (and the stuck-seal
            // recovery) only ever fence a segment still `sealed`, so it would
            // stay unfenced, and undrainable, for good. The fold used to share
            // this transaction and fail on the missing fence, rolling the flip
            // back; now the claim commits first, so the check comes first. A
            // fence only ever goes from absent to present, so a segment fenced
            // here stays fenced.
            let fenced: bool = txn
                .query_one(
                    "select fence_snapshot is not null from segments where seg_seq = $1",
                    &[&seg_seq],
                )
                .await?
                .get(0);
            if !fenced {
                if held.is_empty() {
                    return Err(StagingError::UnfencedSealedSegment { seg_seq }.into());
                }
                break;
            }
            claim::claim(&txn, seg_seq, claimed_by, live_workers).await?;
            let share = held_share(&*txn, seg_seq, claimed_by).await?;
            if share.buckets.is_empty() {
                continue;
            }
            let pages = share.has_cursor || share.estimated_rows > cap as i64;
            if held.is_empty() {
                estimated = share.estimated_rows;
                held.push(share);
                if pages {
                    break;
                }
            } else if pages || estimated + share.estimated_rows > cap as i64 {
                super::liveness::release(&*txn, seg_seq, claimed_by).await?;
                break;
            } else {
                estimated += share.estimated_rows;
                held.push(share);
            }
        }
        txn.commit().await?;
        held
    };
    if held.is_empty() {
        return Ok(None);
    }

    // A `schema_changed` marker pauses the definitions that read a
    // missing column before this drain computes anything they could apply.
    let held_seqs: Vec<i64> = held.iter().map(|share| share.seg_seq).collect();
    super::schema_change::pause_readers(pool, &held_seqs).await?;

    let estimated: i64 = held.iter().map(|share| share.estimated_rows).sum();
    if !held.iter().any(|share| share.has_cursor) && estimated <= cap as i64 {
        let per_segment = {
            let mut client = pool.get().await?;
            let txn = client.transaction().await?;
            let mut per_segment = Vec::with_capacity(held.len());
            let mut total = 0usize;
            for share in &held {
                let folded =
                    fold::fold_limited(&txn, share.seg_seq, &share.filter(&share.buckets), cap + 1)
                        .await?;
                total += folded.len();
                per_segment.push(folded);
                if total > cap {
                    break;
                }
            }
            txn.commit().await?;
            (total <= cap).then_some(per_segment)
        };
        if let Some(per_segment) = per_segment {
            let steps: Vec<SegmentStep> = held
                .iter()
                .map(|share| SegmentStep {
                    seg_seq: share.seg_seq,
                    page: Some(PageClaim {
                        buckets: share.buckets.clone(),
                        held: share.buckets.clone(),
                        next: None,
                    }),
                })
                .collect();
            if let Some(n) = pause_before(hooks, 1) {
                pause(n).await;
            }
            let outcome = drain_batch(
                pool,
                fold::merge_folded_changes(per_segment),
                &steps,
                claimed_by,
                wake_channel,
                lose_commit_reply(hooks, 1),
            )
            .await?;
            return Ok(Some(outcome));
        }
        crate::instance_log::debug!(
            seg_seq = held[0].seg_seq,
            cap,
            "direct fold's guard tripped: the share holds more than the cap; paging instead"
        );
    }

    // Paged: the first segment drains alone. Only a tripped guard gets here
    // holding more than one; the others go back for a later call.
    if held.len() > 1 {
        let client = pool.get().await?;
        for share in &held[1..] {
            super::liveness::release(&**client, share.seg_seq, claimed_by).await?;
        }
    }
    let share = &held[0];
    let cursors = if share.has_cursor {
        let client = pool.get().await?;
        super::page::read_cursors(&**client, share.seg_seq, &share.buckets).await?
    } else {
        HashMap::new()
    };
    let mut groups: BTreeMap<Option<fold::PageKey>, Vec<i16>> = BTreeMap::new();
    for &bucket in &share.buckets {
        groups
            .entry(cursors.get(&bucket).cloned())
            .or_default()
            .push(bucket);
    }

    let mut still_held: Vec<i16> = share.buckets.clone();
    let mut total = ManyApplyOutcome {
        keys_written: 0,
        keys_deleted: 0,
        segments_drained: Vec::new(),
        pages: 0,
    };
    let mut records = 0u64;
    // One unpooled session for the whole call: each cursor group folds its
    // share into the session's `TEMP` table once, and its pages read that
    // table back. Dropping `pages` on any exit, `?` included, closes the
    // session and drops the table with it.
    let started = std::time::Instant::now();
    let mut materialize_time = std::time::Duration::ZERO;
    let mut read_time = std::time::Duration::ZERO;
    let mut pages = super::page::MaterializedPages::open(pool, share.seg_seq).await?;
    for (start, buckets) in groups {
        let materialize_started = std::time::Instant::now();
        records += pages
            .materialize(&share.filter(&buckets), start.as_ref())
            .await?;
        materialize_time += materialize_started.elapsed();
        let mut after = start;
        loop {
            let read_started = std::time::Instant::now();
            let page = pages.next_page(after.as_ref(), cap).await?;
            read_time += read_started.elapsed();
            let step = SegmentStep {
                seg_seq: share.seg_seq,
                page: Some(PageClaim {
                    buckets: buckets.clone(),
                    held: still_held.clone(),
                    next: page.next.clone(),
                }),
            };
            if let Some(n) = pause_before(hooks, total.pages + 1) {
                pause(n).await;
            }
            let outcome = drain_batch(
                pool,
                page.records,
                std::slice::from_ref(&step),
                claimed_by,
                wake_channel,
                lose_commit_reply(hooks, total.pages + 1),
            )
            .await?;
            total.absorb(outcome);
            if hooks
                .stop_after_pages
                .is_some_and(|stop| total.pages >= stop)
            {
                return Ok(Some(total));
            }
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        still_held.retain(|bucket| !buckets.contains(bucket));
    }
    crate::instance_log::info!(
        seg_seq = share.seg_seq,
        pages = total.pages,
        records,
        cap,
        materialize_ms = materialize_time.as_millis() as u64,
        page_read_ms = read_time.as_millis() as u64,
        total_ms = started.elapsed().as_millis() as u64,
        "paged drain: the share was larger than the drain batch cap"
    );
    Ok(Some(total))
}

/// Takes [`DrainHooks::lose_commit_reply_on_page`] when `page` is the one it
/// names.
fn lose_commit_reply(hooks: &mut DrainHooks, page: usize) -> bool {
    hooks
        .lose_commit_reply_on_page
        .take_if(|n| *n == page)
        .is_some()
}

/// Takes [`DrainHooks::pause_before_page`]'s channels when `page` is the one
/// it names.
fn pause_before(
    hooks: &mut DrainHooks,
    page: usize,
) -> Option<(
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
)> {
    if hooks
        .pause_before_page
        .as_ref()
        .is_some_and(|(n, ..)| *n == page)
    {
        hooks
            .pause_before_page
            .take()
            .map(|(_, paused, resume)| (paused, resume))
    } else {
        None
    }
}

async fn pause(
    (paused, resume): (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ),
) {
    let _ = paused.send(());
    let _ = resume.await;
}

/// Phase 2 (compute) and Phase 3 (apply, then `steps`' claim endings) over one
/// batch of folded records, retrying on a version-fence miss, a transient
/// failure, or an isolated-and-evicted poison key — the design doc's
/// "reload, recompute, retry" loop, using [`FenceMissBackoff`] between
/// attempts. One call is one page: the direct path's whole share, or one page
/// of an oversized one.
async fn drain_batch(
    pool: &Pool,
    mut folded: Vec<FoldedChange>,
    steps: &[SegmentStep],
    claimed_by: &str,
    wake_channel: &str,
    mut lose_commit_reply: bool,
) -> Result<ManyApplyOutcome, ApplyError> {
    // Every retry-classification helper below (`classify_and_retry`,
    // `isolate_and_evict`) takes one representative `seg_seq` purely as
    // audit/probe bookkeeping (which batch's contribution a parked poison
    // row names; which real claim a rollback-only probe transaction's
    // completion step exercises) — never as something correctness depends
    // on picking exactly right among several equally-valid coalesced
    // segments. The lowest of this call's segments is as good a
    // representative as any; see `apply_page`'s doc comment on the same
    // choice for `poisoned_park`.
    let representative_seg_seq = steps[0].seg_seq;

    // A primary key redefined since capture was installed pauses
    // the table's readers before this page computes anything for them.
    let tables: std::collections::BTreeSet<&str> =
        folded.iter().map(|c| c.src_table.as_str()).collect();
    super::schema_change::pause_readers_of_redefined_keys(pool, &tables).await?;

    let mut backoff = FenceMissBackoff::new();
    let mut transient = TransientRetry::new();
    let mut halt_retry = HaltRetry::default();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        tracing::Span::current().record("attempt", attempt);
        let plan = match compute_page(pool, &folded, None, halt_retry.skip_frozen).await {
            Ok(plan) => plan,
            Err(ApplyError::SourceTableDropped { source_table }) => {
                // The one sanctioned exception to immutability: the
                // table this batch's folded rows name is gone, not any one
                // row's fault, so no retry or per-key quarantine resolves
                // it. Purge every ring/quarantine row naming it and retry
                // with it excluded. Not counted against
                // `MAX_APPLY_ATTEMPTS` — this corrects `folded` itself
                // rather than retrying the same input.
                crate::instance_log::warn!(
                    seg_seq = representative_seg_seq,
                    source_table = %source_table,
                    "source table no longer exists; purging its staged rows and retrying \
                     without it"
                );
                quarantine::purge_dropped_table(pool, &source_table).await?;
                folded.retain(|c| c.src_table != source_table);
                attempt -= 1;
                continue;
            }
            // Every other Phase 2 failure goes through the exact same
            // classification the Phase 3 branch below uses, and isolation
            // must attribute it the same way regardless of which phase
            // first tripped over it.
            Err(err) => {
                classify_and_retry(
                    pool,
                    steps,
                    claimed_by,
                    wake_channel,
                    &folded,
                    &mut attempt,
                    &mut backoff,
                    &mut transient,
                    &mut halt_retry,
                    err,
                )
                .await?;
                continue;
            }
        };

        let mut client = pool.get().await?;
        let began = std::time::Instant::now();
        let txn = client.transaction().await?;
        let applied = match apply_page(&txn, steps, claimed_by, &plan, wake_channel).await {
            // COMMIT fails like any statement of the page does: a
            // deferred constraint or constraint trigger an application put
            // on a target fires here, and a connection can drop here. Either
            // is classified like an `apply_page` failure, which isolation can
            // charge because its probes check deferred constraints too
            // (`quarantine::probe_records`).
            //
            // A connection can also drop after the COMMIT landed, so a
            // transient retry may re-run a page that committed. A final
            // page's retry finds the claim that commit released gone, and
            // rolls back whole as `ClaimLost`. A non-final page's retry
            // still holds its claim, finds its buckets' cursor already at
            // its last key, and rolls back whole as
            // `PageAlreadyCommitted`, which ends the page as done: a
            // group-basis target would add the page's deltas again.
            Ok(outcome) => match txn.commit().await {
                Ok(()) if std::mem::take(&mut lose_commit_reply) => {
                    Err(end_session(&**client).await)
                }
                Ok(()) => Ok(outcome),
                Err(err) => Err(err.into()),
            },
            Err(err) => {
                let _ = txn.rollback().await;
                Err(err)
            }
        };
        match applied {
            Err(ApplyError::PageAlreadyCommitted) => {
                crate::instance_log::warn!(
                    seg_seq = representative_seg_seq,
                    claimed_by = %claimed_by,
                    "a page's retry found it committed before; its commit's reply was lost, \
                     and nothing was applied twice"
                );
                return Ok(ManyApplyOutcome {
                    keys_written: 0,
                    keys_deleted: 0,
                    segments_drained: steps.iter().map(|step| (step.seg_seq, false)).collect(),
                    pages: 1,
                });
            }
            Ok(outcome) => {
                crate::metrics::record_page_transaction(began.elapsed());
                // Only
                // flush `plan`'s buffered metrics now, once this attempt's
                // transaction has actually committed — never from inside
                // `compute` itself, which the loop above may have called
                // more than once for this same `folded` input.
                flush_apply_metrics(&plan);
                return Ok(outcome);
            }
            Err(err) => {
                // Isolation can run for many probes, each with
                // its own compute and pooled connection. Holding this page's
                // plan and connection across it cost about 1.5x the page's
                // plan in memory, and one pooled connection sitting idle for
                // the whole isolation, which on a small pool left the probes'
                // own checkouts waiting on it until they timed out. A retry
                // recomputes the plan and checks out a connection anyway.
                drop(client);
                drop(plan);
                classify_and_retry(
                    pool,
                    steps,
                    claimed_by,
                    wake_channel,
                    &folded,
                    &mut attempt,
                    &mut backoff,
                    &mut transient,
                    &mut halt_retry,
                    err,
                )
                .await?;
            }
        }
    }
}

/// [`DrainHooks::lose_commit_reply_on_page`]: terminates `client`'s own
/// session and returns the error the statement fails with, a dropped
/// connection.
async fn end_session(client: &impl GenericClient) -> ApplyError {
    client
        .batch_execute("select pg_terminate_backend(pg_backend_pid())")
        .await
        .expect_err("a session that terminates itself fails the statement")
        .into()
}

/// Classifies `err` (per [`quarantine::classify`]) and either retries or
/// propagates, shared by both [`drain_once`] and [`drain_many`]'s Phase 2
/// and Phase 3 failure arms, the page's COMMIT included, so a bad key
/// is attributed identically
/// regardless of which phase — or which of the two orchestrators —
/// first surfaced it.
///
/// `Ok(())` means "retry with `folded` unchanged": a version fence miss or a
/// transient failure, within [`MAX_APPLY_ATTEMPTS`]; a halt, see [`halt`];
/// or an isolation that poisoned at least one key for the definition it
/// fails in, which the retry's recompute leaves out of that definition's
/// apply.
/// `Err(_)` propagates `err` (or a probe's own halting error, once its halt
/// paused nothing, see [`halt`]) unmodified, once retries are exhausted or
/// the failure must never be retried at all —
/// which includes an isolate attempt that evicted nothing, whether no key
/// reproduced the failure or the keys that did are still below the death
/// threshold. That ends the drain call; the next drain cycle re-reads the
/// batch and charges again (see [`quarantine::isolate_and_evict`]'s
/// "one charge per drain call").
///
/// A failure surfaced while charging no key and pausing no definition is
/// recorded as a drain holdup on `steps`' segments first
/// ([`super::holdup`]): a halt that paused nothing, isolation that reproduced
/// nothing or hit its probe limit, and isolate-eligible retries exhausted.
/// A transient failure, a lost claim, a fence miss and a below-threshold
/// charge aren't.
#[allow(clippy::too_many_arguments)]
async fn classify_and_retry(
    pool: &Pool,
    steps: &[SegmentStep],
    claimed_by: &str,
    wake_channel: &str,
    folded: &[FoldedChange],
    attempts: &mut u32,
    backoff: &mut FenceMissBackoff,
    transient: &mut TransientRetry,
    halt_retry: &mut HaltRetry,
    err: ApplyError,
) -> Result<(), ApplyError> {
    match retry_or_surface(
        pool,
        steps[0].seg_seq,
        claimed_by,
        wake_channel,
        folded,
        attempts,
        backoff,
        transient,
        halt_retry,
        err,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(Surface::Plain(err)) => Err(err),
        Err(Surface::Holdup(err)) => {
            super::holdup::record(pool, steps, claimed_by, folded, &err).await;
            Err(err)
        }
    }
}

/// How [`retry_or_surface`] surfaces a page's failure: whether it was
/// charged to no one, and so is recorded as a drain holdup.
#[derive(Debug)]
enum Surface {
    /// Surfaced as is: retries a transient failure or a fence miss ran out
    /// of, a lost claim, a key charged below the death threshold, or a
    /// failure of the retry machinery itself.
    Plain(ApplyError),
    /// Charged to no one: [`classify_and_retry`] records a holdup first.
    Holdup(ApplyError),
}

impl From<ApplyError> for Surface {
    fn from(err: ApplyError) -> Self {
        Surface::Plain(err)
    }
}

/// [`classify_and_retry`]'s decision, one representative `seg_seq` for its
/// log lines and isolation's bookkeeping.
#[allow(clippy::too_many_arguments)]
async fn retry_or_surface(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    wake_channel: &str,
    folded: &[FoldedChange],
    attempts: &mut u32,
    backoff: &mut FenceMissBackoff,
    transient: &mut TransientRetry,
    halt_retry: &mut HaltRetry,
    err: ApplyError,
) -> Result<(), Surface> {
    let attempt = *attempts;
    // A lost claim is nobody's key's fault, and nothing in
    // this call can get it back. Isolating it would probe every record under
    // the same lost claim, reproduce `ClaimLost` for each, and charge every
    // key in the page a death. Surface it: the caller releases, and whoever
    // holds the buckets now resumes from the last committed cursor. Bare or
    // wrapped, as `classify` decides by the innermost error.
    if quarantine::is_claim_lost(&err) {
        return Err(Surface::Plain(err));
    }
    match quarantine::classify(&err) {
        // Version fence miss: reload schema and retry, backing off only on
        // consecutive misses — `FenceMissBackoff` is exactly that state
        // machine, reused as-is.
        quarantine::FailureClass::VersionFenceMiss => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                crate::instance_log::warn!(
                    seg_seq,
                    attempt,
                    error = %err,
                    "version fence miss retries exhausted; surfacing the failure"
                );
                return Err(Surface::Plain(err));
            }
            crate::instance_log::debug!(seg_seq, attempt, error = %err, "version fence miss; retrying");
            let delay = backoff.next_delay();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        }
        // A lock wait that hit `lock_timeout` (ADR-0002 I7):
        // the page's transaction has rolled back, so it holds no snapshot
        // and no locks while it waits here. Back off and retry, keeping the
        // claim (the heartbeat daemon keeps it fresh), for up to
        // `LOCK_RETRY_BUDGET` rather than `MAX_APPLY_ATTEMPTS`: whoever holds
        // the lock may hold it for many timeouts, and releasing the claim
        // only hands the same wait to the next claimant.
        quarantine::FailureClass::Transient if crate::locks::is_lock_not_available(&err) => {
            let waited = transient.lock_waited();
            if waited >= LOCK_RETRY_BUDGET {
                crate::instance_log::warn!(
                    seg_seq,
                    waited_ms = waited.as_millis() as u64,
                    error = %err,
                    "lock timeout retries exhausted; surfacing the failure"
                );
                return Err(Surface::Plain(err));
            }
            let delay = transient.next_delay();
            crate::instance_log::warn!(
                seg_seq,
                delay_ms = delay.as_millis() as u64,
                "drain page waited out its lock_timeout; rolled back, retrying outside the \
                 transaction"
            );
            tokio::time::sleep(delay).await;
            // A lock timeout isn't an attempt at the page: it never ran
            // far enough to fail on its own account.
            *attempts -= 1;
            Ok(())
        }
        // Transient (serialization failure, deadlock, dropped connection,
        // statement timeout): retry, charge nothing, backing off on
        // consecutive transient failures on a schedule of its
        // own, so a fence miss between two doesn't reset it.
        quarantine::FailureClass::Transient => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                crate::instance_log::warn!(
                    seg_seq,
                    attempt,
                    error = %err,
                    "transient failure retries exhausted; surfacing the failure"
                );
                return Err(Surface::Plain(err));
            }
            let delay = transient.next_delay();
            crate::instance_log::debug!(
                seg_seq,
                attempt,
                delay_ms = delay.as_millis() as u64,
                error = %err,
                "transient apply failure; retrying"
            );
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        }
        // Halting schema diagnosis: never quarantine. Pause what it
        // reaches and retry without it.
        quarantine::FailureClass::Halting => halt(pool, seg_seq, attempts, halt_retry, err).await,
        // Everything else: bisect the folded records down to the ones that
        // fail alone to attribute the failure to specific key(s),
        // evicting any past the death
        // threshold and retrying without them. If nothing reproduces alone,
        // the error is surfaced, not blamed; if keys reproduce but none is
        // past the threshold yet, they are charged and the error is surfaced
        // too (the next drain cycle charges them again).
        quarantine::FailureClass::Isolate => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                crate::instance_log::warn!(
                    seg_seq,
                    attempt,
                    error = %err,
                    "isolate-eligible failure retries exhausted; surfacing the failure"
                );
                return Err(Surface::Holdup(err));
            }
            let outcome = match quarantine::isolate_and_evict(
                pool,
                seg_seq,
                claimed_by,
                wake_channel,
                folded,
                quarantine::DEFAULT_DEATH_THRESHOLD,
                halt_retry.skip_frozen,
            )
            .await
            {
                Ok(outcome) => outcome,
                // A probe's own halting failure is no different from the
                // page's (doc 06's "What must never be quarantined").
                Err(probe_err)
                    if quarantine::classify(&probe_err) == quarantine::FailureClass::Halting =>
                {
                    return halt(pool, seg_seq, attempts, halt_retry, probe_err).await;
                }
                Err(probe_err) => return Err(Surface::Plain(probe_err)),
            };
            match outcome {
                // The key is poisoned for the definition it fails in
                // only, so the retry recomputes the same records, and leaves
                // the key out of that definition's apply alone.
                quarantine::IsolationOutcome::Evicted { evicted, charged } => {
                    if charged.is_empty() {
                        crate::instance_log::warn!(
                            seg_seq,
                            evicted,
                            "isolated a key and poisoned it for the definition it fails in; \
                             retrying without it there"
                        );
                    } else {
                        crate::instance_log::warn!(
                            seg_seq,
                            evicted,
                            threshold = quarantine::DEFAULT_DEATH_THRESHOLD,
                            charged = %quarantine::describe_charged_keys(
                                &charged,
                                quarantine::DEFAULT_DEATH_THRESHOLD,
                            ),
                            "isolated a key and poisoned it for the definition it fails in; \
                             retrying without it there (other failing keys charged, still below \
                             the death threshold)"
                        );
                    }
                    Ok(())
                }
                // Warn, not debug: the operator should see which key is
                // heading for eviction, and nothing else logs this surfaced
                // failure (the worker loop just releases and backs off).
                // Bounded, not spam: every key this line names was just
                // charged, so it names a key at most `threshold - 1` times
                // before `evict_key`'s own warn replaces it. A clean drain
                // that applies the key clears its count, so a key that only
                // fails intermittently can start over, but then each line
                // still stands for a real failed drain.
                quarantine::IsolationOutcome::ChargedBelowThreshold { charged } => {
                    crate::instance_log::warn!(
                        seg_seq,
                        threshold = quarantine::DEFAULT_DEATH_THRESHOLD,
                        charged = %quarantine::describe_charged_keys(
                            &charged,
                            quarantine::DEFAULT_DEATH_THRESHOLD,
                        ),
                        error = %err,
                        "isolation pinned the failure on key(s) still below the death threshold; \
                         charged one death each and surfacing the original failure"
                    );
                    Err(Surface::Plain(err))
                }
                quarantine::IsolationOutcome::NothingReproduced => {
                    crate::instance_log::debug!(
                        seg_seq,
                        error = %err,
                        "isolation reproduced nothing; surfacing the original failure"
                    );
                    Err(Surface::Holdup(err))
                }
                // Warn: unlike `NothingReproduced`, part of the batch went
                // unprobed, so a failing key may still be in it.
                quarantine::IsolationOutcome::ProbeLimitReached { probes } => {
                    crate::instance_log::warn!(
                        seg_seq,
                        probes,
                        error = %err,
                        "isolation hit its probe limit without pinning the failure on a key; \
                         surfacing the original failure"
                    );
                    Err(Surface::Holdup(err))
                }
                // Warn: a lock or deadlock storm stopped isolation before it
                // could look at the batch. Surfacing ends this
                // drain call, so the page is retried on a later drain rather
                // than isolated again while the storm lasts. Retrying here
                // instead would hold the claim through up to
                // `MAX_APPLY_ATTEMPTS` more storms, each as long as
                // `MAX_CONSECUTIVE_TRANSIENT_PROBES` lock timeouts.
                quarantine::IsolationOutcome::TransientStorm { probes } => {
                    crate::instance_log::warn!(
                        seg_seq,
                        probes,
                        consecutive_transient = quarantine::MAX_CONSECUTIVE_TRANSIENT_PROBES,
                        error = %err,
                        "isolation stopped: its latest probes all hit transient errors; charged \
                         nothing, surfacing the original failure so a later drain retries the page"
                    );
                    Err(Surface::Plain(err))
                }
                quarantine::IsolationOutcome::FuseDisabled => {
                    crate::instance_log::debug!(
                        seg_seq,
                        error = %err,
                        "row-level death fuse disabled; surfacing the original failure unisolated"
                    );
                    Err(Surface::Holdup(err))
                }
            }
        }
    }
}

/// [`classify_and_retry`]'s halting arm: pauses every definition the
/// halting failure `err` reaches ([`super::halt::halt_closure`]) and retries
/// the page, whose recompute leaves them out. The retry is free, as for a
/// dropped source: it runs without the failing input rather than repeating
/// it. One error line per halt, from the call that paused the closure.
///
/// A halt that paused nothing (a peer paused the closure first, or the
/// failure persists without one) retries once per [`drain_batch`] call, for
/// the peer's case, then surfaces `err`, so the page can't spin. Nothing is
/// charged, so it surfaces as a drain holdup.
///
/// A refused read or write (`42501`) also has every retry skip
/// the tables no unfrozen definition reads ([`HaltRetry::skip_frozen`]).
async fn halt(
    pool: &Pool,
    seg_seq: i64,
    attempts: &mut u32,
    halt_retry: &mut HaltRetry,
    err: ApplyError,
) -> Result<(), Surface> {
    let paused = super::halt::halt_closure(pool, &err).await?;
    if quarantine::is_insufficient_privilege(&err) {
        halt_retry.skip_frozen = true;
    }
    if !paused.is_empty() {
        crate::instance_log::error!(
            seg_seq,
            paused = ?paused,
            error = %err,
            "halting failure; paused every definition it reaches until resumed, and retrying \
             the page without them"
        );
        *attempts -= 1;
        return Ok(());
    }
    if !halt_retry.retried {
        halt_retry.retried = true;
        crate::instance_log::debug!(
            seg_seq,
            error = %err,
            "halting failure whose closure is already paused; retrying the page once"
        );
        return Ok(());
    }
    // Paused nothing twice: a refused read or write the catalog pins on no
    // table, say. Nothing is charged, so it is a holdup.
    Err(Surface::Holdup(err))
}

/// What [`halt`] keeps across one [`drain_batch`] call's retries.
#[derive(Debug, Default)]
struct HaltRetry {
    /// Whether this call has retried a halt that paused nothing.
    retried: bool,
    /// Whether Postgres refused the drain a read or write (`42501`),
    /// so every later attempt skips the tables no unfrozen
    /// definition reads ([`compute_page`]). The halt pauses the readers of
    /// the refused table, and the skip keeps the retry from reading it
    /// again.
    skip_frozen: bool,
}

/// The next batch a free worker should pick up: the lowest-`seg_seq`
/// segment that is `'sealed'` or `'draining'` and not yet fully drained
/// (`drained_mask` short of `(1 << bucket_count) - 1`). Ordered ascending
/// so batches drain roughly in creation order, though nothing here enforces
/// that strictly — a worker could still be mid-drain on an earlier segment
/// while this returns a later one.
///
/// Only fenced segments come back. The seal decides a batch's
/// `has_truncate`, `row_count` and `bucket_count` in the statement that
/// publishes its fence, so before that they are column defaults,
/// not the batch's. A segment returned here unfenced could be fenced by the
/// time its caller claims it, with a truncate this query never saw, and be
/// coalesced behind a predecessor the barrier says must drain first.
///
/// The one exception is the truncate barrier: a truncate is
/// whole-keyspace, but drains are per-bucket, parallel, and — per the
/// paragraph above — explicitly *not* ordered, so a truncate is a
/// two-directional drain barrier. Predecessors must drain first (else an
/// earlier batch's insert would apply after the truncate and wrongly
/// survive); successors must not drain first (else a later batch's
/// post-truncate insert would be wiped when the truncate's clear runs). Let
/// `B` be the lowest `seg_seq` among undrained truncate-bearing segments
/// (`segments.has_truncate`, decided with the fence — see `seal::seal_phase2`);
/// this query never returns a segment past `B`. Because this query always
/// returns the *lowest* eligible `seg_seq`, `B` itself is only ever handed
/// out once every segment below it has drained — one clause gives both
/// directions of the barrier.
///
/// Sealed segments are fenced strictly in order (R9 in ADR-0002: the
/// barrier needs them planned in order): the seal gate
/// (`seal::seal_phase1`) refuses to seal a segment while its predecessor's
/// fence is unpublished, so no segment past one whose `has_truncate` is
/// still undecided is ever fenced, and none is returned here.
#[cfg(any(test, feature = "internals"))]
pub async fn next_claimable_segment(
    client: &impl GenericClient,
) -> Result<Option<i64>, ApplyError> {
    Ok(next_claimable_segments(client, 1).await?.into_iter().next())
}

/// [`next_claimable_segment`] generalized to return several claimable
/// segments at once, for [`drain_many`] to coalesce —
/// the batch a burst of quickly-sealing segments needs so each one doesn't
/// pay its own full compute-and-apply pass.
///
/// Bounded by `drain_batch_cap`, not a segment count. The
/// first segment always comes back, however large; the ones after it come
/// back in ascending order while their `segments.row_count`s sum to at most
/// the cap (an oversized first segment doesn't count against them: if peers
/// hold all of it, this worker coalesces what follows instead of idling).
/// [`drain_many`] then claims in order and stops at the first share that
/// must page, so an oversized segment still drains alone. Row counts are
/// ring rows over the whole fenced window, never fewer than the
/// folded records they produce, so a coalesced batch's shares fit the cap by
/// construction.
///
/// Runs the exact same barrier-respecting query [`next_claimable_segment`]
/// does (see its doc comment for the truncate barrier `B`). The only
/// additional rule this adds is the one [`drain_many`]'s doc comment calls
/// out as its caller-side invariant: **a truncate-bearing segment is never
/// coalesced with another segment.** Because `B` is by definition the
/// *lowest* seg_seq among undrained truncate-bearing segments and this query
/// never returns anything past `B`, the only truncate-bearing segment that
/// can ever appear in the result set is `B` itself, and — being the
/// barrier's own upper bound — it is always the *last* (highest-`seg_seq`)
/// row, never the first. So: walk the ascending rows, taking ordinary
/// (non-truncate) segments into the batch; the moment a truncate-bearing row
/// is reached, stop — returning it alone if the batch collected so far is
/// otherwise empty (it's the lowest claimable segment, so it must be handed
/// out on its own), or returning what's already been collected without it
/// otherwise (it'll be handed out alone on some future call, once nothing
/// ordinary remains ahead of it).
pub async fn next_claimable_segments(
    client: &impl GenericClient,
    drain_batch_cap: usize,
) -> Result<Vec<i64>, ApplyError> {
    let rows = client
        .query_typed(
            "select seg_seq, has_truncate, row_count from segments \
             where state in ('sealed', 'draining') \
               and fence_snapshot is not null \
               and drained_mask <> ((1::bigint << bucket_count) - 1) \
               and seg_seq <= coalesce( \
                   (select min(seg_seq) from segments \
                    where has_truncate \
                      and drained_mask <> ((1::bigint << bucket_count) - 1)), \
                   seg_seq \
               ) \
             order by seg_seq asc",
            &[],
        )
        .await?;

    let cap = drain_batch_cap as i64;
    let mut rows_taken: i64 = 0;
    let mut batch = Vec::with_capacity(rows.len());
    for row in rows {
        let seg_seq: i64 = row.get(0);
        let has_truncate: bool = row.get(1);
        let row_count: i64 = row.get(2);
        if has_truncate {
            if batch.is_empty() {
                batch.push(seg_seq);
            }
            break;
        }
        if batch.is_empty() {
            // The first segment always comes back. When it is over the cap it
            // pages alone if this worker wins any of it, and costs nothing if
            // peers hold all of it, so it doesn't count against the ones after.
            if row_count <= cap {
                rows_taken = row_count;
            }
        } else if row_count > cap || rows_taken + row_count > cap {
            break;
        } else {
            rows_taken += row_count;
        }
        batch.push(seg_seq);
    }
    Ok(batch)
}
