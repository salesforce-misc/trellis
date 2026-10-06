//! Apply ∪ mark-drained (issue #11, stage 05) — the 1-1/scalar subset only.
//! See docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md.
//!
//! **Scope**: only [`crate::defs::ast::KeySpace::OneToOne`] definitions.
//! The aggregate delta model (groups, invertibility, min/max recompute,
//! composite partials, grain migration) is out of scope — it's blocked on
//! aggregate transform-defs, which don't exist yet.
//!
//! The design's three phases map onto three functions:
//!
//! - Phase 1 (the claim, committed on its own) is `drain_segments`' opening
//!   block, reusing [`super::claim::claim`] directly. The fold runs after it
//!   commits, in its own read transaction: [`super::fold::fold_limited`] for
//!   a share that fits `drain_batch_cap`, pages from [`super::page`]
//!   otherwise (issue #620).
//! - Phase 2 (compute: evaluate `f()` against every folded change, no
//!   transaction, no locks) is [`compute`].
//! - Phase 3 (apply ∪ mark-drained, one transaction) is
//!   [`apply_and_mark_drained`].
//!
//! [`drain_once`] is the orchestrator tying the three together, including
//! the version-fence/serialization retry loop the design calls for.
//! [`next_claimable_segment`] is the "which batch should a free worker pick
//! up next" query a real drain loop (not assembled here — issue #11,
//! blocked on aggregate transform-defs) would call before it.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::types::{PgLsn, ToSql};
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::{KeySpace, TransformDef, ValueType};
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::eval::{
    self, EvalError, RelationshipContext, Row, ToManyRelationship, ToOneRelationship,
};
use crate::defs::model::{RelationshipCardinality, RelationshipDefinition};
use crate::defs::validate::{self, ValidationError};
use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident, quote_literal};

use super::append::{self, StagedChange};
use super::claim;
use super::claim::{HeldShare, held_share};
use super::converge;
use super::error::StagingError;
use super::fold::{self, FoldedChange, earliest_origin};
use super::liveness::FenceMissBackoff;
use super::one_to_one_ledger;
use super::quarantine;
use super::target_mutations::TargetMutations;
use super::watermark::StagedWatermark;

/// The absolute ceiling on [`FoldedChange::hop_gen`] propagation, a backstop
/// over and above the schema-derived hop bound doc 05 describes ("one past
/// its deepest trigger"): even if the catalog's own graph analysis is wrong
/// or a definition cycle somehow reaches this stage, propagation cannot
/// wind up more than this many hops before [`ApplyError::HopBoundExceeded`]
/// stops it. `defs::validate` already rejects definition cycles at
/// creation time (`detect_cycle`), so this is defense-in-depth, not the
/// primary guard.
pub const MAX_HOP_GEN: i32 = 32;

/// Issue #135 (epic #127): starvation-freedom threshold for a to-one
/// relationship reverse's guard-gated retry loop (issue #134's
/// `RelationshipReverseDeferred`/`retry_count`) — see the "Issue #135:
/// fairness escalation" section below (right after [`check_reverse_guards`])
/// for the full design and the alternatives it rejects. Once a single
/// reverse *transition* (one parent's old-image/new-image pair — fixed
/// across every retry, never re-derived; see [`RelationshipReverseRecord`]'s
/// doc comment) has been rejected by guard (a), (b), or (c) this many times
/// in a row, the *next* rejection escalates instead of deferring again: it
/// advances the settled parent projection immediately (sound once an
/// independent recheck of guard (d) — the projection's own LSN chain —
/// holds; see [`reverse_ordering_still_holds`]) and resolves the aggregate
/// correction via the pre-#131 image-less `Recompute` fallback, which needs
/// none of #132's four guards for its own correctness.
///
/// **Why 5.** No production signal exists yet to tune this against — this
/// issue's own stress model (see the module's test suite and its report) is
/// what informs the choice, not a fleet metric. 5 is small enough to bound
/// worst-case staleness to a handful of drain cycles (in practice usually
/// far fewer — guard (d) is expected to hold on nearly every attempt once
/// children are the only source of churn, since nothing else is racing to
/// move *this* parent's own projection row; see the design section's
/// reasoning) while large enough that an ordinary transient blip — the
/// *common* case #134's own doc section measured — never escalates. (#623 D5
/// deleted the fast path, so both outcomes re-derive the same rows now; the
/// deferral still keeps the projection's advances in order.)
/// A plain constant, not a runtime setting, following this module's own
/// [`MAX_HOP_GEN`] precedent: promote it to something tunable only if a real
/// deployment's `trellis_relationship_reverse_deferred_total` /
/// `trellis_relationship_reverse_fairness_escalated_total` metrics ever show
/// a need.
pub const RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD: i32 = 5;

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Why a drain attempt (fold, compute, or apply ∪ mark-drained) failed. A
/// module-local enum, plain `Display` + `std::error::Error`, composing with
/// the crate's other error types via `From` — matching
/// [`StagingError`]/[`CatalogError`]/[`DdlError`]/[`EvalError`]'s own
/// convention.
#[derive(Debug)]
pub enum ApplyError {
    /// A failure from the staging ring (claim, fold, append).
    Staging(StagingError),
    /// A failure reading the transform catalog.
    Catalog(CatalogError),
    /// A failure introspecting a source table's primary key.
    Ddl(DdlError),
    /// A failure evaluating a definition's calculated fields.
    Eval(EvalError),
    /// A definition's calculated fields failed to type-infer against its own
    /// persisted `source_columns` — meaning a definition that already passed
    /// [`crate::defs::validate::validate`] at creation time no longer
    /// type-checks against the map it was created with, which should not be
    /// reachable; kept as a typed error rather than a panic per this
    /// module's own convention of not trusting invariants it cannot enforce
    /// itself.
    Validate(ValidationError),
    /// Substituting a `GROUP BY` definition's cross-field-alias references
    /// (`defs::backfill::substituted_field_exprs`) failed — a cyclic alias
    /// chain or a pathologically large expansion. Both are rejected by
    /// [`crate::defs::validate::validate`] (a real cycle) or bounded at
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
    /// Phase 3's version fence found `source_table_versions.version` had
    /// moved since Phase 2 loaded it: a definition change landed on
    /// `src_table` mid-drain. Routine and immediately retryable — see
    /// [`super::liveness::release`]'s doc comment on why a fence miss is
    /// not parked behind the reclaim TTL.
    VersionFenceMiss { src_table: String },
    /// The entry lock of a ledger target (`super::ledger::lock_entries`)
    /// found a key with no entry: the tombstone GC
    /// (`super::retire::collect_tombstones`, #623 D7) collected it between
    /// the placeholder insert and the sorted `for update` (#712). Transient,
    /// like a deadlock: the transaction rolls back, giving up every lock it
    /// took, and its retry inserts the key's placeholder afresh. Retaking
    /// the lock inside the transaction instead would insert that placeholder
    /// while holding the other keys' locks, out of I5's one order.
    LedgerEntryCollected { target: String },
    /// Downstream propagation would have staged a `Recompute` row past
    /// [`MAX_HOP_GEN`]. Named rather than silently truncated: an operator
    /// needs to know a wave ran away, and which target tables it ran away
    /// through, rather than have the tail of it quietly disappear.
    HopBoundExceeded { hop_gen: i32, tables: Vec<String> },
    /// An aggregate target `ledger::route` doesn't take (#623 D5). Every
    /// valid aggregate is on the ledger, so this is a definition the drain
    /// can't apply at all, not a bad row: every key of the target would
    /// reproduce it, so it halts rather than charging each key a death.
    AggregateOffLedger { target: String },
    /// A folded record names a source table Postgres no longer has
    /// (`42P01` from a live query against it) — issue #16's "one sanctioned
    /// exception to immutability": no retry or per-key quarantine can
    /// resolve this, since the table itself is gone, not any one row.
    /// [`drain_once`] routes this to [`quarantine::purge_dropped_table`]
    /// rather than the ordinary isolate/evict path.
    SourceTableDropped { source_table: String },
    /// [`super::quarantine::resume_column`] (or, indirectly,
    /// a `RESUME TRANSFORM <target>.<column>` statement,
    /// [`crate::app::Trellis::apply`]) was asked to resume a
    /// `(transform, column)` pair with no currently-paused `column_status`
    /// row — resuming a column that isn't paused is caller error, not a
    /// silent no-op. Also reused for "no such column on this definition at
    /// all," so an address naming a real transform but the wrong field name
    /// gets a specific error rather than silently doing nothing.
    ColumnNotPaused { transform: String, column: String },
    /// [`super::quarantine::resume_column`] was asked to resume a column
    /// whose owning definition is not currently applying
    /// ([`crate::defs::model::TransformStatus::is_applying`]: `live` or
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
    /// [`super::quarantine::resume_column`] was asked to resume an `ALTER
    /// TRANSFORM` field that is paused until its source's capture images the
    /// column it reads (`column_status.awaiting_capture`, #687). Unpausing it
    /// before then would let a row the narrower capture function staged,
    /// which lacks that column, reach it and fail with `MissingColumn`. The
    /// field build's start unpauses it once the widened capture lands
    /// (`staging::build`'s "Field builds"); the definition's `capture_wait`
    /// or `capture_failure` status says what holds that up.
    ColumnAwaitingCapture { transform: String, column: String },
    /// A failure from [`crate::intake::markers`]'s backfill-marker
    /// machinery (issue #55: [`super::quarantine::resume_transform`]
    /// re-parking a catch-up marker, or clearing/qualifying its source
    /// table).
    Intake(crate::intake::IntakeError),
    /// [`super::quarantine::resume_transform`] was asked to resume a target
    /// with no corresponding `transform_definitions` row at all.
    TransformNotFound { transform: String },
    /// [`super::quarantine::resume_transform`] was asked to resume a target
    /// whose current status is neither of ADR-0014's two frozen states —
    /// [`crate::defs::model::TransformStatus::Quarantined`] (the poison fuse
    /// tripped it) nor [`crate::defs::model::TransformStatus::Paused`] (an
    /// operator froze it deliberately). Resuming a transform that isn't
    /// frozen at all is caller error, not a silent no-op, mirroring
    /// [`ApplyError::ColumnNotPaused`]'s same discipline for the
    /// column-level tier.
    TransformNotPaused { transform: String },
    /// [`super::quarantine::resume_transform`] or
    /// [`super::quarantine::resume_column`] re-ran define-time validation
    /// against the live schema and it failed (#708, #760): define would
    /// refuse the definition as the schema stands now, so a rebuild would
    /// build from what it no longer matches. `reason` is define's own error,
    /// naming the column and what to change. Nothing changed: the
    /// definition (or field) stays paused.
    ResumeRefused {
        transform: String,
        reason: Box<crate::defs::catalog::CatalogError>,
    },
    /// [`from_side_rows_for_trigger_txn`] was asked to resolve a
    /// [`ReverseTrigger::WholeKeyspace`] against live, transactional full row
    /// images. Not reachable today — both of that function's call sites
    /// construct [`ReverseTrigger::Keys`] inline from a single join key they
    /// already hold, and no function in this module takes a `ReverseTrigger`
    /// and forwards one, so no dynamically-chosen variant can ever arrive
    /// there. Kept as a typed error rather than a panic per this module's own
    /// convention of not trusting invariants it cannot enforce itself (see
    /// [`ApplyError::Validate`] and [`ApplyError::Backfill`], which document
    /// the same reasoning) — and specifically because that function's own doc
    /// comment already concedes it cannot enforce its preconditions against a
    /// *new* caller added later. A future path that legitimately needs
    /// whole-keyspace full row images must implement and test that arm; until
    /// then this fails one drain visibly instead of aborting the worker.
    ReverseTriggerNotResolvable { from_table: String },
    /// A folded truncate of `src_table` carries no ring `lsn` (#774). Not
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
            ApplyError::Eval(err) => err.code(),
            ApplyError::Validate(err) => err.code(),
            ApplyError::Backfill(err) => err.code(),
            ApplyError::Db(err) => error_code::classify_pg_error(err),
            ApplyError::Pool(err) => err.code(),
            ApplyError::ClaimLost
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::LedgerEntryCollected { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::AggregateOffLedger { .. }
            | ApplyError::ReverseTriggerNotResolvable { .. }
            | ApplyError::TruncateWithoutLsn { .. } => ErrorCode::Internal,
            ApplyError::SourceTableDropped { .. } => ErrorCode::NotFound,
            ApplyError::ColumnNotPaused { .. } => ErrorCode::NotFound,
            // The definition's persisted status conflicts with what
            // `resume_column` was asked to do, the same category
            // `ValidationError::DuplicateRelationshipName` and
            // `StagingError::ProducerAlreadyRunning` use for "existing state
            // blocks this request" rather than "the request itself is
            // malformed" (-> Validation) or "nothing by that name exists"
            // (-> NotFound).
            ApplyError::DefinitionNotLive { .. } | ApplyError::ColumnAwaitingCapture { .. } => {
                ErrorCode::Conflict
            }
            ApplyError::Intake(err) => err.code(),
            ApplyError::TransformNotFound { .. } => ErrorCode::NotFound,
            ApplyError::TransformNotPaused { .. } => ErrorCode::Conflict,
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
            ApplyError::Eval(err) => write!(f, "calculated-field evaluation error: {err}"),
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
            ApplyError::AggregateOffLedger { target } => {
                write!(f, "aggregate target '{target}' is not on the ledger")
            }
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
            ApplyError::Intake(err) => write!(f, "backfill marker error: {err}"),
            ApplyError::TransformNotFound { transform } => {
                write!(f, "no transform named '{transform}' is registered")
            }
            ApplyError::TransformNotPaused { transform } => write!(
                f,
                "'{transform}' is not currently paused; resuming it re-runs its full \
                 backfill, which is only valid from `paused` or `quarantined`"
            ),
            ApplyError::ResumeRefused { transform, reason } => write!(
                f,
                "'{transform}' can't resume: define would refuse it as the schema stands now: \
                 {reason}. It stays paused"
            ),
            ApplyError::ReverseTriggerNotResolvable { from_table } => write!(
                f,
                "cannot resolve a whole-keyspace reverse trigger against live full row images \
                 for from-table '{from_table}': a TRUNCATE carries no image, so the reverse \
                 delta/fallback path has no per-row old/new parent to diff against; this \
                 combination is unreachable from any current call site and indicates a newly \
                 added caller that must implement the whole-keyspace arm for real"
            ),
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
            ApplyError::Eval(err) => Some(err),
            ApplyError::Validate(err) => Some(err),
            ApplyError::Backfill(err) => Some(err),
            ApplyError::Db(err) => Some(err),
            ApplyError::Pool(err) => Some(err),
            ApplyError::Intake(err) => Some(err),
            ApplyError::ResumeRefused { reason, .. } => Some(reason.as_ref()),
            ApplyError::ClaimLost
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::LedgerEntryCollected { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::AggregateOffLedger { .. }
            | ApplyError::SourceTableDropped { .. }
            | ApplyError::ColumnNotPaused { .. }
            | ApplyError::DefinitionNotLive { .. }
            | ApplyError::ColumnAwaitingCapture { .. }
            | ApplyError::TransformNotFound { .. }
            | ApplyError::TransformNotPaused { .. }
            | ApplyError::ReverseTriggerNotResolvable { .. }
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

impl From<EvalError> for ApplyError {
    fn from(err: EvalError) -> Self {
        ApplyError::Eval(err)
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
/// (issue #74, ADR-0007: `schema_nodes` keys on qualified identity, so a
/// bare lookup there silently finds nothing rather than erroring).
///
/// A no-op for the common case — `src_table` already contains a `.` — which
/// covers every real CDC-staged or backfill-enumerated change (issue #76
/// qualifies `change.src_table` unconditionally at the point it's staged).
/// The one bare shape that still reaches it is a chained definition's own
/// target, bare as [`crate::defs::ddl::neighbor_table_name`] always returns
/// it — the bookkeeping key `compute`'s downstream-reader lookup
/// (terminal-target latency metrics) asks about. That resolves through
/// [`catalog::resolve_graph_identity`]'s bare-target-suffix fallback (for a
/// target that isn't on the `search_path`): the name can only be some other
/// live definition's own target. A relationship's from-side used to be the
/// other bare shape; since issue #288 it carries its own recorded schema
/// ([`crate::defs::RelationshipDefinition::qualified_from_table`]) and never
/// comes through here.
///
/// Issue #267 turned this from an after-the-fact repair of an already-staged
/// bare `src_table` into the canonicalization applied *before* staging: this
/// function's output is now what those rows carry into the ring in the first
/// place, so a bare `src_table` reaching `compute` is no longer something
/// this module itself produces. See [`accumulate_from_side_recomputes`] for
/// the emission site. The reading side still tolerates a bare name anyway:
/// ring rows are durable, and this crate's integration-test fixtures stage
/// bare names by hand.
async fn qualified_schema_node_key(pool: &Pool, src_table: &str) -> Result<String, ApplyError> {
    if src_table.contains('.') {
        return Ok(src_table.to_string());
    }
    Ok(catalog::resolve_graph_identity(pool, src_table).await?)
}

/// Decodes staged jsonb images (each bound as text: this crate has no
/// `serde_json` dependency, matching `append.rs`/`fold.rs`'s convention)
/// into [`Row`]s via `jsonb_each_text`, so the evaluator never has to parse
/// JSON itself. The result is in `images`' order, one [`Row`] per image. A
/// JSON `null` value decodes to `None`, matching `Row`'s "absent column" vs.
/// "present but NULL" distinction the evaluator depends on (`eval.rs`'s
/// `MissingColumn` vs. plain `None` propagation). An empty image (`{}`)
/// decodes to an empty [`Row`].
///
/// Issue #327: one round trip per [`DECODE_CHUNK_IMAGES`] images (or
/// [`DECODE_CHUNK_BYTES`] of image text, whichever fills first), not one
/// per image. Each image still goes through the same `::jsonb` cast and
/// `jsonb_each_text` a lone decode would, so the decoded text is
/// byte-for-byte what a per-image query returns; `with ordinality` only
/// says which image a pair came from. The chunk bounds keep one statement's
/// bind message and buffered result to a few megabytes however many images
/// a batch carries.
async fn decode_images(pool: &Pool, images: &[&str]) -> Result<Vec<Row>, ApplyError> {
    decode_images_in_chunks(pool, images, DECODE_CHUNK_IMAGES, DECODE_CHUNK_BYTES).await
}

/// [`decode_images`] with its chunk bounds as parameters, so a test can put
/// chunk boundaries anywhere in a small corpus.
async fn decode_images_in_chunks(
    pool: &Pool,
    images: &[&str],
    max_images: usize,
    max_bytes: usize,
) -> Result<Vec<Row>, ApplyError> {
    let mut rows: Vec<Row> = vec![Row::new(); images.len()];
    if images.is_empty() {
        return Ok(rows);
    }
    let client = pool.get().await?;
    let mut start = 0;
    while start < images.len() {
        let mut end = start;
        let mut bytes = 0;
        // Always at least one image, however large, so a chunk is never empty.
        while end < images.len()
            && (end == start
                || (end - start < max_images && bytes + images[end].len() <= max_bytes))
        {
            bytes += images[end].len();
            end += 1;
        }
        let chunk = &images[start..end];
        let pairs = client
            .query(
                "select i.ord, e.key, e.value \
                 from unnest($1::text[]) with ordinality as i(image, ord) \
                 cross join lateral jsonb_each_text(i.image::jsonb) as e",
                &[&chunk],
            )
            .await?;
        for pair in pairs {
            let ord: i64 = pair.get(0);
            let key: String = pair.get(1);
            let value: Option<String> = pair.get(2);
            rows[start + ord as usize - 1].insert(key, value);
        }
        start = end;
    }
    Ok(rows)
}

/// At most this many images per [`decode_images`] round trip.
const DECODE_CHUNK_IMAGES: usize = 4096;

/// At most this much image text per [`decode_images`] round trip, unless a
/// single image is larger on its own.
const DECODE_CHUNK_BYTES: usize = 4 << 20;

/// Collects the images one [`compute`] step needs decoded, so they all go
/// through a single [`decode_images`] call, then hands each caller back its
/// own [`Row`] by the slot [`ImageBatch::push`] returned.
#[derive(Default)]
struct ImageBatch<'a> {
    images: Vec<&'a str>,
}

impl<'a> ImageBatch<'a> {
    /// Queues `image` for decoding, returning the slot its [`Row`] lands in.
    fn push(&mut self, image: &'a str) -> usize {
        self.images.push(image);
        self.images.len() - 1
    }

    /// [`ImageBatch::push`], passing a missing image through as `None`.
    fn push_opt(&mut self, image: Option<&'a String>) -> Option<usize> {
        image.map(|image| self.push(image))
    }

    async fn decode(self, pool: &Pool) -> Result<DecodedImages, ApplyError> {
        Ok(DecodedImages(decode_images(pool, &self.images).await?))
    }
}

/// [`ImageBatch::decode`]'s result: each slot's [`Row`], taken exactly once.
struct DecodedImages(Vec<Row>);

impl DecodedImages {
    fn take(&mut self, slot: usize) -> Row {
        std::mem::take(&mut self.0[slot])
    }

    fn take_opt(&mut self, slot: Option<usize>) -> Option<Row> {
        slot.map(|slot| self.take(slot))
    }
}

/// Re-reads every one of `keys`' current rows from `source_table` live, in
/// one round trip, for folded changes that carried no image at all (see
/// [`compute`]'s doc comment on the three shapes) — the batched replacement
/// for what used to be one `read_live_row` round trip per key (issue #13: a
/// backfill's initial enumeration stages every pre-existing row as exactly
/// this shape, so a naive per-key refetch made backfill throughput scale
/// with source table size in network round trips, not rows).
///
/// The `jsonb_each_text` unnest happens in the same query as the `any($1)`
/// row lookup — a `cross join lateral`, one column per matched row — so
/// decoding costs no extra round trip either; [`decode_images`] is only
/// paid for images that arrive already staged (`new_image`), never for a
/// live refetch. A key absent from the returned map means its
/// row is gone (already deleted, or never existed), which [`compute`]
/// treats as a delete, matching `read_live_row`'s old `None` case exactly.
///
/// `pk` may be a composite (multi-column) primary key (issue #126): `keys`
/// are each [`ddl::pk_key_sql_expr`]'s U+001F-joined text — the same shape
/// every [`FoldedChange::key`] already carries for a composite-PK source,
/// whether staged by a capture trigger or by this crate's own
/// reverse-relationship path
/// ([`from_side_rows_for_trigger_txn`]/`from_side_keys`'s callers) —
/// so the two agree on one row identity regardless of which produced it.
/// The batch match itself is a keyset join, one bind-parameter array per
/// `pk` column, rather than a single `= any($1)` — `pk.len() == 1`
/// degenerates to exactly that single-array-parameter shape, so the
/// single-column case (still the overwhelmingly common one) pays no extra
/// cost.
///
/// # NULL-keyed groups (issues #110, #446)
///
/// A `NULL` component decodes off `keys` (via [`ddl::split_pk_key`]) as a
/// real `Option::None`. A plain `t.<col> = u.<c>` never matches it (`NULL`
/// is never `=` anything, including another `NULL`), which is how a
/// `NULL`-keyed aggregate group's live row used to be mistaken for "already
/// deleted" by every downstream consumer of this function. See
/// [`live_rows_query`] for how such keys are matched without giving up the
/// index.
async fn read_live_rows_batch(
    pool: &Pool,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    keys: &[&str],
) -> Result<HashMap<String, Row>, ApplyError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let mut client = pool.get().await?;
    let row_columns = live_row_columns(&**client, source_table).await?;
    let query = live_rows_query(source_table, pk, &row_columns, keys)?;
    // A transaction only for `query_by_entry_key`'s `set local`.
    let txn = client.transaction().await?;
    let db_rows = super::ledger::query_by_entry_key(&txn, &query.sql, &query.params()).await?;
    txn.commit().await?;
    let mut rows: HashMap<String, Row> = HashMap::new();
    for db_row in db_rows {
        let key: String = db_row.get(0);
        let field: String = db_row.get(1);
        let value: Option<String> = db_row.get(2);
        rows.entry(key).or_default().insert(field, value);
    }
    Ok(rows)
}

/// [`read_live_rows_batch`]'s statement and the arrays it binds: one
/// [`LiveRowsArm`] per pattern of `NULL` key columns among the batch's keys.
pub(super) struct LiveRowsQuery<'a> {
    sql: String,
    /// The same read before its `jsonb_each_text` split: one `(k, doc)` row
    /// per matched source row, `doc` its `row_columns` as a text-valued
    /// `jsonb` (#623 D3's Re-derive read, `super::ledger`).
    pub(super) docs: String,
    arms: Vec<LiveRowsArm<'a>>,
}

/// The keys of one [`read_live_rows_batch`] call whose `pk` is `NULL` in
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

/// Builds [`read_live_rows_batch`]'s statement: one `union all` arm per
/// pattern of `NULL` key columns present among `keys` (issue #446), usually
/// just one. Each arm joins its keys' non-`NULL` parts as a keyset relation
/// (one array per such column, so the bind count doesn't grow with the
/// batch) and matches with [`live_rows_join_cond`], so every arm probes an
/// index on `pk`. Each matched row is returned as its own recomputed key
/// text plus one `(column, value)` pair per column, via a `cross join
/// lateral jsonb_each_text` (so decoding costs no extra round trip).
///
/// Matching a whole batch with `is not distinct from` on any column one of
/// its keys binds a `NULL` for (the pre-#446 shape) is correct but not
/// indexable: one `NULL`-keyed group in a batch turned the refetch into a
/// nested loop over a sequential scan of the source. This is the same split
/// `target_mutations::read_new_images` makes (issue #433).
///
/// Every caller runs the statement under `super::ledger::ENTRY_PLAN_SETTINGS`
/// (no sequential scan), and a single-column key's non-`NULL` arm also
/// restricts the column to its array (`t.<col> = any(<array>)`), which the
/// join already implies, so the source's side of whatever join the planner
/// picks is read through the key's index and bounded by the batch (#778).
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
    // Issue #248: an explicit per-column `jsonb_build_object`, not
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
        sql: format!(
            "select m.k, e.key, e.value from ({docs}) m \
             cross join lateral jsonb_each_text(m.doc) e"
        ),
        docs,
        arms: arms.into_values().collect(),
    })
}

/// [`read_live_rows_batch`]'s match for one [`live_rows_query`] arm: a
/// conjunction over `pk` where column `i` is `t.<col> is null` if
/// `pattern[i]` (every key in this arm is `NULL` there), else `t.<col> =
/// u.c<i>`. Never `is not distinct from`: that is not indexable (issue
/// #446), and within one arm it is never needed, since a column is either
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

/// The exact Postgres type of `column` on `table`, as rendered by
/// `format_type` (e.g. `integer`, `bigint`, `uuid`) — the same `pg_catalog`
/// introspection [`ddl::source_primary_key`]/[`to_column_types`] do, kept
/// here as its own single-column helper so a relationship key-lookup can
/// bind its key-array parameter to the column's own native type instead of
/// casting the column itself to `::text` (issue #125): `where col::text =
/// any($1::text[])` puts the cast on the *indexed* side, which defeats any
/// btree index on `col` and degrades what should be an O(touched keys)
/// lookup into an O(table size) sequential scan (measured 275x/794x slower
/// on realistic data volumes). `where col = any($1::text[]::{ty}[])` casts
/// the *bound array* instead — `col` is compared at its own type, so
/// Postgres can still use a btree index on it. `None` if the column doesn't
/// exist, mirroring [`to_column_types`]'s "missing is simply absent"
/// convention; such a caller falls back to the old untyped `::text`
/// comparison, which fails the same way this lookup always did if the
/// column is genuinely gone.
async fn key_column_pg_type(
    pool: &Pool,
    table: &str,
    column: &str,
) -> Result<Option<String>, ApplyError> {
    let client = pool.get().await?;
    key_column_pg_type_in(&**client, table, column).await
}

/// [`key_column_pg_type`] on an already-held connection (Phase 3's `txn`).
async fn key_column_pg_type_in(
    client: &impl GenericClient,
    table: &str,
    column: &str,
) -> Result<Option<String>, ApplyError> {
    let row = client
        .query_opt(
            "select pg_catalog.format_type(a.atttypid, a.atttypmod) \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attname = $2 \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&ddl::regclass_arg(table), &column],
        )
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// Renders a key-array equality filter against `col_ident` (an already
/// `quote_ident`-quoted column reference — this function does no quoting of
/// its own). Issue #125's fix, factored out as its own pure, directly
/// testable function shared by every relationship/join-key lookup below:
/// when `pg_type` (from [`key_column_pg_type`]) is known, casts the *bound
/// `$1` array* to it (`col = any($1::text[]::{ty}[])`), leaving `col_ident`
/// itself uncast so a btree index on it stays usable; when `pg_type` is
/// `None` (the column couldn't be introspected), falls back to the old,
/// unindexable `col_ident::text = any($1::text[])` form, which casts
/// `col_ident` itself. Regression-pinned by this module's own
/// `key_array_filter_*` unit tests below — a future edit that swaps these
/// two arms, or that re-adds a bare `::text` cast on `col_ident` in the
/// `Some` arm, would fail them immediately.
fn key_array_filter(col_ident: &str, pg_type: Option<&str>) -> String {
    match pg_type {
        Some(ty) => format!("{col_ident} = any($1::text[]::{ty}[])"),
        None => format!("{col_ident}::text = any($1::text[])"),
    }
}

/// The live, `attnum`-ordered column names of `table` — the same
/// `to_regclass`-bound `pg_attribute` introspection [`to_column_types`]/
/// [`key_column_pg_type`] already use, but the whole live column list rather
/// than a caller-supplied subset. `table` is the unquoted `schema.table`
/// identity, quoted for the lookup by [`ddl::regclass_arg`] (issue #561).
/// An already-quoted name (e.g. a [`ddl::qualified_relationship_projection_table`]
/// output) is quoted twice, finds nothing, and yields an empty column list.
///
/// Issue #248: every `to_jsonb(t.*)`-based row decode in this crate needs
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

/// [`live_row_columns`], memoized per `table` in `cache` — the caching
/// counterpart Phase 3's reverse-trigger loop needs
/// (`apply_and_mark_drained_many`'s "3d" step, via
/// [`stage_reverse_recompute_fallback`]).
///
/// That loop runs once per distinct touched parent key in the batch, and
/// every record for one relationship shares the same `from_table` — a wide
/// reverse-relationship batch can touch many parent keys in one drain, so an
/// uncached [`live_row_columns`] call per record would be a real per-record
/// `pg_catalog` round trip this fix did not add before issue #248
/// introduced the `row_columns` parameter [`from_side_rows_for_trigger_txn`]
/// now needs. `table`'s column list cannot change mid-transaction (DDL on it
/// would take a lock this transaction already holds something incompatible
/// with, for any table this crate reads), so caching it for the lifetime of
/// one Phase 3 transaction is sound.
async fn cached_row_columns<'c>(
    txn: &Transaction<'_>,
    cache: &'c mut HashMap<String, Vec<String>>,
    table: &str,
) -> Result<&'c [String], ApplyError> {
    if !cache.contains_key(table) {
        let columns = live_row_columns(txn, table).await?;
        cache.insert(table.to_string(), columns);
    }
    Ok(cache
        .get(table)
        .expect("just inserted if it wasn't already present"))
}

/// `to_jsonb(<alias>.*)`'s replacement (issue #248): an explicit
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

/// Issue #173 phase 3: what a to-side (parent) event implies about the
/// from-side rows a relationship-propagation path must reprocess — the one
/// shared decision every path in `docs/relationship-propagation.md`'s
/// obligation table that enumerates from-side rows in reaction to a to-side
/// change is built to make by constructing one of these and driving its own
/// enumeration off a `match` over it, rather than deciding independently
/// (which is exactly how TRUNCATE's key-less sentinel got forgotten three
/// times — #98, regressed by epic #127 and re-filed as #165, re-filed again
/// as #168).
///
/// Deliberately just two variants, both closed, with no path in this module
/// matching on it via a wildcard `_` arm: adding a third variant later is a
/// compile error at every one of those match sites, forcing whoever adds it
/// to decide what the new case means for each existing path instead of
/// leaving a silent gap the way the pre-#173 per-path ad hoc key lookups
/// could.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReverseTrigger<'a> {
    /// One or more specific to-side `to_col` values changed, and every
    /// from-side row whose `from_col` currently equals one of them must be
    /// reprocessed — an ordinary to-side insert/update/delete. A single
    /// to-side row change contributes its own old and/or new key; the
    /// to-many reverse path's batched sweep across every to-side change one
    /// `compute()` call folded contributes the whole set in one lookup.
    Keys(&'a [String]),
    /// The to-side table was `TRUNCATE`d (issue #98): the staged change
    /// carries no image and no key, so there is no specific set of join-key
    /// *values* to match against — the to-side table is now completely
    /// empty. Every from-side row that still points at *something* must
    /// therefore re-derive to `NULL`: there's no way to tell, after the
    /// fact, which of those rows previously matched a real to-side row (and
    /// so must newly go stale) versus already pointed at nothing (and so
    /// were already `NULL`) — both converge to the same `NULL` result once
    /// the to-side is empty, so both are recomputed rather than trying to
    /// distinguish them.
    WholeKeyspace,
}

/// The from-side keys a [`ReverseTrigger`] implies, live, in one round trip —
/// [`ReverseTrigger::Keys`] matches `from_col` against the native-typed key
/// array (issue #125: compared at `from_col`'s own type via
/// [`key_column_pg_type`], falling back to the old `::text` comparison if
/// the column can't be introspected; the relationship join-key convention is
/// otherwise shared with the evaluator — exact for the integer/uuid/text
/// keys relationships allow, numeric keys being rejected at definition
/// time), while [`ReverseTrigger::WholeKeyspace`] instead selects every
/// currently non-`NULL` `from_col` row, matching nothing about a specific
/// value at all (see that variant's own doc comment for why). Returns
/// `(from_pk_text, matched_join_text)`: `matched_join_text` is `Some` for a
/// `Keys` trigger (so the reverse-recompute caller can map each matched
/// from-side row back to the join value — hence the triggering related-row
/// change's `hop_gen` — that pulled it in) and always `None` for
/// `WholeKeyspace` (there is no single value a match is "against"; every
/// caller of that arm assigns one flat hop to the whole result instead). A
/// `NULL` `from_col` never matches either arm (SQL `NULL`, or the explicit
/// `is not null` filter), exactly like the evaluator's LEFT JOIN no-match.
///
/// `from_pk` may be composite (issue #126): the returned `from_pk_text` is
/// [`ddl::pk_key_sql_expr`]'s row identity, in the same U+001F-joined shape
/// [`read_live_rows_batch`] later decodes it back with — `from_col` itself
/// (the relationship's own join column) is always a single column regardless
/// of the from-table's primary key arity, so its matching is unaffected.
async fn from_side_keys(
    pool: &Pool,
    from_table: &str,
    from_pk: &[PrimaryKeyColumn],
    from_col: &str,
    trigger: &ReverseTrigger<'_>,
) -> Result<Vec<(String, Option<String>)>, ApplyError> {
    match trigger {
        ReverseTrigger::Keys(join_keys) => {
            if join_keys.is_empty() {
                return Ok(Vec::new());
            }
            let client = pool.get().await?;
            let col_ident = quote_ident(from_col);
            let pg_type = key_column_pg_type(pool, from_table, from_col).await?;
            let filter = key_array_filter(&col_ident, pg_type.as_deref());
            let sql = format!(
                "select {pk}, {col_ident}::text \
                 from {tbl} \
                 where {filter}",
                pk = ddl::pk_key_sql_expr(from_pk, None),
                tbl = ddl::qualified_source_table(from_table),
            );
            let rows = client.query(&sql, &[join_keys]).await?;
            Ok(rows
                .into_iter()
                .map(|r| (r.get::<_, String>(0), Some(r.get::<_, String>(1))))
                .collect())
        }
        ReverseTrigger::WholeKeyspace => {
            let client = pool.get().await?;
            let sql = format!(
                "select {pk} from {tbl} where {col} is not null",
                pk = ddl::pk_key_sql_expr(from_pk, None),
                col = quote_ident(from_col),
                tbl = ddl::qualified_source_table(from_table),
            );
            let rows = client.query(&sql, &[]).await?;
            Ok(rows
                .into_iter()
                .map(|r| (r.get::<_, String>(0), None))
                .collect())
        }
    }
}

/// Resolves a batch's touched to-side join keys (`key_hops`'s keys, each
/// mapped to the max `hop_gen` that touched it, with `key_src_changed`
/// carrying the matching earliest `src_changed`) into `rel`'s from-side
/// rows, and merges each one into `compute`'s shared `reverse_recomputes`
/// accumulator at `hop + 1`.
///
/// Extracted from the to-many reverse branch so issue #244's image-less
/// to-one trigger handling can reuse it verbatim rather than open-code a
/// second, drifting copy — the two differ only in *which* changes they
/// collect keys from, never in how a collected key becomes a from-side
/// recompute. The accumulator's own `(from_table, from_key)` keying (issue
/// #79's cross-relationship dedupe) and its `max`/`earliest_src_changed`
/// merge rules are the shared part, so both callers get them for free.
async fn accumulate_from_side_recomputes(
    pool: &Pool,
    rel: &crate::defs::RelationshipDefinition,
    key_hops: &HashMap<String, i32>,
    key_src_changed: &HashMap<String, Provenance>,
    reverse_recomputes: &mut HashMap<(String, String), (i32, Provenance)>,
    skip_frozen: FromSideFence<'_>,
) -> Result<(), ApplyError> {
    // Issue #267: this accumulator's entries become staged `src_table`s
    // verbatim ([`apply_and_mark_drained_many`]'s step 4), so they carry the
    // from-table's qualified identity — the spelling capture stages for this
    // same from-side table (which, being a genuine source table, is normally
    // captured), and the spelling the Phase 3
    // `relationship_reverse_fallback` twin of this path has always used
    // (`ReverseRelationshipShape::from_table` is already qualified). Two
    // spellings of one table fold as two unrelated `(src_table, key)` groups,
    // and a batch that upserts both hands Postgres the same conflict key
    // twice (issue #267's live-lock). Issue #288: taken from the
    // relationship's own recorded `from_schema`, not by re-resolving the
    // bare `from_table` through this session's `search_path`, which could
    // land on a same-named table in another schema — and the from-side
    // reads below use it for the same reason.
    accumulate_from_side_recomputes_on(
        pool,
        &rel.qualified_from_table(),
        &rel.def.from_col,
        key_hops,
        key_src_changed,
        reverse_recomputes,
        skip_frozen,
    )
    .await
}

/// [`accumulate_from_side_recomputes`] for a relationship given by its
/// qualified from-table and `from_col`, as a deferred reverse's
/// [`ReverseRelationshipShape`] holds them (#784). `skip_frozen` is
/// [`from_side_key`]'s.
async fn accumulate_from_side_recomputes_on(
    pool: &Pool,
    qualified_from_table: &str,
    from_col: &str,
    key_hops: &HashMap<String, i32>,
    key_src_changed: &HashMap<String, Provenance>,
    reverse_recomputes: &mut HashMap<(String, String), (i32, Provenance)>,
    skip_frozen: FromSideFence<'_>,
) -> Result<(), ApplyError> {
    if key_hops.is_empty() {
        return Ok(());
    }
    let join_keys: Vec<String> = key_hops.keys().cloned().collect();
    let Some(from_pk) = from_side_key(pool, qualified_from_table, skip_frozen).await? else {
        return Ok(());
    };
    let matches = from_side_keys(
        pool,
        qualified_from_table,
        &from_pk,
        from_col,
        &ReverseTrigger::Keys(&join_keys),
    )
    .await?;
    for (from_key, join_text) in matches {
        // `Keys` always reports which key matched — see `from_side_keys`'s
        // own doc comment.
        let join_text =
            join_text.expect("ReverseTrigger::Keys always reports the matched join value");
        let hop = key_hops.get(&join_text).copied().unwrap_or(0) + 1;
        let (src_changed, origin_lsn) = key_src_changed
            .get(&join_text)
            .copied()
            .unwrap_or((None, None));
        reverse_recomputes
            .entry((qualified_from_table.to_string(), from_key))
            .and_modify(|(h, (sc, origin))| {
                *h = (*h).max(hop);
                *sc = earliest_src_changed(*sc, src_changed);
                *origin = earliest_origin(*origin, origin_lsn);
            })
            .or_insert((hop, (src_changed, origin_lsn)));
    }
    Ok(())
}

/// One to-one relationship's settled-parent projection keys a batch's
/// relationship resolution touched (issue #130, epic #127; plan doc §2's
/// guard (b) precondition — "the generation must be bumped... so a reverse
/// can detect that a forward apply landed in between"). [`build_relationship_context`]
/// resolves this in Phase 2 (the same catalog read that finds the projection
/// table to query), so Phase 3 ([`apply_and_mark_drained_many`]'s gen-bump
/// step) needs no catalog/pool access of its own to apply it — the same
/// "decide in Phase 2, apply in Phase 3" split the rest of [`ApplyPlan`]
/// already uses.
///
/// **Issue #133 (closed the gap this comment used to describe):**
/// `touched_keys` used to be derived only from the *folded* change's own
/// old- and new-image join-key values — both folded endpoints (a re-point
/// bumps both the old and new parent), never the pre-fold history. A parent
/// erased by the fold within one batch (`ins(post 3)` + `repoint(3 -> 2)`
/// folding to `new={post: 2}`, with post 3 appearing in neither folded
/// endpoint) was invisible there too, so it never got bumped even though a
/// child briefly pointed at it mid-batch — a reverse holding an enumeration
/// captured before the re-point would then wrongly pass guard (b)'s check
/// against post 3's `gen`.
///
/// [`build_relationship_context`] now unions in each touched change's own
/// [`FoldedChange::group_key`] — the ring's real, pre-fold "every join-key
/// value this row's raw change history touched" signal (see that field's
/// and `staging::fold`'s doc comments for the union merge rule) — on top of
/// the folded-endpoint values it already collected. That union is a strict
/// superset of the old signal (an extra touched key only ever costs an
/// UPDATE that matches zero rows — see the Phase 3 gen-bump step's own doc
/// comment), so keeping both sources rather than replacing one with the
/// other can only add coverage, never regress it, if `group_key` is ever
/// unpopulated for some row (e.g. a table with no cached outbound
/// relationship yet — see `intake::Intake`'s own doc comment on that
/// cache's refresh-on-miss strategy).
#[derive(Debug, Clone)]
pub(crate) struct RelationshipGenBump {
    /// [`ddl::qualified_relationship_projection_table`]'s output — ready for
    /// direct interpolation into the Phase 3 `UPDATE`.
    qualified_projection: String,
    /// The projection's own primary key column (the relationship's `to_col`).
    key_col: String,
    touched_keys: std::collections::HashSet<String>,
}

/// Builds the [`RelationshipContext`] a relationship-enriched from-side target
/// needs to re-evaluate (issue #30 wiring of the #28/#29 evaluator): for each
/// relationship the definition references, the related to-side rows keyed by
/// their `to_col` text, plus the referenced to-side columns' types. Join keys
/// are the distinct `from_col` values of the from-side rows this batch will
/// evaluate — so only the related rows those rows actually need are fetched.
/// `qualified_source` is the definition's own qualified source table
/// ([`crate::defs::Definition::source_table`]) — a relationship's from-table,
/// and the key its name is unique under (issue #288).
///
/// **Issue #130, epic #127**: a to-one relationship (`RelationshipCardinality::ToOne`)
/// resolves against the settled parent projection (#129's
/// `catalog::relationship_projection`), never a live read of the to-side —
/// see this module's doc comment / plan doc §2 for why a live read
/// double-counts the `δA⋈δB` cross term on the forward path. A to-many
/// relationship is untouched: Phase 1 of this epic is to-one relationship
/// *values* only (#94's shape), so `ToManyRelationship` still resolves via
/// [`fetch_to_side_rows`]'s live read, exactly as before.
///
/// `old_rows`, when supplied, is the same-length, same-index decoded
/// pre-image of each of `rows`' underlying changes — used only to widen the
/// gen-bump touched-key set (see [`RelationshipGenBump`]'s doc comment) with
/// each change's *old* join-key value, not to resolve anything the evaluator
/// reads. `None` is the shape a direct writer's Re-derive ([`DirectRederive`])
/// passes, since it isn't part of the staging
/// ring's claim/fold/compute/apply pipeline this gen bump guards — that
/// caller discards the returned gen-bump map entirely, so `None` simply
/// costs it nothing beyond not bothering to compute the old-side half.
///
/// `changes`, when supplied, is the same-length, same-index slice of
/// [`FoldedChange`]s `rows`/`old_rows` were decoded from — issue #133's
/// signal, read for its `group_key` (the real, pre-fold union of touched
/// join keys; see that field's doc comment) and unioned into the same
/// gen-bump touched-key set `old_rows` widens. `None` for the same
/// [`DirectRederive`] caller as `old_rows`: that path has no
/// `FoldedChange`s at all (a live full-table scan, not the staging ring's
/// pipeline) and, as above, discards the gen-bump map regardless.
pub(crate) async fn build_relationship_context(
    pool: &Pool,
    qualified_source: &str,
    def: &TransformDef,
    rows: &[Option<Row>],
    old_rows: Option<&[Option<Row>]>,
    changes: Option<&[&FoldedChange]>,
) -> Result<(RelationshipContext, HashMap<i64, RelationshipGenBump>), ApplyError> {
    // Group the referenced columns by relationship name (a relationship may be
    // read for more than one column across the definition's fields).
    let mut cols_by_rel: HashMap<String, Vec<String>> = HashMap::new();
    for (rel, column) in eval::relationship_references(def) {
        let cols = cols_by_rel.entry(rel).or_default();
        if !cols.contains(&column) {
            cols.push(column);
        }
    }

    let mut by_name: HashMap<String, ToOneRelationship> = HashMap::new();
    let mut to_many_by_name: HashMap<String, ToManyRelationship> = HashMap::new();
    let mut gen_bumps: HashMap<i64, RelationshipGenBump> = HashMap::new();

    for (rel_name, columns) in cols_by_rel {
        let Some(reldef) =
            catalog::relationship_on_source(pool, qualified_source, &rel_name).await?
        else {
            // Unknown relationship: leave it out and let the evaluator surface
            // `EvalError::UnknownRelationship`, the same as the pure path.
            continue;
        };
        let from_col = reldef.def.from_col.clone();
        let to_col = reldef.def.to_col.clone();
        // Issue #372: the to-side the relationship was declared against,
        // never the bare `to_table` re-resolved through this session's
        // `search_path`. Unquoted (issue #561): the lookups below quote it
        // for `to_regclass`, and `fetch_to_side_rows` for interpolation.
        let to_table = reldef.qualified_to_table();

        // The join keys we need on the to-side: the distinct non-NULL
        // `from_col` values of the from-side rows this batch evaluates.
        //
        // Issue #136: also folds in `old_rows`' own `from_col` values, not
        // just `rows`' (new-side) — a `KeySpace::OneToOne` caller never
        // needed this (it only ever evaluates the *new* row, so the old
        // parent's value is never read), but the forward aggregate delta
        // path does: it must resolve a row's contribution under *both* its
        // old and new relationship value when the row's own `from_col`
        // itself changes within one folded update (a re-point), to subtract
        // the old contribution and add the new one rather than silently
        // treating the old side as "no match". Folding in an extra key here
        // only ever costs fetching one more (unused) projection row for a
        // `KeySpace::OneToOne` caller — never a correctness problem, per
        // this function's own "spurious extra touched key" rule below.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut join_keys: Vec<String> = Vec::new();
        let old_rows_iter = old_rows.unwrap_or(&[]).iter().flatten();
        for row in rows.iter().flatten().chain(old_rows_iter) {
            if let Some(Some(text)) = row.get(&from_col)
                && seen.insert(text.as_str())
            {
                join_keys.push(text.clone());
            }
        }

        let to_columns = to_column_types(pool, &to_table, &columns).await?;

        match reldef.cardinality {
            RelationshipCardinality::ToOne => {
                let projection = catalog::relationship_projection(pool, reldef.id).await?;
                let qualified_projection = projection
                    .as_ref()
                    .map(catalog::RelationshipProjection::qualified_table);

                let to_rows_by_key = match &projection {
                    Some(projection) => {
                        fetch_relationship_projection_rows(
                            pool, projection, &to_table, &to_col, &join_keys,
                        )
                        .await?
                    }
                    None => {
                        // Every to-one relationship gets a projection
                        // unconditionally at `create_relationship` time
                        // (#129's `ensure_relationship_projection_in_txn`) —
                        // this should be unreachable. Phase 2 holds no locks
                        // and can't assume the catalog is self-consistent on
                        // that promise alone, so this degrades to "nothing
                        // resolves" (an empty to-side, same as a genuinely
                        // dangling join key) rather than panicking.
                        tracing::error!(
                            relationship = %rel_name,
                            from_table = %qualified_source,
                            "to-one relationship has no settled parent projection; \
                             resolving as empty (should be unreachable — #129 creates \
                             one unconditionally)"
                        );
                        HashMap::new()
                    }
                };
                by_name.insert(
                    rel_name.clone(),
                    ToOneRelationship {
                        from_col: from_col.clone(),
                        cardinality: RelationshipCardinality::ToOne,
                        to_columns,
                        to_rows_by_key,
                    },
                );

                // #130's gen-bump signal, widened by #133 — see
                // [`RelationshipGenBump`]'s doc comment. Three sources, all
                // unioned (a spurious extra touched key only ever costs a
                // zero-row `UPDATE`, never a correctness problem — see the
                // Phase 3 gen-bump step's own doc comment): the folded
                // endpoints (both folded new-side join keys just resolved
                // above, and every change's own folded old-image `from_col`
                // value — a re-point's *previous* parent, which the
                // new-side scan never sees), plus #133's real pre-fold
                // signal: every touched change's own `group_key` union,
                // which is what still names a parent (like the plan doc's
                // "post 3") the fold erased from *both* folded endpoints
                // within this same batch.
                if let Some(qualified_projection) = qualified_projection {
                    let mut touched: std::collections::HashSet<String> =
                        join_keys.iter().cloned().collect();
                    if let Some(old_rows) = old_rows {
                        for old_row in old_rows.iter().flatten() {
                            // Issue #677: a to-one's from-side images
                            // `from_col` (it is in its capture set), so an
                            // old image missing it is `MissingColumn`, not a
                            // parent silently left un-bumped.
                            if let Some(text) = required_column(old_row, &from_col, &rel_name)? {
                                touched.insert(text.clone());
                            }
                        }
                    }
                    if let Some(changes) = changes {
                        for change in changes {
                            if let Some(group_key) = &change.group_key {
                                touched.extend(group_key.iter().cloned());
                            }
                        }
                    }
                    if !touched.is_empty() {
                        gen_bumps
                            .entry(reldef.id)
                            .or_insert_with(|| RelationshipGenBump {
                                qualified_projection,
                                key_col: to_col.clone(),
                                touched_keys: std::collections::HashSet::new(),
                            })
                            .touched_keys
                            .extend(touched);
                    }
                }
            }
            RelationshipCardinality::ToMany => {
                let grouped = fetch_to_side_rows(pool, &to_table, &to_col, &join_keys).await?;
                to_many_by_name.insert(
                    rel_name,
                    ToManyRelationship {
                        from_col,
                        to_columns,
                        to_rows_by_key: grouped,
                    },
                );
            }
        }
    }

    Ok((
        RelationshipContext::new(by_name).with_to_many(to_many_by_name),
        gen_bumps,
    ))
}

// ---------------------------------------------------------------------
// Issue #131, epic #127: the to-one reverse delta
// ---------------------------------------------------------------------
//
// Design summary (see the plan doc's §2 "Reverse" and §7 Phase 1 steps 4-5,
// and this function's own call site in `compute`'s `inbound_rels` loop):
//
// A parent (to-side) change to a to-one relationship no longer stages an
// image-less from-side `Recompute` per touched from-side row (the pre-#131,
// still-current behavior for to-*many* relationships — see the branch in
// `compute` below). Instead it builds one [`RelationshipReverseRecord`] per
// touched parent key, carrying the parent's own old/new image (already
// folded — see this struct's doc comment for why no new ring plumbing is
// needed to get that), a `prev_lsn`/`prev_gen` pair read live off the
// settled parent projection at Phase 2 capture time, and (issue #132) `X`,
// the source's write frontier captured in that same round trip. Phase 3
// ([`apply_and_mark_drained_many`]'s "3d" step, [`check_reverse_guards`])
// re-validates all four of #132's guards — (a) the watermark barrier, (b)
// the generation check, (c) the in-flight check, and (d) #131's own
// `prev_lsn` ordering check, re-read under the same `FOR UPDATE` lock as
// (b) — then advances the projection. When some definition on the from-table
// reads the relationship, it also stages an image-less `Recompute` for every
// from-side row the parent reaches, the same one any of #132's four guards
// stages when it rejects a record. A 1-1 target re-reads the row; an
// aggregate re-derives the row's ledger entry, reading the parent live, and
// the entry already names the group the row was in (#623 D5, which deleted
// #131's true-delta fast path).

/// One to-one relationship's reverse-delta shape (issue #131): everything
/// Phase 3 needs to apply a [`RelationshipReverseRecord`] for this
/// relationship, resolved once per relationship per `compute()` call (Phase
/// 2, which has a `pool`) and shared, via `Arc`, by every parent key this
/// batch's fold touched for it — Phase 3 holds no `pool`, only `txn` (see
/// [`apply_and_mark_drained_many`]'s own doc comment), so nothing here can
/// be re-derived once Phase 3 starts.
#[derive(Debug)]
pub(crate) struct ReverseRelationshipShape {
    /// `relationship_definitions.id` — issue #134's deferred-reverse
    /// persistence needs this to build
    /// [`append::StagedChange::RelationshipReverseDeferred`]'s synthetic
    /// fold identity (`relationship_reverse_deferred_src_table`) and its own
    /// `relationship_id` column, and to look this shape back up
    /// (`catalog::relationship_by_id`) on a later drain without re-deriving
    /// it from anything else persisted.
    id: i64,
    /// The relationship's declared name — what a from-side definition's
    /// `<rel>.<column>` path reads.
    name: String,
    /// Empty (`String::new()`) in the should-be-unreachable case where this
    /// relationship has no projection row at all (#129 creates one
    /// unconditionally at `create_relationship` time) — Phase 3 treats that
    /// the same as an ordering-check miss for every record carrying this
    /// shape, the same defensive posture [`build_relationship_context`]
    /// takes for the identical gap on the forward path.
    qualified_projection: String,
    /// The projection's bare (unquoted) table name — `qualified_projection`
    /// is already quoted/schema-qualified for direct SQL interpolation, so
    /// the projection-advance step needs this separately to introspect
    /// `information_schema.columns` (issue #131's own write path: which
    /// data columns to copy off the parent's new image).
    projection_table_bare: String,
    /// The schema `projection_table_bare` lives in (the instance's catalog
    /// schema, issue #435) — bare, for the same `information_schema.columns`
    /// introspection.
    projection_schema: String,
    to_col: String,
    from_table: String,
    from_col: String,
    /// The from-table's primary key, possibly composite (issue #126) — see
    /// [`from_side_rows_for_trigger_txn`]'s doc comment for how a
    /// multi-column key's row identity is encoded/decoded. `None` when it
    /// can't be used and every definition reading the from-table is frozen
    /// ([`from_side_key`], issue #768): no recompute of its rows is staged.
    from_pk: Option<Vec<PrimaryKeyColumn>>,
    /// Whether some definition on `from_table` reads this relationship. Each
    /// then re-derives every from-side row a parent change reaches: a 1-1
    /// target re-reads the row, and an aggregate on the ledger reads the
    /// parent live (#623 D5).
    needs_recompute_fallback: bool,
    /// The relationship's to-side, read by key for the live-row check
    /// ([`superseded_to_side`]).
    to_side: ToSide,
}

/// A relationship's to-side as Phase 3 reads its live row by key (issues
/// #507, #531).
#[derive(Debug)]
struct ToSide {
    /// The to-side table, quoted and qualified for direct interpolation.
    table: String,
    /// The to-side's unquoted `schema.table` identity, for catalog lookups
    /// ([`ddl::regclass_arg`], issue #561).
    identity: String,
    /// Whether the to-side is one of this instance's own targets, fed to
    /// the reverse path by the target-mutation seam (issue #507), so every
    /// record on it takes the live-row check. A source to-side's record
    /// takes it only at or below the relationship's refresh stamp (issue
    /// #531, [`overtaken_by_refresh`]).
    seam_fed: bool,
    /// `to_col`'s type ([`key_column_pg_type`]), so the lookup by key can
    /// use the to-side's own unique index on it. Resolved in Phase 3 by the
    /// first record that takes the live-row check, so a batch where none
    /// does never reads it.
    key_pg_type: std::sync::OnceLock<Option<String>>,
}

impl ToSide {
    /// The `where` condition matching the to-side row whose `to_col` is
    /// `$1` (text), aliased `t`.
    async fn key_filter(&self, txn: &Transaction<'_>, to_col: &str) -> Result<String, ApplyError> {
        let key_pg_type = match self.key_pg_type.get() {
            Some(ty) => ty,
            None => {
                let ty = key_column_pg_type_in(txn, &self.identity, to_col).await?;
                self.key_pg_type.get_or_init(|| ty)
            }
        };
        let key_ident = quote_ident(to_col);
        Ok(match key_pg_type {
            Some(ty) => format!("t.{key_ident} = $1::text::{ty}"),
            None => format!("t.{key_ident}::text = $1"),
        })
    }
}

/// One to-one relationship's parent-keyed reverse record (issue #131),
/// built in `compute()` (Phase 2) from one already-folded [`FoldedChange`]
/// on the relationship's own `to_table`, applied in Phase 3.
///
/// **No new ring/staging-kind plumbing was needed to get *this* far** — a
/// deliberate, documented deviation from issue #131's own "what to actually
/// do" checklist, which speculated a new [`StagedChange`] variant might be
/// needed. It wasn't, for #131: the parent's own CDC rows are *already*
/// folded, generically, by the existing [`fold::fold`]/
/// [`fold::merge_folded_changes`] (any table's raw CDC rows for one key
/// collapse to one [`FoldedChange`] — first old image, last new image,
/// `lsn` the group's `GREATEST` — before `compute()` ever sees them), so "N
/// parent changes for the same key in one batch fold to one record" falls
/// out of machinery that already exists, for free, the same way it already
/// does for every other table. The only genuinely new datum was `prev_lsn`,
/// which #131's own derivation (confirmed against
/// `ddl::PROJECTION_LSN_COLUMN`'s doc comment, written for #129/#130 ahead
/// of #131 landing) is a **live read of the projection, taken once in
/// Phase 2**, not something that needs to ride along a raw ring row.
///
/// Issue #134 *does* add a real persisted staging kind —
/// [`StagedChange::RelationshipReverseDeferred`], its own `retry_count`
/// field on this struct below, and `compute()`'s dedicated deferred-
/// reconstruction loop (right after the by-source loop) — but only for the
/// narrower case #131 deliberately deferred: a record that issue #132's
/// guards *rejected* needs to be retried later without burning `hop_gen`,
/// which does need to survive across a drain, unlike `prev_lsn`
/// (re-derived live, same as #131 always did) or `prev_gen`/`watermark`
/// (#132's additions, re-derived live the same way on a #134 retry —
/// see [`StagedChange::RelationshipReverseDeferred`]'s own doc comment for
/// why replaying either stale would be unsound). A record built fresh from
/// raw CDC (this struct's other construction site) still needs no new ring
/// plumbing at all, exactly as #131 established; only a *deferred* one
/// does.
///
/// **Issue #132's additions** (guards (a)/(b), alongside #131's own
/// `prev_lsn` for guard (d)): `prev_gen` and `watermark` are captured in the
/// exact same Phase 2 round trip as `prev_lsn` (see
/// [`capture_reverse_guard_state`]) rather than as separate reads — the
/// issue's own "capture X in the same statement as the enumeration"
/// requirement for guard (a), and the natural place to also capture guard
/// (b)'s `gen` alongside guard (d)'s `lsn`, since both come off the exact
/// same projection row.
#[derive(Debug, Clone)]
pub(crate) struct RelationshipReverseRecord {
    shape: Arc<ReverseRelationshipShape>,
    /// The parent's decoded pre-image row, or `None` for a parent INSERT.
    old_row: Option<Row>,
    /// The parent's decoded post-image row, or `None` for a parent DELETE.
    new_row: Option<Row>,
    /// The parent's raw pre-image JSON text (unparsed), mirroring
    /// `new_image` below — `old_row`'s own decode source. Unlike
    /// `new_image`, nothing in the pre-#134 apply path ever needed this
    /// text form (the projection's delete half only needs `old_row`'s
    /// *key*), so it went unstored until issue #134: a guard-rejected
    /// record now needs to persist it verbatim into
    /// [`append::StagedChange::RelationshipReverseDeferred::old_image`]
    /// without re-serializing `old_row`.
    old_image: Option<String>,
    /// The parent's raw post-image JSON text (unparsed) — used by the
    /// projection's own UPSERT, which leans on Postgres's
    /// `jsonb_populate_record` to coerce JSON into the projection's real
    /// column types rather than this crate re-deriving a per-column cast,
    /// and (issue #134) by the guard-rejection deferral path for the same
    /// reason `old_image` above is now stored.
    new_image: Option<String>,
    /// The folded change's own `GREATEST` `lsn` — this record's own
    /// identity in the projection's LSN chain once it applies.
    lsn: Option<PgLsn>,
    /// The projection's `__trellis_lsn` as read live, in Phase 2, for
    /// whichever of `old_row`/`new_row`'s key was available (preferring the
    /// old key, the pre-this-batch identity) — see this struct's own doc
    /// comment and [`ddl::PROJECTION_LSN_COLUMN`]'s for the chain this
    /// forms. `None` when no projection row existed yet to read (a parent
    /// INSERT, or the should-be-unreachable missing-projection case) —
    /// Phase 3 treats that as "nothing to conflict with," not a miss.
    ///
    /// Guard (d) (#131's original stopgap, formalized as one of #132's four
    /// guards): Phase 3 re-reads this same column under `FOR UPDATE` and
    /// requires it still equal `prev_lsn` before applying.
    prev_lsn: Option<PgLsn>,
    /// Issue #132 guard (b): the projection row's [`ddl::PROJECTION_GEN_COLUMN`]
    /// as read live, in Phase 2, alongside `prev_lsn` above (same query, same
    /// row, same "taken once in Phase 2" shape) — `None` under the identical
    /// conditions `prev_lsn` is `None` (no projection row yet). Phase 3
    /// re-reads it under the same `FOR UPDATE` lock `prev_lsn`'s re-check
    /// uses and requires it unchanged: a forward apply that resolved this
    /// parent through the projection between Phase 2's capture and Phase 3's
    /// apply bumps this column (`apply_and_mark_drained_many`'s "3c" step),
    /// so a mismatch here means this reverse's enumeration may no longer
    /// equal the parent's *applied* value.
    prev_gen: Option<i64>,
    /// Issue #132 guard (a): `X`, "the source's write frontier," captured in
    /// the same Phase 2 statement as `prev_lsn`/`prev_gen` above (via
    /// `pg_current_wal_insert_lsn()` against the same connection). Phase 3
    /// must not apply this record until [`StagedWatermark::get`] reports
    /// everything committed at or before this value is staged (always, under
    /// trigger capture). Guard (c) also bounds its in-flight check by it, and
    /// there a lower value is the unsafe direction: it would miss a committed
    /// change's pending ring rows, which is why this is the insert position
    /// and not the write position (issue #697).
    watermark: PgLsn,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    /// The parent change's origin (issue #469), carried to every row this
    /// record stages, as `src_changed` is.
    origin_lsn: Option<PgLsn>,
    /// Issue #134: how many times this exact reverse (same relationship,
    /// same parent key, same underlying transition) has already been
    /// deferred by a guard rejection — `0` for a record built fresh from
    /// raw parent CDC, or the persisted
    /// [`append::StagedChange::RelationshipReverseDeferred::retry_count`]
    /// for one reconstructed from a previously-deferred ring row. Never
    /// derived from, or folded into, `hop_gen` above — see that field's own
    /// migration/doc comment for why the two must stay independent.
    retry_count: i32,
}

/// Issue #134: the synthetic `src_table` every
/// [`append::StagedChange::RelationshipReverseDeferred`] for relationship
/// `relationship_id` is staged under — see that variant's own doc comment
/// for why it must be distinct from the relationship's real `to_table` (so
/// this op's rows never fold, at the SQL fold's `(src_table, key)` grouping,
/// with genuine CDC on the parent's real table, which would let them reach
/// the ordinary per-source forward-evaluation loop and double-apply an
/// already-forward-applied delta) and distinct *per relationship* (so two
/// different relationships pointing at keys that happen to share text never
/// fold together either). Prefixed with U+001F (INFORMATION SEPARATOR ONE),
/// the same "no real qualified table name contains this" assumption
/// [`append::TRUNCATE_SENTINEL_KEY`] already relies on — a real
/// `information_schema`-qualified table name can't contain it.
pub(crate) fn relationship_reverse_deferred_src_table(relationship_id: i64) -> String {
    format!("\u{1f}trellis-rel-reverse-deferred:{relationship_id}")
}

/// Builds `rel`'s [`ReverseRelationshipShape`] (issue #131) — the one-time,
/// per-relationship, per-`compute()`-call catalog resolution every parent
/// key this batch's fold touches for `rel` shares (see
/// [`RelationshipReverseRecord`]'s doc comment).
///
/// `needs_recompute_fallback` is set when any definition on the from-table
/// reads the relationship; every such definition, 1-1 or aggregate, then
/// re-derives each from-side row the parent reaches (#623 D5).
async fn build_reverse_relationship_shape(
    pool: &Pool,
    rel: &RelationshipDefinition,
    skip_frozen: FromSideFence<'_>,
) -> Result<ReverseRelationshipShape, ApplyError> {
    let projection = catalog::relationship_projection(pool, rel.id).await?;
    let (qualified_projection, projection_schema, projection_table_bare) = match projection {
        Some(p) => (p.qualified_table(), p.projection_schema, p.projection_table),
        None => {
            tracing::error!(
                relationship = %rel.def.name,
                to_table = %rel.qualified_to_table(),
                "to-one relationship has no settled parent projection; every \
                 reverse record for it will be treated as an ordering-check \
                 miss (should be unreachable — #129 creates one unconditionally)"
            );
            (String::new(), String::new(), String::new())
        }
    };
    // `transforms_for_source` matches `schema_nodes.table_name` exactly
    // (ADR-0007's fully-qualified keying) and every SQL-emitting site below
    // (`from_side_rows_for_trigger_txn`'s `ddl::qualified_source_table`) documents the same requirement, so this
    // shape stores the qualified form of `from_table` throughout — the
    // relationship's own recorded one (issue #288), never `rel.def.from_table`
    // re-resolved through this session's `search_path`.
    let qualified_from_table = rel.qualified_from_table();
    let from_pk = from_side_key(pool, &qualified_from_table, skip_frozen).await?;
    let defs = catalog::transforms_for_source(pool, &qualified_from_table).await?;

    let needs_recompute_fallback = defs.iter().any(|def| {
        eval::relationship_references(&def.def)
            .iter()
            .any(|(name, _)| name == &rel.def.name)
    });
    let qualified_to_table = rel.qualified_to_table();
    let seam_fed = {
        let client = pool.get().await?;
        catalog::is_definition_target(&**client, &qualified_to_table).await?
    };
    let to_side = ToSide {
        table: ddl::qualified_source_table(&qualified_to_table),
        identity: qualified_to_table,
        seam_fed,
        key_pg_type: std::sync::OnceLock::new(),
    };

    Ok(ReverseRelationshipShape {
        id: rel.id,
        name: rel.def.name.clone(),
        qualified_projection,
        projection_table_bare,
        projection_schema,
        to_col: rel.def.to_col.clone(),
        from_table: qualified_from_table,
        from_col: rel.def.from_col.clone(),
        from_pk,
        needs_recompute_fallback,
        to_side,
    })
}

/// The `to_col` text value off `row`, or `None` if `row` is absent or its
/// `to_col` is SQL `NULL` ("no key here" for join purposes). A present `row`
/// missing `to_col` entirely is [`EvalError::MissingColumn`] against
/// relationship `rel_name` (issue #677, see
/// [`required_column`]): `row` is the to-side change's own
/// image, and the join key is always part of it: `to_col` is in the
/// to-side's capture set, and a capture trigger images the whole row, an
/// unchanged TOASTed value included.
fn relationship_key_text(
    row: &Option<Row>,
    to_col: &str,
    rel_name: &str,
) -> Result<Option<String>, EvalError> {
    match row {
        Some(row) => Ok(required_column(row, to_col, rel_name)?.cloned()),
        None => Ok(None),
    }
}

/// [`relationship_key_text`] off the old image, falling back to the new one
/// when the old image is absent or its key is `NULL` — the key a to-one
/// reverse record's guard state is captured under.
fn relationship_read_key(
    old_row: &Option<Row>,
    new_row: &Option<Row>,
    to_col: &str,
    rel_name: &str,
) -> Result<Option<String>, EvalError> {
    match relationship_key_text(old_row, to_col, rel_name)? {
        Some(key) => Ok(Some(key)),
        None => relationship_key_text(new_row, to_col, rel_name),
    }
}

/// The Phase 2 capture behind three of #132's four guards — [`prev_lsn`],
/// [`prev_gen`], and [`watermark`] (guard (d)'s ordering check, guard (b)'s
/// generation check, and guard (a)'s watermark barrier, respectively; see
/// [`RelationshipReverseRecord`]'s doc comment for each field's role in
/// Phase 3).
///
/// [`prev_lsn`]: ReverseCapture::prev_lsn
/// [`prev_gen`]: ReverseCapture::prev_gen
/// [`watermark`]: ReverseCapture::watermark
struct ReverseCapture {
    prev_lsn: Option<PgLsn>,
    prev_gen: Option<i64>,
    watermark: PgLsn,
}

/// Live-reads the settled parent projection's current
/// [`ddl::PROJECTION_LSN_COLUMN`]/[`ddl::PROJECTION_GEN_COLUMN`] for `key`,
/// **and** captures guard (a)'s `X` — `pg_current_wal_insert_lsn()`, "the
/// source's write frontier" — in the very same statement, not as a separate
/// round trip. The insert position, not `pg_current_wal_lsn()`'s write
/// position (issue #697): a change committed with `synchronous_commit = off`
/// is visible to the live reads guard (c) protects before its WAL is written,
/// and its ring rows' `lsn` (an insert position) can be above the write
/// position, so guard (c) would not see it pending. A plain, unlocked read taken once in
/// Phase 2; Phase 3 (`apply_and_mark_drained_many`'s "3d" step,
/// `check_reverse_guards`) re-validates `prev_lsn`/`prev_gen` under `FOR
/// UPDATE` and re-checks the watermark against the live
/// [`StagedWatermark`].
///
/// `prev_lsn`/`prev_gen` are both `None` when `qualified_projection` is
/// empty (the should-be-unreachable no-projection case), `key` is `None`
/// (defensive — every record reaching this point has at least one of
/// old/new key, per `compute`'s own "both images absent" skip, but this
/// function does not assume that), or no projection row exists yet for
/// `key` (a parent that's about to be INSERTed) — in every such case this
/// still issues a bare `select pg_current_wal_insert_lsn()` so `watermark` is
/// always populated: guard (a) applies to every reverse record, including a
/// parent insert, not only ones with an existing projection row.
async fn capture_reverse_guard_state(
    pool: &Pool,
    qualified_projection: &str,
    to_col: &str,
    key: Option<&str>,
) -> Result<ReverseCapture, ApplyError> {
    let client = pool.get().await?;
    if let (Some(key), false) = (key, qualified_projection.is_empty()) {
        let lsn_ident = quote_ident(ddl::PROJECTION_LSN_COLUMN);
        let gen_ident = quote_ident(ddl::PROJECTION_GEN_COLUMN);
        let key_ident = quote_ident(to_col);
        let row = client
            .query_opt(
                &format!(
                    "select {lsn_ident}, {gen_ident}, pg_current_wal_insert_lsn() \
                     from {qualified_projection} where {key_ident}::text = $1"
                ),
                &[&key],
            )
            .await?;
        if let Some(row) = row {
            return Ok(ReverseCapture {
                prev_lsn: row.get(0),
                prev_gen: row.get(1),
                watermark: row.get(2),
            });
        }
    }
    let row = client
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await?;
    Ok(ReverseCapture {
        prev_lsn: None,
        prev_gen: None,
        watermark: row.get(0),
    })
}

// ---------------------------------------------------------------------
// Issue #131, epic #127: Phase 3 helpers for the reverse-delta apply
// ---------------------------------------------------------------------

/// Live-enumerates `from_table`'s rows a [`ReverseTrigger`] matches, full row
/// images, inside the already-locked Phase 3 transaction —
/// [`apply_and_mark_drained_many`]'s "3d" step's from-side enumeration, used
/// by the reverse fallback ([`stage_reverse_recompute_fallback`]).
///
/// **This reads live state** — safe only because [`check_reverse_guards`]
/// (issue #132) has already run, immediately before every call site below,
/// and proven the live state *equals* the applied one for the specific
/// join key(s) this call is about to read: guard (a) (the watermark
/// barrier) plus guard (c) (the in-flight check) together prove nothing
/// committed at or before this record's captured `X` is still mid-flight
/// for these keys, and guards (b)/(d) prove no forward apply or
/// out-of-order sibling reverse landed between Phase 2's enumeration and
/// this transaction's lock. Before #132, this doc comment flagged that gap
/// as open; it is now closed by every caller's own guard check, not by
/// anything in this function itself — this function still does nothing on
/// its own to enforce it, so a *new* caller added later must run the guard
/// check first too.
///
/// [`ReverseTrigger::Keys`] is looped one join key at a time — the exact
/// per-key round trip both callers below already made before issue #173
/// phase 3 folded their own loops into this function; a caller passing both
/// an old and new key that happen to be equal still issues one query per
/// slice entry, unchanged from before (the caller's own `seen_keys` dedup is
/// what collapses the resulting duplicate rows, exactly as it always has).
///
/// `row_columns` is `from_table`'s live column list (issue #248's
/// `row_as_text_jsonb_sql` needs it in place of `to_jsonb(t.*)` — see that
/// function's doc comment), **resolved by the caller, not here**: this
/// function is called once per distinct touched parent key in Phase 3's own
/// `for record in &plan.relationship_reverses` loop
/// (`apply_and_mark_drained_many`'s "3d" step, via
/// [`stage_reverse_recompute_fallback`]), and `from_table` is invariant across many
/// records sharing one relationship — introspecting it fresh on every call
/// would be a real per-record `pg_catalog` round trip on a path that already
/// fans out with wide reverse-relationship batches (a regression this fix
/// did not have before issue #248 introduced this parameter). The caller
/// resolves it once per distinct `from_table`, cached across that whole
/// loop, and passes the same slice into every call.
/// [`ReverseTrigger::WholeKeyspace`] is unreachable here, for two independent
/// reasons. Structurally: both call sites construct [`ReverseTrigger::Keys`]
/// inline from a single join key they already hold, and no function in this
/// module takes a `ReverseTrigger` and forwards one, so no dynamically-chosen
/// variant can arrive here at all. Semantically: a `TRUNCATE`'s key-less
/// sentinel never has an image to build a [`RelationshipReverseRecord`] from
/// in the first place (see that struct's own construction site, and
/// `ReverseTrigger`'s doc comment), so neither caller — both exclusively fed
/// by `RelationshipReverseRecord`s — would have one to pass even if the
/// plumbing allowed it.
///
/// It is nonetheless a typed [`ApplyError::ReverseTriggerNotResolvable`]
/// rather than a panic, per this module's own convention of not trusting
/// invariants it cannot enforce itself (the same reasoning
/// [`ApplyError::Validate`] and [`ApplyError::Backfill`] document) — and
/// pointedly because the paragraph above already concedes this function does
/// nothing to enforce its own preconditions against a *new* caller added
/// later. A future path that legitimately needs to resolve a `WholeKeyspace`
/// trigger against live, transactional full row images must implement and
/// test that arm for real; it must not be silently satisfied by a
/// `Keys`-shaped read, and it must not abort the drain worker either.
async fn from_side_rows_for_trigger_txn(
    txn: &Transaction<'_>,
    from_table: &str,
    from_col: &str,
    from_pk: &[PrimaryKeyColumn],
    trigger: &ReverseTrigger<'_>,
    row_columns: &[String],
) -> Result<Vec<(String, Row)>, ApplyError> {
    let join_keys: &[String] = match trigger {
        ReverseTrigger::Keys(join_keys) => join_keys,
        ReverseTrigger::WholeKeyspace => {
            return Err(ApplyError::ReverseTriggerNotResolvable {
                from_table: from_table.to_string(),
            });
        }
    };
    if join_keys.is_empty() {
        return Ok(Vec::new());
    }
    // Issue #248: an explicit per-column `jsonb_build_object`, not
    // `to_jsonb(t.*)` — see `row_as_text_jsonb_sql`'s doc comment.
    // `row_columns` arrives pre-resolved (see this function's own doc
    // comment on why it isn't introspected here).
    let doc_expr = row_as_text_jsonb_sql("t", row_columns);
    let mut rows: HashMap<String, Row> = HashMap::new();
    for join_key in join_keys {
        let sql = format!(
            "select m.k, e.key, e.value \
             from (select {pk} as k, {doc_expr} as doc from {tbl} t \
                   where {col}::text = $1) m \
             cross join lateral jsonb_each_text(m.doc) e",
            pk = ddl::pk_key_sql_expr(from_pk, Some("t")),
            col = quote_ident(from_col),
            tbl = ddl::qualified_source_table(from_table),
        );
        let db_rows = txn.query(&sql, &[join_key]).await?;
        for db_row in db_rows {
            let key: String = db_row.get(0);
            let field: String = db_row.get(1);
            let value: Option<String> = db_row.get(2);
            rows.entry(key).or_default().insert(field, value);
        }
    }
    Ok(rows.into_iter().collect())
}

// ---------------------------------------------------------------------
// Issue #132, epic #127: the four guards
// ---------------------------------------------------------------------
//
// See the plan doc's §2 guard table and this module's own "Issue #131,
// epic #127" section above for the mechanism these formalize. All four are
// checked together, in [`check_reverse_guards`], as one Phase 3 step per
// [`RelationshipReverseRecord`] — a failure on any one of them gets the
// exact same treatment: no target/projection write for this record, defer
// and re-stage it as a [`StagedChange::RelationshipReverseDeferred`] for a
// later drain to retry (issue #134) — see that variant's own doc comment,
// and the guard-rejection branch of `apply_and_mark_drained_many`'s "3d"
// step, for the mechanism. Before #134 landed, every rejection fell back to
// re-staging its from-side rows as an image-less `Recompute` at
// `hop_gen + 1` instead (the same stopgap #131 shipped for guard (d) alone,
// then shared by all four for #132/#133) — replaced outright, not kept as
// a fallback of its own.

/// Which of #132's four guards rejected a [`RelationshipReverseRecord`] —
/// carried only as far as the `tracing::warn!` in
/// [`apply_and_mark_drained_many`]'s "3d" step; every rejection gets
/// identical treatment downstream (the Recompute-fallback stopgap), so nothing
/// else branches on which variant fired. A typed enum rather than a bare
/// `&'static str` purely so a future metrics/observability pass (the plan
/// doc's Phase 1 step 7 flags exactly this need — "the deferral counters
/// should become engine metrics") has something to match on without
/// re-parsing a log message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReverseGuardFailure {
    /// Guard (a): the ring does not yet hold everything committed at or
    /// before this record's captured watermark `X`.
    Watermark,
    /// Guard (b): the projection row's `__trellis_gen` moved since Phase 2's
    /// capture — a forward apply landed in between.
    Generation,
    /// Guard (c): a staged from-side change for this parent's old/new join
    /// key, committed at or before `X`, is still undrained.
    InFlight,
    /// Guard (d): the projection row's `__trellis_lsn` no longer matches
    /// this record's `prev_lsn` — an out-of-order sibling reverse (or this
    /// same one, replayed) already advanced it, or moved it out from under
    /// this one.
    Ordering,
}

impl fmt::Display for ReverseGuardFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ReverseGuardFailure::Watermark => "watermark barrier (guard a)",
            ReverseGuardFailure::Generation => "generation check (guard b)",
            ReverseGuardFailure::InFlight => "in-flight check (guard c)",
            ReverseGuardFailure::Ordering => "per-parent ordering (guard d)",
        };
        f.write_str(s)
    }
}

impl ReverseGuardFailure {
    /// The [`crate::metrics::increment_relationship_reverse_deferred`]
    /// label this guard's rejection records under (issue #134/#135) — the
    /// plan doc's own `d5_block_*` names (§7 step 7's "the deferral
    /// counters (`d5_block_*`) should become engine metrics"), used
    /// verbatim as label *values* on one counter rather than as four
    /// separate metric names, matching this crate's existing
    /// one-metric-plus-label convention (`transform`, `state`, ...).
    pub(crate) fn metric_label(self) -> &'static str {
        match self {
            ReverseGuardFailure::Watermark => "d5_block_barrier",
            ReverseGuardFailure::Generation => "d5_block_gen",
            ReverseGuardFailure::InFlight => "d5_block_inflight",
            ReverseGuardFailure::Ordering => "d5_block_order",
        }
    }
}

/// Guard (c) (plan doc §2; the guard measured to do most of the correctness
/// work, 1174/3000 ablation runs corrupted without it): "is there a staged
/// change on `from_table`, matching any of `keys` via `from_col` against
/// either its `old_image` or `new_image`, staged (`lsn`) at or before
/// `watermark_x`, that hasn't drained yet?"
///
/// "Hasn't drained yet" mirrors [`converge::converged_through`]'s own
/// condition 3 (a row is pending if its owning segment's `state <>
/// 'drained'`, or — the straggler case — the segment *is* `'drained'` but
/// this particular row landed after that segment's own fence snapshot was
/// captured, so the segment's completion never actually scanned it): scoped
/// here to one `src_table`/join-key/`lsn` predicate instead of
/// `converged_through`'s whole-token one, and, unlike that predicate, this
/// one has no need to special-case the *active* slot for query-plan reasons
/// — every physical ring row this predicate can even match already carries
/// a concrete `lsn <= watermark_x`, so a plain per-table `exists` (rather
/// than `converged_through`'s `min(origin_lsn)` optimization for the
/// continuously-growing active slot) is cheap regardless of which slot is
/// active. The active slot's own `segments` row always has `state =
/// 'active'`, which is `<> 'drained'`, so it falls out of the same `exists`
/// clause with no separate arm needed.
///
/// Only `op in ('insert', 'update', 'delete')` rows can match at all — a
/// `Recompute` row carries no images (`old_image`/`new_image` both `NULL`,
/// so `->> from_col` is `NULL` either way) and a `Truncate` row is
/// key-less — so the explicit `op` filter is redundant with that but kept
/// for readability. Deliberately does not special-case a `Truncate` on
/// `from_table` itself (out of scope for this issue — see the module's own
/// "what to actually do" list's to-many/`rel_joins` exclusion, and no
/// existing test exercises a to-one relationship's from-side table being
/// truncated mid-flight).
///
/// **Pre-commit positions (issues #402, #622).** No ring row's `lsn` is its
/// writer's commit: the capture trigger stamps `pg_current_wal_insert_lsn()`
/// inside the writer's transaction, and the target-mutation seam stamps a
/// token its writer reads before committing. So guard (a)'s old premise,
/// "once the ring holds everything through `X`, every change at or below `X`
/// is in the ring", does not hold: a row at or below `X` can belong to a
/// writer that has not committed, and this scan can't see it. The check is
/// still no weaker than it was against intake's commit positions. A
/// pre-commit position is below its writer's commit, so a write that
/// committed at or below `X` has a row at or below `X` too, and a writer that
/// committed before this scan committed its row with it. A row this scan
/// can't see belongs to a writer that commits after the scan, so after `X`
/// was captured: its commit position would have been above `X` and excluded
/// as well. The argument only uses "position below commit", so it does not
/// matter that `X` is a WAL *write* position while a row's `lsn` is an
/// *insert* position. What does change is that a row can match here although
/// its writer committed after `X`, which only defers more.
async fn from_side_change_in_flight(
    txn: &Transaction<'_>,
    from_table: &str,
    from_col: &str,
    keys: &[&str],
    watermark_x: PgLsn,
) -> Result<bool, ApplyError> {
    if keys.is_empty() {
        return Ok(false);
    }
    let arms = converge::per_ring_table(" union all ", |slot, table| {
        format!(
            "select 1 from {table} r \
             where r.src_table = $1 \
               and r.op in ('insert', 'update', 'delete') \
               and r.lsn <= $2 \
               and (r.old_image ->> $3 = any($4::text[]) \
                    or r.new_image ->> $3 = any($4::text[])) \
               and exists ( \
                   select 1 from segments s \
                   where s.ring_slot = {slot} \
                     and (s.state <> 'drained' \
                          or (s.fence_snapshot is not null \
                              and not pg_visible_in_snapshot(r.row_txid, s.fence_snapshot))) \
               )"
        )
    });
    let sql = format!("select exists ({arms})");
    let row = txn
        .query_one(&sql, &[&from_table, &watermark_x, &from_col, &keys])
        .await?;
    Ok(row.get(0))
}

/// Checks all four of #132's guards for one [`RelationshipReverseRecord`],
/// inside the already-open Phase 3 `txn` — [`apply_and_mark_drained_many`]'s
/// "3d" step's single "may this record's delta apply?" decision, replacing
/// #131's own inline guard-(d)-only check. Cheapest/most-locking-averse
/// first: guard (a) is a bare in-memory comparison (no SQL at all), so it
/// short-circuits before this function ever touches the projection row;
/// guards (b)/(d) share the one `FOR UPDATE` read #131 already took (adding
/// `__trellis_gen` to its `SELECT` list, not a second query); guard (c) —
/// the most expensive, a ring scan — runs last, only once the cheaper three
/// have already passed.
///
/// **Locking discipline** (the issue's own "Locking" section): this
/// function's `FOR UPDATE` on the projection row is the *reverse* side of
/// "a forward apply touching a parent takes `FOR SHARE`... a reverse takes
/// `FOR UPDATE`." The forward side is `apply_and_mark_drained_many`'s "3c"
/// step's `UPDATE ... SET gen = gen + 1 WHERE key = ANY(...)` — an `UPDATE`
/// already takes the same exclusive row lock `FOR UPDATE` would (Postgres's
/// row-level locking has no weaker mode an `UPDATE` could take instead), so
/// no separate explicit `SELECT ... FOR SHARE` is needed there: the
/// `UPDATE` itself *is* the serializing lock. That is what makes this
/// function's guard (b) re-check meaningful rather than racy — a
/// concurrent forward apply's gen-bump `UPDATE` and this reverse's `FOR
/// UPDATE` read can never interleave mid-row; whichever transaction gets
/// there first blocks the other until it commits or rolls back, so by the
/// time this `SELECT ... FOR UPDATE` returns, it has either (a) landed
/// before any concurrent forward apply touched this row (nothing to
/// detect — `prev_gen` still matches) or (b) waited for that forward
/// apply's `UPDATE` to commit and then observed its bumped `gen` (guard (b)
/// correctly fails). There is no third interleaving.
///
/// Returns `Ok(None)` when every guard passes. Returns `Ok(Some(failure))`
/// naming the *first* guard that didn't — never more than one, since guard
/// evaluation stops at the first failure (there is nothing further to learn
/// from checking the rest once this record is already going to the
/// fallback path).
async fn check_reverse_guards(
    txn: &Transaction<'_>,
    shape: &ReverseRelationshipShape,
    record: &RelationshipReverseRecord,
    old_key: &Option<String>,
    new_key: &Option<String>,
    watermark: &StagedWatermark,
) -> Result<Option<ReverseGuardFailure>, ApplyError> {
    // Guard (a): watermark barrier. A bare in-memory read — no SQL, no
    // lock — so this always runs first.
    if watermark.get() < record.watermark {
        return Ok(Some(ReverseGuardFailure::Watermark));
    }

    // Guards (b)/(d): one row-locked read of the projection, shared between
    // both — see this function's own doc comment on why the lock this takes
    // is what makes guard (b) sound rather than racy.
    let lock_key = old_key.as_deref().or(new_key.as_deref());
    let (current_lsn, current_gen): (Option<Option<PgLsn>>, Option<Option<i64>>) = match lock_key {
        Some(key) if !shape.qualified_projection.is_empty() => {
            let lsn_ident = quote_ident(ddl::PROJECTION_LSN_COLUMN);
            let gen_ident = quote_ident(ddl::PROJECTION_GEN_COLUMN);
            let key_ident = quote_ident(&shape.to_col);
            let row = txn
                .query_opt(
                    &format!(
                        "select {lsn_ident}, {gen_ident} from {} \
                         where {key_ident}::text = $1 for update",
                        shape.qualified_projection,
                    ),
                    &[&key],
                )
                .await?;
            match row {
                Some(row) => (Some(row.get(0)), Some(row.get(1))),
                None => (None, None),
            }
        }
        _ => (None, None),
    };

    // Guard (b): a missing projection row (a parent about to be INSERTed,
    // or the should-be-unreachable no-projection case) has no `gen` to have
    // moved — nothing to conflict with, always passes, same posture guard
    // (d) already took for this case pre-#132.
    let gen_ok = match current_gen {
        None => true,
        Some(current) => current == record.prev_gen,
    };
    if !gen_ok {
        return Ok(Some(ReverseGuardFailure::Generation));
    }

    // Guard (d): #131's original stopgap, unchanged in substance, now one
    // arm of this unified check.
    let ordering_ok = match current_lsn {
        None => true,
        Some(current) => current == record.prev_lsn,
    };
    if !ordering_ok {
        return Ok(Some(ReverseGuardFailure::Ordering));
    }

    // Guard (c): the in-flight check, last (most expensive) and only once
    // (a) captured X, (b) the gen, and (d) the ordering have all already
    // passed — the from-side child rows a from-side change touching either
    // `old_key` or `new_key`, deduped so an ordinary same-key attribute
    // update (`old_key == new_key`) doesn't scan the same key twice.
    //
    // **Resolved by #133, on the plan doc's §3.1 fold-erasure finding**
    // ("post 3 appears in neither folded image" when a from-side row is
    // inserted then re-pointed within the same batch): that finding was
    // about guard (b) specifically — `RelationshipGenBump` used to be
    // resolved purely from the *folded* view (`compute`'s per-def
    // evaluation), so a parent key an intermediate, erased image touched
    // never got its projection row's `gen` bumped at all, and guard (b)'s
    // re-check couldn't detect a conflict that never bumped anything.
    // `build_relationship_context` now also unions in each touched change's
    // `FoldedChange::group_key` — the real, pre-fold union of touched
    // join keys the ring carries precisely for this (see that field's and
    // `staging::fold`'s doc comments for the merge rule) — so the erased
    // parent's `gen` does bump, and guard (b) alone now catches the
    // scenario. See `tests/apply_relationship_reverse.rs`'s
    // `issue_133_a_within_batch_repoint_still_bumps_the_erased_intermediate_parents_gen`
    // for the dedicated regression pin this comment used to ask for.
    //
    // `from_side_change_in_flight` below still independently scans the
    // **raw** ring rows directly (`seg_N`'s physical rows, one per raw CDC
    // change, never mutated by the fold — only `claim`/`fold`'s *read-time*
    // collapsing ever loses the intermediate image) for the same erased
    // touch, so it also catches it — but only as long as the erasing batch
    // is still undrained when this check runs (see the doc comment history
    // in git blame for the exact "once that batch fully drains, there is
    // nothing left in the ring for this scan to find" reasoning this guard
    // used to have to lean on alone). With #133 landed, guard (c) catching
    // it too is redundant-but-harmless defense in depth, not the only
    // safety net for this scenario anymore.
    let mut keys: Vec<&str> = Vec::with_capacity(2);
    if let Some(k) = old_key.as_deref() {
        keys.push(k);
    }
    if let Some(k) = new_key.as_deref()
        && Some(k) != old_key.as_deref()
    {
        keys.push(k);
    }
    if from_side_change_in_flight(
        txn,
        &shape.from_table,
        &shape.from_col,
        &keys,
        record.watermark,
    )
    .await?
    {
        return Ok(Some(ReverseGuardFailure::InFlight));
    }

    Ok(None)
}

// ---------------------------------------------------------------------
// Issue #135, epic #127: fairness escalation — starvation freedom for the
// reverse retry loop
// ---------------------------------------------------------------------
//
// **The problem.** Every guard-rejected [`RelationshipReverseRecord`] is
// deferred and retried later (issue #134). Under sustained from-side churn
// on one hot parent — children being inserted/updated/re-pointed onto it
// continuously, faster than one drain cycle settles — that retry loop can
// fail forever, for two independent, structural reasons, not merely bad
// luck:
//
// * **Guard (c) (in-flight).** A batch's own from-side writes are staged
//   into segments that are `state = 'draining'` (not yet `'drained'`) until
//   *this same transaction*'s step 5, which runs *after* step 3d's guard
//   check. So a reverse co-batched with any of its own parent's children
//   sees them as "still undrained" on the very first attempt, by
//   construction — not a race, a guarantee. Retrying buys nothing if the
//   *next* batch also contains fresh children for the same hot parent, which
//   sustained churn guarantees it will.
// * **Guard (b) (generation).** Step 3c bumps a parent's projection `gen` on
//   *every* batch whose forward evaluation resolves that parent for any
//   child — regardless of whether the child's own value changed. Under
//   sustained churn this fires on essentially every batch touching the
//   parent's children, so the window between Phase 2's live capture and
//   Phase 3's `FOR UPDATE` recheck is very likely to contain at least one
//   bump whenever concurrent apply workers are active.
//
// Both are driven by the *children's* own ordinary traffic, not by the
// parent itself changing repeatedly — the parent's own to-side row may have
// changed exactly once. That rules out any fix that tries to catch a quiet
// instant, since sustained churn need never produce one.
//
// **The issue's own sketch, and why it isn't the mechanism below.** The
// issue proposed a persisted "hold" that blocks *new* forward applies for a
// parent once its reverse has deferred N times, until the in-flight set
// drains to empty. Worked through concretely, this does not hold up:
//
// 1. Guard (c)'s own query treats *any* non-`'drained'` segment — including
//    the still-open *active* segment nothing has even claimed yet — as
//    in-flight. "Blocking" a child's forward apply by simply not processing
//    it leaves its row sitting in exactly such a segment, still in-flight,
//    forever. That is the priority-inversion the issue itself named:
//    holding children back to protect the parent's reverse would keep the
//    in-flight set the reverse is waiting on non-empty *by the hold's own
//    action* — a self-inflicted deadlock, not a fix.
// 2. Even granting some other way to "pause" children, guard (b)'s
//    projection `gen` bump cannot be safely suppressed for a held parent:
//    that bump is what lets guard (b) catch a forward apply that resolved a
//    stale relationship value *and already fully drained* before guard (c)
//    could ever see it (the #133/#134-era race guard (b) exists for
//    specifically). Suppressing it to protect the reverse would silently
//    reopen that exact double-application hazard. A "hold" that is honest
//    about this needs to stop children's forward evaluation from resolving
//    the relationship at all, which is a much larger change (a new
//    forward-defer capability, per the issue's own callout) with its own
//    correctness surface this issue's scope does not budget for.
//
// **The mechanism actually shipped: escalate away from the fragile fast
// path, not block the forward path.** (#623 D5 deleted the fast path, so
// every record now takes the fallback; the guards stay because guard (d)
// still orders the projection's advances.) Guards (a)/(b)/(c) exist to protect
// exactly one thing: the *fast-path delta*'s assumption that the from-side
// enumeration it reads is race-free against any other write touching the
// same aggregate contributions. They are not needed for the *pre-#131*
// image-less `Recompute` fallback (`ReverseRelationshipShape::needs_recompute_fallback`,
// and the `!fast_path_safe` case below) — that path just re-evaluates each
// row against current live state on its own next drain, with the same
// per-row locking every other definition's ordinary forward evaluation
// already relies on, and converges regardless of how much concurrent
// activity raced it. It was every guard-rejected record's own treatment
// before #134 introduced defer-and-retry as a cheaper common case.
//
// So: once a transition's `retry_count` reaches
// [`RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`], the *next* guard rejection
// (for guard (a), (b), or (c) — never (d), see below) escalates instead of
// deferring again — [`stage_reverse_recompute_fallback`] stages the
// always-correct recompute for every touched from-side row, exactly as the
// existing fallback branch does, and the settled parent projection is
// advanced immediately via [`apply_projection_advance`], exactly as the
// all-guards-passed path already does. This reuses two already-proven
// mechanisms; nothing about how children are forward-applied changes, so
// there is no new blocking, no new "hold" state, and no new deadlock
// surface.
//
// **Why advancing the projection here is sound.** [`check_reverse_guards`]
// short-circuits at the *first* failing guard, in order (a), (b), (d), (c) —
// so a `Generation`/`Watermark`/`InFlight` failure it reports carries no
// information about whether guard (d) (the projection's own LSN chain) also
// holds; that check may never have run. Guard (d) is the one guard that
// gates whether it is *safe to write* the projection at all (an
// out-of-order or replayed transition must not stomp a fresher one); guards
// (a)/(b)/(c) only ever gate the *fast-path delta's* correctness, never the
// plain "does this transition's `prev_lsn` still match the projection"
// question. [`reverse_ordering_still_holds`] answers that question
// independently, under the same `FOR UPDATE` lock (re-acquiring a lock this
// transaction already holds is a no-op, not a second wait), before
// escalation is allowed to touch the projection. If guard (d) itself is
// what's failing, escalation never happens — the record keeps deferring
// (unchanged from #134) — see the residual limitation this leaves, below.
//
// **The liveness bound this gives, and what it does not cover.** Guard (d)
// only fails when *another* reverse for the *same parent* raced this one —
// i.e. the parent's own row being edited again, not its children churning —
// and the ordinary SQL fold already collapses multiple same-batch edits to
// one parent into a single transition, so this is expected to be rare under
// the "hot parent, churning children" scenario this issue targets. Modulo
// that, every reverse transition is guaranteed to fully resolve (both the
// projection advance and the aggregate correction) within
// [`RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`] deferred-retry drain cycles,
// unconditionally — the escalation branch performs its action outright, it
// does not merely retry again. **What this does not guarantee**: a parent
// whose own row is itself repeatedly, concurrently re-edited by more than
// one racing writer fast enough to keep failing guard (d) on every
// escalation attempt too is not covered by this mechanism and could still
// defer indefinitely — a narrower and different failure mode than the one
// this issue's own stress model exercises (sustained *child* churn against
// a parent edited once). Flagged here, and in this issue's report, as a
// known residual limitation rather than left implicit.

/// Issue #135: an independent, guard-(d)-only recheck used *only* by the
/// fairness-escalation path — never by [`check_reverse_guards`]'s own
/// ordinary defer-or-apply decision, which already covers guard (d) as one
/// arm of its short-circuiting evaluation (see that function's doc comment
/// and this section's own "Why advancing the projection here is sound"
/// above for why a `Generation`/`Watermark`/`InFlight` failure it reports
/// carries no information about whether guard (d) also holds).
///
/// Takes the same `FOR UPDATE` lock [`check_reverse_guards`] already took
/// (or would take) on this row, inside the same transaction — Postgres
/// re-locking a row this transaction already holds is a no-op, not a second
/// wait.
async fn reverse_ordering_still_holds(
    txn: &Transaction<'_>,
    shape: &ReverseRelationshipShape,
    old_key: &Option<String>,
    new_key: &Option<String>,
    record: &RelationshipReverseRecord,
) -> Result<bool, ApplyError> {
    let lock_key = old_key.as_deref().or(new_key.as_deref());
    let Some(key) = lock_key else {
        // No key at all: `check_reverse_guards` never rejects such a record
        // in the first place (its own "both keys `None`" short-circuit
        // always passes), so this function is never actually called in that
        // shape — defensive `true` matches that function's own posture.
        return Ok(true);
    };
    if shape.qualified_projection.is_empty() {
        return Ok(true);
    }
    let lsn_ident = quote_ident(ddl::PROJECTION_LSN_COLUMN);
    let key_ident = quote_ident(&shape.to_col);
    let row = txn
        .query_opt(
            &format!(
                "select {lsn_ident} from {} where {key_ident}::text = $1 for update",
                shape.qualified_projection,
            ),
            &[&key],
        )
        .await?;
    match row {
        None => Ok(true),
        Some(row) => {
            let current_lsn: Option<PgLsn> = row.get(0);
            Ok(current_lsn == record.prev_lsn)
        }
    }
}

/// Where a propagated change traces back to: its source commit's
/// `src_changed` (issues #51/#52) and `origin_lsn` (issue #469), merged with
/// [`earliest_src_changed`] and [`earliest_origin`] respectively.
type Provenance = (Option<std::time::SystemTime>, Option<PgLsn>);

/// One key a drain re-derives by recompute:
/// `(src_table, key, hop_gen, src_changed, origin_lsn)`.
type DerivedRecompute = (
    String,
    String,
    i32,
    Option<std::time::SystemTime>,
    Option<PgLsn>,
);

/// Stages the pre-#131 `Recompute` fallback (issue #131's own stopgap,
/// shared since by every guard rejection before #134, and since #623 D5 by
/// every record whose relationship some definition reads) for every from-side row currently matching `old_key`/`new_key` via
/// `shape.from_col` — live-enumerated inside the already-locked Phase 3
/// `txn`, deduped against `seen_keys` (shared across every call this same
/// record makes, so a same-key `old_key == new_key` update is not staged
/// twice). Needs none of #132's four guards for its own correctness: each
/// staged `Recompute` is picked up on a later drain and re-evaluated against
/// whatever is live *then*, the same way any other definition's ordinary
/// forward evaluation already is — which is exactly why issue #135's
/// fairness escalation (see this module's own design section above) can
/// lean on it as the always-safe exit from the guard-gated retry loop.
///
/// The `Recompute`s carry no image (#623 D5): a 1-1 target re-reads the
/// row, and an aggregate's ledger entry already names the group the row
/// leaves while its Re-derive reads the parent live.
///
/// `row_columns` is `shape.from_table`'s live column list, resolved once by
/// the caller via [`cached_row_columns`] — not re-introspected per call here
/// — since this function's own caller (the "3d" step's `for record in
/// &plan.relationship_reverses` loop) runs once per distinct touched parent
/// key in the batch, and many records touching one relationship all share
/// this same `from_table`. See [`from_side_rows_for_trigger_txn`]'s doc
/// comment for why that function takes the same parameter rather than
/// introspecting it itself.
async fn stage_reverse_recompute_fallback(
    txn: &Transaction<'_>,
    record: &RelationshipReverseRecord,
    old_key: &Option<String>,
    new_key: &Option<String>,
    seen_keys: &mut std::collections::HashSet<String>,
    fallback: &mut Vec<DerivedRecompute>,
    row_columns: &[String],
) -> Result<(), ApplyError> {
    let shape = &record.shape;
    let Some(from_pk) = &shape.from_pk else {
        return Ok(());
    };
    for key in [old_key.clone(), new_key.clone()].into_iter().flatten() {
        let trigger = ReverseTrigger::Keys(std::slice::from_ref(&key));
        let from_rows = from_side_rows_for_trigger_txn(
            txn,
            &shape.from_table,
            &shape.from_col,
            from_pk,
            &trigger,
            row_columns,
        )
        .await?;
        for (from_key, _) in from_rows {
            if seen_keys.insert(from_key.clone()) {
                fallback.push((
                    shape.from_table.clone(),
                    from_key,
                    record.hop_gen + 1,
                    record.src_changed,
                    record.origin_lsn,
                ));
            }
        }
    }
    Ok(())
}

/// The settled parent projection's current data columns (excluding the key
/// and the two bookkeeping columns) — the same `information_schema.columns`
/// introspection [`catalog::ensure_relationship_projection_in_txn`] already
/// uses, duplicated here since Phase 3 holds no `pool` (only `txn`) and that
/// function is private to `defs::catalog`.
async fn projection_data_columns(
    txn: &Transaction<'_>,
    projection_schema: &str,
    projection_table_bare: &str,
    to_col: &str,
) -> Result<Vec<String>, ApplyError> {
    let bookkeeping = [
        to_col,
        ddl::PROJECTION_GEN_COLUMN,
        ddl::PROJECTION_LSN_COLUMN,
    ];
    let rows = txn
        .query(
            "select column_name from information_schema.columns \
             where table_schema = $1 and table_name = $2 order by ordinal_position",
            &[&projection_schema, &projection_table_bare],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .filter(|c| !bookkeeping.contains(&c.as_str()))
        .collect())
}

/// Advances the settled parent projection for one applied
/// [`RelationshipReverseRecord`] (issue #131) — a symmetric delete-then-upsert
/// keyed independently by `old_key`/`new_key`, so a parent PK-changing
/// update (a genuinely different projection row identity — rare, but
/// Postgres imposes no immutability on `to_col` itself) is handled the same
/// way as an ordinary same-key update, a parent insert (`old_key` absent),
/// or a parent delete (`new_key` absent). Sets [`ddl::PROJECTION_LSN_COLUMN`]
/// to `lsn`; **never touches [`ddl::PROJECTION_GEN_COLUMN`]** — that column
/// is step 3c's forward bookkeeping (a *different* signal: "has a forward
/// apply landed since some Phase-2 capture"), and this reverse advance is
/// not one — see both columns' own doc comments for the distinction.
///
/// The upsert leans on Postgres's own `jsonb_populate_record` against the
/// projection table's real composite row type, rather than this crate
/// re-deriving a per-column cast from `information_schema`: every projected
/// data column's name already appears as a key in the parent's raw new
/// image (a full row dump), so Postgres's own JSON-to-record coercion does
/// exactly the right thing for whichever subset of columns the projection
/// actually carries.
async fn apply_projection_advance(
    txn: &Transaction<'_>,
    shape: &ReverseRelationshipShape,
    old_key: &Option<String>,
    new_key: &Option<String>,
    new_image: Option<&str>,
    lsn: Option<PgLsn>,
) -> Result<(), ApplyError> {
    if shape.qualified_projection.is_empty() {
        return Ok(());
    }
    let key_ident = quote_ident(&shape.to_col);

    if let Some(old_key) = old_key
        && new_key.as_deref() != Some(old_key.as_str())
    {
        txn.execute(
            &format!(
                "delete from {} where {key_ident}::text = $1",
                shape.qualified_projection
            ),
            &[old_key],
        )
        .await?;
    }

    // A NULL `to_col` is no projection key (the key column is `not null`,
    // and a NULL never joins a from-side row anyway), the same exclusion
    // `catalog::ensure_relationship_projection_in_txn`'s seed makes. A
    // nullable `UNIQUE` to-side column can hold one, and an aggregate
    // target's NULL group routinely does (issue #403).
    let (Some(_), Some(new_image)) = (new_key, new_image) else {
        return Ok(());
    };
    let data_columns = projection_data_columns(
        txn,
        &shape.projection_schema,
        &shape.projection_table_bare,
        &shape.to_col,
    )
    .await?;
    let gen_ident = quote_ident(ddl::PROJECTION_GEN_COLUMN);
    let lsn_ident = quote_ident(ddl::PROJECTION_LSN_COLUMN);

    let mut insert_cols = vec![key_ident.clone()];
    let mut select_exprs = vec![format!("r.{key_ident}")];
    let mut update_sets = Vec::new();
    for col in &data_columns {
        let ident = quote_ident(col);
        insert_cols.push(ident.clone());
        select_exprs.push(format!("r.{ident}"));
        update_sets.push(format!("{ident} = excluded.{ident}"));
    }
    insert_cols.push(gen_ident.clone());
    select_exprs.push("0".to_string());
    insert_cols.push(lsn_ident.clone());
    select_exprs.push("$2::pg_lsn".to_string());
    update_sets.push(format!("{lsn_ident} = excluded.{lsn_ident}"));

    let sql = format!(
        "insert into {proj} ({insert_cols}) \
         select {select_exprs} from jsonb_populate_record(null::{proj}, $1::text::jsonb) r \
         on conflict ({key_ident}) do update set {update_sets}",
        proj = shape.qualified_projection,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        update_sets = update_sets.join(", "),
    );
    txn.execute(&sql, &[&new_image, &lsn]).await?;
    Ok(())
}

/// Issue #531: whether a reverse record at `lsn` on a source to-side may
/// carry images the relationship's last projection refresh overtook: the
/// refresh stamped the relationship at `stamp`
/// (`relationship_projections.refreshed_lsn`, read `for share` by Phase 3),
/// and the record's change is at or below it. A record above the stamp, or
/// on a relationship never refreshed, applies its images unchecked, so the
/// steady state pays no live-row read. A record with no `lsn` (never staged
/// by CDC for a source to-side) is checked whenever a stamp exists.
fn overtaken_by_refresh(lsn: Option<PgLsn>, stamp: Option<PgLsn>) -> bool {
    match (lsn, stamp) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(lsn), Some(stamp)) => lsn <= stamp,
    }
}

/// Issue #531: whether a to-side `TRUNCATE` at `lsn` is one the
/// relationship's last projection refresh (stamped at `stamp`) already read,
/// so clearing the projection for it would drop what the refresh found
/// written after it. The refresh holds `ACCESS SHARE` on the to-side from its
/// first read until it commits, so a truncate either committed before that
/// read (and its commit is at or below the stamp) or waits for the refresh
/// to commit (and lands above it). Unlike [`overtaken_by_refresh`], a
/// truncate with no `lsn` is never treated as read: skipping a clear is not
/// the conservative choice.
fn truncate_overtaken_by_refresh(lsn: Option<PgLsn>, stamp: Option<PgLsn>) -> bool {
    matches!((lsn, stamp), (Some(lsn), Some(stamp)) if lsn <= stamp)
}

/// Issues #507/#531: whether a reverse record carries images its to-side has
/// since moved past by a write that no later record will bring: the live row
/// for its new key no longer equals its new image, or a row it deleted (or
/// moved off `old_key`) is back.
///
/// A to-side's records carry every write the projection hears about, so the
/// projection normally follows their images. Two kinds of write reach it only
/// through a refresh from the to-side's rows
/// (`catalog::refresh_relationship_projections_in_txn`), and a record
/// staged before that refresh but drained after it would put an older image
/// back with nothing after it to correct it. For a target to-side (fed by
/// the target-mutation seam) that is a resumed definition's rebuild, which
/// bypasses the seam. For a source to-side it is a change no ring row
/// carries (one written while the table's capture was uninstalled), which
/// the refresh read. So a record whose
/// images the live row contradicts writes the projection from the live row
/// ([`apply_projection_from_live`]) and re-derives its from-side rows with
/// the image-less fallback, never a delta from images that no longer hold. A
/// record that merely lags a newer write to the same key (both pending in
/// different segments) lands here too. That is correct as well, since the
/// newer record finds the projection already at its image.
///
/// Every record on a target to-side pays this keyed read. A source
/// to-side's record pays it only at or below the relationship's refresh
/// stamp ([`overtaken_by_refresh`]).
async fn to_side_superseded(
    txn: &Transaction<'_>,
    shape: &ReverseRelationshipShape,
    to_side_columns: &[String],
    old_key: &Option<String>,
    new_key: &Option<String>,
    new_image: Option<&str>,
) -> Result<bool, ApplyError> {
    let filter = shape.to_side.key_filter(txn, &shape.to_col).await?;
    let doc = row_as_text_jsonb_sql("t", to_side_columns);
    let sql = format!(
        "select ({doc}) is not distinct from $2::text::jsonb from {} t where {filter}",
        shape.to_side.table
    );
    // The live row for `key` equals `image`, or is absent when `image` is.
    let holds = async |key: &str, image: Option<&str>| -> Result<bool, ApplyError> {
        let row = txn.query_opt(&sql, &[&key, &image]).await?;
        Ok(match row {
            Some(row) => image.is_some() && row.get::<_, bool>(0),
            None => image.is_none(),
        })
    };
    if let Some(key) = new_key.as_deref()
        && !holds(key, new_image).await?
    {
        return Ok(true);
    }
    if let Some(key) = old_key.as_deref()
        && new_key.as_deref() != Some(key)
        && !holds(key, None).await?
    {
        return Ok(true);
    }
    Ok(false)
}

/// `column`'s value on `row` when the column is structurally required
/// there, telling an absent column from a present `NULL` the way the
/// evaluator does (issue #677): `Ok(None)` is a real SQL `NULL`, and a
/// column missing from the image is [`eval::EvalError::MissingColumn`]
/// against `field`. A `GROUP BY` key or a relationship join key read as
/// `NULL` when absent would silently route the change to the wrong group
/// or parent. Under trigger capture (#622) images carry only the capture
/// column set, and an under-capture has to fail loudly instead.
fn required_column<'r>(
    row: &'r Row,
    column: &str,
    field: &str,
) -> Result<Option<&'r String>, eval::EvalError> {
    match row.get(column) {
        Some(value) => Ok(value.as_ref()),
        None => Err(eval::EvalError::MissingColumn {
            field: field.to_string(),
            column: column.to_string(),
        }),
    }
}

/// Whether [`to_side_superseded`] finds `record`'s images stale, checked
/// only for a record that may be stale: every record on a seam-fed to-side,
/// and one on a source to-side at or below its relationship's refresh stamp
/// in `refresh_stamps` ([`overtaken_by_refresh`]).
async fn superseded_to_side(
    txn: &Transaction<'_>,
    row_columns_cache: &mut HashMap<String, Vec<String>>,
    refresh_stamps: &HashMap<i64, PgLsn>,
    record: &RelationshipReverseRecord,
    old_key: &Option<String>,
    new_key: &Option<String>,
) -> Result<bool, ApplyError> {
    let shape = &record.shape;
    if !shape.to_side.seam_fed
        && !overtaken_by_refresh(record.lsn, refresh_stamps.get(&shape.id).copied())
    {
        return Ok(false);
    }
    let columns = cached_row_columns(txn, row_columns_cache, &shape.to_side.identity)
        .await?
        .to_vec();
    to_side_superseded(
        txn,
        shape,
        &columns,
        old_key,
        new_key,
        record.new_image.as_deref(),
    )
    .await
}

/// Issue #531: each of `relationship_ids`' refresh stamp
/// (`relationship_projections.refreshed_lsn`), keyed by `relationship_id`,
/// read under a `for share` lock taken in `relationship_id` order. A
/// relationship never refreshed has no entry. No query at all when
/// `relationship_ids` is empty.
async fn relationship_refresh_stamps(
    txn: &Transaction<'_>,
    relationship_ids: impl Iterator<Item = i64>,
) -> Result<HashMap<i64, PgLsn>, ApplyError> {
    let mut ids: Vec<i64> = relationship_ids.collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    ids.sort_unstable();
    ids.dedup();
    let rows = txn
        .query(
            "select relationship_id, refreshed_lsn from relationship_projections \
             where relationship_id = any($1) order by relationship_id for share",
            &[&ids],
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| Some((row.get::<_, i64>(0), row.get::<_, Option<PgLsn>>(1)?)))
        .collect())
}

/// [`apply_projection_advance`] for a record [`to_side_superseded`] found
/// stale: writes the projection row of each of `old_key`/`new_key` from the
/// live to-side row instead of the record's image, deleting it where the
/// to-side has no row. Moves [`ddl::PROJECTION_LSN_COLUMN`] to `lsn` exactly
/// as the image advance would, and never touches
/// [`ddl::PROJECTION_GEN_COLUMN`].
///
/// Issue #754: a key whose latest pending change after `pending_after` (the
/// record's own `lsn`, so none of the record's own ring rows count) names it
/// in its new image is left to that change, the rule
/// `catalog::pending_to_side_keys` applies to the catch-up inserts (#726):
/// the projection row is deleted rather than written from the live row, and
/// that change's record writes it. Written from the live row, it could
/// outlive the to-side row. A re-insert pending when a delete drains,
/// superseded by it, folds with a later delete into a record with no image,
/// which names no key, so no record would ever remove the row the live
/// write put there. Deleting the row is what the pending change's record
/// expects, since the key was absent before it whenever that change is an
/// insert; when the change keeps the key (an update), its record writes the
/// row whole either way, and re-derives the from-side rows it reaches.
///
/// "Pending" is per change (issue #762,
/// `staging::page::ring_row_pending_sql`): no committed drain has applied
/// it, by its segment's `drained_mask` and its bucket's `drain_cursor`, and
/// `claim`, the Phase 3 transaction this runs in, is not applying it. Both
/// are monotonic, so once a change has applied, no later write to the
/// projection row can make it look pending again. A change this transaction
/// applies is its own: one it already applied wrote the row, and one it
/// hasn't yet will (#761 read "already applied" off the row's `lsn`, which a
/// superseded older record's live write lowers, so a second older record
/// then took an applied re-insert for pending and deleted its key).
///
/// One statement per key, so the pending read, the projection read and the
/// live read share one snapshot (ADR-0002 I1), and the delete and the upsert
/// never both act on the row: the delete acts only where the to-side has no
/// row, or where the upsert is withheld. A drain applying a pending change
/// concurrently commits its row write with its drain state. Guard (b)/(d)
/// locked one key's row `for update` in an earlier statement, and the
/// release locks every key's, so on such a row that drain has committed
/// before this snapshot or waits for this transaction. For any other row,
/// the withheld-upsert delete requires the row's `lsn` to be the one this
/// snapshot read: a to-side write committed while this statement waited on
/// the row (that drain's, or any other) stamps a new `lsn`, so the row is
/// left alone, and the changes still pending after that write apply after
/// it. The test is on `lsn`, not the row version (`xmin`): a from-side
/// apply's `__trellis_gen` bump (step 3c) also writes the row, holding its
/// lock until it commits, without applying any to-side change, and the
/// delete must still go ahead after it.
async fn apply_projection_from_live(
    txn: &Transaction<'_>,
    shape: &ReverseRelationshipShape,
    old_key: &Option<String>,
    new_key: &Option<String>,
    lsn: Option<PgLsn>,
    pending_after: Option<PgLsn>,
    claim: Option<&super::page::ClaimScope<'_>>,
) -> Result<(), ApplyError> {
    if shape.qualified_projection.is_empty() {
        return Ok(());
    }
    let to_side = &shape.to_side;
    let key_ident = quote_ident(&shape.to_col);
    let filter = to_side.key_filter(txn, &shape.to_col).await?;
    let data_columns = projection_data_columns(
        txn,
        &shape.projection_schema,
        &shape.projection_table_bare,
        &shape.to_col,
    )
    .await?;
    let gen_ident = quote_ident(ddl::PROJECTION_GEN_COLUMN);
    let lsn_ident = quote_ident(ddl::PROJECTION_LSN_COLUMN);
    let mut insert_cols = vec![key_ident.clone()];
    let mut select_exprs = vec![format!("t.{key_ident}")];
    let mut update_sets = Vec::new();
    for col in &data_columns {
        let ident = quote_ident(col);
        insert_cols.push(ident.clone());
        select_exprs.push(format!("t.{ident}"));
        update_sets.push(format!("{ident} = excluded.{ident}"));
    }
    insert_cols.push(gen_ident);
    select_exprs.push("0".to_string());
    insert_cols.push(lsn_ident.clone());
    select_exprs.push("$2::pg_lsn".to_string());
    update_sets.push(format!("{lsn_ident} = excluded.{lsn_ident}"));
    let col = quote_literal(&shape.to_col);
    let pending = catalog::pending_to_side_changes(
        &to_side.identity,
        &shape.to_col,
        shape.id,
        &format!(
            "and r.lsn > $3::pg_lsn \
             and (r.old_image ->> {col} = $1::text or r.new_image ->> {col} = $1::text)"
        ),
        claim,
    );
    let sql = format!(
        "with latest as materialized ( \
             select c.new_key is not distinct from $1::text as present \
             from ({pending}) c order by c.lsn desc, c.change_id desc limit 1), \
         held as (select 1 from latest l where l.present), \
         seen as materialized ( \
             select q.{lsn_ident} as row_lsn from {proj} q where q.{key_ident}::text = $1), \
         gone as ( \
             delete from {proj} p where p.{key_ident}::text = $1 \
             and (not exists (select 1 from {to_table} t where {filter}) \
                  or (exists (select 1 from held) \
                      and exists (select 1 from seen v \
                                  where v.row_lsn is not distinct from p.{lsn_ident})))) \
         insert into {proj} ({insert_cols}) \
         select {select_exprs} from {to_table} t \
         where {filter} and not exists (select 1 from held) \
         on conflict ({key_ident}) do update set {update_sets}",
        proj = shape.qualified_projection,
        to_table = to_side.table,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        update_sets = update_sets.join(", "),
    );
    let pending_after = pending_after.unwrap_or(PgLsn::from(0));
    let mut keys: Vec<&str> = old_key.iter().chain(new_key).map(String::as_str).collect();
    keys.sort_unstable();
    keys.dedup();
    for key in keys {
        txn.execute(&sql, &[&key, &lsn, &pending_after]).await?;
    }
    Ok(())
}

/// Issue #754: writes, from the live to-side row, the projection row of
/// every key a released to-side key's parked changes named, for each to-one
/// relationship whose to-side is `src_table` (the canonical name). `images`
/// are the parked changes' images, as JSON text; the live row of `key` names
/// one more.
///
/// `quarantine::release_key` discards the parked rows and stages an
/// image-less `Recompute`, which builds no reverse record, so without this
/// nothing would carry what the parked changes did to the to-side into the
/// projection. The write is [`apply_projection_from_live`]'s, counting the
/// pending changes after `parked_through`, the greatest parked `lsn`: a key
/// a later pending change will write is left to it. A parked change's ring
/// row is applied once the page that parked it commits (issue #762), but an
/// eviction parks a key in its own transaction, before the page retries
/// without it, and the release discards the parked changes, so the key must
/// never be left to them. Each relationship's
/// refresh stamp is locked `for share` first and the projection rows `for
/// update` in key order, the order a drain takes them (ADR-0002 I5), and the
/// rows are stamped with the release's WAL position.
#[cfg(any(test, feature = "internals"))]
pub(crate) async fn release_to_one_projections(
    pool: &Pool,
    txn: &Transaction<'_>,
    src_table: &str,
    key: &str,
    images: &[String],
    parked_through: Option<PgLsn>,
) -> Result<(), ApplyError> {
    let mut relationships: Vec<RelationshipDefinition> =
        catalog::relationships_to_table(pool, src_table)
            .await?
            .into_iter()
            .filter(|rel| rel.cardinality == RelationshipCardinality::ToOne)
            .collect();
    if relationships.is_empty() {
        return Ok(());
    }
    relationships.sort_by_key(|rel| rel.id);
    relationship_refresh_stamps(txn, relationships.iter().map(|rel| rel.id)).await?;
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await?
        .get(0);
    let pk = ddl::source_primary_key(pool, src_table).await?;
    let pk_expr = ddl::pk_key_sql_expr(&pk, Some("t"));
    // A release is no drain page: no fence is committed under, so what the
    // shape reads of the fence goes unused.
    let mut versions = HashMap::new();
    for rel in &relationships {
        let shape =
            build_reverse_relationship_shape(pool, rel, FromSideFence::new(&mut versions, false))
                .await?;
        if shape.qualified_projection.is_empty() {
            continue;
        }
        let key_ident = quote_ident(&shape.to_col);
        let keys: Vec<String> = txn
            .query(
                &format!(
                    "select k from ( \
                         select i::jsonb ->> $3 as k from unnest($1::text[]) i \
                         union select t.{key_ident}::text from {to_table} t \
                         where {pk_expr} = $2) keys \
                     where k is not null order by k collate \"C\"",
                    to_table = shape.to_side.table,
                ),
                &[&images, &key, &shape.to_col],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        txn.execute(
            &format!(
                "select 1 from {proj} where {key_ident}::text = any($1) \
                 order by {key_ident} for update",
                proj = shape.qualified_projection,
            ),
            &[&keys],
        )
        .await?;
        for k in keys {
            apply_projection_from_live(
                txn,
                &shape,
                &Some(k),
                &None,
                Some(lsn),
                parked_through,
                None,
            )
            .await?;
        }
    }
    Ok(())
}

/// The to-side rows whose `to_col` matches any of `join_keys` (compared at
/// `to_col`'s own native type via [`key_column_pg_type`] — issue #125,
/// falling back to the old `::text` comparison if the column can't be
/// introspected), grouped by that key's `::text` (the evaluator's key
/// convention, shared with [`from_side_keys`]). A `NULL` `to_col` is
/// absent (SQL `NULL` never joins) — matching the evaluator's requirement
/// that such a to-side row carry no key. To-one relationships get exactly one
/// row per key (`to_col` is UNIQUE); to-many get the full related set.
/// Decodes each row's columns via the same in-SQL `jsonb_each_text` unnest
/// [`read_live_rows_batch`] uses. `to_table` is the to-side's unquoted,
/// qualified identity (issues #372, #561), quoted here for interpolation.
async fn fetch_to_side_rows(
    pool: &Pool,
    to_table: &str,
    to_col: &str,
    join_keys: &[String],
) -> Result<HashMap<String, Vec<Row>>, ApplyError> {
    if join_keys.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let col_ident = quote_ident(to_col);
    let tbl_ident = ddl::qualified_source_table(to_table);
    let pg_type = key_column_pg_type(pool, to_table, to_col).await?;
    let filter = key_array_filter(&col_ident, pg_type.as_deref());
    // Issue #248: an explicit per-column `jsonb_build_object`, not
    // `to_jsonb(t.*)` — see `row_as_text_jsonb_sql`'s doc comment.
    let row_columns = live_row_columns(&**client, to_table).await?;
    let doc_expr = row_as_text_jsonb_sql("t", &row_columns);
    let sql = format!(
        "select m.jk, m.rn, e.key, e.value \
         from (select {col_ident}::text as jk, \
                      row_number() over () as rn, \
                      {doc_expr} as doc \
               from {tbl_ident} t \
               where {filter}) m \
         cross join lateral jsonb_each_text(m.doc) e",
    );
    let db_rows = client.query(&sql, &[&join_keys]).await?;
    // Assemble each row by its stable `rn`, carrying its join key, then group.
    let mut assembled: HashMap<i64, (String, Row)> = HashMap::new();
    for db_row in db_rows {
        let jk: String = db_row.get(0);
        let rn: i64 = db_row.get(1);
        let field: String = db_row.get(2);
        let value: Option<String> = db_row.get(3);
        let entry = assembled.entry(rn).or_insert_with(|| (jk, Row::new()));
        entry.1.insert(field, value);
    }
    let mut grouped: HashMap<String, Vec<Row>> = HashMap::new();
    for (_, (jk, row)) in assembled {
        grouped.entry(jk).or_default().push(row);
    }
    Ok(grouped)
}

/// The settled parent projection's rows whose key column matches any of
/// `join_keys` (issue #130, epic #127) — [`build_relationship_context`]'s
/// to-one counterpart to [`fetch_to_side_rows`], reading `projection`
/// instead of the live to-side table. The projection's key column is a real
/// `primary key` (`catalog::ensure_relationship_projection_in_txn`'s DDL), so
/// unlike `fetch_to_side_rows` there is at most one row per key — no
/// `row_number()`/grouping dance needed, just a per-key `Row` assembled the
/// same `jsonb_each_text` way every other decode in this module uses. This
/// also happens to return every column the projection carries (bookkeeping
/// columns `__trellis_gen`/`__trellis_lsn` included, plus any data column a
/// *different* consumer widened in) — harmless, since the evaluator only ever
/// reads the specific columns a definition's own fields reference
/// ([`eval::eval_expr`]'s `Row::get`), and a `Row` carrying extra unread keys
/// is exactly what every other decode in this module already produces.
///
/// Issue #125: the filter compares `key_col` at its own native type via
/// [`key_column_pg_type`], not an untyped `::text` cast, so a btree index on
/// the projection's key column (always present — it's the table's own
/// `primary key`) stays usable. The type is looked up on `to_table` (the
/// relationship's live to-side table) rather than the projection itself:
/// `catalog::ensure_relationship_projection_in_txn` creates the projection's
/// key column with exactly `to_table`'s `to_col` type, so the two always
/// agree. `to_table` is the to-side's unquoted, qualified identity (issues
/// #372, #561).
async fn fetch_relationship_projection_rows(
    pool: &Pool,
    projection: &catalog::RelationshipProjection,
    to_table: &str,
    key_col: &str,
    join_keys: &[String],
) -> Result<HashMap<String, Row>, ApplyError> {
    if join_keys.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let key_ident = quote_ident(key_col);
    let pg_type = key_column_pg_type(pool, to_table, key_col).await?;
    let filter = key_array_filter(&format!("p.{key_ident}"), pg_type.as_deref());
    // Issue #248: an explicit per-column `jsonb_build_object`, not
    // `to_jsonb(p.*)` — see `row_as_text_jsonb_sql`'s doc comment.
    let row_columns = live_row_columns(&**client, &projection.identity()).await?;
    let qualified_projection = projection.qualified_table();
    let doc_expr = row_as_text_jsonb_sql("p", &row_columns);
    let sql = format!(
        "select p.{key_ident}::text as jk, e.key, e.value \
         from {qualified_projection} p \
         cross join lateral jsonb_each_text({doc_expr}) e \
         where {filter}"
    );
    let db_rows = client.query(&sql, &[&join_keys]).await?;
    let mut rows: HashMap<String, Row> = HashMap::new();
    for db_row in db_rows {
        let jk: String = db_row.get(0);
        let field: String = db_row.get(1);
        let value: Option<String> = db_row.get(2);
        rows.entry(jk).or_default().insert(field, value);
    }
    Ok(rows)
}

/// The [`ValueType`] of each named column on `table`, introspected live from
/// `pg_catalog` via its raw `atttypid` OID and [`crate::defs::pg_type::value_type_for_oid`]
/// (issue #108 — previously a second, independently-drifting copy of
/// `catalog`'s own `format_type`-text matching lived here), so a to-side
/// relationship column's text is typed the same way the from-side source
/// columns are. A column not found is simply absent — the evaluator
/// defaults an absent to-side column to `Numeric`.
pub(crate) async fn to_column_types(
    pool: &Pool,
    table: &str,
    columns: &[String],
) -> Result<HashMap<String, ValueType>, ApplyError> {
    if columns.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let rows = client
        .query(
            "select a.attname::text, a.atttypid \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attname = any($2::text[]) \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&ddl::regclass_arg(table), &columns],
        )
        .await?;
    let mut types = HashMap::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let type_oid: u32 = row.get(1);
        // Issue #117: also recognizes a user-defined enum type, reaching
        // for `client` only when `type_oid` isn't a fixed builtin — see
        // `pg_type::value_type_for_oid`'s own doc comment.
        types.insert(
            name,
            crate::defs::pg_type::value_type_for_oid(&**client, type_oid).await?,
        );
    }
    Ok(types)
}

// ---------------------------------------------------------------------
// Phase 2: compute
// ---------------------------------------------------------------------

/// Issue #51/ADR-0009 decision 5: buffers one applied change's per-transform
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
/// rows (issue #409), while both latency histograms still observe once per
/// folded change.
///
/// **Buffers, does not record** (the epic #49 cross-cutting review's fix,
/// closing a gap in issues #51/#52): `compute` (Phase 2) has no transaction
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
/// Issue #52: every `Some(src_changed)` this function sees is also buffered
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
/// adds to `trellis_changes_applied_total` (issue #409: one per staged ring
/// row, not one per folded change).
#[derive(Debug, Clone)]
struct TransformObservation {
    transform: String,
    src_changed: Option<std::time::SystemTime>,
    row_count: u64,
}

/// One key's write into a target table: the evaluated calculated-field
/// values, rendered to their canonical text form (aligned with the owning
/// [`TargetPlan::field_names`]/[`TargetPlan::field_types`]) plus the
/// `hop_gen` it carries forward if this write propagates downstream.
///
/// Kept as text rather than [`eval::Value`] so [`apply_target`] can bind it
/// straight into a parameterized query — every one of [`ValueType`]'s three
/// variants renders to a plain text form Postgres's own `::text::<type>`
/// cast round-trips exactly (numeric's decimal text, `Display for bool`'s
/// `true`/`false`, text values verbatim) — matching this module's existing
/// "text in, typed cast in SQL" convention for every other value it writes.
///
/// `src_changed` (issues #51/#52's multi-hop gap) is the triggering
/// [`FoldedChange::src_changed`], carried forward the same way `hop_gen` is
/// — so a downstream `Recompute` row this write's own propagation stages
/// (see [`apply_and_mark_drained_many`]'s step 4) keeps a real origin
/// instead of losing it at this hop.
#[derive(Debug, Clone)]
struct TargetWrite {
    pk_text: String,
    values: Vec<Option<String>>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    /// The triggering [`FoldedChange::origin_lsn`] (issue #469), carried
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

/// One folded change to a 1-1 target (#623 D6): an Apply of its last change,
/// evaluated in Phase 2 from its image, or a Re-derive (`apply` `None`),
/// which Phase 3 evaluates from the row it reads under the entry lock (see
/// `super::one_to_one_ledger`).
#[derive(Debug, Clone)]
struct OneToOneRecord {
    pk_text: String,
    apply: Option<OneToOneApply>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
    /// The ring's spelling of the source table, for a re-stage.
    src_table: String,
}

/// A 1-1 Apply's `(lsn, txid)` and its values, `None` for a delete.
type OneToOneApply = (PgLsn, String, Option<Vec<Option<String>>>);

/// A relationship-enriched definition's Phase 2 context, and each
/// relationship's `from_col` with the join keys that context resolved.
type JoinCoverage = (
    RelationshipContext,
    Vec<(String, std::collections::HashSet<String>)>,
);

/// Everything Phase 3 needs to settle one 1-1 target: its primary key shape
/// (for the pre-lock/upsert/delete SQL), the calculated-field column names
/// and their inferred [`ValueType`]s (both aligned with every
/// [`TargetWrite::values`], so [`apply_target`] knows which Postgres type
/// each column casts to), this page's records for it, and how to Re-derive
/// a key.
#[derive(Debug, Clone)]
struct TargetPlan {
    pk: Vec<PrimaryKeyColumn>,
    field_names: Vec<String>,
    field_types: Vec<ValueType>,
    records: Vec<OneToOneRecord>,
    /// The persisted, fully-qualified `"schema.table"` identity of this
    /// target (issue #73's `Definition::target_table`, ADR-0007) —
    /// carried alongside the bare `def.def.target` this plan is keyed by
    /// (see [`ApplyPlan::targets`]'s doc comment on why the map key itself
    /// stays bare) so [`apply_target`] can bind the *right* physical table
    /// into its `INSERT`/`UPDATE`/`DELETE` SQL, rather than leaving a
    /// target explicitly qualified into a non-default schema (issue #76) to
    /// resolve against whatever `search_path` the executing session
    /// happens to carry.
    qualified_target: String,
    /// The ring's spelling of the source table, which the Re-derive read
    /// reads.
    qualified_source: String,
    /// Every column of the source, which the Re-derive read renders.
    row_columns: Vec<String>,
    rederive: Arc<Rederive>,
}

/// How Phase 3 evaluates a 1-1 Re-derive's row (#623 D6).
struct Rederive {
    def: TransformDef,
    source_columns: HashMap<String, ValueType>,
    paused: std::collections::HashSet<String>,
    /// A direct writer's: a row that fails to evaluate is left as it is.
    skip_failing: bool,
    /// For a definition that reads a relationship. A row read in Phase 3
    /// whose join key is not among the resolved ones is re-staged, so a
    /// later page builds a context for it.
    relationships: Option<JoinCoverage>,
}

impl std::fmt::Debug for Rederive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rederive")
            .field("def", &self.def.target)
            .field("paused", &self.paused)
            .field("relationships", &self.relationships.is_some())
            .finish()
    }
}

/// One target table this batch must clear in full before its own keyed
/// writes/deletes apply (issue #60: a truncate on `src_table` clears every
/// row a 1-1 transform ever derived from it). `pk` is the target's primary
/// key shape — reused, per [`compute`]'s existing convention, from the
/// truncated source's own PK introspection (a 1-1 target's key column
/// mirrors its source's) — needed so Phase 3's `DELETE ... RETURNING` can
/// name the right column. `hop_gen` is the triggering truncate sentinel's
/// own `hop_gen`, carried forward so keys the clear physically removes
/// propagate downstream at `hop_gen + 1`, exactly like any other
/// physically-changed key.
///
/// `src_changed` is the triggering truncate sentinel's own `src_changed`
/// (issues #51/#52's multi-hop gap), fan-in tie-broken by `min` across
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
    /// #623 D6: the latest truncating commit's ring `lsn` this batch
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
    /// `ddl::identity_key_columns` reports them (ungated by key type, issue
    /// #385) — so the clear can report each cleared group's key to the seam
    /// (issue #315).
    pk: Vec<PrimaryKeyColumn>,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
    /// #623 D3: whether the target is on the ledger, whose clear also
    /// empties the ledger and raises the truncate floor to `truncate_lsn`.
    on_ledger: bool,
    /// The latest truncating commit's ring `lsn` this batch carries.
    truncate_lsn: PgLsn,
}

/// The fan-in tie-break for [`StagedChange::Recompute::src_changed`]
/// (issues #51/#52's multi-hop gap): when more than one to-side change in a
/// batch feeds the same propagated key (forward propagation's `changed` map,
/// or reverse recompute's `(from_table, from_key)` accumulator), the
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

/// A [`Row`] as a JSON object of column text (`null` for SQL NULL), the
/// shape capture writes an image in. Hand-built, since this crate has no
/// JSON dependency.
#[cfg(test)]
fn row_to_json_text(row: &Row) -> String {
    let mut fields: Vec<String> = row
        .iter()
        .map(|(name, value)| {
            let value = match value {
                Some(text) => crate::intake::json_string(text),
                None => "null".to_string(),
            };
            format!("{}:{value}", crate::intake::json_string(name))
        })
        .collect();
    fields.sort_unstable();
    format!("{{{}}}", fields.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};
    use tokio_postgres::NoTls;
    use tokio_postgres::types::PgLsn;

    /// Issue #327: the per-image decode [`decode_images`] replaced, kept
    /// verbatim as the differential oracle.
    async fn decode_image_one_round_trip_each(pool: &Pool, image: &str) -> Row {
        let client = pool.get().await.expect("pool checkout");
        let rows = client
            .query(
                "select key, value from jsonb_each_text($1::text::jsonb)",
                &[&image],
            )
            .await
            .expect("decode one image");
        let mut row = Row::with_capacity(rows.len());
        for r in rows {
            row.insert(r.get(0), r.get(1));
        }
        row
    }

    /// Issue #327: the batched decode returns exactly what one
    /// `jsonb_each_text` round trip per image did, over images of every
    /// column type in each shape the ring carries them in, NULLs, empty
    /// images, unicode and escapes, wherever the chunk boundaries fall.
    #[tokio::test]
    async fn batched_image_decode_matches_one_round_trip_per_image() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        // A same-crate pool: see
        // `compute_only_buffers_metrics_and_a_single_flush_records_them_exactly_once`.
        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        )
        .expect("build a same-crate pool");
        let client = pool.get().await.expect("pool checkout");
        client
            .batch_execute(
                r#"create type mood as enum ('sad', 'ok', 'happy');
                create table corpus (
                    id integer primary key, i2 smallint, i8 bigint, num numeric,
                    r4 real, f8 double precision, t text, vc varchar(20), ch char(3),
                    b boolean, u uuid, d date, tm time, tmz timetz, ts timestamp,
                    tsz timestamptz, iv interval, by bytea, j json, jb jsonb, ip inet,
                    cidr_ cidr, mac macaddr, mac8 macaddr8, bt bit(4), vb varbit,
                    m money, x xml, tv tsvector, tq tsquery, o oid, e mood,
                    ia integer[], ta text[]);
                insert into corpus values
                (1, 7, 9007199254740993, 1.50, 1.5, 0.1, 'plain', 'v', 'ab', true,
                 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-02-29', '13:45:00.5',
                 '13:45:00+05:30', '2024-02-29 13:45:00.123456',
                 '2024-02-29 13:45:00+00', '1 year 2 mons 3 days 04:05:06',
                 '\x00ff10', '{"b": [1, 2.50, "x"], "a": {"n": null}}',
                 '{"b": 1e3, "a": "é", "c": [true, null]}', '10.0.0.1/8', '10.0.0.0/8',
                 '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05', B'1010', B'10101',
                 12.34, '<a>x</a>', 'a fat cat', 'fat & rat', 42, 'happy',
                 '{1,NULL,3}', '{"a b",NULL,"c\"d","e\\f",""}'),
                (2, -32768, -9223372036854775808, 'NaN', 'Infinity', '-0',
                 E'quote " backslash \\ newline \n tab \t bell \x07 é 值 🎉', '', '   ',
                 false, '00000000-0000-0000-0000-000000000000', 'infinity', '00:00',
                 '00:00-12', '-infinity', 'infinity', '-1 days -00:00:01', '\x', '[]',
                 '{}', '::1', '::/0', '00:00:00:00:00:00', '00:00:00:00:00:00:00:00',
                 B'0000', B'', -0.01, '', '', 'a', 0, 'sad', '{}', '{}'),
                (3, null, null, 123456789012345678901234567890.000000000000000000001,
                 '-Infinity', 1e308, E'ключ 😀 \u200b', null, null, null, null, null,
                 null, null, null, null, null, null, 'null', 'null', null, null, null,
                 null, null, null, null, null, null, null, null, null, null, null),
                (4, null, null, null, null, null, null, null, null, null, null, null,
                 null, null, null, null, null, null, null, null, null, null, null,
                 null, null, null, null, null, null, null, null, null, null, null);"#,
            )
            .await
            .expect("seed the corpus table");
        let columns: Vec<String> = client
            .query(
                "select attname::text from pg_attribute \
                 where attrelid = 'corpus'::regclass and attnum > 0 and not attisdropped \
                 order by attnum",
                &[],
            )
            .await
            .expect("read corpus columns")
            .iter()
            .map(|r| r.get(0))
            .collect();
        let mut corpus: Vec<String> = Vec::new();
        // The shape a live-row-derived image (a recompute's prior image)
        // carries: every value as its text, NULL as JSON null.
        let text_sql = format!(
            "select ({})::text from corpus t order by id",
            row_as_text_jsonb_sql("t", &columns)
        );
        for r in client.query(&text_sql, &[]).await.expect("text images") {
            corpus.push(r.get(0));
        }
        // Native JSON values: numbers, booleans, nested objects and arrays.
        for r in client
            .query("select to_jsonb(t.*)::text from corpus t order by id", &[])
            .await
            .expect("to_jsonb images")
        {
            corpus.push(r.get(0));
        }
        // The CDC shape, hand-encoded by `crate::intake::json_string`.
        let values_sql = format!(
            "select {} from corpus t order by id",
            columns
                .iter()
                .map(|c| format!("t.{}::text", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        for r in client.query(&values_sql, &[]).await.expect("cdc values") {
            let row: Row = columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.clone(), r.get::<_, Option<String>>(i)))
                .collect();
            corpus.push(row_to_json_text(&row));
        }
        for image in [
            "{}",
            " { } ",
            r#"{"a":null}"#,
            r#"{"":""}"#,
            r#"{"a":"1","a":"2"}"#,
            r#"{"b":"2","a":"1","c":null}"#,
            r#"{"\u00e9t\u00e9":"\ud83c\udf89","ключ":"值 🎉","esc":"\"\\\/\b\f\n\r\t\u0001"}"#,
            r#"{"n1":1.50,"n2":1e3,"n3":-0,"n4":0.000001,"n5":12345678901234567890,"n6":-1.5E-7}"#,
            r#"{"t":true,"f":false,"z":null,"arr":[1,"a",null,{"y":2,"x":1}],"obj":{"b":{},"a":[]}}"#,
            "{\n  \"spaced\" :\t\"out\" ,\r\n \"k\": [ 1 , 2 ] }",
            r#"{"line\nbreak key":"v","tab\tkey":null}"#,
        ] {
            corpus.push(image.to_string());
        }
        let images: Vec<&str> = corpus.iter().map(String::as_str).collect();
        let mut expected = Vec::with_capacity(images.len());
        for image in &images {
            expected.push(decode_image_one_round_trip_each(&pool, image).await);
        }
        assert!(
            expected.iter().any(Row::is_empty)
                && expected.iter().any(|row| row.values().any(Option::is_none))
                && expected.iter().any(|row| row.len() == columns.len()),
            "the corpus covers empty images, NULLs and every column"
        );

        for (max_images, max_bytes) in [
            (DECODE_CHUNK_IMAGES, DECODE_CHUNK_BYTES),
            (1, usize::MAX),
            (2, usize::MAX),
            (5, usize::MAX),
            (usize::MAX, 1),
            (usize::MAX, 200),
            (0, 0),
        ] {
            let decoded = decode_images_in_chunks(&pool, &images, max_images, max_bytes)
                .await
                .expect("batched decode");
            assert_eq!(
                decoded, expected,
                "chunks of at most {max_images} images / {max_bytes} bytes"
            );
        }

        // Many chunks at the production bounds, with empty images at the
        // edges and between chunks.
        let mut many: Vec<&str> = vec!["{}"];
        let mut many_expected: Vec<Row> = vec![Row::new()];
        for i in 0..(2 * DECODE_CHUNK_IMAGES + 3) {
            many.push(images[i % images.len()]);
            many_expected.push(expected[i % images.len()].clone());
        }
        many.push("{}");
        many_expected.push(Row::new());
        assert_eq!(
            decode_images(&pool, &many).await.expect("batched decode"),
            many_expected
        );
        assert_eq!(
            decode_images(&pool, &[]).await.expect("empty batch"),
            Vec::<Row>::new()
        );
        assert!(
            decode_images(&pool, &["{}", "not json"]).await.is_err(),
            "an undecodable image fails the batch, as it failed its own decode"
        );
    }

    /// Issue #677: a to-side image missing the relationship's `to_col` is
    /// `MissingColumn`, not "no key" (which would silently skip the parent's
    /// reverse propagation). An absent image, or a present `NULL` key, is
    /// still no key.
    #[test]
    fn a_relationship_key_missing_from_a_present_image_raises_missing_column() {
        let missing: Option<Row> =
            Some(HashMap::from([("name".to_string(), Some("a".to_string()))]));
        assert_eq!(
            relationship_key_text(&missing, "id", "buyer"),
            Err(EvalError::MissingColumn {
                field: "buyer".to_string(),
                column: "id".to_string(),
            })
        );
        let null_key: Option<Row> = Some(HashMap::from([("id".to_string(), None)]));
        assert_eq!(relationship_key_text(&null_key, "id", "buyer"), Ok(None));
        assert_eq!(relationship_key_text(&None, "id", "buyer"), Ok(None));
        let keyed: Option<Row> = Some(HashMap::from([("id".to_string(), Some("1".to_string()))]));
        assert_eq!(
            relationship_read_key(&null_key, &keyed, "id", "buyer"),
            Ok(Some("1".to_string())),
            "a NULL old key still falls back to the new image's key"
        );
        assert!(
            relationship_read_key(&missing, &keyed, "id", "buyer").is_err(),
            "an old image missing the key fails rather than falling back"
        );
    }

    /// Issue #344: a basis is compared to the source's current row as jsonb,
    /// so it must be valid JSON whatever text a column holds, keep SQL NULL
    /// distinct from the text `"null"`, and come out the same for the same
    /// row regardless of map order.
    #[test]
    fn a_row_basis_is_valid_escaped_json_with_nulls_kept_distinct() {
        let row: Row = HashMap::from([
            ("id".to_string(), Some("1".to_string())),
            ("note".to_string(), Some("say \"hi\"\n\\".to_string())),
            ("gone".to_string(), None),
            ("literal".to_string(), Some("null".to_string())),
        ]);
        assert_eq!(
            row_to_json_text(&row),
            r#"{"gone":null,"id":"1","literal":"null","note":"say \"hi\"\n\\"}"#
        );
    }

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

    /// Issue #134/#135 review follow-up: `ReverseGuardFailure::metric_label`'s
    /// per-variant mapping, as a pure in-memory unit test — no DB, no
    /// shared process-wide metrics registry, so (unlike an integration test
    /// that reads `trellis::metrics::Metrics::render_prometheus`, itself
    /// shared with every other test in the same binary and run in
    /// parallel by default) this can never be flaky. Deliberately narrow:
    /// this is the "each guard maps to its own, correct label" proof;
    /// `tests/apply_relationship_reverse.rs`'s own metrics test is what
    /// proves the *end-to-end wiring* (Phase 3 discovery -> buffered
    /// `ApplyOutcome::deferral_counts` -> post-commit flush -> registry)
    /// actually works, which a pure unit test of this function alone
    /// cannot.
    #[test]
    fn reverse_guard_failure_metric_labels_match_the_plan_docs_own_d5_block_names() {
        assert_eq!(
            ReverseGuardFailure::Watermark.metric_label(),
            "d5_block_barrier"
        );
        assert_eq!(
            ReverseGuardFailure::Generation.metric_label(),
            "d5_block_gen"
        );
        assert_eq!(
            ReverseGuardFailure::InFlight.metric_label(),
            "d5_block_inflight"
        );
        assert_eq!(
            ReverseGuardFailure::Ordering.metric_label(),
            "d5_block_order"
        );
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

    /// Epic #49's final cross-cutting review found that [`compute`] (Phase
    /// 2: no transaction, no locks) used to record straight into the global
    /// metrics registry — but [`drain_once`]/[`drain_many`]'s "reload,
    /// recompute, retry" loop calls `compute` again, unchanged, on a
    /// version-fence miss or a rolled-back Phase 3 failure, so a change that
    /// took more than one attempt to actually land got double-(or worse-)
    /// counted into `trellis_changes_applied_total` and both latency
    /// histograms. The fix: `compute` only *buffers* observations onto the
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
    /// Issue #409: the key is staged as three ring rows (an insert and two
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
        // Issue #512: ordered offsets above the real WAL insert position, as
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

    /// Issues #110/#446 regression pin: [`live_rows_join_cond`] matches a
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

    /// Issue #446: a [`read_live_rows_batch`] batch that includes
    /// `NULL`-keyed groups (an aggregate target read as a source) still
    /// probes the table's `UNIQUE NULLS NOT DISTINCT` index, at a
    /// single-column and a composite key, and still finds every key's row.
    /// Before the fix, one `NULL` key switched the whole batch's match on
    /// that column to `is not distinct from`, which can't use the index, so
    /// the plan was a nested loop over a sequential scan.
    #[tokio::test]
    async fn read_live_rows_batch_probes_the_index_when_a_batch_binds_a_null() {
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

        // The pre-#446 shape over a single-column batch with one NULL key can
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
            "the pre-#446 shape should not be able to probe the index, got:\n{plan}"
        );
    }

    /// Issue #778: a [`live_rows_query`] batch, run as its callers run it
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
                 analyze composite;",
            )
            .await
            .expect("seed the sources");
        // (source, whether its key is a single column: bounded, with lagging statistics)
        for (table, single) in [("public.single", true), ("public.composite", false)] {
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

    /// Issue #790's review: a composite-key 1-1 target's pre-lock
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
            let parts: Vec<&Vec<String>> = rows.iter().map(|(_, parts)| parts).collect();
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

    /// Issue #793: a single-column-key target's pre-lock
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

    /// Issue #531: only a record at or below its relationship's refresh
    /// stamp takes the live-row check. A relationship never refreshed, or a
    /// record above the stamp (every change after the refresh), reads
    /// nothing extra.
    #[test]
    fn only_a_record_at_or_below_the_refresh_stamp_is_overtaken() {
        let stamp = PgLsn::from(0x2000);
        assert!(!overtaken_by_refresh(Some(PgLsn::from(0x1000)), None));
        assert!(!overtaken_by_refresh(None, None));
        assert!(!overtaken_by_refresh(
            Some(PgLsn::from(0x2001)),
            Some(stamp)
        ));
        assert!(overtaken_by_refresh(Some(stamp), Some(stamp)));
        assert!(overtaken_by_refresh(Some(PgLsn::from(0x1000)), Some(stamp)));
        assert!(overtaken_by_refresh(None, Some(stamp)));
    }

    /// Issue #531: a to-side truncate's projection clear is skipped only
    /// when the truncate is at or below the refresh stamp. With no stamp, or
    /// no `lsn` to compare, the clear runs.
    #[test]
    fn only_a_truncate_at_or_below_the_refresh_stamp_skips_its_clear() {
        let stamp = PgLsn::from(0x2000);
        assert!(!truncate_overtaken_by_refresh(
            Some(PgLsn::from(0x1000)),
            None
        ));
        assert!(!truncate_overtaken_by_refresh(None, None));
        assert!(!truncate_overtaken_by_refresh(None, Some(stamp)));
        assert!(!truncate_overtaken_by_refresh(
            Some(PgLsn::from(0x2001)),
            Some(stamp)
        ));
        assert!(truncate_overtaken_by_refresh(Some(stamp), Some(stamp)));
        assert!(truncate_overtaken_by_refresh(
            Some(PgLsn::from(0x1000)),
            Some(stamp)
        ));
    }

    /// Issue #125 regression pin, part 1: [`key_array_filter`] renders the
    /// fixed, indexable shape — the bound `$1` array cast to the column's
    /// own type, the column reference itself left uncast — when the
    /// column's native type is known. A future edit that puts the `::text`
    /// cast back on `col_ident` (rather than on the array) fails this
    /// immediately, without needing a live Postgres connection at all.
    #[test]
    fn key_array_filter_casts_the_bound_array_not_the_column_when_type_is_known() {
        let filter = key_array_filter(r#""join_key""#, Some("integer"));
        assert_eq!(filter, r#""join_key" = any($1::text[]::integer[])"#);
        assert!(
            !filter.starts_with(r#""join_key"::text"#),
            "the column reference itself must never be cast to ::text — that's exactly the \
             cast that defeats a btree index on it (issue #125): {filter}"
        );
    }

    /// Issue #125 regression pin, part 2: without a known native type (the
    /// column couldn't be introspected), [`key_array_filter`] must still
    /// fall back to the old, unindexable `::text` form rather than emitting
    /// invalid SQL (an untyped `any($1::text[])` with no cast at all would
    /// leave `col_ident`'s type to ordinary operator resolution, which is
    /// not guaranteed to match `col_ident`'s real type for every allowed
    /// join-key type).
    #[test]
    fn key_array_filter_falls_back_to_the_old_text_cast_when_type_is_unknown() {
        let filter = key_array_filter(r#""join_key""#, None);
        assert_eq!(filter, r#""join_key"::text = any($1::text[])"#);
    }

    /// Issue #125's actual regression: three relationship key-lookup sites
    /// in this module (`from_side_keys`, `fetch_to_side_rows`,
    /// `fetch_relationship_projection_rows`) used to render their join-key
    /// filter as `col::text = any($1::text[])` — a cast on the *column*,
    /// which Postgres can never satisfy with a plain btree index on that
    /// column, regardless of table size or statistics. This proves the
    /// fixed shape's real-world effect end to end against a live Postgres:
    /// [`key_column_pg_type`] correctly discovers a real column's native
    /// type from `pg_catalog`, and the resulting [`key_array_filter`]
    /// clause lets the planner pick an index scan for a lookup of a handful
    /// of keys out of a much larger indexed table — while the old,
    /// pre-#125 filter shape, run against the very same table and index,
    /// can only ever plan a sequential scan (proving the bug was real, not
    /// just that the fixed form happens to also allow one).
    #[tokio::test]
    async fn key_array_filter_lets_postgres_use_the_index_the_old_text_cast_form_could_not() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Large enough that an index scan over a handful of keys is
        // unambiguously cheaper than a full scan, so the planner's own cost
        // model — not just "an index technically exists" — picks the index
        // for the fixed filter shape.
        client
            .batch_execute(
                "create table big_from_side (id bigint primary key, join_key integer); \
                 create index big_from_side_join_key_idx on big_from_side (join_key); \
                 insert into big_from_side \
                 select g, g % 5000 from generate_series(1, 200000) g; \
                 analyze big_from_side;",
            )
            .await
            .expect("seed a large indexed table");

        let pool_config =
            crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = crate::pool::Pool::new(&pool_config).expect("build a same-crate pool");

        let pg_type = key_column_pg_type(&pool, "big_from_side", "join_key")
            .await
            .expect("introspect join_key's type");
        assert_eq!(
            pg_type.as_deref(),
            Some("integer"),
            "the join column's native pg_catalog type must be discovered correctly"
        );

        let keys = vec!["42".to_string()];

        let fixed_filter = key_array_filter(r#""join_key""#, pg_type.as_deref());
        let fixed_plan = explain_plan_lines(
            &client,
            &format!("select 1 from \"big_from_side\" where {fixed_filter}"),
            &keys,
        )
        .await;
        assert!(
            fixed_plan.iter().any(|line| line.contains("Index")),
            "issue #125's fix should let Postgres use the btree index on join_key, got:\n{}",
            fixed_plan.join("\n")
        );
        assert!(
            !fixed_plan.iter().any(|line| line.contains("Seq Scan")),
            "issue #125's fix should not fall back to a sequential scan, got:\n{}",
            fixed_plan.join("\n")
        );

        // Sanity check / regression pin: the pre-#125 shape casts the
        // *column* to `::text`, which no btree index on the untransformed
        // column can ever satisfy — this must remain a sequential scan on
        // this same table and index, proving the bug this test guards
        // against was real.
        let old_filter = r#""join_key"::text = any($1::text[])"#;
        let old_plan = explain_plan_lines(
            &client,
            &format!("select 1 from \"big_from_side\" where {old_filter}"),
            &keys,
        )
        .await;
        assert!(
            old_plan.iter().any(|line| line.contains("Seq Scan")),
            "sanity check: the old col::text cast form must force a sequential scan \
             (if this fails, Postgres itself changed how it plans this — re-check the \
             fixture), got:\n{}",
            old_plan.join("\n")
        );
    }

    /// `EXPLAIN`'s plan text, one line per returned row — used by
    /// [`key_array_filter_lets_postgres_use_the_index_the_old_text_cast_form_could_not`]
    /// to assert on the *shape* of the plan Postgres actually chooses
    /// (`Index` vs. `Seq Scan`) rather than merely on row-level output,
    /// which can't distinguish "fast, indexed lookup" from "slow, full
    /// table scan that happens to return the same rows."
    async fn explain_plan_lines(
        client: &tokio_postgres::Client,
        sql: &str,
        keys: &[String],
    ) -> Vec<String> {
        client
            .query(&format!("explain {sql}"), &[&keys])
            .await
            .expect("explain")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect()
    }

    // -----------------------------------------------------------------
    // Issue #173 phase 3: ReverseTrigger — one test per variant, per
    // shared enumeration function, pinning that each existing
    // propagation path's own from-side determination (unchanged by this
    // refactor — see the full trellis test suite, which exercises the
    // to-many reverse, TRUNCATE-clear, reverse-delta, and reverse-fallback
    // paths end to end through these same two functions) is driven by the
    // exhaustive match this issue introduces, not by ad hoc per-path SQL.
    // -----------------------------------------------------------------

    /// A small from-side fixture shared by every `ReverseTrigger` test
    /// below: two rows share `join_key = 'a'`, one carries `'b'`, and one
    /// is `NULL` — enough to distinguish "matches a specific key,"
    /// "matches nothing," and "excluded because NULL" in one table.
    async fn seed_reverse_trigger_fixture(
        client: &tokio_postgres::Client,
    ) -> Vec<PrimaryKeyColumn> {
        client
            .batch_execute(
                "create table from_side_fixture (id bigint primary key, join_key text); \
                 insert into from_side_fixture (id, join_key) values \
                 (1, 'a'), (2, 'a'), (3, 'b'), (4, null);",
            )
            .await
            .expect("seed the from-side fixture");
        vec![PrimaryKeyColumn {
            name: "id".to_string(),
            data_type: "bigint".to_string(),
            nullable: false,
            collation: None,
        }]
    }

    /// [`ReverseTrigger::Keys`] via [`from_side_keys`] — the shape the
    /// to-many reverse path (`compute`'s by-source loop) and the
    /// keyed half of every other path construct. Matches exactly the rows
    /// whose `join_key` is in the requested set, reporting back which key
    /// each one matched (so a caller can attribute `hop_gen`/`src_changed`
    /// per key, per this function's own doc comment) — a key present in
    /// the trigger but matching no row (`"z"`) contributes nothing, and the
    /// `NULL`-keyed row is never returned for any key.
    #[tokio::test]
    async fn reverse_trigger_keys_matches_from_side_keys_and_reports_the_matched_value() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let from_pk = seed_reverse_trigger_fixture(&client).await;

        let pool_config =
            crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = crate::pool::Pool::new(&pool_config).expect("build a same-crate pool");

        let join_keys = vec!["a".to_string(), "z".to_string()];
        let mut matches = from_side_keys(
            &pool,
            "from_side_fixture",
            &from_pk,
            "join_key",
            &ReverseTrigger::Keys(&join_keys),
        )
        .await
        .expect("from_side_keys(Keys)");
        matches.sort();
        assert_eq!(
            matches,
            vec![
                ("1".to_string(), Some("a".to_string())),
                ("2".to_string(), Some("a".to_string())),
            ],
            "only the rows matching a requested key come back, each reporting which \
             key it matched; a requested key with no match (\"z\") and the NULL-keyed \
             row must both be absent"
        );
    }

    /// [`ReverseTrigger::WholeKeyspace`] via [`from_side_keys`] — the
    /// TRUNCATE-clear path's shape (issue #98/#165/#168): every currently
    /// non-`NULL` `join_key` row comes back, regardless of its specific
    /// value (there is no value list to match against — see the variant's
    /// own doc comment), and each is reported with `None` rather than a
    /// specific matched key, since none was matched against.
    #[tokio::test]
    async fn reverse_trigger_whole_keyspace_matches_every_non_null_row_via_from_side_keys() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let from_pk = seed_reverse_trigger_fixture(&client).await;

        let pool_config =
            crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = crate::pool::Pool::new(&pool_config).expect("build a same-crate pool");

        let mut matches = from_side_keys(
            &pool,
            "from_side_fixture",
            &from_pk,
            "join_key",
            &ReverseTrigger::WholeKeyspace,
        )
        .await
        .expect("from_side_keys(WholeKeyspace)");
        matches.sort();
        assert_eq!(
            matches,
            vec![
                ("1".to_string(), None),
                ("2".to_string(), None),
                ("3".to_string(), None),
            ],
            "every non-NULL-keyed row must come back regardless of its specific value \
             (rows 1-3), the NULL-keyed row (4) must not, and none of them report a \
             specific matched key"
        );
    }

    /// Issue #561: a [`ToSide`] types its key filter from the catalog entry
    /// of its unquoted `identity`, and reads the row under its quoted
    /// `table`. Handed the quoted name, the lookup would find nothing
    /// (`to_regclass` of a twice-quoted name is `NULL`) and the filter would
    /// silently fall back to casting the column, which no index serves
    /// (#125) — correct rows, so nothing downstream notices. Pinned here on a
    /// mixed-case to-side in a mixed-case schema.
    #[tokio::test]
    async fn to_side_key_filter_types_a_mixed_case_to_side_by_its_identity() {
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
                "create schema \"Shop\"; \
                 create table \"Shop\".\"Catalog\" (id bigint primary key, price numeric); \
                 insert into \"Shop\".\"Catalog\" (id, price) values (1, 10)",
            )
            .await
            .expect("seed a mixed-case to-side");

        let identity = "Shop.Catalog".to_string();
        let to_side = ToSide {
            table: ddl::qualified_source_table(&identity),
            identity,
            seam_fed: true,
            key_pg_type: std::sync::OnceLock::new(),
        };
        let txn = client.transaction().await.expect("open txn");
        let filter = to_side.key_filter(&txn, "id").await.expect("key filter");
        assert_eq!(
            filter, r#"t."id" = $1::text::bigint"#,
            "the key filter compares the native column, typed from the catalog"
        );
        let price: String = txn
            .query_one(
                &format!("select price::text from {} t where {filter}", to_side.table),
                &[&"1"],
            )
            .await
            .expect("read the to-side row by key")
            .get(0);
        assert_eq!(price, "10");
    }

    /// [`ReverseTrigger::Keys`] via [`from_side_rows_for_trigger_txn`] — the
    /// full-row, transactional shape [`stage_reverse_recompute_fallback`]
    /// drives, inside
    /// Phase 3's already-open transaction. Proves the enum-driven dispatch
    /// still returns full row images (not just the primary key
    /// [`from_side_keys`] returns), and still excludes a key with no match
    /// and the NULL-keyed row.
    #[tokio::test]
    async fn reverse_trigger_keys_fetches_full_rows_via_from_side_rows_for_trigger_txn() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let from_pk = seed_reverse_trigger_fixture(&client).await;

        let txn = client.transaction().await.expect("open txn");
        let join_keys = vec!["a".to_string()];
        let row_columns = live_row_columns(&txn, "from_side_fixture")
            .await
            .expect("introspect from_side_fixture's columns");
        let mut rows = from_side_rows_for_trigger_txn(
            &txn,
            "from_side_fixture",
            "join_key",
            &from_pk,
            &ReverseTrigger::Keys(&join_keys),
            &row_columns,
        )
        .await
        .expect("from_side_rows_for_trigger_txn(Keys)");
        txn.rollback().await.expect("rollback");

        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec!["1", "2"],
            "both rows matching join_key = 'a' must come back, full row images"
        );
        for (_, row) in &rows {
            assert_eq!(
                row.get("join_key"),
                Some(&Some("a".to_string())),
                "the full row image must carry the matched column's real value, not \
                 just its primary key"
            );
        }
    }

    /// [`ReverseTrigger::WholeKeyspace`] via
    /// [`from_side_rows_for_trigger_txn`] — no current call site can reach
    /// this (both construct [`ReverseTrigger::Keys`] inline, and nothing in
    /// the module forwards a `ReverseTrigger`), but the arm is a typed
    /// [`ApplyError::ReverseTriggerNotResolvable`] rather than a panic, per
    /// this module's stated convention for invariants a function cannot
    /// enforce itself. Pinned so a future caller that wires a whole-keyspace
    /// trigger into the reverse delta/fallback path gets a visible failed
    /// drain rather than an aborted worker — and so nobody "fixes" this by
    /// quietly falling through to a `Keys`-shaped read, which would silently
    /// enumerate nothing.
    #[tokio::test]
    async fn reverse_trigger_whole_keyspace_is_a_typed_error_not_a_panic_in_the_txn_lookup() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let from_pk = seed_reverse_trigger_fixture(&client).await;

        let txn = client.transaction().await.expect("open txn");
        // `WholeKeyspace` errors out before `row_columns` is ever read (see
        // the function's own early `match trigger`), so an empty slice is
        // fine here — this test is about the error arm, not the row decode.
        let err = from_side_rows_for_trigger_txn(
            &txn,
            "from_side_fixture",
            "join_key",
            &from_pk,
            &ReverseTrigger::WholeKeyspace,
            &[],
        )
        .await
        .expect_err("a whole-keyspace trigger must not resolve against full row images");
        txn.rollback().await.expect("rollback");

        match err {
            ApplyError::ReverseTriggerNotResolvable { from_table } => {
                assert_eq!(
                    from_table, "from_side_fixture",
                    "the error must name the from-table whose enumeration was asked for, so an \
                     operator can locate the offending relationship"
                );
            }
            other => panic!(
                "expected a typed ReverseTriggerNotResolvable, got {other:?} — the whole-keyspace \
                 arm must stay a typed error, not a panic and not a silent empty result"
            ),
        }
    }

    /// Issue #670 review: `classify` decides by the innermost `ApplyError`,
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
            7,
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
}

/// Phase 2's output: an in-memory plan Phase 3 applies inside one
/// transaction, with no further catalog reads of its own beyond the version
/// fence and the target-mutation seam's reader lookup.
///
/// `versions` is the version-fence's read set: every source table this
/// batch evaluated against, and the `source_table_versions.version` Phase 2
/// loaded for it (`None` for a source with no definitions at all — still
/// fenced, so a definition created against it mid-drain is caught too).
/// Which targets have downstream readers is *not* decided here: Phase 3's
/// target-mutation seam (`staging::target_mutations`) resolves that inside
/// its own transaction, for every target any step wrote.
#[derive(Debug, Clone, Default)]
pub struct ApplyPlan {
    versions: HashMap<String, Option<i64>>,
    targets: BTreeMap<String, TargetPlan>,
    /// The aggregate targets, all on the ledger (`super::ledger`, #623 D3 to
    /// D5), each with this page's records for it, keyed by the definition's
    /// bare target name. Ordered, so every page writes them in one order.
    ledger_targets: BTreeMap<String, super::ledger::LedgerTargetPlan>,
    /// Targets to clear in full at Phase 3, keyed by target table name —
    /// issue #60's truncate propagation. See [`ClearPlan`].
    clears: HashMap<String, ClearPlan>,
    /// The aggregate-target counterpart to [`ApplyPlan::clears`]: a
    /// truncate on an aggregate definition's source clears every group.
    /// Applied by the same `clear_target` as a 1-1 clear, keyed by the
    /// target's `GROUP BY` columns, so each cleared group reaches the seam
    /// and a transform chained off the aggregate target sees it (issue #315;
    /// this used to be a documented propagation gap).
    aggregate_clears: HashMap<String, AggregateClearPlan>,
    /// Issue #16: the (non-truncate) folded records whose `(src_table,
    /// key)` is poisoned for a definition that reads it, each paired with
    /// that definition's id — left out of that definition's plan only
    /// (#799: every other reader applies it), and instead parked into
    /// `poison_held` for it by [`apply_and_mark_drained`], in the same
    /// Phase-3 transaction, before the drained mark. See
    /// `quarantine::park_batch_contribution`'s doc comment for why this must
    /// happen even when this batch didn't cause the key's eviction.
    poisoned_park: Vec<(i64, FoldedChange)>,
    /// Issue #16: every non-truncate `(src_table, key)` this batch computed
    /// against for at least one definition — cleared from `key_deaths` by
    /// [`apply_and_mark_drained`] on a successful commit, per doc 06's "a
    /// clean drain clears the counters for the keys it just applied", for
    /// every definition the key isn't poisoned for (#799).
    applied_keys: Vec<(String, String)>,
    /// Issue #30's reverse recompute: when a *to-side* (related) row changed,
    /// each `(from_table, from_key_text, hop_gen)` here is a from-side row
    /// whose relationship enrichment depends on that changed related row and
    /// so must be re-derived. Resolved in Phase 2 (a live join-key lookup on
    /// `from_table` — see [`from_side_keys`]) and emitted as ordinary
    /// image-less [`StagedChange::Recompute`]s by [`apply_and_mark_drained`],
    /// reusing the same async staging/apply/fence pipeline forward propagation
    /// uses rather than any bespoke persisted reverse index. `hop_gen` is the
    /// triggering related-row change's own `hop_gen + 1`, hop-bounded at emit.
    /// The trailing `Option<SystemTime>` is the triggering change's
    /// `src_changed`, fan-in tie-broken by [`earliest_src_changed`] when more
    /// than one to-side change resolves to the same `(from_table,
    /// from_key)` (issues #51/#52's multi-hop gap).
    reverse_recomputes: Vec<DerivedRecompute>,
    /// Epic #49 cross-cutting review fix (issues #51/#52): every
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
    /// Issue #130, epic #127: every to-one relationship this batch's
    /// relationship resolution touched, keyed by `relationship_definitions.id`
    /// — accumulated across every [`build_relationship_context`] call this
    /// `compute` pass makes (several definitions, or several sources, can
    /// share one relationship) and bumped, once per touched key, in Phase 3
    /// by [`apply_and_mark_drained_many`]. See [`RelationshipGenBump`]'s doc
    /// comment for exactly what "touched" means and the #133 gap it's a
    /// documented approximation of.
    relationship_gen_bumps: HashMap<i64, RelationshipGenBump>,
    /// Issue #131, epic #127: every to-one relationship's parent-keyed
    /// reverse record this batch's fold produced — see
    /// [`RelationshipReverseRecord`]'s doc comment. Applied by
    /// [`apply_and_mark_drained_many`]'s "3d" step, after the ordinary
    /// aggregate-target writes (step 3b) and the forward gen-bump (step 3c).
    relationship_reverses: Vec<RelationshipReverseRecord>,
    /// Issue #168: every to-one relationship's settled parent projection
    /// (qualified table name) a `TRUNCATE` on that relationship's to-side
    /// emptied this batch — cleared in full by
    /// [`apply_and_mark_drained_many`], alongside [`ApplyPlan::clears`]/
    /// [`ApplyPlan::aggregate_clears`]. See the `compute` truncate loop's
    /// own comment on why this can't reuse [`ApplyPlan::relationship_reverses`]
    /// (a `TRUNCATE`'s sentinel carries no image to upsert or delete with).
    /// Keyed by `relationship_id`.
    relationship_projection_clears: std::collections::BTreeMap<i64, RelationshipProjectionClear>,
}

/// One entry of [`ApplyPlan::relationship_projection_clears`].
#[derive(Debug, Clone)]
struct RelationshipProjectionClear {
    /// The settled projection, qualified.
    qualified_projection: String,
    /// The latest truncating commit this batch carries for the to-side
    /// (the folded truncate's `lsn`). Issue #531: a truncate at or below
    /// the relationship's refresh stamp is one the refresh already read, so
    /// the clear is skipped ([`truncate_overtaken_by_refresh`]).
    lsn: Option<PgLsn>,
}

/// Every join value `change`'s raw ring rows gave a to-side's `to_col`,
/// including any the fold erased from both folded images (#784): a parent
/// born and deleted inside the batch, or one re-keyed through a value and
/// on. A child may have read the parent live under such a value (an
/// aggregate's ledger write does, #623 D5), and no reverse record names it.
///
/// When the `to_col` is the to-side's whole, non-nullable row identity
/// (`key_col`), the ring key is its only value. Otherwise the fold read its
/// values out of every raw row's new image, labelled by column
/// ([`FoldedChange::to_col_values`], #785).
fn touched_join_values<'a>(
    change: &'a FoldedChange,
    to_col: &'a str,
    key_col: Option<&str>,
) -> impl Iterator<Item = &'a String> + 'a {
    let ring_key = (key_col == Some(to_col)).then_some(&change.key);
    ring_key.into_iter().chain(
        change
            .to_col_values
            .iter()
            .filter(move |(column, _)| column == to_col)
            .map(|(_, value)| value),
    )
}

/// The image [`compute`] decodes as a change's *old side*: its folded
/// `old_image` when it carries real images, or, for an image-less recompute,
/// its prior-image hint (issue #315, `FoldedChange::prior_image`) — the row
/// as it stood before an upstream target write. An image-less change is
/// still re-read live for its new side; the old side only tells a
/// relationship reader which parent the row moved away from.
fn old_side_image(change: &FoldedChange) -> Option<&String> {
    if change.old_image.is_none() && change.new_image.is_none() {
        change.prior_image.as_ref()
    } else {
        change.old_image.as_ref()
    }
}

/// The primary key [`compute`] keys `qualified_source`'s changes by
/// ([`ddl::source_primary_key`]), or `None` when the key can't be used and
/// no definition that isn't frozen reads the table, so `compute` skips them.
///
/// A dropped source is [`ApplyError::SourceTableDropped`], reported under
/// `qualified_source`, the ring's own spelling (issue #267): its consumers
/// (`quarantine::purge_dropped_table` and `drain_once`/`drain_many`'s
/// `folded.retain`) match it against a ring row's `src_table` as an exact
/// string.
///
/// Issue #768: a key that no longer passes the key gate (a type off the
/// allowlist, or no key at all) halts the page while any definition that
/// isn't frozen reads the table ([`catalog::has_unfrozen_reader`], under the
/// canonical `source_key`), directly or through a relationship: one that
/// applies would key its rows wrongly or not at all, and one still waiting
/// for its build, or under a chunked or direct one, re-derives from the
/// relationship's settled projection its rows keep current. A table whose
/// readers are all frozen (paused, capture-failed or quarantined) is skipped
/// instead. Its changes drain with the page, which marks its claim drained
/// whatever the plan holds, exactly as a paused definition's share is
/// dropped when `catalog::transforms_for_source` leaves it out, and a resume
/// rebuilds from the source. A relationship's settled projection on it gets
/// none of them either, so a resume refreshes the projections on every
/// to-side the resumed definition reads (`quarantine::resume_transform`).
/// Asked only on the error, so a drain over usable keys reads nothing more.
/// The answer is fenced: the version of each relationship from-side reading
/// the table is read into `versions` first, and a definition, its resume and
/// its edit each bump their source's, so a page can't commit a skip after a
/// reader it didn't see.
///
/// The halt pauses every such reader and what is downstream of it
/// (`super::halt`, #663, which is #703 R2's "pause the reader in the
/// drain"), so the page's retry finds none and skips the table.
async fn source_key_for_apply(
    pool: &Pool,
    qualified_source: &str,
    source_key: &str,
    versions: &mut HashMap<String, Option<i64>>,
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
    if !no_unfrozen_reader(pool, source_key, versions).await? {
        return Err(err.into());
    }
    tracing::warn!(
        src_table = %qualified_source,
        error = %err,
        "every definition reading this table is frozen and its key can't be used; \
         skipping its changes"
    );
    Ok(None)
}

/// Whether no definition that isn't frozen reads `source_key`, directly or
/// through a relationship ([`has_unfrozen_reader`]), so a page may skip the
/// table's changes ([`source_key_for_apply`], [`compute_page`]).
///
/// The answer is fenced. The fence the skip is judged under is read before
/// the readers are: a reader of `source_key` through a relationship is a
/// definition on the relationship's from-side, and a definition, its resume
/// or its edit commits with a bump of its source's fence, so each such
/// from-side's version goes into `versions`. `compute`'s caller already
/// holds `source_key`'s own entry, for a direct reader. So a page that skips
/// the table can't commit after a reader it didn't see.
async fn no_unfrozen_reader(
    pool: &Pool,
    source_key: &str,
    versions: &mut HashMap<String, Option<i64>>,
) -> Result<bool, ApplyError> {
    for rel in catalog::relationships_to_table(pool, source_key).await? {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            versions.entry(rel.qualified_from_table())
        {
            let version = catalog::source_table_version(pool, entry.key()).await?;
            entry.insert(version);
        }
    }
    Ok(!has_unfrozen_reader(pool, source_key).await?)
}

/// Whether `err`, from [`ddl::source_primary_key`], is its key gate: the
/// table has no usable key (none, or one of a type off the allowlist).
fn is_key_gate(err: &DdlError) -> bool {
    matches!(
        err,
        DdlError::NoPrimaryKey { .. } | DdlError::UnsupportedPrimaryKeyType { .. }
    )
}

/// [`catalog::has_unfrozen_reader`] on a pooled connection.
async fn has_unfrozen_reader(pool: &Pool, table: &str) -> Result<bool, ApplyError> {
    let client = pool.get().await?;
    Ok(catalog::has_unfrozen_reader(&**client, table).await?)
}

/// A relationship from-side's primary key, which a reverse recompute keys
/// the from-side rows a to-side change reaches by, or `None` when the key
/// can't be used and every definition reading the from-side is frozen.
///
/// Issue #768, the from-side's half of [`source_key_for_apply`]: the
/// recomputes re-derive the from-side's rows for the definitions reading
/// it, and a frozen one's resume rebuilds it from the source, so with no
/// other reader the caller stages none, as `compute` skips the from-side's
/// own changes. Without this a write to the to-side halted the drain on the
/// from-side's key though no reader would apply what it staged. A reader
/// that isn't frozen halts the page, which pauses it (`super::halt`, #663),
/// and so does a from-side that is gone.
///
/// Both skips are fenced (#823). A resume's build reads the relationship's
/// settled projection, so a page that skipped a recompute and commits after
/// the resume would leave the resumed reader without that change. The
/// from-side's own fence is read first, then its readers
/// ([`no_unfrozen_reader`]), and a definition, its resume and its edit each
/// bump their source's fence, so a page can't commit after a resume it
/// didn't see.
///
/// `fence.skip_frozen` is [`compute_page`]'s (issue #766): on the retry after
/// Postgres refused the drain a read, a from-side no unfrozen definition
/// reads is skipped whatever its key, as `compute_page` skips that table's
/// own changes. Its readers' halt may be what froze them, for that very
/// refusal, and the from-side read would be refused again on every pass.
async fn from_side_key(
    pool: &Pool,
    qualified_from_table: &str,
    fence: FromSideFence<'_>,
) -> Result<Option<Vec<PrimaryKeyColumn>>, ApplyError> {
    let FromSideFence {
        versions,
        skip_frozen,
    } = fence;
    if skip_frozen {
        read_fence(pool, qualified_from_table, versions).await?;
        if no_unfrozen_reader(pool, qualified_from_table, versions).await? {
            tracing::warn!(
                from_table = %qualified_from_table,
                "every definition reading this relationship from-side is frozen, and the drain \
                 was refused a read or write; staging no recompute of its rows"
            );
            return Ok(None);
        }
    }
    let err = match ddl::source_primary_key(pool, qualified_from_table).await {
        Ok(pk) => return Ok(Some(pk)),
        Err(err) => err,
    };
    if !is_key_gate(&err) || quarantine::source_table_missing(pool, qualified_from_table).await? {
        return Err(err.into());
    }
    read_fence(pool, qualified_from_table, versions).await?;
    if !no_unfrozen_reader(pool, qualified_from_table, versions).await? {
        return Err(err.into());
    }
    tracing::warn!(
        from_table = %qualified_from_table,
        error = %err,
        "every definition reading this relationship from-side is frozen and its key \
         can't be used; staging no recompute of its rows"
    );
    Ok(None)
}

/// Reads `table`'s version fence into `versions` unless the page already
/// holds it: the first read is the oldest, so the safest to commit under.
async fn read_fence(
    pool: &Pool,
    table: &str,
    versions: &mut HashMap<String, Option<i64>>,
) -> Result<(), ApplyError> {
    if let std::collections::hash_map::Entry::Vacant(entry) = versions.entry(table.to_string()) {
        let version = catalog::source_table_version(pool, entry.key()).await?;
        entry.insert(version);
    }
    Ok(())
}

/// The page's version fence (`versions`, the fence's read set) and whether the
/// page is on its retry after Postgres refused the drain a read
/// (`skip_frozen`, [`compute_page`]'s), which [`from_side_key`] judges its
/// skips under.
struct FromSideFence<'a> {
    versions: &'a mut HashMap<String, Option<i64>>,
    skip_frozen: bool,
}

impl<'a> FromSideFence<'a> {
    fn new(versions: &'a mut HashMap<String, Option<i64>>, skip_frozen: bool) -> Self {
        Self {
            versions,
            skip_frozen,
        }
    }
}

/// Each of `changes`' [`KeyExclusion`] (#799), or `None` when every one is
/// empty, the common case, which costs no allocation: `changes` are the folded
/// changes to `source_key` (canonical), `defs` the definitions that apply
/// it directly, `inbound_rels` the relationships whose to-side it is, and
/// `poisoned` the batch's poison ([`quarantine::poisoned_keys_among`]).
///
/// A relationship's reverse work leaves a poisoned key out only when every
/// reader of the relationship that isn't frozen holds the key: it keeps the
/// settled projection and the from-side's rows current for all of them at
/// once. A relationship with no such reader serves nobody, so it leaves the
/// key out too: a define or resume of its first reader refreshes its
/// projection from the table (#768), and a failure in its work, which
/// isolation can charge to no reader of it, would otherwise fail the page on
/// every drain once every direct reader holds the key. Its readers are read
/// only when one of `changes` is poisoned. `focus` overrides one key's direct
/// readers for an isolation probe, and never a relationship's.
async fn key_exclusions(
    pool: &Pool,
    source_key: &str,
    changes: &[&FoldedChange],
    defs: &[crate::defs::model::Definition],
    inbound_rels: &[RelationshipDefinition],
    poisoned: &HashMap<(String, String), HashSet<i64>>,
    focus: Option<&ProbeFocus<'_>>,
) -> Result<Option<Vec<KeyExclusion>>, ApplyError> {
    let lookup = |key: &str| poisoned.get(&(source_key.to_string(), key.to_string()));
    let any_poisoned = !poisoned.is_empty() && changes.iter().any(|c| lookup(&c.key).is_some());
    if !any_poisoned && focus.is_none() {
        return Ok(None);
    }
    let rel_readers = if any_poisoned && !inbound_rels.is_empty() {
        relationship_reader_map(pool).await?
    } else {
        HashMap::new()
    };
    let no_readers: Vec<(i64, String)> = Vec::new();
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
                    exclusion.park_for.insert(def.id);
                }
            }
            if let Some(ids) = poisoned_for {
                for rel in inbound_rels {
                    let readers = rel_readers.get(&rel.id).unwrap_or(&no_readers);
                    if readers.iter().all(|(id, _)| ids.contains(id)) {
                        exclusion.rels.insert(rel.id);
                    }
                    for (id, _) in readers {
                        if ids.contains(id) {
                            exclusion.park_for.insert(*id);
                        }
                    }
                }
            }
            exclusion
        })
        .collect();
    Ok((!exclusions.iter().all(KeyExclusion::is_empty)).then_some(exclusions))
}

/// Whether `exclusions` ([`key_exclusions`]) leave change `i` out of the
/// reverse work of relationship `rel`.
fn skips_relationship(exclusions: &Option<Vec<KeyExclusion>>, i: usize, rel: i64) -> bool {
    exclusions
        .as_ref()
        .is_some_and(|exclusions| exclusions[i].rels.contains(&rel))
}

/// One definition's share of a source's `changes` and their decoded images
/// (`rows`, `old_rows`): every change but those `exclusions` leave out of
/// `transform_id` (#799). Borrowed when it leaves out none, the common case.
#[allow(clippy::type_complexity)]
fn definition_share<'c, 'f>(
    transform_id: i64,
    changes: &'c [&'f FoldedChange],
    rows: &'c [Option<Row>],
    old_rows: &'c [Option<Row>],
    exclusions: &Option<Vec<KeyExclusion>>,
) -> (
    Cow<'c, [&'f FoldedChange]>,
    Cow<'c, [Option<Row>]>,
    Cow<'c, [Option<Row>]>,
) {
    let Some(exclusions) = exclusions
        .as_ref()
        .filter(|exclusions| exclusions.iter().any(|e| e.defs.contains(&transform_id)))
    else {
        return (
            Cow::Borrowed(changes),
            Cow::Borrowed(rows),
            Cow::Borrowed(old_rows),
        );
    };
    let keep: Vec<usize> = (0..changes.len())
        .filter(|&i| !exclusions[i].defs.contains(&transform_id))
        .collect();
    (
        Cow::Owned(keep.iter().map(|&i| changes[i]).collect()),
        Cow::Owned(keep.iter().map(|&i| rows[i].clone()).collect()),
        Cow::Owned(keep.iter().map(|&i| old_rows[i].clone()).collect()),
    )
}

/// The definitions that aren't frozen and read through each relationship,
/// by relationship id, each with its bare target, in id order (#799).
async fn relationship_reader_map(
    pool: &Pool,
) -> Result<HashMap<i64, Vec<(i64, String)>>, ApplyError> {
    let client = pool.get().await?;
    let catalog = crate::capture::columns::load_catalog(&**client, pool.schema()).await?;
    let unfrozen: HashSet<i64> = client
        .query(
            "select id from transform_definitions where status = any($1)",
            &[&crate::defs::model::TransformStatus::dispatchable()],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    Ok(catalog
        .relationships
        .iter()
        .map(|rel| {
            let readers = crate::capture::columns::relationship_readers(&catalog, rel)
                .into_iter()
                .filter(|reader| unfrozen.contains(&reader.id))
                .map(|reader| (reader.id, reader.def.target.clone()))
                .collect();
            (rel.id, readers)
        })
        .collect())
}

/// The definitions that aren't frozen and read `table` (canonical) through a
/// relationship to it, each with its bare target, in id order (#799): whose
/// share of a to-side key's work isolation charges a failure in it to
/// (`quarantine::attribute`).
pub(super) async fn relationship_readers_of(
    pool: &Pool,
    table: &str,
) -> Result<Vec<(i64, String)>, ApplyError> {
    let rels = catalog::relationships_to_table(pool, table).await?;
    if rels.is_empty() {
        return Ok(Vec::new());
    }
    let mut by_rel = relationship_reader_map(pool).await?;
    let mut readers: BTreeMap<i64, String> = BTreeMap::new();
    for rel in rels {
        readers.extend(by_rel.remove(&rel.id).unwrap_or_default());
    }
    Ok(readers.into_iter().collect())
}

/// Phase 2 (design doc: "no transaction, no locks"): evaluates every
/// folded change's `f()` against the transform currently reading its
/// source table, grouped by (unqualified) `src_table` so each source's
/// catalog version is loaded — and fenced against — exactly once.
///
/// Reloads the catalog fresh on every call, including retries: this is
/// what makes [`drain_once`]'s retry-on-fence-miss loop "reload, recompute"
/// rather than needing any separate invalidation path.
///
/// Issue #56/ADR-0009 decision 3: this span is the "hop" half of the
/// source-commit → hop → hop → apply tree — one span per compute pass over
/// a folded batch, with a per-source-table [`tracing::debug!`] event inside
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

/// One key an isolation probe (`quarantine::attribute`, #799) applies with
/// every definition that reads its table directly left out, but `only`:
/// the probe that tells which definition's apply a key fails in. The key's
/// poison for the definitions reading it through a relationship stands as
/// it is, so with `only` `None` the probe applies just their share.
pub(super) struct ProbeFocus<'a> {
    /// Canonical, as `poison` keys it.
    pub src_table: &'a str,
    pub key: &'a str,
    pub only: Option<i64>,
}

impl ProbeFocus<'_> {
    /// Whether this focus leaves `key` of `src_table` out of `transform_id`.
    /// `None` for a key it doesn't name.
    fn excludes(&self, src_table: &str, key: &str, transform_id: i64) -> Option<bool> {
        (self.src_table == src_table && self.key == key).then_some(self.only != Some(transform_id))
    }
}

/// Which work a folded change's key is left out of (#799): the direct
/// readers it's poisoned for, and the relationships whose every reader it's
/// poisoned for.
#[derive(Default)]
struct KeyExclusion {
    /// Readers of the change's own table (`defs` ids) that skip it.
    defs: HashSet<i64>,
    /// Relationships to the change's table (ids) whose reverse work skips it.
    rels: HashSet<i64>,
    /// Every definition the change is parked for: each one above, and each
    /// relationship reader the key is poisoned for.
    park_for: BTreeSet<i64>,
}

impl KeyExclusion {
    /// Whether it leaves the change out of nothing.
    fn is_empty(&self) -> bool {
        self.defs.is_empty() && self.rels.is_empty() && self.park_for.is_empty()
    }
}

/// [`compute`], with `focus` overriding one key's direct readers for an
/// isolation probe, and with `skip_frozen` (issue #766) also skipping every
/// table no definition that isn't frozen reads, as [`source_key_for_apply`]
/// skips one whose key can't be used ([`no_unfrozen_reader`], fenced the
/// same way). [`drain_batch`] asks for it on the retry after Postgres
/// refused the drain a read or write (`42501`, see [`HaltRetry`]), and so do
/// that call's isolation probes: the halt has paused the definitions reading
/// the refused table, but the page still reads it for a relationship's
/// settled projection, which is kept current whatever its readers' status,
/// and would fail the same way forever. Asked only then, so a drain that was
/// refused nothing reads nothing more.
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
    // Issue #16: find the already-poisoned keys before anything else touches
    // them. Whole-key poison is per transform (#799): a poisoned key is left
    // out of the apply of each definition it's poisoned for, decided per
    // source below, and every other reader applies it. Truncate sentinels
    // are never candidates: a truncate is whole-keyspace, not a key
    // quarantine can attribute anything to.
    //
    // Issue #283: every quarantine table — `poison` included — is keyed on the
    // *canonical* (qualified, where resolvable) identity of a source table, not
    // the raw ring spelling, so each distinct `src_table` in this batch is
    // resolved once here and that canonical value is what this exclusion query,
    // `applied_keys` (`clear_key_deaths`) and `poisoned_park`
    // (`park_batch_contribution`) all use below. Matching raw would miss a key
    // already poisoned under the other spelling of its own table and
    // re-evaluate (then re-poison) it.
    //
    // Issue #380: `by_source` and the version fence (`versions`) key on the
    // same canonical identity, so truncates are resolved here too (their
    // fence entry needs it). Keying either on the bare table-name suffix
    // merged same-named tables in different schemas into one bucket and one
    // fence row.
    let mut canonical_srcs = quarantine::CanonicalSrcTables::default();
    for change in folded {
        if change.relationship_reverse_deferred.is_some() {
            continue;
        }
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
        .filter(|c| !c.is_truncate && c.relationship_reverse_deferred.is_none())
        .map(|c| (canonical_of(&c.src_table), c.key.clone()))
        .collect();
    let candidates: Vec<(&str, &str)> = canonical_candidates
        .iter()
        .map(|(t, k)| (t.as_str(), k.as_str()))
        .collect();
    let poisoned = quarantine::poisoned_keys_among(pool, &candidates).await?;
    tracing::Span::current().record("poisoned", poisoned.len());

    let mut by_source: HashMap<String, Vec<&FoldedChange>> = HashMap::new();
    // Truncate sentinels (issue #60) never enter the keyed by-source
    // evaluation loop below — they carry no key of their own (see
    // `append::TRUNCATE_SENTINEL_KEY`) and produce no write/delete;
    // they're handled separately, right after that loop.
    let mut truncated: Vec<&FoldedChange> = Vec::new();
    // Issue #134: deferred relationship reverses (`op = 'rel_reverse_deferred'`)
    // never enter the keyed by-source evaluation loop below either, and for a
    // sharper reason than truncate's "carries no key" — they carry the
    // parent's *real* key, on a *synthetic* `src_table`
    // (`relationship_reverse_deferred_src_table`), specifically so they
    // can't. Letting one reach `by_source` would run it through the
    // ordinary per-source forward-evaluation loop and double-apply the
    // delta its original CDC row already forward-applied when it first
    // landed — only the reverse-relationship delta was ever deferred, never
    // the parent's own forward apply. Reconstructed into a fresh
    // `RelationshipReverseRecord` in its own loop, right after `by_source`'s
    // — see that loop's comment.
    let mut relationship_reverse_deferrals: Vec<&FoldedChange> = Vec::new();
    let mut poisoned_park: Vec<(i64, FoldedChange)> = Vec::new();
    let mut applied_keys: Vec<(String, String)> = Vec::new();
    for change in folded {
        if change.is_truncate {
            truncated.push(change);
            continue;
        }
        if change.relationship_reverse_deferred.is_some() {
            relationship_reverse_deferrals.push(change);
            continue;
        }
        // Canonical, per this function's `poisoned` comment above (issue
        // #283): the exclusion match, the parked row's own `src_table`
        // (`poison_held` is keyed canonically, and issue #267 made the
        // qualified spelling the ring invariant a release would replay it
        // under anyway), and `applied_keys`, whose only consumer is
        // `clear_key_deaths` against the canonically-keyed `key_deaths`.
        by_source
            .entry(canonical_of(&change.src_table))
            .or_default()
            .push(change);
    }
    tracing::Span::current().record("sources", by_source.len());

    let mut versions: HashMap<String, Option<i64>> = HashMap::new();
    let mut targets: BTreeMap<String, TargetPlan> = BTreeMap::new();
    // #623 D3: aggregate targets on the ledger, in target order, so every
    // page takes their locks in one order.
    let mut ledger_targets: BTreeMap<String, super::ledger::LedgerTargetPlan> = BTreeMap::new();
    // Issue #52: every `Some(src_changed)` origin timestamp
    // `buffer_transform_apply_metrics` sees below, buffered per consuming
    // target — filtered into `ApplyPlan::end_to_end_origins` only for
    // targets the `downstream_readers` computation at the end of this
    // function finds terminal (see that call site's comment). Not itself
    // part of `ApplyPlan` — only the terminal-filtered subset is.
    let mut end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>> = HashMap::new();
    // Epic #49 cross-cutting review fix (issues #51/#52): every
    // observation `buffer_transform_apply_metrics` below
    // would previously have recorded immediately — now buffered here and
    // carried out on `ApplyPlan`, flushed post-commit by
    // [`flush_apply_metrics`]. See `buffer_transform_apply_metrics`'s doc
    // comment for why eager recording here was the bug.
    let mut transform_observations: Vec<TransformObservation> = Vec::new();
    // Issue #79: deduped across *every* relationship (and every source_key)
    // this whole `compute` call processes, not just within one relationship's
    // `key_hops` — two distinct inbound relationships sharing the same
    // `from_table` (e.g. `posts` and `comments` both pointing at `authors`)
    // otherwise each independently queue a full reverse-recompute pass over
    // every touched from-side key, doubling (or worse, with N relationships)
    // the backlog for no benefit: only one recompute per from-side row is
    // ever needed, at the highest hop_gen any contributing relationship
    // required. Keyed by `(from_table, from_key)`; drained into the
    // `Vec` shape `ApplyPlan` expects right before it's constructed below.
    // The `Option<SystemTime>` half is `src_changed` (issues #51/#52's
    // multi-hop gap), fan-in tie-broken by `earliest_src_changed` (min) —
    // deliberately the opposite merge direction from `hop_gen`'s `max`, see
    // that function's doc comment.
    let mut reverse_recomputes: HashMap<(String, String), (i32, Provenance)> = HashMap::new();
    // Issue #130, epic #127: accumulated across every source/definition this
    // `compute` pass evaluates a to-one relationship for — see
    // [`ApplyPlan::relationship_gen_bumps`]'s doc comment.
    let mut relationship_gen_bumps: HashMap<i64, RelationshipGenBump> = HashMap::new();
    // Issue #131, epic #127: one `ReverseRelationshipShape` per to-one
    // relationship this `compute()` call builds a reverse record for,
    // resolved once (a handful of catalog reads) and shared by every
    // touched parent key — see `RelationshipReverseRecord`'s doc comment.
    let mut relationship_reverse_shapes: HashMap<i64, Arc<ReverseRelationshipShape>> =
        HashMap::new();
    // Issue #131: every to-one relationship's parent-keyed reverse record
    // this batch's fold produced, applied by `apply_and_mark_drained_many`'s
    // "3d" step.
    let mut relationship_reverses: Vec<RelationshipReverseRecord> = Vec::new();
    // Issue #168: settled parent projections to clear in full at Phase 3 —
    // a `TRUNCATE` on a to-one relationship's to-side table. Unlike the
    // row-driven path (`relationship_reverses`, just above), a `TRUNCATE`
    // stages one key-less sentinel row, never a per-row image, so it can
    // never build a `RelationshipReverseRecord` (that needs an old/new row
    // to upsert or delete into the projection). Without this, the
    // projection silently keeps serving every to-side row's last-known
    // value forever after the physical table is emptied — see the
    // `truncated` loop below, where this is populated, for the full story.
    let mut relationship_projection_clears: std::collections::BTreeMap<
        i64,
        RelationshipProjectionClear,
    > = std::collections::BTreeMap::new();

    for (source_key, mut changes) in by_source {
        tracing::debug!(
            src_table = %source_key,
            changes = changes.len(),
            "evaluating a source table's folded changes"
        );
        let version = catalog::source_table_version(pool, &source_key).await?;
        // The first read of a table's fence is kept: a skipped table's may
        // have read this source's already, to fence the readers it judged
        // under (`source_key_for_apply`), and a bump since then must miss.
        versions.entry(source_key.clone()).or_insert(version);

        // The source identity this batch's own CDC producer staged (issue
        // #76, ADR-0007) — the ring's own spelling of `change.src_table`,
        // which `SourceTableDropped` below must echo back verbatim. Every
        // change in this bucket resolves to the same canonical `source_key`
        // by construction (`by_source` grouped on it, issue #380), so any one
        // of them names the right physical table for the reads below.
        let qualified_source = changes[0].src_table.as_str();

        if skip_frozen && no_unfrozen_reader(pool, &source_key, &mut versions).await? {
            tracing::warn!(
                src_table = %qualified_source,
                "every definition reading this table is frozen, and the drain was refused a \
                 read or write; skipping its changes"
            );
            continue;
        }

        // `source_key` alone determines the source table's primary key, not
        // the individual definition (issue #69) — introspected once per
        // source here and reused both below (every definition subscribed to
        // this source) and by the row decode below (every change, whichever
        // definition it's evaluated against). A dropped source is
        // [`ApplyError::SourceTableDropped`] (issue #16's purge, #267's
        // spelling); `None` is a key that can't be used on a table no
        // definition applies (issue #768), whose changes are dropped with
        // the page. See [`source_key_for_apply`].
        let Some(pk) =
            source_key_for_apply(pool, qualified_source, &source_key, &mut versions).await?
        else {
            continue;
        };

        // Issue #344: the source column list a 1-1 target's Phase 3 check
        // renders the current row with. Loaded at most once per source, and
        // only when some definition on it is 1-1.
        let mut source_row_columns: Option<Vec<String>> = None;

        // `qualified_source` (via `qualified_schema_node_key`):
        // `schema_nodes`/`schema_edges` key on qualified identity (issue #74,
        // ADR-0007), so `transforms_for_source` (a thin `dependents_of`
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

        // Which of the image decodes below this source's readers need.
        let needs_old_rows = defs.iter().any(|def| {
            def.def.key_space == KeySpace::OneToOne
                && !eval::relationship_references(&def.def).is_empty()
        });
        // Reverse recompute (issue #30): this source is some relationship's
        // *to-side*. A change to a related row must re-derive every from-side
        // row whose enrichment reads it. For each relationship pointing at
        // this table, collect the join-key text of every to-side row this
        // batch touched — the related row's `to_col`. A capture trigger
        // images `to_col` in every image, a delete's pre-image included,
        // since it is in the to-side's capture set (issue #622), whether it
        // is the to-side's key (a to-one) or not (a to-many). An image with
        // no `to_col` at all is `MissingColumn` below (issue #677).
        // Then resolve, with one live lookup, the from-side keys whose
        // `from_col` matches, and stage each as an image-less recompute at the
        // triggering change's `hop_gen + 1`.
        let inbound_rels = catalog::relationships_to_table(pool, &source_key).await?;
        // Issue #784: a `to_col` that is this table's row identity has the
        // ring key as its only value (`touched_join_values`).
        let key_col = ddl::sole_key_column(&pk);

        // #799: which work each change is left out of (see `KeyExclusion`).
        // A change poisoned for a reader of this table is left out of that
        // reader's apply only, and parked for it; one poisoned for every
        // reader of a relationship to this table is left out of that
        // relationship's reverse work. A change left out of everything is
        // dropped here, before its images are decoded, so a failure that is
        // really in the source (an image that won't decode) stops once every
        // reader holds the key. A clean commit clears the death counters of
        // every definition that applied the rest (`clear_key_deaths`).
        let exclusions = key_exclusions(
            pool,
            &source_key,
            &changes,
            &defs,
            &inbound_rels,
            &poisoned,
            focus,
        )
        .await?;
        // #785 review: a change a relationship's reverse work leaves out (its
        // key is poisoned for every reader of the relationship) has its
        // release re-staged as an image-less `Recompute`
        // (`quarantine::release_key`), whose reverse names only the first
        // parked pre-image and the live row. A non-key `to_col` value the fold
        // erased between them lives only in this change's `to_col_values`,
        // which nothing parks, and a child may have read the parent live under
        // it. So its children are re-derived now, as for a change the
        // relationship applies. A key `to_col`'s only value is the ring key,
        // which the release's `Recompute` names itself.
        for (change, exclusion) in changes.iter().zip(exclusions.iter().flatten()) {
            if exclusion.rels.is_empty() || change.to_col_values.is_empty() {
                continue;
            }
            for rel in inbound_rels
                .iter()
                .filter(|rel| exclusion.rels.contains(&rel.id))
            {
                let mut key_hops: HashMap<String, i32> = HashMap::new();
                let mut key_src_changed: HashMap<String, Provenance> = HashMap::new();
                for (column, value) in &change.to_col_values {
                    if *column == rel.def.to_col {
                        key_hops.insert(value.clone(), change.hop_gen);
                        key_src_changed
                            .insert(value.clone(), (change.src_changed, change.origin_lsn));
                    }
                }
                accumulate_from_side_recomputes(
                    pool,
                    rel,
                    &key_hops,
                    &key_src_changed,
                    &mut reverse_recomputes,
                    FromSideFence::new(&mut versions, skip_frozen),
                )
                .await?;
            }
        }
        let exclusions = match exclusions {
            None => {
                applied_keys.extend(changes.iter().map(|c| (source_key.clone(), c.key.clone())));
                None
            }
            Some(exclusions) => {
                let mut kept: Vec<&FoldedChange> = Vec::with_capacity(changes.len());
                let mut kept_exclusions: Vec<KeyExclusion> = Vec::with_capacity(changes.len());
                for (change, exclusion) in changes.into_iter().zip(exclusions) {
                    for &transform_id in &exclusion.park_for {
                        let mut parked = change.clone();
                        parked.src_table = source_key.clone();
                        poisoned_park.push((transform_id, parked));
                    }
                    let readers = defs.len() + inbound_rels.len();
                    if readers > 0
                        && exclusion.defs.len() == defs.len()
                        && exclusion.rels.len() == inbound_rels.len()
                    {
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

        // Decoded/re-read once per change here — not once per (definition,
        // change) — since every definition subscribed to this source
        // evaluates the exact same row (issue #69): the image a change
        // carries, or the live re-read for an image-less recompute trigger,
        // doesn't depend on which definition is reading it. The `(None,
        // None)` shape — a bare recompute trigger with no image, the shape
        // every backfill enumeration produces — is collected instead of
        // re-read immediately, so every such key in this batch is fetched
        // in one [`read_live_rows_batch`] round trip rather than one
        // round trip per key (issue #13).
        //
        // Every image this source's changes need is decoded in one
        // [`decode_images`] call (issue #327), not one round trip each:
        // - `rows`: each change's new image.
        // - `old_rows`: each change's old-side image ([`old_side_image`]),
        //   decoded only when something reads it. A 1-1 definition reading a
        //   to-one relationship needs the old join-key value, to bump `gen`
        //   for the parent a re-point/delete moved *away* from (issue #130,
        //   see `RelationshipGenBump`'s doc comment); and each relationship
        //   pointing at this table as its to-side reads the join key off it
        //   for a delete/re-parent. The overwhelmingly common 1-1-only,
        //   relationship-free source decodes none.
        // #623 D3: a definition on the ledger reads its images and its
        // Re-derives in Phase 3 (`super::ledger`), so a source read only by
        // such definitions, and by no relationship, decodes and re-reads
        // nothing here.
        let mut ledger_shapes: Vec<Option<super::ledger::LedgerShape>> =
            Vec::with_capacity(defs.len());
        for def in &defs {
            ledger_shapes.push(super::ledger::route_definition(pool, def).await?);
        }
        let ledger_only = inbound_rels.is_empty() && ledger_shapes.iter().all(Option::is_some);
        // #623 D6: so does a relationship-free 1-1 definition's Re-derive,
        // so a source read only by those re-reads nothing here either.
        let rederives_in_phase3 = inbound_rels.is_empty()
            && defs.iter().zip(&ledger_shapes).all(|(def, shape)| {
                shape.is_some()
                    || (def.def.key_space == KeySpace::OneToOne
                        && eval::relationship_references(&def.def).is_empty())
            });
        let decode_old_side = needs_old_rows || !inbound_rels.is_empty();
        let mut batch = ImageBatch::default();
        let mut slots: Vec<(Option<usize>, Option<usize>)> = Vec::with_capacity(changes.len());
        let mut live_refetch_indices: Vec<usize> = Vec::new();
        for (i, change) in changes.iter().enumerate() {
            if ledger_only {
                slots.push((None, None));
                continue;
            }
            if change.new_image.is_none() && change.old_image.is_none() && !rederives_in_phase3 {
                live_refetch_indices.push(i);
            }
            let new_slot = batch.push_opt(change.new_image.as_ref());
            let old_slot = if decode_old_side {
                batch.push_opt(old_side_image(change))
            } else {
                None
            };
            slots.push((new_slot, old_slot));
        }
        let mut decoded = batch.decode(pool).await?;
        let mut rows: Vec<Option<Row>> = Vec::with_capacity(changes.len());
        let mut old_rows: Vec<Option<Row>> = Vec::with_capacity(changes.len());
        for (new_slot, old_slot) in slots {
            rows.push(decoded.take_opt(new_slot));
            old_rows.push(decoded.take_opt(old_slot));
        }
        if !live_refetch_indices.is_empty() {
            let live_keys: Vec<&str> = live_refetch_indices
                .iter()
                .map(|&i| changes[i].key.as_str())
                .collect();
            let mut live_rows =
                read_live_rows_batch(pool, qualified_source, &pk, &live_keys).await?;
            for &i in &live_refetch_indices {
                rows[i] = live_rows.remove(changes[i].key.as_str());
            }
        }

        for rel in &inbound_rels {
            // Issue #131, epic #127: to-one relationships get the new
            // parent-keyed reverse record + delta mechanism below;
            // to-many relationships keep exactly this pre-#131 image-less
            // recompute path, unchanged — Phase 1 of this epic is to-one
            // relationship *values* only (see the plan doc §7).
            if rel.cardinality == RelationshipCardinality::ToMany {
                // Join-key text -> the max `hop_gen` of the to-side changes
                // that touched it (a re-parent update touches both its old
                // and new key; a delete carries only its pre-image), and
                // (issues #51/#52's multi-hop gap) the *earliest* (`min`)
                // `src_changed` among those same changes — see
                // `earliest_src_changed`'s doc comment for why the two use
                // opposite merge directions.
                let mut key_hops: HashMap<String, i32> = HashMap::new();
                let mut key_src_changed: HashMap<String, Provenance> = HashMap::new();
                for (i, change) in changes.iter().enumerate() {
                    if skips_relationship(&exclusions, i, rel.id) {
                        continue;
                    }
                    let mut note = |value: &Option<String>, hop: i32| {
                        if let Some(text) = value {
                            key_hops
                                .entry(text.clone())
                                .and_modify(|h| *h = (*h).max(hop))
                                .or_insert(hop);
                            key_src_changed
                                .entry(text.clone())
                                .and_modify(|(sc, origin)| {
                                    *sc = earliest_src_changed(*sc, change.src_changed);
                                    *origin = earliest_origin(*origin, change.origin_lsn);
                                })
                                .or_insert((change.src_changed, change.origin_lsn));
                        }
                    };
                    // Issue #677: a present image missing `to_col` is
                    // `MissingColumn`, not "no key" — see
                    // `relationship_key_text`.
                    for row in [&rows[i], &old_rows[i]] {
                        let key = relationship_key_text(row, &rel.def.to_col, &rel.def.name)?;
                        note(&key, change.hop_gen);
                    }
                    // Issue #784: every key the fold erased from both
                    // images.
                    for key in touched_join_values(change, &rel.def.to_col, key_col) {
                        note(&Some(key.clone()), change.hop_gen);
                    }
                }
                accumulate_from_side_recomputes(
                    pool,
                    rel,
                    &key_hops,
                    &key_src_changed,
                    &mut reverse_recomputes,
                    FromSideFence::new(&mut versions, skip_frozen),
                )
                .await?;
                continue;
            }

            // Issue #244: an image-less change — a bare
            // `StagedChange::Recompute` trigger — is *not* a parent state
            // transition, and must never become a reverse **delta**.
            //
            // Such a trigger asserts only "this key exists as of now" (see
            // `intake::markers::enumerate_and_append`'s doc comment); it
            // carries no images at all, and it is staged in quantity by
            // paths that have nothing to do with the parent changing: a
            // definition's ring backfill enumeration of its own source
            // table, forward propagation's chained-target hop, and every
            // reverse/TRUNCATE-clear fallback (whose from-side table is very
            // often *also* some other relationship's to-side).
            //
            // A reverse record can't represent that. It reads `rows[i]`,
            // which for an image-less trigger is `compute`'s own *live
            // re-read* of the row — so the record it would build is
            // `old_row = None`, `new_row = Some(live row)`, byte-for-byte
            // the shape of a genuine parent INSERT, and the delta fast path
            // (deleted in #623 D5) would dutifully add the parent's
            // contribution to every matching from-side row's group a
            // second time, on top of the
            // contribution already folded in when the parent was really
            // inserted. That is a silent 2x `SUM` (issue #244's generative
            // repro: `t4[1].rel_agg` 59 -> 118).
            //
            // The right treatment is the one the to-many branch just above
            // already gives every trigger it sees, image-less included: an
            // image-less `Recompute` of the matching from-side rows, which
            // re-derives each affected group from live state and is
            // therefore idempotent no matter how many times it runs — the
            // same always-correct fallback `needs_recompute_fallback` and
            // the guard-rejection paths route to. It costs a hop generation
            // and a live pass instead of a delta, which is exactly the
            // trade the fallback exists to make, and it keeps a *genuine*
            // propagation (a chained to-side target this apply just
            // rewrote, staged as an image-less hop) reaching its dependents
            // rather than being dropped.
            //
            // Issue #392: a `recompute` that folded with the parent's own CDC
            // change leaves a record with images, which becomes a delta
            // record below as usual (it also advances the projection). The
            // recompute still asked for the from-side rows to be re-derived,
            // so they are, from both of the record's images.
            //
            // Issue #784: an aggregate's ledger write reads the parent live
            // (#623 D5), so a child may have read the parent under a join
            // key the fold erased from both of this record's images: a
            // parent born and deleted inside the batch (no image on either
            // side), or one re-keyed through a value and on. Those keys come
            // from the ring key or the raw new images (`touched_join_values`), and
            // their from-side rows are re-derived here like an image-less
            // record's, since no reverse record names them.
            let mut key_hops: HashMap<String, i32> = HashMap::new();
            let mut key_src_changed: HashMap<String, Provenance> = HashMap::new();
            let mut note_key = |key: String, change: &FoldedChange| {
                key_hops
                    .entry(key.clone())
                    .and_modify(|h| *h = (*h).max(change.hop_gen))
                    .or_insert(change.hop_gen);
                key_src_changed
                    .entry(key)
                    .and_modify(|(sc, origin)| {
                        *sc = earliest_src_changed(*sc, change.src_changed);
                        *origin = earliest_origin(*origin, change.origin_lsn);
                    })
                    .or_insert((change.src_changed, change.origin_lsn));
            };
            for (i, change) in changes.iter().enumerate() {
                if skips_relationship(&exclusions, i, rel.id) {
                    continue;
                }
                let image_less = change.old_image.is_none() && change.new_image.is_none();
                if image_less || change.has_recompute {
                    // An image-less change carries no image of its own:
                    // its new side is the live re-read, and its old side
                    // (`old_side_image`) is its prior-image hint, the row as
                    // readers last saw it, as the to-many branch above reads
                    // it. A re-read that came back empty (the key no longer
                    // exists) names no key, so only the hint reaches the
                    // from-side rows of a parent that went away; issue #754:
                    // a released parked delete is such a recompute.
                    for row in [rows[i].as_ref(), old_rows[i].as_ref()]
                        .into_iter()
                        .flatten()
                    {
                        // Issue #677: absent `to_col` is `MissingColumn`,
                        // only a `NULL` one is "no key".
                        if let Some(join_text) =
                            required_column(row, &rel.def.to_col, &rel.def.name)?
                        {
                            note_key(join_text.clone(), change);
                        }
                    }
                    for key in touched_join_values(change, &rel.def.to_col, key_col) {
                        note_key(key.clone(), change);
                    }
                } else {
                    // The reverse record below re-derives the from-side rows
                    // of its two images' keys; only the keys between them
                    // are left.
                    let old_key =
                        relationship_key_text(&old_rows[i], &rel.def.to_col, &rel.def.name)?;
                    let new_key = relationship_key_text(&rows[i], &rel.def.to_col, &rel.def.name)?;
                    for key in touched_join_values(change, &rel.def.to_col, key_col) {
                        if Some(key) != old_key.as_ref() && Some(key) != new_key.as_ref() {
                            note_key(key.clone(), change);
                        }
                    }
                }
            }
            accumulate_from_side_recomputes(
                pool,
                rel,
                &key_hops,
                &key_src_changed,
                &mut reverse_recomputes,
                FromSideFence::new(&mut versions, skip_frozen),
            )
            .await?;

            // Issue #131: to-one. One `ReverseRelationshipShape` per
            // relationship per `compute()` call, shared (via `Arc`) by every
            // parent key this batch's fold touches for it.
            let shape = match relationship_reverse_shapes.get(&rel.id) {
                Some(shape) => Arc::clone(shape),
                None => {
                    let shape = Arc::new(
                        build_reverse_relationship_shape(
                            pool,
                            rel,
                            FromSideFence::new(&mut versions, skip_frozen),
                        )
                        .await?,
                    );
                    relationship_reverse_shapes.insert(rel.id, Arc::clone(&shape));
                    shape
                }
            };
            for (i, change) in changes.iter().enumerate() {
                if skips_relationship(&exclusions, i, rel.id) {
                    continue;
                }
                // Issue #244: an image-less change (both of the *change's
                // own* images absent — a bare recompute trigger) is never a
                // parent state transition, so it never becomes a delta
                // record; the block just above has already routed it to the
                // idempotent from-side recompute instead. Tested on
                // `change.old_image`/`change.new_image`, **not** on the
                // decoded `old_row`/`new_row` pair this used to test: an
                // image-less trigger's `rows[i]` is `compute`'s own live
                // re-read, so the old condition only ever fired for a key
                // whose live re-read also came back empty — the delta path
                // saw every still-existing row as a parent INSERT and
                // double-counted its contribution (see the long comment on
                // the image-less block above).
                if change.old_image.is_none() && change.new_image.is_none() {
                    continue;
                }
                let old_row = old_rows[i].clone();
                let new_row = rows[i].clone();
                let read_key =
                    relationship_read_key(&old_row, &new_row, &rel.def.to_col, &rel.def.name)?;
                let capture = capture_reverse_guard_state(
                    pool,
                    &shape.qualified_projection,
                    &shape.to_col,
                    read_key.as_deref(),
                )
                .await?;
                relationship_reverses.push(RelationshipReverseRecord {
                    shape: Arc::clone(&shape),
                    old_row,
                    new_row,
                    old_image: change.old_image.clone(),
                    new_image: change.new_image.clone(),
                    lsn: change.lsn,
                    prev_lsn: capture.prev_lsn,
                    prev_gen: capture.prev_gen,
                    watermark: capture.watermark,
                    hop_gen: change.hop_gen,
                    src_changed: change.src_changed,
                    origin_lsn: change.origin_lsn,
                    retry_count: 0,
                });
            }
        }

        for (def, ledger_shape) in defs.iter().zip(ledger_shapes) {
            // #799: this definition's share, without the changes whose key
            // is poisoned for it (parked above).
            let (changes, rows, old_rows) =
                definition_share(def.id, &changes, &rows, &old_rows, &exclusions);
            if changes.is_empty() {
                continue;
            }
            // #623 D3: a plain aggregate on the ledger takes the records as
            // they are; Phase 3 evaluates them (`super::ledger`).
            if let Some(shape) = ledger_shape {
                let target_plan = match ledger_targets.entry(def.def.target.clone()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let identity = {
                            let client = pool.get().await?;
                            ddl::identity_key_columns(&**client, &def.target_table).await?
                        };
                        entry.insert(super::ledger::LedgerTargetPlan::new(
                            &def.target_table,
                            qualified_source,
                            pk.clone(),
                            identity,
                            shape,
                        ))
                    }
                };
                for change in changes.iter() {
                    target_plan.push(change);
                    buffer_transform_apply_metrics(
                        &def.def.target,
                        change,
                        &mut end_to_end_origins,
                        &mut transform_observations,
                    );
                }
                continue;
            }
            // #623 D5: `ledger::route` takes every valid aggregate.
            if matches!(def.def.key_space, KeySpace::Aggregate { .. }) {
                return Err(ApplyError::AggregateOffLedger {
                    target: def.def.target.clone(),
                });
            }
            let field_names: Vec<String> = def.def.fields.iter().map(|f| f.name.clone()).collect();
            let field_types = one_to_one_field_types(
                pool,
                &def.def,
                &def.source_columns,
                &def.target_table,
                &field_names,
            )
            .await?;

            // ADR-0003's amendment (column-level quarantine): a column
            // the fuse has paused is excluded from both this plan's
            // column list (so `apply_target`'s generated SQL never
            // mentions it at all — decision: freeze at the last
            // successfully computed value, don't null it out or keep
            // reattempting a formula that's already fused off) and from
            // evaluation itself below (so a still-broken paused formula
            // doesn't keep reproducing the same failure on every batch).
            // Empty for every definition with nothing currently paused —
            // the overwhelmingly common case — so this is a cheap,
            // indexed no-op read then, behavior-identical to before this
            // amendment.
            let paused = quarantine::paused_columns_for(pool, &def.def).await?;
            let (field_names, field_types): (Vec<String>, Vec<ValueType>) = if paused.is_empty() {
                (field_names, field_types)
            } else {
                field_names
                    .into_iter()
                    .zip(field_types)
                    .filter(|(name, _)| !paused.contains(name))
                    .unzip()
            };

            // Issue #121/#126: `pk` is this source's full (possibly
            // composite) primary key — a `KeySpace::OneToOne` target's own primary
            // key now mirrors the source's in full, at whatever arity it
            // has, rather than narrowing to one column.
            let target_pk = pk.clone();
            let columns = match &source_row_columns {
                Some(columns) => columns.clone(),
                None => {
                    let client = pool.get().await?;
                    let columns = live_row_columns(&**client, qualified_source).await?;
                    source_row_columns = Some(columns.clone());
                    columns
                }
            };

            // Reused across every change below (issue #68): `regexp_count`'s
            // pattern is a validated string literal, so its compiled `Regex`
            // is the same for every row this definition evaluates, and
            // recompiling it per row would be wasted work at realistic row
            // volumes.
            let mut regex_cache = eval::RegexCache::new();

            // Relationship enrichment (issues #28/#29 eval, wired here by
            // #30): a target reading a `<rel>.<column>` path (to-one) or an
            // aggregate over one (to-many) needs the related to-side rows
            // built into a `RelationshipContext`. Built once per definition
            // over this source's from-side rows — the join keys are their
            // `from_col` values — then threaded into every row eval below
            // and into Phase 3's Re-derives (#623 D6). A definition with no
            // relationship references stays on the plain `eval::evaluate`
            // path.
            let relationships = if eval::relationship_references(&def.def).is_empty() {
                None
            } else {
                let (ctx, gen_bumps) = build_relationship_context(
                    pool,
                    &def.source_table,
                    &def.def,
                    &rows,
                    Some(&old_rows),
                    Some(&changes[..]),
                )
                .await?;
                // Issue #130: merge this definition's touched-parent keys
                // into the whole-batch accumulator — several definitions
                // (or several sources, across loop iterations) can share
                // one relationship, and every one of them needs to land
                // in the same Phase 3 bump.
                for (rel_id, bump) in gen_bumps {
                    relationship_gen_bumps
                        .entry(rel_id)
                        .and_modify(|existing| {
                            existing
                                .touched_keys
                                .extend(bump.touched_keys.iter().cloned());
                        })
                        .or_insert(bump);
                }
                Some(join_coverage(
                    ctx,
                    rows.iter().chain(old_rows.iter()).flatten(),
                ))
            };
            let rederive = Arc::new(Rederive {
                def: def.def.clone(),
                source_columns: def.source_columns.clone(),
                paused: paused.clone(),
                skip_failing: false,
                relationships,
            });
            let plan = targets
                .entry(def.def.target.clone())
                .or_insert_with(|| TargetPlan {
                    pk: target_pk,
                    field_names: field_names.clone(),
                    field_types: field_types.clone(),
                    records: Vec::new(),
                    // The persisted, fully-qualified identity (issue #73)
                    // — not re-derived, since `def` (this source's own
                    // catalog `Definition`) already carries it. See
                    // `TargetPlan::qualified_target`'s doc comment.
                    qualified_target: def.target_table.clone(),
                    qualified_source: qualified_source.to_string(),
                    row_columns: columns,
                    rederive: Arc::clone(&rederive),
                });

            // #623 D6: an Apply evaluates its change's new image here —
            // a delete when it has none. A Re-derive (a `recompute`, or a
            // change folded with one) is evaluated in Phase 3, from the
            // row it reads under the entry lock.
            for (change, row) in changes.iter().zip(rows.iter()) {
                let apply = match &change.last_change {
                    Some(last) if !change.has_recompute => {
                        let values = match (&change.new_image, row) {
                            (Some(_), Some(row)) => {
                                let mut evaluated =
                                    evaluate_one_to_one(&rederive, row, &mut regex_cache)?;
                                Some(evaluated_values(&field_names, &mut evaluated))
                            }
                            _ => None,
                        };
                        Some((last.lsn, last.row_txid.clone(), values))
                    }
                    _ => None,
                };
                plan.records.push(OneToOneRecord {
                    pk_text: change.key.clone(),
                    apply,
                    hop_gen: change.hop_gen,
                    src_changed: change.src_changed,
                    origin_lsn: change.origin_lsn,
                    src_table: change.src_table.clone(),
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

    // Issue #134: reconstruct a fresh `RelationshipReverseRecord` for every
    // deferred reverse this batch's fold produced (see
    // `relationship_reverse_deferrals`'s own comment above for why these
    // never entered the by-source loop above). Guard state
    // (`prev_lsn`/`prev_gen`/`watermark`) is re-derived live here, via the
    // exact same `capture_reverse_guard_state` call the fresh-from-CDC path
    // above uses — never replayed from anything persisted on the ring row
    // (this op's own migration deliberately carries none of the three) —
    // see that migration's doc comment for why: a stale replay could
    // wrongly pass a guard that should now fail, or wrongly fail one that
    // would now legitimately pass, silently reintroducing exactly the class
    // of bug issues #132/#133 closed. `relationship_reverse_shapes` is the
    // same cache the by-source loop above populates, so a relationship
    // touched by both a genuine parent CDC row and a deferred retry in the
    // same batch only ever builds its shape once.
    // Every deferred reverse's images, decoded in one batch (issue #327).
    let mut deferred_images = ImageBatch::default();
    let deferred_slots: Vec<(Option<usize>, Option<usize>)> = relationship_reverse_deferrals
        .iter()
        .map(|change| {
            (
                deferred_images.push_opt(change.old_image.as_ref()),
                deferred_images.push_opt(change.new_image.as_ref()),
            )
        })
        .collect();
    let mut deferred_decoded = deferred_images.decode(pool).await?;
    for (change, (old_slot, new_slot)) in relationship_reverse_deferrals.iter().zip(deferred_slots)
    {
        let Some(rel_id) = change.relationship_reverse_deferred else {
            unreachable!("filtered on relationship_reverse_deferred.is_some() above")
        };
        let shape = match relationship_reverse_shapes.get(&rel_id) {
            Some(shape) => Arc::clone(shape),
            None => {
                let Some(rel) = catalog::relationship_by_id(pool, rel_id).await? else {
                    // The relationship was dropped between the original
                    // deferral and this retry — nothing left to retry
                    // against. Drop the deferred row rather than erroring
                    // the whole batch: a dropped relationship's own
                    // catalog-side cleanup is responsible for anything else
                    // that implies, not this drain.
                    tracing::warn!(
                        relationship_id = rel_id,
                        "a deferred relationship reverse's relationship no longer exists; \
                         dropping the retry"
                    );
                    continue;
                };
                let shape = Arc::new(
                    build_reverse_relationship_shape(
                        pool,
                        &rel,
                        FromSideFence::new(&mut versions, skip_frozen),
                    )
                    .await?,
                );
                relationship_reverse_shapes.insert(rel_id, Arc::clone(&shape));
                shape
            }
        };
        let old_row = deferred_decoded.take_opt(old_slot);
        let new_row = deferred_decoded.take_opt(new_slot);
        // Issue #784: the join keys the folded retries named that neither
        // folded image does. A child may have read the parent live under
        // one of them (an aggregate's ledger write does, #623 D5), and no
        // reverse record is left to re-derive it, so its children are
        // re-derived here, as `compute`'s by-source loop does for the
        // parent's own rows (`touched_join_values`). A deferred row's
        // `group_key` holds only this relationship's keys, since its
        // `src_table` is the relationship's own.
        if shape.needs_recompute_fallback {
            let old_key = relationship_key_text(&old_row, &shape.to_col, &shape.name)?;
            let new_key = relationship_key_text(&new_row, &shape.to_col, &shape.name)?;
            let mut key_hops: HashMap<String, i32> = HashMap::new();
            let mut key_src_changed: HashMap<String, Provenance> = HashMap::new();
            for key in change.group_key.iter().flatten() {
                if Some(key) != old_key.as_ref() && Some(key) != new_key.as_ref() {
                    key_hops.insert(key.clone(), 0);
                    key_src_changed.insert(key.clone(), (change.src_changed, change.origin_lsn));
                }
            }
            accumulate_from_side_recomputes_on(
                pool,
                &shape.from_table,
                &shape.from_col,
                &key_hops,
                &key_src_changed,
                &mut reverse_recomputes,
                FromSideFence::new(&mut versions, skip_frozen),
            )
            .await?;
        }
        if old_row.is_none() && new_row.is_none() {
            // The retries folded to no image on either side: the parent was
            // born and deleted across them (#784), and its children were
            // re-derived above. A single deferred row always has an image
            // (`check_reverse_guards` passes a record with neither key), so
            // nothing else folds to this.
            continue;
        }
        let read_key = relationship_read_key(&old_row, &new_row, &shape.to_col, &shape.name)?;
        let capture = capture_reverse_guard_state(
            pool,
            &shape.qualified_projection,
            &shape.to_col,
            read_key.as_deref(),
        )
        .await?;
        relationship_reverses.push(RelationshipReverseRecord {
            shape: Arc::clone(&shape),
            old_row,
            new_row,
            old_image: change.old_image.clone(),
            new_image: change.new_image.clone(),
            lsn: change.lsn,
            prev_lsn: capture.prev_lsn,
            prev_gen: capture.prev_gen,
            watermark: capture.watermark,
            // Deliberately 0, not `change.hop_gen` (this op's ring row
            // never meaningfully carries one — always staged as 0, see
            // `append::ChangeRow`'s `RelationshipReverseDeferred` arm):
            // retrying is not propagation, and must never contribute
            // toward the hop bound.
            hop_gen: 0,
            src_changed: change.src_changed,
            origin_lsn: change.origin_lsn,
            retry_count: change.retry_count,
        });
    }

    if !poisoned_park.is_empty() {
        tracing::warn!(
            parked = poisoned_park.len(),
            "batch leaves already-poisoned keys out of the definitions they're poisoned for, \
             parking this batch's own contribution for each"
        );
    }

    // Truncate clears (issue #60): for each truncated src_table, resolve its
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
        let Some(pk) =
            source_key_for_apply(pool, &change.src_table, &source_key, &mut versions).await?
        else {
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
            // The floor the clear raises (#774, see the error's doc).
            let truncate_lsn = change.lsn.ok_or_else(|| ApplyError::TruncateWithoutLsn {
                src_table: change.src_table.clone(),
            })?;
            match &def.def.key_space {
                KeySpace::Aggregate { .. } => {
                    if let Some(existing) = aggregate_clears.get_mut(&def.def.target) {
                        existing.hop_gen = existing.hop_gen.max(change.hop_gen);
                        existing.src_changed =
                            earliest_src_changed(existing.src_changed, change.src_changed);
                        existing.origin_lsn =
                            earliest_origin(existing.origin_lsn, change.origin_lsn);
                        existing.truncate_lsn = existing.truncate_lsn.max(truncate_lsn);
                    } else {
                        // Issue #385: the ungated `identity_key_columns`, not
                        // `source_primary_key`. The clear only renders this
                        // identity through `pk_key_sql_expr` (a `::text` of
                        // each column) to report cleared groups to the seam,
                        // so the key-type gate buys nothing here — and a
                        // `numeric` `GROUP BY` is still admitted, so the gate
                        // halted the instance on every such truncate. A
                        // chained reader can't depend on this key's text
                        // being stable: #371 refuses to chain off an
                        // aggregate whose identity fails the gate.
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
                        aggregate_clears.insert(
                            def.def.target.clone(),
                            AggregateClearPlan {
                                hop_gen: change.hop_gen,
                                qualified_target: def.target_table.clone(),
                                pk,
                                src_changed: change.src_changed,
                                origin_lsn: change.origin_lsn,
                                on_ledger: super::ledger::route_definition(pool, def)
                                    .await?
                                    .is_some(),
                                truncate_lsn,
                            },
                        );
                    }
                }
                KeySpace::OneToOne => {
                    // Issue #121/#126: same full, un-narrowed primary key as
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
            // by_source loop above (issue #51/ADR-0009 decision 5).
            buffer_transform_apply_metrics(
                &def.def.target,
                change,
                &mut end_to_end_origins,
                &mut transform_observations,
            );
        }

        // Issue #98: a TRUNCATE clears definitions reading this table
        // directly (above), but definitions that read it only *through* a
        // relationship — this table is some relationship's to-side — need
        // clearing too, and the "truncate clears" mechanism above only
        // resolves direct source readers via `transforms_for_source`. Reuse
        // the reverse-recompute mechanism (issue #30) that the row-driven
        // `by_source` loop above feeds for exactly this situation, staging
        // every from-side row currently pointing at this (now-empty) table
        // as a recompute — see `ReverseTrigger::WholeKeyspace`'s doc comment
        // for why "every non-NULL join column", not a specific value list,
        // is the right query for a TRUNCATE. Pushed into the same
        // `reverse_recomputes` accumulator the row-driven path uses, so it's
        // deduped the same way (issue #79) and drained through the same
        // `Recompute` pipeline below — no separate emission path needed.
        // Image-less, unless an aggregate groups by the relationship (issue
        // #520, below).
        let inbound_rels = catalog::relationships_to_table(pool, &source_key).await?;
        for rel in &inbound_rels {
            // Issue #168: for a to-one relationship, the staged recompute
            // above only re-derives the from-side row's enrichment — it
            // says nothing about *what value* that recompute will read. For
            // an ordinary row-driven change, that value comes from
            // `catalog::relationship_projection`'s settled parent
            // projection (`build_relationship_context`'s doc comment),
            // which the row-driven `by_source` loop keeps in sync via
            // `RelationshipReverseRecord`/`apply_projection_advance` (a
            // delete-then-upsert keyed off each change's own old/new
            // image). A `TRUNCATE` never reaches that loop at all (its one
            // key-less sentinel carries no image to upsert or delete with),
            // so without this, the projection would keep serving every
            // to-side row's pre-truncate value forever — the from-side
            // recompute would re-derive against stale data, not against
            // the now-empty table. Cleared in full at Phase 3
            // (`ApplyPlan::relationship_projection_clears`), same "whole
            // table, not a key list" shape as the truncate-clear on a
            // direct target above, since every row this projection held
            // for this relationship just vanished with the truncate.
            // To-many relationships have no projection at all (`ToMany`
            // still resolves via a live `LEFT JOIN` every time —
            // `build_relationship_context`'s own doc comment), so nothing
            // to clear there.
            if rel.cardinality == RelationshipCardinality::ToOne
                && let Some(projection) = catalog::relationship_projection(pool, rel.id).await?
            {
                relationship_projection_clears
                    .entry(rel.id)
                    .and_modify(|clear| clear.lsn = clear.lsn.max(change.lsn))
                    .or_insert(RelationshipProjectionClear {
                        qualified_projection: projection.qualified_table(),
                        lsn: change.lsn,
                    });
            }
            // Issue #267: canonicalized to qualified identity for the same
            // reason [`accumulate_from_side_recomputes`] does it — this shares
            // that function's accumulator, and its entries are staged as
            // `src_table` verbatim.
            let qualified_from_table = rel.qualified_from_table();
            let Some(from_pk) = from_side_key(
                pool,
                &qualified_from_table,
                FromSideFence::new(&mut versions, skip_frozen),
            )
            .await?
            else {
                continue;
            };
            // #623 D5: an aggregate on the from-table keeps each row's group
            // on its ledger entry, so the recompute needs no prior image.
            let from_keys = from_side_keys(
                pool,
                &qualified_from_table,
                &from_pk,
                &rel.def.from_col,
                &ReverseTrigger::WholeKeyspace,
            )
            .await?;
            let hop = change.hop_gen + 1;
            for (from_key, _) in from_keys {
                reverse_recomputes
                    .entry((qualified_from_table.clone(), from_key))
                    .and_modify(|(h, (sc, origin))| {
                        *h = (*h).max(hop);
                        *sc = earliest_src_changed(*sc, change.src_changed);
                        *origin = earliest_origin(*origin, change.origin_lsn);
                    })
                    .or_insert((hop, (change.src_changed, change.origin_lsn)));
            }
        }
    }

    let mut downstream_readers = HashMap::new();
    let mut all_targets: std::collections::HashSet<&String> = targets.keys().collect();
    all_targets.extend(clears.keys());
    all_targets.extend(ledger_targets.keys());
    all_targets.extend(aggregate_clears.keys());
    // Epic #49 cross-cutting review fix (issues #51/#52): only the
    // terminal-filtered subset of `end_to_end_origins` survives into
    // `ApplyPlan` — flushed post-commit by `flush_apply_metrics`, not
    // recorded here.
    let mut terminal_end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>> =
        HashMap::new();
    for target in all_targets {
        // `target` is bare (`def.def.target`) — `schema_nodes` now keys on
        // qualified identity (issue #74, ADR-0007), so a bare lookup here
        // would silently find nothing and permanently disable downstream
        // propagation for every chained transform.
        // `qualified_schema_node_key` resolves it the same way it resolves
        // a bare `Recompute`-staged `src_table` above (this *is* exactly
        // that case, one step earlier: `target` is about to become such a
        // row's `src_table` the moment this loop's caller stages it).
        //
        // Issue #267: that resolution is now *kept*, not discarded once the
        // reader question is answered, and Phase 3 stages it verbatim as the
        // propagated row's `src_table`. Reusing the identity this lookup
        // already computed is what makes the emitted spelling agree, by
        // construction, with the one a capture trigger independently stages
        // for the same physical table (its spec's `markers::qualify`
        // spelling) — `resolve_graph_identity`'s first step is a
        // `search_path` lookup of the real relation, which a target table
        // satisfies, and both sides then format the schema it finds through
        // that same `markers::qualify`, so the two strings are equal by
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
        // Issue #52/ADR-0009 decision 2: end-to-end latency is only ever
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

    let reverse_recomputes: Vec<DerivedRecompute> = reverse_recomputes
        .into_iter()
        .map(
            |((from_table, from_key), (hop, (src_changed, origin_lsn)))| {
                (from_table, from_key, hop, src_changed, origin_lsn)
            },
        )
        .collect();

    Ok(ApplyPlan {
        versions,
        targets,
        ledger_targets,
        clears,
        aggregate_clears,
        poisoned_park,
        applied_keys,
        reverse_recomputes,
        transform_observations,
        end_to_end_origins: terminal_end_to_end_origins,
        relationship_gen_bumps,
        relationship_reverses,
        relationship_projection_clears,
    })
}

/// Epic #49 cross-cutting review fix (issues #51/#52): flushes `plan`'s
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

/// Issue #134/#135 review follow-up: flushes [`ApplyOutcome::deferral_counts`]/
/// [`ManyApplyOutcome::deferral_counts`] into
/// [`crate::metrics::increment_relationship_reverse_deferred`] — the
/// deferral-metrics counterpart to [`flush_apply_metrics`], with the exact
/// same "only after this attempt's transaction has actually committed"
/// contract and for the same reason: `drain_once`/`drain_many`'s retry loop
/// can call `apply_and_mark_drained`/`apply_and_mark_drained_many` more than
/// once for the same folded input on a `VersionFenceMiss` or a transient
/// Phase-3 failure (doc 06's `FenceMissBackoff`, an *ordinary*, routine
/// occurrence, not a rare edge case) — recording eagerly, from inside that
/// function itself, would inflate a guard's count once per losing attempt,
/// exactly the kind of skew #135's fairness/starvation decisions can least
/// afford under the high-contention conditions where they matter most.
/// Not folded into `flush_apply_metrics` itself (which takes `&ApplyPlan`,
/// a Phase-2-only artifact): these counts are discovered live during Phase
/// 3, so they ride on the apply outcome instead.
fn flush_relationship_reverse_deferral_metrics(deferral_counts: &HashMap<&'static str, u64>) {
    for (guard, count) in deferral_counts {
        for _ in 0..*count {
            crate::metrics::increment_relationship_reverse_deferred(guard);
        }
    }
}

/// Issue #135: the fairness-escalation counterpart to
/// [`flush_relationship_reverse_deferral_metrics`] — same post-commit-only
/// contract and the same reason (see that function's doc comment).
fn flush_relationship_reverse_fairness_escalation_metric(count: u64) {
    for _ in 0..count {
        crate::metrics::increment_relationship_reverse_fairness_escalated();
    }
}

// ---------------------------------------------------------------------
// Phase 3: apply ∪ mark-drained
// ---------------------------------------------------------------------

/// Postgres's wire protocol caps one statement's total bound parameters at
/// `i16::MAX` (65535) — the same limit `append::append`'s `MAX_ROWS_PER_STATEMENT`
/// exists to respect. A write row's parameter count scales with its target's
/// field count, so unlike `append::append` (whose row shape is fixed) this
/// is a parameter budget, not a row count: [`apply_target`] divides it by
/// `cols_per_row` to get the actual chunk size. 60000 leaves headroom below
/// 65535 regardless of column count.
const MAX_WRITE_PARAMS_PER_STATEMENT: usize = 60_000;

/// Decodes one [`TargetWrite::pk_text`]/[`TargetDelete::pk_text`] — this
/// crate's shared key-contract text ([`ddl::pk_key_sql_expr`]/[`ddl::join_pk_key`],
/// or a source row's own PK straight from CDC/backfill) — back into the real
/// value(s) [`apply_target`] must treat `key` as, via [`ddl::split_pk_key`]
/// (issue #205), at `pk`'s own arity (issue #121: a [`KeySpace::OneToOne`]
/// target's own primary key mirrors the source's in full, composite or not,
/// rather than narrowing to one column).
///
/// A `None` result (any one part decoding to a genuine `NULL` component —
/// only reachable for a [`KeySpace::OneToOne`] definition chained directly
/// off an aggregate target's own nullable grouping-column PK, issue #110's
/// `NULL_KEY_SENTINEL`/escape treatment) can never be stored as this target's
/// own primary-key value: [`ddl::create_target_table`] always declares every
/// primary-key column a real `primary key` column, which Postgres makes `NOT
/// NULL` unconditionally, regardless of whether the *source* column this
/// target's key was narrowed from is itself nullable. [`apply_target`]'s
/// callers treat `None` as "no representable row" and skip the key entirely
/// — the same outcome `defs::backfill::discover_pk_ranges`'s ordered
/// `(lo, hi]` PK-range walk already, structurally, produces for a NULL-keyed
/// source row: every range's `<=`/`>` bound is `NULL` (unknown) for a `NULL`
/// operand, so such a row is never selected by any chunk's `WHERE` and a full
/// backfill never attempts to insert it either. Skipping here keeps live CDC
/// apply's answer — "this group has no row in the target" — consistent with
/// backfill's, rather than attempting an insert Postgres's own `NOT NULL`
/// constraint would reject anyway.
pub(super) fn decode_target_pk_parts(
    pk: &[PrimaryKeyColumn],
    target: &str,
    key: &str,
) -> Result<Option<Vec<String>>, ApplyError> {
    Ok(ddl::split_pk_key(pk, target, key)?
        .into_iter()
        .map(|part| part.map(|c| c.into_owned()))
        .collect())
}

/// [`decode_target_pk_parts`]'s parts, rejoined into this crate's single
/// shared key-contract text ([`ddl::join_pk_key`]) — the shape
/// [`apply_and_mark_drained_many`]'s `hop_gen_of`/`src_changed_of` lookups
/// need to key on (matching what [`apply_target`]'s own `RETURNING`, built
/// from [`ddl::pk_key_sql_expr`], produces for the same row). A single part
/// renders verbatim (no join, matching [`ddl::join_pk_key`]'s own arity-1
/// exemption).
fn decode_target_pk_text(
    pk: &[PrimaryKeyColumn],
    target: &str,
    key: &str,
) -> Result<Option<String>, ApplyError> {
    Ok(decode_target_pk_parts(pk, target, key)?.map(|parts| join_pk_parts(&parts)))
}

/// Joins already-[`decode_target_pk_parts`]-decoded parts back into this
/// crate's single shared key-contract text, the same shape
/// [`ddl::join_pk_key`] produces — a thin wrapper so every call site spells
/// the arity-1 short-circuit (`parts[0].clone()`, no join) the same way
/// rather than each re-deriving it.
fn join_pk_parts(parts: &[String]) -> String {
    if parts.len() == 1 {
        parts[0].clone()
    } else {
        ddl::join_pk_key(parts.iter().map(|s| s.as_str()))
    }
}

/// `c0`, `c1`, … — collision-free column aliases for a keyset `unnest(...)`
/// relation, one per `pk` column, the same convention as
/// `intake::resume_orphans`'s `keyset_col` for its `GROUP BY` keyset (kept
/// as its own small copy here, not shared, since the two keysets differ in
/// what types they cast to — [`PrimaryKeyColumn::data_type`] here, a
/// [`ValueType`] there).
pub(super) fn pk_keyset_col(i: usize) -> String {
    format!("c{i}")
}

/// `unnest($start::text[]::t0[], $start+1::text[]::t1[], …) as k(c0, c1, …)`
/// — `pk`'s columns as a bound keyset relation, one array bind parameter per
/// column (so the whole keyset costs `pk.len()` bind parameters regardless of
/// how many keys it carries, well under Postgres's bind cap). The multi-
/// column match [`apply_target`] needs once a composite (arity > 1) primary
/// key means a plain `= any($1::text[]::cast[])` no longer identifies a row
/// on its own (issue #121) — arity 1 keeps that simpler, pre-existing form
/// instead (see [`apply_target`]'s own arity branch). A column recompute
/// used it at every arity (issue #377), since joining on the target's own
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
/// of `alias` against [`pk_keyset_unnest`]'s bound relation. Always plain
/// `=` (never `is not distinct from`): a 1-1 target's own primary-key columns
/// are never nullable (`ddl::create_target_table` always declares them a
/// real `primary key`, which Postgres makes `NOT NULL` unconditionally),
/// unlike `intake::resume_orphans`'s `GROUP BY` keyset match, which also
/// needs an `is null` arm for a nullable grouping column.
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
/// keyset join that takes such a bound asks here (#778, #790).
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
/// [`lock_composite_keys`]) do.
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
/// high that it read a 400k-row target in full and filtered it by the keys
/// (#793); with the settings it probes the key's index. With fresh
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
    txn.query(
        &single_lock_statement(target_ident, pk_ident, pk_cast, columns),
        &[&keys],
    )
    .await
}

/// [`apply_target`]'s pre-lock for a composite key (issue #121): the target
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
/// with the settings (#790). With fresh statistics the planner already
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
/// `rows` (each an already-[`decode_target_pk_parts`]-decoded key, in `pk`'s
/// own declared column order) so column `j`'s array is every row's `j`th
/// part.
pub(super) fn transpose_pk_parts<'a>(arity: usize, rows: &[&'a Vec<String>]) -> Vec<Vec<&'a str>> {
    (0..arity)
        .map(|j| rows.iter().map(|r| r[j].as_str()).collect())
        .collect()
}

/// What [`settle_one_to_one_target`] did to one target: how many keys it
/// physically wrote and deleted (the keys themselves went to its
/// [`TargetMutations`]), and the Re-derives it re-staged instead of applying.
type AppliedTarget = (usize, usize, Vec<Restage>);

/// One evaluated row's values in `field_names` order, rendered to the text
/// [`TargetWrite::values`] carries — shared by [`compute`]'s Applies and
/// [`settle_one_to_one_target`]'s Re-derives so the two can't drift.
fn evaluated_values(
    field_names: &[String],
    evaluated: &mut HashMap<String, Option<eval::Value>>,
) -> Vec<Option<String>> {
    field_names
        .iter()
        .map(|name| match evaluated.remove(name) {
            Some(Some(value)) => Some(value.to_string()),
            Some(None) | None => None,
        })
        .collect()
}

/// A key [`settle_one_to_one_target`] couldn't settle in Phase 3, to re-stage
/// as an image-less recompute.
type Restage = DerivedRecompute;

/// Evaluates a 1-1 definition over one source row, without its paused
/// columns: an Apply's image in Phase 2, or a Re-derive's row in Phase 3.
fn evaluate_one_to_one(
    rederive: &Rederive,
    row: &Row,
    regex_cache: &mut eval::RegexCache,
) -> Result<HashMap<String, Option<eval::Value>>, ApplyError> {
    Ok(match &rederive.relationships {
        Some((ctx, _)) => eval::evaluate_with_relationships_excluding(
            &rederive.def,
            row,
            &rederive.source_columns,
            ctx,
            regex_cache,
            &rederive.paused,
        )?,
        None => eval::evaluate_excluding(
            &rederive.def,
            row,
            &rederive.source_columns,
            regex_cache,
            &rederive.paused,
        )?,
    })
}

/// A 1-1 definition's `field_names`' types. A relationship-enriched field's
/// type can't come from `infer_field_types` (it rejects a `<rel>.<column>`
/// path, whose type is a *to-side* column's, unknown to the from-side type
/// map). The field's inferred type *is* its target column's declared type,
/// though (see `infer_field_types`' doc), and that column already exists —
/// introspect it, at `target_table`, the persisted, fully-qualified identity
/// (issue #73; `def.target` is always bare, issue #76). A definition with no
/// relationship keeps the pure inference.
async fn one_to_one_field_types(
    pool: &Pool,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    target_table: &str,
    field_names: &[String],
) -> Result<Vec<ValueType>, ApplyError> {
    let types = if eval::relationship_references(def).is_empty() {
        // No relationship metadata needed: an empty map (issue #40).
        validate::infer_field_types(def, source_columns, &HashMap::new())?
    } else {
        to_column_types(pool, target_table, field_names).await?
    };
    Ok(field_names
        .iter()
        .map(|name| types.get(name).copied().unwrap_or(ValueType::Numeric))
        .collect())
}

/// Each of `ctx`'s relationships' `from_col`, with the join keys `rows`
/// carry for it: the ones the context resolved (see [`Rederive`]).
fn join_coverage<'a>(
    ctx: RelationshipContext,
    rows: impl Iterator<Item = &'a Row> + Clone,
) -> JoinCoverage {
    let covered = ctx
        .join_columns()
        .map(|from_col| {
            let keys = rows
                .clone()
                .filter_map(|row| row.get(from_col).cloned().flatten())
                .collect();
            (from_col.to_string(), keys)
        })
        .collect();
    (ctx, covered)
}

/// A direct writer's Re-derive of 1-1 keys, outside a page (#623 D6):
/// a field build's chunk over a relationship-enriched 1-1 target (a column
/// resume, #625 F8b, `staging::build`). Settled on the
/// ledger exactly as a page settles a Re-derive
/// ([`settle_one_to_one_target`]). Built before the writer's transaction,
/// since a relationship-enriched definition's context is read through the
/// pool, as Phase 2 reads it.
pub(crate) struct DirectRederive {
    target: String,
    plan: TargetPlan,
}

impl DirectRederive {
    /// A Re-derive of source keys `keys` that writes every field but
    /// `excluded`, which it leaves as they are. With `skip_failing`, a row
    /// that fails to evaluate is left as it is, instead of failing the call.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        pool: &Pool,
        def: &TransformDef,
        source_table: &str,
        target_table: &str,
        source_columns: &HashMap<String, ValueType>,
        excluded: std::collections::HashSet<String>,
        skip_failing: bool,
        keys: &[String],
    ) -> Result<Self, ApplyError> {
        let pk = ddl::source_primary_key(pool, source_table).await?;
        let field_names: Vec<String> = def
            .fields
            .iter()
            .map(|f| f.name.clone())
            .filter(|name| !excluded.contains(name))
            .collect();
        let field_types =
            one_to_one_field_types(pool, def, source_columns, target_table, &field_names).await?;
        let row_columns = {
            let client = pool.get().await?;
            live_row_columns(&**client, source_table).await?
        };
        let relationships = if eval::relationship_references(def).is_empty() {
            None
        } else {
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            let mut live = read_live_rows_batch(pool, source_table, &pk, &key_refs).await?;
            let rows: Vec<Option<Row>> = keys.iter().map(|key| live.remove(key)).collect();
            let (ctx, _gen_bumps) =
                build_relationship_context(pool, source_table, def, &rows, None, None).await?;
            Some(join_coverage(ctx, rows.iter().flatten()))
        };
        let records = keys
            .iter()
            .map(|key| OneToOneRecord {
                pk_text: key.clone(),
                apply: None,
                hop_gen: 0,
                src_changed: None,
                origin_lsn: None,
                src_table: source_table.to_string(),
            })
            .collect();
        Ok(Self {
            target: def.target.clone(),
            plan: TargetPlan {
                pk,
                field_names,
                field_types,
                records,
                qualified_target: target_table.to_string(),
                qualified_source: source_table.to_string(),
                row_columns,
                rederive: Arc::new(Rederive {
                    def: def.clone(),
                    source_columns: source_columns.clone(),
                    paused: excluded,
                    skip_failing,
                    relationships,
                }),
            },
        })
    }

    /// Settles the Re-derive in `txn`, reporting every changed key to
    /// `mutations`. A tombstone this writes is stamped with the latest
    /// segment there is, so it is collected once everything staged so far
    /// has drained. A key whose row now joins through a key the context didn't
    /// resolve is left as it is: each writer parks a catch-up that
    /// re-derives every row once it finishes.
    pub(crate) async fn settle(
        &self,
        txn: &Transaction<'_>,
        mutations: &mut TargetMutations,
    ) -> Result<(), ApplyError> {
        let seg: i64 = txn
            .query_one("select coalesce(max(seg_seq), 0) from segments", &[])
            .await?
            .get(0);
        // Every record is a Re-derive already, so the first segment
        // changes nothing.
        settle_one_to_one_target(txn, &self.target, &self.plan, seg, seg, mutations).await?;
        Ok(())
    }
}

/// Settles one 1-1 target's records on its ledger, in the page's
/// transaction (#623 D6): locks the entries, reads and evaluates the
/// Re-derives' rows, updates the entries, then writes the target rows of
/// only the keys whose entry changed. See `super::one_to_one_ledger`.
///
/// A key carried by more than one record settles once, from the record that
/// reflects the latest state: a Re-derive, which reads it, or else the
/// latest Apply. A Re-derive of a relationship-enriched definition whose row
/// now joins through a key Phase 2's context didn't resolve is re-staged
/// instead, so a later page builds a context for it.
///
/// `first_seg` and `seg_seq` are the lowest and highest segments of the
/// page's batches. A page whose first is at or below the target's
/// `build_seg` re-derives every record instead of applying it, as a ledger
/// target's page does (#733, `super::ledger::page_may_predate_build`): a
/// change committed before the target's Re-derive build started may have
/// had a later change drained before the start, which never reached the
/// target, and a key deleted that way has no source row for a chunk to
/// find. A tombstone takes `seg_seq` as its `applied_seg`, or the
/// Re-derive read's newest segment when that is newer (#742): the read is
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
    let rederived_records: Vec<OneToOneRecord>;
    let records: &[OneToOneRecord] = if plan.records.iter().any(|r| r.apply.is_some())
        && super::ledger::page_may_predate_build(txn, &plan.qualified_target, first_seg).await?
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
    // A key that decodes to `None` (a NULL key part) has no target row to
    // write; it is never settled.
    let mut by_key: BTreeMap<&str, &OneToOneRecord> = BTreeMap::new();
    for record in records {
        if decode_target_pk_parts(&plan.pk, target, &record.pk_text)?.is_none() {
            continue;
        }
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
        return Ok((0, 0, Vec::new()));
    }
    // Planted bug (#557): #344/#392, apply every change without ADR-0002's
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
    let inserted = one_to_one_ledger::lock_entries(
        txn,
        &plan.qualified_target,
        &to_lock,
        seg_seq,
        predicate,
        false,
    )
    .await?;

    let rederive_keys: Vec<&str> = by_key
        .values()
        .filter(|r| r.apply.is_none())
        .map(|r| r.pk_text.as_str())
        .collect();
    // The entries' segment stamp: the page's latest segment, or the
    // Re-derive read's when that is newer (#742, see `read_rows`).
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
    let mut regex_cache = eval::RegexCache::new();
    let mut restage: Vec<Restage> = Vec::new();
    let mut rederived: HashMap<&str, Option<Vec<Option<String>>>> = HashMap::new();
    for key in rederive_keys {
        let values = match read.remove(key) {
            None => None,
            Some(row) => {
                if let Some((_, covered)) = &plan.rederive.relationships
                    && covered.iter().any(|(from_col, joined)| {
                        matches!(row.get(from_col), Some(Some(value)) if !joined.contains(value))
                    })
                {
                    let r = by_key[key];
                    restage.push((
                        r.src_table.clone(),
                        r.pk_text.clone(),
                        r.hop_gen,
                        r.src_changed,
                        r.origin_lsn,
                    ));
                    continue;
                }
                let mut evaluated =
                    match evaluate_one_to_one(&plan.rederive, &row, &mut regex_cache) {
                        Ok(evaluated) => evaluated,
                        Err(_) if plan.rederive.skip_failing => continue,
                        Err(err) => return Err(err),
                    };
                Some(evaluated_values(&plan.field_names, &mut evaluated))
            }
        };
        rederived.insert(key, values);
    }

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
        &plan.qualified_target,
        &changes,
        snapshot.as_deref(),
        entry_seg,
        predicate,
    )
    .await?;
    changed.extend(inserted.applied);
    // A placeholder nothing wrote doesn't outlive the page (#774).
    let unwritten: Vec<&str> = inserted
        .keys
        .iter()
        .filter(|key| !changed.contains(*key))
        .map(String::as_str)
        .collect();
    one_to_one_ledger::drop_placeholders(txn, &plan.qualified_target, &unwritten).await?;

    let mut writes = Vec::new();
    let mut deletes = Vec::new();
    for record in by_key.values() {
        if !changed.contains(&record.pk_text) {
            continue;
        }
        let values = match &record.apply {
            Some((_, _, values)) => values.clone(),
            None => rederived.remove(record.pk_text.as_str()).flatten(),
        };
        match values {
            Some(values) => writes.push(TargetWrite {
                pk_text: record.pk_text.clone(),
                values,
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
    let (written, deleted) = apply_target(txn, target, plan, &writes, &deletes, mutations).await?;
    Ok((written, deleted, restage))
}

/// Runs one target table's ordered pre-lock, then its no-op-suppressed
/// upsert and delete, reporting every key Postgres actually wrote or deleted
/// (as opposed to every key this batch merely *proposed* — the
/// no-op-suppression `WHERE ... IS DISTINCT FROM ...` guard can mean a
/// proposed write physically changes nothing) to `mutations`, with the
/// key's prior image (issue #315, which subsumes #196's image-bearing
/// delete; a relationship reader downstream takes a deleted row's old join
/// value from it until #624). Returns only the counts.
///
/// The pre-lock takes every key this call touches (write or delete) `FOR
/// UPDATE`, ordered ascending, in one round trip — the deadlock-avoidance
/// convention doc 05 calls for between concurrent workers writing
/// overlapping target rows. At arity 1 it binds the whole key set as a
/// single `text[]` parameter; a composite (arity > 1) key instead binds one
/// typed array per column and joins the target to that bound keyset (issue
/// #121) —
/// either way the parameter count is `O(pk.len())`, not `O(batch size)`, so
/// — unlike the upsert below — it never approaches the bind-parameter cap
/// regardless of batch size. Because this transaction already holds every
/// lock it needs before the upsert/delete run, chunking those into multiple
/// statements below doesn't reopen the ordering gap the pre-lock exists to
/// close: two transactions racing on overlapping keys still each take every
/// lock, in the same ascending order, before either writes anything.
///
/// Issue #56/ADR-0009 decision 3: the finest-grained span in the
/// propagation tree — one per consuming transform per batch, downstream of
/// fold (`docs/observability.md`'s "Logs and traces" section), the exact
/// same grouping #51's `buffer_transform_apply_metrics` observes its
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
    // `plan.qualified_target` (issue #73's persisted identity), not a bare
    // `quote_ident(target)` — a target explicitly qualified into a
    // non-default schema (issue #76) isn't necessarily on this connection's
    // pinned `search_path`. See `TargetPlan::qualified_target`'s doc comment
    // and `ddl::qualified_target_table_ident`'s.
    let target_ident = ddl::qualified_target_table_ident(&plan.qualified_target);
    let field_idents: Vec<String> = plan.field_names.iter().map(|n| quote_ident(n)).collect();

    // Issue #315: the pre-lock below doubles as the prior-image capture every
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

    // Issue #205/#121: `write.pk_text`/`delete.pk_text` is this target's
    // shared key-contract text, not necessarily raw PK value(s) yet — decode
    // each one through `decode_target_pk_parts` before treating it as a
    // literal PK value (or a lock/match key) anywhere below. See that
    // function's doc comment: `None` (a genuine NULL-keyed group, only
    // reachable for a `KeySpace::OneToOne` definition chained off a nullable
    // aggregate grouping key) can never be this target's own stored PK
    // value, so such a key is simply dropped from every step below, rather
    // than bound as a literal `NULL_KEY_SENTINEL`/escaped string (the
    // corruption issue #205 closes) or as a literal SQL `NULL` (which this
    // target's own `NOT NULL` primary-key column(s) would just as reliably
    // reject).
    let decoded_writes: Vec<Option<Vec<String>>> = plan_writes
        .iter()
        .map(|w| decode_target_pk_parts(&plan.pk, target, &w.pk_text))
        .collect::<Result<_, _>>()?;
    let decoded_deletes: Vec<Option<Vec<String>>> = plan_deletes
        .iter()
        .map(|d| decode_target_pk_parts(&plan.pk, target, &d.pk_text))
        .collect::<Result<_, _>>()?;

    // Deduplicated lock keys (issue #205's decoded form, so a batch never
    // locks the same physical row twice under two differently-encoded
    // spellings of the same key) — the actual lock *order* comes from each
    // branch's own `order by` below, not from this collection's order, so a
    // plain dedup is all that's needed here.
    let mut lock_key_parts: Vec<&Vec<String>> = decoded_writes
        .iter()
        .chain(decoded_deletes.iter())
        .filter_map(|k| k.as_ref())
        .collect();
    lock_key_parts.sort_unstable();
    lock_key_parts.dedup();

    let locked = if arity == 1 {
        // A single bound `text[]` array.
        let pk_ident = &pk_idents[0];
        let pk_cast = plan.pk[0].data_type.as_str();
        let lock_keys: Vec<&str> = lock_key_parts.iter().map(|p| p[0].as_str()).collect();
        lock_single_keys(
            txn,
            &target_ident,
            pk_ident,
            pk_cast,
            &format!("{lock_key_expr}{prior_select}"),
            &lock_keys,
        )
        .await?
    } else {
        // Issue #121: a composite key is matched against a bound keyset
        // relation instead; see `composite_lock_statement`.
        let arrays = transpose_pk_parts(arity, &lock_key_parts);
        lock_composite_keys(
            txn,
            &target_ident,
            &plan.pk,
            &format!("{lock_key_expr}{prior_select}"),
            &arrays,
        )
        .await?
    };
    let prior_images: HashMap<String, String> = if prior_image_expr.is_some() {
        locked
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect()
    } else {
        HashMap::new()
    };

    let field_pg_types: Vec<Cow<'static, str>> = plan
        .field_types
        .iter()
        .map(|t| ddl::pg_type_name(*t))
        .collect();

    let col_list = pk_idents
        .iter()
        .cloned()
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    // Only a write whose key actually decoded to a representable (non-NULL)
    // PK value is a candidate for insertion — see `decode_target_pk_parts`'s
    // doc comment on why a NULL-decoded write has no row to write at all.
    let writable: Vec<(&TargetWrite, &Vec<String>)> = plan_writes
        .iter()
        .zip(decoded_writes.iter())
        .filter_map(|(w, k)| k.as_ref().map(|k| (w, k)))
        .collect();

    let mut written = Vec::new();
    if !writable.is_empty() {
        let cols_per_row = arity + plan.field_names.len();
        let rows_per_chunk = (MAX_WRITE_PARAMS_PER_STATEMENT / cols_per_row).max(1);

        // Every one of this target's calculated columns can be paused at
        // once (ADR-0003's amendment) — a single-field definition whose lone
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
            format!(
                "on conflict ({pk_col_list}) do update set {set_list} \
                 where ({target_cols}) is distinct from ({excluded_cols})"
            )
        };

        // The composite key's own canonical identity text (issue #121) —
        // what this statement's `RETURNING` reports as each written row's
        // key, matching `ddl::pk_key_sql_expr`'s row-identity encoding at any
        // arity (a bare `{col}::text` at arity 1, byte-identical to before
        // this issue) so a downstream chained definition's live re-fetch
        // parses it back the same way.
        let returning_pk_expr = ddl::pk_key_sql_expr(&plan.pk, None);

        for chunk in writable.chunks(rows_per_chunk) {
            let mut rows_sql = Vec::with_capacity(chunk.len());
            let mut params: Vec<&(dyn ToSql + Sync)> =
                Vec::with_capacity(chunk.len() * cols_per_row);
            for (i, (write, pk_parts)) in chunk.iter().enumerate() {
                let base = i * cols_per_row;
                let mut row_parts = Vec::with_capacity(cols_per_row);
                for (j, c) in plan.pk.iter().enumerate() {
                    row_parts.push(format!("${}::text::{}", base + j + 1, c.data_type));
                    params.push(&pk_parts[j]);
                }
                for (j, pg_type) in field_pg_types.iter().enumerate() {
                    row_parts.push(format!("${}::text::{pg_type}", base + arity + j + 1));
                    params.push(&write.values[j]);
                }
                rows_sql.push(format!("({})", row_parts.join(", ")));
            }

            let sql = format!(
                "insert into {target_ident} ({col_list}) \
                 select * from (values {}) as v({col_list}) \
                 {on_conflict} \
                 returning {returning_pk_expr} as pk",
                rows_sql.join(", "),
            );
            let rows = txn.query(&sql, &params).await?;
            written.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
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
    // decoded keys (issue #205), joined to one canonical text (issue #121)
    // so a composite key compares as a whole tuple, not accidentally by its
    // first column alone.
    let write_keys: std::collections::HashSet<String> = writable
        .iter()
        .map(|(_, parts)| join_pk_parts(parts))
        .collect();

    let mut deleted = Vec::new();
    // A `None`-decoded delete has no matching write (a NULL-keyed group
    // never reaches `writable` either) and no representable row to delete —
    // see `decode_target_pk_parts`'s doc comment — so it's dropped here the
    // same way a `None`-decoded write is dropped above.
    let delete_key_parts: Vec<&Vec<String>> = decoded_deletes
        .iter()
        .filter_map(|k| k.as_ref())
        .filter(|k| !write_keys.contains(&join_pk_parts(k)))
        .collect();
    if !delete_key_parts.is_empty() {
        // `t.`-qualified (issue #121: the composite branch's `exists (...)`
        // subquery below introduces a second relation into scope, so an
        // unqualified column reference would become ambiguous).
        let returning_pk_expr = ddl::pk_key_sql_expr(&plan.pk, Some("t"));
        let rows = if arity == 1 {
            let pk_ident = &pk_idents[0];
            let pk_cast = plan.pk[0].data_type.as_str();
            let delete_keys: Vec<&str> = delete_key_parts.iter().map(|p| p[0].as_str()).collect();
            txn.query(
                &format!(
                    "delete from {target_ident} as t \
                     where t.{pk_ident} = any($1::text[]::{pk_cast}[]) \
                     returning {returning_pk_expr} as pk"
                ),
                &[&delete_keys],
            )
            .await?
        } else {
            // Issue #121: a composite key has no single column an
            // `= any(...)` array test could name, so the delete instead
            // matches any row whose full key tuple appears in a bound
            // keyset relation (a correlated `exists`, mirroring the
            // pre-lock's join above).
            let arrays = transpose_pk_parts(arity, &delete_key_parts);
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

    // Issue #315: report every physically-changed key to the seam, with the
    // `hop_gen`/`src_changed` of the change that produced it and the prior
    // image the pre-lock captured (`None` for a key this call created). Keyed
    // by the *decoded* PK text (issue #205/#121), which for a target chained
    // off a nullable grouping key can differ from `write.pk_text`'s encoded
    // form — so the lookups below decode it the same way. A deleted row's
    // prior image is what lets a downstream aggregate find the group it left
    // (issues #180/#196, now a recompute hint rather than a delta).
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
        if let Some(decoded) = decode_target_pk_text(&plan.pk, target, pk_text)? {
            origin_of.insert(decoded, (hop_gen, (src_changed, origin_lsn)));
        }
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

/// A source `TRUNCATE`'s clear of one target (issue #60 for a 1-1 target,
/// every group of an aggregate one): a plain `DELETE FROM <target>`, each
/// removed row's key (in `pk`'s shared key-contract encoding,
/// `ddl::pk_key_sql_expr` — for an aggregate target its `GROUP BY`
/// columns) and, when something reads
/// the target, its image reported to `mutations` as the key's prior image
/// (issue #315). Returns how many rows it deleted.
async fn clear_target(
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
/// 2. **Truncate clears** (issue #60), one plain `DELETE FROM <target>` per
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
///    #623 D6): the sorted entry lock, the Re-derive read, the entries'
///    ADR-0002 I2 test, then the ordered pre-lock + upsert/delete of only
///    the keys it changed ([`apply_target`]), immediately followed by the **relationship
///    settled-parent projection gen bump** (issue #130, epic #127): every
///    to-one relationship parent this batch's relationship resolution
///    touched (`plan.relationship_gen_bumps`) gets its projection's
///    `__trellis_gen` bumped by 1, in this same transaction — see that
///    step's own inline comment for the exact semantics and the #133 gap it
///    documents.
/// 4. **Downstream propagation**: every key a step above physically changed
///    (write or delete — no-op-suppressed writes don't count) was reported to
///    a [`TargetMutations`] (issue #315); for every such target some `live`
///    definition reads, it stages an image-less `Recompute` at `hop_gen + 1`
///    carrying the key's prior image, enforcing [`MAX_HOP_GEN`] first. See
///    `staging::target_mutations` for why this seam, not CDC, is how a
///    chained hop hears about its upstream target. A definition created
///    between Phase 2 and this commit that starts reading a target is not
///    missed: its own backfill (or catch-up marker) derives it from current
///    target state.
/// 5. **The completion statement**: deletes this claim's `seg_claims` rows
///    and ORs their buckets into `segments.drained_mask`, flipping
///    `state` to `'drained'` once every bucket has drained — one statement,
///    so "this claim released" and "its buckets marked drained" can never
///    observably happen one without the other. Empty `DELETE ... RETURNING`
///    means the claim was already gone — [`ApplyError::ClaimLost`].
/// 6. A `pg_notify` on `wake_channel`, for anything awaiting convergence.
///
/// A thin single-segment wrapper over [`apply_and_mark_drained_many`] (issue
/// #63 Milestone 2) — every step below is shared verbatim with the
/// multi-segment path; this function exists only to keep the pre-#63 public
/// signature (and the every-`drain_once`-drains-exactly-one-segment
/// contract every existing caller and test relies on) unchanged.
pub async fn apply_and_mark_drained(
    txn: &Transaction<'_>,
    seg_seq: i64,
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
    watermark: &StagedWatermark,
) -> Result<ApplyOutcome, ApplyError> {
    let outcome =
        apply_and_mark_drained_many(txn, &[seg_seq], claimed_by, plan, wake_channel, watermark)
            .await?;
    Ok(ApplyOutcome {
        keys_written: outcome.keys_written,
        keys_deleted: outcome.keys_deleted,
        batch_drained: outcome.segments_drained[0].1,
        deferral_counts: outcome.deferral_counts,
        fairness_escalations: outcome.fairness_escalations,
        pages: outcome.pages,
    })
}

/// The [`apply_and_mark_drained`] steps generalized over `seg_seqs` — issue
/// #63 Milestone 2's segment-coalescing seam. `plan` (Phase 2's output) was
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
/// Issue #56/ADR-0009 decision 3: the batch-level span in the propagation
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
    watermark: &StagedWatermark,
) -> Result<ManyApplyOutcome, ApplyError> {
    let steps: Vec<SegmentStep> = seg_seqs
        .iter()
        .map(|&seg_seq| SegmentStep {
            seg_seq,
            page: None,
        })
        .collect();
    apply_page(txn, &steps, claimed_by, plan, wake_channel, watermark).await
}

/// How one segment's claim ends a Phase 3 transaction (issue #620 A2a).
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

/// [`apply_and_mark_drained_many`] with an explicit claim ending per segment
/// (issue #620 A2a): a non-final page's transaction applies, checks the claim
/// and advances the cursor; a final page's applies and completes. Everything
/// before step 5 is the same either way.
#[tracing::instrument(
    name = "staging.apply_and_mark_drained",
    skip(txn, steps, plan, wake_channel, watermark),
    fields(
        segments = steps.len(),
        targets = plan.targets.len(),
        ledger_targets = plan.ledger_targets.len(),
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
    watermark: &StagedWatermark,
) -> Result<ManyApplyOutcome, ApplyError> {
    // 1. Version fence. `source_key` is the canonical (qualified, where
    // resolvable) source identity `compute` keyed `plan.versions` on, and
    // `source_table_versions.source_table` is qualified (issue #72), so this
    // matches exactly — see `defs::source_table_version`, which reads the same
    // row the same way.
    for (source_key, loaded_version) in &plan.versions {
        let row = txn
            .query_opt(
                "select version from source_table_versions \
                 where source_table = $1 for share",
                &[source_key],
            )
            .await?;
        let current: Option<i64> = row.map(|r| r.get(0));
        if current != *loaded_version {
            tracing::debug!(
                src_table = %source_key,
                "version fence miss: source table's definitions changed mid-drain"
            );
            return Err(ApplyError::VersionFenceMiss {
                src_table: source_key.clone(),
            });
        }
    }

    // 1b. Issue #16: park this batch's own folded contribution for every
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
    // Issue #134/#135 review follow-up: per-guard deferral counts this
    // Phase 3 pass discovers, buffered here rather than recorded eagerly —
    // see [`ManyApplyOutcome::deferral_counts`]'s own doc comment for why
    // (this function's own `VersionFenceMiss`/transient-failure retry loop,
    // one layer up in `drain_once`/`drain_many`, can call it more than once
    // for the same folded input; only the attempt whose transaction
    // actually commits may ever reach the metrics registry).
    let mut deferral_counts: HashMap<&'static str, u64> = HashMap::new();
    // Issue #135: count of reverse transitions this pass resolved via
    // fairness escalation (see [`ManyApplyOutcome::fairness_escalations`]'s
    // doc comment) — buffered under the exact same post-commit-only
    // contract as `deferral_counts` just above, for the same reason.
    let mut fairness_escalations: u64 = 0;
    // Issue #315: every target write below reports its physically-changed
    // keys here, and step 4 turns them into downstream `Recompute` rows. See
    // `staging::target_mutations` — no writer hands its keys back to this
    // function, so none can skip propagation.
    let mut mutations = TargetMutations::new();

    // 2. Truncate clears, before this target's own upsert/delete below —
    // see this function's doc comment on why "clear, then write" is safe
    // here specifically (single-bucket batch, barrier-drained).
    for clear in plan.clears.values() {
        one_to_one_ledger::truncate(txn, &clear.qualified_target, clear.truncate_lsn).await?;
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
    // target's `GROUP BY` key (issue #315: each cleared group reaches the
    // seam, where it used to go unpropagated).
    for clear in plan.aggregate_clears.values() {
        // #623 D3: a ledger target's truncate empties its ledger and raises
        // its truncate floor (the D split's Q6), then clears its groups.
        if clear.on_ledger {
            super::ledger::truncate_ledger(txn, &clear.qualified_target, clear.truncate_lsn)
                .await?;
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

    // 3. The 1-1 targets on their ledgers (#623 D6), one at a time in
    // target order, so every page and every direct writer takes their
    // entry locks in one order: see `settle_one_to_one_target`.
    let page_seg = steps.iter().map(|step| step.seg_seq).max().unwrap_or(0);
    let page_first_seg = steps.iter().map(|step| step.seg_seq).min().unwrap_or(0);
    let mut restaged: Vec<Restage> = Vec::new();
    for (target, target_plan) in &plan.targets {
        let (written, deleted, restage) = settle_one_to_one_target(
            txn,
            target,
            target_plan,
            page_first_seg,
            page_seg,
            &mut mutations,
        )
        .await?;
        restaged.extend(restage);
        keys_written += written;
        keys_deleted += deleted;
    }

    // 3b. Aggregate targets, all on the ledger (#623 D3 to D5), in target
    // order. A tombstone's `applied_seg` is the page's latest segment: a key
    // never splits across a page's segments by more than that, and a later
    // stamp only delays tombstone GC (the D split's Q7).
    for ledger_plan in plan.ledger_targets.values() {
        let (written, deleted) = super::ledger::apply_ledger_target(
            txn,
            ledger_plan,
            page_first_seg,
            page_seg,
            &mut mutations,
        )
        .await?;
        keys_written += written;
        keys_deleted += deleted;
    }

    // Before 2c and 3c, issue #531: the refresh stamp of every relationship
    // this batch carries a reverse record or a projection clear for, locked
    // `for share` before this transaction touches any projection row (2c,
    // 3c and 3d below), in `relationship_id` order. A projection refresh
    // locks the same rows `for update`, in the same order, before it touches
    // a projection row (`catalog::refresh_relationship_projections_in_txn`),
    // so either it commits first and a record at or below its stamp reads
    // the stamp here (and takes the live-row check, `superseded_to_side`, or
    // for a truncate skips the clear), or this transaction commits first and
    // the refresh reads what it wrote. Taken after the target writes (steps
    // 2 to 3b), the order the discharge takes the same kinds of row in (its
    // orphan sweep, then the refresh). One keyed read per batch with reverse
    // records or projection clears, none without.
    let refresh_stamps = relationship_refresh_stamps(
        txn,
        plan.relationship_reverses
            .iter()
            .map(|r| r.shape.id)
            .chain(plan.relationship_projection_clears.keys().copied()),
    )
    .await?;

    // 2c. Issue #168: settled parent projection clears — a `TRUNCATE` on a
    // to-one relationship's to-side empties that relationship's
    // projection too, as a whole-table delete: every row this projection held
    // for this relationship just vanished along with the to-side's. No
    // downstream propagation of its own — the projection is not a target
    // table, and the from-side recompute this same truncate
    // stages via `ApplyPlan::reverse_recomputes` is what actually reaches a
    // definition; the projection is only ever read by
    // `build_relationship_context`/the reverse-guard machinery, never a
    // definition's own downstream consumer. Run after the target writes
    // rather than beside the target clears (step 2) so the stamp lock above
    // precedes it. Issue #531: a truncate at or below the relationship's
    // refresh stamp is one the refresh already read, so emptying the
    // projection would drop the rows the refresh found written after it.
    for (relationship_id, clear) in &plan.relationship_projection_clears {
        if truncate_overtaken_by_refresh(clear.lsn, refresh_stamps.get(relationship_id).copied()) {
            continue;
        }
        txn.execute(&format!("delete from {}", clear.qualified_projection), &[])
            .await?;
    }

    // 3c. Relationship settled-parent projection gen bump (issue #130, epic
    // #127; plan doc §2 guard (b)'s precondition): every to-one relationship
    // parent this batch's relationship resolution touched
    // (`ApplyPlan::relationship_gen_bumps`, resolved in Phase 2) gets its
    // projection row's `__trellis_gen` bumped by exactly 1, in this same
    // transaction — so guard (b) (#132) can detect, by re-reading under `FOR
    // UPDATE` and comparing against a value it captured earlier, that a
    // forward apply landed in between. One `UPDATE ... SET gen = gen + 1
    // WHERE key = ANY(...)` per relationship, over the *deduped* set of
    // touched keys (`RelationshipGenBump::touched_keys` is a `HashSet`) — so
    // a parent touched by two different from-side rows in this same batch
    // (e.g. two children re-pointing onto the same parent) still only
    // advances its generation by 1 for the whole transaction, not once per
    // touching row: guard (b) only needs "did anything land since I captured
    // this," not a count of how many things did. A key with no projection
    // row yet (see `ensure_relationship_projection_in_txn`'s own doc comment
    // on the widen-only catch-up gap #131 closes) simply bumps nothing — no
    // error, same as any `UPDATE ... WHERE` matching zero rows. Plain
    // `key_col::text = any($1::text[])`, not the native-typed cast
    // `from_side_keys`'s own doc comment flags as unindexed
    // (P0.1/plan doc §6) — out of scope here, matches this module's other
    // untyped relationship lookups. The touched-key array is sorted before
    // binding (review follow-up to #132) — see the inline comment at that
    // sort for why.
    for bump in plan.relationship_gen_bumps.values() {
        if bump.touched_keys.is_empty() {
            continue;
        }
        // Ascending-key lock order (review follow-up to #132): `touched_keys`
        // is a `HashSet`, whose iteration order is unspecified and can vary
        // run to run. Without a deterministic sort here, two concurrent
        // `apply_and_mark_drained_many` calls whose batches both touch an
        // overlapping set of relationship-projection parent keys (plausible
        // whenever two segments both contain from-side rows re-pointing
        // among the same hot parents) could have their `UPDATE ... WHERE key
        // = ANY($1)` lock those rows in different orders and deadlock —
        // Postgres detects and aborts one side rather than corrupting
        // anything, but it's a needless liveness hazard, and this codebase
        // already has the fix for exactly this class of bug: the
        // target-write lock just above sorts (and dedups) its keys before
        // taking `FOR UPDATE` locks, with `two_overlapping_group_writers_serialize_via_ascending_lock_order_not_deadlock`
        // as its regression pin. Sorting the bound array doesn't change
        // *what* this bare `UPDATE` locks, only lines up every concurrent
        // caller's lock-acquisition order onto the same ascending sequence.
        let mut keys: Vec<&str> = bump.touched_keys.iter().map(String::as_str).collect();
        keys.sort_unstable();
        let key_ident = quote_ident(&bump.key_col);
        let gen_ident = quote_ident(ddl::PROJECTION_GEN_COLUMN);
        txn.execute(
            &format!(
                "update {} set {gen_ident} = {gen_ident} + 1 \
                 where {key_ident}::text = any($1::text[])",
                bump.qualified_projection,
            ),
            &[&keys],
        )
        .await?;
    }

    // 3d. Issue #131, epic #127: to-one relationship reverse apply — see
    // `RelationshipReverseRecord`'s doc comment for the mechanism. Each
    // record's from-side Recomputes are staged for a later drain.
    let mut relationship_reverse_fallback: Vec<DerivedRecompute> = Vec::new();
    // Issue #134: guard-rejected records are re-staged as
    // `StagedChange::RelationshipReverseDeferred` (never touching `hop_gen`)
    // rather than folded into `recompute_changes` (which enforces
    // `MAX_HOP_GEN` below) — appended separately, after this loop, via its
    // own `append::append` call.
    let mut relationship_reverse_deferrals: Vec<StagedChange> = Vec::new();
    // Issue #248 review follow-up: `from_side_rows_for_trigger_txn` needs
    // each touched `from_table`'s live column list (`row_as_text_jsonb_sql`,
    // in place of `to_jsonb(t.*)`), and this loop runs once per distinct
    // touched parent key in the batch — many records sharing one
    // relationship all share one `from_table`. Caching per `from_table`
    // across the *whole* loop (not just within one record) is what keeps
    // that at one `pg_catalog` round trip per distinct `from_table`, not one
    // per record, on a path that already fans out with wide
    // reverse-relationship batches.
    let mut row_columns_cache: HashMap<String, Vec<String>> = HashMap::new();
    // Issue #762: the ring rows this transaction applies, which a live
    // write's pending read counts as applied.
    let claim = super::page::ClaimScope { steps, claimed_by };
    for record in &plan.relationship_reverses {
        let shape = &record.shape;
        let old_key = relationship_key_text(&record.old_row, &shape.to_col, &shape.name)?;
        let new_key = relationship_key_text(&record.new_row, &shape.to_col, &shape.name)?;
        // Resolved once per record from the batch-wide cache above — every
        // `from_side_rows_for_trigger_txn` call this record makes (via
        // `stage_reverse_recompute_fallback`) reuses this same slice.
        let row_columns = cached_row_columns(txn, &mut row_columns_cache, &shape.from_table)
            .await?
            .to_vec();

        // Issue #132: all four guards, checked together — see
        // `check_reverse_guards`'s own doc comment for the mechanism, the
        // locking discipline, and why they're combined into one Phase 3
        // step instead of four independent ones.
        if let Some(failure) =
            check_reverse_guards(txn, shape, record, &old_key, &new_key, watermark).await?
        {
            // Issue #135: fairness escalation — see this module's own
            // "Issue #135, epic #127: fairness escalation" design section
            // (right after `check_reverse_guards`) for the full mechanism
            // and why it is sound. Guard (d) failing itself is never
            // eligible (see that section's "residual limitation"): the next
            // check independently confirms guard (d) still holds before
            // this branch is allowed to touch the projection at all.
            if failure != ReverseGuardFailure::Ordering
                && record.retry_count + 1 >= RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD
                && reverse_ordering_still_holds(txn, shape, &old_key, &new_key, record).await?
            {
                *deferral_counts.entry(failure.metric_label()).or_insert(0) += 1;
                fairness_escalations += 1;
                tracing::warn!(
                    relationship_projection = %shape.qualified_projection,
                    guard = %failure,
                    retry_count = record.retry_count + 1,
                    threshold = RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD,
                    "issue #135 fairness escalation: this reverse's guard-gated retry budget \
                     is exhausted; advancing the projection and falling back to the \
                     always-correct recompute instead of deferring again"
                );
                let mut seen_keys: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                stage_reverse_recompute_fallback(
                    txn,
                    record,
                    &old_key,
                    &new_key,
                    &mut seen_keys,
                    &mut relationship_reverse_fallback,
                    &row_columns,
                )
                .await?;
                let superseded = superseded_to_side(
                    txn,
                    &mut row_columns_cache,
                    &refresh_stamps,
                    record,
                    &old_key,
                    &new_key,
                )
                .await?;
                if superseded {
                    apply_projection_from_live(
                        txn,
                        shape,
                        &old_key,
                        &new_key,
                        record.lsn,
                        record.lsn,
                        Some(&claim),
                    )
                    .await?
                } else {
                    apply_projection_advance(
                        txn,
                        shape,
                        &old_key,
                        &new_key,
                        record.new_image.as_deref(),
                        record.lsn,
                    )
                    .await?
                }
                continue;
            }

            // Issue #134: defer and re-stage, rather than #131/#132/#133's
            // stopgap of falling back to an image-less `Recompute` of every
            // touched from-side row at `hop_gen + 1`. Do NOT touch any
            // target table or the projection for this record — exactly the
            // same "nothing applied, nothing lost" posture the stopgap took
            // — but re-stage the record itself (not its from-side rows) as
            // a `RelationshipReverseDeferred`, so the next drain that picks
            // it up re-derives fresh guard state and re-attempts the exact
            // same delta this one just failed to apply, at no `hop_gen`
            // cost: deferral is measured to be the *common* case for this
            // mechanism (see this module's own doc section), and burning a
            // hop generation per retry would trip `MAX_HOP_GEN` after 32
            // routine deferrals.
            //
            // Buffered into `deferral_counts`, not recorded straight into
            // the metrics registry here — see that variable's own comment.
            *deferral_counts.entry(failure.metric_label()).or_insert(0) += 1;
            tracing::warn!(
                relationship_projection = %shape.qualified_projection,
                guard = %failure,
                retry_count = record.retry_count + 1,
                "issue #132 reverse guard rejected this record's delta; \
                 deferring and re-staging it for retry (issue #134)"
            );
            // Guaranteed `Some` here: `check_reverse_guards` can only reach
            // this branch (rather than passing every guard) when at least
            // one of `old_key`/`new_key` is `Some` — see
            // `check_reverse_guards`'s own `lock_key` match, whose `_ =>
            // (None, None)` arm (both keys absent) always passes every
            // guard and never returns `Some(failure)` at all.
            let key = old_key
                .clone()
                .or_else(|| new_key.clone())
                .expect("check_reverse_guards only rejects a record with a known key");
            relationship_reverse_deferrals.push(StagedChange::RelationshipReverseDeferred {
                src_table: relationship_reverse_deferred_src_table(shape.id),
                key,
                old_image: record.old_image.clone(),
                new_image: record.new_image.clone(),
                lsn: record.lsn,
                src_changed: record.src_changed,
                origin_lsn: record.origin_lsn,
                relationship_id: shape.id,
                retry_count: record.retry_count + 1,
                group_key: Some(
                    old_key
                        .iter()
                        .chain(new_key.iter().filter(|k| Some(*k) != old_key.as_ref()))
                        .cloned()
                        .collect(),
                ),
            });
            continue;
        }

        // Issues #507/#531: a record whose images its to-side has moved
        // past (see `to_side_superseded`) advances the projection to the
        // live row.
        let superseded = superseded_to_side(
            txn,
            &mut row_columns_cache,
            &refresh_stamps,
            record,
            &old_key,
            &new_key,
        )
        .await?;
        // #623 D5: every target reading this relationship re-derives each
        // from-side row the parent change reaches; an aggregate on the
        // ledger reads the parent live, so it needs no images.
        if shape.needs_recompute_fallback {
            let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
            stage_reverse_recompute_fallback(
                txn,
                record,
                &old_key,
                &new_key,
                &mut seen_keys,
                &mut relationship_reverse_fallback,
                &row_columns,
            )
            .await?;
        }

        // Advance the projection — issue #131's own write, distinct from
        // step 3c's forward `__trellis_gen` bump above (never touched
        // here; see `apply_projection_advance`'s doc comment for the
        // distinction).
        if superseded {
            apply_projection_from_live(
                txn,
                shape,
                &old_key,
                &new_key,
                record.lsn,
                record.lsn,
                Some(&claim),
            )
            .await?
        } else {
            apply_projection_advance(
                txn,
                shape,
                &old_key,
                &new_key,
                record.new_image.as_deref(),
                record.lsn,
            )
            .await?
        }
    }

    // 4. Downstream propagation, with the hop bound checked before staging
    // anything. Issue #315: every key a write above physically changed, for
    // every target some `live` definition reads, becomes one image-less
    // `Recompute` at `hop_gen + 1` carrying the key's prior image — see
    // `staging::target_mutations`. `lsn: None`, like every other row this
    // step stages: a propagated hop has no source LSN of its own. (A target
    // the seam feeds as a relationship endpoint gets a CDC-shaped row with
    // the write token as its `lsn` instead, issue #402.)
    let propagation = mutations.into_staged(txn).await?;
    let mut recompute_changes = propagation.changes;
    let mut hop_bound_tables = propagation.hop_bound_tables;
    let mut worst_hop_gen = propagation.worst_hop_gen;

    // Reverse recompute (issue #30): from-side rows a changed related row must
    // re-derive, resolved in Phase 2 and staged here as ordinary image-less
    // recomputes — the same shape and same hop bound forward propagation uses,
    // just keyed by the from-side table/PK rather than a touched target key.
    for (from_table, key, hop_gen, src_changed, origin_lsn) in &plan.reverse_recomputes {
        if *hop_gen > MAX_HOP_GEN {
            hop_bound_tables.push(from_table.clone());
            worst_hop_gen = worst_hop_gen.max(*hop_gen);
            continue;
        }
        recompute_changes.push(StagedChange::Recompute {
            src_table: from_table.clone(),
            key: key.clone(),
            hop_gen: *hop_gen,
            group_key: None,
            src_changed: *src_changed,
            prior_image: None,
            origin_lsn: *origin_lsn,
        });
    }

    // Re-derived keys whose row now joins through a key Phase 2 didn't
    // resolve ([`settle_one_to_one_target`]), staged at their own `hop_gen`
    // — a re-read of the same hop, not a step further downstream.
    for (src_table, key, hop_gen, src_changed, origin_lsn) in restaged {
        recompute_changes.push(StagedChange::Recompute {
            src_table,
            key,
            hop_gen,
            group_key: None,
            src_changed,
            prior_image: None,
            origin_lsn,
        });
    }

    // Issue #131: the same recompute staging, for step 3d's own fallback:
    // every from-side row a parent change reaches, image-less (#623 D5).
    for (from_table, key, hop_gen, src_changed, origin_lsn) in relationship_reverse_fallback {
        if hop_gen > MAX_HOP_GEN {
            hop_bound_tables.push(from_table);
            worst_hop_gen = worst_hop_gen.max(hop_gen);
            continue;
        }
        recompute_changes.push(StagedChange::Recompute {
            src_table: from_table,
            key,
            hop_gen,
            group_key: None,
            src_changed,
            prior_image: None,
            origin_lsn,
        });
    }

    if !hop_bound_tables.is_empty() {
        hop_bound_tables.sort();
        hop_bound_tables.dedup();
        // Debug: the drain's halt (#663) logs the closure it pauses for it.
        tracing::debug!(
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
        tracing::debug!(
            count = recompute_changes.len(),
            "staged downstream recomputes from this batch's physically-changed keys"
        );
    }
    append::append(txn, &recompute_changes).await?;

    // Issue #134: guard-rejected reverses, re-staged as their own kind —
    // deliberately a separate `append::append` call from `recompute_changes`
    // above, never subject to the hop-bound check that ran just above it
    // (this op has no `hop_gen` to bound in the first place). Landing in the
    // *active* segment (`append::append` always resolves the pointer fresh,
    // inside this same `txn`) rather than the one being drained is the same
    // property every other Phase-3 producer already relies on (doc 05
    // property 1) — this reuses that exact mechanism, not a new one.
    if !relationship_reverse_deferrals.is_empty() {
        tracing::debug!(
            count = relationship_reverse_deferrals.len(),
            "staged deferred relationship reverse retries (issue #134)"
        );
    }
    append::append(txn, &relationship_reverse_deferrals).await?;

    // 4b. Issue #16: a clean drain clears the death counters for every key
    // it just applied (not the poisoned ones it parked above) — doc 06's
    // "clean drain clears counters for keys it applied."
    quarantine::clear_key_deaths(txn, &plan.applied_keys).await?;

    // 5. Completion: release each coalesced segment's claim and mark its
    // buckets drained, in one statement per segment — inherently per-segment
    // (each has its own `seg_claims` rows and `drained_mask`), unlike steps
    // 1-4 above, which already ran once for the whole coalesced batch.
    //
    // Issue #620 A2a: a page that isn't its buckets' last instead checks the
    // claim and advances their cursor. See `end_segment_step`.
    let mut segments_drained = Vec::with_capacity(steps.len());
    for step in steps {
        let batch_drained = end_segment_step(txn, step, claimed_by).await?;
        segments_drained.push((step.seg_seq, batch_drained));
    }

    // 6. Wake anything awaiting convergence.
    txn.execute("select pg_notify($1, '')", &[&wake_channel])
        .await?;

    // Issue #166: every DML statement this Phase 3 pass will ever issue
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
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    for target in plan
        .targets
        .values()
        .map(|t| &t.qualified_target)
        .chain(plan.ledger_targets.values().map(|t| &t.target))
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
        deferral_counts,
        fairness_escalations,
        pages: 1,
    })
}

/// Step 5 of [`apply_page`] for one segment: returns whether this
/// transaction flipped the segment to `'drained'`.
///
/// **A non-final page** (issue #620 A2a) runs the claim check, then advances
/// the cursor:
///
/// - `update seg_claims set claimed_at = clock_timestamp() ... returning
///   bucket` must return every bucket the worker still holds, or the page is
///   [`ApplyError::ClaimLost`] and its whole transaction rolls back. The row
///   lock this takes is what makes `RECLAIM_STALE_SQL`'s `skip locked` pass
///   over an in-flight page, and the update doubles as a heartbeat. It stamps
///   the clock, not `now()`: `now()` is the transaction's start, so a page
///   whose apply ran for 20 s would commit a claim already 20 s old,
///   overwriting the daemon's fresher refresh from mid-page (issue #654). A
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
        tracing::warn!(
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
    // `claim::claim` documents (issue #690). Deleting first, then waiting
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

/// Issue #166 (a genuine `SIGKILL`-mid-drain test, not an in-process
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
    tracing::warn!(
        trigger_path,
        "TRELLIS_TEST_PAUSE_TRIGGER fired: pausing Phase 3 indefinitely before commit \
         (test-only hook, issue #166)"
    );
    if let Ok(marker_path) = std::env::var("TRELLIS_TEST_PAUSE_MARKER")
        && let Err(err) = std::fs::write(&marker_path, b"paused")
    {
        tracing::warn!(?err, marker_path, "failed to write Phase 3 pause marker");
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
///
/// No longer `Copy` as of issue #134/#135's review follow-up
/// (`deferral_counts` is a `HashMap`) — every existing call site only ever
/// read this by field or by a single `let` binding, never relied on
/// implicit copies, so this is additive in practice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub batch_drained: bool,
    /// Issue #134/#135: per-guard deferral counts this call's Phase 3 pass
    /// discovered, keyed by [`ReverseGuardFailure::metric_label`] — the
    /// caller's own responsibility to flush into
    /// [`crate::metrics::increment_relationship_reverse_deferred`] *after*
    /// this call's transaction has actually committed (see
    /// [`flush_apply_metrics`]'s doc comment for why: this whole call can
    /// be retried in full — a losing attempt's counts must never reach the
    /// registry). Empty whenever no reverse guard rejected anything this
    /// pass, which — reused from [`ManyApplyOutcome::deferral_counts`] via
    /// [`apply_and_mark_drained`]'s wrapper — is the overwhelming common
    /// case.
    pub deferral_counts: HashMap<&'static str, u64>,
    /// Issue #135: count of to-one relationship reverse transitions this
    /// call's Phase 3 pass resolved via fairness escalation (see
    /// [`RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`]'s doc comment and the
    /// "Issue #135" design section right after [`check_reverse_guards`])
    /// rather than deferring again — the caller's own responsibility to
    /// flush into
    /// [`crate::metrics::increment_relationship_reverse_fairness_escalated`]
    /// under the same post-commit-only contract as `deferral_counts` above.
    /// Reused from [`ManyApplyOutcome::fairness_escalations`] via
    /// [`apply_and_mark_drained`]'s wrapper.
    pub fairness_escalations: u64,
    /// Issue #620: see [`ManyApplyOutcome::pages`].
    pub pages: usize,
}

/// What one successful [`apply_and_mark_drained_many`] call did — the
/// coalesced-segment counterpart to [`ApplyOutcome`] (issue #63 Milestone
/// 2): the same physically-written/deleted key counts, now totalled across
/// every segment this call drained from, plus each individual segment's own
/// `(seg_seq, batch_drained)` completion result — a coalesced call can flip
/// some of its segments to `'drained'` while leaving others still short a
/// peer's bucket, exactly as any one of them would on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManyApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub segments_drained: Vec<(i64, bool)>,
    /// Issue #134/#135 review follow-up: buffered, not recorded eagerly —
    /// see [`ApplyOutcome::deferral_counts`]'s doc comment (this field is
    /// that one's source, for the coalesced-segment path).
    pub deferral_counts: HashMap<&'static str, u64>,
    /// Issue #135: see [`ApplyOutcome::fairness_escalations`]'s doc comment
    /// (this field is that one's source, for the coalesced-segment path).
    pub fairness_escalations: u64,
    /// Issue #620: how many compute-and-apply transactions (pages) the drain
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

/// How long one page keeps retrying lock timeouts (ADR-0002 I7, issue #621)
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

/// [`drain_batch`]'s state for retrying transient failures (issue #621):
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
/// folded records one drain batch holds at once (issue #620, ADR-0002). A
/// worker's peak memory is about this many changes' worth, whatever size the
/// segment it drains grew to: ~0.8 GB across 8 workers at ~1 KB per
/// image-less change, ~3.2 GB at ~4 KB per image-bearing one.
pub const DEFAULT_DRAIN_BATCH_CAP: usize = 100_000;

/// Test seams for [`drain_many_with_hooks`] (issue #620 A2a): the
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
}

/// Runs one full drain of `seg_seq`: see [`drain_many`], which this is the
/// single-segment form of. Returns `Ok(None)` if this call's claim won (and
/// already owned) nothing.
///
/// Issue #56/ADR-0009 decision 3: the outermost span in the propagation
/// tree's apply phase — parent, across however many retries and pages this
/// call takes, of every [`compute`]/[`apply_and_mark_drained`] span (and,
/// through those, every per-transform [`apply_target`] span). `attempt` is
/// recorded once per loop iteration, so its final exported value is however
/// many attempts the last page took.
#[tracing::instrument(
    name = "staging.drain_once",
    skip(pool, wake_channel, watermark),
    fields(claimed_by = %claimed_by, attempt = tracing::field::Empty)
)]
#[cfg(any(test, feature = "internals"))]
pub async fn drain_once(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    watermark: &StagedWatermark,
) -> Result<Option<ApplyOutcome>, ApplyError> {
    let outcome = drain_segments(
        pool,
        &[seg_seq],
        claimed_by,
        live_workers,
        wake_channel,
        watermark,
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
        deferral_counts: outcome.deferral_counts,
        fairness_escalations: outcome.fairness_escalations,
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
    watermark: &StagedWatermark,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    drain_many_with_cap(
        pool,
        seg_seqs,
        claimed_by,
        live_workers,
        wake_channel,
        watermark,
        DEFAULT_DRAIN_BATCH_CAP,
    )
    .await
}

/// Claims this worker's share of `seg_seqs` and drains it, holding at most
/// `drain_batch_cap` folded records at a time (issue #620 A2a).
///
/// **Phase 1, the claim**, is `CLAIM_SQL` per segment and a commit, nothing
/// else. The fold used to share the claim's transaction, so every peer's
/// `CLAIM_SQL` queued on the uncommitted `sealed -> draining` flip and
/// `seg_claims` rows for the whole fold (#328). The fenced window is immutable
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
/// Issue #56/ADR-0009 decision 3: [`drain_once`]'s doc comment describes the
/// span this creates — same role, just parenting a coalesced batch's spans
/// instead of a single segment's.
#[tracing::instrument(
    name = "staging.drain_many",
    skip(pool, wake_channel, watermark),
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
    watermark: &StagedWatermark,
    drain_batch_cap: usize,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    drain_segments(
        pool,
        seg_seqs,
        claimed_by,
        live_workers,
        wake_channel,
        watermark,
        drain_batch_cap,
        &mut DrainHooks::default(),
    )
    .await
}

/// [`drain_many_with_cap`] with [`DrainHooks`], for the exactly-once tests.
#[cfg(any(test, feature = "internals"))]
#[allow(clippy::too_many_arguments)]
pub async fn drain_many_with_hooks(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    watermark: &StagedWatermark,
    drain_batch_cap: usize,
    hooks: &mut DrainHooks,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    drain_segments(
        pool,
        seg_seqs,
        claimed_by,
        live_workers,
        wake_channel,
        watermark,
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
        for (label, count) in page.deferral_counts {
            *self.deferral_counts.entry(label).or_default() += count;
        }
        self.fairness_escalations += page.fairness_escalations;
        self.pages += page.pages;
    }
}

/// The body [`drain_once`], [`drain_many_with_cap`] and
/// [`drain_many_with_hooks`] share; see [`drain_many_with_cap`].
#[allow(clippy::too_many_arguments)]
async fn drain_segments(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    watermark: &StagedWatermark,
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

    // #622 C6: a `schema_changed` marker pauses the definitions that read a
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
                watermark,
            )
            .await?;
            return Ok(Some(outcome));
        }
        tracing::debug!(
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
        deferral_counts: HashMap::new(),
        fairness_escalations: 0,
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
                watermark,
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
    tracing::info!(
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
    watermark: &StagedWatermark,
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
                // Issue #16's one sanctioned exception to immutability: the
                // table this batch's folded rows name is gone, not any one
                // row's fault, so no retry or per-key quarantine resolves
                // it. Purge every ring/quarantine row naming it and retry
                // with it excluded. Not counted against
                // `MAX_APPLY_ATTEMPTS` — this corrects `folded` itself
                // rather than retrying the same input.
                tracing::warn!(
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
            // Every other Phase 2 failure (an evaluator error against a
            // malformed staged image is the common case) goes through the
            // exact same classification the Phase 3 branch below uses — a
            // bad key's image fails `compute()` for the whole batch just as
            // surely as it would fail Phase 3, and isolation must attribute
            // it the same way regardless of which phase first tripped over
            // it.
            Err(err) => {
                classify_and_retry(
                    pool,
                    representative_seg_seq,
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
        let txn = client.transaction().await?;
        match apply_page(&txn, steps, claimed_by, &plan, wake_channel, watermark).await {
            Ok(outcome) => {
                txn.commit().await?;
                // Epic #49 cross-cutting review fix (issues #51/#52): only
                // flush `plan`'s buffered metrics now, once this attempt's
                // transaction has actually committed — never from inside
                // `compute` itself, which the loop above may have called
                // more than once for this same `folded` input.
                flush_apply_metrics(&plan);
                // Issue #134/#135 review follow-up: same post-commit-only
                // contract, for the deferral counters this attempt's own
                // Phase 3 pass discovered.
                flush_relationship_reverse_deferral_metrics(&outcome.deferral_counts);
                // Issue #135: same post-commit-only contract, for the
                // fairness-escalation count this attempt's own Phase 3 pass
                // discovered.
                flush_relationship_reverse_fairness_escalation_metric(outcome.fairness_escalations);
                return Ok(outcome);
            }
            Err(err) => {
                let _ = txn.rollback().await;
                // Issue #670: isolation can run for many probes, each with
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
                    representative_seg_seq,
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

/// Classifies `err` (per [`quarantine::classify`]) and either retries or
/// propagates, shared by both [`drain_once`] and [`drain_many`]'s Phase 2
/// and Phase 3 failure arms so a bad key is attributed identically
/// regardless of which phase — or which of the two orchestrators —
/// first surfaced it.
///
/// `Ok(())` means "retry with `folded` unchanged": a version fence miss or a
/// transient failure, within [`MAX_APPLY_ATTEMPTS`]; a halt, see [`halt`];
/// or an isolation that poisoned at least one key for the definition it
/// fails in, which the retry's recompute leaves out of that definition's
/// apply (#799).
/// `Err(_)` propagates `err` (or a probe's own halting error, once its halt
/// paused nothing, see [`halt`]) unmodified, once retries are exhausted or
/// the failure must never be retried at all —
/// which includes an isolate attempt that evicted nothing, whether no key
/// reproduced the failure or the keys that did are still below the death
/// threshold. That ends the drain call; the next drain cycle re-reads the
/// batch and charges again (see [`quarantine::isolate_and_evict`]'s
/// "one charge per drain call").
#[allow(clippy::too_many_arguments)]
async fn classify_and_retry(
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
) -> Result<(), ApplyError> {
    let attempt = *attempts;
    // Issue #620 A2a: a lost claim is nobody's key's fault, and nothing in
    // this call can get it back. Isolating it would probe every record under
    // the same lost claim, reproduce `ClaimLost` for each, and charge every
    // key in the page a death. Surface it: the caller releases, and whoever
    // holds the buckets now resumes from the last committed cursor. Bare or
    // wrapped (issue #670), as `classify` decides by the innermost error.
    if quarantine::is_claim_lost(&err) {
        return Err(err);
    }
    match quarantine::classify(&err) {
        // Version fence miss: reload schema and retry, backing off only on
        // consecutive misses — `FenceMissBackoff` is exactly that state
        // machine, reused as-is.
        quarantine::FailureClass::VersionFenceMiss => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                tracing::warn!(
                    seg_seq,
                    attempt,
                    error = %err,
                    "version fence miss retries exhausted; surfacing the failure"
                );
                return Err(err);
            }
            tracing::debug!(seg_seq, attempt, error = %err, "version fence miss; retrying");
            let delay = backoff.next_delay();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        }
        // A lock wait that hit `lock_timeout` (ADR-0002 I7, issue #621):
        // the page's transaction has rolled back, so it holds no snapshot
        // and no locks while it waits here. Back off and retry, keeping the
        // claim (the heartbeat daemon keeps it fresh), for up to
        // `LOCK_RETRY_BUDGET` rather than `MAX_APPLY_ATTEMPTS`: whoever holds
        // the lock may hold it for many timeouts, and releasing the claim
        // only hands the same wait to the next claimant.
        quarantine::FailureClass::Transient if crate::locks::is_lock_not_available(&err) => {
            let waited = transient.lock_waited();
            if waited >= LOCK_RETRY_BUDGET {
                tracing::warn!(
                    seg_seq,
                    waited_ms = waited.as_millis() as u64,
                    error = %err,
                    "lock timeout retries exhausted; surfacing the failure"
                );
                return Err(err);
            }
            let delay = transient.next_delay();
            tracing::warn!(
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
        // consecutive transient failures (issue #621) on a schedule of its
        // own, so a fence miss between two doesn't reset it.
        quarantine::FailureClass::Transient => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                tracing::warn!(
                    seg_seq,
                    attempt,
                    error = %err,
                    "transient failure retries exhausted; surfacing the failure"
                );
                return Err(err);
            }
            let delay = transient.next_delay();
            tracing::debug!(
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
        // reaches and retry without it (#663).
        quarantine::FailureClass::Halting => halt(pool, seg_seq, attempts, halt_retry, err).await,
        // Everything else: bisect the folded records down to the ones that
        // fail alone to attribute the failure to specific key(s) (issue
        // #655), evicting any past the death
        // threshold and retrying without them. If nothing reproduces alone,
        // the error is surfaced, not blamed; if keys reproduce but none is
        // past the threshold yet, they are charged and the error is surfaced
        // too (the next drain cycle charges them again).
        quarantine::FailureClass::Isolate => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                tracing::warn!(
                    seg_seq,
                    attempt,
                    error = %err,
                    "isolate-eligible failure retries exhausted; surfacing the failure"
                );
                return Err(err);
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
                Err(probe_err) => return Err(probe_err),
            };
            match outcome {
                // #799: the key is poisoned for the definition it fails in
                // only, so the retry recomputes the same records, and leaves
                // the key out of that definition's apply alone.
                quarantine::IsolationOutcome::Evicted { evicted, charged } => {
                    if charged.is_empty() {
                        tracing::warn!(
                            seg_seq,
                            evicted,
                            "isolated a key and poisoned it for the definition it fails in; \
                             retrying without it there"
                        );
                    } else {
                        tracing::warn!(
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
                    tracing::warn!(
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
                    Err(err)
                }
                quarantine::IsolationOutcome::NothingReproduced => {
                    tracing::debug!(
                        seg_seq,
                        error = %err,
                        "isolation reproduced nothing; surfacing the original failure"
                    );
                    Err(err)
                }
                // Warn: unlike `NothingReproduced`, part of the batch went
                // unprobed, so a failing key may still be in it (issue #655).
                quarantine::IsolationOutcome::ProbeLimitReached { probes } => {
                    tracing::warn!(
                        seg_seq,
                        probes,
                        error = %err,
                        "isolation hit its probe limit without pinning the failure on a key; \
                         surfacing the original failure"
                    );
                    Err(err)
                }
                // Warn: a lock or deadlock storm stopped isolation before it
                // could look at the batch (issue #670). Surfacing ends this
                // drain call, so the page is retried on a later drain rather
                // than isolated again while the storm lasts. Retrying here
                // instead would hold the claim through up to
                // `MAX_APPLY_ATTEMPTS` more storms, each as long as
                // `MAX_CONSECUTIVE_TRANSIENT_PROBES` lock timeouts.
                quarantine::IsolationOutcome::TransientStorm { probes } => {
                    tracing::warn!(
                        seg_seq,
                        probes,
                        consecutive_transient = quarantine::MAX_CONSECUTIVE_TRANSIENT_PROBES,
                        error = %err,
                        "isolation stopped: its latest probes all hit transient errors; charged \
                         nothing, surfacing the original failure so a later drain retries the page"
                    );
                    Err(err)
                }
                quarantine::IsolationOutcome::FuseDisabled => {
                    tracing::debug!(
                        seg_seq,
                        error = %err,
                        "row-level death fuse disabled; surfacing the original failure unisolated"
                    );
                    Err(err)
                }
            }
        }
    }
}

/// [`classify_and_retry`]'s halting arm (#663): pauses every definition the
/// halting failure `err` reaches ([`super::halt::halt_closure`]) and retries
/// the page, whose recompute leaves them out. The retry is free, as for a
/// dropped source: it runs without the failing input rather than repeating
/// it. One error line per halt, from the call that paused the closure.
///
/// A halt that paused nothing (a peer paused the closure first, or the
/// failure persists without one) retries once per [`drain_batch`] call, for
/// the peer's case, then surfaces `err` as before, so the page can't spin.
///
/// A refused read or write (`42501`, issue #766) also has every retry skip
/// the tables no unfrozen definition reads ([`HaltRetry::skip_frozen`]).
async fn halt(
    pool: &Pool,
    seg_seq: i64,
    attempts: &mut u32,
    halt_retry: &mut HaltRetry,
    err: ApplyError,
) -> Result<(), ApplyError> {
    let paused = super::halt::halt_closure(pool, &err).await?;
    if quarantine::is_insufficient_privilege(&err) {
        halt_retry.skip_frozen = true;
    }
    if !paused.is_empty() {
        tracing::error!(
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
        tracing::debug!(
            seg_seq,
            error = %err,
            "halting failure whose closure is already paused; retrying the page once"
        );
        return Ok(());
    }
    Err(err)
}

/// What [`halt`] keeps across one [`drain_batch`] call's retries.
#[derive(Debug, Default)]
struct HaltRetry {
    /// Whether this call has retried a halt that paused nothing (#663).
    retried: bool,
    /// Whether Postgres refused the drain a read or write (`42501`, issue
    /// #766), so every later attempt skips the tables no unfrozen
    /// definition reads ([`compute_page`]). The halt pauses the readers of
    /// the refused table, but the page reads it for a relationship's
    /// settled projection whatever its readers' status, so without the skip
    /// the retry would be refused again. A resume, or a new definition,
    /// refreshes the projections it reads, as for a table skipped for its
    /// key (#768).
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
/// publishes its fence (issue #598), so before that they are column defaults,
/// not the batch's. A segment returned here unfenced could be fenced by the
/// time its caller claims it, with a truncate this query never saw, and be
/// coalesced behind a predecessor the barrier says must drain first.
///
/// The one exception is the truncate barrier (issue #60): a truncate is
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
#[cfg(any(test, feature = "internals"))]
pub async fn next_claimable_segment(
    client: &impl GenericClient,
) -> Result<Option<i64>, ApplyError> {
    Ok(next_claimable_segments(client, 1).await?.into_iter().next())
}

/// [`next_claimable_segment`] generalized to return several claimable
/// segments at once (issue #63 Milestone 2), for [`drain_many`] to coalesce —
/// the batch a burst of quickly-sealing segments needs so each one doesn't
/// pay its own full compute-and-apply pass.
///
/// Issue #620 A2a: bounded by `drain_batch_cap`, not a segment count. The
/// first segment always comes back, however large; the ones after it come
/// back in ascending order while their `segments.row_count`s sum to at most
/// the cap (an oversized first segment doesn't count against them: if peers
/// hold all of it, this worker coalesces what follows instead of idling).
/// [`drain_many`] then claims in order and stops at the first share that
/// must page, so an oversized segment still drains alone. Row counts are
/// ring rows over the whole fenced window (issue #598), never fewer than the
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
        .query(
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
